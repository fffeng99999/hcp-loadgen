use crate::config::OutputConfig;
use anyhow::Result;
use hdrhistogram::Histogram;
use prometheus::{Encoder, HistogramOpts, HistogramVec, IntCounter, IntGauge, Registry, TextEncoder};
use serde::Serialize;
use std::fs::File;
use std::io::{self, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};

/// 指标收集器的外部句柄，内部通过 Arc 共享状态，支持多线程克隆。
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

/// 指标收集器的内部状态，维护发送、成功、失败计数、延迟直方图、系统资源使用及输出目标。
struct MetricsInner {
    /// 已发送交易总数
    sent: AtomicU64,
    /// 成功交易总数
    success: AtomicU64,
    /// 被拒绝/失败交易总数
    reject: AtomicU64,
    /// 成功交易的总延迟（微秒）
    total_latency_us: AtomicU64,
    /// 延迟直方图，用于计算分位值（微秒级）
    latency_hist: Mutex<Histogram<u64>>,
    /// CPU 使用率（乘以 100 的整数）
    cpu_percent: AtomicU64,
    /// 内存使用量（字节）
    mem_bytes: AtomicU64,
    /// 指标收集开始时间
    start: Instant,
    /// Prometheus 注册表
    registry: Registry,
    /// Prometheus 计数器：已发送
    prom_sent: IntCounter,
    /// Prometheus 计数器：成功
    prom_success: IntCounter,
    /// Prometheus 计数器：被拒绝
    prom_reject: IntCounter,
    /// Prometheus 延迟直方图向量（按 success/reject 标签区分）
    prom_latency: HistogramVec,
    /// Prometheus 仪表盘：CPU 使用率
    prom_cpu: IntGauge,
    /// Prometheus 仪表盘：内存使用量
    prom_mem: IntGauge,
    /// CSV 写入器（可选）
    csv: Option<Mutex<csv::Writer<File>>>,
    /// 输出配置
    output: OutputConfig,
    /// 指标采集间隔（毫秒）
    metrics_interval_ms: u64,
}

/// 指标快照，用于 JSON 输出、CSV 导出或实时监控。
#[derive(Debug, Clone, Serialize)]
pub struct MetricsSnapshot {
    /// 已运行时间（秒）
    pub elapsed_s: f64,
    /// 已发送交易数
    pub sent: u64,
    /// 成功交易数
    pub success: u64,
    /// 被拒绝交易数
    pub reject: u64,
    /// 实际 TPS（成功交易 / 已运行时间）
    pub actual_tps: f64,
    /// 成功率
    pub success_rate: f64,
    /// 拒绝率
    pub reject_rate: f64,
    /// 平均延迟（毫秒）
    pub latency_avg_ms: f64,
    /// P50 延迟（毫秒）
    pub latency_p50_ms: f64,
    /// P90 延迟（毫秒）
    pub latency_p90_ms: f64,
    /// P95 延迟（毫秒）
    pub latency_p95_ms: f64,
    /// P99 延迟（毫秒）
    pub latency_p99_ms: f64,
    /// CPU 使用率（%）
    pub cpu_percent: f64,
    /// 内存使用量（字节）
    pub mem_bytes: u64,
}

impl Metrics {
    /// 创建新的指标收集器，初始化 Prometheus 指标、可选 CSV 文件和延迟直方图。
    pub fn new(output: OutputConfig, metrics_interval_ms: u64) -> Result<Self> {
        let registry = Registry::new();
        let prom_sent = IntCounter::new("hcp_loadgen_sent_total", "sent total")?;
        let prom_success = IntCounter::new("hcp_loadgen_success_total", "success total")?;
        let prom_reject = IntCounter::new("hcp_loadgen_reject_total", "reject total")?;
        let prom_latency = HistogramVec::new(
            HistogramOpts::new("hcp_loadgen_latency_ms", "latency ms"),
            &["result"],
        )?;
        let prom_cpu = IntGauge::new("hcp_loadgen_cpu_percent", "cpu percent")?;
        let prom_mem = IntGauge::new("hcp_loadgen_mem_bytes", "mem bytes")?;
        registry.register(Box::new(prom_sent.clone()))?;
        registry.register(Box::new(prom_success.clone()))?;
        registry.register(Box::new(prom_reject.clone()))?;
        registry.register(Box::new(prom_latency.clone()))?;
        registry.register(Box::new(prom_cpu.clone()))?;
        registry.register(Box::new(prom_mem.clone()))?;

        let csv = if let Some(path) = &output.csv_path {
            let file = File::create(path)?;
            let mut writer = csv::Writer::from_writer(file);
            writer.write_record([
                "elapsed_s",
                "sent",
                "success",
                "reject",
                "actual_tps",
                "success_rate",
                "reject_rate",
                "latency_avg_ms",
                "latency_p50_ms",
                "latency_p90_ms",
                "latency_p95_ms",
                "latency_p99_ms",
                "cpu_percent",
                "mem_bytes",
            ])?;
            Some(Mutex::new(writer))
        } else {
            None
        };

        Ok(Self {
            inner: Arc::new(MetricsInner {
                sent: AtomicU64::new(0),
                success: AtomicU64::new(0),
                reject: AtomicU64::new(0),
                total_latency_us: AtomicU64::new(0),
                latency_hist: Mutex::new(Histogram::new(3)?),
                cpu_percent: AtomicU64::new(0),
                mem_bytes: AtomicU64::new(0),
                start: Instant::now(),
                registry,
                prom_sent,
                prom_success,
                prom_reject,
                prom_latency,
                prom_cpu,
                prom_mem,
                csv,
                output,
                metrics_interval_ms,
            }),
        })
    }

    /// 记录一笔已发送的交易。
    pub fn record_sent(&self) {
        self.inner.sent.fetch_add(1, Ordering::Relaxed);
        self.inner.prom_sent.inc();
    }

    /// 记录一笔成功的交易及其延迟。
    pub fn record_success(&self, latency_ms: f64) {
        let latency_us = (latency_ms * 1000.0) as u64;
        self.inner.success.fetch_add(1, Ordering::Relaxed);
        self.inner.total_latency_us.fetch_add(latency_us, Ordering::Relaxed);
        self.inner.prom_success.inc();
        self.inner
            .prom_latency
            .with_label_values(&["success"])
            .observe(latency_ms);
        if let Ok(mut hist) = self.inner.latency_hist.try_lock() {
            let _ = hist.record(latency_us);
        }
    }

    /// 记录一笔被拒绝或失败的交易及其延迟。
    pub fn record_reject(&self, latency_ms: f64) {
        let latency_us = (latency_ms * 1000.0) as u64;
        self.inner.reject.fetch_add(1, Ordering::Relaxed);
        self.inner.prom_reject.inc();
        self.inner
            .prom_latency
            .with_label_values(&["reject"])
            .observe(latency_ms);
        if let Ok(mut hist) = self.inner.latency_hist.try_lock() {
            let _ = hist.record(latency_us);
        }
    }

    /// 获取当前指标快照，计算 TPS、成功率、延迟分位值和系统资源使用。
    pub fn snapshot(&self) -> MetricsSnapshot {
        let sent = self.inner.sent.load(Ordering::Relaxed);
        let success = self.inner.success.load(Ordering::Relaxed);
        let reject = self.inner.reject.load(Ordering::Relaxed);
        let elapsed = self.inner.start.elapsed().as_secs_f64();
        let actual_tps = if elapsed > 0.0 {
            success as f64 / elapsed
        } else {
            0.0
        };
        let success_rate = if sent > 0 {
            success as f64 / sent as f64
        } else {
            0.0
        };
        let reject_rate = if sent > 0 {
            reject as f64 / sent as f64
        } else {
            0.0
        };
        let total_latency_us = self.inner.total_latency_us.load(Ordering::Relaxed);
        let latency_avg_ms = if success > 0 {
            total_latency_us as f64 / success as f64 / 1000.0
        } else {
            0.0
        };
        let (latency_p50_ms, latency_p90_ms, latency_p95_ms, latency_p99_ms) = if let Ok(hist) =
            self.inner.latency_hist.try_lock()
        {
            (
                hist.value_at_quantile(0.50) as f64 / 1000.0,
                hist.value_at_quantile(0.90) as f64 / 1000.0,
                hist.value_at_quantile(0.95) as f64 / 1000.0,
                hist.value_at_quantile(0.99) as f64 / 1000.0,
            )
        } else {
            (0.0, 0.0, 0.0, 0.0)
        };
        let cpu_percent = self.inner.cpu_percent.load(Ordering::Relaxed) as f64 / 100.0;
        let mem_bytes = self.inner.mem_bytes.load(Ordering::Relaxed);
        MetricsSnapshot {
            elapsed_s: elapsed,
            sent,
            success,
            reject,
            actual_tps,
            success_rate,
            reject_rate,
            latency_avg_ms,
            latency_p50_ms,
            latency_p90_ms,
            latency_p95_ms,
            latency_p99_ms,
            cpu_percent,
            mem_bytes,
        }
    }

    /// 启动后台任务：定期采集系统资源（CPU/内存）、输出 JSON/CSV 指标，以及暴露 Prometheus 端点。
    pub fn start_background(&self) {
        let metrics = self.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(metrics.inner.metrics_interval_ms));
            let mut system = System::new_with_specifics(
                RefreshKind::new()
                    .with_cpu(CpuRefreshKind::everything())
                    .with_memory(MemoryRefreshKind::everything()),
            );
            loop {
                ticker.tick().await;
                system.refresh_cpu();
                system.refresh_memory();
                let cpu = system.global_cpu_info().cpu_usage();
                let mem = system.used_memory();
                metrics
                    .inner
                    .cpu_percent
                    .store((cpu * 100.0) as u64, Ordering::Relaxed);
                metrics
                    .inner
                    .mem_bytes
                    .store(mem, Ordering::Relaxed);
                metrics.inner.prom_cpu.set((cpu * 100.0) as i64);
                metrics.inner.prom_mem.set(mem as i64);
            }
        });

        let json_metrics = self.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_millis(json_metrics.inner.output.json_interval_ms));
            loop {
                ticker.tick().await;
                let snapshot = json_metrics.snapshot();
                if let Ok(line) = serde_json::to_string(&snapshot) {
                    println!("{}", line);
                    let _ = io::stdout().flush();
                }
                if let Some(writer) = &json_metrics.inner.csv {
                    let mut writer = writer.lock().await;
                    let _ = writer.serialize(&snapshot);
                    let _ = writer.flush();
                }
            }
        });

        if let Some(addr) = self.inner.output.prometheus_addr.clone() {
            let registry = self.inner.registry.clone();
            tokio::spawn(async move {
                let app = axum::Router::new().route(
                    "/metrics",
                    axum::routing::get(move || async move { encode_metrics(&registry) }),
                );
                if let Ok(listener) = tokio::net::TcpListener::bind(addr).await {
                    let _ = axum::serve(listener, app).await;
                }
            });
        }
    }
}

/// 将 Prometheus 注册表中的指标编码为文本格式。
fn encode_metrics(registry: &Registry) -> String {
    let encoder = TextEncoder::new();
    let metric_families = registry.gather();
    let mut buffer = Vec::new();
    let _ = encoder.encode(&metric_families, &mut buffer);
    String::from_utf8(buffer).unwrap_or_default()
}
