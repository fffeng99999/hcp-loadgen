/**
 * hcp-loadgen 主入口模块
 *
 * 负责整个负载生成器的启动流程编排，包括：
 * - 加载配置
 * - 初始化指标采集系统
 * - 连接数据库并加载/生成测试账户
 * - 创建交易广播器（HTTP 或 gRPC）
 * - 启动调度器发送交易
 * - 优雅关闭与结果持久化
 */

// 子模块声明
mod account_pool;   // 账户池管理：维护测试账户并提供并发安全的账户选择
mod config;         // 配置系统：支持 TOML/CLI/环境变量加载
mod core;           // 核心引擎：调度器、交易构建、签名、广播
mod metrics;        // 指标采集：延迟、TPS、成功率、系统资源等
mod persistence;    // 持久化层：PostgreSQL 存储
mod types;          // 核心数据结构：账户、交易记录、余额快照等

use account_pool::AccountPool;
use anyhow::Result;
use config::{load_config, Protocol};
use core::broadcaster::{Broadcaster, GrpcBroadcaster, HttpBroadcaster};
use core::scheduler::Scheduler;
use metrics::Metrics;
use persistence::storage::{Storage, StorageConfig};
use serde_json::json;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use types::TransactionRecord;

/**
 * 程序主入口
 *
 * 执行流程：
 * 1. 加载配置并计算有效参数（背压阈值、通道容量等）
 * 2. 初始化指标系统并启动后台采集
 * 3. 连接 PostgreSQL，加载或生成测试账户池
 * 4. 根据配置选择账户选择模式（Zipf/Random/RoundRobin）
 * 5. 创建对应协议的广播器（HTTP/gRPC）
 * 6. 启动持久化后台任务，接收交易记录并批量写入数据库
 * 7. 创建调度器并运行，支持 Ctrl-C 优雅关闭
 * 8. 实验结束后输出最终指标并刷写余额/账户信息到数据库
 */
#[tokio::main]
async fn main() -> Result<()> {
    // 加载配置：从 TOML 文件、环境变量或命令行参数解析
    let config = load_config()?;

    // 计算有效的背压阈值：若配置为 0 则使用默认值 500 万
    let effective_backpressure = if config.backpressure_threshold == 0 {
        5_000_000
    } else {
        config.backpressure_threshold as u64
    };

    // 计算有效的通道容量：至少为 16，防止过小导致频繁阻塞
    let effective_channel_capacity = config.storage_channel_size.max(16);

    // 获取当前时间戳（毫秒），用于日志输出
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;

    // 打印启动配置摘要（JSON 格式，便于下游工具解析）
    println!(
        "{}",
        json!({
            "timestamp_ms": timestamp_ms,
            "worker_threads": config.worker_threads,
            "worker_buffer_capacity": config.worker_buffer_capacity,
            "backpressure_threshold": effective_backpressure,
            "channel_capacity": effective_channel_capacity,
            "target_tps": config.target_tps,
            "concurrency": config.concurrency
        })
    );

    // 初始化指标系统：支持 JSON 日志、CSV 文件、Prometheus 三种输出
    let metrics = Metrics::new(config.output.clone(), config.metrics_interval_ms)?;
    metrics.start_background();

    // 创建数据库连接池并初始化存储层
    let storage = Storage::new(StorageConfig {
        database_url: config.database_url.clone(),
        db_schema: config.db_schema.clone(),
        reset_schema_on_start: config.reset_schema_on_start,
        max_connections: config.storage_max_connections,
    })
    .await?;

    // 加载初始账户状态：优先从数据库加载已有账户，否则生成新账户
    let mut pool: AccountPool = storage
        .load_initial_state(
            config.account_count,
            config.initial_nonce,
            config.initial_balance,
            config.account_file.as_ref().map(PathBuf::from),
        )
        .await?;

    // 根据配置设置账户选择模式
    match config.account_selection_mode {
        config::AccountSelectionMode::Zipf => {
            // Zipf 分布：模拟真实场景中的热点账户访问模式
            pool.set_zipf_mode(config.zipf_alpha);
            println!("Account selection: Zipf (alpha={})", config.zipf_alpha);
        }
        config::AccountSelectionMode::Random => {
            // 随机选择：每个账户被选中的概率均等
            pool.set_random_mode(true);
            println!("Account selection: Random");
        }
        _ => {
            // 轮询：按顺序循环选择账户，保证均匀分布
            println!("Account selection: RoundRobin");
        }
    }

    // 根据配置协议创建对应的广播器，使用 Arc<dyn Broadcaster> 实现多态共享
    let broadcaster: Arc<dyn Broadcaster> = match config.protocol {
        Protocol::Http => Arc::new(HttpBroadcaster::new(
            config.http_endpoint.clone(),
            config.concurrency,
        )?),
        Protocol::Grpc => Arc::new(
            GrpcBroadcaster::new(config.grpc_endpoint.clone(), config.broadcast_mode.clone()).await?,
        ),
    };

    // 背压计数器：记录当前待持久化的交易记录数量
    let backlog_records = Arc::new(AtomicU64::new(0));

    // 创建异步通道：Worker 发送交易记录，持久化任务接收并批量写入数据库
    let (persist_tx, mut persist_rx) =
        mpsc::channel::<Vec<TransactionRecord>>(effective_channel_capacity);

    // 克隆存储和计数器引用，用于持久化后台任务
    let persist_storage = storage.clone();
    let persist_backlog = backlog_records.clone();

    // 启动持久化后台任务：循环接收交易记录批次，批量写入数据库
    let persist_task = tokio::spawn(async move {
        while let Some(records) = persist_rx.recv().await {
            let count = records.len() as u64;
            // 批量写入交易记录到数据库
            if let Err(err) = persist_storage.flush_trade_batch(records).await {
                eprintln!("spill flush failed: {}", err);
            }
            // 写入完成后减少背压计数
            persist_backlog.fetch_sub(count, Ordering::Relaxed);
        }
    });

    // 创建调度器：负责按配置模式（Fixed/Burst/Sustained/Jitter）调度交易发送
    let scheduler = Scheduler::new(
        config,
        pool.clone(),
        broadcaster,
        metrics.clone(),
        persist_tx,
        backlog_records,
    );

    // 创建关闭信号通道：用于接收 Ctrl-C 信号并通知调度器优雅退出
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // 主事件循环：调度器运行与信号监听并发执行
    tokio::select! {
        result = scheduler.run(shutdown_rx) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            // 收到 Ctrl-C 信号，发送关闭通知
            let _ = shutdown_tx.send(true);
        }
    }

    // 输出最终指标快照
    if let Ok(line) = serde_json::to_string(&metrics.snapshot()) {
        println!("{}", line);
    }

    // 释放调度器资源，触发 Worker 关闭
    drop(scheduler);

    // 等待持久化任务完成所有剩余数据的写入
    let _ = persist_task.await;

    // 获取最终余额和账户信息，刷写到数据库
    let final_balances = pool.snapshot_balances();
    let identities = pool.snapshot_accounts();
    storage
        .flush_results_to_db(Vec::new(), final_balances, identities)
        .await?;

    Ok(())
}
