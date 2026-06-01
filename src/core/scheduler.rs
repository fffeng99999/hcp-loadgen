use crate::account_pool::AccountPool;
use crate::config::{Config, SendMode};
use crate::core::broadcaster::Broadcaster;
use crate::core::signer::Signer;
use crate::core::tx_builder::{TxBuilder, TxKind};
use crate::metrics::Metrics;
use crate::types::TransactionRecord;
use anyhow::{anyhow, Result};
use base64::Engine;
use rand::Rng;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};

/// 工作线程消息枚举，用于控制工作线程的发送和关闭。
enum WorkerMessage {
    /// 触发一次交易发送
    Dispatch,
    /// 通知工作线程优雅退出
    Shutdown,
}

/// 工作线程运行时句柄，包含所有工作线程的发送通道和 JoinHandle。
struct WorkerRuntime {
    senders: Vec<mpsc::Sender<WorkerMessage>>,
    handles: Vec<JoinHandle<()>>,
}

/// CLI 签名上下文，当使用外部二进制（如 hcapd）进行签名时所需参数。
#[derive(Clone)]
struct CliSigningContext {
    cli_binary: String,
    chain_id: String,
    keyring_backend: String,
    keyring_home: String,
    rpc_endpoint: String,
    denom: String,
    fee_amount: u64,
    gas_limit: u64,
}

/// 调度器，负责根据配置的发送模式（Fixed/Burst/Sustained/Jitter）
/// 将交易分发给多个工作线程执行。
#[derive(Clone)]
pub struct Scheduler {
    config: Config,
    pool: AccountPool,
    builder: TxBuilder,
    signer: Arc<Signer>,
    broadcaster: Arc<dyn Broadcaster>,
    metrics: Metrics,
    dispatch_index: Arc<AtomicUsize>,
    persistence_sender: mpsc::Sender<Vec<TransactionRecord>>,
    backlog_records: Arc<AtomicU64>,
    worker_buffer_capacity: usize,
    backpressure_threshold: u64,
}

impl Scheduler {
    /// 创建新的调度器实例，初始化交易构建器、签名器和背压参数。
    pub fn new(
        config: Config,
        pool: AccountPool,
        broadcaster: Arc<dyn Broadcaster>,
        metrics: Metrics,
        persistence_sender: mpsc::Sender<Vec<TransactionRecord>>,
        backlog_records: Arc<AtomicU64>,
    ) -> Self {
        let kinds = vec![TxKind::from(config.tx_type.clone())];
        let worker_buffer_capacity = config.worker_buffer_capacity.max(1);
        let backpressure_threshold = if config.backpressure_threshold == 0 {
            5_000_000
        } else {
            config.backpressure_threshold as u64
        };
        let builder = TxBuilder::new(
            config.payload_size,
            kinds,
            pool.addresses(),
            config.tx_encoding.clone(),
            config.compression.clone(),
            config.denom.clone(),
            config.send_amount,
            config.gas_limit,
        );
        Self {
            config,
            pool,
            builder,
            signer: Arc::new(Signer),
            broadcaster,
            metrics,
            dispatch_index: Arc::new(AtomicUsize::new(0)),
            persistence_sender,
            backlog_records,
            worker_buffer_capacity,
            backpressure_threshold,
        }
    }

    /// 调度器主入口，根据配置选择对应的发送模式并启动工作线程。
    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let workers = self.start_workers();
        let run_result = match self.config.mode {
            SendMode::Fixed => self.run_fixed(&workers, &mut shutdown).await,
            SendMode::Burst => self.run_burst(&workers, &mut shutdown).await,
            SendMode::Sustained => self.run_sustained(&workers, &mut shutdown).await,
            SendMode::Jitter => self.run_jitter(&workers, &mut shutdown).await,
        };
        self.stop_workers(workers).await;
        run_result
    }

    /// 启动若干工作线程，每个线程独立监听消息并处理交易发送。
    fn start_workers(&self) -> WorkerRuntime {
        let worker_count = self.config.worker_threads.max(1);
        let mut senders = Vec::with_capacity(worker_count);
        let mut handles = Vec::with_capacity(worker_count);
        for _ in 0..worker_count {
            let (sender, mut receiver) = mpsc::channel(self.config.storage_channel_size.max(16));
            senders.push(sender);
            let pool = self.pool.clone();
            let builder = self.builder.clone();
            let signer = self.signer.clone();
            let broadcaster = self.broadcaster.clone();
            let metrics = self.metrics.clone();
            let persistence_sender = self.persistence_sender.clone();
            let backlog_records = self.backlog_records.clone();
            let amount = self.config.send_amount as f64;
            let amount_u64 = self.config.send_amount;
            let cli_signing = self
                .config
                .keyring_home
                .as_ref()
                .map(|home| CliSigningContext {
                    cli_binary: self.config.cli_binary.clone(),
                    chain_id: self.config.chain_id.clone(),
                    keyring_backend: self.config.keyring_backend.clone(),
                    keyring_home: home.clone(),
                    rpc_endpoint: self.config.rpc_endpoint.clone(),
                    denom: self.config.denom.clone(),
                    fee_amount: self.config.fee_amount,
                    gas_limit: self.config.gas_limit,
                });
            let buffer_capacity = self.worker_buffer_capacity;
            let handle = tokio::spawn(async move {
                let mut local_buffer: Vec<TransactionRecord> = Vec::with_capacity(buffer_capacity);
                loop {
                    match receiver.recv().await {
                        Some(WorkerMessage::Dispatch) => {
                            process_send(
                                &pool,
                                &builder,
                                &signer,
                                &broadcaster,
                                &metrics,
                                amount,
                                amount_u64,
                                cli_signing.as_ref(),
                                &mut local_buffer,
                            )
                            .await;
                            if local_buffer.len() >= buffer_capacity {
                                flush_local_buffer(
                                    &persistence_sender,
                                    &backlog_records,
                                    &mut local_buffer,
                                )
                                .await;
                            }
                        }
                        Some(WorkerMessage::Shutdown) | None => {
                            flush_local_buffer(&persistence_sender, &backlog_records, &mut local_buffer)
                                .await;
                            break;
                        }
                    }
                }
            });
            handles.push(handle);
        }
        WorkerRuntime { senders, handles }
    }

    /// 向所有工作线程发送 Shutdown 消息并等待线程结束。
    async fn stop_workers(&self, workers: WorkerRuntime) {
        for sender in &workers.senders {
            let _ = sender.send(WorkerMessage::Shutdown).await;
        }
        for handle in workers.handles {
            let _ = handle.await;
        }
    }

    /// 固定间隔发送模式：按 target_tps 计算间隔，定时触发交易。
    async fn run_fixed(
        &self,
        workers: &WorkerRuntime,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let interval_ns = self.resolve_interval_ns();
        let start = Instant::now();
        let mut sent = 0u64;
        let mut ticker = interval(Duration::from_nanos(interval_ns));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    if self.should_stop(start, sent) {
                        break;
                    }
                    if self.dispatch_send(&workers.senders).await {
                        sent += 1;
                    }
                }
                _ = shutdown.changed() => break,
            }
        }
        Ok(())
    }

    /// 突发发送模式：每隔 burst_interval_ms 连续发送 burst_size 笔交易。
    async fn run_burst(
        &self,
        workers: &WorkerRuntime,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let start = Instant::now();
        let mut sent = 0u64;
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = self.send_burst(workers, start, &mut sent) => {},
            }
            if self.should_stop(start, sent) {
                break;
            }
            sleep(Duration::from_millis(self.config.burst_interval_ms)).await;
        }
        Ok(())
    }

    /// 持续饱和发送模式：尽可能快地循环发送交易，不主动等待。
    async fn run_sustained(
        &self,
        workers: &WorkerRuntime,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let start = Instant::now();
        let mut sent = 0u64;
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = self.dispatch_send(&workers.senders) => {
                    sent = sent.saturating_add(1);
                },
            }
            if self.should_stop(start, sent) {
                break;
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    /// 抖动发送模式：在固定间隔基础上增加随机抖动，模拟更真实的流量波动。
    async fn run_jitter(
        &self,
        workers: &WorkerRuntime,
        shutdown: &mut watch::Receiver<bool>,
    ) -> Result<()> {
        let base_interval_ns = self.resolve_interval_ns();
        let start = Instant::now();
        let mut sent = 0u64;
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = self.dispatch_send(&workers.senders) => {
                    sent = sent.saturating_add(1);
                },
            }
            if self.should_stop(start, sent) {
                break;
            }
            let jitter = rand::thread_rng().gen_range(0.0..self.config.jitter_range.max(0.0));
            let jitter_ns = (base_interval_ns as f64 * jitter / 100.0) as u64;
            sleep(Duration::from_nanos(base_interval_ns + jitter_ns)).await;
        }
        Ok(())
    }

    /// 执行一次突发发送，连续发送 burst_size 笔交易。
    async fn send_burst(&self, workers: &WorkerRuntime, start: Instant, sent: &mut u64) {
        for _ in 0..self.config.burst_size {
            if self.should_stop(start, *sent) {
                break;
            }
            if self.dispatch_send(&workers.senders).await {
                *sent = sent.saturating_add(1);
            }
        }
    }

    /// 将发送任务分派给下一个工作线程，支持背压检查。
    async fn dispatch_send(&self, senders: &[mpsc::Sender<WorkerMessage>]) -> bool {
        if senders.is_empty() {
            return false;
        }
        // 当在途记录数超过背压阈值时，短暂休眠等待消费
        while self.backlog_records.load(Ordering::Relaxed) > self.backpressure_threshold {
            sleep(Duration::from_millis(2)).await;
        }
        let idx = self.dispatch_index.fetch_add(1, Ordering::Relaxed) % senders.len();
        senders[idx].send(WorkerMessage::Dispatch).await.is_ok()
    }

    /// 根据配置计算发送间隔（纳秒），优先使用 send_interval_ns，否则按 target_tps 计算。
    fn resolve_interval_ns(&self) -> u64 {
        if self.config.send_interval_ns > 0 {
            self.config.send_interval_ns
        } else if self.config.target_tps == 0 {
            1_000_000_000
        } else {
            1_000_000_000 / self.config.target_tps
        }
    }

    /// 判断是否应该停止发送：达到持续时间或总交易数上限时返回 true。
    fn should_stop(&self, start: Instant, sent: u64) -> bool {
        if self.config.duration > 0 && start.elapsed().as_secs() >= self.config.duration {
            return true;
        }
        if self.config.total_txs > 0 && sent >= self.config.total_txs {
            return true;
        }
        false
    }
}

/// 处理单笔交易的发送流程：选账户 -> 构建交易 -> 签名 -> 广播 -> 记录指标和持久化数据。
#[allow(clippy::too_many_arguments)]
async fn process_send(
    pool: &AccountPool,
    builder: &TxBuilder,
    signer: &Signer,
    broadcaster: &Arc<dyn Broadcaster>,
    metrics: &Metrics,
    amount: f64,
    amount_u64: u64,
    cli_signing: Option<&CliSigningContext>,
    local_buffer: &mut Vec<TransactionRecord>,
) {
    let account = match pool.next_account() {
        Some(account) => account,
        None => {
            metrics.record_reject(0.0);
            return;
        }
    };
    let nonce = account.next_nonce();
    let mut tx = builder.build_tx(&account, nonce);
    // 根据是否使用 CLI 签名选择不同的签名/编码路径
    let payload = if let Some(signing) = cli_signing {
        if let Some(from_name) = account.signer_name.as_ref() {
            match build_cli_tx_bytes(signing, from_name, &tx.from, &tx.to, amount_u64, nonce).await {
                Ok(payload) => payload,
                Err(err) => {
                    eprintln!("build cli tx failed: {}", err);
                    metrics.record_reject(0.0);
                    return;
                }
            }
        } else {
            let preimage = format!("{}:{}:{}", tx.from, tx.nonce, tx.payload_hex);
            let signature = signer.sign(&account.priv_key, preimage.as_bytes());
            tx.signature_hex = hex::encode(signature);
            builder.encode_tx(&tx)
        }
    } else {
        let preimage = format!("{}:{}:{}", tx.from, tx.nonce, tx.payload_hex);
        let signature = signer.sign(&account.priv_key, preimage.as_bytes());
        tx.signature_hex = hex::encode(signature);
        builder.encode_tx(&tx)
    };
    metrics.record_sent();
    let (success, latency_ms) = match broadcaster.send(payload).await {
        Ok(result) => {
            if result.success {
                metrics.record_success(result.latency_ms);
            } else {
                metrics.record_reject(result.latency_ms);
            }
            (result.success, result.latency_ms)
        }
        Err(err) => {
            eprintln!("broadcast error: {}", err);
            metrics.record_reject(0.0);
            (false, 0.0)
        }
    };
    let side = if rand::thread_rng().gen_bool(0.5) {
        "BUY".to_string()
    } else {
        "SELL".to_string()
    };
    if success {
        account.apply_fill(amount, &side);
    }
    local_buffer.push(TransactionRecord {
        tx_hash: format!("{}:{}:{}:{}", tx.from, tx.nonce, tx.to, now_ms()),
        account_id: account.account_id,
        side,
        price: (amount / 10.0).max(1.0),
        quantity: amount,
        latency_ms,
        timestamp: now_ms() as i64,
    });
}

/// 将本地缓冲区中的交易记录批量刷新到持久化发送通道，并更新在途记录计数。
async fn flush_local_buffer(
    sender: &mpsc::Sender<Vec<TransactionRecord>>,
    backlog_records: &Arc<AtomicU64>,
    local_buffer: &mut Vec<TransactionRecord>,
) {
    if local_buffer.is_empty() {
        return;
    }
    let mut chunk = Vec::with_capacity(local_buffer.len());
    std::mem::swap(local_buffer, &mut chunk);
    let count = chunk.len() as u64;
    if sender.send(chunk).await.is_ok() {
        backlog_records.fetch_add(count, Ordering::Relaxed);
    }
}

/// 获取当前 Unix 时间戳（毫秒）。
fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

/// 从 genesis.json 中解析指定地址的 account_number 和 sequence。
fn resolve_account_info(genesis_path: &std::path::Path, address: &str) -> Option<(u64, u64)> {
    let content = std::fs::read_to_string(genesis_path).ok()?;
    let genesis: serde_json::Value = serde_json::from_str(&content).ok()?;
    let accounts = genesis.get("app_state")?.get("auth")?.get("accounts")?.as_array()?;
    for acc in accounts {
        let acc_addr = acc.get("address")?.as_str()?;
        if acc_addr == address {
            let account_number = acc.get("account_number")?.as_str()?.parse::<u64>().ok()?;
            let sequence = acc.get("sequence")?.as_str()?.parse::<u64>().ok()?;
            return Some((account_number, sequence));
        }
    }
    None
}

/// 使用外部 CLI（hcapd）构建并签名交易，返回编码后的交易字节。
async fn build_cli_tx_bytes(
    signing: &CliSigningContext,
    from_name: &str,
    from_address: &str,
    to_address: &str,
    amount: u64,
    sequence: u64,
) -> Result<Vec<u8>> {
    let amount_arg = format!("{}{}", amount.max(1), signing.denom);
    let fees_arg = format!("{}{}", signing.fee_amount.max(1), signing.denom);
    let gas_limit = signing.gas_limit.to_string();
    let from_name_owned = from_name.to_string();
    let from_address_owned = from_address.to_string();
    let to_address_owned = to_address.to_string();
    let cli_binary = signing.cli_binary.clone();
    let chain_id = signing.chain_id.clone();
    let keyring_backend = signing.keyring_backend.clone();
    let keyring_home = signing.keyring_home.clone();
    let rpc_endpoint = signing.rpc_endpoint.clone();

    let genesis_path = std::path::Path::new(&keyring_home).join("config").join("genesis.json");
    let (account_number, _) = resolve_account_info(&genesis_path, from_address).unwrap_or((0, 0));

    // 第一步：生成未签名交易 JSON
    let unsigned = tokio::task::spawn_blocking({
        let cli_binary = cli_binary.clone();
        let chain_id = chain_id.clone();
        let keyring_backend = keyring_backend.clone();
        let keyring_home = keyring_home.clone();
        let rpc_endpoint = rpc_endpoint.clone();
        let from_name = from_name_owned.clone();
        let to_address = to_address_owned.clone();
        let amount_arg = amount_arg.clone();
        let fees_arg = fees_arg.clone();
        let gas_limit = gas_limit.clone();
        move || {
            run_cli(
                &cli_binary,
                &[
                    "tx",
                    "bank",
                    "send",
                    &from_name,
                    &to_address,
                    &amount_arg,
                    "--generate-only",
                    "--output",
                    "json",
                    "--chain-id",
                    &chain_id,
                    "--keyring-backend",
                    &keyring_backend,
                    "--home",
                    &keyring_home,
                    "--node",
                    &rpc_endpoint,
                    "--gas",
                    &gas_limit,
                    "--fees",
                    &fees_arg,
                ],
                None,
            )
        }
    })
    .await
    .map_err(|err| anyhow!("spawn build unsigned tx failed: {}", err))??;

    // 第二步：对未签名交易进行离线签名
    let signed = tokio::task::spawn_blocking({
        let cli_binary = cli_binary.clone();
        let chain_id = chain_id.clone();
        let keyring_backend = keyring_backend.clone();
        let keyring_home = keyring_home.clone();
        let rpc_endpoint = rpc_endpoint.clone();
        let from_name = from_name_owned.clone();
        let account_number = account_number;
        let sequence = sequence;
        move || {
            run_cli(
                &cli_binary,
                &[
                    "tx",
                    "sign",
                    "-",
                    "--from",
                    &from_name,
                    "--chain-id",
                    &chain_id,
                    "--keyring-backend",
                    &keyring_backend,
                    "--home",
                    &keyring_home,
                    "--node",
                    &rpc_endpoint,
                    "--output",
                    "json",
                    "--offline",
                    "--account-number",
                    &account_number.to_string(),
                    "--sequence",
                    &sequence.to_string(),
                ],
                Some(&unsigned),
            )
        }
    })
    .await
    .map_err(|err| anyhow!("spawn sign tx failed: {}", err))??;

    // 第三步：将签名后的交易编码为 base64 字符串
    let encoded = tokio::task::spawn_blocking({
        let cli_binary = cli_binary.clone();
        move || run_cli(&cli_binary, &["tx", "encode", "-"], Some(&signed))
    })
    .await
    .map_err(|err| anyhow!("spawn encode tx failed: {}", err))??;

    let payload = encoded.trim();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(payload)
        .map_err(|err| anyhow!("decode tx payload failed: {}", err))?;
    Ok(bytes)
}

/// 执行外部 CLI 命令，可选通过 stdin 传递输入，返回 stdout 内容。
fn run_cli(binary: &str, args: &[&str], input: Option<&str>) -> Result<String> {
    let mut command = Command::new(binary);
    command.args(args);
    if input.is_some() {
        command.stdin(Stdio::piped());
    }
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());
    let mut child = command.spawn()?;
    if let Some(input) = input {
        if let Some(mut stdin) = child.stdin.take() {
            use std::io::Write;
            stdin.write_all(input.as_bytes())?;
        }
    }
    let output = child.wait_with_output()?;
    if !output.status.success() {
        return Err(anyhow!(
            "cli failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}
