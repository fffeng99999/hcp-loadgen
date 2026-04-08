use crate::account_pool::AccountPool;
use crate::config::{Config, SendMode};
use crate::core::broadcaster::Broadcaster;
use crate::core::signer::Signer;
use crate::core::tx_builder::{TxBuilder, TxKind};
use crate::metrics::Metrics;
use crate::types::TransactionRecord;
use anyhow::Result;
use rand::Rng;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep};

enum WorkerMessage {
    Dispatch,
    Shutdown,
}

struct WorkerRuntime {
    senders: Vec<mpsc::Sender<WorkerMessage>>,
    handles: Vec<JoinHandle<()>>,
}

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

    async fn stop_workers(&self, workers: WorkerRuntime) {
        for sender in &workers.senders {
            let _ = sender.send(WorkerMessage::Shutdown).await;
        }
        for handle in workers.handles {
            let _ = handle.await;
        }
    }

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

    async fn dispatch_send(&self, senders: &[mpsc::Sender<WorkerMessage>]) -> bool {
        if senders.is_empty() {
            return false;
        }
        while self.backlog_records.load(Ordering::Relaxed) > self.backpressure_threshold {
            sleep(Duration::from_millis(2)).await;
        }
        let idx = self.dispatch_index.fetch_add(1, Ordering::Relaxed) % senders.len();
        senders[idx].send(WorkerMessage::Dispatch).await.is_ok()
    }

    fn resolve_interval_ns(&self) -> u64 {
        if self.config.send_interval_ns > 0 {
            self.config.send_interval_ns
        } else if self.config.target_tps == 0 {
            1_000_000_000
        } else {
            1_000_000_000 / self.config.target_tps
        }
    }

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

async fn process_send(
    pool: &AccountPool,
    builder: &TxBuilder,
    signer: &Signer,
    broadcaster: &Arc<dyn Broadcaster>,
    metrics: &Metrics,
    amount: f64,
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
    let preimage = format!("{}:{}:{}", tx.from, tx.nonce, tx.payload_hex);
    let signature = signer.sign(&account.priv_key, preimage.as_bytes());
    tx.signature_hex = hex::encode(signature);
    let payload = builder.encode_tx(&tx);
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

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
