use crate::account_pool::AccountPool;
use crate::broadcaster::Broadcaster;
use crate::config::{Config, SendMode};
use crate::metrics::Metrics;
use crate::signer::Signer;
use crate::storage::{Storage, TransactionRecord};
use crate::tx_builder::{TxBuilder, TxKind};
use anyhow::Result;
use rand::Rng;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::{watch, Semaphore};
use tokio::time::{interval, sleep};

#[derive(Clone)]
pub struct Scheduler {
    config: Config,
    pool: AccountPool,
    builder: TxBuilder,
    signer: Arc<Signer>,
    broadcaster: Arc<dyn Broadcaster>,
    metrics: Metrics,
    semaphore: Arc<Semaphore>,
    storage: Option<Storage>,
}

impl Scheduler {
    pub fn new(
        config: Config,
        pool: AccountPool,
        broadcaster: Arc<dyn Broadcaster>,
        metrics: Metrics,
        storage: Option<Storage>,
    ) -> Self {
        let kinds = vec![TxKind::from(config.tx_type.clone())];
        let builder = TxBuilder::new(
            config.payload_size,
            kinds,
            pool.addresses(),
            config.tx_encoding.clone(),
            config.compression.clone(),
        );
        let semaphore = Arc::new(Semaphore::new(config.concurrency));
        Self {
            config,
            pool,
            builder,
            signer: Arc::new(Signer),
            broadcaster,
            metrics,
            semaphore,
            storage,
        }
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        match self.config.mode {
            SendMode::Fixed => self.run_fixed(&mut shutdown).await,
            SendMode::Burst => self.run_burst(&mut shutdown).await,
            SendMode::Sustained => self.run_sustained(&mut shutdown).await,
            SendMode::Jitter => self.run_jitter(&mut shutdown).await,
        }
    }

    async fn run_fixed(&self, shutdown: &mut watch::Receiver<bool>) -> Result<()> {
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
                    if self.spawn_send().await {
                        sent += 1;
                    }
                }
                _ = shutdown.changed() => break,
            }
        }
        Ok(())
    }

    async fn run_burst(&self, shutdown: &mut watch::Receiver<bool>) -> Result<()> {
        let start = Instant::now();
        let mut sent = 0u64;
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = self.send_burst(start, &mut sent) => {},
            }
            if self.should_stop(start, sent) {
                break;
            }
            sleep(Duration::from_millis(self.config.burst_interval_ms)).await;
        }
        Ok(())
    }

    async fn run_sustained(&self, shutdown: &mut watch::Receiver<bool>) -> Result<()> {
        let start = Instant::now();
        let mut sent = 0u64;
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = self.spawn_send() => {
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

    async fn run_jitter(&self, shutdown: &mut watch::Receiver<bool>) -> Result<()> {
        let base_interval_ns = self.resolve_interval_ns();
        let start = Instant::now();
        let mut sent = 0u64;
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = self.spawn_send() => {
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

    async fn send_burst(&self, start: Instant, sent: &mut u64) {
        for _ in 0..self.config.burst_size {
            if self.should_stop(start, *sent) {
                break;
            }
            if self.spawn_send().await {
                *sent = sent.saturating_add(1);
            }
        }
    }

    async fn spawn_send(&self) -> bool {
        let permit = match self.semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return false,
        };
        let pool = self.pool.clone();
        let builder = self.builder.clone();
        let signer = self.signer.clone();
        let broadcaster = self.broadcaster.clone();
        let metrics = self.metrics.clone();
        let storage = self.storage.clone();
        let gas_limit = self.config.gas_limit as i64;
        let amount = self.config.fee_amount as i64;
        tokio::spawn(async move {
            let account = pool.next_account();
            let nonce = account.next_nonce();
            let mut tx = builder.build_tx(&account, nonce);
            let preimage = format!("{}:{}:{}", tx.from, tx.nonce, tx.payload_hex);
            let signature = signer.sign(&account.private_key, preimage.as_bytes());
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
                Err(_) => {
                    metrics.record_reject(0.0);
                    (false, 0.0)
                }
            };
            if let Some(storage) = storage {
                let record = TransactionRecord {
                    hash: format!("{}:{}:{}:{}", tx.from, tx.nonce, tx.to, now_ms()),
                    from_address: tx.from.clone(),
                    to_address: tx.to.clone(),
                    amount,
                    gas_price: None,
                    gas_limit,
                    nonce: tx.nonce,
                    status: if success { "confirmed".to_string() } else { "failed".to_string() },
                    latency_ms,
                };
                storage.enqueue_transaction(record).await;
            }
            drop(permit);
        });
        true
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

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
