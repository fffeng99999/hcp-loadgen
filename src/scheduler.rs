use crate::account_pool::AccountPool;
use crate::broadcaster::Broadcaster;
use crate::config::{Config, SendMode};
use crate::metrics::Metrics;
use crate::signer::Signer;
use crate::tx_builder::{TxBuilder, TxKind};
use anyhow::Result;
use rand::Rng;
use std::sync::Arc;
use std::time::Duration;
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
}

impl Scheduler {
    pub fn new(
        config: Config,
        pool: AccountPool,
        broadcaster: Arc<dyn Broadcaster>,
        metrics: Metrics,
    ) -> Self {
        let kinds = config
            .tx_types
            .clone()
            .into_iter()
            .map(TxKind::from)
            .collect::<Vec<_>>();
        let builder = TxBuilder::new(config.payload_size, kinds, pool.addresses());
        let semaphore = Arc::new(Semaphore::new(config.concurrency));
        Self {
            config,
            pool,
            builder,
            signer: Arc::new(Signer),
            broadcaster,
            metrics,
            semaphore,
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
        let interval_ns = if self.config.target_tps == 0 {
            1_000_000_000
        } else {
            1_000_000_000 / self.config.target_tps
        };
        let mut ticker = interval(Duration::from_nanos(interval_ns));
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.spawn_send().await;
                }
                _ = shutdown.changed() => break,
            }
        }
        Ok(())
    }

    async fn run_burst(&self, shutdown: &mut watch::Receiver<bool>) -> Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = self.send_burst() => {},
            }
            sleep(Duration::from_millis(self.config.burst_interval_ms)).await;
        }
        Ok(())
    }

    async fn run_sustained(&self, shutdown: &mut watch::Receiver<bool>) -> Result<()> {
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = self.spawn_send() => {},
            }
            tokio::task::yield_now().await;
        }
        Ok(())
    }

    async fn run_jitter(&self, shutdown: &mut watch::Receiver<bool>) -> Result<()> {
        let base_interval_ns = if self.config.target_tps == 0 {
            1_000_000_000
        } else {
            1_000_000_000 / self.config.target_tps
        };
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = self.spawn_send() => {},
            }
            let jitter = rand::thread_rng().gen_range(0.0..self.config.jitter_percent.max(0.0));
            let jitter_ns = (base_interval_ns as f64 * jitter / 100.0) as u64;
            sleep(Duration::from_nanos(base_interval_ns + jitter_ns)).await;
        }
        Ok(())
    }

    async fn send_burst(&self) {
        for _ in 0..self.config.burst_size {
            self.spawn_send().await;
        }
    }

    async fn spawn_send(&self) {
        let permit = match self.semaphore.clone().acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => return,
        };
        let pool = self.pool.clone();
        let builder = self.builder.clone();
        let signer = self.signer.clone();
        let broadcaster = self.broadcaster.clone();
        let metrics = self.metrics.clone();
        tokio::spawn(async move {
            let account = pool.next_account();
            let nonce = account.next_nonce();
            let mut tx = builder.build_tx(&account, nonce);
            let preimage = format!("{}:{}:{}", tx.from, tx.nonce, tx.payload_hex);
            let signature = signer.sign(&account.private_key, preimage.as_bytes());
            tx.signature_hex = hex::encode(signature);
            let payload = builder.encode_tx(&tx);
            metrics.record_sent();
            match broadcaster.send(payload).await {
                Ok(result) => {
                    if result.success {
                        metrics.record_success(result.latency_ms);
                    } else {
                        metrics.record_reject(result.latency_ms);
                    }
                }
                Err(_) => {
                    metrics.record_reject(0.0);
                }
            }
            drop(permit);
        });
    }
}
