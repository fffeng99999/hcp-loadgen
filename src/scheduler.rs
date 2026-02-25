use crate::account_pool::AccountPool;
use crate::broadcaster::Broadcaster;
use crate::config::{Config, SendMode};
use crate::metrics::Metrics;
use crate::signer::Signer;
use crate::storage::{Storage, TransactionRecord};
use crate::tx_builder::{TxBuilder, TxKind};
use anyhow::Result;
use rand::Rng;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use std::process::{Command, Stdio};
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
        self.wait_for_inflight().await;
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
        self.wait_for_inflight().await;
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
        self.wait_for_inflight().await;
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
        self.wait_for_inflight().await;
        Ok(())
    }

    async fn wait_for_inflight(&self) {
        let permits = self.config.concurrency as u32;
        if permits == 0 {
            return;
        }
        if let Ok(permits) = self.semaphore.acquire_many(permits).await {
            drop(permits);
        }
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
        let amount = self.config.send_amount as i64;
        let use_cli = self.config.keyring_home.is_some() && self.config.account_file.is_some();
        let config = self.config.clone();
        tokio::spawn(async move {
            let account = pool.next_account();
            let nonce = account.next_nonce();
            let mut tx = builder.build_tx(&account, nonce);
            let payload = if use_cli {
                let from_name = match account.name.as_ref() {
                    Some(name) => name.clone(),
                    None => {
                        metrics.record_reject(0.0);
                        drop(permit);
                        return;
                    }
                };
                let keyring_home = match config.keyring_home.as_ref() {
                    Some(home) => home.clone(),
                    None => {
                        metrics.record_reject(0.0);
                        drop(permit);
                        return;
                    }
                };
                let result = build_cli_tx_bytes(
                    &config,
                    &from_name,
                    &tx.to,
                    amount,
                    &keyring_home,
                )
                .await;
                match result {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        eprintln!("build cli tx failed: {}", err);
                        metrics.record_reject(0.0);
                        drop(permit);
                        return;
                    }
                }
            } else {
                let private_key = match account.private_key {
                    Some(private_key) => private_key,
                    None => {
                        metrics.record_reject(0.0);
                        drop(permit);
                        return;
                    }
                };
                let preimage = format!("{}:{}:{}", tx.from, tx.nonce, tx.payload_hex);
                let signature = signer.sign(&private_key, preimage.as_bytes());
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

async fn build_cli_tx_bytes(
    config: &Config,
    from_name: &str,
    to_address: &str,
    amount: i64,
    keyring_home: &str,
) -> Result<Vec<u8>> {
    let amount_arg = format!("{}{}", amount.max(1), config.denom);
    let fees_arg = format!("{}{}", config.fee_amount.max(1), config.denom);
    let unsigned = tokio::task::spawn_blocking({
        let cli_binary = config.cli_binary.clone();
        let chain_id = config.chain_id.clone();
        let keyring_backend = config.keyring_backend.clone();
        let rpc_endpoint = config.rpc_endpoint.clone();
        let gas_limit = config.gas_limit.to_string();
        let from_name = from_name.to_string();
        let to_address = to_address.to_string();
        let keyring_home = keyring_home.to_string();
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
    .await??;
    let signed = tokio::task::spawn_blocking({
        let cli_binary = config.cli_binary.clone();
        let chain_id = config.chain_id.clone();
        let keyring_backend = config.keyring_backend.clone();
        let rpc_endpoint = config.rpc_endpoint.clone();
        let from_name = from_name.to_string();
        let keyring_home = keyring_home.to_string();
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
                ],
                Some(&unsigned),
            )
        }
    })
    .await??;
    let encoded = tokio::task::spawn_blocking({
        let cli_binary = config.cli_binary.clone();
        move || run_cli(&cli_binary, &["tx", "encode", "-"], Some(&signed))
    })
    .await??;
    let payload = encoded.trim();
    Ok(STANDARD.decode(payload)?)
}

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
        return Err(anyhow::anyhow!(
            "cli failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
