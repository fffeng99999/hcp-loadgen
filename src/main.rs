mod account_pool;
mod broadcaster;
mod config;
mod metrics;
mod scheduler;
mod signer;
mod storage;
mod tx_builder;

use account_pool::AccountPool;
use anyhow::Result;
use broadcaster::{Broadcaster, GrpcBroadcaster, HttpBroadcaster};
use config::{load_config, Protocol};
use metrics::Metrics;
use scheduler::Scheduler;
use storage::{Storage, StorageConfig};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    let metrics = Metrics::new(config.output.clone(), config.metrics_interval_ms)?;
    metrics.start_background();

    let storage = Storage::new(StorageConfig {
        database_url: config.database_url.clone(),
        flush_interval_ms: config.storage_flush_interval_ms,
        batch_size: config.batch_size,
        channel_size: config.storage_channel_size,
        drop_on_overflow: config.drop_on_overflow,
        max_connections: config.storage_max_connections,
    })
    .await?;
    let storage = Some(storage);
    let pool = AccountPool::new(
        config.account_count,
        config.initial_nonce,
        config.initial_balance,
        storage.clone(),
        100,
        config
            .account_file
            .as_ref()
            .map(|path| PathBuf::from(path)),
    )
    .await;
    let broadcaster: Arc<dyn Broadcaster> = match config.protocol {
        Protocol::Http => Arc::new(HttpBroadcaster::new(
            config.http_endpoint.clone(),
            config.concurrency,
        )?),
        Protocol::Grpc => Arc::new(GrpcBroadcaster::new(config.grpc_endpoint.clone()).await?),
    };

    let scheduler = Scheduler::new(config, pool, broadcaster, metrics, storage.clone());
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::select! {
        result = scheduler.run(shutdown_rx) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            let _ = shutdown_tx.send(true);
        }
    }
    if let Some(storage) = storage {
        storage.shutdown().await;
    }
    Ok(())
}
