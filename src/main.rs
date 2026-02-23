mod account_pool;
mod broadcaster;
mod config;
mod metrics;
mod scheduler;
mod signer;
mod tx_builder;

use account_pool::AccountPool;
use anyhow::Result;
use broadcaster::{Broadcaster, GrpcBroadcaster, HttpBroadcaster};
use config::{load_config, Protocol};
use metrics::Metrics;
use scheduler::Scheduler;
use std::sync::Arc;
use tokio::sync::watch;

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    let metrics = Metrics::new(config.output.clone())?;
    metrics.start_background();

    let pool = AccountPool::new(config.account_count, config.initial_nonce);
    let broadcaster: Arc<dyn Broadcaster> = match config.protocol {
        Protocol::Http => Arc::new(HttpBroadcaster::new(
            config.http_endpoint.clone(),
            config.concurrency,
        )?),
        Protocol::Grpc => Arc::new(GrpcBroadcaster::new(config.grpc_endpoint.clone()).await?),
    };

    let scheduler = Scheduler::new(config, pool, broadcaster, metrics);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::select! {
        result = scheduler.run(shutdown_rx) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            let _ = shutdown_tx.send(true);
        }
    }
    Ok(())
}
