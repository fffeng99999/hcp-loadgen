mod account_pool;
mod config;
mod core;
mod metrics;
mod persistence;
mod types;

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

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;
    let effective_backpressure = if config.backpressure_threshold == 0 {
        5_000_000
    } else {
        config.backpressure_threshold as u64
    };
    let effective_channel_capacity = config.storage_channel_size.max(16);
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64;
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
    let metrics = Metrics::new(config.output.clone(), config.metrics_interval_ms)?;
    metrics.start_background();

    let storage = Storage::new(StorageConfig {
        database_url: config.database_url.clone(),
        db_schema: config.db_schema.clone(),
        reset_schema_on_start: config.reset_schema_on_start,
        max_connections: config.storage_max_connections,
    })
    .await?;
    let mut pool: AccountPool = storage
        .load_initial_state(
            config.account_count,
            config.initial_nonce,
            config.initial_balance,
            config.account_file.as_ref().map(PathBuf::from),
        )
        .await?;
    match config.account_selection_mode {
        config::AccountSelectionMode::Zipf => {
            pool.set_zipf_mode(config.zipf_alpha);
            println!("Account selection: Zipf (alpha={})", config.zipf_alpha);
        }
        config::AccountSelectionMode::Random => {
            pool.set_random_mode(true);
            println!("Account selection: Random");
        }
        _ => {
            println!("Account selection: RoundRobin");
        }
    }
    let broadcaster: Arc<dyn Broadcaster> = match config.protocol {
        Protocol::Http => Arc::new(HttpBroadcaster::new(
            config.http_endpoint.clone(),
            config.concurrency,
        )?),
        Protocol::Grpc => Arc::new(
            GrpcBroadcaster::new(config.grpc_endpoint.clone(), config.broadcast_mode.clone()).await?,
        ),
    };
    let backlog_records = Arc::new(AtomicU64::new(0));
    let (persist_tx, mut persist_rx) =
        mpsc::channel::<Vec<TransactionRecord>>(effective_channel_capacity);
    let persist_storage = storage.clone();
    let persist_backlog = backlog_records.clone();
    let persist_task = tokio::spawn(async move {
        while let Some(records) = persist_rx.recv().await {
            let count = records.len() as u64;
            if let Err(err) = persist_storage.flush_trade_batch(records).await {
                eprintln!("spill flush failed: {}", err);
            }
            persist_backlog.fetch_sub(count, Ordering::Relaxed);
        }
    });

    let scheduler = Scheduler::new(
        config,
        pool.clone(),
        broadcaster,
        metrics.clone(),
        persist_tx,
        backlog_records,
    );
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    tokio::select! {
        result = scheduler.run(shutdown_rx) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            let _ = shutdown_tx.send(true);
        }
    }
    if let Ok(line) = serde_json::to_string(&metrics.snapshot()) {
        println!("{}", line);
    }
    drop(scheduler);
    let _ = persist_task.await;
    let final_balances = pool.snapshot_balances();
    let identities = pool.snapshot_accounts();
    storage
        .flush_results_to_db(Vec::new(), final_balances, identities)
        .await?;
    Ok(())
}
