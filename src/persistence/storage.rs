use crate::account_pool::AccountPool;
use crate::types::{AccountIdentity, BalanceSnapshot, InMemoryAccount, TransactionRecord};
use anyhow::Result;
use futures::pin_mut;
use rand::RngCore;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use std::fs;
use std::path::PathBuf;
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::types::Type;
use tokio_postgres::NoTls;

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub database_url: String,
    pub max_connections: u32,
}

#[derive(Clone)]
pub struct Storage {
    database_url: String,
    pool: PgPool,
}

impl Storage {
    pub async fn new(config: StorageConfig) -> Result<Self> {
        let database_url = config.database_url.clone();
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await?;
        Ok(Self { database_url, pool })
    }

    pub async fn load_initial_state(
        &self,
        account_count: usize,
        initial_nonce: u64,
        initial_balance: u64,
        account_file: Option<PathBuf>,
    ) -> Result<AccountPool> {
        let mut accounts = self.load_accounts_from_db(initial_nonce).await?;
        if accounts.is_empty() {
            accounts = self.load_or_generate_accounts(account_count, initial_nonce, initial_balance, account_file)?;
        }
        Ok(AccountPool::from_accounts(accounts))
    }

    pub async fn flush_results_to_db(
        &self,
        records: Vec<TransactionRecord>,
        final_balances: Vec<BalanceSnapshot>,
        identities: Vec<AccountIdentity>,
    ) -> Result<()> {
        if !records.is_empty() {
            self.copy_trades(&records).await?;
        }
        let mut tx = self.pool.begin().await?;
        if !identities.is_empty() {
            let ids: Vec<i64> = identities.iter().map(|v| v.account_id as i64).collect();
            let addresses: Vec<String> = identities.iter().map(|v| v.address.clone()).collect();
            sqlx::query(
                r#"
                INSERT INTO loadgendata.accounts (account_id, address, username)
                SELECT v.account_id, v.address, NULL::VARCHAR
                FROM UNNEST($1::BIGINT[], $2::TEXT[]) AS v(account_id, address)
                ON CONFLICT (account_id) DO UPDATE SET address = EXCLUDED.address;
                "#,
            )
            .bind(ids)
            .bind(addresses)
            .execute(&mut *tx)
            .await?;
        }
        if !final_balances.is_empty() {
            let account_ids: Vec<i64> = final_balances.iter().map(|v| v.account_id as i64).collect();
            let available: Vec<f64> = final_balances.iter().map(|v| v.available_balance).collect();
            let frozen: Vec<f64> = final_balances.iter().map(|v| v.frozen_balance).collect();
            sqlx::query(
                r#"
                INSERT INTO loadgendata.balances (account_id, asset_symbol, available, frozen, updated_at)
                SELECT v.account_id, 'HCP', v.available::NUMERIC, v.frozen::NUMERIC, NOW()
                FROM UNNEST($1::BIGINT[], $2::DOUBLE PRECISION[], $3::DOUBLE PRECISION[]) AS v(account_id, available, frozen)
                ON CONFLICT (account_id, asset_symbol)
                DO UPDATE SET available = EXCLUDED.available, frozen = EXCLUDED.frozen, updated_at = NOW();
                "#,
            )
            .bind(account_ids)
            .bind(available)
            .bind(frozen)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn flush_trade_batch(&self, records: Vec<TransactionRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        self.copy_trades(&records).await?;
        Ok(())
    }

    async fn load_accounts_from_db(&self, initial_nonce: u64) -> Result<Vec<InMemoryAccount>> {
        let rows = sqlx::query(
            r#"
            SELECT
                a.account_id,
                a.address,
                COALESCE(SUM(b.available), 0)::DOUBLE PRECISION AS available_balance,
                COALESCE(SUM(b.frozen), 0)::DOUBLE PRECISION AS frozen_balance
            FROM loadgendata.accounts a
            LEFT JOIN loadgendata.balances b ON b.account_id = a.account_id
            GROUP BY a.account_id, a.address
            ORDER BY a.account_id;
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        let mut accounts = Vec::with_capacity(rows.len());
        for row in rows {
            let account_id = row.try_get::<i64, _>("account_id")? as u64;
            let address = row.try_get::<String, _>("address")?;
            let available_balance = row.try_get::<f64, _>("available_balance")?;
            let frozen_balance = row.try_get::<f64, _>("frozen_balance")?;
            let priv_key = derive_private_key_from_text(&address);
            accounts.push(InMemoryAccount::new(
                account_id,
                address,
                priv_key,
                initial_nonce,
                available_balance,
                frozen_balance,
            ));
        }
        Ok(accounts)
    }

    fn load_or_generate_accounts(
        &self,
        account_count: usize,
        initial_nonce: u64,
        initial_balance: u64,
        account_file: Option<PathBuf>,
    ) -> Result<Vec<InMemoryAccount>> {
        let mut accounts = Vec::with_capacity(account_count);
        if let Some(path) = account_file {
            let contents = fs::read_to_string(path)?;
            for (index, line) in contents.lines().enumerate() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if let Ok(record) = serde_json::from_str::<AccountFileRecord>(line) {
                    let account_id = (index + 1) as u64;
                    let priv_key = derive_private_key_from_text(&record.address);
                    accounts.push(InMemoryAccount::new(
                        account_id,
                        record.address,
                        priv_key,
                        initial_nonce,
                        initial_balance as f64,
                        0.0,
                    ));
                    if accounts.len() >= account_count {
                        return Ok(accounts);
                    }
                }
            }
        }
        while accounts.len() < account_count {
            let mut private_key = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut private_key);
            let address = derive_address(&private_key);
            let account_id = (accounts.len() + 1) as u64;
            accounts.push(InMemoryAccount::new(
                account_id,
                address,
                private_key,
                initial_nonce,
                initial_balance as f64,
                0.0,
            ));
        }
        Ok(accounts)
    }

    async fn copy_trades(&self, records: &[TransactionRecord]) -> Result<()> {
        let (mut client, connection) = tokio_postgres::connect(&self.database_url, NoTls).await?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let tx = client.transaction().await?;
        let write_result: Result<()> = async {
            tx.batch_execute(
                r#"
                CREATE TEMP TABLE IF NOT EXISTS loadgen_trades_copy_buffer (
                    buy_order_id BIGINT,
                    sell_order_id BIGINT,
                    price DOUBLE PRECISION,
                    quantity DOUBLE PRECISION,
                    tx_hash TEXT,
                    latency_ms INTEGER,
                    ts_ms BIGINT
                ) ON COMMIT DROP;
                TRUNCATE loadgen_trades_copy_buffer;
                "#,
            )
            .await?;
            let sink = tx
                .copy_in(
                    "COPY loadgen_trades_copy_buffer (buy_order_id, sell_order_id, price, quantity, tx_hash, latency_ms, ts_ms) FROM STDIN BINARY",
                )
                .await?;
            let writer = BinaryCopyInWriter::new(
                sink,
                &[
                    Type::INT8,
                    Type::INT8,
                    Type::FLOAT8,
                    Type::FLOAT8,
                    Type::TEXT,
                    Type::INT4,
                    Type::INT8,
                ],
            );
            pin_mut!(writer);
            for record in records {
                let buy_order_id = if record.side == "BUY" {
                    Some(record.account_id as i64)
                } else {
                    None
                };
                let sell_order_id = if record.side == "SELL" {
                    Some(record.account_id as i64)
                } else {
                    None
                };
                let latency_ms = if record.latency_ms.is_finite() {
                    record.latency_ms.max(0.0).round() as i32
                } else {
                    0
                };
                writer
                    .as_mut()
                    .write(&[
                        &buy_order_id,
                        &sell_order_id,
                        &record.price,
                        &record.quantity,
                        &record.tx_hash,
                        &latency_ms,
                        &record.timestamp,
                    ])
                    .await?;
            }
            let _ = writer.as_mut().finish().await?;
            tx.execute(
                r#"
                INSERT INTO loadgendata.trades (buy_order_id, sell_order_id, price, quantity, tx_hash, latency_ms, created_at)
                SELECT
                    buy_order_id,
                    sell_order_id,
                    price::NUMERIC,
                    quantity::NUMERIC,
                    tx_hash,
                    latency_ms,
                    to_timestamp(ts_ms::DOUBLE PRECISION / 1000.0)
                FROM loadgen_trades_copy_buffer;
                "#,
                &[],
            )
            .await?;
            Ok(())
        }
        .await;
        match write_result {
            Ok(()) => {
                tx.commit().await?;
                Ok(())
            }
            Err(err) => {
                let _ = tx.rollback().await;
                Err(err)
            }
        }
    }
}

#[derive(Debug, Deserialize)]
struct AccountFileRecord {
    address: String,
}

fn derive_address(private_key: &[u8; 32]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(private_key);
    let hash = hasher.finalize();
    hex::encode(&hash[..20])
}

fn derive_private_key_from_text(text: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    let digest = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest[..32]);
    key
}
