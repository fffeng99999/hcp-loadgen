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
    pub db_schema: String,
    pub reset_schema_on_start: bool,
    pub max_connections: u32,
}

#[derive(Clone)]
pub struct Storage {
    database_url: String,
    db_schema: String,
    pool: PgPool,
}

impl Storage {
    pub async fn new(config: StorageConfig) -> Result<Self> {
        let database_url = config.database_url.clone();
        let db_schema = config.db_schema.clone();
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await?;
        let storage = Self {
            database_url,
            db_schema,
            pool,
        };
        storage.prepare_schema(config.reset_schema_on_start).await?;
        Ok(storage)
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
            let upsert_accounts_sql = format!(
                r#"
                INSERT INTO {}.accounts (account_id, address, username)
                SELECT v.account_id, v.address, NULL::VARCHAR
                FROM UNNEST($1::BIGINT[], $2::TEXT[]) AS v(account_id, address)
                ON CONFLICT (account_id) DO UPDATE SET address = EXCLUDED.address;
                "#,
                self.db_schema
            );
            sqlx::query(&upsert_accounts_sql)
            .bind(ids)
            .bind(addresses)
            .execute(&mut *tx)
            .await?;
        }
        if !final_balances.is_empty() {
            let account_ids: Vec<i64> = final_balances.iter().map(|v| v.account_id as i64).collect();
            let available: Vec<f64> = final_balances.iter().map(|v| v.available_balance).collect();
            let frozen: Vec<f64> = final_balances.iter().map(|v| v.frozen_balance).collect();
            let upsert_balances_sql = format!(
                r#"
                INSERT INTO {}.balances (account_id, asset_symbol, available, frozen, updated_at)
                SELECT v.account_id, 'HCP', v.available::NUMERIC, v.frozen::NUMERIC, NOW()
                FROM UNNEST($1::BIGINT[], $2::DOUBLE PRECISION[], $3::DOUBLE PRECISION[]) AS v(account_id, available, frozen)
                ON CONFLICT (account_id, asset_symbol)
                DO UPDATE SET available = EXCLUDED.available, frozen = EXCLUDED.frozen, updated_at = NOW();
                "#,
                self.db_schema
            );
            sqlx::query(&upsert_balances_sql)
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
        let load_accounts_sql = format!(
            r#"
            SELECT
                a.account_id,
                a.address,
                a.username,
                COALESCE(SUM(b.available), 0)::DOUBLE PRECISION AS available_balance,
                COALESCE(SUM(b.frozen), 0)::DOUBLE PRECISION AS frozen_balance
            FROM {}.accounts a
            LEFT JOIN {}.balances b ON b.account_id = a.account_id
            WHERE a.username IS NOT NULL
            GROUP BY a.account_id, a.address, a.username
            ORDER BY a.account_id;
            "#,
            self.db_schema, self.db_schema
        );
        let rows = sqlx::query(&load_accounts_sql)
        .fetch_all(&self.pool)
        .await?;
        let mut accounts = Vec::with_capacity(rows.len());
        for row in rows {
            let account_id = row.try_get::<i64, _>("account_id")? as u64;
            let address = row.try_get::<String, _>("address")?;
            let signer_name = row.try_get::<Option<String>, _>("username")?;
            let available_balance = row.try_get::<f64, _>("available_balance")?;
            let frozen_balance = row.try_get::<f64, _>("frozen_balance")?;
            let priv_key = derive_private_key_from_text(&address);
            accounts.push(InMemoryAccount::new(
                account_id,
                address,
                signer_name,
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
                match serde_json::from_str::<AccountFileRecord>(line) {
                    Ok(record) => {
                        let account_id = (index + 1) as u64;
                        let priv_key = derive_private_key_from_text(&record.address);
                        accounts.push(InMemoryAccount::new(
                            account_id,
                            record.address,
                            record.name,
                            priv_key,
                            initial_nonce,
                            initial_balance as f64,
                            0.0,
                        ));
                    }
                    Err(err) => {
                        eprintln!("failed to parse account line: {} err={}", line, err);
                    }
                }
                if accounts.len() >= account_count {
                    return Ok(accounts);
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
                None,
                private_key,
                initial_nonce,
                initial_balance as f64,
                0.0,
            ));
        }
        Ok(accounts)
    }

    async fn prepare_schema(&self, reset_schema_on_start: bool) -> Result<()> {
        let create_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {};", self.db_schema);
        sqlx::query(&create_schema_sql).execute(&self.pool).await?;
        if reset_schema_on_start {
            let drop_sql = format!("DROP SCHEMA IF EXISTS {} CASCADE;", self.db_schema);
            sqlx::query(&drop_sql).execute(&self.pool).await?;
            sqlx::query(&create_schema_sql).execute(&self.pool).await?;
        }
        let create_accounts_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.accounts (
                account_id BIGSERIAL PRIMARY KEY,
                address VARCHAR(64) NOT NULL UNIQUE,
                username VARCHAR(50),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            "#,
            self.db_schema
        );
        let create_balances_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.balances (
                account_id BIGINT REFERENCES {}.accounts(account_id),
                asset_symbol VARCHAR(10) NOT NULL,
                available DECIMAL(20, 8) DEFAULT 0,
                frozen DECIMAL(20, 8) DEFAULT 0,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (account_id, asset_symbol)
            );
            "#,
            self.db_schema, self.db_schema
        );
        let create_orders_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.orders (
                order_id BIGSERIAL PRIMARY KEY,
                account_id BIGINT REFERENCES {}.accounts(account_id),
                side VARCHAR(10) CHECK (side IN ('BUY', 'SELL')),
                price DECIMAL(20, 8) NOT NULL,
                quantity DECIMAL(20, 8) NOT NULL,
                filled_qty DECIMAL(20, 8) DEFAULT 0,
                status VARCHAR(20) DEFAULT 'PENDING',
                timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            "#,
            self.db_schema, self.db_schema
        );
        let create_trades_sql = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {}.trades (
                trade_id BIGSERIAL PRIMARY KEY,
                buy_order_id BIGINT,
                sell_order_id BIGINT,
                price DECIMAL(20, 8) NOT NULL,
                quantity DECIMAL(20, 8) NOT NULL,
                tx_hash VARCHAR(128),
                latency_ms INTEGER,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
            "#,
            self.db_schema
        );
        let create_trade_index_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_trades_tx_hash ON {}.trades(tx_hash);",
            self.db_schema, self.db_schema
        );
        let create_balance_index_sql = format!(
            "CREATE INDEX IF NOT EXISTS idx_{}_balances_account_id ON {}.balances(account_id);",
            self.db_schema, self.db_schema
        );
        sqlx::query(&create_accounts_sql).execute(&self.pool).await?;
        sqlx::query(&create_balances_sql).execute(&self.pool).await?;
        sqlx::query(&create_orders_sql).execute(&self.pool).await?;
        sqlx::query(&create_trades_sql).execute(&self.pool).await?;
        sqlx::query(&create_trade_index_sql).execute(&self.pool).await?;
        sqlx::query(&create_balance_index_sql).execute(&self.pool).await?;
        Ok(())
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
            let insert_trades_sql = format!(
                r#"
                INSERT INTO {}.trades (buy_order_id, sell_order_id, price, quantity, tx_hash, latency_ms, created_at)
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
                self.db_schema
            );
            tx.execute(&insert_trades_sql, &[])
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
    name: Option<String>,
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
