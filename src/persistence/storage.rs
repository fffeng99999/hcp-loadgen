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

/// 存储配置，定义数据库连接参数和 schema 行为。
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// PostgreSQL 数据库连接字符串
    pub database_url: String,
    /// 数据库 schema 名称
    pub db_schema: String,
    /// 启动时是否重置 schema（清空已有数据）
    pub reset_schema_on_start: bool,
    /// 数据库连接池最大连接数
    pub max_connections: u32,
}

/// 存储层，封装数据库连接池和 schema 操作，提供账户加载与交易持久化功能。
#[derive(Clone)]
pub struct Storage {
    /// 数据库连接字符串（用于 COPY 等独立连接场景）
    database_url: String,
    /// 使用的 schema 名称
    db_schema: String,
    /// SQLx 连接池
    pool: PgPool,
}

impl Storage {
    /// 创建新的存储层实例，初始化连接池并准备数据库 schema。
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

    /// 加载初始账户状态：优先从数据库加载，若为空则根据配置生成或从文件导入。
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

    /// 批量刷新交易记录、账户身份和最终余额到数据库。
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

    /// 将一批交易记录通过 COPY BINARY 高效写入数据库。
    pub async fn flush_trade_batch(&self, records: Vec<TransactionRecord>) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }
        self.copy_trades(&records).await?;
        Ok(())
    }

    /// 从数据库加载已有账户（要求 username 不为 NULL）。
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

    /// 从文件加载账户，若不足则随机生成新账户，直到达到指定数量。
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

    /// 准备数据库 schema：创建 schema、表和索引；若配置要求则先重置。
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

    /// 使用 PostgreSQL COPY BINARY 协议将交易记录批量写入临时表，再插入到目标 trades 表。
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

/// 账户文件中的单条记录格式（JSON Lines）。
#[derive(Debug, Deserialize)]
struct AccountFileRecord {
    name: Option<String>,
    address: String,
}

/// 从私钥派生地址（取 SHA-256 哈希的前 20 字节并编码为 hex）。
fn derive_address(private_key: &[u8; 32]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(private_key);
    let hash = hasher.finalize();
    hex::encode(&hash[..20])
}

/// 从文本派生私钥（对文本做 SHA-256 后取完整 32 字节）。
fn derive_private_key_from_text(text: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    let digest = hasher.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&digest[..32]);
    key
}
