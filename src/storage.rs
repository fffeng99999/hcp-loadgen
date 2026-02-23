use anyhow::Result;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::time::Duration;
use tokio::sync::{mpsc, oneshot};
use tokio::time::interval;

#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub database_url: String,
    pub flush_interval_ms: u64,
    pub batch_size: usize,
    pub channel_size: usize,
    pub drop_on_overflow: bool,
    pub max_connections: u32,
}

#[derive(Clone)]
pub struct Storage {
    sender: mpsc::Sender<StorageEvent>,
    drop_on_overflow: bool,
}

#[derive(Debug, Clone)]
pub struct UserRecord {
    pub username: String,
    pub email: String,
    pub password_hash: String,
    pub role: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct AccountRecord {
    pub username: String,
    pub email: String,
    pub password_hash: String,
    pub role: String,
    pub status: String,
}

#[derive(Debug, Clone)]
pub struct TransactionRecord {
    pub hash: String,
    pub from_address: String,
    pub to_address: String,
    pub amount: i64,
    pub gas_price: Option<i64>,
    pub gas_limit: i64,
    pub nonce: u64,
    pub status: String,
    pub latency_ms: f64,
}

enum StorageEvent {
    User(UserRecord),
    Account(AccountRecord),
    Transaction(TransactionRecord),
    Shutdown(oneshot::Sender<()>),
}

impl Storage {
    pub async fn new(config: StorageConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .connect(&config.database_url)
            .await?;

        let (sender, mut receiver) = mpsc::channel(config.channel_size);
        let drop_on_overflow = config.drop_on_overflow;
        tokio::spawn(async move {
            let mut users = Vec::new();
            let mut accounts = Vec::new();
            let mut transactions = Vec::new();
            let mut ticker = interval(Duration::from_millis(config.flush_interval_ms));
            loop {
                tokio::select! {
                    maybe_event = receiver.recv() => {
                        let event = match maybe_event {
                            Some(event) => event,
                            None => break,
                        };
                        match event {
                            StorageEvent::User(record) => users.push(record),
                            StorageEvent::Account(record) => accounts.push(record),
                            StorageEvent::Transaction(record) => transactions.push(record),
                            StorageEvent::Shutdown(done) => {
                                let _ = flush_all(&pool, &mut users, &mut accounts, &mut transactions).await;
                                let _ = done.send(());
                                break;
                            }
                        }
                        if users.len() + accounts.len() + transactions.len() >= config.batch_size {
                            let _ = flush_all(&pool, &mut users, &mut accounts, &mut transactions).await;
                        }
                    }
                    _ = ticker.tick() => {
                        let _ = flush_all(&pool, &mut users, &mut accounts, &mut transactions).await;
                    }
                }
            }
        });

        Ok(Self {
            sender,
            drop_on_overflow,
        })
    }

    pub async fn enqueue_user(&self, record: UserRecord) {
        let event = StorageEvent::User(record);
        if self.drop_on_overflow {
            let _ = self.sender.try_send(event);
        } else {
            let _ = self.sender.send(event).await;
        }
    }

    pub async fn enqueue_account(&self, record: AccountRecord) {
        let event = StorageEvent::Account(record);
        if self.drop_on_overflow {
            let _ = self.sender.try_send(event);
        } else {
            let _ = self.sender.send(event).await;
        }
    }

    pub async fn enqueue_transaction(&self, record: TransactionRecord) {
        let event = StorageEvent::Transaction(record);
        if self.drop_on_overflow {
            let _ = self.sender.try_send(event);
        } else {
            let _ = self.sender.send(event).await;
        }
    }

    pub async fn shutdown(&self) {
        let (done_tx, done_rx) = oneshot::channel();
        let _ = self.sender.send(StorageEvent::Shutdown(done_tx)).await;
        let _ = done_rx.await;
    }
}

async fn flush_all(
    pool: &PgPool,
    users: &mut Vec<UserRecord>,
    accounts: &mut Vec<AccountRecord>,
    transactions: &mut Vec<TransactionRecord>,
) -> Result<()> {
    if users.is_empty() && accounts.is_empty() && transactions.is_empty() {
        return Ok(());
    }
    let mut tx = pool.begin().await?;
    if !users.is_empty() {
        for record in users.drain(..) {
            sqlx::query(
                r#"
                INSERT INTO users (username, password_hash, email, role, status)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (username) DO NOTHING;
                "#,
            )
            .bind(record.username)
            .bind(record.password_hash)
            .bind(record.email)
            .bind(record.role)
            .bind(record.status)
            .execute(&mut *tx)
            .await?;
        }
    }
    if !accounts.is_empty() {
        for record in accounts.drain(..) {
            sqlx::query(
                r#"
                INSERT INTO users (username, password_hash, email, role, status)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (username) DO NOTHING;
                "#,
            )
            .bind(record.username)
            .bind(record.password_hash)
            .bind(record.email)
            .bind(record.role)
            .bind(record.status)
            .execute(&mut *tx)
            .await?;
        }
    }
    if !transactions.is_empty() {
        for record in transactions.drain(..) {
            sqlx::query(
                r#"
                INSERT INTO transactions
                    (hash, from_address, to_address, amount, gas_price, gas_limit, nonce, status, submitted_at, confirmed_at, latency_ms)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW(), CASE WHEN $8 = 'confirmed' THEN NOW() ELSE NULL END, $9)
                ON CONFLICT DO NOTHING;
                "#,
            )
            .bind(record.hash)
            .bind(record.from_address)
            .bind(record.to_address)
            .bind(record.amount)
            .bind(record.gas_price)
            .bind(record.gas_limit)
            .bind(record.nonce as i64)
            .bind(record.status)
            .bind(record.latency_ms)
            .execute(&mut *tx)
            .await?;
        }
    }
    tx.commit().await?;
    Ok(())
}
