use crate::storage::{AccountRecord, Storage, UserRecord};
use rand::RngCore;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct Account {
    pub name: Option<String>,
    pub address: String,
    pub private_key: Option<[u8; 32]>,
    nonce: AtomicU64,
}

impl Account {
    pub fn next_nonce(&self) -> u64 {
        self.nonce.fetch_add(1, Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct AccountPool {
    accounts: Arc<Vec<Arc<Account>>>,
    index: Arc<AtomicUsize>,
}

impl AccountPool {
    pub async fn new(
        count: usize,
        initial_nonce: u64,
        _initial_balance: u64,
        storage: Option<Storage>,
        user_account_pairs: usize,
        account_file: Option<PathBuf>,
    ) -> Self {
        let mut accounts = Vec::with_capacity(count);
        if let Some(path) = account_file {
            if let Ok(contents) = fs::read_to_string(path) {
                for line in contents.lines() {
                    let line = line.trim();
                    if line.is_empty() {
                        continue;
                    }
                    if let Ok(record) = serde_json::from_str::<AccountFileRecord>(line) {
                        let account = Arc::new(Account {
                            name: Some(record.name),
                            address: record.address,
                            private_key: None,
                            nonce: AtomicU64::new(initial_nonce),
                        });
                        accounts.push(account);
                    }
                }
            }
        }
        if accounts.is_empty() {
            for _ in 0..count {
                let mut private_key = [0u8; 32];
                rand::thread_rng().fill_bytes(&mut private_key);
                let address = derive_address(&private_key);
                let account = Arc::new(Account {
                    name: None,
                    address,
                    private_key: Some(private_key),
                    nonce: AtomicU64::new(initial_nonce),
                });
                accounts.push(account);
            }
        }
        if let Some(storage) = storage {
            let pair_count = user_account_pairs.min(accounts.len());
            for index in 0..pair_count {
                let user_id = format!("user-{:04}", index + 1);
                storage
                    .enqueue_user(UserRecord {
                        username: user_id.clone(),
                        email: format!("{}@loadgen.local", user_id),
                        password_hash: "loadgen".to_string(),
                        role: "user".to_string(),
                        status: "active".to_string(),
                    })
                    .await;
            }
            for (index, account) in accounts.iter().enumerate() {
                let user_id = if index < pair_count {
                    Some(format!("user-{:04}", index + 1))
                } else {
                    None
                };
                let account_id = format!("account-{:04}", index + 1);
                let account_email = user_id
                    .as_ref()
                    .map(|id| format!("{}@loadgen.local", id))
                    .unwrap_or_else(|| "unbound@loadgen.local".to_string());
                storage
                    .enqueue_account(AccountRecord {
                        username: account_id,
                        email: account_email,
                        password_hash: account
                            .private_key
                            .map(|private_key| derive_fingerprint(&private_key))
                            .unwrap_or_else(|| account.address.clone()),
                        role: "account".to_string(),
                        status: "active".to_string(),
                    })
                    .await;
            }
        }
        Self {
            accounts: Arc::new(accounts),
            index: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn next_account(&self) -> Arc<Account> {
        let idx = self.index.fetch_add(1, Ordering::Relaxed);
        let pos = idx % self.accounts.len();
        self.accounts[pos].clone()
    }

    pub fn addresses(&self) -> Vec<String> {
        self.accounts.iter().map(|a| a.address.clone()).collect()
    }
}

#[derive(Debug, Deserialize)]
struct AccountFileRecord {
    name: String,
    address: String,
}

fn derive_address(private_key: &[u8; 32]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(private_key);
    let hash = hasher.finalize();
    hex::encode(&hash[..20])
}

fn derive_fingerprint(private_key: &[u8; 32]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(private_key);
    let hash = hasher.finalize();
    hex::encode(hash)
}
