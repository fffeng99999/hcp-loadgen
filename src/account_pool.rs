use rand::RngCore;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Debug)]
pub struct Account {
    pub address: String,
    pub private_key: [u8; 32],
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
    pub fn new(count: usize, initial_nonce: u64) -> Self {
        let mut accounts = Vec::with_capacity(count);
        for _ in 0..count {
            let mut private_key = [0u8; 32];
            rand::thread_rng().fill_bytes(&mut private_key);
            let address = derive_address(&private_key);
            let account = Arc::new(Account {
                address,
                private_key,
                nonce: AtomicU64::new(initial_nonce),
            });
            accounts.push(account);
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

fn derive_address(private_key: &[u8; 32]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(private_key);
    let hash = hasher.finalize();
    hex::encode(&hash[..20])
}
