use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

#[derive(Debug)]
pub struct InMemoryAccount {
    pub account_id: u64,
    pub address: String,
    pub priv_key: [u8; 32],
    pub nonce: AtomicU64,
    available_balance: RwLock<f64>,
    frozen_balance: RwLock<f64>,
}

impl InMemoryAccount {
    pub fn new(
        account_id: u64,
        address: String,
        priv_key: [u8; 32],
        nonce: u64,
        available_balance: f64,
        frozen_balance: f64,
    ) -> Self {
        Self {
            account_id,
            address,
            priv_key,
            nonce: AtomicU64::new(nonce),
            available_balance: RwLock::new(available_balance),
            frozen_balance: RwLock::new(frozen_balance),
        }
    }

    pub fn next_nonce(&self) -> u64 {
        self.nonce.fetch_add(1, Ordering::Relaxed)
    }

    pub fn balances(&self) -> (f64, f64) {
        let available = *self.available_balance.read().unwrap_or_else(|e| e.into_inner());
        let frozen = *self.frozen_balance.read().unwrap_or_else(|e| e.into_inner());
        (available, frozen)
    }

    pub fn apply_fill(&self, quantity: f64, side: &str) {
        let mut available = self
            .available_balance
            .write()
            .unwrap_or_else(|e| e.into_inner());
        let mut frozen = self
            .frozen_balance
            .write()
            .unwrap_or_else(|e| e.into_inner());
        if side == "BUY" {
            *frozen = (*frozen - quantity).max(0.0);
            *available += quantity;
        } else {
            *frozen += quantity;
            *available = (*available - quantity).max(0.0);
        }
    }
}

#[derive(Debug, Clone)]
pub struct TransactionRecord {
    pub tx_hash: String,
    pub account_id: u64,
    pub side: String,
    pub price: f64,
    pub quantity: f64,
    pub latency_ms: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct BalanceSnapshot {
    pub account_id: u64,
    pub available_balance: f64,
    pub frozen_balance: f64,
}

#[derive(Debug, Clone)]
pub struct AccountIdentity {
    pub account_id: u64,
    pub address: String,
}
