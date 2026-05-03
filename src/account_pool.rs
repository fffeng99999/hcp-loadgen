use crate::types::{AccountIdentity, BalanceSnapshot, InMemoryAccount};
use dashmap::DashMap;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct AccountPool {
    accounts: Arc<DashMap<u64, Arc<InMemoryAccount>>>,
    rotation: Arc<Vec<u64>>,
    index: Arc<AtomicUsize>,
    zipf_dist: Option<Arc<WeightedIndex<f64>>>,
    use_random: bool,
}

impl AccountPool {
    pub fn from_accounts(accounts: Vec<InMemoryAccount>) -> Self {
        let map = DashMap::with_capacity(accounts.len());
        let mut rotation = Vec::with_capacity(accounts.len());
        for account in accounts {
            let account_id = account.account_id;
            rotation.push(account_id);
            map.insert(account_id, Arc::new(account));
        }
        Self {
            accounts: Arc::new(map),
            rotation: Arc::new(rotation),
            index: Arc::new(AtomicUsize::new(0)),
            zipf_dist: None,
            use_random: false,
        }
    }

    pub fn set_zipf_mode(&mut self, alpha: f64) {
        let n = self.rotation.len();
        if n == 0 || alpha <= 0.0 {
            self.zipf_dist = None;
            return;
        }
        let weights: Vec<f64> = (1..=n)
            .map(|rank| 1.0 / (rank as f64).powf(alpha))
            .collect();
        match WeightedIndex::new(&weights) {
            Ok(dist) => self.zipf_dist = Some(Arc::new(dist)),
            Err(_) => self.zipf_dist = None,
        }
    }

    pub fn set_random_mode(&mut self, enable: bool) {
        self.use_random = enable;
    }

    pub fn next_account(&self) -> Option<Arc<InMemoryAccount>> {
        if self.rotation.is_empty() {
            return None;
        }
        let position = if let Some(dist) = self.zipf_dist.as_ref() {
            let mut rng = rand::thread_rng();
            dist.sample(&mut rng)
        } else if self.use_random {
            let mut rng = rand::thread_rng();
            rng.gen_range(0..self.rotation.len())
        } else {
            let idx = self.index.fetch_add(1, Ordering::Relaxed);
            idx % self.rotation.len()
        };
        let account_id = self.rotation[position];
        self.accounts.get(&account_id).map(|entry| entry.clone())
    }

    pub fn addresses(&self) -> Vec<String> {
        self.accounts
            .iter()
            .map(|entry| entry.value().address.clone())
            .collect()
    }

    pub fn snapshot_balances(&self) -> Vec<BalanceSnapshot> {
        self.accounts
            .iter()
            .map(|entry| {
                let account = entry.value();
                let (available_balance, frozen_balance) = account.balances();
                BalanceSnapshot {
                    account_id: account.account_id,
                    available_balance,
                    frozen_balance,
                }
            })
            .collect()
    }

    pub fn snapshot_accounts(&self) -> Vec<AccountIdentity> {
        self.accounts
            .iter()
            .map(|entry| {
                let account = entry.value();
                AccountIdentity {
                    account_id: account.account_id,
                    address: account.address.clone(),
                }
            })
            .collect()
    }
}
