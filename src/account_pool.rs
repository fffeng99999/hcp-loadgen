use crate::types::{AccountIdentity, BalanceSnapshot, InMemoryAccount};
use dashmap::DashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[derive(Clone)]
pub struct AccountPool {
    accounts: Arc<DashMap<u64, Arc<InMemoryAccount>>>,
    rotation: Arc<Vec<u64>>,
    index: Arc<AtomicUsize>,
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
        }
    }

    pub fn next_account(&self) -> Option<Arc<InMemoryAccount>> {
        if self.rotation.is_empty() {
            return None;
        }
        let idx = self.index.fetch_add(1, Ordering::Relaxed);
        let position = idx % self.rotation.len();
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
