use crate::types::{AccountIdentity, BalanceSnapshot, InMemoryAccount};
use dashmap::DashMap;
use rand::distributions::WeightedIndex;
use rand::prelude::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

/// 账户池，管理所有预生成账户并提供线程安全的账户选择策略。
/// 支持轮询（RoundRobin）、随机（Random）和 Zipf 分布三种选择模式。
#[derive(Clone)]
pub struct AccountPool {
    /// 以 account_id 为键的并发哈希表，存储所有账户
    accounts: Arc<DashMap<u64, Arc<InMemoryAccount>>>,
    /// 账户 ID 列表，用于轮询、随机或 Zipf 选择
    rotation: Arc<Vec<u64>>,
    /// 轮询模式下的原子索引计数器
    index: Arc<AtomicUsize>,
    /// Zipf 分布的加权索引（可选），仅在 Zipf 模式下使用
    zipf_dist: Option<Arc<WeightedIndex<f64>>>,
    /// 是否启用随机选择模式
    use_random: bool,
}

impl AccountPool {
    /// 从一组账户创建账户池，建立 ID 到账户的映射并初始化轮询列表。
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

    /// 启用 Zipf 分布选择模式，根据 alpha 参数计算每个账户的权重。
    /// alpha 越大，排名靠前的账户被选中概率越高，模拟热点账户场景。
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

    /// 设置是否启用随机选择模式。
    pub fn set_random_mode(&mut self, enable: bool) {
        self.use_random = enable;
    }

    /// 根据当前选择模式（Zipf > Random > RoundRobin）获取下一个账户。
    /// 返回 Arc<InMemoryAccount> 以便多线程共享。
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

    /// 获取所有账户的地址列表。
    pub fn addresses(&self) -> Vec<String> {
        self.accounts
            .iter()
            .map(|entry| entry.value().address.clone())
            .collect()
    }

    /// 获取所有账户的余额快照，用于状态检查或持久化。
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

    /// 获取所有账户的身份信息列表（ID 和地址）。
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
