use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

/// 内存中维护的账户数据结构，包含地址、私钥、nonce 及余额信息。
/// 使用原子类型和读写锁支持高并发访问。
#[derive(Debug)]
pub struct InMemoryAccount {
    /// 账户唯一标识符
    pub account_id: u64,
    /// 账户区块链地址（如 bech32 编码字符串）
    pub address: String,
    /// 签名者名称（可选），用于密钥环标识
    pub signer_name: Option<String>,
    /// 32 字节私钥，用于交易签名
    pub priv_key: [u8; 32],
    /// 原子计数器，记录当前 nonce，每次发交易时自动递增
    pub nonce: AtomicU64,
    /// 可用余额，读写锁保护
    available_balance: RwLock<f64>,
    /// 冻结余额，读写锁保护
    frozen_balance: RwLock<f64>,
}

impl InMemoryAccount {
    /// 创建一个新的内存账户实例。
    pub fn new(
        account_id: u64,
        address: String,
        signer_name: Option<String>,
        priv_key: [u8; 32],
        nonce: u64,
        available_balance: f64,
        frozen_balance: f64,
    ) -> Self {
        Self {
            account_id,
            address,
            signer_name,
            priv_key,
            nonce: AtomicU64::new(nonce),
            available_balance: RwLock::new(available_balance),
            frozen_balance: RwLock::new(frozen_balance),
        }
    }

    /// 获取当前 nonce 值，并将内部计数器原子递增 1。
    pub fn next_nonce(&self) -> u64 {
        self.nonce.fetch_add(1, Ordering::Relaxed)
    }

    /// 读取当前可用余额和冻结余额，返回 (available, frozen) 元组。
    pub fn balances(&self) -> (f64, f64) {
        let available = *self.available_balance.read().unwrap_or_else(|e| e.into_inner());
        let frozen = *self.frozen_balance.read().unwrap_or_else(|e| e.into_inner());
        (available, frozen)
    }

    /// 根据交易方向（BUY/SELL）更新可用余额和冻结余额。
    /// BUY：解冻并增加可用余额；SELL：冻结并减少可用余额。
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

/// 交易记录数据结构，用于持久化或指标分析。
#[derive(Debug, Clone)]
pub struct TransactionRecord {
    /// 交易哈希（十六进制字符串）
    pub tx_hash: String,
    /// 发起账户 ID
    pub account_id: u64,
    /// 交易方向，如 "BUY" 或 "SELL"
    pub side: String,
    /// 交易价格
    pub price: f64,
    /// 交易数量
    pub quantity: f64,
    /// 从发送到收到响应的延迟（毫秒）
    pub latency_ms: f64,
    /// 交易时间戳（Unix 时间，毫秒或秒）
    pub timestamp: i64,
}

/// 账户余额快照，用于在特定时间点记录账户状态。
#[derive(Debug, Clone)]
pub struct BalanceSnapshot {
    /// 账户唯一标识符
    pub account_id: u64,
    /// 快照时的可用余额
    pub available_balance: f64,
    /// 快照时的冻结余额
    pub frozen_balance: f64,
}

/// 账户身份信息，用于标识和查找账户。
#[derive(Debug, Clone)]
pub struct AccountIdentity {
    /// 账户唯一标识符
    pub account_id: u64,
    /// 账户区块链地址
    pub address: String,
}
