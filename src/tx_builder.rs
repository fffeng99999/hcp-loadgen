use crate::account_pool::Account;
use crate::config::TxType;
use rand::Rng;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxKind {
    Transfer,
    Stake,
    ContractCall,
}

impl From<TxType> for TxKind {
    fn from(value: TxType) -> Self {
        match value {
            TxType::Transfer => TxKind::Transfer,
            TxType::Stake => TxKind::Stake,
            TxType::ContractCall => TxKind::ContractCall,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tx {
    pub from: String,
    pub to: String,
    pub nonce: u64,
    pub kind: TxKind,
    pub payload_hex: String,
    pub signature_hex: String,
}

#[derive(Clone)]
pub struct TxBuilder {
    payload_size: usize,
    kinds: Vec<TxKind>,
    to_addresses: Vec<String>,
}

impl TxBuilder {
    pub fn new(payload_size: usize, kinds: Vec<TxKind>, to_addresses: Vec<String>) -> Self {
        Self {
            payload_size,
            kinds,
            to_addresses,
        }
    }

    pub fn build_tx(&self, from: &Account, nonce: u64) -> Tx {
        let mut rng = rand::thread_rng();
        let kind = self.kinds[rng.gen_range(0..self.kinds.len())].clone();
        let to = self.to_addresses[rng.gen_range(0..self.to_addresses.len())].clone();
        let payload_hex = random_payload(self.payload_size);
        Tx {
            from: from.address.clone(),
            to,
            nonce,
            kind,
            payload_hex,
            signature_hex: String::new(),
        }
    }

    pub fn encode_tx(&self, tx: &Tx) -> Vec<u8> {
        serde_json::to_vec(tx).unwrap_or_default()
    }
}

fn random_payload(size: usize) -> String {
    let mut bytes = vec![0u8; size];
    rand::thread_rng().fill(&mut bytes[..]);
    hex::encode(bytes)
}
