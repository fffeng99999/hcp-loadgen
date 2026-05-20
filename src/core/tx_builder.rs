use crate::config::{Compression, TxEncoding, TxType};
use crate::types::InMemoryAccount;
use cosmos_sdk_proto::cosmos::bank::v1beta1::MsgSend;
use cosmos_sdk_proto::cosmos::base::v1beta1::Coin;
use cosmos_sdk_proto::cosmos::tx::v1beta1::{AuthInfo, Fee, TxBody, TxRaw};
use cosmos_sdk_proto::Any;
use prost::Message;
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
    tx_encoding: TxEncoding,
    compression: Compression,
    denom: String,
    send_amount: u64,
    gas_limit: u64,
}

impl TxBuilder {
    pub fn new(
        payload_size: usize,
        kinds: Vec<TxKind>,
        to_addresses: Vec<String>,
        tx_encoding: TxEncoding,
        compression: Compression,
        denom: String,
        send_amount: u64,
        gas_limit: u64,
    ) -> Self {
        Self {
            payload_size,
            kinds,
            to_addresses,
            tx_encoding,
            compression,
            denom,
            send_amount,
            gas_limit,
        }
    }

    pub fn build_tx(&self, from: &InMemoryAccount, nonce: u64) -> Tx {
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
        let encoded = match self.tx_encoding {
            TxEncoding::Proto => self.encode_sdk_tx_raw(tx),
            TxEncoding::Json => serde_json::to_vec(tx).unwrap_or_default(),
        };
        match self.compression {
            Compression::None => encoded,
            Compression::Gzip => compress_gzip(&encoded),
        }
    }

    fn encode_sdk_tx_raw(&self, tx: &Tx) -> Vec<u8> {
        let msg = MsgSend {
            from_address: tx.from.clone(),
            to_address: tx.to.clone(),
            amount: vec![Coin {
                denom: self.denom.clone(),
                amount: self.send_amount.max(1).to_string(),
            }],
        };
        let body = TxBody {
            messages: vec![Any {
                type_url: "/cosmos.bank.v1beta1.MsgSend".to_string(),
                value: msg.encode_to_vec(),
            }],
            memo: format!("nonce={};payload={}", tx.nonce, tx.payload_hex),
            timeout_height: 0,
            extension_options: Vec::new(),
            non_critical_extension_options: Vec::new(),
        };
        let auth_info = AuthInfo {
            signer_infos: Vec::new(),
            fee: Some(Fee {
                amount: Vec::new(),
                gas_limit: self.gas_limit,
                payer: String::new(),
                granter: String::new(),
            }),
            tip: None,
        };
        let raw = TxRaw {
            body_bytes: body.encode_to_vec(),
            auth_info_bytes: auth_info.encode_to_vec(),
            signatures: Vec::new(),
        };
        raw.encode_to_vec()
    }
}

fn random_payload(size: usize) -> String {
    let mut bytes = vec![0u8; size];
    rand::thread_rng().fill(&mut bytes[..]);
    hex::encode(bytes)
}

fn compress_gzip(data: &[u8]) -> Vec<u8> {
    use flate2::write::GzEncoder;
    use flate2::Compression as GzipCompression;
    use std::io::Write;

    let mut encoder = GzEncoder::new(Vec::new(), GzipCompression::default());
    let _ = encoder.write_all(data);
    encoder.finish().unwrap_or_default()
}
