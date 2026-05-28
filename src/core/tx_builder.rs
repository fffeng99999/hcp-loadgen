use crate::config::{Compression, TxEncoding, TxType};
use crate::types::InMemoryAccount;
use cosmos_sdk_proto::cosmos::bank::v1beta1::MsgSend;
use cosmos_sdk_proto::cosmos::base::v1beta1::Coin;
use cosmos_sdk_proto::cosmos::tx::v1beta1::{AuthInfo, Fee, TxBody, TxRaw};
use cosmos_sdk_proto::Any;
use prost::Message;
use rand::Rng;
use serde::{Deserialize, Serialize};

/// 交易类型枚举，用于标识交易的业务种类。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TxKind {
    /// 转账交易
    Transfer,
    /// 质押交易
    Stake,
    /// 合约调用交易
    ContractCall,
}

impl From<TxType> for TxKind {
    /// 从配置层的 TxType 转换为构建层的 TxKind。
    fn from(value: TxType) -> Self {
        match value {
            TxType::Transfer => TxKind::Transfer,
            TxType::Stake => TxKind::Stake,
            TxType::ContractCall => TxKind::ContractCall,
        }
    }
}

/// 交易结构体，包含发送方、接收方、nonce、类型、负载和签名。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tx {
    /// 发送方地址
    pub from: String,
    /// 接收方地址
    pub to: String,
    /// 交易 nonce（序列号）
    pub nonce: u64,
    /// 交易类型
    pub kind: TxKind,
    /// 十六进制编码的随机负载数据
    pub payload_hex: String,
    /// 十六进制编码的签名
    pub signature_hex: String,
}

/// 交易构建器，负责构造和编码交易负载。
/// 支持 Protobuf 和 JSON 两种编码格式，以及可选的 Gzip 压缩。
#[derive(Clone)]
pub struct TxBuilder {
    /// 随机负载大小（字节）
    payload_size: usize,
    /// 可用的交易类型列表
    kinds: Vec<TxKind>,
    /// 候选接收方地址列表
    to_addresses: Vec<String>,
    /// 交易编码格式
    tx_encoding: TxEncoding,
    /// 压缩方式
    compression: Compression,
    /// 代币单位
    denom: String,
    /// 转账金额
    send_amount: u64,
    /// Gas 上限
    gas_limit: u64,
}

impl TxBuilder {
    /// 创建新的交易构建器实例。
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

    /// 构建一笔新的交易，随机选择交易类型和接收方，并生成随机负载。
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

    /// 编码交易：先按指定格式编码，再根据配置决定是否压缩。
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

    /// 使用 Cosmos SDK 的 TxRaw 格式编码交易（Protobuf）。
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

/// 生成指定大小的随机字节序列，并以十六进制字符串返回。
fn random_payload(size: usize) -> String {
    let mut bytes = vec![0u8; size];
    rand::thread_rng().fill(&mut bytes[..]);
    hex::encode(bytes)
}

/// 使用 Gzip 压缩数据。
fn compress_gzip(data: &[u8]) -> Vec<u8> {
    use flate2::write::GzEncoder;
    use flate2::Compression as GzipCompression;
    use std::io::Write;

    let mut encoder = GzEncoder::new(Vec::new(), GzipCompression::default());
    let _ = encoder.write_all(data);
    encoder.finish().unwrap_or_default()
}
