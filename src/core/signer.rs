use sha2::{Digest, Sha256};

/// 签名器，提供基于 SHA-256 的简易签名实现。
/// 将私钥与待签名数据拼接后进行哈希，生成签名结果。
pub struct Signer;

impl Signer {
    /// 使用私钥对数据进行签名，返回 SHA-256 哈希值（32 字节）。
    pub fn sign(&self, private_key: &[u8; 32], data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(private_key);
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}
