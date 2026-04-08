use sha2::{Digest, Sha256};

pub struct Signer;

impl Signer {
    pub fn sign(&self, private_key: &[u8; 32], data: &[u8]) -> Vec<u8> {
        let mut hasher = Sha256::new();
        hasher.update(private_key);
        hasher.update(data);
        hasher.finalize().to_vec()
    }
}
