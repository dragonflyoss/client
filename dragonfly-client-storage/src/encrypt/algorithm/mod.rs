mod aes_ctr;

pub use aes_ctr::Aes256Ctr;

pub trait EncryptionAlgorithm {
    /// Bytes of key
    const KEY_SIZE: usize;
    /// Bytes of nonce
    const NONCE_SIZE: usize;

    fn new(key: &[u8], nonce: &[u8]) -> Self;

    fn apply_keystream(&mut self, data: &mut [u8]);

    fn build_nonce(task_id: &str, piece_num: u32) -> Vec<u8> {
        let mut nonce = vec![0u8; Self::NONCE_SIZE];
        let task_bytes = task_id.as_bytes();

        let len = std::cmp::min(12, task_bytes.len());
        assert!(len > 0);
        nonce[..len].copy_from_slice(&task_bytes[..len]); // first 12 bytes dor task_id
        nonce[12..].copy_from_slice(&piece_num.to_be_bytes()); // remaining bytes for piece num
        nonce
    }
}
