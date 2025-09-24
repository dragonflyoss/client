mod aes_ctr;

use generic_array::{GenericArray, ArrayLength};
use hkdf::Hkdf;
use sha2::Sha256;

pub use aes_ctr::Aes256Ctr;

/// EncryptionAlgorithm is a trait that defines the encryption algorithm.
pub trait EncryptionAlgorithm {
    /// Bytes of key
    type KeySize: ArrayLength<u8>;
    /// Bytes of nonce
    type NonceSize: ArrayLength<u8>;

    /// new creates a new encryption algorithm from a key and a nonce using u8 slice.
    fn new(key: &[u8], nonce: &[u8]) -> Self where Self: Sized {
        // will panic if length is not fit
        let key_array = GenericArray::<u8, Self::KeySize>::from_slice(key);
        let nonce_array = GenericArray::<u8, Self::NonceSize>::from_slice(nonce);
        Self::new_from_array(key_array, nonce_array)
    }

    /// new_from_array creates a new encryption algorithm from a key and a nonce using GenericArray.
    fn new_from_array(
        key: &GenericArray<u8, Self::KeySize>, 
        nonce: &GenericArray<u8, Self::NonceSize>
    ) -> Self where Self: Sized;

    /// apply_keystream applies the keystream to the data.
    fn apply_keystream(&mut self, data: &mut [u8]);

    /// derive_key derives a key from a master key and a task id.
    fn derive_key(master_key: &[u8], task_id: &str) -> GenericArray<u8, Self::KeySize> {
        let hk = Hkdf::<Sha256>::new(Some(task_id.as_bytes()), master_key);
        let mut okm: GenericArray<u8, Self::KeySize> = GenericArray::default();
        hk.expand(b"task-encryption", &mut okm).unwrap();
        okm
    }
}
