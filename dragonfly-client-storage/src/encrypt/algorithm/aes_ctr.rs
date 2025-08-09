use aes::Aes256;
use ctr::Ctr128BE;
use ctr::cipher::{StreamCipher, KeyIvInit};

use super::EncryptAlgo;

pub type Aes256Ctr = Ctr128BE<Aes256>;

impl EncryptAlgo for Aes256Ctr {
    const NONCE_SIZE: usize = 16;
    const KEY_SIZE: usize = 32;

    fn new(key: &[u8], nonce: &[u8]) -> Self {
        let key_array: [u8; Self::KEY_SIZE] = key.try_into()
            .expect("key should be exactly 32 bytes");
        let nonce_array: [u8; Self::NONCE_SIZE] = nonce.try_into()
            .expect("nonce should be exactly 16 bytes");
        
        <Ctr128BE<Aes256> as KeyIvInit>::new(&key_array.into(), &nonce_array.into())
    }

    fn apply_keystream(&mut self, data: &mut [u8]) {
        <Aes256Ctr as StreamCipher>::apply_keystream(self, data);
    }
}

