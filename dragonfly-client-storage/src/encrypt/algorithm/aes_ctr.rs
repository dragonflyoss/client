use aes::Aes256;
use ctr::Ctr128BE;
use ctr::cipher::{StreamCipher, KeyIvInit};
use generic_array::typenum::{U16, U32};
use generic_array::GenericArray;

use super::EncryptionAlgorithm;

pub type Aes256Ctr = Ctr128BE<Aes256>;

impl EncryptionAlgorithm for Aes256Ctr {
    type NonceSize = U16;
    type KeySize = U32;

    fn new_from_array(
            key: &GenericArray<u8, Self::KeySize>, 
            nonce: &GenericArray<u8, Self::NonceSize>
        ) -> Self where Self: Sized {
        <Aes256Ctr as KeyIvInit>::new(key, nonce)
    }

    fn apply_keystream(&mut self, data: &mut [u8]) {
        <Aes256Ctr as StreamCipher>::apply_keystream(self, data);
    }

}

