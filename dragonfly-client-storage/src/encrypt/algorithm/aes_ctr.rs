use aes::Aes256;
use ctr::Ctr128BE;
use ctr::cipher::{StreamCipher, KeyIvInit};
use generic_array::typenum::{U16, U32};
// use generic_array::GenericArray;

use super::EncryptionAlgorithm;

pub type Aes256Ctr = Ctr128BE<Aes256>;

impl EncryptionAlgorithm for Aes256Ctr {
    type NonceSize = U16;
    type KeySize = U32;

    // fn new(key: &[u8], nonce: &[u8]) -> Self {
    //     // if key.len() != <Self::KeySize as Unsigned>::to_usize() {
    //     //     panic!("invalid key length");
    //     // }
    //     // if nonce.len() != <Self::NonceSize as Unsigned>::to_usize() {
    //     //     panic!("invalid nonce length");
    //     // }

    //     let key_array = GenericArray::<u8, Self::KeySize>::from_slice(key);
    //     let nonce_array = GenericArray::<u8, Self::NonceSize>::from_slice(nonce);

    //     // <Ctr128BE<Aes256> as KeyIvInit>::new(key_array, nonce_array)
    //     Self::new_from_array(key_array, nonce_array)
    // }

    fn new_from_array(
            key: &generic_array::GenericArray<u8, Self::KeySize>, 
            nonce: &generic_array::GenericArray<u8, Self::NonceSize>
        ) -> Self where Self: Sized {
        <Ctr128BE<Aes256> as KeyIvInit>::new(key, nonce)
    }

    fn apply_keystream(&mut self, data: &mut [u8]) {
        <Aes256Ctr as StreamCipher>::apply_keystream(self, data);
    }

}

