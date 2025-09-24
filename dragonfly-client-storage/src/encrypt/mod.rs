mod algorithm;
mod cryptor;

use algorithm::{EncryptionAlgorithm, Aes256Ctr};

pub use cryptor::{EncryptReader, DecryptReader};
