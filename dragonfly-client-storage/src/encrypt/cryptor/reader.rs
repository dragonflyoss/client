use tokio::io::AsyncRead;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::ReadBuf;
use cipher::StreamCipherSeek;
use generic_array::GenericArray;

use crate::encrypt::{EncryptionAlgorithm, Aes256Ctr};

pub struct EncryptReader<R: AsyncRead, A: EncryptionAlgorithm> {
    inner: R,
    cipher: A,
}

// impl<R, A: EncryptionAlgorithm> EncryptReader<R, A> {
//     pub fn new(inner: R, key: &[u8], piece_id: &str) -> Self {
//         let (task_id, piece_num) = parse_piece_id(piece_id)
//             .expect("should have task_id and piece_num");

//         let nonce = A::build_nonce(task_id, piece_num);
//         let cipher = A::new(key, &nonce);

//         Self { inner, cipher }
//     }
// }

impl<R: AsyncRead> EncryptReader<R, Aes256Ctr> {
    /// default for Aes256Ctr
    pub fn new(inner: R, key: &[u8], task_id: &str, offset: u64) -> Self {
        let key = <Aes256Ctr as EncryptionAlgorithm>::derive_key(key, task_id);
        // let nonce = [0u8; <Aes256Ctr as EncryptionAlgorithm>::NONCE_SIZE];
        let zero_nonce = GenericArray::<u8, <Aes256Ctr as EncryptionAlgorithm>::NonceSize>::default();

        let mut cipher = <Aes256Ctr as EncryptionAlgorithm>::new_from_array(&key, &zero_nonce);
        cipher.seek(offset);

        Self { inner, cipher }
    }
}

impl<R: AsyncRead, A: EncryptionAlgorithm> Unpin for EncryptReader<R, A> where R: Unpin {}

impl<R: AsyncRead + Unpin, A: EncryptionAlgorithm> AsyncRead for EncryptReader<R, A> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let prev_filled = buf.filled().len();
        let poll = Pin::new(&mut self.inner).poll_read(cx, buf);
        if let Poll::Ready(Ok(())) = &poll {
            let filled = &mut buf.filled_mut()[prev_filled..];
            self.cipher.apply_keystream(filled);
        }
        poll
    }
}

// same for decrypt
pub type DecryptReader<R, A> = EncryptReader<R, A>;

#[cfg(test)]
mod tests {
    use super::*;
    use rand::{rngs::OsRng, RngCore};
    use tokio::io::{self, AsyncReadExt};
    use std::io::Cursor;

    const TEST_DATA: &[u8] = b"The quick brown fox jumps over the lazy dog";
    const PIECE_ID: &str = "d3c4e940ad06c47fc36ac67801e6f8e36cb400e2391708620bc7e865b102062c-0";
    const TASK_ID: &str = "d3c4e940ad06c47fc36ac67801e6f8e36cb400e2391708620bc7e865b102062c";

    fn generate_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        OsRng.fill_bytes(&mut key);
        key
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_cycle() {
        // let (key, iv) = generate_key_iv();
        let key = generate_key();

        // Simulate input reader with AsyncCursor
        let input = Cursor::new(TEST_DATA);

        // Encrypt
        let mut encrypt_reader = EncryptReader::<_, Aes256Ctr>::new(input, &key, TASK_ID, 0);
        let mut encrypted = Vec::new();
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

        println!("Encrypted: {}", String::from_utf8_lossy(&encrypted));

        // Decrypt
        let encrypted_cursor = Cursor::new(encrypted);
        let mut decrypt_reader = DecryptReader::<_, Aes256Ctr>::new(encrypted_cursor, &key, TASK_ID, 0);
        let mut decrypted = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted).await.unwrap();

        println!("Decrypted: {}", String::from_utf8_lossy(&decrypted));

        // Assert round-trip
        assert_eq!(decrypted, TEST_DATA);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_cycle_direct_new() {
        let key = generate_key();
        let input = Cursor::new(TEST_DATA);

        // default A: Aes256Ctr EncryptReader::new
        let mut encrypt_reader = EncryptReader::new(input, &key, TASK_ID, 0);
        let mut encrypted = Vec::new();
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

        let encrypted_cursor = Cursor::new(encrypted);
        let mut decrypt_reader = DecryptReader::new(encrypted_cursor, &key, TASK_ID, 0);
        let mut decrypted = Vec::new();
        decrypt_reader.read_to_end(&mut decrypted).await.unwrap();

        // Assert round-trip
        assert_eq!(decrypted, TEST_DATA);
    }

    #[tokio::test]
    async fn test_encrypt_decrypt_cycle_fail() {
        let mut key = generate_key();

        // Simulate input reader with Cursor
        let input = Cursor::new(TEST_DATA);

        // Encrypt
        let mut encrypt_reader = EncryptReader::<_, Aes256Ctr>::new(input, &key, TASK_ID, 0);
        let mut encrypted = Vec::new();
        encrypt_reader.read_to_end(&mut encrypted).await.unwrap();

        println!("Encrypted: {}", String::from_utf8_lossy(&encrypted));

        // Decrypt
        key[0] += 1;
        let encrypted_cursor = Cursor::new(encrypted);
        let mut decrypt_reader = DecryptReader::<_, Aes256Ctr>::new(encrypted_cursor, &key, TASK_ID, 0);
        let mut decrypted = Vec::new();
        let _ = decrypt_reader.read_to_end(&mut decrypted).await.unwrap();
        
        println!("Decrypted: {}", String::from_utf8_lossy(&decrypted));

        assert_eq!(decrypted.len(), TEST_DATA.len());
        assert_ne!(decrypted, TEST_DATA);
    }
}
