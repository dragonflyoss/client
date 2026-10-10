/*
 *     Copyright 2025 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use async_trait::async_trait;
use aws_lc_rs::aead::{Aad, Nonce, RandomizedNonceKey, AES_256_GCM, NONCE_LEN};
use aws_lc_rs::digest::{digest, SHA256};
use base64ct::{Base64, Encoding};
use dragonfly_client_config::dfdaemon::{Config, KmsProvider};
use dragonfly_client_core::{Error, Result};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tracing::info;
use zeroize::Zeroizing;

/// The length of the data encryption key and the key encryption key in bytes
/// (AES-256).
pub const KEY_LEN: usize = 32;

/// The length of the authentication tag of AES-256-GCM in bytes.
const TAG_LEN: usize = 16;

/// The number of bytes of the SHA-256 digest of a key encryption key used as
/// its identifier.
const KEY_ID_LEN: usize = 8;

/// A plaintext symmetric key, zeroed when dropped.
pub type DataKey = Zeroizing<[u8; KEY_LEN]>;

/// A data encryption key wrapped by a key encryption key of the key provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrappedKey {
    /// The identifier of the key encryption key that wrapped the data key.
    pub key_id: String,

    /// The opaque ciphertext of the data key, interpreted by the provider.
    pub ciphertext: Vec<u8>,
}

/// KeyProvider is the key management service used for the envelope encryption of
/// the data encryption keys: it wraps a freshly generated data key with a key
/// encryption key it manages and unwraps it again when the content is read.
///
/// The `context` passed to `wrap` and `unwrap` binds the wrapped key to the
/// parameters of the encrypted file. The provider must refuse to unwrap a key
/// with a different context, so the parameters cannot be tampered with.
#[async_trait]
pub trait KeyProvider: Send + Sync {
    /// Returns the name of the provider, for logging.
    fn name(&self) -> &'static str;

    /// Returns the identifier of the key encryption key used to wrap new data
    /// keys.
    fn active_key_id(&self) -> &str;

    /// Wraps the data key with the active key encryption key.
    async fn wrap(&self, data_key: &DataKey, context: &[u8]) -> Result<WrappedKey>;

    /// Unwraps the data key with the key encryption key it was wrapped with.
    async fn unwrap(&self, wrapped_key: &WrappedKey, context: &[u8]) -> Result<DataKey>;
}

/// Creates the key provider of the configured key management service.
pub async fn new_key_provider(config: &Config) -> Result<Arc<dyn KeyProvider>> {
    let kms = &config.storage.encryption.kms;
    match kms.provider {
        KmsProvider::File => {
            let key_file = kms.file.key_file.as_ref().ok_or_else(|| {
                Error::Encryption(
                    "storage.encryption.kms.file.keyFile is required by the file provider"
                        .to_string(),
                )
            })?;

            let provider = FileKeyProvider::new(key_file, &kms.file.previous_key_files).await?;
            info!(
                "encryption at rest enabled with the {} key provider, active key encryption key: {}",
                provider.name(),
                provider.active_key_id()
            );
            Ok(Arc::new(provider))
        }
    }
}

/// A key encryption key held in memory by the file provider.
struct KeyEncryptionKey {
    /// The identifier of the key, derived from its digest.
    id: String,

    /// The AES-256-GCM key.
    key: RandomizedNonceKey,
}

/// FileKeyProvider loads the key encryption keys from local files, e.g. mounted
/// Kubernetes secrets or files written by an external secret manager, and keeps
/// them in memory. The first key wraps the new data keys and the previous keys
/// only unwrap the data keys of the tasks created before a rotation.
pub struct FileKeyProvider {
    /// The key encryption keys, the first one being the active key.
    keys: Vec<KeyEncryptionKey>,
}

/// Implements the file key provider.
impl FileKeyProvider {
    /// Creates a new file key provider from the active key file and the
    /// previous key files.
    pub async fn new(key_file: &Path, previous_key_files: &[PathBuf]) -> Result<Self> {
        let mut keys: Vec<KeyEncryptionKey> = Vec::new();
        for path in std::iter::once(key_file).chain(previous_key_files.iter().map(|p| p.as_path()))
        {
            let key = load_key_file(path).await?;
            let id = key_id(key.as_ref());
            if keys.iter().any(|k| k.id == id) {
                continue;
            }

            keys.push(KeyEncryptionKey {
                id,
                key: RandomizedNonceKey::new(&AES_256_GCM, key.as_ref()).map_err(|_| {
                    Error::Encryption(format!("invalid key encryption key in {path:?}"))
                })?,
            });
        }

        Ok(Self { keys })
    }

    /// Creates a new file key provider from in-memory keys, the first one being
    /// the active key.
    pub fn from_keys(keys: &[DataKey]) -> Result<Self> {
        if keys.is_empty() {
            return Err(Error::Encryption(
                "at least one key encryption key is required".to_string(),
            ));
        }

        let keys = keys
            .iter()
            .map(|key| {
                Ok(KeyEncryptionKey {
                    id: key_id(key.as_ref()),
                    key: RandomizedNonceKey::new(&AES_256_GCM, key.as_ref())
                        .map_err(|_| Error::Encryption("invalid key encryption key".to_string()))?,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self { keys })
    }
}

/// Implements the key provider for the file key provider.
#[async_trait]
impl KeyProvider for FileKeyProvider {
    /// Returns the name of the provider.
    fn name(&self) -> &'static str {
        "file"
    }

    /// Returns the identifier of the active key encryption key.
    fn active_key_id(&self) -> &str {
        self.keys[0].id.as_str()
    }

    /// Wraps the data key with the active key encryption key, the ciphertext is
    /// the random nonce followed by the encrypted key and its tag.
    async fn wrap(&self, data_key: &DataKey, context: &[u8]) -> Result<WrappedKey> {
        let active = &self.keys[0];
        let mut in_out = Zeroizing::new(data_key.to_vec());
        let nonce = active
            .key
            .seal_in_place_append_tag(Aad::from(context), &mut *in_out)
            .map_err(|_| Error::Encryption("wrap data encryption key failed".to_string()))?;

        let mut ciphertext = Vec::with_capacity(NONCE_LEN + in_out.len());
        ciphertext.extend_from_slice(nonce.as_ref());
        ciphertext.extend_from_slice(&in_out);
        Ok(WrappedKey {
            key_id: active.id.clone(),
            ciphertext,
        })
    }

    /// Unwraps the data key with the key encryption key identified by the
    /// wrapped key.
    async fn unwrap(&self, wrapped_key: &WrappedKey, context: &[u8]) -> Result<DataKey> {
        let key = self
            .keys
            .iter()
            .find(|k| k.id == wrapped_key.key_id)
            .ok_or_else(|| {
                Error::Encryption(format!(
                    "key encryption key {} is not available",
                    wrapped_key.key_id
                ))
            })?;

        if wrapped_key.ciphertext.len() != NONCE_LEN + KEY_LEN + TAG_LEN {
            return Err(Error::Encryption(
                "invalid wrapped data encryption key".to_string(),
            ));
        }

        let (nonce, ciphertext) = wrapped_key.ciphertext.split_at(NONCE_LEN);
        let nonce = Nonce::try_assume_unique_for_key(nonce)
            .map_err(|_| Error::Encryption("invalid wrapped data encryption key".to_string()))?;

        let mut in_out = Zeroizing::new(ciphertext.to_vec());
        let plaintext = key
            .key
            .open_in_place(nonce, Aad::from(context), &mut in_out)
            .map_err(|_| {
                Error::Encryption(format!(
                    "unwrap data encryption key with key encryption key {} failed",
                    wrapped_key.key_id
                ))
            })?;

        let mut data_key = DataKey::new([0u8; KEY_LEN]);
        data_key.copy_from_slice(plaintext);
        Ok(data_key)
    }
}

/// Returns the identifier of a key encryption key, the truncated hex encoded
/// SHA-256 digest of the key.
pub fn key_id(key: &[u8]) -> String {
    hex::encode(&digest(&SHA256, key).as_ref()[..KEY_ID_LEN])
}

/// Loads a key encryption key from a file.
async fn load_key_file(path: &Path) -> Result<DataKey> {
    let content = Zeroizing::new(fs::read(path).await.map_err(|err| {
        Error::Encryption(format!("read key encryption key {path:?} failed: {err}"))
    })?);

    parse_key(&content).ok_or_else(|| {
        Error::Encryption(format!(
            "key encryption key {path:?} must be 32 raw bytes, 64 hex characters or base64"
        ))
    })
}

/// Parses a key encryption key from the content of a key file: the 32 raw key
/// bytes, the key as 64 hex characters or the key as standard base64. The
/// decoders run in constant time with respect to the key material, so the
/// decoding leaks nothing about the key through the cache or the branch
/// predictor.
pub fn parse_key(content: &[u8]) -> Option<DataKey> {
    let mut key = DataKey::new([0u8; KEY_LEN]);
    if content.len() == KEY_LEN {
        key.copy_from_slice(content);
        return Some(key);
    }

    let trimmed = content.trim_ascii();
    if trimmed.len() == KEY_LEN * 2 {
        return decode_hex_ct(trimmed, key.as_mut()).then_some(key);
    }

    let mut decoded = Zeroizing::new([0u8; KEY_LEN]);
    match Base64::decode(trimmed, decoded.as_mut()) {
        Ok(decoded) if decoded.len() == KEY_LEN => {
            key.copy_from_slice(decoded);
            Some(key)
        }
        _ => None,
    }
}

/// Decodes the hex characters into `out` in constant time, returning whether
/// every character was a hex digit. Each character is mapped to its nibble
/// with branchless arithmetic, so neither the control flow nor the memory
/// accesses depend on the key material.
fn decode_hex_ct(hex: &[u8], out: &mut [u8]) -> bool {
    if hex.len() != out.len() * 2 {
        return false;
    }

    let mut invalid: u8 = 0;
    for (byte, pair) in out.iter_mut().zip(hex.chunks_exact(2)) {
        let (high, high_invalid) = decode_hex_digit_ct(pair[0]);
        let (low, low_invalid) = decode_hex_digit_ct(pair[1]);
        *byte = (high << 4) | low;
        invalid |= high_invalid | low_invalid;
    }

    invalid == 0
}

/// Decodes one hex character in constant time, returning its value and a
/// non-zero flag when it is not a hex digit.
fn decode_hex_digit_ct(c: u8) -> (u8, u8) {
    // Each range test yields 0xff when the character is in the range, using
    // the borrow of a wrapping subtraction instead of a comparison.
    let is_digit = in_range_ct(c, b'0', b'9');
    let is_lower = in_range_ct(c, b'a', b'f');
    let is_upper = in_range_ct(c, b'A', b'F');

    let value = (is_digit & c.wrapping_sub(b'0'))
        | (is_lower & (c.wrapping_sub(b'a').wrapping_add(10)))
        | (is_upper & (c.wrapping_sub(b'A').wrapping_add(10)));
    (value, !(is_digit | is_lower | is_upper))
}

/// Returns 0xff when `low <= c <= high` and 0 otherwise, without branching.
fn in_range_ct(c: u8, low: u8, high: u8) -> u8 {
    // (c - low) borrows when c < low, and (high - c) borrows when c > high;
    // the borrow sets the top bit of the 16 bit difference.
    let above = (c as u16).wrapping_sub(low as u16) >> 8;
    let below = (high as u16).wrapping_sub(c as u16) >> 8;
    (((above | below) & 1) as u8).wrapping_sub(1)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_key(byte: u8) -> DataKey {
        DataKey::new([byte; KEY_LEN])
    }

    #[test]
    fn test_parse_key() {
        let raw = [7u8; KEY_LEN];
        assert_eq!(parse_key(&raw).unwrap().as_ref(), &raw);

        let hex_key = format!("{}\n", hex::encode(raw));
        assert_eq!(parse_key(hex_key.as_bytes()).unwrap().as_ref(), &raw);

        let base64_key = Base64::encode_string(&raw);
        assert_eq!(parse_key(base64_key.as_bytes()).unwrap().as_ref(), &raw);

        let upper_hex_key = hex::encode_upper(raw);
        assert_eq!(parse_key(upper_hex_key.as_bytes()).unwrap().as_ref(), &raw);

        assert!(parse_key(b"too short").is_none());
        assert!(parse_key(&[0u8; 33]).is_none());

        // 64 characters that are not all hex digits are rejected rather than
        // decoded as garbage.
        let mut invalid = hex::encode(raw).into_bytes();
        invalid[10] = b'g';
        assert!(parse_key(&invalid).is_none());
    }

    #[test]
    fn test_decode_hex_ct() {
        for c in 0u8..=255 {
            let (value, invalid) = decode_hex_digit_ct(c);
            match (c as char).to_digit(16) {
                Some(expected) => {
                    assert_eq!(value, expected as u8, "character {c}");
                    assert_eq!(invalid, 0, "character {c}");
                }
                None => assert_ne!(invalid, 0, "character {c}"),
            }
        }

        let mut out = [0u8; 4];
        assert!(decode_hex_ct(b"00ffA5c3", &mut out));
        assert_eq!(out, [0x00, 0xff, 0xa5, 0xc3]);
        assert!(!decode_hex_ct(b"00ffA5", &mut out));
        assert!(!decode_hex_ct(b"00ffA5cz", &mut out));
    }

    #[tokio::test]
    async fn test_file_key_provider_wrap_unwrap() {
        let provider = FileKeyProvider::from_keys(&[data_key(1)]).unwrap();
        let dek = data_key(9);
        let wrapped = provider.wrap(&dek, b"context").await.unwrap();
        assert_eq!(wrapped.key_id, provider.active_key_id());
        assert_eq!(wrapped.ciphertext.len(), NONCE_LEN + KEY_LEN + TAG_LEN);

        let unwrapped = provider.unwrap(&wrapped, b"context").await.unwrap();
        assert_eq!(unwrapped.as_ref(), dek.as_ref());

        // A different context must fail to authenticate.
        assert!(provider.unwrap(&wrapped, b"other").await.is_err());

        // A tampered ciphertext must fail to authenticate.
        let mut tampered = wrapped.clone();
        tampered.ciphertext[NONCE_LEN] ^= 1;
        assert!(provider.unwrap(&tampered, b"context").await.is_err());

        // Every wrap uses a fresh nonce.
        let again = provider.wrap(&dek, b"context").await.unwrap();
        assert_ne!(again.ciphertext, wrapped.ciphertext);
    }

    #[tokio::test]
    async fn test_file_key_provider_rotation() {
        let old = FileKeyProvider::from_keys(&[data_key(1)]).unwrap();
        let dek = data_key(9);
        let wrapped = old.wrap(&dek, b"context").await.unwrap();

        // The rotated provider still unwraps keys wrapped by the previous key
        // and wraps new keys with the new key.
        let rotated = FileKeyProvider::from_keys(&[data_key(2), data_key(1)]).unwrap();
        assert_ne!(rotated.active_key_id(), old.active_key_id());
        assert_eq!(
            rotated.unwrap(&wrapped, b"context").await.unwrap().as_ref(),
            dek.as_ref()
        );
        assert_eq!(
            rotated.wrap(&dek, b"context").await.unwrap().key_id,
            rotated.active_key_id()
        );

        // A provider without the previous key cannot unwrap.
        let replaced = FileKeyProvider::from_keys(&[data_key(2)]).unwrap();
        assert!(replaced.unwrap(&wrapped, b"context").await.is_err());
    }

    #[tokio::test]
    async fn test_file_key_provider_from_files() {
        let dir = tempfile::tempdir().unwrap();
        let active = dir.path().join("kek.key");
        let previous = dir.path().join("kek-old.key");
        fs::write(&active, hex::encode([1u8; KEY_LEN]))
            .await
            .unwrap();
        fs::write(&previous, [2u8; KEY_LEN]).await.unwrap();

        let provider = FileKeyProvider::new(&active, &[previous.clone(), active.clone()])
            .await
            .unwrap();
        assert_eq!(provider.keys.len(), 2);
        assert_eq!(provider.active_key_id(), key_id(&[1u8; KEY_LEN]));

        assert!(FileKeyProvider::new(&dir.path().join("missing"), &[])
            .await
            .is_err());

        let invalid = dir.path().join("invalid.key");
        fs::write(&invalid, b"not a key").await.unwrap();
        assert!(FileKeyProvider::new(&invalid, &[]).await.is_err());
    }
}
