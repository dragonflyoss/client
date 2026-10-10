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

//! Encryption at rest of the task content.
//!
//! When `storage.encryption.enable` is set, every task content file written by
//! dfdaemon is encrypted with its own data encryption key (DEK) using
//! AES-256-GCM over fixed-size chunks (see [`layout::Layout`]), and the DEK is
//! wrapped by a key encryption key (KEK) managed by the configured key
//! management service (see [`kms::KeyProvider`]), i.e. envelope encryption.
//! The wrapped DEK and the layout parameters are stored in a header at the
//! start of the file, so a file is readable by any dfdaemon holding the KEK.
//!
//! The pieces of the content are encrypted independently and written to
//! disjoint regions of the file, so the piece-based download, the concurrent
//! piece writes, the partial downloads and the random-access range reads of
//! the plaintext storage keep working unchanged. The plaintext digest of a
//! piece is still calculated and verified, so the P2P protocol is unaffected.
//!
//! This protects the cached content on disk only: authorized peers still
//! receive the plaintext over the transport, and hard links between the cache
//! and the output paths are refused in favor of decrypting copies.

use aws_lc_rs::aead::{Aad, Nonce, RandomizedNonceKey, Tag, AES_256_GCM};
use dashmap::DashMap;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::buffer_pool::BufferPool;
use dragonfly_client_util::fs::fallocate;
use serde::{Deserialize, Serialize};
use std::cmp::min;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{debug, error, warn};
use zeroize::Zeroizing;

pub mod io;
pub mod kms;
pub mod layout;

pub use io::{write_encrypted_range, EncryptedRangeReader};
pub use kms::{DataKey, KeyProvider, WrappedKey};
pub use layout::{Layout, DEFAULT_CHUNK_SIZE, HEADER_LEN};

/// The magic bytes at the start of an encrypted task content file.
const MAGIC: &[u8; 8] = b"DFLYENC\0";

/// The version of the encrypted file format.
const FORMAT_VERSION: u16 = 1;

/// The length of the magic and the length prefix of the header.
const HEADER_PREFIX_LEN: usize = MAGIC.len() + std::mem::size_of::<u32>();

/// Header is the header of an encrypted task content file, holding the layout
/// parameters and the wrapped data encryption key. It is serialized with
/// bincode behind the magic and a length prefix, and padded to `HEADER_LEN`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Header {
    /// The version of the encrypted file format.
    version: u16,

    /// The length of the plaintext of a chunk.
    chunk_size: u64,

    /// The length of the plaintext of a piece.
    piece_length: u64,

    /// The length of the plaintext content.
    content_length: u64,

    /// The identifier of the key encryption key that wrapped the data key.
    key_id: String,

    /// The wrapped data encryption key.
    wrapped_key: Vec<u8>,
}

/// Implements the header.
impl Header {
    /// Returns the layout described by the header.
    fn layout(&self) -> Result<Layout> {
        Layout::new(self.chunk_size, self.piece_length, self.content_length)
    }

    /// Returns the context binding the wrapped data key to the identity of the
    /// content file and to the layout parameters of the header, used as the
    /// associated data of the wrapping. A file moved to another task or
    /// another kind of task fails to unwrap its data key.
    fn context(&self, identity: &str) -> Result<Vec<u8>> {
        bincode::serialize(&(
            identity,
            self.version,
            self.chunk_size,
            self.piece_length,
            self.content_length,
        ))
        .map_err(|err| Error::Encryption(format!("serialize header context failed: {err}")))
    }

    /// Encodes the header into `HEADER_LEN` bytes.
    fn encode(&self) -> Result<Vec<u8>> {
        let body = bincode::serialize(self)
            .map_err(|err| Error::Encryption(format!("serialize header failed: {err}")))?;
        if HEADER_PREFIX_LEN + body.len() > HEADER_LEN as usize {
            return Err(Error::Encryption("header is too long".to_string()));
        }

        let mut bytes = Vec::with_capacity(HEADER_LEN as usize);
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&body);
        bytes.resize(HEADER_LEN as usize, 0);
        Ok(bytes)
    }

    /// Decodes the header from the start of a file, returning `None` when the
    /// file is not an encrypted task content file.
    fn decode(bytes: &[u8]) -> Result<Option<Self>> {
        if !is_encrypted(bytes) {
            return Ok(None);
        }

        if bytes.len() < HEADER_LEN as usize {
            return Err(Error::Encryption("header is truncated".to_string()));
        }

        let length = u32::from_le_bytes(bytes[MAGIC.len()..HEADER_PREFIX_LEN].try_into()?) as usize;
        if HEADER_PREFIX_LEN + length > HEADER_LEN as usize {
            return Err(Error::Encryption("header length is invalid".to_string()));
        }

        let header: Header =
            bincode::deserialize(&bytes[HEADER_PREFIX_LEN..HEADER_PREFIX_LEN + length])
                .map_err(|err| Error::Encryption(format!("deserialize header failed: {err}")))?;
        if header.version != FORMAT_VERSION {
            return Err(Error::Encryption(format!(
                "unsupported encrypted file format version {}",
                header.version
            )));
        }

        Ok(Some(header))
    }
}

/// Returns whether the start of a file marks an encrypted task content file.
fn is_encrypted(bytes: &[u8]) -> bool {
    bytes.len() >= MAGIC.len() && &bytes[..MAGIC.len()] == MAGIC
}

/// TaskCipher encrypts and decrypts the chunks of one task with its data
/// encryption key.
pub struct TaskCipher {
    /// The AES-256-GCM data encryption key with random nonces.
    key: RandomizedNonceKey,

    /// The layout of the task content.
    layout: Layout,
}

/// Implements the task cipher.
impl TaskCipher {
    /// Creates a new task cipher from the data key and the layout.
    pub fn new(data_key: &DataKey, layout: Layout) -> Result<Self> {
        Ok(Self {
            key: RandomizedNonceKey::new(&AES_256_GCM, data_key.as_ref())
                .map_err(|_| Error::Encryption("invalid data encryption key".to_string()))?,
            layout,
        })
    }

    /// Returns the layout of the task content.
    pub fn layout(&self) -> &Layout {
        &self.layout
    }

    /// Encrypts the plaintext of the chunk at `plaintext_offset` in place,
    /// returning the random nonce and the tag to store with it.
    pub fn seal_chunk(&self, plaintext_offset: u64, in_out: &mut [u8]) -> Result<(Nonce, Tag)> {
        self.key
            .seal_in_place_separate_tag(Aad::from(plaintext_offset.to_be_bytes()), in_out)
            .map_err(|_| {
                Error::Encryption(format!("encrypt chunk at offset {plaintext_offset} failed"))
            })
    }

    /// Decrypts and authenticates the `ciphertext || tag` of the chunk at
    /// `plaintext_offset` in place, returning the plaintext.
    pub fn open_chunk<'a>(
        &self,
        plaintext_offset: u64,
        nonce: &[u8; layout::NONCE_LEN as usize],
        in_out: &'a mut [u8],
    ) -> Result<&'a mut [u8]> {
        self.key
            .open_in_place(
                Nonce::assume_unique_for_key(*nonce),
                Aad::from(plaintext_offset.to_be_bytes()),
                in_out,
            )
            .map_err(|_| {
                Error::Encryption(format!(
                    "chunk at offset {plaintext_offset} failed to authenticate"
                ))
            })
    }
}

/// Encryptor manages the encryption at rest of the task content files of a
/// content storage: it creates the encrypted files with a fresh data key each,
/// wraps the data keys with the key provider and caches the unwrapped ciphers
/// of the open tasks.
pub struct Encryptor {
    /// The key provider wrapping and unwrapping the data keys.
    provider: Arc<dyn KeyProvider>,

    /// The ciphers of the tasks by content path.
    ciphers: DashMap<PathBuf, Arc<TaskCipher>>,
}

/// Implements the encryptor.
impl Encryptor {
    /// Creates the encryptor of the configuration, or `None` when the
    /// encryption at rest is disabled.
    pub async fn from_config(config: &Config) -> Result<Option<Self>> {
        if !config.storage.encryption.enable {
            return Ok(None);
        }

        let provider = kms::new_key_provider(config).await?;
        Ok(Some(Self::new(provider)))
    }

    /// Creates a new encryptor with the key provider.
    pub fn new(provider: Arc<dyn KeyProvider>) -> Self {
        Self {
            provider,
            ciphers: DashMap::new(),
        }
    }

    /// Returns the key provider.
    pub fn provider(&self) -> &Arc<dyn KeyProvider> {
        &self.provider
    }

    /// Returns whether the file is an encrypted task content file.
    pub async fn is_encrypted_file(path: &Path) -> Result<bool> {
        let mut f = fs::File::open(path).await?;
        let mut magic = [0u8; MAGIC.len()];
        match f.read_exact(&mut magic).await {
            Ok(_) => Ok(is_encrypted(&magic)),
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    /// Creates the encrypted content file of a task at `path` with a fresh data
    /// key, preallocated to hold the whole encrypted content. The header is
    /// written to a temporary file renamed into place, so the file never
    /// exists without a valid header.
    pub async fn create_file(
        &self,
        path: &Path,
        piece_length: u64,
        content_length: u64,
    ) -> Result<()> {
        let layout = Layout::new(DEFAULT_CHUNK_SIZE, piece_length, content_length)?;

        let mut data_key = DataKey::new([0u8; kms::KEY_LEN]);
        aws_lc_rs::rand::fill(data_key.as_mut())
            .map_err(|_| Error::Encryption("generate data encryption key failed".to_string()))?;

        let mut header = Header {
            version: FORMAT_VERSION,
            chunk_size: layout.chunk_size,
            piece_length: layout.piece_length,
            content_length: layout.content_length,
            key_id: String::new(),
            wrapped_key: Vec::new(),
        };
        let wrapped_key = self
            .provider
            .wrap(&data_key, &header.context(&file_identity(path)?)?)
            .await?;
        header.key_id = wrapped_key.key_id;
        header.wrapped_key = wrapped_key.ciphertext;
        let bytes = header.encode()?;

        let tmp_path = temporary_path(path)?;
        let mut f = fs::File::create(&tmp_path).await.inspect_err(|err| {
            error!("create {:?} failed: {}", tmp_path, err);
        })?;

        let result = async {
            f.write_all(&bytes).await?;
            fallocate(&f, layout.file_length()).await?;
            f.sync_all().await?;
            fs::rename(&tmp_path, path).await?;
            Ok::<(), Error>(())
        }
        .await;

        if let Err(err) = result {
            error!("create encrypted {:?} failed: {}", path, err);
            fs::remove_file(&tmp_path).await.unwrap_or_else(|err| {
                warn!("remove {:?} failed: {}", tmp_path, err);
            });
            return Err(err);
        }

        self.ciphers.insert(
            path.to_path_buf(),
            Arc::new(TaskCipher::new(&data_key, layout)?),
        );
        debug!(
            "created encrypted {:?} with key encryption key {}",
            path, header.key_id
        );
        Ok(())
    }

    /// Returns the cipher of the task content file at `path`, reading its
    /// header and unwrapping its data key on the first use.
    pub async fn cipher(&self, path: &Path) -> Result<Arc<TaskCipher>> {
        if let Some(cipher) = self.ciphers.get(path) {
            return Ok(cipher.clone());
        }

        let cipher = Arc::new(self.load_cipher(path).await?);
        Ok(self
            .ciphers
            .entry(path.to_path_buf())
            .or_insert(cipher)
            .clone())
    }

    /// Loads the cipher of the task content file at `path` from its header,
    /// unwrapping its data key with the key provider.
    async fn load_cipher(&self, path: &Path) -> Result<TaskCipher> {
        let header = read_header(path).await?.ok_or_else(|| {
            Error::Encryption(format!("{path:?} is not an encrypted task content"))
        })?;

        let wrapped_key = WrappedKey {
            key_id: header.key_id.clone(),
            ciphertext: header.wrapped_key.clone(),
        };
        let data_key = self
            .provider
            .unwrap(&wrapped_key, &header.context(&file_identity(path)?)?)
            .await?;
        TaskCipher::new(&data_key, header.layout()?)
    }

    /// Forgets the cipher of the task content file at `path`.
    pub fn evict(&self, path: &Path) {
        self.ciphers.remove(path);
    }

    /// Returns whether the task content file at `path` is encrypted and its
    /// data key is unwrappable with the key provider, i.e. it is readable. The
    /// cipher is not cached, so checking many tasks at startup stays cheap.
    pub async fn is_readable(&self, path: &Path) -> Result<bool> {
        if self.ciphers.contains_key(path) {
            return Ok(true);
        }

        if !Self::is_encrypted_file(path).await? {
            return Ok(false);
        }

        match self.load_cipher(path).await {
            Ok(_) => Ok(true),
            Err(Error::Encryption(err)) => {
                warn!("encrypted {:?} is not readable: {}", path, err);
                Ok(false)
            }
            Err(err) => Err(err),
        }
    }

    /// Decrypts the whole content of the task content file at `path` to the
    /// file at `to`, created or truncated, and returns the number of plaintext
    /// bytes copied.
    pub async fn copy_file(
        &self,
        path: &Path,
        to: &Path,
        buffer_size: usize,
        buffer_pool: &BufferPool,
    ) -> Result<u64> {
        let cipher = self.cipher(path).await?;
        let layout = *cipher.layout();
        let fd = Arc::new(fs::File::open(path).await?.into_std().await);
        let mut to_file = fs::File::create(to).await?;

        let mut copied = 0;
        let mut offset = 0;
        while offset < layout.content_length {
            let length = min(layout.piece_length, layout.content_length - offset);
            let mut reader = EncryptedRangeReader::new(
                fd.clone(),
                cipher.clone(),
                offset,
                length,
                offset,
                length,
                buffer_size,
                buffer_pool.clone(),
            )?;
            copied += tokio::io::copy_buf(&mut reader, &mut to_file).await?;
            offset += length;
        }

        to_file.flush().await?;
        debug!("decrypt copy {:?} to {:?} success", path, to);
        Ok(copied)
    }
}

/// Reads the header of the task content file at `path`, returning `None` when
/// the file is not encrypted.
async fn read_header(path: &Path) -> Result<Option<Header>> {
    let mut f = fs::File::open(path).await?;
    let mut bytes = Zeroizing::new(vec![0u8; HEADER_LEN as usize]);
    let mut filled = 0;
    while filled < bytes.len() {
        let n = f.read(&mut bytes[filled..]).await?;
        if n == 0 {
            break;
        }

        filled += n;
    }

    Header::decode(&bytes[..filled])
}

/// Returns the identity of a content file bound to its data key: the kind of
/// task and the task id, i.e. the directory of the kind and the file name of
/// the path `<kind>/<prefix>/<task id>` laid out by the content storage.
fn file_identity(path: &Path) -> Result<String> {
    let mut components = path
        .components()
        .rev()
        .filter_map(|component| component.as_os_str().to_str());
    let task_id = components
        .next()
        .ok_or_else(|| Error::Encryption(format!("invalid content path {path:?}")))?;
    let kind = components.nth(1).unwrap_or_default();
    Ok(format!("{kind}/{task_id}"))
}

/// Returns the path of the temporary file the content file is created as.
fn temporary_path(path: &Path) -> Result<PathBuf> {
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| Error::Encryption(format!("invalid content path {path:?}")))?;
    Ok(path.with_file_name(format!("{name}.tmp")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::kms::FileKeyProvider;
    use crate::encryption::layout::CHUNK_OVERHEAD;
    use std::fs::File;
    use std::io::Cursor;
    use std::os::unix::fs::FileExt;
    use tempfile::tempdir;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt};

    fn data_key(byte: u8) -> DataKey {
        DataKey::new([byte; kms::KEY_LEN])
    }

    fn encryptor() -> Encryptor {
        Encryptor::new(Arc::new(
            FileKeyProvider::from_keys(&[data_key(1)]).unwrap(),
        ))
    }

    fn buffer_pool() -> BufferPool {
        BufferPool::new(64 * 1024 * 1024)
    }

    fn pattern(length: usize) -> Vec<u8> {
        (0..length).map(|i| (i % 251) as u8).collect()
    }

    async fn open(path: &Path) -> Arc<File> {
        Arc::new(
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .open(path)
                .unwrap(),
        )
    }

    /// Writes the whole content as pieces and returns the file descriptor.
    async fn write_content(
        encryptor: &Encryptor,
        path: &Path,
        piece_length: u64,
        content: &[u8],
        buffer_size: usize,
    ) -> Arc<File> {
        encryptor
            .create_file(path, piece_length, content.len() as u64)
            .await
            .unwrap();
        let cipher = encryptor.cipher(path).await.unwrap();
        let fd = open(path).await;
        for (number, piece) in content.chunks(piece_length as usize).enumerate() {
            let offset = number as u64 * piece_length;
            let response = write_encrypted_range(
                fd.clone(),
                cipher.clone(),
                offset,
                piece.len() as u64,
                buffer_size,
                &mut Cursor::new(piece.to_vec()),
                &buffer_pool(),
            )
            .await
            .unwrap();
            assert_eq!(response.length, piece.len() as u64);
            assert_eq!(response.hash, crc32fast::hash(piece).to_string());
        }

        fd
    }

    #[test]
    fn test_header_roundtrip() {
        let header = Header {
            version: FORMAT_VERSION,
            chunk_size: DEFAULT_CHUNK_SIZE,
            piece_length: 4 * 1024 * 1024,
            content_length: 10 * 1024 * 1024 + 3,
            key_id: "0123456789abcdef".to_string(),
            wrapped_key: vec![7u8; 60],
        };

        let bytes = header.encode().unwrap();
        assert_eq!(bytes.len(), HEADER_LEN as usize);
        assert_eq!(Header::decode(&bytes).unwrap(), Some(header.clone()));

        // A plaintext file has no header.
        assert_eq!(Header::decode(b"hello, world!").unwrap(), None);
        assert_eq!(Header::decode(b"").unwrap(), None);

        // A truncated or corrupted header is an error.
        assert!(Header::decode(&bytes[..100]).is_err());
        let mut corrupted = bytes.clone();
        corrupted[HEADER_PREFIX_LEN + 1] ^= 0xff;
        assert!(Header::decode(&corrupted).is_err());
        let mut unsupported = header.clone();
        unsupported.version = 2;
        assert!(Header::decode(&unsupported.encode().unwrap()).is_err());

        // The context binds the identity of the file and the layout parameters.
        let mut other = header.clone();
        other.content_length += 1;
        assert_ne!(
            header.context("tasks/a").unwrap(),
            other.context("tasks/a").unwrap()
        );
        assert_ne!(
            header.context("tasks/a").unwrap(),
            header.context("tasks/b").unwrap()
        );
        assert_ne!(
            header.context("tasks/a").unwrap(),
            header.context("persistent-tasks/a").unwrap()
        );
    }

    #[test]
    fn test_file_identity() {
        assert_eq!(
            file_identity(Path::new("/var/lib/dragonfly/content/tasks/abc/abcdef")).unwrap(),
            "tasks/abcdef"
        );
        assert_eq!(
            file_identity(Path::new("content/persistent-tasks/abc/abcdef")).unwrap(),
            "persistent-tasks/abcdef"
        );
        assert_eq!(file_identity(Path::new("abcdef")).unwrap(), "/abcdef");
    }

    #[tokio::test]
    async fn test_file_identity_is_bound_to_the_data_key() {
        let dir = tempdir().unwrap();
        let encryptor = encryptor();
        let tasks = dir.path().join("tasks").join("abc");
        let persistent = dir.path().join("persistent-tasks").join("abc");
        fs::create_dir_all(&tasks).await.unwrap();
        fs::create_dir_all(&persistent).await.unwrap();

        let path = tasks.join("abcdef");
        encryptor.create_file(&path, 100, 250).await.unwrap();
        encryptor.evict(&path);
        assert!(encryptor.is_readable(&path).await.unwrap());

        // The same file under another task id or another kind of task is not
        // readable, as its data key is bound to the original identity.
        let other = tasks.join("abcxyz");
        fs::rename(&path, &other).await.unwrap();
        assert!(!encryptor.is_readable(&other).await.unwrap());
        assert!(encryptor.cipher(&other).await.is_err());

        let other_kind = persistent.join("abcdef");
        fs::rename(&other, &other_kind).await.unwrap();
        assert!(!encryptor.is_readable(&other_kind).await.unwrap());

        // Moved back, it reads again.
        fs::rename(&other_kind, &path).await.unwrap();
        assert!(encryptor.is_readable(&path).await.unwrap());
    }

    #[tokio::test]
    async fn test_create_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("task");
        let encryptor = encryptor();
        encryptor.create_file(&path, 1024, 2500).await.unwrap();

        assert!(Encryptor::is_encrypted_file(&path).await.unwrap());
        assert!(!dir.path().join("task.tmp").exists());
        let layout = Layout::new(DEFAULT_CHUNK_SIZE, 1024, 2500).unwrap();
        assert_eq!(
            fs::metadata(&path).await.unwrap().len(),
            layout.file_length()
        );

        // The cipher is cached by the creation and reloaded from the header
        // by a fresh encryptor holding the key encryption key.
        let cipher = encryptor.cipher(&path).await.unwrap();
        assert_eq!(cipher.layout(), &layout);
        encryptor.evict(&path);
        let cipher = encryptor.cipher(&path).await.unwrap();
        assert_eq!(cipher.layout(), &layout);
        assert!(encryptor.is_readable(&path).await.unwrap());

        // Another key encryption key cannot unwrap the data key.
        let other = Encryptor::new(Arc::new(
            FileKeyProvider::from_keys(&[data_key(2)]).unwrap(),
        ));
        assert!(other.cipher(&path).await.is_err());
        assert!(!other.is_readable(&path).await.unwrap());

        // A plaintext file is neither encrypted nor readable.
        let plaintext = dir.path().join("plaintext");
        fs::write(&plaintext, b"hello, world!").await.unwrap();
        assert!(!Encryptor::is_encrypted_file(&plaintext).await.unwrap());
        assert!(!encryptor.is_readable(&plaintext).await.unwrap());
        assert!(encryptor.cipher(&plaintext).await.is_err());

        let empty = dir.path().join("empty");
        fs::write(&empty, b"").await.unwrap();
        assert!(!Encryptor::is_encrypted_file(&empty).await.unwrap());

        assert!(Encryptor::is_encrypted_file(&dir.path().join("missing"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn test_write_and_read_pieces() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("task");
        let encryptor = encryptor();

        // Pieces of two and a half chunks, with a short last piece, written
        // with a buffer of one chunk to exercise the batching.
        let chunk = DEFAULT_CHUNK_SIZE as usize;
        let piece_length = (2 * chunk + chunk / 2) as u64;
        let content = pattern(3 * piece_length as usize + 123);
        let fd = write_content(&encryptor, &path, piece_length, &content, chunk).await;
        let cipher = encryptor.cipher(&path).await.unwrap();

        // The ciphertext never contains the plaintext.
        let file = fs::read(&path).await.unwrap();
        assert!(!file
            .windows(64)
            .any(|window| content.windows(64).next() == Some(window)));

        // Read the whole pieces back with various buffer sizes.
        for buffer_size in [1, chunk - 1, chunk, 3 * chunk, 1024 * 1024] {
            for (number, piece) in content.chunks(piece_length as usize).enumerate() {
                let offset = number as u64 * piece_length;
                let mut reader = EncryptedRangeReader::new(
                    fd.clone(),
                    cipher.clone(),
                    offset,
                    piece.len() as u64,
                    offset,
                    piece.len() as u64,
                    buffer_size,
                    buffer_pool(),
                )
                .unwrap();
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await.unwrap();
                assert_eq!(buffer, piece, "piece {number} buffer size {buffer_size}");
            }
        }
    }

    #[tokio::test]
    async fn test_read_ranges() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("task");
        let encryptor = encryptor();

        let chunk = DEFAULT_CHUNK_SIZE as usize;
        let piece_length = (3 * chunk) as u64;
        let content = pattern(2 * piece_length as usize + 77);
        let fd = write_content(&encryptor, &path, piece_length, &content, 4 * chunk).await;
        let cipher = encryptor.cipher(&path).await.unwrap();

        // Random-access ranges within a piece: inside one chunk, across chunk
        // boundaries, at the start and the end of the piece, and empty.
        let piece_offset = piece_length;
        let piece = &content[piece_offset as usize..2 * piece_offset as usize];
        let ranges = [
            (0, 1),
            (5, 100),
            (chunk - 10, 20),
            (chunk, chunk),
            (chunk + 1, 2 * chunk - 2),
            (0, 3 * chunk),
            (3 * chunk - 1, 1),
            (chunk / 2, 0),
        ];
        for (start, length) in ranges {
            for buffer_size in [1, chunk, 2 * chunk] {
                let mut reader = EncryptedRangeReader::new(
                    fd.clone(),
                    cipher.clone(),
                    piece_offset,
                    piece_length,
                    piece_offset + start as u64,
                    length as u64,
                    buffer_size,
                    buffer_pool(),
                )
                .unwrap();

                let (span_offset, span_length) = reader.ciphertext_span();
                if length > 0 {
                    assert!(span_offset >= HEADER_LEN);
                    assert!(span_length as usize >= length);
                }

                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await.unwrap();
                assert_eq!(
                    buffer,
                    &piece[start..start + length],
                    "range {start}+{length} buffer size {buffer_size}"
                );
            }
        }

        // The last, short piece.
        let last_offset = 2 * piece_length;
        let last = &content[last_offset as usize..];
        let mut reader = EncryptedRangeReader::new(
            fd.clone(),
            cipher.clone(),
            last_offset,
            last.len() as u64,
            last_offset + 10,
            last.len() as u64 - 10,
            chunk,
            buffer_pool(),
        )
        .unwrap();
        let mut buffer = Vec::new();
        reader.read_to_end(&mut buffer).await.unwrap();
        assert_eq!(buffer, &last[10..]);

        // Ranges outside the piece are rejected.
        assert!(EncryptedRangeReader::new(
            fd.clone(),
            cipher.clone(),
            piece_offset,
            piece_length,
            piece_offset - 1,
            2,
            chunk,
            buffer_pool()
        )
        .is_err());
        assert!(EncryptedRangeReader::new(
            fd.clone(),
            cipher.clone(),
            piece_offset,
            piece_length,
            piece_offset,
            piece_length + 1,
            chunk,
            buffer_pool()
        )
        .is_err());
        assert!(EncryptedRangeReader::new(
            fd.clone(),
            cipher.clone(),
            piece_offset + 1,
            piece_length,
            piece_offset + 1,
            1,
            chunk,
            buffer_pool()
        )
        .is_err());
    }

    #[tokio::test]
    async fn test_read_chunk_and_fill_buf() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("task");
        let encryptor = encryptor();

        let chunk = DEFAULT_CHUNK_SIZE as usize;
        let content = pattern(2 * chunk + 100);
        let fd = write_content(&encryptor, &path, content.len() as u64, &content, chunk).await;
        let cipher = encryptor.cipher(&path).await.unwrap();

        // read_chunk returns the plaintext a fill at a time, honoring the
        // skipped start of the range, and mixes with the buffered reads.
        let mut reader = EncryptedRangeReader::new(
            fd.clone(),
            cipher.clone(),
            0,
            content.len() as u64,
            10,
            content.len() as u64 - 10,
            chunk,
            buffer_pool(),
        )
        .unwrap();
        let first = reader.fill_buf().await.unwrap().to_vec();
        assert_eq!(first, &content[10..chunk]);
        reader.consume(5);

        let mut collected = content[10..15].to_vec();
        loop {
            let bytes = reader.read_chunk().await.unwrap();
            if bytes.is_empty() {
                break;
            }
            collected.extend_from_slice(&bytes);
        }
        assert_eq!(collected, &content[10..]);
        assert!(reader.read_chunk().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_tampering_is_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("task");
        let encryptor = encryptor();

        let chunk = DEFAULT_CHUNK_SIZE as usize;
        let content = pattern(2 * chunk);
        let fd = write_content(&encryptor, &path, content.len() as u64, &content, chunk).await;
        let cipher = encryptor.cipher(&path).await.unwrap();

        let content_length = content.len() as u64;
        let read_all = |offset: u64, length: u64| {
            let fd = fd.clone();
            let cipher = cipher.clone();
            async move {
                let mut reader = EncryptedRangeReader::new(
                    fd,
                    cipher,
                    0,
                    content_length,
                    offset,
                    length,
                    chunk,
                    buffer_pool(),
                )
                .unwrap();
                let mut buffer = Vec::new();
                reader.read_to_end(&mut buffer).await.map(|_| buffer)
            }
        };

        // Flip a bit of the ciphertext of the second chunk: the first chunk
        // still reads, the second fails to authenticate.
        let second_chunk = HEADER_LEN + DEFAULT_CHUNK_SIZE + CHUNK_OVERHEAD;
        let mut byte = [0u8; 1];
        fd.read_exact_at(&mut byte, second_chunk + 20).unwrap();
        fd.write_all_at(&[byte[0] ^ 1], second_chunk + 20).unwrap();
        assert_eq!(read_all(0, chunk as u64).await.unwrap(), &content[..chunk]);
        assert!(read_all(chunk as u64, 1).await.is_err());
        assert!(read_all(0, content_length).await.is_err());
        fd.write_all_at(&byte, second_chunk + 20).unwrap();
        assert_eq!(read_all(0, content_length).await.unwrap(), content);

        // Swapping two chunks is detected by the associated data.
        let mut first = vec![0u8; (DEFAULT_CHUNK_SIZE + CHUNK_OVERHEAD) as usize];
        let mut second = first.clone();
        fd.read_exact_at(&mut first, HEADER_LEN).unwrap();
        fd.read_exact_at(&mut second, second_chunk).unwrap();
        fd.write_all_at(&second, HEADER_LEN).unwrap();
        fd.write_all_at(&first, second_chunk).unwrap();
        assert!(read_all(0, chunk as u64).await.is_err());
        assert!(read_all(chunk as u64, chunk as u64).await.is_err());
    }

    #[tokio::test]
    async fn test_write_errors() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("task");
        let encryptor = encryptor();
        encryptor.create_file(&path, 100, 250).await.unwrap();
        let cipher = encryptor.cipher(&path).await.unwrap();
        let fd = open(&path).await;

        // A short reader is a length mismatch.
        let result = write_encrypted_range(
            fd.clone(),
            cipher.clone(),
            0,
            100,
            1024,
            &mut Cursor::new(vec![1u8; 50]),
            &buffer_pool(),
        )
        .await;
        assert!(result.is_err());

        // Only the first `expected_length` bytes of a long reader are written.
        let response = write_encrypted_range(
            fd.clone(),
            cipher.clone(),
            100,
            100,
            1024,
            &mut Cursor::new(vec![2u8; 150]),
            &buffer_pool(),
        )
        .await
        .unwrap();
        assert_eq!(response.length, 100);

        // Ranges that are not a piece are rejected.
        assert!(write_encrypted_range(
            fd.clone(),
            cipher.clone(),
            50,
            100,
            1024,
            &mut Cursor::new(vec![3u8; 100]),
            &buffer_pool(),
        )
        .await
        .is_err());
        assert!(write_encrypted_range(
            fd.clone(),
            cipher.clone(),
            200,
            100,
            1024,
            &mut Cursor::new(vec![3u8; 100]),
            &buffer_pool(),
        )
        .await
        .is_err());

        // A zero length write of an empty content succeeds.
        let empty = dir.path().join("empty");
        encryptor.create_file(&empty, 0, 0).await.unwrap();
        let cipher = encryptor.cipher(&empty).await.unwrap();
        let response = write_encrypted_range(
            open(&empty).await,
            cipher,
            0,
            0,
            1024,
            &mut Cursor::new(Vec::new()),
            &buffer_pool(),
        )
        .await
        .unwrap();
        assert_eq!(response.length, 0);
    }

    #[tokio::test]
    async fn test_concurrent_piece_writes() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("task");
        let encryptor = Arc::new(encryptor());

        let chunk = DEFAULT_CHUNK_SIZE as usize;
        let piece_length = (chunk + chunk / 3) as u64;
        let content = Arc::new(pattern(8 * piece_length as usize - 1));
        encryptor
            .create_file(&path, piece_length, content.len() as u64)
            .await
            .unwrap();
        let cipher = encryptor.cipher(&path).await.unwrap();
        let fd = open(&path).await;

        // Write the pieces concurrently and out of order.
        let mut handles = Vec::new();
        for number in (0..8).rev() {
            let fd = fd.clone();
            let cipher = cipher.clone();
            let content = content.clone();
            handles.push(tokio::spawn(async move {
                let offset = number as u64 * piece_length;
                let end = min(offset + piece_length, content.len() as u64);
                let piece = content[offset as usize..end as usize].to_vec();
                write_encrypted_range(
                    fd,
                    cipher,
                    offset,
                    piece.len() as u64,
                    chunk,
                    &mut Cursor::new(piece),
                    &buffer_pool(),
                )
                .await
                .unwrap()
            }));
        }
        for handle in handles {
            handle.await.unwrap();
        }

        // A fresh encryptor decrypts the whole content.
        let other = encryptor.clone();
        other.evict(&path);
        let to = dir.path().join("plaintext");
        let copied = other
            .copy_file(&path, &to, 1024 * 1024, &buffer_pool())
            .await
            .unwrap();
        assert_eq!(copied, content.len() as u64);
        assert_eq!(fs::read(&to).await.unwrap(), *content);
    }

    #[tokio::test]
    async fn test_copy_file() {
        let dir = tempdir().unwrap();
        let encryptor = encryptor();

        // An empty content copies to an empty file.
        let path = dir.path().join("empty");
        encryptor.create_file(&path, 0, 0).await.unwrap();
        let to = dir.path().join("empty-plaintext");
        assert_eq!(
            encryptor
                .copy_file(&path, &to, 1024, &buffer_pool())
                .await
                .unwrap(),
            0
        );
        assert_eq!(fs::read(&to).await.unwrap(), b"");

        // A partially written content fails to copy instead of producing
        // garbage.
        let path = dir.path().join("partial");
        let content = pattern(300);
        encryptor.create_file(&path, 100, 300).await.unwrap();
        let cipher = encryptor.cipher(&path).await.unwrap();
        write_encrypted_range(
            open(&path).await,
            cipher,
            0,
            100,
            1024,
            &mut Cursor::new(content[..100].to_vec()),
            &buffer_pool(),
        )
        .await
        .unwrap();
        let to = dir.path().join("partial-plaintext");
        assert!(encryptor
            .copy_file(&path, &to, 1024, &buffer_pool())
            .await
            .is_err());
    }
}
