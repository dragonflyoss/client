/*
 *     Copyright 2026 The Dragonfly Authors
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

use chacha20poly1305::aead::{AeadInPlace, KeyInit, OsRng};
use chacha20poly1305::{XChaCha20Poly1305, XNonce};
use dashmap::DashMap;
use dragonfly_client_config::dfdaemon::StorageEncryption;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::fs::fd::{WriteFD, DEFAULT_FD_CACHE_CAPACITY};
use lru::LruCache;
use rand_core_compat::RngCore;
use std::cmp::min;
use std::fs::File;
use std::io;
use std::num::NonZeroUsize;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, RwLock};

const MAGIC: &[u8; 8] = b"DFENC001";
const HEADER_SIZE: u64 = 48;
const NONCE_SIZE: usize = 24;
const TAG_SIZE: usize = 16;
const LENGTH_SIZE: u64 = 4;
const RECORD_OVERHEAD: u64 = NONCE_SIZE as u64 + LENGTH_SIZE + TAG_SIZE as u64;
pub(crate) const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

mod rand_core_compat {
    pub use chacha20poly1305::aead::rand_core::RngCore;
}

#[derive(Clone)]
pub(crate) struct Encryption {
    cipher: Arc<XChaCha20Poly1305>,
    chunk_size: usize,
    locks: Arc<DashMap<PathBuf, Arc<RwLock<()>>>>,
    write_fds: Arc<Mutex<LruCache<PathBuf, WriteFD>>>,
}

struct Header {
    bytes: [u8; HEADER_SIZE as usize],
    content_length: u64,
}

impl Encryption {
    pub(crate) fn from_config(config: &StorageEncryption) -> Result<Option<Arc<Self>>> {
        if !config.enable {
            return Ok(None);
        }

        let key = match (&config.key_file, &config.key_env) {
            (Some(path), None) => {
                let bytes = std::fs::read(path).map_err(|err| {
                    Error::Unknown(format!(
                        "failed to read storage encryption key file {}: {err}",
                        path.display()
                    ))
                })?;
                decode_key(&bytes)?
            }
            (None, Some(name)) => {
                let value = std::env::var(name).map_err(|_| {
                    Error::Unknown(format!(
                        "storage encryption key environment variable {name} is not set"
                    ))
                })?;
                decode_key(value.as_bytes())?
            }
            _ => {
                return Err(Error::Unknown(
                    "storage encryption requires exactly one of keyFile or keyEnv".to_string(),
                ))
            }
        };

        Ok(Some(Arc::new(Self::new(key, DEFAULT_CHUNK_SIZE))))
    }

    fn new(key: [u8; 32], chunk_size: usize) -> Self {
        let cipher = XChaCha20Poly1305::new_from_slice(&key)
            .expect("a 32-byte key is valid for XChaCha20-Poly1305");
        Self {
            cipher: Arc::new(cipher),
            chunk_size,
            locks: Arc::new(DashMap::new()),
            write_fds: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(DEFAULT_FD_CACHE_CAPACITY).unwrap(),
            ))),
        }
    }

    pub(crate) fn chunk_size(&self) -> usize {
        self.chunk_size
    }

    pub(crate) fn encrypted_length(&self, content_length: u64) -> io::Result<u64> {
        let chunks = content_length.div_ceil(self.chunk_size as u64);
        HEADER_SIZE
            .checked_add(
                chunks
                    .checked_mul(self.record_size())
                    .ok_or_else(|| invalid_input("encrypted content length overflow"))?,
            )
            .ok_or_else(|| invalid_input("encrypted content length overflow"))
    }

    pub(crate) fn physical_range(&self, offset: u64, length: u64) -> io::Result<(u64, u64)> {
        if length == 0 {
            return Ok((0, 0));
        }

        let end = offset
            .checked_add(length)
            .ok_or_else(|| invalid_input("plaintext range overflow"))?;
        let first_chunk = offset / self.chunk_size as u64;
        let last_chunk = (end - 1) / self.chunk_size as u64;
        let physical_offset = self.record_offset(first_chunk)?;
        let physical_end = self.record_offset(
            last_chunk
                .checked_add(1)
                .ok_or_else(|| invalid_input("chunk index overflow"))?,
        )?;
        Ok((physical_offset, physical_end - physical_offset))
    }

    pub(crate) fn initialize(&self, fd: &File, content_length: u64) -> io::Result<()> {
        let mut bytes = [0u8; HEADER_SIZE as usize];
        bytes[..MAGIC.len()].copy_from_slice(MAGIC);
        bytes[8..12].copy_from_slice(&(self.chunk_size as u32).to_le_bytes());
        bytes[12..20].copy_from_slice(&content_length.to_le_bytes());
        OsRng.fill_bytes(&mut bytes[20..36]);
        fd.set_len(self.encrypted_length(content_length)?)?;
        fd.write_all_at(&bytes, 0)?;

        let header = Header {
            bytes,
            content_length,
        };
        for chunk_index in 0..content_length.div_ceil(self.chunk_size as u64) {
            let chunk_length = self.chunk_length(&header, chunk_index)?;
            let mut nonce = [0u8; NONCE_SIZE];
            OsRng.fill_bytes(&mut nonce);
            let aad = self.aad(&header, chunk_index, chunk_length);
            let mut plaintext = Vec::new();
            let tag = self
                .cipher
                .encrypt_in_place_detached(XNonce::from_slice(&nonce), &aad, &mut plaintext)
                .map_err(|_| invalid_data("failed to initialize encrypted content chunk"))?;
            let mut placeholder = Vec::with_capacity(NONCE_SIZE + LENGTH_SIZE as usize + TAG_SIZE);
            placeholder.extend_from_slice(&nonce);
            placeholder.extend_from_slice(&0u32.to_le_bytes());
            placeholder.extend_from_slice(&tag);
            fd.write_all_at(&placeholder, self.record_offset(chunk_index)?)?;
        }
        Ok(())
    }

    pub(crate) fn content_length(&self, fd: &File) -> io::Result<u64> {
        Ok(self.read_header(fd)?.content_length)
    }

    pub(crate) async fn open_write(&self, path: &Path) -> Result<WriteFD> {
        if let Some(fd) = self.write_fds.lock()?.get(path) {
            return Ok(fd.clone());
        }

        let file = tokio::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .truncate(false)
            .open(path)
            .await?;
        let fd = WriteFD::new(Arc::new(file.into_std().await));
        Ok(self
            .write_fds
            .lock()?
            .get_or_insert(path.to_path_buf(), || fd)
            .clone())
    }

    pub(crate) fn evict_write_fd(&self, path: &Path) -> Result<()> {
        self.write_fds.lock()?.pop(path);
        Ok(())
    }

    pub(crate) fn remove_cached_state(&self, path: &Path) {
        if let Err(err) = self.evict_write_fd(path) {
            tracing::error!(
                "remove {:?} from encrypted write fd cache failed: {}",
                path,
                err
            );
        }
        self.locks.remove(path);
    }

    pub(crate) fn write_at(
        &self,
        fd: &File,
        path: &Path,
        offset: u64,
        mut data: &[u8],
    ) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }

        let lock = self.lock(path);
        let _guard = lock
            .write()
            .map_err(|err| io::Error::other(err.to_string()))?;
        let header = self.read_header(fd)?;
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| invalid_input("plaintext write range overflow"))?;
        if end > header.content_length {
            return Err(invalid_input("plaintext write exceeds the content length"));
        }

        let mut offset = offset;
        while !data.is_empty() {
            let chunk_index = offset / self.chunk_size as u64;
            let chunk_offset = (offset % self.chunk_size as u64) as usize;
            let chunk_length = self.chunk_length(&header, chunk_index)?;
            let length = min(data.len(), chunk_length - chunk_offset);
            let full_chunk = chunk_offset == 0 && length == chunk_length;
            let mut plaintext = if full_chunk {
                data[..length].to_vec()
            } else {
                self.read_chunk(fd, &header, chunk_index)?
            };
            plaintext[chunk_offset..chunk_offset + length].copy_from_slice(&data[..length]);

            let mut nonce = [0u8; NONCE_SIZE];
            OsRng.fill_bytes(&mut nonce);
            let aad = self.aad(&header, chunk_index, chunk_length);
            let tag = self
                .cipher
                .encrypt_in_place_detached(XNonce::from_slice(&nonce), &aad, &mut plaintext)
                .map_err(|_| invalid_data("failed to encrypt content chunk"))?;

            let mut record = Vec::with_capacity(self.record_size() as usize);
            record.extend_from_slice(&nonce);
            record.extend_from_slice(&(chunk_length as u32).to_le_bytes());
            record.extend_from_slice(&plaintext);
            record.resize(NONCE_SIZE + LENGTH_SIZE as usize + self.chunk_size, 0);
            record.extend_from_slice(&tag);
            fd.write_all_at(&record, self.record_offset(chunk_index)?)?;

            offset += length as u64;
            data = &data[length..];
        }

        Ok(())
    }

    pub(crate) fn read_at(
        &self,
        fd: &File,
        path: &Path,
        offset: u64,
        length: u64,
    ) -> io::Result<Vec<u8>> {
        let lock = self.lock(path);
        let _guard = lock
            .read()
            .map_err(|err| io::Error::other(err.to_string()))?;
        let header = self.read_header(fd)?;
        if offset >= header.content_length || length == 0 {
            return Ok(Vec::new());
        }

        let end = min(
            offset
                .checked_add(length)
                .ok_or_else(|| invalid_input("plaintext read range overflow"))?,
            header.content_length,
        );
        let mut output = Vec::with_capacity((end - offset) as usize);
        let mut offset = offset;
        while offset < end {
            let chunk_index = offset / self.chunk_size as u64;
            let chunk_offset = (offset % self.chunk_size as u64) as usize;
            let plaintext = self.read_chunk(fd, &header, chunk_index)?;
            let length = min(plaintext.len() - chunk_offset, (end - offset) as usize);
            output.extend_from_slice(&plaintext[chunk_offset..chunk_offset + length]);
            offset += length as u64;
        }

        Ok(output)
    }

    fn lock(&self, path: &Path) -> Arc<RwLock<()>> {
        self.locks
            .entry(path.to_path_buf())
            .or_insert_with(|| Arc::new(RwLock::new(())))
            .clone()
    }

    fn record_size(&self) -> u64 {
        self.chunk_size as u64 + RECORD_OVERHEAD
    }

    fn record_offset(&self, chunk_index: u64) -> io::Result<u64> {
        HEADER_SIZE
            .checked_add(
                chunk_index
                    .checked_mul(self.record_size())
                    .ok_or_else(|| invalid_input("encrypted record offset overflow"))?,
            )
            .ok_or_else(|| invalid_input("encrypted record offset overflow"))
    }

    fn chunk_length(&self, header: &Header, chunk_index: u64) -> io::Result<usize> {
        let start = chunk_index
            .checked_mul(self.chunk_size as u64)
            .ok_or_else(|| invalid_data("encrypted chunk offset overflow"))?;
        if start >= header.content_length {
            return Err(invalid_data("encrypted chunk index is out of bounds"));
        }

        Ok(min(self.chunk_size as u64, header.content_length - start) as usize)
    }

    fn read_header(&self, fd: &File) -> io::Result<Header> {
        let mut bytes = [0u8; HEADER_SIZE as usize];
        read_exact_at(fd, &mut bytes, 0).map_err(|err| {
            if err.kind() == io::ErrorKind::UnexpectedEof {
                invalid_data("legacy plaintext or truncated content is not supported")
            } else {
                err
            }
        })?;
        if &bytes[..MAGIC.len()] != MAGIC {
            return Err(invalid_data(
                "legacy plaintext or unsupported encrypted content is not supported",
            ));
        }

        let chunk_size = u32::from_le_bytes(bytes[8..12].try_into().unwrap()) as usize;
        if chunk_size != self.chunk_size || bytes[36..].iter().any(|byte| *byte != 0) {
            return Err(invalid_data("invalid encrypted content header"));
        }

        let content_length = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
        let expected_length = self.encrypted_length(content_length)?;
        if fd.metadata()?.len() != expected_length {
            return Err(invalid_data(
                "encrypted content length does not match its header",
            ));
        }

        Ok(Header {
            bytes,
            content_length,
        })
    }

    fn read_chunk(&self, fd: &File, header: &Header, chunk_index: u64) -> io::Result<Vec<u8>> {
        let chunk_length = self.chunk_length(header, chunk_index)?;
        let mut record = vec![0u8; self.record_size() as usize];
        read_exact_at(fd, &mut record, self.record_offset(chunk_index)?)?;

        let stored_length = u32::from_le_bytes(
            record[NONCE_SIZE..NONCE_SIZE + LENGTH_SIZE as usize]
                .try_into()
                .unwrap(),
        ) as usize;
        let ciphertext_start = NONCE_SIZE + LENGTH_SIZE as usize;
        if stored_length == 0 {
            if record[ciphertext_start + TAG_SIZE..]
                .iter()
                .any(|byte| *byte != 0)
            {
                return Err(invalid_data("encrypted empty chunk is invalid"));
            }

            let mut plaintext = Vec::new();
            let aad = self.aad(header, chunk_index, chunk_length);
            self.cipher
                .decrypt_in_place_detached(
                    XNonce::from_slice(&record[..NONCE_SIZE]),
                    &aad,
                    &mut plaintext,
                    chacha20poly1305::Tag::from_slice(
                        &record[ciphertext_start..ciphertext_start + TAG_SIZE],
                    ),
                )
                .map_err(|_| invalid_data("encrypted content authentication failed"))?;
            return Ok(vec![0u8; chunk_length]);
        }
        if stored_length != chunk_length {
            return Err(invalid_data("encrypted chunk length is invalid"));
        }

        let mut plaintext = record[ciphertext_start..ciphertext_start + chunk_length].to_vec();
        let aad = self.aad(header, chunk_index, chunk_length);
        self.cipher
            .decrypt_in_place_detached(
                XNonce::from_slice(&record[..NONCE_SIZE]),
                &aad,
                &mut plaintext,
                chacha20poly1305::Tag::from_slice(
                    &record[self.record_size() as usize - TAG_SIZE..],
                ),
            )
            .map_err(|_| invalid_data("encrypted content authentication failed"))?;
        Ok(plaintext)
    }

    fn aad(&self, header: &Header, chunk_index: u64, chunk_length: usize) -> Vec<u8> {
        let mut aad = Vec::with_capacity(HEADER_SIZE as usize + 12);
        aad.extend_from_slice(&header.bytes);
        aad.extend_from_slice(&chunk_index.to_le_bytes());
        aad.extend_from_slice(&(chunk_length as u32).to_le_bytes());
        aad
    }
}

pub(super) fn ensure_plaintext(fd: &File) -> io::Result<()> {
    let mut header = [0u8; HEADER_SIZE as usize];
    if let Err(err) = read_exact_at(fd, &mut header, 0) {
        if err.kind() == io::ErrorKind::UnexpectedEof {
            return Ok(());
        }
        return Err(err);
    }

    let chunk_size = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;
    if &header[..MAGIC.len()] == MAGIC
        && chunk_size == DEFAULT_CHUNK_SIZE
        && header[36..].iter().all(|byte| *byte == 0)
    {
        return Err(invalid_data(
            "encrypted content requires storage encryption to be enabled",
        ));
    }
    Ok(())
}

fn decode_key(bytes: &[u8]) -> Result<[u8; 32]> {
    if bytes.len() == 32 {
        return bytes.try_into().map_err(|_| {
            Error::Unknown("storage encryption key must contain exactly 32 bytes".to_string())
        });
    }

    let encoded = std::str::from_utf8(bytes)
        .map_err(|_| Error::Unknown("storage encryption key is not valid hexadecimal".to_string()))?
        .trim();
    let decoded = hex::decode(encoded).map_err(|_| {
        Error::Unknown("storage encryption key is not valid hexadecimal".to_string())
    })?;
    decoded.try_into().map_err(|_| {
        Error::Unknown("storage encryption key must decode to exactly 32 bytes".to_string())
    })
}

fn read_exact_at(fd: &File, mut buffer: &mut [u8], mut offset: u64) -> io::Result<()> {
    while !buffer.is_empty() {
        match fd.read_at(buffer, offset) {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "failed to fill whole buffer",
                ))
            }
            Ok(n) => {
                offset += n as u64;
                buffer = &mut buffer[n..];
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => {}
            Err(err) => return Err(err),
        }
    }

    Ok(())
}

fn invalid_data(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn invalid_input(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_client_util::fs::fd::WritePriority;
    use std::collections::HashSet;
    use std::fs::OpenOptions;
    use tempfile::tempdir;

    fn encryption(chunk_size: usize) -> Encryption {
        let mut key = [0u8; 32];
        OsRng.fill_bytes(&mut key);
        Encryption::new(key, chunk_size)
    }

    fn data(length: usize) -> Vec<u8> {
        (0..length).map(|index| (index % 251) as u8).collect()
    }

    fn create_file(path: &Path, encryption: &Encryption, length: u64) -> File {
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        encryption.initialize(&file, length).unwrap();
        file
    }

    #[tokio::test]
    async fn open_write_shares_one_priority_queue_between_concurrent_misses() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("content");
        tokio::fs::write(&path, b"").await.unwrap();
        let encryption = encryption(64);
        let (low, high) = tokio::join!(encryption.open_write(&path), encryption.open_write(&path));
        let (mut low, mut high) = (low.unwrap(), high.unwrap());
        assert!(Arc::ptr_eq(low.get_ref(), high.get_ref()));
        low.set_priority(WritePriority::Low);
        high.set_priority(WritePriority::High);

        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let blocking = low
            .spawn_blocking(move |_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
            })
            .unwrap();
        started_rx.await.unwrap();

        let low_write = low
            .spawn_blocking(|fd| {
                let offset = fd.metadata()?.len();
                fd.write_all_at(b"low", offset)
            })
            .unwrap();
        let high_write = high
            .spawn_blocking(|fd| {
                let offset = fd.metadata()?.len();
                fd.write_all_at(b"high", offset)
            })
            .unwrap();
        release_tx.send(()).unwrap();

        blocking.await.unwrap();
        low_write.await.unwrap().unwrap();
        high_write.await.unwrap().unwrap();
        assert_eq!(tokio::fs::read(path).await.unwrap(), b"highlow");
    }

    #[test]
    fn round_trip_supports_empty_chunked_partial_and_ranged_content() {
        let temp_dir = tempdir().unwrap();
        let encryption = encryption(64);
        let content = data(1027);
        let path = temp_dir.path().join("content");
        let file = create_file(&path, &encryption, content.len() as u64);

        let mut offset = 0;
        for length in [1, 63, 2, 127, 255, 65, 514] {
            encryption
                .write_at(
                    &file,
                    &path,
                    offset,
                    &content[offset as usize..(offset + length) as usize],
                )
                .unwrap();
            offset += length;
        }
        assert_eq!(offset as usize, content.len());
        assert_eq!(
            encryption
                .read_at(&file, &path, 0, content.len() as u64)
                .unwrap(),
            content
        );
        assert_eq!(
            encryption.read_at(&file, &path, 61, 131).unwrap(),
            content[61..192]
        );
        assert_eq!(
            encryption.read_at(&file, &path, 1020, 100).unwrap(),
            content[1020..]
        );

        let empty_path = temp_dir.path().join("empty");
        let empty = create_file(&empty_path, &encryption, 0);
        assert!(encryption
            .read_at(&empty, &empty_path, 0, 0)
            .unwrap()
            .is_empty());
        assert_eq!(empty.metadata().unwrap().len(), HEADER_SIZE);
    }

    #[test]
    fn tampering_and_truncation_are_rejected() {
        let temp_dir = tempdir().unwrap();
        let encryption = encryption(64);
        let content = data(130);
        let path = temp_dir.path().join("content");
        let file = create_file(&path, &encryption, content.len() as u64);
        encryption.write_at(&file, &path, 0, &content).unwrap();

        let ciphertext_offset = HEADER_SIZE + NONCE_SIZE as u64 + LENGTH_SIZE + 7;
        let mut byte = [0u8; 1];
        read_exact_at(&file, &mut byte, ciphertext_offset).unwrap();
        byte[0] ^= 1;
        file.write_all_at(&byte, ciphertext_offset).unwrap();
        assert_eq!(
            encryption
                .read_at(&file, &path, 0, content.len() as u64)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );

        byte[0] ^= 1;
        file.write_all_at(&byte, ciphertext_offset).unwrap();
        assert_eq!(
            encryption
                .read_at(&file, &path, 0, content.len() as u64)
                .unwrap(),
            content
        );
        file.set_len(file.metadata().unwrap().len() - 1).unwrap();
        assert!(encryption
            .read_at(&file, &path, 0, content.len() as u64)
            .is_err());
    }

    #[test]
    fn zeroed_record_is_not_accepted_during_a_partial_rewrite() {
        let temp_dir = tempdir().unwrap();
        let encryption = encryption(64);
        let content = data(64);
        let path = temp_dir.path().join("content");
        let file = create_file(&path, &encryption, content.len() as u64);
        encryption.write_at(&file, &path, 0, &content[..1]).unwrap();
        file.write_all_at(&vec![0u8; encryption.record_size() as usize], HEADER_SIZE)
            .unwrap();

        assert!(encryption
            .write_at(&file, &path, 1, &content[1..2])
            .is_err());
        assert!(encryption.read_at(&file, &path, 0, 64).is_err());
    }

    #[test]
    fn concurrent_partial_writes_preserve_the_whole_chunk() {
        let temp_dir = tempdir().unwrap();
        let encryption = Arc::new(encryption(64));
        let content = Arc::new(data(127));
        let path = Arc::new(temp_dir.path().join("content"));
        let file = Arc::new(create_file(&path, &encryption, content.len() as u64));
        std::thread::scope(|scope| {
            for start in (0..content.len()).step_by(7) {
                let end = (start + 7).min(content.len());
                let encryption = Arc::clone(&encryption);
                let content = Arc::clone(&content);
                let path = Arc::clone(&path);
                let file = Arc::clone(&file);
                scope.spawn(move || {
                    encryption
                        .write_at(&file, &path, start as u64, &content[start..end])
                        .unwrap();
                });
            }
        });

        assert_eq!(
            encryption
                .read_at(&file, &path, 0, content.len() as u64)
                .unwrap(),
            content.as_slice()
        );
    }

    #[test]
    fn every_chunk_and_rewrite_uses_a_unique_nonce() {
        let temp_dir = tempdir().unwrap();
        let encryption = encryption(64);
        let content = data(130);
        let path = temp_dir.path().join("content");
        let file = create_file(&path, &encryption, content.len() as u64);
        encryption.write_at(&file, &path, 0, &content).unwrap();

        let mut nonces = HashSet::new();
        for chunk_index in 0..3 {
            let mut nonce = [0u8; NONCE_SIZE];
            read_exact_at(
                &file,
                &mut nonce,
                encryption.record_offset(chunk_index).unwrap(),
            )
            .unwrap();
            assert!(nonces.insert(nonce));
        }

        encryption
            .write_at(&file, &path, 64, &content[64..128])
            .unwrap();
        let mut rewritten_nonce = [0u8; NONCE_SIZE];
        read_exact_at(
            &file,
            &mut rewritten_nonce,
            encryption.record_offset(1).unwrap(),
        )
        .unwrap();
        assert!(nonces.insert(rewritten_nonce));
    }

    #[test]
    fn legacy_plaintext_is_rejected() {
        let temp_dir = tempdir().unwrap();
        let encryption = encryption(64);
        let path = temp_dir.path().join("plaintext");
        let file = File::create(&path).unwrap();
        file.write_all_at(&data(128), 0).unwrap();
        assert!(encryption.read_at(&file, &path, 0, 128).is_err());
    }

    #[test]
    fn key_configuration_fails_fast_and_loads_raw_or_hex_keys() {
        let config = StorageEncryption {
            enable: true,
            ..Default::default()
        };
        assert!(Encryption::from_config(&config).is_err());
        assert!(Encryption::from_config(&StorageEncryption::default())
            .unwrap()
            .is_none());

        let temp_dir = tempdir().unwrap();
        let mut key = [0u8; 32];
        OsRng.fill_bytes(&mut key);
        let raw_path = temp_dir.path().join("raw-key");
        std::fs::write(&raw_path, key).unwrap();
        let raw_config = StorageEncryption {
            enable: true,
            key_file: Some(raw_path),
            key_env: None,
        };
        assert!(Encryption::from_config(&raw_config).unwrap().is_some());

        let hex_path = temp_dir.path().join("hex-key");
        std::fs::write(&hex_path, hex::encode(key)).unwrap();
        let hex_config = StorageEncryption {
            enable: true,
            key_file: Some(hex_path),
            key_env: None,
        };
        assert!(Encryption::from_config(&hex_config).unwrap().is_some());

        let conflicting_config = StorageEncryption {
            enable: true,
            key_file: Some(temp_dir.path().join("key")),
            key_env: Some("UNUSED_TEST_KEY".to_string()),
        };
        assert!(Encryption::from_config(&conflicting_config).is_err());
    }

    #[test]
    fn environment_key_is_loaded_and_missing_key_fails_fast() {
        let name = format!(
            "DRAGONFLY_TEST_KEY_{}_{}",
            std::process::id(),
            OsRng.next_u64()
        );
        let mut key = [0u8; 32];
        OsRng.fill_bytes(&mut key);
        std::env::set_var(&name, hex::encode(key));
        let config = StorageEncryption {
            enable: true,
            key_file: None,
            key_env: Some(name.clone()),
        };
        assert!(Encryption::from_config(&config).unwrap().is_some());
        std::env::remove_var(&name);
        assert!(Encryption::from_config(&config).is_err());
    }

    #[test]
    fn encrypted_length_and_physical_ranges_cover_all_overhead() {
        let encryption = encryption(64);
        assert_eq!(encryption.encrypted_length(0).unwrap(), HEADER_SIZE);
        assert_eq!(encryption.encrypted_length(1).unwrap(), HEADER_SIZE + 108);
        assert_eq!(encryption.encrypted_length(64).unwrap(), HEADER_SIZE + 108);
        assert_eq!(encryption.encrypted_length(65).unwrap(), HEADER_SIZE + 216);
        assert_eq!(
            encryption.physical_range(1, 63).unwrap(),
            (HEADER_SIZE, 108)
        );
        assert_eq!(
            encryption.physical_range(63, 2).unwrap(),
            (HEADER_SIZE, 216)
        );
        assert_eq!(encryption.physical_range(0, 0).unwrap(), (0, 0));
    }

    #[test]
    fn wrong_key_is_rejected() {
        let temp_dir = tempdir().unwrap();
        let first = encryption(64);
        let second = encryption(64);
        let content = data(80);
        let path = temp_dir.path().join("content");
        let file = create_file(&path, &first, content.len() as u64);
        first.write_at(&file, &path, 0, &content).unwrap();
        assert!(second
            .read_at(&file, &path, 0, content.len() as u64)
            .is_err());
    }

    #[test]
    fn key_file_with_invalid_length_is_rejected() {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().join("key");
        std::fs::write(&path, data(31)).unwrap();
        let config = StorageEncryption {
            enable: true,
            key_file: Some(path),
            key_env: None,
        };
        assert!(Encryption::from_config(&config).is_err());
    }

    #[test]
    fn raw_ciphertext_does_not_equal_plaintext() {
        let temp_dir = tempdir().unwrap();
        let encryption = encryption(64);
        let content = data(128);
        let path = temp_dir.path().join("content");
        let file = create_file(&path, &encryption, content.len() as u64);
        encryption.write_at(&file, &path, 0, &content).unwrap();
        let raw = std::fs::read(path).unwrap();
        assert_ne!(
            &raw[HEADER_SIZE as usize..HEADER_SIZE as usize + 28],
            &content[..28]
        );
    }
}
