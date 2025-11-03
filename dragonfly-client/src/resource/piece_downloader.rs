/*
 *     Copyright 2024 The Dragonfly Authors
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

use crate::grpc::dfdaemon_upload::DfdaemonUploadClient;
use dragonfly_api::dfdaemon::v2::{DownloadPersistentCachePieceRequest, DownloadPieceRequest};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_storage::{client::quic::QUICClient, client::tcp::TCPClient, metadata};
use dragonfly_client_util::pool::{Builder as PoolBuilder, Entry, Factory, Pool};
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncRead;
use tracing::{error, instrument};

/// DEFAULT_DOWNLOADER_CAPACITY is the default capacity of the downloader to store the clients.
const DEFAULT_DOWNLOADER_CAPACITY: usize = 2000;

/// DEFAULT_DOWNLOADER_IDLE_TIMEOUT is the default idle timeout for the downloader.
const DEFAULT_DOWNLOADER_IDLE_TIMEOUT: Duration = Duration::from_secs(420);

/// Downloader is the interface for downloading pieces, which is implemented by different
/// protocols. The downloader is used to download pieces from the other peers.
#[tonic::async_trait]
pub trait Downloader: Send + Sync {
    /// download_piece downloads a piece from the other peer by different protocols.
    async fn download_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)>;

    /// download_persistent_cache_piece downloads a persistent cache piece from the other peer by different
    /// protocols.
    async fn download_persistent_cache_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)>;
}

/// DownloaderFactory is the factory for creating different downloaders by different protocols.
pub struct DownloaderFactory {
    /// downloader is the downloader for downloading pieces, which is implemented by different
    /// protocols.
    downloader: Arc<dyn Downloader + Send + Sync>,
}

/// DownloadFactory implements the DownloadFactory trait.
impl DownloaderFactory {
    /// new returns a new DownloadFactory.
    pub fn new(protocol: &str, config: Arc<Config>) -> Result<Self> {
        let downloader: Arc<dyn Downloader> = match protocol {
            "tcp" => Arc::new(TCPDownloader::new(
                config.clone(),
                DEFAULT_DOWNLOADER_CAPACITY,
                DEFAULT_DOWNLOADER_IDLE_TIMEOUT,
            )),
            "grpc" => Arc::new(GRPCDownloader::new(
                config.clone(),
                DEFAULT_DOWNLOADER_CAPACITY,
                DEFAULT_DOWNLOADER_IDLE_TIMEOUT,
            )),
            "quic" => Arc::new(QUICDownloader::new(
                config.clone(),
                DEFAULT_DOWNLOADER_CAPACITY,
                DEFAULT_DOWNLOADER_IDLE_TIMEOUT,
            )),
            _ => {
                error!("unsupported protocol: {}", protocol);
                return Err(Error::InvalidParameter);
            }
        };

        Ok(Self { downloader })
    }

    /// build returns the downloader.
    pub fn build(&self) -> Arc<dyn Downloader> {
        self.downloader.clone()
    }
}

/// Factory for creating DfdaemonUploadClient instances.
struct DfdaemonUploadClientFactory {
    config: Arc<Config>,
}

/// DfdaemonUploadClientFactory implements the Factory trait for creating DfdaemonUploadClient
/// instances.
#[tonic::async_trait]
impl Factory<String, DfdaemonUploadClient> for DfdaemonUploadClientFactory {
    type Error = Error;
    /// Creates a new DfdaemonUploadClient for the given address.
    async fn make_client(&self, addr: &String) -> Result<DfdaemonUploadClient> {
        DfdaemonUploadClient::new(self.config.clone(), format!("http://{}", addr), true).await
    }
}

/// GRPCDownloader is the downloader for downloading pieces by the gRPC protocol.
/// It will reuse the dfdaemon upload clients to download pieces from the other peers by
/// peer's address.
pub struct GRPCDownloader {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// client_pool is the pool of the dfdaemon upload clients.
    client_pool: Pool<String, String, DfdaemonUploadClient, DfdaemonUploadClientFactory>,
}

/// GRPCDownloader implements the downloader with the gRPC protocol.
impl GRPCDownloader {
    /// MAX_CONNECTIONS_PER_ADDRESS is the maximum number of connections per address.
    const MAX_CONNECTIONS_PER_ADDRESS: usize = 32;

    /// new returns a new GRPCDownloader.
    pub fn new(config: Arc<Config>, capacity: usize, idle_timeout: Duration) -> Self {
        Self {
            config: config.clone(),
            client_pool: PoolBuilder::new(DfdaemonUploadClientFactory { config })
                .capacity(capacity)
                .idle_timeout(idle_timeout)
                .build(),
        }
    }

    /// get_client_entry returns a client entry by the address.
    async fn get_client_entry(
        &self,
        key: String,
        addr: String,
    ) -> Result<Entry<DfdaemonUploadClient>> {
        self.client_pool.entry(&key, &addr).await
    }

    /// remove_client_entry removes the client if it is idle.
    async fn remove_client_entry(&self, key: String) {
        self.client_pool.remove_entry(&key).await;
    }

    /// get_entry_key generates a semi-random key by combining the client address with
    /// a random number. The randomization helps distribute connections across multiple
    /// slots when the same address attempts to establish multiple concurrent connections.
    fn get_entry_key(&self, addr: &str) -> String {
        format!(
            "{}-{}",
            addr,
            fastrand::usize(..Self::MAX_CONNECTIONS_PER_ADDRESS)
        )
    }
}

/// GRPCDownloader implements the Downloader trait.
#[tonic::async_trait]
impl Downloader for GRPCDownloader {
    /// download_piece downloads a piece from the other peer by the gRPC protocol.
    #[instrument(skip_all)]
    async fn download_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        let response = match entry
            .client
            .download_piece(
                DownloadPieceRequest {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    piece_number: number,
                },
                self.config.download.piece_timeout,
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                return Err(err);
            }
        };

        let Some(piece) = response.piece else {
            error!("resource piece is missing");
            return Err(Error::InvalidParameter);
        };

        let Some(content) = piece.content else {
            error!("resource piece content is missing");
            return Err(Error::InvalidParameter);
        };

        // Calculate the digest of the piece metadata and compare it with the expected digest,
        // it verifies the integrity of the piece metadata.
        let piece_metadata = metadata::Piece {
            number,
            length: piece.length,
            offset: piece.offset,
            digest: piece.digest.clone(),
            ..Default::default()
        };

        if let Some(expected_digest) = response.digest {
            let digest = piece_metadata.calculate_digest();
            if expected_digest != digest {
                return Err(Error::DigestMismatch(
                    expected_digest.to_string(),
                    digest.to_string(),
                ));
            }
        }

        Ok((Box::new(Cursor::new(content)), piece.offset, piece.digest))
    }

    /// download_persistent_cache_piece downloads a persistent cache piece from the other peer by
    /// the gRPC protocol.
    #[instrument(skip_all)]
    async fn download_persistent_cache_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        let response = match entry
            .client
            .download_persistent_cache_piece(
                DownloadPersistentCachePieceRequest {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    piece_number: number,
                },
                self.config.download.piece_timeout,
            )
            .await
        {
            Ok(response) => response,
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                return Err(err);
            }
        };

        let Some(piece) = response.piece else {
            error!("persistent cache piece is missing");
            return Err(Error::InvalidParameter);
        };

        let Some(content) = piece.content else {
            error!("persistent cache piece content is missing");
            return Err(Error::InvalidParameter);
        };

        // Calculate the digest of the piece metadata and compare it with the expected digest,
        // it verifies the integrity of the piece metadata.
        let piece_metadata = metadata::Piece {
            number,
            length: piece.length,
            offset: piece.offset,
            digest: piece.digest.clone(),
            ..Default::default()
        };

        if let Some(expected_digest) = response.digest {
            let digest = piece_metadata.calculate_digest();
            if expected_digest != digest {
                return Err(Error::DigestMismatch(
                    expected_digest.to_string(),
                    digest.to_string(),
                ));
            }
        }

        Ok((Box::new(Cursor::new(content)), piece.offset, piece.digest))
    }
}

/// QUICDownloader is the downloader for downloading pieces by the QUIC protocol.
/// It will reuse the quic clients to download pieces from the other peers by
/// peer's address.
pub struct QUICDownloader {
    /// client_pool is the pool of the quic clients.
    client_pool: Pool<String, String, QUICClient, QUICClientFactory>,
}

/// Factory for creating QUICClient instances.
struct QUICClientFactory {
    config: Arc<Config>,
}

/// QUICClientFactory implements the Factory trait for creating QUICClient instances.
#[tonic::async_trait]
impl Factory<String, QUICClient> for QUICClientFactory {
    type Error = Error;

    /// Creates a new QUICClient for the given address.
    async fn make_client(&self, addr: &String) -> Result<QUICClient> {
        Ok(QUICClient::new(self.config.clone(), addr.clone()))
    }
}

/// QUICDownloader implements the downloader with the QUIC protocol.
impl QUICDownloader {
    /// MAX_CONNECTIONS_PER_ADDRESS is the maximum number of connections per address.
    const MAX_CONNECTIONS_PER_ADDRESS: usize = 32;

    /// new returns a new QUICDownloader.
    pub fn new(config: Arc<Config>, capacity: usize, idle_timeout: Duration) -> Self {
        Self {
            client_pool: PoolBuilder::new(QUICClientFactory {
                config: config.clone(),
            })
            .capacity(capacity)
            .idle_timeout(idle_timeout)
            .build(),
        }
    }

    /// get_client_entry returns a client entry by the address.
    async fn get_client_entry(&self, key: String, addr: String) -> Result<Entry<QUICClient>> {
        self.client_pool.entry(&key, &addr).await
    }

    /// remove_client_entry removes the client if it is idle.
    async fn remove_client_entry(&self, key: String) {
        self.client_pool.remove_entry(&key).await;
    }
    /// get_entry_key generates a semi-random key by combining the client address with
    /// a random number. The randomization helps distribute connections across multiple
    /// slots when the same address attempts to establish multiple concurrent connections.
    fn get_entry_key(&self, addr: &str) -> String {
        format!(
            "{}-{}",
            addr,
            fastrand::usize(..Self::MAX_CONNECTIONS_PER_ADDRESS)
        )
    }
}

/// QUICDownloader implements the Downloader trait.
#[tonic::async_trait]
impl Downloader for QUICDownloader {
    /// download_piece downloads a piece from the other peer by the QUIC protocol.
    #[instrument(skip_all)]
    async fn download_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry.client.download_piece(number, task_id).await {
            Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }

    /// download_persistent_cache_piece downloads a persistent cache piece from the other peer by
    /// the QUIC protocol.
    #[instrument(skip_all)]
    async fn download_persistent_cache_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry
            .client
            .download_persistent_cache_piece(number, task_id)
            .await
        {
            Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }
}

/// TCPDownloader is the downloader for downloading pieces by the TCP protocol.
/// It will reuse the tcp clients to download pieces from the other peers by
/// peer's address.
pub struct TCPDownloader {
    /// client_pool is the pool of the tcp clients.
    client_pool: Pool<String, String, TCPClient, TCPClientFactory>,
}

/// Factory for creating TCPClient instances.
struct TCPClientFactory {
    config: Arc<Config>,
}

/// TCPClientFactory implements the Factory trait for creating TCPClient instances.
#[tonic::async_trait]
impl Factory<String, TCPClient> for TCPClientFactory {
    type Error = Error;

    /// Creates a new TCPClient for the given address.
    async fn make_client(&self, addr: &String) -> Result<TCPClient> {
        Ok(TCPClient::new(self.config.clone(), addr.clone()))
    }
}

/// TCPDownloader implements the downloader with the TCP protocol.
impl TCPDownloader {
    /// MAX_CONNECTIONS_PER_ADDRESS is the maximum number of connections per address.
    const MAX_CONNECTIONS_PER_ADDRESS: usize = 32;

    /// new returns a new TCPDownloader.
    pub fn new(config: Arc<Config>, capacity: usize, idle_timeout: Duration) -> Self {
        Self {
            client_pool: PoolBuilder::new(TCPClientFactory {
                config: config.clone(),
            })
            .capacity(capacity)
            .idle_timeout(idle_timeout)
            .build(),
        }
    }

    /// get_client_entry returns a client entry by the address.
    async fn get_client_entry(&self, key: String, addr: String) -> Result<Entry<TCPClient>> {
        self.client_pool.entry(&key, &addr).await
    }

    /// remove_client_entry removes the client if it is idle.
    async fn remove_client_entry(&self, key: String) {
        self.client_pool.remove_entry(&key).await;
    }

    /// get_entry_key generates a semi-random key by combining the client address with
    /// a random number. The randomization helps distribute connections across multiple
    /// slots when the same address attempts to establish multiple concurrent connections.
    fn get_entry_key(&self, addr: &str) -> String {
        format!(
            "{}-{}",
            addr,
            fastrand::usize(..Self::MAX_CONNECTIONS_PER_ADDRESS)
        )
    }
}

/// TCPDownloader implements the Downloader trait.
#[tonic::async_trait]
impl Downloader for TCPDownloader {
    /// download_piece downloads a piece from the other peer by the TCP protocol.
    #[instrument(skip_all)]
    async fn download_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry.client.download_piece(number, task_id).await {
            Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }

    /// download_persistent_cache_piece downloads a persistent cache piece from the other peer by
    /// the TCP protocol.
    #[instrument(skip_all)]
    async fn download_persistent_cache_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(Box<dyn AsyncRead + Send + Unpin>, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry
            .client
            .download_persistent_cache_piece(number, task_id)
            .await
        {
            Ok((reader, offset, digest)) => Ok((Box::new(reader), offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }
}
