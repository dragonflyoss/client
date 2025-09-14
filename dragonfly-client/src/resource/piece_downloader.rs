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
use dragonfly_api::dfdaemon::v2::{
    DownloadCachePieceRequest, DownloadPersistentCachePieceRequest, DownloadPieceRequest,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_storage::metadata;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, error, info, instrument};

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
    ) -> Result<(Vec<u8>, u64, String)>;

    /// download_persistent_cache_piece downloads a persistent cache piece from the other peer by different
    /// protocols.
    async fn download_persistent_cache_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(Vec<u8>, u64, String)>;

    /// download_cache_piece downloads a cache piece from the other peer by different protocols.
    async fn download_cache_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(Vec<u8>, u64, String)>;
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
        let downloader = match protocol {
            "grpc" => Arc::new(GRPCDownloader::new(
                config.clone(),
                DEFAULT_DOWNLOADER_CAPACITY,
                DEFAULT_DOWNLOADER_IDLE_TIMEOUT,
            )),
            _ => {
                error!("downloader unsupported protocol: {}", protocol);
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

/// RequestGuard is the guard for the request.
struct RequestGuard {
    /// active_requests is the number of the active requests.
    active_requests: Arc<AtomicUsize>,
}

/// RequestGuard implements the guard for the request to add or subtract the active requests.
impl RequestGuard {
    /// new returns a new RequestGuard.
    fn new(active_requests: Arc<AtomicUsize>) -> Self {
        active_requests.fetch_add(1, Ordering::SeqCst);
        Self { active_requests }
    }
}

/// RequestGuard implements the Drop trait.
impl Drop for RequestGuard {
    /// drop subtracts the active requests.
    fn drop(&mut self) {
        self.active_requests.fetch_sub(1, Ordering::SeqCst);
    }
}

/// DfdaemonUploadClientEntry is the entry of the dfdaemon upload client.
#[derive(Clone)]
struct DfdaemonUploadClientEntry {
    /// client is the dfdaemon upload client.
    client: DfdaemonUploadClient,

    /// active_requests is the number of the active requests.
    active_requests: Arc<AtomicUsize>,

    /// actived_at is the time when the client is the last active time.
    actived_at: Arc<std::sync::Mutex<Instant>>,
}

/// GRPCDownloader is the downloader for downloading pieces by the gRPC protocol.
/// It will reuse the dfdaemon upload clients to download pieces from the other peers by
/// peer's address.
pub struct GRPCDownloader {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// clients is the map of the dfdaemon upload clients.
    clients: Arc<Mutex<HashMap<String, DfdaemonUploadClientEntry>>>,

    /// capacity is the capacity of the dfdaemon upload clients. If the number of the
    /// clients exceeds the capacity, it will clean up the idle clients.
    capacity: usize,

    /// client_idle_timeout is the idle timeout for the client. If the client is idle for a long
    /// time, it will be removed when cleaning up the idle clients.
    idle_timeout: Duration,

    /// cleanup_at is the time when the client is the last cleanup time.
    cleanup_at: Arc<Mutex<Instant>>,
}

/// GRPCDownloader implements the downloader with the gRPC protocol.
impl GRPCDownloader {
    /// new returns a new GRPCDownloader.
    pub fn new(config: Arc<Config>, capacity: usize, idle_timeout: Duration) -> Self {
        Self {
            config,
            clients: Arc::new(Mutex::new(HashMap::new())),
            capacity,
            idle_timeout,
            cleanup_at: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// client_entry returns the dfdaemon upload client entry by the address.
    ///
    /// Opterations:
    /// 1. If the client entry exists, it will return the client directly to reuse the client by
    ///    the address.
    /// 2. If the client entry does not exist, it will create a new client entry and insert it
    ///    into the clients map.
    async fn client_entry(&self, addr: &str) -> Result<DfdaemonUploadClientEntry> {
        let now = Instant::now();

        // Cleanup the idle clients first to avoid the clients exceeding the capacity and the
        // clients are idle for a long time.
        self.cleanup_idle_client_entries().await;

        let clients = self.clients.lock().await;
        if let Some(entry) = clients.get(addr) {
            debug!("reusing client: {}", addr);
            *entry.actived_at.lock().unwrap() = now;
            return Ok(entry.clone());
        }
        drop(clients);

        // If there are many concurrent requests to create the client, it will create multiple
        // clients for the same address. But it will reuse the same client by entry operation.
        debug!("creating client: {}", addr);
        let client =
            DfdaemonUploadClient::new(self.config.clone(), format!("http://{}", addr), true)
                .await?;

        let mut clients = self.clients.lock().await;
        let entry = clients
            .entry(addr.to_string())
            .or_insert(DfdaemonUploadClientEntry {
                client: client.clone(),
                active_requests: Arc::new(AtomicUsize::new(0)),
                actived_at: Arc::new(std::sync::Mutex::new(now)),
            });

        // If it is created by other concurrent requests and reused client, need to update the
        // last active time.
        *entry.actived_at.lock().unwrap() = now;
        Ok(entry.clone())
    }

    /// remove_client_entry removes the client entry if it is idle.
    async fn remove_client_entry(&self, addr: &str) {
        let mut clients = self.clients.lock().await;
        if let Some(entry) = clients.get(addr) {
            if entry.active_requests.load(Ordering::SeqCst) == 0 {
                clients.remove(addr);
            }
        }
    }

    /// cleanup_idle_clients cleans up the idle clients, which are idle for a long time or have no
    /// active requests.
    async fn cleanup_idle_client_entries(&self) {
        let now = Instant::now();

        // Avoid hot cleanup for the clients.
        let cleanup_at = self.cleanup_at.lock().await;
        let interval = self.idle_timeout / 2;
        if now.duration_since(*cleanup_at) < interval {
            debug!("avoid hot cleanup");
            return;
        }
        drop(cleanup_at);

        let mut clients = self.clients.lock().await;
        let exceeds_capacity = clients.len() > self.capacity;
        clients.retain(|addr, entry| {
            let active_requests = entry.active_requests.load(Ordering::SeqCst);
            let is_active = active_requests > 0;
            let actived_at = entry.actived_at.lock().unwrap();
            let idel_duration = now.duration_since(*actived_at);
            let is_recent = idel_duration <= self.idle_timeout;

            // Retain the client if it is active or not exceeds the capacity and is recent.
            let should_retain = is_active || (!exceeds_capacity && is_recent);
            if !should_retain {
                info!(
                    "removing idle client: {}, exceeds_capacity: {}, idle_duration: {}s",
                    addr,
                    exceeds_capacity,
                    idel_duration.as_secs(),
                );
            }

            should_retain
        });

        // Update the cleanup time.
        *self.cleanup_at.lock().await = now;
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
    ) -> Result<(Vec<u8>, u64, String)> {
        let entry = self.client_entry(addr).await?;
        let request_guard = RequestGuard::new(entry.active_requests.clone());
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
                self.remove_client_entry(addr).await;
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

        Ok((content, piece.offset, piece.digest))
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
    ) -> Result<(Vec<u8>, u64, String)> {
        let entry = self.client_entry(addr).await?;
        let request_guard = RequestGuard::new(entry.active_requests.clone());
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
                self.remove_client_entry(addr).await;
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

        Ok((content, piece.offset, piece.digest))
    }

    /// download_cache_piece downloads a cache piece from the other peer by the gRPC protocol.
    #[instrument(skip_all)]
    async fn download_cache_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(Vec<u8>, u64, String)> {
        let entry = self.client_entry(addr).await?;
        let request_guard = RequestGuard::new(entry.active_requests.clone());
        let response = match entry
            .client
            .download_cache_piece(
                DownloadCachePieceRequest {
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
                self.remove_client_entry(addr).await;
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

        Ok((content, piece.offset, piece.digest))
    }
}
