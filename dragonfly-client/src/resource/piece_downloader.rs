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

use async_trait::async_trait;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_storage::{
    client::quic::QUICClient, client::tcp::TCPClient, client::PieceContentStream,
};
use dragonfly_client_util::pool::{Builder as PoolBuilder, Entry, Factory, Pool};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, instrument};

/// The default capacity of the downloader to store the clients.
const DEFAULT_DOWNLOADER_CAPACITY: usize = 2000;

/// The default idle timeout for the downloader.
const DEFAULT_DOWNLOADER_IDLE_TIMEOUT: Duration = Duration::from_secs(420);

/// The interface for downloading pieces, which is implemented by different
/// protocols. The downloader is used to download pieces from the other peers.
#[async_trait]
pub trait Downloader: Send + Sync {
    /// Downloads a piece from the other peer by different protocols.
    async fn download_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(PieceContentStream, u64, String)>;

    /// Downloads a persistent piece from the other peer by different
    /// protocols.
    async fn download_persistent_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(PieceContentStream, u64, String)>;

    /// Downloads a persistent cache piece from the other peer by different
    /// protocols.
    async fn download_persistent_cache_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(PieceContentStream, u64, String)>;
}

/// The factory for creating different downloaders by different protocols.
pub struct DownloaderFactory {
    /// The downloader for downloading pieces, which is implemented by different
    /// protocols.
    downloader: Arc<dyn Downloader + Send + Sync>,
}

/// DownloadFactory implements the DownloadFactory trait.
impl DownloaderFactory {
    /// Returns a new DownloadFactory.
    pub fn new(protocol: &str, config: Arc<Config>) -> Result<Self> {
        let downloader: Arc<dyn Downloader> = match protocol {
            "tcp" => Arc::new(TCPDownloader::new(
                config.clone(),
                DEFAULT_DOWNLOADER_CAPACITY,
                DEFAULT_DOWNLOADER_IDLE_TIMEOUT,
            )),
            "quic" => Arc::new(QUICDownloader::new(
                config.clone(),
                DEFAULT_DOWNLOADER_CAPACITY,
                DEFAULT_DOWNLOADER_IDLE_TIMEOUT,
            )),
            #[cfg(feature = "rdma")]
            "rdma" => Arc::new(rdma::RDMADownloader::new(config.clone())),
            _ => {
                error!("unsupported protocol: {}", protocol);
                return Err(Error::InvalidParameter);
            }
        };

        Ok(Self { downloader })
    }

    /// Returns the downloader.
    pub fn build(&self) -> Arc<dyn Downloader> {
        self.downloader.clone()
    }
}

/// The downloader for downloading pieces by the QUIC protocol.
/// It will reuse the quic clients to download pieces from the other peers by
/// peer's address.
pub struct QUICDownloader {
    /// The pool of the quic clients.
    client_pool: Pool<String, String, QUICClient, QUICClientFactory>,
}

/// Factory for creating QUICClient instances.
struct QUICClientFactory {
    config: Arc<Config>,
}

/// Implements the Factory trait for creating QUICClient instances.
#[async_trait]
impl Factory<String, QUICClient> for QUICClientFactory {
    type Error = Error;

    /// Creates a new QUICClient for the given address.
    async fn make_client(&self, addr: &String) -> Result<QUICClient> {
        Ok(QUICClient::new(self.config.clone(), addr.clone()))
    }
}

/// Implements the downloader with the QUIC protocol.
impl QUICDownloader {
    /// The maximum number of connections per address.
    const MAX_CONNECTIONS_PER_ADDRESS: usize = 32;

    /// Returns a new QUICDownloader.
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

    /// Returns a client entry by the address.
    async fn get_client_entry(&self, key: String, addr: String) -> Result<Entry<QUICClient>> {
        self.client_pool.entry(&key, &addr).await
    }

    /// Removes the client if it is idle.
    async fn remove_client_entry(&self, key: String) {
        self.client_pool.remove_entry(&key).await;
    }
    /// Generates a semi-random key by combining the client address with
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

/// Implements the Downloader trait.
#[async_trait]
impl Downloader for QUICDownloader {
    /// Downloads a piece from the other peer by the QUIC protocol.
    #[instrument(skip_all)]
    async fn download_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(PieceContentStream, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry.client.download_piece(number, task_id).await {
            Ok((stream, offset, digest)) => Ok((stream, offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }

    /// Downloads a persistent piece from the other peer by
    /// the QUIC protocol.
    #[instrument(skip_all)]
    async fn download_persistent_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(PieceContentStream, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry
            .client
            .download_persistent_piece(number, task_id)
            .await
        {
            Ok((stream, offset, digest)) => Ok((stream, offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }

    /// Downloads a persistent cache piece from the other peer by
    /// the QUIC protocol.
    #[instrument(skip_all)]
    async fn download_persistent_cache_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(PieceContentStream, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry
            .client
            .download_persistent_cache_piece(number, task_id)
            .await
        {
            Ok((stream, offset, digest)) => Ok((stream, offset, digest)),
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

/// The downloader for downloading pieces by the TCP protocol.
/// It will reuse the tcp clients to download pieces from the other peers by
/// peer's address.
pub struct TCPDownloader {
    /// The pool of the tcp clients.
    client_pool: Pool<String, String, TCPClient, TCPClientFactory>,
}

/// Factory for creating TCPClient instances.
struct TCPClientFactory {
    config: Arc<Config>,
}

/// Implements the Factory trait for creating TCPClient instances.
#[async_trait]
impl Factory<String, TCPClient> for TCPClientFactory {
    type Error = Error;

    /// Creates a new TCPClient for the given address.
    async fn make_client(&self, addr: &String) -> Result<TCPClient> {
        Ok(TCPClient::new(self.config.clone(), addr.clone()))
    }
}

/// Implements the downloader with the TCP protocol.
impl TCPDownloader {
    /// The maximum number of connections per address.
    const MAX_CONNECTIONS_PER_ADDRESS: usize = 32;

    /// Returns a new TCPDownloader.
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

    /// Returns a client entry by the address.
    async fn get_client_entry(&self, key: String, addr: String) -> Result<Entry<TCPClient>> {
        self.client_pool.entry(&key, &addr).await
    }

    /// Removes the client if it is idle.
    async fn remove_client_entry(&self, key: String) {
        self.client_pool.remove_entry(&key).await;
    }

    /// Generates a semi-random key by combining the client address with
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

/// Implements the Downloader trait.
#[async_trait]
impl Downloader for TCPDownloader {
    /// Downloads a piece from the other peer by the TCP protocol.
    #[instrument(skip_all)]
    async fn download_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(PieceContentStream, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry.client.download_piece(number, task_id).await {
            Ok((stream, offset, digest)) => Ok((stream, offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }

    /// Downloads a persistent piece from the other peer by
    /// the TCP protocol.
    #[instrument(skip_all)]
    async fn download_persistent_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(PieceContentStream, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry
            .client
            .download_persistent_piece(number, task_id)
            .await
        {
            Ok((stream, offset, digest)) => Ok((stream, offset, digest)),
            Err(err) => {
                // If the request fails, it will drop the request guard and remove the client
                // entry to avoid using the invalid client.
                drop(request_guard);
                self.remove_client_entry(key).await;
                Err(err)
            }
        }
    }

    /// Downloads a persistent cache piece from the other peer by
    /// the TCP protocol.
    #[instrument(skip_all)]
    async fn download_persistent_cache_piece(
        &self,
        addr: &str,
        number: u32,
        _host_id: &str,
        task_id: &str,
    ) -> Result<(PieceContentStream, u64, String)> {
        let key = self.get_entry_key(addr);
        let entry = self.get_client_entry(key.clone(), addr.to_string()).await?;
        let request_guard = entry.request_guard();

        match entry
            .client
            .download_persistent_cache_piece(number, task_id)
            .await
        {
            Ok((stream, offset, digest)) => Ok((stream, offset, digest)),
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

/// rdma provides the libfabric piece downloader (AWS EFA and RoCE/InfiniBand). It is an
/// optimization layer: every error surfaces to the caller, which falls back to the TCP
/// downloader for that piece.
#[cfg(feature = "rdma")]
pub mod rdma {
    use super::*;
    use dragonfly_client_config::dfdaemon::RdmaProvider;
    use dragonfly_client_storage::client::rdma::{discover, RDMAClient};
    use dragonfly_client_storage::rdma::fabric::Fabric;
    use dragonfly_client_storage::rdma::rendezvous::{RdmaAdvertisement, WireCapability};
    use futures::StreamExt;
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::time::Instant;
    use tokio_util::io::ReaderStream;
    use tracing::{info, warn};

    /// FABRIC_RETRY_INTERVAL is how long to wait before retrying fabric initialization
    /// after a failure.
    const FABRIC_RETRY_INTERVAL: Duration = Duration::from_secs(300);

    /// INCOMPATIBLE_PARENT_TTL is how long a parent that reported fabric incompatibility
    /// is skipped before RDMA is attempted again. Incompatibility is a property of the peer's
    /// configuration, so retrying sooner than this cannot succeed.
    const INCOMPATIBLE_PARENT_TTL: Duration = Duration::from_secs(60);

    /// UNHEALTHY_PARENT_MIN_BACKOFF is how long a parent is skipped after its first RDMA
    /// transfer failure. A transfer failure, unlike incompatibility, may be a transient blip,
    /// so the first penalty is short enough that one bad piece does not cost a working parent
    /// its fast path.
    const UNHEALTHY_PARENT_MIN_BACKOFF: Duration = Duration::from_secs(2);

    /// UNHEALTHY_PARENT_MAX_BACKOFF caps the penalty applied to a parent that keeps failing.
    /// Without a cap a parent that recovers would stay on the TCP path indefinitely.
    const UNHEALTHY_PARENT_MAX_BACKOFF: Duration = Duration::from_secs(60);

    /// CAPABLE_PARENT_TTL bounds how long a successful discovery result is reused.
    const CAPABLE_PARENT_TTL: Duration = Duration::from_secs(60);

    /// Failure says why RDMA to a parent did not work, which decides how long to avoid it.
    ///
    /// This is passed in rather than recovered from the error, because the two cases are not
    /// distinguishable after the fact: a peer that cannot form a fabric pair and a peer that is
    /// merely unreachable both surface as an error from the same call.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Failure {
        /// Incompatible means the peers cannot form a fabric pair at all.
        Incompatible,

        /// Transport means an attempt failed, which may or may not repeat: an unreachable parent,
        /// a parent at its transfer admission limit, or a transfer that died part way.
        Transport,
    }

    /// ParentPenalty skips RDMA for a parent that just failed.
    ///
    /// Both failure kinds land here because both make the next RDMA attempt against that parent a
    /// waste: a rendezvous round trip, a timeout, and then the TCP fallback that would have served
    /// the piece directly. Incompatibility gets a flat penalty. A transfer failure gets a doubling
    /// one, so a parent whose fabric is broken stops being probed once per piece while a parent
    /// that dropped a single transfer is retried almost immediately.
    struct ParentPenalty {
        /// until is when RDMA may be attempted against this parent again.
        until: Instant,

        /// backoff is the penalty applied on the most recent failure, and the basis for the next.
        backoff: Duration,
    }

    /// FabricState tracks the lazily initialized process-shared fabric endpoint.
    enum FabricState {
        /// Uninitialized means no initialization has been attempted yet.
        Uninitialized,

        /// Failed records when initialization last failed, for retry backoff.
        Failed(Instant),

        /// Ready holds the shared endpoint and the local negotiation capability.
        Ready(Arc<Fabric>, WireCapability),
    }

    /// RDMADownloader downloads pieces over libfabric with a shared fabric endpoint. The
    /// endpoint is opened lazily on the first download so a misconfigured or unsupported
    /// host degrades to TCP instead of failing at startup.
    pub struct RDMADownloader {
        /// config is the configuration of the dfdaemon.
        config: Arc<Config>,

        /// fabric is the lazily initialized shared endpoint.
        fabric: tokio::sync::Mutex<FabricState>,

        /// unhealthy_parents skips parents whose last RDMA attempt failed, so every piece does
        /// not pay a doomed rendezvous round trip.
        unhealthy_parents: std::sync::Mutex<HashMap<String, ParentPenalty>>,

        /// capable_parents caches successful discovery so every piece does not add a control
        /// round trip. Transfer failures evict the entry immediately.
        capable_parents: std::sync::Mutex<HashMap<String, (Instant, RdmaAdvertisement)>>,
    }

    /// RDMADownloader implements the downloader over the libfabric transport.
    impl RDMADownloader {
        /// new returns a new RDMADownloader.
        pub fn new(config: Arc<Config>) -> Self {
            Self {
                config,
                fabric: tokio::sync::Mutex::new(FabricState::Uninitialized),
                unhealthy_parents: std::sync::Mutex::new(HashMap::new()),
                capable_parents: std::sync::Mutex::new(HashMap::new()),
            }
        }

        /// fabric returns the shared endpoint and local capability, initializing them on
        /// first use and applying retry backoff after failures.
        async fn fabric(&self) -> Result<(Arc<Fabric>, WireCapability)> {
            let mut state = self.fabric.lock().await;
            match &*state {
                FabricState::Ready(fabric, capability) if !fabric.is_failed() => {
                    return Ok((fabric.clone(), capability.clone()))
                }
                FabricState::Ready(_, _) => {
                    // A retired endpoint cannot recover by returning errors forever. Drop it
                    // and let the normal initialization path create a fresh provider endpoint.
                    *state = FabricState::Uninitialized;
                }
                FabricState::Failed(at) if at.elapsed() < FABRIC_RETRY_INTERVAL => {
                    return Err(Error::Unsupported(
                        "rdma fabric initialization failed recently".to_string(),
                    ));
                }
                _ => {}
            }

            let rdma_config = &self.config.storage.server.rdma;
            let Some(fabric_tag) = rdma_config
                .fabric_tag
                .as_deref()
                .filter(|tag| !tag.is_empty())
            else {
                *state = FabricState::Failed(Instant::now());
                return Err(Error::Unsupported(
                    "rdma requires storage.server.rdma.fabricTag".to_string(),
                ));
            };

            let provider = match rdma_config.provider {
                RdmaProvider::Auto => None,
                provider => Some(provider.to_string()),
            };
            match Fabric::new(
                provider.as_deref(),
                rdma_config.device.as_deref(),
                rdma_config.max_registered_bytes.as_u64(),
                rdma_config.allow_software_provider,
            ) {
                Ok(fabric) => {
                    let fabric = Arc::new(fabric);
                    let capability = WireCapability {
                        provider: fabric.provider().to_string(),
                        fabric_tag: fabric_tag.to_string(),
                    };
                    info!(
                        "rdma downloader ready: provider {}, fabric tag {}",
                        capability.provider, capability.fabric_tag
                    );
                    *state = FabricState::Ready(fabric.clone(), capability.clone());
                    Ok((fabric, capability))
                }
                Err(err) => {
                    warn!("rdma fabric initialization failed: {}", err);
                    *state = FabricState::Failed(Instant::now());
                    Err(err)
                }
            }
        }

        /// retire_failed_fabric removes a poisoned shared endpoint after a transfer failure.
        /// Ordinary peer incompatibility leaves the shared endpoint intact.
        async fn retire_failed_fabric(&self) {
            let mut state = self.fabric.lock().await;
            if matches!(&*state, FabricState::Ready(fabric, _) if fabric.is_failed()) {
                *state = FabricState::Uninitialized;
            }
        }

        /// check_parent errors fast for parents that are still serving a penalty. An expired entry
        /// is left in place, carrying its accumulated backoff, so that a parent which fails every
        /// time is not reset to the shortest penalty by each retry. Success clears it.
        fn check_parent(&self, addr: &str) -> Result<()> {
            match self.unhealthy_parents.lock().unwrap().get(addr) {
                Some(penalty) if penalty.until > Instant::now() => Err(Error::Unsupported(
                    format!("parent {addr} recently failed over rdma"),
                )),
                _ => Ok(()),
            }
        }

        /// record_failure penalizes a parent whose RDMA attempt failed, and drops any discovery
        /// result cached for it.
        fn record_failure(&self, addr: &str, failure: Failure) {
            self.capable_parents.lock().unwrap().remove(addr);

            let mut unhealthy_parents = self.unhealthy_parents.lock().unwrap();
            let backoff = match failure {
                // Incompatibility is a stable fact about the peer, so there is nothing for a
                // doubling backoff to discover.
                Failure::Incompatible => INCOMPATIBLE_PARENT_TTL,
                Failure::Transport => unhealthy_parents
                    .get(addr)
                    .map(|penalty| (penalty.backoff * 2).min(UNHEALTHY_PARENT_MAX_BACKOFF))
                    .unwrap_or(UNHEALTHY_PARENT_MIN_BACKOFF),
            };

            unhealthy_parents.insert(
                addr.to_string(),
                ParentPenalty {
                    until: Instant::now() + backoff,
                    backoff,
                },
            );
        }

        /// record_success clears a parent's penalty once an attempt against it works again.
        fn record_success(&self, addr: &str) {
            self.unhealthy_parents.lock().unwrap().remove(addr);
        }

        /// advertisement returns a cached live capability or discovers it through the parent's
        /// advertised TCP piece endpoint.
        async fn advertisement(
            &self,
            addr: &str,
            local: &WireCapability,
        ) -> Result<RdmaAdvertisement> {
            let cached = self.capable_parents.lock().unwrap().get(addr).cloned();
            if let Some((at, advertisement)) = cached {
                if at.elapsed() < CAPABLE_PARENT_TTL {
                    return Ok(advertisement);
                }
                self.capable_parents.lock().unwrap().remove(addr);
            }

            let advertisement = discover(addr, self.config.storage.server.rdma.transfer_timeout)
                .await
                .map_err(|err| {
                    self.record_failure(addr, Failure::Transport);
                    Error::Unsupported(format!("rdma discovery from {addr} failed: {err}"))
                })?;
            local
                .compatible(&advertisement.capability)
                .map_err(|reason| {
                    self.record_failure(addr, Failure::Incompatible);
                    Error::Unsupported(format!("rdma incompatible: {reason}"))
                })?;
            self.capable_parents
                .lock()
                .unwrap()
                .insert(addr.to_string(), (Instant::now(), advertisement.clone()));
            Ok(advertisement)
        }

        /// client builds an RDMAClient for one parent address.
        async fn client(&self, addr: &str) -> Result<RDMAClient> {
            self.check_parent(addr)?;
            let (fabric, capability) = self.fabric().await?;
            // advertisement records its own failures, since only it can tell an unreachable
            // parent apart from one that answered and is incompatible.
            let advertisement = self.advertisement(addr, &capability).await?;
            let mut rendezvous_addr: SocketAddr = addr.parse().map_err(|err| {
                Error::Unsupported(format!("invalid parent piece address {addr}: {err}"))
            })?;
            rendezvous_addr.set_port(advertisement.port);
            Ok(RDMAClient::new(
                self.config.clone(),
                fabric,
                capability,
                rendezvous_addr.to_string(),
            ))
        }
    }

    /// RDMADownloader implements the Downloader trait.
    #[async_trait]
    impl Downloader for RDMADownloader {
        /// download_piece downloads a piece from the other peer over the fabric.
        #[instrument(skip_all)]
        async fn download_piece(
            &self,
            addr: &str,
            number: u32,
            _host_id: &str,
            task_id: &str,
        ) -> Result<(PieceContentStream, u64, String)> {
            let (reader, offset, digest) =
                self.download_piece_stream(addr, number, task_id).await?;
            Ok((self.content_stream(reader), offset, digest))
        }

        /// download_persistent_piece downloads a persistent piece from the other peer over
        /// the fabric.
        #[instrument(skip_all)]
        async fn download_persistent_piece(
            &self,
            addr: &str,
            number: u32,
            _host_id: &str,
            task_id: &str,
        ) -> Result<(PieceContentStream, u64, String)> {
            let client = self.client(addr).await?;
            match client.download_persistent_piece(number, task_id).await {
                Ok((reader, offset, digest)) => {
                    self.record_success(addr);
                    Ok((self.content_stream(reader), offset, digest))
                }
                Err(err) => {
                    if client.fabric_failed() {
                        self.retire_failed_fabric().await;
                    }
                    self.record_failure(addr, Failure::Transport);
                    Err(err)
                }
            }
        }

        /// download_persistent_cache_piece downloads a persistent cache piece from the
        /// other peer over the fabric.
        #[instrument(skip_all)]
        async fn download_persistent_cache_piece(
            &self,
            addr: &str,
            number: u32,
            _host_id: &str,
            task_id: &str,
        ) -> Result<(PieceContentStream, u64, String)> {
            let client = self.client(addr).await?;
            match client
                .download_persistent_cache_piece(number, task_id)
                .await
            {
                Ok((reader, offset, digest)) => {
                    self.record_success(addr);
                    Ok((self.content_stream(reader), offset, digest))
                }
                Err(err) => {
                    if client.fabric_failed() {
                        self.retire_failed_fabric().await;
                    }
                    self.record_failure(addr, Failure::Transport);
                    Err(err)
                }
            }
        }
    }

    impl RDMADownloader {
        /// content_stream adapts an RDMA reader to the chunk stream the Downloader trait returns.
        /// The piece path uses [`Self::download_piece_stream`] instead, which keeps the windows in
        /// registered memory; the paths reached through the trait still stage each window here.
        fn content_stream(
            &self,
            reader: dragonfly_client_storage::client::rdma::RDMAStreamReader,
        ) -> PieceContentStream {
            ReaderStream::with_capacity(reader, self.config.storage.write_buffer_size).boxed()
        }

        /// download_piece_stream returns the concrete RDMA reader so callers can write registered
        /// windows without a staging buffer.
        #[instrument(skip_all)]
        pub async fn download_piece_stream(
            &self,
            addr: &str,
            number: u32,
            task_id: &str,
        ) -> Result<(
            dragonfly_client_storage::client::rdma::RDMAStreamReader,
            u64,
            String,
        )> {
            let client = self.client(addr).await?;
            match client.download_piece(number, task_id).await {
                Ok(downloaded) => {
                    self.record_success(addr);
                    Ok(downloaded)
                }
                Err(err) => {
                    if client.fabric_failed() {
                        self.retire_failed_fabric().await;
                    }
                    self.record_failure(addr, Failure::Transport);
                    Err(err)
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[tokio::test]
        async fn receive_only_config_can_initialize_downloader_fabric() {
            let mut config = Config::default();
            config.download.protocol = "rdma".to_string();
            config.storage.server.rdma.enable = false;
            config.storage.server.rdma.allow_software_provider = true;
            config.storage.server.rdma.fabric_tag = Some("test-fabric".to_string());

            let downloader = RDMADownloader::new(Arc::new(config));
            let (_, capability) = downloader.fabric().await.unwrap();

            assert_eq!(capability.fabric_tag, "test-fabric");
        }

        fn test_downloader() -> RDMADownloader {
            RDMADownloader::new(Arc::new(Config::default()))
        }

        fn backoff_of(downloader: &RDMADownloader, addr: &str) -> Duration {
            downloader.unhealthy_parents.lock().unwrap()[addr].backoff
        }

        #[test]
        fn transport_failures_park_a_parent_and_back_off() {
            let downloader = test_downloader();
            let addr = "127.0.0.1:4001";
            assert!(downloader.check_parent(addr).is_ok());

            // A parent that just failed is skipped rather than probed again by the next piece,
            // which is what turns one broken parent into a rendezvous round trip per piece.
            downloader.record_failure(addr, Failure::Transport);
            assert!(downloader.check_parent(addr).is_err());
            assert_eq!(backoff_of(&downloader, addr), UNHEALTHY_PARENT_MIN_BACKOFF);

            downloader.record_failure(addr, Failure::Transport);
            assert_eq!(
                backoff_of(&downloader, addr),
                UNHEALTHY_PARENT_MIN_BACKOFF * 2
            );

            for _ in 0..16 {
                downloader.record_failure(addr, Failure::Transport);
            }
            assert_eq!(
                backoff_of(&downloader, addr),
                UNHEALTHY_PARENT_MAX_BACKOFF,
                "backoff must stay bounded so a recovered parent is retried"
            );
        }

        #[test]
        fn incompatible_parents_skip_the_doubling() {
            let downloader = test_downloader();
            let addr = "127.0.0.1:4001";

            downloader.record_failure(addr, Failure::Incompatible);
            assert_eq!(backoff_of(&downloader, addr), INCOMPATIBLE_PARENT_TTL);
            assert!(downloader.check_parent(addr).is_err());
        }

        #[test]
        fn success_clears_the_penalty() {
            let downloader = test_downloader();
            let addr = "127.0.0.1:4001";

            downloader.record_failure(addr, Failure::Transport);
            downloader.record_success(addr);
            assert!(downloader.check_parent(addr).is_ok());

            // The next failure starts over at the shortest penalty, so a parent that works is not
            // punished for something that happened to it long ago.
            downloader.record_failure(addr, Failure::Transport);
            assert_eq!(backoff_of(&downloader, addr), UNHEALTHY_PARENT_MIN_BACKOFF);
        }

        #[test]
        fn an_expired_penalty_allows_a_retry_without_resetting_the_backoff() {
            let downloader = test_downloader();
            let addr = "127.0.0.1:4001";

            downloader.record_failure(addr, Failure::Transport);
            downloader.record_failure(addr, Failure::Transport);
            downloader
                .unhealthy_parents
                .lock()
                .unwrap()
                .get_mut(addr)
                .unwrap()
                .until = Instant::now() - Duration::from_secs(1);

            assert!(downloader.check_parent(addr).is_ok());
            downloader.record_failure(addr, Failure::Transport);
            assert_eq!(
                backoff_of(&downloader, addr),
                UNHEALTHY_PARENT_MIN_BACKOFF * 4,
                "a retry that fails again must keep escalating"
            );
        }
    }
}
