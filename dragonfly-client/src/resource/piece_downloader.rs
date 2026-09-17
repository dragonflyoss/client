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

    /// Creates a new QUICClient connected to the given address.
    async fn make_client(&self, addr: &String) -> Result<QUICClient> {
        QUICClient::new(self.config.clone(), addr.clone()).await
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

    /// Returns a client entry by the address, recreating the client if its
    /// connection is closed.
    async fn get_client_entry(&self, key: String, addr: String) -> Result<Entry<QUICClient>> {
        let entry = self.client_pool.entry(&key, &addr).await?;
        if !entry.client.is_closed() {
            return Ok(entry);
        }

        self.client_pool.remove_entry(&key).await;
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
    use dragonfly_client_storage::rdma::fabric::{Fabric, RegisteredMemoryBudget};
    use dragonfly_client_storage::rdma::rendezvous::{
        PieceKind, RdmaAdvertisement, WireCapability, ERROR_CODE_BUSY, ERROR_CODE_INCOMPATIBLE,
        ERROR_CODE_NOT_FOUND, ERROR_CODE_TOO_LARGE,
    };
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

    /// MIN_DISCOVERY_BUDGET is the least time worth spending on a capability probe. Below it the
    /// attempt declines locally rather than starting a request that will time out and be charged
    /// to a parent that was never reached.
    const MIN_DISCOVERY_BUDGET: Duration = Duration::from_millis(100);

    /// Bound daemon memory even when scheduling sees continual parent churn.
    const MAX_CACHED_PARENTS: usize = 4096;

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
        recorded_at: Instant,

        /// kind is what produced `backoff`. Only a transport backoff is a doubling sequence, so
        /// only it may be used as the basis for the next one.
        kind: Failure,
    }

    /// FabricState tracks the lazily initialized process-shared fabric endpoint.
    enum FabricState {
        /// Uninitialized means no initialization has been attempted yet.
        Uninitialized,

        /// Initialization remains owned here when the requesting piece is cancelled.
        Initializing(tokio::task::JoinHandle<Result<(Arc<Fabric>, WireCapability)>>),

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

        memory_budget: Arc<RegisteredMemoryBudget>,

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
            let budget = Arc::new(RegisteredMemoryBudget::new(
                config.storage.server.rdma.max_registered_bytes.as_u64(),
            ));
            Self::new_with_budget(config, budget)
        }

        /// Shares one registered-memory cap with the serving endpoint and retired generations.
        pub fn new_with_budget(
            config: Arc<Config>,
            memory_budget: Arc<RegisteredMemoryBudget>,
        ) -> Self {
            Self {
                config,
                memory_budget,
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
                    *state = FabricState::Failed(Instant::now());
                    return Err(Error::Unsupported(
                        "rdma fabric is cooling down after failure".to_string(),
                    ));
                }
                FabricState::Failed(at) if at.elapsed() < FABRIC_RETRY_INTERVAL => {
                    return Err(Error::Unsupported(
                        "rdma fabric initialization failed recently".to_string(),
                    ))
                }
                _ => {}
            }
            if !matches!(&*state, FabricState::Initializing(_)) {
                let rdma_config = &self.config.storage.server.rdma;
                let Some(fabric_tag) = rdma_config.fabric_tag.clone().filter(|tag| !tag.is_empty())
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
                let device = rdma_config.device.clone();
                let budget = self.memory_budget.clone();
                let allow_software = rdma_config.allow_software_provider;
                *state = FabricState::Initializing(tokio::task::spawn_blocking(move || {
                    let fabric = Arc::new(Fabric::new_with_budget(
                        provider.as_deref(),
                        device.as_deref(),
                        budget,
                        allow_software,
                    )?);
                    let capability = WireCapability {
                        provider: fabric.provider().to_string(),
                        fabric_tag,
                    };
                    Ok((fabric, capability))
                }));
            }
            let FabricState::Initializing(handle) = &mut *state else {
                unreachable!()
            };
            match handle
                .await
                .map_err(Error::TokioJoinError)
                .and_then(|result| result)
            {
                Ok((fabric, capability)) => {
                    info!(
                        "rdma downloader ready: provider {}, fabric tag {}",
                        capability.provider, capability.fabric_tag
                    );
                    *state = FabricState::Ready(fabric.clone(), capability.clone());
                    Ok((fabric, capability))
                }
                Err(err) => {
                    warn!("rdma fabric initialization failed: {err}");
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
                *state = FabricState::Failed(Instant::now());
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
            // Expired penalties are only consulted through check_parent, which leaves them in
            // place. Dropping them here keeps a long-lived daemon from carrying entries for
            // parents that recovered, and keeps the eviction scan below meaningful.
            let now = Instant::now();
            unhealthy_parents.retain(|held, penalty| held == addr || penalty.until > now);

            let backoff = match failure {
                // Incompatibility is a stable fact about the peer, so there is nothing for a
                // doubling backoff to discover.
                Failure::Incompatible => INCOMPATIBLE_PARENT_TTL,
                // Double only a previous transport backoff. An earlier incompatibility parks the
                // entry at the 60s ceiling, and reusing that as the base would send the first
                // transport failure straight to the maximum instead of starting at 2s.
                Failure::Transport => unhealthy_parents
                    .get(addr)
                    .filter(|penalty| penalty.kind == Failure::Transport)
                    .map(|penalty| (penalty.backoff * 2).min(UNHEALTHY_PARENT_MAX_BACKOFF))
                    .unwrap_or(UNHEALTHY_PARENT_MIN_BACKOFF),
            };

            if unhealthy_parents.len() >= MAX_CACHED_PARENTS
                && !unhealthy_parents.contains_key(addr)
            {
                if let Some(oldest) = unhealthy_parents
                    .iter()
                    .min_by_key(|(_, penalty)| penalty.recorded_at)
                    .map(|(addr, _)| addr.clone())
                {
                    unhealthy_parents.remove(&oldest);
                }
            }
            unhealthy_parents.insert(
                addr.to_string(),
                ParentPenalty {
                    until: Instant::now() + backoff,
                    backoff,
                    recorded_at: Instant::now(),
                    kind: failure,
                },
            );
        }

        /// record_success clears a parent's penalty once an attempt against it works again.
        pub(crate) fn record_success(&self, addr: &str) {
            self.unhealthy_parents.lock().unwrap().remove(addr);
        }

        /// advertisement returns a cached live capability or discovers it through the parent's
        /// advertised TCP piece endpoint.
        async fn advertisement(
            &self,
            addr: &str,
            local: &WireCapability,
            deadline: tokio::time::Instant,
        ) -> Result<RdmaAdvertisement> {
            let cached = self.capable_parents.lock().unwrap().get(addr).cloned();
            if let Some((at, advertisement)) = cached {
                if at.elapsed() < CAPABLE_PARENT_TTL {
                    return Ok(advertisement);
                }
                self.capable_parents.lock().unwrap().remove(addr);
            }

            // Opening the local fabric happens before this and can consume most of the attempt
            // budget on a cold first use. Handing discovery what is left would time it out and
            // record that against the parent, backing off a peer that was never contacted.
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining < MIN_DISCOVERY_BUDGET {
                return Err(Error::Unknown(
                    "no time left to discover rdma capability after local setup".to_string(),
                ));
            }

            let advertisement = discover(
                addr,
                self.config
                    .storage
                    .server
                    .rdma
                    .transfer_timeout
                    .min(remaining),
            )
            .await
            .inspect_err(|err| {
                self.record_request_failure(addr, err);
            })?;
            local
                .compatible(&advertisement.capability)
                .map_err(|reason| {
                    self.record_failure(addr, Failure::Incompatible);
                    Error::Unsupported(format!("rdma incompatible: {reason}"))
                })?;
            self.cache_advertisement(addr, advertisement.clone());
            Ok(advertisement)
        }

        fn cache_advertisement(&self, addr: &str, advertisement: RdmaAdvertisement) {
            let mut parents = self.capable_parents.lock().unwrap();
            if parents.len() >= MAX_CACHED_PARENTS && !parents.contains_key(addr) {
                if let Some(oldest) = parents
                    .iter()
                    .min_by_key(|(_, (at, _))| *at)
                    .map(|(addr, _)| addr.clone())
                {
                    parents.remove(&oldest);
                }
            }
            parents.insert(addr.to_owned(), (Instant::now(), advertisement));
        }

        /// client builds an RDMAClient for one parent address.
        async fn client(&self, addr: &str, deadline: tokio::time::Instant) -> Result<RDMAClient> {
            self.check_parent(addr)?;
            let (fabric, capability) = self.fabric().await?;
            // advertisement records its own failures, since only it can tell an unreachable
            // parent apart from one that answered and is incompatible.
            let advertisement = self.advertisement(addr, &capability, deadline).await?;
            let mut rendezvous_addr: SocketAddr = addr.parse().map_err(|err| {
                Error::Unsupported(format!("invalid parent piece address {addr}: {err}"))
            })?;
            rendezvous_addr.set_port(advertisement.port);
            Ok(RDMAClient::new(
                self.config.clone(),
                fabric,
                capability,
                rendezvous_addr.to_string(),
            )
            .with_deadline(deadline))
        }
    }

    // RDMADownloader deliberately does not implement Downloader. That trait cannot carry the
    // caller's expected offset and length, so an implementation would have to write whatever
    // range the parent reported. download_stream is the validating entry point and the only
    // one production uses.

    impl RDMADownloader {
        fn content_stream(
            &self,
            reader: dragonfly_client_storage::client::rdma::RDMAStreamReader,
        ) -> PieceContentStream {
            ReaderStream::with_capacity(reader, self.config.storage.write_buffer_size).boxed()
        }

        fn record_request_failure(&self, addr: &str, err: &Error) {
            match err {
                Error::RdmaRejected {
                    code: ERROR_CODE_INCOMPATIBLE,
                    ..
                } => self.record_failure(addr, Failure::Incompatible),
                Error::RdmaRejected {
                    code: ERROR_CODE_BUSY | ERROR_CODE_NOT_FOUND | ERROR_CODE_TOO_LARGE,
                    ..
                }
                | Error::InvalidParameter
                | Error::Unsupported(_) => {}
                _ => self.record_failure(addr, Failure::Transport),
            }
        }

        /// Called only for a failed receive or integrity check, never a local storage error.
        pub(crate) async fn record_transfer_failure(&self, addr: &str) {
            let local_failure = matches!(&*self.fabric.lock().await, FabricState::Ready(fabric, _) if fabric.is_failed());
            self.retire_failed_fabric().await;
            if !local_failure {
                self.record_failure(addr, Failure::Transport);
            }
        }

        /// Opens a stream without claiming success. Only the caller that has stored and
        /// verified the complete piece may clear the parent's accumulated penalty.
        async fn open_stream(
            &self,
            kind: PieceKind,
            addr: &str,
            number: u32,
            task_id: &str,
            deadline: tokio::time::Instant,
        ) -> Result<(
            dragonfly_client_storage::client::rdma::RDMAStreamReader,
            u64,
            String,
        )> {
            let client = tokio::time::timeout_at(deadline, self.client(addr, deadline)).await??;
            let result = match kind {
                PieceKind::Piece => client.download_piece(number, task_id).await,
                PieceKind::PersistentPiece => {
                    client.download_persistent_piece(number, task_id).await
                }
                PieceKind::PersistentCachePiece => {
                    client
                        .download_persistent_cache_piece(number, task_id)
                        .await
                }
            };
            if let Err(err) = &result {
                if client.fabric_failed() {
                    self.retire_failed_fabric().await;
                } else {
                    self.record_request_failure(addr, err);
                }
            }
            result
        }

        /// Reject metadata mismatches before any write can target the task file.
        #[allow(clippy::too_many_arguments)]
        pub async fn download_stream(
            &self,
            kind: PieceKind,
            addr: &str,
            number: u32,
            task_id: &str,
            expected_offset: u64,
            expected_length: u64,
            deadline: tokio::time::Instant,
        ) -> Result<(PieceContentStream, u64, String)> {
            let (reader, offset, digest) = self
                .open_stream(kind, addr, number, task_id, deadline)
                .await?;
            if offset != expected_offset || reader.length() != expected_length {
                self.record_failure(addr, Failure::Transport);
                return Err(Error::Unknown(format!("rdma piece range mismatch: expected {expected_offset}+{expected_length}, got {offset}+{}", reader.length())));
            }
            Ok((self.content_stream(reader), offset, digest))
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
        fn parent_churn_keeps_both_caches_bounded() {
            let downloader = test_downloader();
            for index in 0..MAX_CACHED_PARENTS + 10 {
                downloader.record_failure(&format!("failed-{index}"), Failure::Transport);
                downloader.cache_advertisement(
                    &format!("capable-{index}"),
                    RdmaAdvertisement {
                        port: 1,
                        capability: WireCapability {
                            provider: "efa".to_string(),
                            fabric_tag: "test".to_string(),
                        },
                    },
                );
            }
            assert_eq!(
                downloader.unhealthy_parents.lock().unwrap().len(),
                MAX_CACHED_PARENTS
            );
            assert_eq!(
                downloader.capable_parents.lock().unwrap().len(),
                MAX_CACHED_PARENTS
            );
            assert!(!downloader
                .unhealthy_parents
                .lock()
                .unwrap()
                .contains_key("failed-0"));
            assert!(!downloader
                .capable_parents
                .lock()
                .unwrap()
                .contains_key("capable-0"));
        }

        #[test]
        fn capacity_and_missing_pieces_do_not_penalize_the_parent() {
            let downloader = test_downloader();
            let addr = "127.0.0.1:4001";
            for code in [ERROR_CODE_BUSY, ERROR_CODE_NOT_FOUND, ERROR_CODE_TOO_LARGE] {
                downloader.record_request_failure(
                    addr,
                    &Error::RdmaRejected {
                        code,
                        message: "declined".to_string(),
                    },
                );
                assert!(downloader.unhealthy_parents.lock().unwrap().is_empty());
            }
            downloader.record_request_failure(
                addr,
                &Error::RdmaRejected {
                    code: ERROR_CODE_INCOMPATIBLE,
                    message: "incompatible".to_string(),
                },
            );
            assert_eq!(backoff_of(&downloader, addr), INCOMPATIBLE_PARENT_TTL);
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
