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

use crate::grpc::dfdaemon_upload::DfdaemonUploadClient;
use crate::resource::piece_collector::CollectedParent;
use dashmap::DashMap;
use dragonfly_api::common::v2::{Host, Peer};
use dragonfly_api::dfdaemon::v2::SyncHostRequest;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Error;
use dragonfly_client_core::Result;
use dragonfly_client_util::id_generator::IDGenerator;
use dragonfly_client_util::shutdown::{self, Shutdown};
use rand::distr::weighted::WeightedIndex;
use rand::distr::Distribution;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, warn, Instrument};

/// Connection manages a single parent connection.
struct Connection {
    /// client is the dfdaemon upload client for this parent.
    client: DfdaemonUploadClient,

    /// connection_guard tracks how many download tasks are using this connection.
    connection_guard: ConnectionGuard,

    /// shutdown is used to signal the sync host to stop.
    shutdown: Shutdown,
}

impl Connection {
    /// new creates a new Connection.
    pub fn new(client: DfdaemonUploadClient) -> Self {
        Self {
            client,
            connection_guard: ConnectionGuard::new(Arc::new(AtomicUsize::new(0))),
            shutdown: Shutdown::new(),
        }
    }

    /// connection_guard increments the reference count.
    pub fn connection_guard(&self) {
        self.connection_guard.acquire();
    }

    /// active_count returns the number of active requests.
    pub fn active_requests(&self) -> usize {
        self.connection_guard.active_requests()
    }

    /// release_request decrements the reference count.
    pub fn release_request(&self) {
        self.connection_guard.release();
    }

    /// shutdown triggers shutdown of the sync host.
    pub fn shutdown(&self) {
        self.shutdown.trigger();
    }
}

/// ConnectionGuard automatically manages reference counting for parent connections.
pub struct ConnectionGuard {
    active_requests: Arc<AtomicUsize>,
}

impl ConnectionGuard {
    fn new(active_connections: Arc<AtomicUsize>) -> Self {
        Self {
            active_requests: active_connections,
        }
    }

    fn active_requests(&self) -> usize {
        self.active_requests.load(Ordering::SeqCst)
    }

    fn acquire(&self) {
        self.active_requests.fetch_add(1, Ordering::SeqCst);
    }

    fn release(&self) {
        self.active_requests.fetch_sub(1, Ordering::SeqCst);
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.active_requests.fetch_sub(1, Ordering::SeqCst);
    }
}

/// ParentSelector manages parent connections and selects optimal parents.
pub struct ParentSelector {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// timeout is the timeout for the sync host.
    timeout: Duration,

    /// host_id is the id of the host.
    host_id: String,

    /// peer_id is the id of the peer.
    peer_id: String,

    /// weights stores the latest host information and bandwidth weights for different parents.
    weights: Arc<DashMap<String, u32>>,

    /// connections stores parent connections with reference counting.
    connections: Arc<DashMap<String, Connection>>,

    /// shutdown is used to shutdown the garbage collector.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the garbage collector is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// ParentSelector implements the parent selector.
impl ParentSelector {
    /// new returns a ParentSelector.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        host_id: String,
        peer_id: String,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> ParentSelector {
        let config = config.clone();
        let timeout = config.download.parent_selector.timeout;
        let weights = Arc::new(DashMap::new());

        Self {
            config,
            timeout,
            host_id,
            peer_id,
            weights,
            connections: Arc::new(DashMap::new()),
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// sync_host is a sub thread to sync host info from the parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn sync_host(
        host_id: String,
        peer_id: String,
        remote_host_id: String,
        weights: Arc<DashMap<String, u32>>,
        timeout: Duration,
        client: DfdaemonUploadClient,
        mut shutdown: Shutdown,
        mut dfdaemon_shutdown: Shutdown,
    ) -> Result<()> {
        let response = client
            .sync_host(SyncHostRequest { host_id, peer_id })
            .await
            .inspect_err(|err| {
                error!("sync host from host {} failed: {}", remote_host_id, err);
            })?;

        let out_stream = response.into_inner().timeout(timeout);
        tokio::pin!(out_stream);

        loop {
            tokio::select! {
                result = out_stream.try_next() => {
                    match result.inspect_err(|err| {
                        error!("sync host from host {} failed: {}", remote_host_id, err);
                    })? {
                        Some(message) => {
                            debug!("sync host from host {} received message", remote_host_id);
                            let message = message?;

                            // Calculate weight from host information.
                            let weight = Self::get_idle_upload_rate(&message) as u32;

                            // Update the parent's host info with calculated weight.
                            weights.insert(remote_host_id.clone(), weight);
                        }
                        None => break,
                    }
                }
                _ = shutdown.recv() => {
                    debug!("parent selector: shutdown signal received for host {}", remote_host_id);
                    break;
                }
                _ = dfdaemon_shutdown.recv() => {
                    info!("parent selector shutting down");
                    break;
                }
            }
        }

        Ok(())
    }

    /// select_parent selects the best parent for the task based on bandwidth.
    pub fn select_parent(&self, parents: Vec<CollectedParent>) -> Option<CollectedParent> {
        let remote_hosts: Vec<String> = parents
            .iter()
            .map(|parent| {
                IDGenerator::new(
                    parent.host.as_ref().unwrap().ip.clone(),
                    parent.host.as_ref().unwrap().hostname.clone(),
                    false,
                )
                .host_id()
            })
            .collect();
        let weights: Vec<u32> = remote_hosts
            .iter()
            .map(|remote_host| self.weights.get(remote_host).map(|w| *w).unwrap_or(0))
            .collect();

        match WeightedIndex::new(weights) {
            Ok(dist) => {
                let mut rng = rand::rng();
                let index = dist.sample(&mut rng);
                let selected_parent = &parents[index];
                debug!("selected parent {}", selected_parent.id);

                Some(selected_parent.clone())
            }
            Err(_) => parents.get(fastrand::usize(..parents.len())).cloned(),
        }
    }

    /// unregister_parents triggers shutdown.
    pub fn unregister_parents(&self, parents: Vec<Peer>) {
        for parent in parents {
            let host_id = IDGenerator::new(
                parent.host.as_ref().unwrap().ip.clone(),
                parent.host.as_ref().unwrap().hostname.clone(),
                false,
            )
            .host_id();

            if let Some(connection) = self.connections.get(&host_id) {
                connection.release_request();
                if connection.active_requests() == 0 {
                    info!("shutting down sync host for parent {}", host_id);
                    connection.shutdown();
                    drop(connection);
                    self.weights.remove(&host_id);
                    self.connections.remove(&host_id);
                }
            }
        }
    }

    /// get_connection_client returns a client for the given parent, creating the connection if needed.
    pub async fn get_connection_client(
        &self,
        parent: &Option<Host>,
    ) -> Result<DfdaemonUploadClient> {
        let Some(parent) = parent else {
            error!("parent is not found");
            return Err(Error::InvalidPeer(String::new()));
        };

        let remote_host_id =
            IDGenerator::new(parent.ip.clone(), parent.hostname.clone(), false).host_id();

        // Try to get existing connection
        if let Some(connection) = self.connections.get(&remote_host_id) {
            connection.connection_guard();
            return Ok(connection.client.clone());
        }

        let client = DfdaemonUploadClient::new(
            self.config.clone(),
            format!("http://{}:{}", parent.ip, parent.port),
            false,
        )
        .await?;

        let connection = Connection::new(client.clone());
        connection.connection_guard();

        match self.connections.entry(remote_host_id.clone()) {
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(connection);
                Ok(client)
            }
            dashmap::mapref::entry::Entry::Occupied(entry) => {
                let existing_connection = entry.get();
                existing_connection.connection_guard();
                let client = existing_connection.client.clone();
                Ok(client)
            }
        }
    }

    /// register_parents registers multiple parents.
    pub async fn register_parents(&self, parents: &[CollectedParent]) -> Result<()> {
        let dfdaemon_shutdown = self.shutdown.clone();

        let mut join_set = JoinSet::<Result<()>>::new();

        for parent in parents {
            let remote_host_id = IDGenerator::new(
                parent.host.as_ref().unwrap().ip.clone(),
                parent.host.as_ref().unwrap().hostname.clone(),
                false,
            )
            .host_id();

            // Check if connection already has active requests
            if let Some(connection) = self.connections.get(&remote_host_id) {
                if connection.active_requests() > 0 {
                    debug!("sync host already running for parent {}", parent.id);
                    continue;
                }
            }

            // Get or create connection for the sync host.
            let client = self.get_connection_client(&parent.host).await?;

            // Start sync host for this parent.
            let parent = parent.clone();
            let weights = self.weights.clone();
            let timeout = self.timeout;
            let host_id = self.host_id.clone();
            let peer_id = self.peer_id.clone();
            let shutdown = self
                .connections
                .get(&remote_host_id)
                .map(|conn| conn.shutdown.clone())
                .unwrap_or_default();
            let dfdaemon_shutdown_clone = dfdaemon_shutdown.clone();

            join_set.spawn(
                async move {
                    info!("started sync host for parent {}", parent.id);

                    if let Err(err) = Self::sync_host(
                        host_id,
                        peer_id,
                        remote_host_id.clone(),
                        weights.clone(),
                        timeout,
                        client,
                        shutdown,
                        dfdaemon_shutdown_clone,
                    )
                    .await
                    {
                        error!("sync host for parent {} failed: {}", remote_host_id, err);
                        return Err(err);
                    }

                    Ok(())
                }
                .in_current_span(),
            );
        }

        // Spawn a task to manage this JoinSet.
        tokio::spawn(async move {
            while let Some(result) = join_set.join_next().await {
                match result {
                    Ok(Ok(_)) => debug!("parent sync host completed successfully"),
                    Ok(Err(_)) => debug!("parent sync host failed"),
                    Err(err) => debug!("parent sync host join error: {}", err),
                }
            }
        });

        Ok(())
    }

    /// get_idle_upload_rate returns the available upload rate of a host.
    fn get_idle_upload_rate(host: &Host) -> u64 {
        let network = match &host.network {
            Some(network) => network,
            None => return 0,
        };

        let tx_bandwidth = network.tx_bandwidth.unwrap_or(0);
        let max_tx_bandwidth = network.max_tx_bandwidth;

        if tx_bandwidth < max_tx_bandwidth {
            max_tx_bandwidth.saturating_sub(tx_bandwidth)
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_api::common::v2::{CacheTask, Host, PersistentCacheTask, Task};
    use dragonfly_api::dfdaemon::v2::*;
    use dragonfly_client_config::dfdaemon::Config;
    use dragonfly_client_util::shutdown::Shutdown;
    use std::pin::Pin;
    use std::io::ErrorKind;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::TcpListenerStream;
    use tokio_stream::Stream;
    use tonic::transport::Server;
    use tonic::{Request, Response, Status};
    use dragonfly_api::dfdaemon::v2::dfdaemon_upload_server::{
        DfdaemonUpload, DfdaemonUploadServer as DfdaemonUploadGRPCServer,
    };

    type GrpcResult<T> = std::result::Result<T, Status>;
    type BoxStream<T> = Pin<Box<dyn Stream<Item = GrpcResult<T>> + Send>>;

    struct MockUploadService {
        hosts: Vec<Host>,
    }

    #[tonic::async_trait]
    impl DfdaemonUpload for MockUploadService {
        type DownloadTaskStream = BoxStream<DownloadTaskResponse>;
        type DownloadCacheTaskStream = BoxStream<DownloadCacheTaskResponse>;
        type SyncCachePiecesStream = BoxStream<SyncCachePiecesResponse>;
        type SyncPiecesStream = BoxStream<SyncPiecesResponse>;
        type SyncHostStream = BoxStream<Host>;
        type DownloadPersistentCacheTaskStream = BoxStream<DownloadPersistentCacheTaskResponse>;
        type SyncPersistentCachePiecesStream = BoxStream<SyncPersistentCachePiecesResponse>;

        async fn download_task(
            &self,
            _request: Request<DownloadTaskRequest>,
        ) -> GrpcResult<Response<Self::DownloadTaskStream>> {
            Err(Status::unimplemented("download_task"))
        }

        async fn stat_task(
            &self,
            _request: Request<StatTaskRequest>,
        ) -> GrpcResult<Response<Task>> {
            Err(Status::unimplemented("stat_task"))
        }

        async fn list_task_entries(
            &self,
            _request: Request<ListTaskEntriesRequest>,
        ) -> GrpcResult<Response<ListTaskEntriesResponse>> {
            Err(Status::unimplemented("list_task_entries"))
        }

        async fn delete_task(
            &self,
            _request: Request<DeleteTaskRequest>,
        ) -> GrpcResult<Response<()>> {
            Err(Status::unimplemented("delete_task"))
        }

        async fn sync_pieces(
            &self,
            _request: Request<SyncPiecesRequest>,
        ) -> GrpcResult<Response<Self::SyncPiecesStream>> {
            Err(Status::unimplemented("sync_pieces"))
        }

        async fn download_piece(
            &self,
            _request: Request<DownloadPieceRequest>,
        ) -> GrpcResult<Response<DownloadPieceResponse>> {
            Err(Status::unimplemented("download_piece"))
        }

        async fn download_cache_task(
            &self,
            _request: Request<DownloadCacheTaskRequest>,
        ) -> GrpcResult<Response<Self::DownloadCacheTaskStream>> {
            Err(Status::unimplemented("download_cache_task"))
        }

        async fn stat_cache_task(
            &self,
            _request: Request<StatCacheTaskRequest>,
        ) -> GrpcResult<Response<CacheTask>> {
            Err(Status::unimplemented("stat_cache_task"))
        }

        async fn delete_cache_task(
            &self,
            _request: Request<DeleteCacheTaskRequest>,
        ) -> GrpcResult<Response<()>> {
            Err(Status::unimplemented("delete_cache_task"))
        }

        async fn sync_cache_pieces(
            &self,
            _request: Request<SyncCachePiecesRequest>,
        ) -> GrpcResult<Response<Self::SyncCachePiecesStream>> {
            Err(Status::unimplemented("sync_cache_pieces"))
        }

        async fn download_cache_piece(
            &self,
            _request: Request<DownloadCachePieceRequest>,
        ) -> GrpcResult<Response<DownloadCachePieceResponse>> {
            Err(Status::unimplemented("download_cache_piece"))
        }

        async fn sync_host(
            &self,
            _request: Request<SyncHostRequest>,
        ) -> GrpcResult<Response<Self::SyncHostStream>> {
            let stream = tokio_stream::iter(self.hosts.clone().into_iter().map(Ok));
            Ok(Response::new(Box::pin(stream) as Self::SyncHostStream))
        }

        async fn download_persistent_cache_task(
            &self,
            _request: Request<DownloadPersistentCacheTaskRequest>,
        ) -> GrpcResult<Response<Self::DownloadPersistentCacheTaskStream>> {
            Err(Status::unimplemented("download_persistent_cache_task"))
        }

        async fn update_persistent_cache_task(
            &self,
            _request: Request<UpdatePersistentCacheTaskRequest>,
        ) -> GrpcResult<Response<()>> {
            Err(Status::unimplemented("update_persistent_cache_task"))
        }

        async fn stat_persistent_cache_task(
            &self,
            _request: Request<StatPersistentCacheTaskRequest>,
        ) -> GrpcResult<Response<PersistentCacheTask>> {
            Err(Status::unimplemented("stat_persistent_cache_task"))
        }

        async fn delete_persistent_cache_task(
            &self,
            _request: Request<DeletePersistentCacheTaskRequest>,
        ) -> GrpcResult<Response<()>> {
            Err(Status::unimplemented("delete_persistent_cache_task"))
        }

        async fn sync_persistent_cache_pieces(
            &self,
            _request: Request<SyncPersistentCachePiecesRequest>,
        ) -> GrpcResult<Response<Self::SyncPersistentCachePiecesStream>> {
            Err(Status::unimplemented("sync_persistent_cache_pieces"))
        }

        async fn download_persistent_cache_piece(
            &self,
            _request: Request<DownloadPersistentCachePieceRequest>,
        ) -> GrpcResult<Response<DownloadPersistentCachePieceResponse>> {
            Err(Status::unimplemented("download_persistent_cache_piece"))
        }

        async fn exchange_ib_verbs_queue_pair_endpoint(
            &self,
            _request: Request<ExchangeIbVerbsQueuePairEndpointRequest>,
        ) -> GrpcResult<Response<ExchangeIbVerbsQueuePairEndpointResponse>> {
            Err(Status::unimplemented(
                "exchange_ib_verbs_queue_pair_endpoint",
            ))
        }
    }

    fn create_parent_selector() -> ParentSelector {
        let (shutdown_complete_tx, _shutdown_complete_rx) = mpsc::unbounded_channel();
        ParentSelector::new(
            Arc::new(Config::default()),
            "local-host".to_string(),
            "peer-id".to_string(),
            Shutdown::new(),
            shutdown_complete_tx,
        )
    }

    #[tokio::test]
    async fn test_register_parents() {
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == ErrorKind::PermissionDenied => {
                eprintln!("register_parents_syncs_weights_and_connections skipped: {err}");
                return;
            }
            Err(err) => panic!("bind listener: {err}"),
        };
        let addr = listener.local_addr().unwrap();

        let mock_hosts = vec![Host {
            network: Some(dragonfly_api::common::v2::Network {
                max_tx_bandwidth: 100,
                tx_bandwidth: Some(40),
                ..Default::default()
            }),
            ..Default::default()
        }];

        tokio::spawn(async move {
            Server::builder()
                .add_service(DfdaemonUploadGRPCServer::new(MockUploadService {
                    hosts: mock_hosts,
                }))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let parent = CollectedParent {
            id: "parent-grpc".to_string(),
            host: Some(Host {
                ip: addr.ip().to_string(),
                hostname: "grpc-parent".to_string(),
                port: addr.port() as i32,
                ..Default::default()
            }),
            download_ip: None,
            download_tcp_port: None,
            download_quic_port: None,
        };

        let selector = create_parent_selector();

        selector
            .register_parents(&[parent.clone()])
            .await
            .expect("register parents");

        tokio::time::sleep(Duration::from_millis(200)).await;

        let host_id = IDGenerator::new(
            parent.host.as_ref().unwrap().ip.clone(),
            parent.host.as_ref().unwrap().hostname.clone(),
            false,
        )
        .host_id();

        let weight = selector.weights.get(&host_id).map(|w| *w);
        assert_eq!(weight, Some(60), "weight should use idle upload rate");
        assert!(
            selector.connections.get(&host_id).is_some(),
            "connection should be tracked"
        );
    }

    #[tokio::test]
    async fn test_unregister_parents() {
        let listener = match tokio::net::TcpListener::bind("127.0.0.1:0").await {
            Ok(listener) => listener,
            Err(err) if err.kind() == ErrorKind::PermissionDenied => {
                eprintln!("unregister_parents_cleans_up_connections_and_weights skipped: {err}");
                return;
            }
            Err(err) => panic!("bind listener: {err}"),
        };
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            Server::builder()
                .add_service(DfdaemonUploadGRPCServer::new(MockUploadService {
                    hosts: vec![Host {
                        network: Some(dragonfly_api::common::v2::Network {
                            max_tx_bandwidth: 100,
                            tx_bandwidth: Some(10),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }],
                }))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        let parent = CollectedParent {
            id: "parent-to-clean".to_string(),
            host: Some(Host {
                ip: addr.ip().to_string(),
                hostname: "grpc-parent-clean".to_string(),
                port: addr.port() as i32,
                ..Default::default()
            }),
            download_ip: None,
            download_tcp_port: None,
            download_quic_port: None,
        };

        let selector = create_parent_selector();

        selector.register_parents(&[parent.clone()]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;

        let peer = Peer {
            id: parent.id.clone(),
            host: parent.host.clone(),
            ..Default::default()
        };
        selector.unregister_parents(vec![peer]);

        tokio::time::sleep(Duration::from_millis(50)).await;

        let host_id = IDGenerator::new(
            parent.host.as_ref().unwrap().ip.clone(),
            parent.host.as_ref().unwrap().hostname.clone(),
            false,
        )
        .host_id();

        assert!(
            selector.weights.get(&host_id).is_none(),
            "weight should be removed after unregister"
        );
        assert!(
            selector.connections.get(&host_id).is_none(),
            "connection should be removed after unregister"
        );
    }

    #[test]
    fn test_select_parent() {
        let parent_a = CollectedParent {
            id: "parent-a".to_string(),
            host: Some(Host {
                ip: "192.168.0.10".to_string(),
                hostname: "host-a".to_string(),
                port: 6500,
                ..Default::default()
            }),
            download_ip: None,
            download_tcp_port: None,
            download_quic_port: None,
        };
        let parent_b = CollectedParent {
            id: "parent-b".to_string(),
            host: Some(Host {
                ip: "192.168.0.11".to_string(),
                hostname: "host-b".to_string(),
                port: 6500,
                ..Default::default()
            }),
            download_ip: None,
            download_tcp_port: None,
            download_quic_port: None,
        };

        let parent_b_host_id = IDGenerator::new(
            parent_b.host.as_ref().unwrap().ip.clone(),
            parent_b.host.as_ref().unwrap().hostname.clone(),
            false,
        )
        .host_id();

        struct TestCase {
            name: &'static str,
            parents: Vec<CollectedParent>,
            weighted_host: Option<String>,
            expected_ids: Vec<String>,
        }

        let test_cases = vec![
            TestCase {
                name: "prefers weighted parent_b",
                parents: vec![parent_a.clone(), parent_b.clone()],
                weighted_host: Some(parent_b_host_id.clone()),
                expected_ids: vec![parent_b.id.clone()],
            },
            TestCase {
                name: "falls back when no weights",
                parents: vec![parent_a.clone(), parent_b.clone()],
                weighted_host: None,
                expected_ids: vec![parent_a.id.clone(), parent_b.id.clone()],
            },
        ];

        for case in test_cases {
            let (shutdown_complete_tx, _shutdown_complete_rx) = mpsc::unbounded_channel();
            let selector = ParentSelector::new(
                Arc::new(Config::default()),
                "local-host".to_string(),
                "peer-id".to_string(),
                Shutdown::new(),
                shutdown_complete_tx,
            );

            if let Some(weighted_host) = case.weighted_host {
                selector.weights.insert(weighted_host, 100);
            }

            let selected = selector
                .select_parent(case.parents.clone())
                .unwrap_or_else(|| panic!("{}: expected a parent to be selected", case.name));

            assert!(
                case.expected_ids.contains(&selected.id),
                "{}: expected selected parent to be in {:?}, got {}",
                case.name,
                case.expected_ids,
                selected.id
            );
        }
    }

    #[test]
    fn test_get_idle_upload_rate() {
        struct TestCase {
            name: &'static str,
            host: Host,
            expected: u64,
        }

        let test_cases = vec![
            TestCase {
                name: "no network",
                host: Host {
                    network: None,
                    ..Default::default()
                },
                expected: 0,
            },
            TestCase {
                name: "tx_bandwidth none",
                host: Host {
                    network: Some(dragonfly_api::common::v2::Network {
                        tx_bandwidth: None,
                        max_tx_bandwidth: 100,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                expected: 100,
            },
            TestCase {
                name: "idle bandwidth",
                host: Host {
                    network: Some(dragonfly_api::common::v2::Network {
                        tx_bandwidth: Some(50),
                        max_tx_bandwidth: 100,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                expected: 50,
            },
            TestCase {
                name: "no idle bandwidth",
                host: Host {
                    network: Some(dragonfly_api::common::v2::Network {
                        tx_bandwidth: Some(100),
                        max_tx_bandwidth: 100,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                expected: 0,
            },
            TestCase {
                name: "tx greater than max",
                host: Host {
                    network: Some(dragonfly_api::common::v2::Network {
                        tx_bandwidth: Some(150),
                        max_tx_bandwidth: 100,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                expected: 0,
            },
        ];

        for test_case in test_cases {
            assert_eq!(
                ParentSelector::get_idle_upload_rate(&test_case.host),
                test_case.expected,
                "Failed for test case: {}",
                test_case.name
            );
        }
    }
}
