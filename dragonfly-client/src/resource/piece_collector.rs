/*
 *     Copyright 2023 The Dragonfly Authors
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
use dashmap::DashMap;
use dragonfly_api::common::v2::Host;
use dragonfly_api::dfdaemon::v2::{
    SyncPersistentCachePiecesRequest, SyncPersistentPiecesRequest, SyncPiecesRequest,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_storage::metadata;
use dragonfly_client_util::net::format_url;
use std::net::IpAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, Instrument};

/// Collected parent is the parent peer collected from the parent.
#[derive(Clone, Debug, Default)]
pub struct CollectedParent {
    /// ID is the id of the parent.
    pub id: String,

    /// Host is the host of the parent.
    pub host: Option<Host>,

    // IP is used to indicate the IP address of the peer. If protocol is rdma,
    // the IP is used to exchange the queue pair endpoint of IBVerbs.
    pub download_ip: Option<String>,

    // TCP port is used to indicate the tcp server port of the peer.
    pub download_tcp_port: Option<i32>,

    // QUIC port is used to indicate the quic server port of the peer.
    pub download_quic_port: Option<i32>,
}

/// Collected piece is the piece collected from a peer.
#[derive(Clone, Debug, Default)]
pub struct CollectedPiece {
    /// Number is the piece number.
    pub number: u32,

    /// Length is the piece length.
    pub length: u64,

    /// Parents is the parents providing the piece.
    pub parents: Vec<CollectedParent>,
}

/// Piece collector is used to collect pieces from peers.
#[derive(Clone, Debug, Default)]
pub struct PieceCollector {
    /// Config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// Host id is the id of the host.
    host_id: String,

    /// Task id is the id of the task.
    task_id: String,

    /// Parents is the parent peers.
    parents: Vec<CollectedParent>,

    /// Interested pieces is the pieces interested by the collector.
    interested_pieces: Vec<metadata::Piece>,

    /// Collected pieces is a map to store the collected pieces from different parents.
    collected_pieces: Arc<DashMap<u32, CollectedPiece>>,
}

/// Piece collector is used to collect pieces from peers.
impl PieceCollector {
    /// Creates a new PieceCollector.
    pub async fn new(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        parents: Vec<CollectedParent>,
    ) -> Self {
        let collected_pieces = Arc::new(DashMap::with_capacity(interested_pieces.len()));
        for interested_piece in &interested_pieces {
            collected_pieces.insert(
                interested_piece.number,
                CollectedPiece {
                    number: interested_piece.number,
                    length: interested_piece.length,
                    ..Default::default()
                },
            );
        }

        Self {
            config,
            task_id: task_id.to_string(),
            host_id: host_id.to_string(),
            parents,
            interested_pieces,
            collected_pieces,
        }
    }

    /// Runs the piece collector.
    #[instrument(skip_all)]
    pub async fn run(&self) -> Receiver<CollectedPiece> {
        let config = self.config.clone();
        let host_id = self.host_id.clone();
        let task_id = self.task_id.clone();
        let parents = Arc::new(
            self.parents
                .iter()
                .cloned()
                .map(|parent| (parent.id.clone(), parent))
                .collect::<DashMap<_, _>>(),
        );
        let interested_pieces = self.interested_pieces.clone();
        let collected_pieces = self.collected_pieces.clone();
        let collected_piece_timeout = self.config.download.collected_piece_timeout;
        let (collected_piece_tx, collected_piece_rx) = mpsc::channel(1024);

        tokio::spawn(
            async move {
                Self::collect_from_parents(
                    config,
                    &host_id,
                    &task_id,
                    parents,
                    interested_pieces,
                    collected_pieces,
                    collected_piece_tx,
                    collected_piece_timeout,
                )
                .await
                .unwrap_or_else(|err| {
                    error!("collect pieces failed: {}", err);
                });
            }
            .in_current_span(),
        );

        collected_piece_rx
    }

    /// Collects piece metadata from multiple parent peers concurrently.
    ///
    /// This function spawns a set of asynchronous tasks, one per parent peer, to synchronize
    /// piece information. Each task establishes a gRPC connection to the parent's dfdaemon
    /// upload service, sends a `SyncPiecesRequest` with the interested piece numbers, and
    /// streams back piece metadata responses.
    ///
    /// # Piece Collection Strategy
    ///
    /// For each piece reported by a parent, the function accumulates parents in
    /// `collected_pieces`. A piece is not dispatched to the downloader immediately upon the
    /// first response. It waits until all candidate parents have had a chance to
    /// report availability. This ensures that when a piece is finally sent through
    /// `collected_piece_tx`, it carries a complete list of available parents, enabling
    /// load-balanced concurrent downloads across different peers.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn collect_from_parents(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        parents: Arc<DashMap<String, CollectedParent>>,
        interested_pieces: Vec<metadata::Piece>,
        collected_pieces: Arc<DashMap<u32, CollectedPiece>>,
        collected_piece_tx: Sender<CollectedPiece>,
        collected_piece_timeout: Duration,
    ) -> Result<()> {
        // Create a task to collect pieces from peers.
        let mut join_set = JoinSet::new();
        for parent in parents.iter().map(|parent| parent.value().clone()) {
            #[allow(clippy::too_many_arguments)]
            async fn sync_pieces(
                config: Arc<Config>,
                host_id: String,
                task_id: String,
                mut parent: CollectedParent,
                parents: Arc<DashMap<String, CollectedParent>>,
                interested_pieces: Vec<metadata::Piece>,
                collected_pieces: Arc<DashMap<u32, CollectedPiece>>,
                collected_piece_tx: Sender<CollectedPiece>,
                collected_piece_timeout: Duration,
            ) -> Result<CollectedParent> {
                debug!("sync pieces from parent {}", parent.id);

                // If candidate_parent.host is None, skip it.
                let host = parent.host.clone().ok_or_else(|| {
                    error!("peer {:?} host is empty", parent);
                    Error::InvalidPeer(parent.id.clone())
                })?;

                // Create a dfdaemon client.
                let dfdaemon_upload_client = DfdaemonUploadClient::new(
                    config,
                    format_url("http", IpAddr::from_str(&host.ip)?, host.port as u16),
                    false,
                )
                .await
                .inspect_err(|err| {
                    error!(
                        "create dfdaemon upload client from parent {} failed: {}",
                        parent.id, err
                    );
                })?;

                let response = dfdaemon_upload_client
                    .sync_pieces(SyncPiecesRequest {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        interested_piece_numbers: interested_pieces
                            .iter()
                            .map(|piece| piece.number)
                            .collect(),
                    })
                    .await
                    .inspect_err(|err| {
                        error!("sync pieces from parent {} failed: {}", parent.id, err);
                    })?;

                // If the response repeating timeout exceeds the piece download timeout, the stream will return error.
                let out_stream = response.into_inner().timeout(collected_piece_timeout);
                tokio::pin!(out_stream);

                while let Some(message) = out_stream.try_next().await.inspect_err(|err| {
                    error!("sync pieces from parent {} failed: {}", parent.id, err);
                })? {
                    let message = message?;

                    if let Some(mut piece) = collected_pieces.get_mut(&message.number) {
                        parent.download_ip = Some(message.ip);
                        parent.download_tcp_port = message.tcp_port;
                        parent.download_quic_port = message.quic_port;
                        piece.parents.push(parent.clone());

                        // When a piece has already been collected, wait until we've
                        // gathered responses from all candidate parents before proceeding.
                        // This ensures load is balanced across different parents during
                        // concurrent piece downloads.
                        if piece.parents.len() < parents.len() {
                            continue;
                        }
                    } else {
                        // Piece is not interested or already collected, skip it.
                        continue;
                    }

                    let piece = match collected_pieces.remove(&message.number) {
                        Some((_, piece)) => piece,
                        None => continue,
                    };

                    debug!(
                        "receive piece {}-{} metadata from parents {:?}",
                        task_id,
                        message.number,
                        piece
                            .parents
                            .iter()
                            .map(|p| &p.id)
                            .collect::<Vec<&String>>()
                    );

                    collected_piece_tx.send(piece).await.inspect_err(|err| {
                        error!("send CollectedPiece failed: {}", err);
                    })?;
                }

                Ok(parent)
            }

            let parent_id = parent.id.clone();
            let config = config.clone();
            let host_id = host_id.to_string();
            let task_id = task_id.to_string();
            let parents = parents.clone();
            let interested_pieces = interested_pieces.clone();
            let collected_pieces_for_sync = collected_pieces.clone();
            let collected_piece_tx_for_sync = collected_piece_tx.clone();
            join_set.spawn(
                async move {
                    (
                        parent_id,
                        sync_pieces(
                            config,
                            host_id,
                            task_id,
                            parent,
                            parents,
                            interested_pieces,
                            collected_pieces_for_sync,
                            collected_piece_tx_for_sync,
                            collected_piece_timeout,
                        )
                        .await,
                    )
                }
                .in_current_span(),
            );
        }

        // Wait for all tasks to finish.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok((parent_id, result)) => {
                    match result {
                        Ok(_) => debug!("peer {} sync pieces finished", parent_id),
                        Err(err) => error!("sync pieces from parent {} failed: {}", parent_id, err),
                    }

                    // If all the parents are removed, it means all pieces are collected,
                    // and need to send already collected pieces to piece downloader.
                    parents.remove(&parent_id);
                    if parents.is_empty() {
                        for number in collected_pieces
                            .iter()
                            .filter(|entry| !entry.value().parents.is_empty())
                            .map(|entry| *entry.key())
                            .collect::<Vec<u32>>()
                        {
                            if let Some((_, piece)) = collected_pieces.remove(&number) {
                                collected_piece_tx.send(piece).await.inspect_err(|err| {
                                    error!("send CollectedPiece failed: {}", err);
                                })?;
                            }
                        }
                    }

                    // If all pieces are collected, abort all tasks.
                    if collected_pieces.is_empty() {
                        info!("all pieces are collected, abort all tasks");
                        join_set.shutdown().await;
                    }
                }
                Err(err) => error!("task join error: {}", err),
            }
        }

        Ok(())
    }
}

/// Persistent piece collector is used to collect persistent pieces from peers.
pub struct PersistentPieceCollector {
    /// Config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// Host id is the id of the host.
    host_id: String,

    /// Task id is the id of the persistent task.
    task_id: String,

    /// Parents is the parent peers.
    parents: Vec<CollectedParent>,

    /// Interested pieces is the pieces interested by the collector.
    interested_pieces: Vec<metadata::Piece>,

    /// Collected pieces is a map to store the collected pieces from different parents.
    collected_pieces: Arc<DashMap<u32, CollectedPiece>>,
}

/// Persistent piece collector is used to collect persistent pieces from peers.
impl PersistentPieceCollector {
    /// Creates a new PieceCollector.
    pub async fn new(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        parents: Vec<CollectedParent>,
    ) -> Self {
        let collected_pieces = Arc::new(DashMap::with_capacity(interested_pieces.len()));
        for interested_piece in &interested_pieces {
            collected_pieces.insert(
                interested_piece.number,
                CollectedPiece {
                    number: interested_piece.number,
                    length: interested_piece.length,
                    ..Default::default()
                },
            );
        }

        Self {
            config,
            task_id: task_id.to_string(),
            host_id: host_id.to_string(),
            parents,
            interested_pieces,
            collected_pieces,
        }
    }

    /// Runs the piece collector.
    #[instrument(skip_all)]
    pub async fn run(&self) -> Receiver<CollectedPiece> {
        let config = self.config.clone();
        let host_id = self.host_id.clone();
        let task_id = self.task_id.clone();
        let parents = Arc::new(
            self.parents
                .iter()
                .cloned()
                .map(|parent| (parent.id.clone(), parent))
                .collect::<DashMap<_, _>>(),
        );
        let interested_pieces = self.interested_pieces.clone();
        let collected_pieces = self.collected_pieces.clone();
        let collected_piece_timeout = self.config.download.piece_timeout;
        let (collected_piece_tx, collected_piece_rx) = mpsc::channel(1024);

        tokio::spawn(
            async move {
                Self::collect_from_parents(
                    config,
                    &host_id,
                    &task_id,
                    parents,
                    interested_pieces,
                    collected_pieces,
                    collected_piece_tx,
                    collected_piece_timeout,
                )
                .await
                .unwrap_or_else(|err| {
                    error!("collect persistent pieces failed: {}", err);
                });
            }
            .in_current_span(),
        );

        collected_piece_rx
    }

    /// Collects piece metadata from multiple parent peers concurrently.
    ///
    /// This function spawns a set of asynchronous tasks, one per parent peer, to synchronize
    /// piece information. Each task establishes a gRPC connection to the parent's dfdaemon
    /// upload service, sends a `SyncPiecesRequest` with the interested piece numbers, and
    /// streams back piece metadata responses.
    ///
    /// # Piece Collection Strategy
    ///
    /// For each piece reported by a parent, the function accumulates parents in
    /// `collected_pieces`. A piece is not dispatched to the downloader immediately upon the
    /// first response. It waits until all candidate parents have had a chance to
    /// report availability. This ensures that when a piece is finally sent through
    /// `collected_piece_tx`, it carries a complete list of available parents, enabling
    /// load-balanced concurrent downloads across different peers.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn collect_from_parents(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        parents: Arc<DashMap<String, CollectedParent>>,
        interested_pieces: Vec<metadata::Piece>,
        collected_pieces: Arc<DashMap<u32, CollectedPiece>>,
        collected_piece_tx: Sender<CollectedPiece>,
        collected_piece_timeout: Duration,
    ) -> Result<()> {
        // Create a task to collect pieces from peers.
        let mut join_set = JoinSet::new();
        for parent in parents.iter().map(|parent| parent.value().clone()) {
            #[allow(clippy::too_many_arguments)]
            async fn sync_pieces(
                config: Arc<Config>,
                host_id: String,
                task_id: String,
                mut parent: CollectedParent,
                parents: Arc<DashMap<String, CollectedParent>>,
                interested_pieces: Vec<metadata::Piece>,
                collected_pieces: Arc<DashMap<u32, CollectedPiece>>,
                collected_piece_tx: Sender<CollectedPiece>,
                collected_piece_timeout: Duration,
            ) -> Result<CollectedParent> {
                debug!("sync persistent pieces from parent {}", parent.id);

                // If candidate_parent.host is None, skip it.
                let host = parent.host.clone().ok_or_else(|| {
                    error!("persistent peer {:?} host is empty", parent);
                    Error::InvalidPeer(parent.id.clone())
                })?;

                // Create a dfdaemon client.
                let dfdaemon_upload_client = DfdaemonUploadClient::new(
                    config,
                    format_url("http", IpAddr::from_str(&host.ip)?, host.port as u16),
                    false,
                )
                .await
                .inspect_err(|err| {
                    error!(
                        "create dfdaemon upload client from parent {} failed: {}",
                        parent.id, err
                    );
                })?;

                let response = dfdaemon_upload_client
                    .sync_persistent_pieces(SyncPersistentPiecesRequest {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        interested_piece_numbers: interested_pieces
                            .iter()
                            .map(|piece| piece.number)
                            .collect(),
                    })
                    .await
                    .inspect_err(|err| {
                        error!(
                            "sync persistent pieces from parent {} failed: {}",
                            parent.id, err
                        );
                    })?;

                // If the response repeating timeout exceeds the piece download timeout, the stream will return error.
                let out_stream = response.into_inner().timeout(collected_piece_timeout);
                tokio::pin!(out_stream);

                while let Some(message) = out_stream.try_next().await.inspect_err(|err| {
                    error!(
                        "sync persistent pieces from parent {} failed: {}",
                        parent.id, err
                    );
                })? {
                    let message = message?;
                    if let Some(mut piece) = collected_pieces.get_mut(&message.number) {
                        parent.download_ip = Some(message.ip);
                        parent.download_tcp_port = message.tcp_port;
                        parent.download_quic_port = message.quic_port;
                        piece.parents.push(parent.clone());

                        // When a piece has already been collected, wait until we've
                        // gathered responses from all candidate parents before proceeding.
                        // This ensures load is balanced across different parents during
                        // concurrent piece downloads.
                        if piece.parents.len() < parents.len() {
                            continue;
                        }
                    } else {
                        // Piece is not interested or already collected, skip it.
                        continue;
                    }

                    let piece = match collected_pieces.remove(&message.number) {
                        Some((_, piece)) => piece,
                        None => continue,
                    };

                    debug!(
                        "receive persistent piece {}-{} metadata from parents {:?}",
                        task_id,
                        message.number,
                        piece
                            .parents
                            .iter()
                            .map(|p| &p.id)
                            .collect::<Vec<&String>>()
                    );

                    collected_piece_tx.send(piece).await.inspect_err(|err| {
                        error!("send CollectedPiece failed: {}", err);
                    })?;
                }

                Ok(parent)
            }

            let parent_id = parent.id.clone();
            let config = config.clone();
            let host_id = host_id.to_string();
            let task_id = task_id.to_string();
            let parents = parents.clone();
            let interested_pieces = interested_pieces.clone();
            let collected_pieces_for_sync = collected_pieces.clone();
            let collected_piece_tx_for_sync = collected_piece_tx.clone();
            join_set.spawn(
                async move {
                    (
                        parent_id,
                        sync_pieces(
                            config,
                            host_id,
                            task_id,
                            parent,
                            parents,
                            interested_pieces,
                            collected_pieces_for_sync,
                            collected_piece_tx_for_sync,
                            collected_piece_timeout,
                        )
                        .await,
                    )
                }
                .in_current_span(),
            );
        }

        // Wait for all tasks to finish.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok((parent_id, result)) => {
                    match result {
                        Ok(_) => debug!("peer {} sync persistent pieces finished", parent_id),
                        Err(err) => error!(
                            "sync persistent pieces from parent {} failed: {}",
                            parent_id, err
                        ),
                    }

                    // If all the parents are removed, it means all pieces are collected,
                    // and need to send already collected pieces to piece downloader.
                    parents.remove(&parent_id);
                    if parents.is_empty() {
                        for number in collected_pieces
                            .iter()
                            .filter(|entry| !entry.value().parents.is_empty())
                            .map(|entry| *entry.key())
                            .collect::<Vec<u32>>()
                        {
                            if let Some((_, piece)) = collected_pieces.remove(&number) {
                                collected_piece_tx.send(piece).await.inspect_err(|err| {
                                    error!("send CollectedPiece failed: {}", err);
                                })?;
                            }
                        }
                    }

                    // If all pieces are collected, abort all tasks.
                    if collected_pieces.is_empty() {
                        info!("all persistent pieces are collected, abort all tasks");
                        join_set.shutdown().await;
                    }
                }
                Err(err) => error!("task join error: {}", err),
            }
        }

        Ok(())
    }
}

/// Persistent cache piece collector is used to collect persistent cache pieces from peers.
pub struct PersistentCachePieceCollector {
    /// Config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// Host id is the id of the host.
    host_id: String,

    /// Task id is the id of the persistent task.
    task_id: String,

    /// Parents is the parent peers.
    parents: Vec<CollectedParent>,

    /// Interested pieces is the pieces interested by the collector.
    interested_pieces: Vec<metadata::Piece>,

    /// Collected pieces is a map to store the collected pieces from different parents.
    collected_pieces: Arc<DashMap<u32, CollectedPiece>>,
}

/// Persistent cache piece collector is used to collect persistent cache pieces from peers.
impl PersistentCachePieceCollector {
    /// Creates a new PieceCollector.
    pub async fn new(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        parents: Vec<CollectedParent>,
    ) -> Self {
        let collected_pieces = Arc::new(DashMap::with_capacity(interested_pieces.len()));
        for interested_piece in &interested_pieces {
            collected_pieces.insert(
                interested_piece.number,
                CollectedPiece {
                    number: interested_piece.number,
                    length: interested_piece.length,
                    ..Default::default()
                },
            );
        }

        Self {
            config,
            task_id: task_id.to_string(),
            host_id: host_id.to_string(),
            parents,
            interested_pieces,
            collected_pieces,
        }
    }

    /// Runs the piece collector.
    #[instrument(skip_all)]
    pub async fn run(&self) -> Receiver<CollectedPiece> {
        let config = self.config.clone();
        let host_id = self.host_id.clone();
        let task_id = self.task_id.clone();
        let parents = Arc::new(
            self.parents
                .iter()
                .cloned()
                .map(|parent| (parent.id.clone(), parent))
                .collect::<DashMap<_, _>>(),
        );
        let interested_pieces = self.interested_pieces.clone();
        let collected_pieces = self.collected_pieces.clone();
        let collected_piece_timeout = self.config.download.piece_timeout;
        let (collected_piece_tx, collected_piece_rx) = mpsc::channel(1024);

        tokio::spawn(
            async move {
                Self::collect_from_parents(
                    config,
                    &host_id,
                    &task_id,
                    parents,
                    interested_pieces,
                    collected_pieces,
                    collected_piece_tx,
                    collected_piece_timeout,
                )
                .await
                .unwrap_or_else(|err| {
                    error!("collect persistent cache pieces failed: {}", err);
                });
            }
            .in_current_span(),
        );

        collected_piece_rx
    }

    /// Collects piece metadata from multiple parent peers concurrently.
    ///
    /// This function spawns a set of asynchronous tasks, one per parent peer, to synchronize
    /// piece information. Each task establishes a gRPC connection to the parent's dfdaemon
    /// upload service, sends a `SyncPiecesRequest` with the interested piece numbers, and
    /// streams back piece metadata responses.
    ///
    /// # Piece Collection Strategy
    ///
    /// For each piece reported by a parent, the function accumulates parents in
    /// `collected_pieces`. A piece is not dispatched to the downloader immediately upon the
    /// first response. It waits until all candidate parents have had a chance to
    /// report availability. This ensures that when a piece is finally sent through
    /// `collected_piece_tx`, it carries a complete list of available parents, enabling
    /// load-balanced concurrent downloads across different peers.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn collect_from_parents(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        parents: Arc<DashMap<String, CollectedParent>>,
        interested_pieces: Vec<metadata::Piece>,
        collected_pieces: Arc<DashMap<u32, CollectedPiece>>,
        collected_piece_tx: Sender<CollectedPiece>,
        collected_piece_timeout: Duration,
    ) -> Result<()> {
        // Create a task to collect pieces from peers.
        let mut join_set = JoinSet::new();
        for parent in parents.iter().map(|parent| parent.value().clone()) {
            #[allow(clippy::too_many_arguments)]
            async fn sync_pieces(
                config: Arc<Config>,
                host_id: String,
                task_id: String,
                mut parent: CollectedParent,
                parents: Arc<DashMap<String, CollectedParent>>,
                interested_pieces: Vec<metadata::Piece>,
                collected_pieces: Arc<DashMap<u32, CollectedPiece>>,
                collected_piece_tx: Sender<CollectedPiece>,
                collected_piece_timeout: Duration,
            ) -> Result<CollectedParent> {
                debug!("sync persistent cache pieces from parent {}", parent.id);

                // If candidate_parent.host is None, skip it.
                let host = parent.host.clone().ok_or_else(|| {
                    error!("persistent cache peer {:?} host is empty", parent);
                    Error::InvalidPeer(parent.id.clone())
                })?;

                // Create a dfdaemon client.
                let dfdaemon_upload_client = DfdaemonUploadClient::new(
                    config,
                    format_url("http", IpAddr::from_str(&host.ip)?, host.port as u16),
                    false,
                )
                .await
                .inspect_err(|err| {
                    error!(
                        "create dfdaemon upload client from parent {} failed: {}",
                        parent.id, err
                    );
                })?;

                let response = dfdaemon_upload_client
                    .sync_persistent_cache_pieces(SyncPersistentCachePiecesRequest {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        interested_piece_numbers: interested_pieces
                            .iter()
                            .map(|piece| piece.number)
                            .collect(),
                    })
                    .await
                    .inspect_err(|err| {
                        error!(
                            "sync persistent cache pieces from parent {} failed: {}",
                            parent.id, err
                        );
                    })?;

                // If the response repeating timeout exceeds the piece download timeout, the stream will return error.
                let out_stream = response.into_inner().timeout(collected_piece_timeout);
                tokio::pin!(out_stream);

                while let Some(message) = out_stream.try_next().await.inspect_err(|err| {
                    error!(
                        "sync persistent cache pieces from parent {} failed: {}",
                        parent.id, err
                    );
                })? {
                    let message = message?;
                    if let Some(mut piece) = collected_pieces.get_mut(&message.number) {
                        parent.download_ip = Some(message.ip);
                        parent.download_tcp_port = message.tcp_port;
                        parent.download_quic_port = message.quic_port;
                        piece.parents.push(parent.clone());

                        // When a piece has already been collected, wait until we've
                        // gathered responses from all candidate parents before proceeding.
                        // This ensures load is balanced across different parents during
                        // concurrent piece downloads.
                        if piece.parents.len() < parents.len() {
                            continue;
                        }
                    } else {
                        // Piece is not interested or already collected, skip it.
                        continue;
                    }

                    let piece = match collected_pieces.remove(&message.number) {
                        Some((_, piece)) => piece,
                        None => continue,
                    };

                    debug!(
                        "receive persistent cache piece {}-{} metadata from parents {:?}",
                        task_id,
                        message.number,
                        piece
                            .parents
                            .iter()
                            .map(|p| &p.id)
                            .collect::<Vec<&String>>()
                    );

                    collected_piece_tx.send(piece).await.inspect_err(|err| {
                        error!("send CollectedPiece failed: {}", err);
                    })?;
                }

                Ok(parent)
            }

            let parent_id = parent.id.clone();
            let config = config.clone();
            let host_id = host_id.to_string();
            let task_id = task_id.to_string();
            let parents = parents.clone();
            let interested_pieces = interested_pieces.clone();
            let collected_pieces_for_sync = collected_pieces.clone();
            let collected_piece_tx_for_sync = collected_piece_tx.clone();
            join_set.spawn(
                async move {
                    (
                        parent_id,
                        sync_pieces(
                            config,
                            host_id,
                            task_id,
                            parent,
                            parents,
                            interested_pieces,
                            collected_pieces_for_sync,
                            collected_piece_tx_for_sync,
                            collected_piece_timeout,
                        )
                        .await,
                    )
                }
                .in_current_span(),
            );
        }

        // Wait for all tasks to finish.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok((parent_id, result)) => {
                    match result {
                        Ok(_) => debug!("peer {} sync cache persistent pieces finished", parent_id),
                        Err(err) => error!(
                            "sync persistent cache pieces from parent {} failed: {}",
                            parent_id, err
                        ),
                    }

                    // If all the parents are removed, it means all pieces are collected,
                    // and need to send already collected pieces to piece downloader.
                    parents.remove(&parent_id);
                    if parents.is_empty() {
                        for number in collected_pieces
                            .iter()
                            .filter(|entry| !entry.value().parents.is_empty())
                            .map(|entry| *entry.key())
                            .collect::<Vec<u32>>()
                        {
                            if let Some((_, piece)) = collected_pieces.remove(&number) {
                                collected_piece_tx.send(piece).await.inspect_err(|err| {
                                    error!("send CollectedPiece failed: {}", err);
                                })?;
                            }
                        }
                    }

                    // If all pieces are collected, abort all tasks.
                    if collected_pieces.is_empty() {
                        info!("all persistent cache pieces are collected, abort all tasks");
                        join_set.shutdown().await;
                    }
                }
                Err(err) => error!("task join error: {}", err),
            }
        }

        Ok(())
    }
}
