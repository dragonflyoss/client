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
use dragonfly_api::dfdaemon::v2::{SyncPersistentCachePiecesRequest, SyncPiecesRequest};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_storage::metadata;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{error, info, instrument, Instrument};

const DEFAULT_WAIT_FOR_PIECE_FROM_DIFFERENT_PARENTS: Duration = Duration::from_millis(5);

/// CollectedParent is the parent peer collected from the parent.
#[derive(Clone, Debug)]
pub struct CollectedParent {
    /// id is the id of the parent.
    pub id: String,

    /// host is the host of the parent.
    pub host: Option<Host>,
}

/// CollectedPiece is the piece collected from a peer.
pub struct CollectedPiece {
    /// number is the piece number.
    pub number: u32,

    /// length is the piece length.
    pub length: u64,

    /// parent is the parent peer.
    pub parent: CollectedParent,
}

/// PieceCollector is used to collect pieces from peers.
pub struct PieceCollector {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// host_id is the id of the host.
    host_id: String,

    /// task_id is the id of the task.
    task_id: String,

    /// parents is the parent peers.
    parents: Vec<CollectedParent>,

    /// interested_pieces is the pieces interested by the collector.
    interested_pieces: Vec<metadata::Piece>,

    /// collected_pieces is a map to store the collected pieces from different parents.
    collected_pieces: Arc<DashMap<u32, Vec<CollectedParent>>>,
}

/// PieceCollector is used to collect pieces from peers.
impl PieceCollector {
    /// new creates a new PieceCollector.
    pub async fn new(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        parents: Vec<CollectedParent>,
    ) -> Self {
        let collected_pieces = Arc::new(DashMap::with_capacity(interested_pieces.len()));
        for interested_piece in &interested_pieces {
            collected_pieces.insert(interested_piece.number, Vec::new());
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

    /// run runs the piece collector.
    #[instrument(skip_all)]
    pub async fn run(&self) -> Receiver<CollectedPiece> {
        let config = self.config.clone();
        let host_id = self.host_id.clone();
        let task_id = self.task_id.clone();
        let parents = self.parents.clone();
        let interested_pieces = self.interested_pieces.clone();
        let collected_pieces = self.collected_pieces.clone();
        let collected_piece_timeout = self.config.download.collected_piece_timeout;
        let (collected_piece_tx, collected_piece_rx) = mpsc::channel(128 * 1024);
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

    /// collect_from_parents collects pieces from multiple parents with load balancing strategy.
    ///
    /// The collection process works in two phases:
    /// 1. **Synchronization Phase**: Waits for a configured duration (DEFAULT_WAIT_FOR_PIECE_FROM_DIFFERENT_PARENTS)
    ///    to collect the same piece information from different parents. This allows the collector
    ///    to gather multiple sources for each piece.
    ///
    /// 2. **Selection Phase**: After the wait period, randomly selects one parent from the available
    ///    candidates for each piece and forwards it to the piece downloader.
    ///
    /// **Load Balancing Strategy**:
    /// The random parent selection is designed to distribute download load across multiple parents
    /// during concurrent piece downloads. This approach ensures:
    /// - Optimal utilization of bandwidth from multiple parent nodes
    /// - Prevention of overwhelming any single parent with too many requests
    /// - Better overall download performance through parallel connections
    ///
    /// This strategy is particularly effective when downloading multiple pieces simultaneously,
    /// as it naturally spreads the workload across the available parent pool.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn collect_from_parents(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        parents: Vec<CollectedParent>,
        interested_pieces: Vec<metadata::Piece>,
        collected_pieces: Arc<DashMap<u32, Vec<CollectedParent>>>,
        collected_piece_tx: Sender<CollectedPiece>,
        collected_piece_timeout: Duration,
    ) -> Result<()> {
        // Create a task to collect pieces from peers.
        let mut join_set = JoinSet::new();
        for parent in parents.iter() {
            #[allow(clippy::too_many_arguments)]
            async fn sync_pieces(
                config: Arc<Config>,
                host_id: String,
                task_id: String,
                parent: CollectedParent,
                interested_pieces: Vec<metadata::Piece>,
                collected_pieces: Arc<DashMap<u32, Vec<CollectedParent>>>,
                collected_piece_tx: Sender<CollectedPiece>,
                collected_piece_timeout: Duration,
            ) -> Result<CollectedParent> {
                info!("sync pieces from parent {}", parent.id);

                // If candidate_parent.host is None, skip it.
                let host = parent.host.clone().ok_or_else(|| {
                    error!("peer {:?} host is empty", parent);
                    Error::InvalidPeer(parent.id.clone())
                })?;

                // Create a dfdaemon client.
                let dfdaemon_upload_client = DfdaemonUploadClient::new(
                    config.clone(),
                    format!("http://{}:{}", host.ip, host.port),
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

                    if let Some(mut parents) = collected_pieces.get_mut(&message.number) {
                        parents.push(parent.clone());
                    } else {
                        continue;
                    }

                    // Wait for collecting the piece from different parents when the first
                    // piece is collected.
                    tokio::time::sleep(DEFAULT_WAIT_FOR_PIECE_FROM_DIFFERENT_PARENTS).await;
                    let parents = match collected_pieces.remove(&message.number) {
                        Some((_, parents)) => parents,
                        None => continue,
                    };

                    let parent = match parents.get(fastrand::usize(..parents.len())) {
                        Some(parent) => parent,
                        None => {
                            error!(
                                "collected_pieces does not contain parent for piece {}",
                                message.number
                            );
                            continue;
                        }
                    };

                    info!(
                        "picked up piece {}-{} metadata from parent {}",
                        task_id, message.number, parent.id
                    );

                    collected_piece_tx
                        .send(CollectedPiece {
                            number: message.number,
                            length: message.length,
                            parent: parent.clone(),
                        })
                        .await
                        .inspect_err(|err| {
                            error!("send CollectedPiece failed: {}", err);
                        })?;
                }

                Ok(parent)
            }

            join_set.spawn(
                sync_pieces(
                    config.clone(),
                    host_id.to_string(),
                    task_id.to_string(),
                    parent.clone(),
                    interested_pieces.clone(),
                    collected_pieces.clone(),
                    collected_piece_tx.clone(),
                    collected_piece_timeout,
                )
                .in_current_span(),
            );
        }

        // Wait for all tasks to finish.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok(Ok(peer)) => {
                    info!("peer {} sync pieces finished", peer.id);

                    // If all pieces are collected, abort all tasks.
                    if collected_pieces.is_empty() {
                        info!("all pieces are collected, abort all tasks");
                        join_set.shutdown().await;
                    }
                }
                Ok(Err(err)) => {
                    error!("sync pieces failed: {}", err);
                }
                Err(err) => {
                    error!("sync pieces failed: {}", err);
                }
            }
        }

        Ok(())
    }
}

/// PersistentCachePieceCollector is used to collect persistent cache pieces from peers.
pub struct PersistentCachePieceCollector {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// host_id is the id of the host.
    host_id: String,

    /// task_id is the id of the persistent cache task.
    task_id: String,

    /// parents is the parent peers.
    parents: Vec<CollectedParent>,

    /// interested_pieces is the pieces interested by the collector.
    interested_pieces: Vec<metadata::Piece>,

    /// collected_pieces is a map to store the collected pieces from different parents.
    collected_pieces: Arc<DashMap<u32, Vec<CollectedParent>>>,
}

/// PersistentCachePieceCollector is used to collect persistent cache pieces from peers.
impl PersistentCachePieceCollector {
    /// new creates a new PieceCollector.
    pub async fn new(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        parents: Vec<CollectedParent>,
    ) -> Self {
        let collected_pieces = Arc::new(DashMap::with_capacity(interested_pieces.len()));
        for interested_piece in &interested_pieces {
            collected_pieces.insert(interested_piece.number, Vec::new());
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

    /// run runs the piece collector.
    #[instrument(skip_all)]
    pub async fn run(&self) -> Receiver<CollectedPiece> {
        let config = self.config.clone();
        let host_id = self.host_id.clone();
        let task_id = self.task_id.clone();
        let parents = self.parents.clone();
        let interested_pieces = self.interested_pieces.clone();
        let collected_pieces = self.collected_pieces.clone();
        let collected_piece_timeout = self.config.download.piece_timeout;
        let (collected_piece_tx, collected_piece_rx) = mpsc::channel(10 * 1024);
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

    /// collect_from_parents collects pieces from multiple parents with load balancing strategy.
    ///
    /// The collection process works in two phases:
    /// 1. **Synchronization Phase**: Waits for a configured duration (DEFAULT_WAIT_FOR_PIECE_FROM_DIFFERENT_PARENTS)
    ///    to collect the same piece information from different parents. This allows the collector
    ///    to gather multiple sources for each piece.
    ///
    /// 2. **Selection Phase**: After the wait period, randomly selects one parent from the available
    ///    candidates for each piece and forwards it to the piece downloader.
    ///
    /// **Load Balancing Strategy**:
    /// The random parent selection is designed to distribute download load across multiple parents
    /// during concurrent piece downloads. This approach ensures:
    /// - Optimal utilization of bandwidth from multiple parent nodes
    /// - Prevention of overwhelming any single parent with too many requests
    /// - Better overall download performance through parallel connections
    ///
    /// This strategy is particularly effective when downloading multiple pieces simultaneously,
    /// as it naturally spreads the workload across the available parent pool.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn collect_from_parents(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        parents: Vec<CollectedParent>,
        interested_pieces: Vec<metadata::Piece>,
        collected_pieces: Arc<DashMap<u32, Vec<CollectedParent>>>,
        collected_piece_tx: Sender<CollectedPiece>,
        collected_piece_timeout: Duration,
    ) -> Result<()> {
        // Create a task to collect pieces from peers.
        let mut join_set = JoinSet::new();
        for parent in parents.iter() {
            #[allow(clippy::too_many_arguments)]
            async fn sync_pieces(
                config: Arc<Config>,
                host_id: String,
                task_id: String,
                parent: CollectedParent,
                interested_pieces: Vec<metadata::Piece>,
                collected_pieces: Arc<DashMap<u32, Vec<CollectedParent>>>,
                collected_piece_tx: Sender<CollectedPiece>,
                collected_piece_timeout: Duration,
            ) -> Result<CollectedParent> {
                info!("sync persistent cache pieces from parent {}", parent.id);

                // If candidate_parent.host is None, skip it.
                let host = parent.host.clone().ok_or_else(|| {
                    error!("persistent cache peer {:?} host is empty", parent);
                    Error::InvalidPeer(parent.id.clone())
                })?;

                // Create a dfdaemon client.
                let dfdaemon_upload_client = DfdaemonUploadClient::new(
                    config,
                    format!("http://{}:{}", host.ip, host.port),
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
                    if let Some(mut parents) = collected_pieces.get_mut(&message.number) {
                        parents.push(parent.clone());
                    } else {
                        continue;
                    }

                    // Wait for collecting the piece from different parents when the first
                    // piece is collected.
                    tokio::time::sleep(DEFAULT_WAIT_FOR_PIECE_FROM_DIFFERENT_PARENTS).await;
                    let parents = match collected_pieces.remove(&message.number) {
                        Some((_, parents)) => parents,
                        None => continue,
                    };

                    let parent = match parents.get(fastrand::usize(..parents.len())) {
                        Some(parent) => parent,
                        None => {
                            error!(
                                "collected_pieces does not contain parent for piece {}",
                                message.number
                            );
                            continue;
                        }
                    };

                    info!(
                        "picked up piece {}-{} metadata from parent {}",
                        task_id, message.number, parent.id
                    );

                    collected_piece_tx
                        .send(CollectedPiece {
                            number: message.number,
                            length: message.length,
                            parent: parent.clone(),
                        })
                        .await
                        .inspect_err(|err| {
                            error!("send CollectedPiece failed: {}", err);
                        })?;
                }

                Ok(parent)
            }

            join_set.spawn(
                sync_pieces(
                    config.clone(),
                    host_id.to_string(),
                    task_id.to_string(),
                    parent.clone(),
                    interested_pieces.clone(),
                    collected_pieces.clone(),
                    collected_piece_tx.clone(),
                    collected_piece_timeout,
                )
                .in_current_span(),
            );
        }

        // Wait for all tasks to finish.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok(Ok(peer)) => {
                    info!("peer {} sync persistent cache pieces finished", peer.id);

                    // If all pieces are collected, abort all tasks.
                    if collected_pieces.is_empty() {
                        info!("all persistent cache pieces are collected, abort all tasks");
                        join_set.shutdown().await;
                    }
                }
                Ok(Err(err)) => {
                    error!("sync persistent cache pieces failed: {}", err);
                }
                Err(err) => {
                    error!("sync persistent cache pieces failed: {}", err);
                }
            }
        }

        Ok(())
    }
}
/// PieceParentCollector is used to collect pieces from peers.
pub struct PieceParentCollector {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// host_id is the id of the host.
    host_id: String,

    /// task_id is the id of the task.
    task_id: String,

    /// parents is the parent peers.
    parents: Vec<CollectedParent>,

    /// interested_pieces is the pieces interested by the collector.
    interested_pieces: Vec<metadata::Piece>,

    /// collected_pieces is a map to store the collected pieces from different parents.
    collected_pieces: Arc<DashMap<u32, Vec<CollectedParent>>>,

    /// candidate_parents is the candidates of the parents for parent selection.
    candidate_parents: Arc<DashMap<u32, Vec<CollectedParent>>>,
}

/// PieceParentCollector is used to collect pieces from peers.
impl PieceParentCollector {
    /// new creates a new PieceCollector.
    pub async fn new(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        parents: Vec<CollectedParent>,
    ) -> Self {
        let collected_pieces = Arc::new(DashMap::with_capacity(interested_pieces.len()));
        for interested_piece in &interested_pieces {
            collected_pieces.insert(interested_piece.number, Vec::new());
        }
        let candidate_parents = Arc::new(DashMap::with_capacity(interested_pieces.len()));

        Self {
            config,
            task_id: task_id.to_string(),
            host_id: host_id.to_string(),
            parents,
            interested_pieces,
            collected_pieces,
            candidate_parents,
        }
    }

    /// run runs the piece collector.
    #[instrument(skip_all)]
    pub async fn run(&self) -> Receiver<CollectedPiece> {
        let config = self.config.clone();
        let host_id = self.host_id.clone();
        let task_id = self.task_id.clone();
        let parents = self.parents.clone();
        let candidate_parents = self.candidate_parents.clone();
        let interested_pieces = self.interested_pieces.clone();
        let collected_pieces = self.collected_pieces.clone();
        let collected_piece_timeout = self.config.download.collected_piece_timeout;
        let (collected_piece_tx, collected_piece_rx) = mpsc::channel(128 * 1024);
        tokio::spawn(
            async move {
                Self::collect_from_parents(
                    config,
                    &host_id,
                    &task_id,
                    parents,
                    candidate_parents,
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

    /// collect_from_parents collects pieces from multiple parents with load balancing strategy.
    ///
    /// The collection process works in two phases:
    /// 1. **Synchronization Phase**: Waits for a configured duration (DEFAULT_WAIT_FOR_PIECE_FROM_DIFFERENT_PARENTS)
    ///    to collect the same piece information from different parents. This allows the collector
    ///    to gather multiple sources for each piece.
    ///
    /// 2. **Selection Phase**: After the wait period, randomly selects one parent from the available
    ///    candidates for each piece and forwards it to the piece downloader.
    ///
    /// **Load Balancing Strategy**:
    /// The random parent selection is designed to distribute download load across multiple parents
    /// during concurrent piece downloads. This approach ensures:
    /// - Optimal utilization of bandwidth from multiple parent nodes
    /// - Prevention of overwhelming any single parent with too many requests
    /// - Better overall download performance through parallel connections
    ///
    /// This strategy is particularly effective when downloading multiple pieces simultaneously,
    /// as it naturally spreads the workload across the available parent pool.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn collect_from_parents(
        config: Arc<Config>,
        host_id: &str,
        task_id: &str,
        parents: Vec<CollectedParent>,
        candidate_parents: Arc<DashMap<u32, Vec<CollectedParent>>>,
        interested_pieces: Vec<metadata::Piece>,
        collected_pieces: Arc<DashMap<u32, Vec<CollectedParent>>>,
        collected_piece_tx: Sender<CollectedPiece>,
        collected_piece_timeout: Duration,
    ) -> Result<()> {
        // Create a task to collect pieces from peers.
        let mut join_set = JoinSet::new();
        for parent in parents.iter() {
            #[allow(clippy::too_many_arguments)]
            async fn sync_pieces(
                config: Arc<Config>,
                host_id: String,
                task_id: String,
                parent: CollectedParent,
                candidate_parents: Arc<DashMap<u32, Vec<CollectedParent>>>,
                interested_pieces: Vec<metadata::Piece>,
                collected_pieces: Arc<DashMap<u32, Vec<CollectedParent>>>,
                collected_piece_tx: Sender<CollectedPiece>,
                collected_piece_timeout: Duration,
            ) -> Result<CollectedParent> {
                info!("sync pieces from parent {}", parent.id);

                // If candidate_parent.host is None, skip it.
                let host = parent.host.clone().ok_or_else(|| {
                    error!("peer {:?} host is empty", parent);
                    Error::InvalidPeer(parent.id.clone())
                })?;

                // Create a dfdaemon client.
                let dfdaemon_upload_client = DfdaemonUploadClient::new(
                    config.clone(),
                    format!("http://{}:{}", host.ip, host.port),
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

                    if config.download.parent_selector.enable {
                        match candidate_parents.entry(message.number) {
                            dashmap::mapref::entry::Entry::Occupied(mut e) => {
                                e.get_mut().push(parent.clone());
                            }
                            dashmap::mapref::entry::Entry::Vacant(e) => {
                                e.insert(vec![parent.clone()]);
                            }
                        }
                    }

                    if let Some(mut parents) = collected_pieces.get_mut(&message.number) {
                        parents.push(parent.clone());
                    } else {
                        continue;
                    }

                    // Wait for collecting the piece from different parents when the first
                    // piece is collected.
                    tokio::time::sleep(DEFAULT_WAIT_FOR_PIECE_FROM_DIFFERENT_PARENTS).await;
                    let parents = match collected_pieces.remove(&message.number) {
                        Some((_, parents)) => parents,
                        None => continue,
                    };

                    let parent = match parents.get(fastrand::usize(..parents.len())) {
                        Some(parent) => parent,
                        None => {
                            error!(
                                "collected_pieces does not contain parent for piece {}",
                                message.number
                            );
                            continue;
                        }
                    };

                    info!(
                        "picked up piece {}-{} metadata from parent {}",
                        task_id, message.number, parent.id
                    );

                    collected_piece_tx
                        .send(CollectedPiece {
                            number: message.number,
                            length: message.length,
                            parent: parent.clone(),
                        })
                        .await
                        .inspect_err(|err| {
                            error!("send CollectedPiece failed: {}", err);
                        })?;
                }

                Ok(parent)
            }

            join_set.spawn(
                sync_pieces(
                    config.clone(),
                    host_id.to_string(),
                    task_id.to_string(),
                    parent.clone(),
                    candidate_parents.clone(),
                    interested_pieces.clone(),
                    collected_pieces.clone(),
                    collected_piece_tx.clone(),
                    collected_piece_timeout,
                )
                .in_current_span(),
            );
        }

        // Wait for all tasks to finish.
        while let Some(message) = join_set.join_next().await {
            match message {
                Ok(Ok(peer)) => {
                    info!("peer {} sync pieces finished", peer.id);

                    // If all pieces are collected, abort all tasks.
                    if collected_pieces.is_empty() {
                        info!("all pieces are collected, abort all tasks");
                        join_set.abort_all();
                    }
                }
                Ok(Err(err)) => {
                    error!("sync pieces failed: {}", err);
                }
                Err(err) => {
                    error!("sync pieces failed: {}", err);
                }
            }
        }

        Ok(())
    }

    /// get_candidate_parents returns the list of parents that have a specific piece
    pub fn get_candidate_parents(&self, piece_number: u32) -> Option<Vec<CollectedParent>> {
        self.candidate_parents
            .get(&piece_number)
            .map(|parents| parents.clone())
    }
}
