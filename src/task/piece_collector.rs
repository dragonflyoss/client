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
use crate::storage::metadata;
use dashmap::{DashMap, DashSet};
use dragonfly_api::common::v2::Peer;
use dragonfly_api::dfdaemon::v2::SyncPiecesRequest;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinSet;
use tokio_stream::StreamExt;
use tracing::{error, info, Instrument};

// CollectedPiece is the piece collected from a peer.
pub struct CollectedPiece {
    // number is the piece number.
    pub number: u32,

    // length is the piece length.
    pub length: u64,

    // parent is the parent peer.
    pub parent: Peer,
}

// PieceCollector is used to collect pieces from peers.
pub struct PieceCollector {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

    // task_id is the id of the task.
    task_id: String,

    // parents is the parent peers.
    parents: Vec<Peer>,

    // interested_pieces is the pieces interested by the collector.
    interested_pieces: Vec<metadata::Piece>,

    // collected_pieces is the pieces collected from peers.
    collected_pieces: Arc<DashMap<u32, DashSet<String>>>,
}

impl PieceCollector {
    // new creates a new PieceCollector.
    pub fn new(
        config: Arc<Config>,
        task_id: &str,
        interested_pieces: Vec<metadata::Piece>,
        parents: Vec<Peer>,
    ) -> Self {
        // Initialize collected_pieces.
        let collected_pieces = Arc::new(DashMap::new());
        interested_pieces
            .clone()
            .into_iter()
            .for_each(|interested_piece| {
                collected_pieces.insert(interested_piece.number, DashSet::new());
            });

        Self {
            config,
            task_id: task_id.to_string(),
            parents,
            interested_pieces,
            collected_pieces,
        }
    }

    // run runs the piece collector.
    pub async fn run(&self) -> Receiver<CollectedPiece> {
        let task_id = self.task_id.clone();
        let parents = self.parents.clone();
        let interested_pieces = self.interested_pieces.clone();
        let collected_pieces = self.collected_pieces.clone();
        let collected_piece_timeout = self.config.download.piece_timeout;
        let (collected_piece_tx, collected_piece_rx) = mpsc::channel(1024);
        tokio::spawn(async move {
            Self::collect_from_remote_peers(
                task_id,
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
        });

        collected_piece_rx
    }

    // collect_from_remote_peers collects pieces from remote peers.
    async fn collect_from_remote_peers(
        task_id: String,
        parents: Vec<Peer>,
        interested_pieces: Vec<metadata::Piece>,
        collected_pieces: Arc<DashMap<u32, DashSet<String>>>,
        collected_piece_tx: Sender<CollectedPiece>,
        collected_piece_timeout: Duration,
    ) -> Result<()> {
        // Create a task to collect pieces from peers.
        let mut join_set = JoinSet::new();
        for parent in parents.iter() {
            async fn sync_pieces(
                task_id: String,
                parent: Peer,
                parents: Vec<Peer>,
                interested_pieces: Vec<metadata::Piece>,
                collected_pieces: Arc<DashMap<u32, DashSet<String>>>,
                collected_piece_tx: Sender<CollectedPiece>,
                collected_piece_timeout: Duration,
            ) -> Result<Peer> {
                info!("sync pieces from parent {}", parent.id);

                // If candidate_parent.host is None, skip it.
                let host = parent.host.clone().ok_or_else(|| {
                    error!("peer {:?} host is empty", parent);
                    Error::InvalidPeer(parent.id.clone())
                })?;

                // Create a dfdaemon client.
                let dfdaemon_upload_client =
                    DfdaemonUploadClient::new(format!("http://{}:{}", host.ip, host.port))
                        .await
                        .map_err(|err| {
                            error!(
                                "create dfdaemon upload client from parent {} failed: {}",
                                parent.id, err
                            );
                            err
                        })?;

                let response = dfdaemon_upload_client
                    .sync_pieces(SyncPiecesRequest {
                        task_id: task_id.to_string(),
                        interested_piece_numbers: interested_pieces
                            .iter()
                            .map(|piece| piece.number)
                            .collect(),
                    })
                    .await
                    .map_err(|err| {
                        error!("sync pieces from parent {} failed: {}", parent.id, err);
                        err
                    })?;

                // If the response repeating timeout exceeds the piece download timeout, the stream will return error.
                let out_stream = response.into_inner().timeout(collected_piece_timeout);
                tokio::pin!(out_stream);

                while let Some(message) = out_stream.try_next().await? {
                    let message = message?;
                    collected_pieces.entry(message.number).and_modify(|peers| {
                        peers.insert(parent.id.clone());
                    });
                    info!(
                        "received piece metadata {} from parent {}",
                        message.number, parent.id
                    );

                    match collected_pieces.get(&message.number) {
                        Some(parent_ids) => {
                            if let Some(parent_id) = parent_ids.iter().next() {
                                let number = message.number;
                                let length = message.length;
                                let parent = parents
                                    .iter()
                                    .find(|parent| parent.id == parent_id.as_str())
                                    .ok_or_else(|| {
                                        error!("parent {} not found", parent_id.as_str());
                                        Error::InvalidPeer(parent_id.clone())
                                    })?;

                                collected_piece_tx
                                    .send(CollectedPiece {
                                        number,
                                        length,
                                        parent: parent.clone(),
                                    })
                                    .await?;
                            }
                        }
                        None => continue,
                    };

                    // Remove the piece from collected_pieces.
                    collected_pieces.remove(&message.number);
                }

                Ok(parent)
            }

            join_set.spawn(
                sync_pieces(
                    task_id.clone(),
                    parent.clone(),
                    parents.clone(),
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
                    if collected_pieces.len() == 0 {
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
}
