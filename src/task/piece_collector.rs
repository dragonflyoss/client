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

use crate::grpc::dfdaemon::DfdaemonClient;
use crate::storage::metadata;
use crate::{Error, Result};
use dragonfly_api::common::v2::Peer;
use std::collections::{HashMap, HashSet};
use tokio::task::JoinSet;
use tracing::{error, info};

// CollectedPiece is the piece collected from a peer.
pub struct CollectedPiece {
    // number is the piece number.
    pub number: u32,

    // parent is the parent peer.
    pub parent: Peer,
}

// PieceCollector is used to collect pieces from peers.
pub struct PieceCollector {
    // task_id is the id of the task.
    task_id: String,

    // parents is the parent peers.
    parents: Vec<Peer>,

    // interested_pieces is the pieces interested by the collector.
    interested_pieces: Vec<metadata::Piece>,

    // collected_pieces is the pieces collected from parents.
    collected_pieces: HashMap<u32, HashSet<Peer>>,
}

impl PieceCollector {
    // NewPieceCollector returns a new PieceCollector.
    pub fn new(task_id: &str, interested_pieces: Vec<metadata::Piece>, parents: Vec<Peer>) -> Self {
        // Initialize collected_pieces.
        let mut collected_pieces = HashMap::new();
        interested_pieces.into_iter().for_each(|interested_piece| {
            collected_pieces.insert(interested_piece.number, HashSet::new());
        });

        Self {
            task_id: task_id.to_string(),
            parents,
            interested_pieces,
            collected_pieces,
        }
    }

    // Run runs the collector.
    pub async fn run(&self) {
        // Create a task to collect pieces from parents.
        let mut join_set = JoinSet::new();
        for parent in self.parents {
            async fn sync_pieces(
                task_id: &str,
                peer: Peer,
                interested_pieces: Vec<metadata::Piece>,
                collected_pieces: HashMap<u32, HashSet<Peer>>,
            ) -> Result<()> {
                // If candidate_parent.host is None, skip it.
                let host = peer.host.clone().ok_or_else(|| {
                    error!("peer {:?} host is empty", peer);
                    Error::InvalidPeer(peer.id.clone())
                })?;

                // Create a dfdaemon client.
                let dfdaemon_client =
                    DfdaemonClient::new(format!("http://{}:{}", host.ip, host.port)).await?;

                Ok(())
            }
        }
    }

    // Pop pops a piece from the collector.
    pub async fn pop(&self) -> Result<Option<CollectedPiece>> {
        // 如果没有 Sync Pieces 连接存在的时候，直接返回完成收集。
        // Sync Pieces 断掉一个就从 parents vec 里面删除掉对应的 Parent。 如果 parents vec 为空，直接返回完成收集。
        // 返回过的 Piece，直接在 HashMap 里面删除掉。一个 Piece 只会返回一次。HashMap key 为空，直接返回完成收集。
        Err(Error::Unimplemented())
    }
}
