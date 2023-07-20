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

use crate::config::dfdaemon::Config;
use crate::grpc::{self, manager::ManagerClient, scheduler::SchedulerClient};
use crate::shutdown;
use dragonfly_api::manager::{SourceType, UpdateSeedPeerRequest};
use tokio::sync::mpsc;

// Error is the error for GRPC.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // GRPC is the error for the grpc server.
    #[error(transparent)]
    GRPC(#[from] grpc::Error),
}

// Result is the result for Announcer.
pub type Result<T> = std::result::Result<T, Error>;

// Announcer is used to announce the dfdaemon information to the manager and scheduler.
pub struct Announcer {
    // config is the configuration of the dfdaemon.
    config: Config,

    // manager_client is the grpc client of the manager.
    manager_client: ManagerClient,

    // scheduler_client is the grpc client of the scheduler.
    scheduler_client: SchedulerClient,

    // shutdown is used to shutdown the announcer.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the announcer is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// Announcer implements the announcer of the dfdaemon.
impl Announcer {
    // new creates a new announcer.
    pub fn new(
        config: Config,
        manager_client: ManagerClient,
        scheduler_client: SchedulerClient,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            config,
            manager_client,
            scheduler_client,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    // run starts the announcer.
    pub async fn run(&mut self) {
        let mut interval = tokio::time::interval(self.config.scheduler.announce_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                }
                _ = self.shutdown.recv() => {
                }
            }
        }
    }

    // announce_to_manager announces the dfdaemon information to the manager.
    pub async fn announce_to_manager(&mut self) -> Result<()> {
        if self.config.seed_peer.enable {
            let object_storage_port = if self.config.object_storage.enable {
                self.config.object_storage.port
            } else {
                u16::MIN
            };

            // Register the seed peer to the manager.
            self.manager_client
                .update_seed_peer(UpdateSeedPeerRequest {
                    source_type: SourceType::SeedPeerSource.into(),
                    hostname: self.config.host.hostname.clone(),
                    r#type: self.config.seed_peer.kind.to_string(),
                    idc: self.config.host.idc.clone().map_or("".to_string(), |v| v),
                    location: self
                        .config
                        .host
                        .location
                        .clone()
                        .map_or("".to_string(), |v| v),
                    ip: self.config.host.ip.unwrap().to_string(),
                    port: self.config.server.port as i32,
                    download_port: self.config.server.port as i32,
                    seed_peer_cluster_id: self.config.seed_peer.cluster_id,
                    object_storage_port: object_storage_port as i32,
                })
                .await?;
        }

        Ok(())
    }

    // announce_to_scheduler announces the dfdaemon information to the scheduler.
    pub async fn announce_to_scheduler(&mut self) -> Result<()> {
        Ok(())
    }
}
