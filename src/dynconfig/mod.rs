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

use crate::config::{dfdaemon::Config, CARGO_PKG_VERSION, GIT_HASH};
use crate::grpc::manager::ManagerClient;
use crate::shutdown;
use crate::{Error, Result};
use dragonfly_api::manager::v2::{
    GetObjectStorageRequest, ListSchedulersRequest, ListSchedulersResponse, ObjectStorage,
    SourceType,
};
use tokio::sync::mpsc;
use tracing::{error, info};

// Data is the dynamic configuration of the dfdaemon.
pub struct Data {
    // schedulers is the schedulers of the dfdaemon.
    schedulers: ListSchedulersResponse,

    // object_storage is the object storage configuration of the dfdaemon.
    object_storage: Option<ObjectStorage>,
}

// Dynconfig supports dynamic configuration of the client.
pub struct Dynconfig {
    // config is the configuration of the dfdaemon.
    config: Config,

    // data is the dynamic configuration of the dfdaemon.
    data: Data,

    // manager_client is the grpc client of the manager.
    manager_client: ManagerClient,

    // shutdown is used to shutdown the announcer.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the announcer is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// Dynconfig is the implementation of Dynconfig.
impl Dynconfig {
    // new creates a new Dynconfig.
    pub async fn new(
        config: Config,
        manager_client: ManagerClient,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Result<Self> {
        // Create a new Dynconfig.
        let mut dc = Dynconfig {
            config,
            data: Data {
                schedulers: ListSchedulersResponse::default(),
                object_storage: None,
            },
            manager_client,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        };

        // Get the initial dynamic configuration.
        dc.refresh().await?;
        Ok(dc)
    }

    // run starts the dynconfig server.
    pub async fn run(&mut self) -> Result<()> {
        // Start the refresh loop.
        let mut interval = tokio::time::interval(self.config.dynconfig.refresh_interval);

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    if let Err(err) = self.refresh().await {
                        error!("refresh dynconfig failed: {}", err);
                    };
                }
                _ = self.shutdown.recv() => {
                    // Dynconfig server shutting down with signals.
                    info!("dynconfig server shutting down");
                    return Ok(());
                }
            }
        }
    }

    // get_schedulers returns the schedulers of the dfdaemon.
    pub fn get_schedulers(&self) -> ListSchedulersResponse {
        self.data.schedulers.clone()
    }

    // get_object_storage returns the object storage configuration of the dfdaemon.
    pub fn get_object_storage(&self) -> Option<ObjectStorage> {
        self.data.object_storage.clone()
    }

    // refresh refreshes the dynamic configuration of the dfdaemon.
    async fn refresh(&mut self) -> Result<()> {
        self.data = self.get().await?;
        Ok(())
    }

    // get returns the dynamic configuration of the dfdaemon.
    async fn get(&mut self) -> Result<Data> {
        // Get the source type.
        let source_type = if self.config.seed_peer.enable {
            SourceType::SeedPeerSource.into()
        } else {
            SourceType::PeerSource.into()
        };

        // Get the schedulers from the manager.
        let schedulers = self
            .manager_client
            .list_schedulers(ListSchedulersRequest {
                source_type,
                hostname: self.config.host.hostname.clone(),
                ip: self.config.host.ip.unwrap().to_string(),
                idc: self.config.host.idc.clone(),
                location: self.config.host.location.clone(),
                version: CARGO_PKG_VERSION.to_string(),
                commit: GIT_HASH.unwrap_or("").to_string(),
            })
            .await?;

        // Get the object storage configuration from the manager.
        match self
            .manager_client
            .get_object_storage(GetObjectStorageRequest {
                source_type,
                hostname: self.config.host.hostname.clone(),
                ip: self.config.host.ip.unwrap().to_string(),
            })
            .await
        {
            Ok(object_storage) => Ok(Data {
                schedulers,
                object_storage: Some(object_storage),
            }),
            Err(err) => {
                // If the object storage is not found, return the schedulers only.
                if let Error::TonicStatus(status) = err {
                    if status.code() == tonic::Code::NotFound {
                        return Ok(Data {
                            schedulers,
                            object_storage: None,
                        });
                    }

                    Err(Error::TonicStatus(status))
                } else {
                    Err(err)
                }
            }
        }
    }
}
