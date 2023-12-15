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

use crate::config::{
    dfdaemon::{Config, HostType},
    CARGO_PKG_RUSTC_VERSION, CARGO_PKG_VERSION, GIT_HASH,
};
use crate::grpc::{manager::ManagerClient, scheduler::SchedulerClient};
use crate::shutdown;
use crate::Result;
use dragonfly_api::common::v2::{Build, Cpu, Host, Memory, Network};
use dragonfly_api::manager::v2::{DeleteSeedPeerRequest, SourceType, UpdateSeedPeerRequest};
use dragonfly_api::scheduler::v2::AnnounceHostRequest;
use std::env;
use std::sync::Arc;
use sysinfo::{CpuExt, ProcessExt, System, SystemExt};
use tokio::sync::mpsc;
use tracing::{error, info};

// ManagerAnnouncer is used to announce the dfdaemon information to the manager.
pub struct ManagerAnnouncer {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

    // manager_client is the grpc client of the manager.
    manager_client: Arc<ManagerClient>,

    // shutdown is used to shutdown the announcer.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the announcer is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// ManagerAnnouncer implements the manager announcer of the dfdaemon.
impl ManagerAnnouncer {
    // new creates a new manager announcer.
    pub fn new(
        config: Arc<Config>,
        manager_client: Arc<ManagerClient>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            config,
            manager_client,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    // run announces the dfdaemon information to the manager.
    pub async fn run(&self) -> Result<()> {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // If the seed peer is enabled, we should announce the seed peer to the manager.
        if self.config.seed_peer.enable {
            // Get the object storage port.
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
                    idc: self.config.host.idc.clone(),
                    location: self.config.host.location.clone(),
                    ip: self.config.host.ip.unwrap().to_string(),
                    port: self.config.upload.server.port as i32,
                    download_port: self.config.upload.server.port as i32,
                    seed_peer_cluster_id: self.config.seed_peer.cluster_id,
                    object_storage_port: object_storage_port as i32,
                })
                .await?;

            // Announce to scheduler shutting down with signals.
            shutdown.recv().await;

            // Delete the seed peer from the manager.
            self.manager_client
                .delete_seed_peer(DeleteSeedPeerRequest {
                    source_type: SourceType::SeedPeerSource.into(),
                    hostname: self.config.host.hostname.clone(),
                    ip: self.config.host.ip.unwrap().to_string(),
                    seed_peer_cluster_id: self.config.seed_peer.cluster_id,
                })
                .await?;

            info!("announce to manager shutting down");
        } else {
            shutdown.recv().await;
            info!("announce to manager shutting down");
        }

        Ok(())
    }
}

// Announcer is used to announce the dfdaemon information to the manager and scheduler.
pub struct SchedulerAnnouncer {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

    // host_id is the id of the host.
    host_id: String,

    // scheduler_client is the grpc client of the scheduler.
    scheduler_client: Arc<SchedulerClient>,

    // shutdown is used to shutdown the announcer.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the announcer is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// SchedulerAnnouncer implements the scheduler announcer of the dfdaemon.
impl SchedulerAnnouncer {
    // new creates a new scheduler announcer.
    pub async fn new(
        config: Arc<Config>,
        host_id: String,
        scheduler_client: Arc<SchedulerClient>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Result<Self> {
        let announcer = Self {
            config,
            host_id,
            scheduler_client,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        };

        // Initialize the scheduler announcer.
        announcer
            .scheduler_client
            .init_announce_host(announcer.make_announce_host_request()?)
            .await?;
        Ok(announcer)
    }

    // run announces the dfdaemon information to the scheduler.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Start the scheduler announcer.
        let mut interval = tokio::time::interval(self.config.scheduler.announce_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let request = match self.make_announce_host_request() {
                        Ok(request) => request,
                        Err(err) => {
                            error!("make announce host request failed: {}", err);
                            continue;
                        }
                    };

                    if let Err(err) = self.scheduler_client.init_announce_host(request).await {
                        error!("announce host to scheduler failed: {}", err);
                    };
                }
                _ = shutdown.recv() => {
                    // Announce to scheduler shutting down with signals.
                    info!("announce to scheduler shutting down");
                    return
                }
            }
        }
    }

    // make_announce_host_request makes the announce host request.
    fn make_announce_host_request(&self) -> Result<AnnounceHostRequest> {
        // If the seed peer is enabled, we should announce the seed peer to the scheduler.
        let host_type = if self.config.seed_peer.enable {
            self.config.seed_peer.kind
        } else {
            HostType::Normal
        };

        // Get the system information.
        let mut sys = System::new_all();
        sys.refresh_all();

        // Get the process information.
        let process = sys.process(sysinfo::get_current_pid().unwrap()).unwrap();

        // Get the cpu information.
        let cpu = Cpu {
            logical_count: sys.physical_core_count().unwrap_or_default() as u32,
            physical_count: sys.physical_core_count().unwrap_or_default() as u32,
            percent: sys.global_cpu_info().cpu_usage() as f64,
            process_percent: process.cpu_usage() as f64,

            // TODO Get the cpu times.
            times: None,
        };

        // Get the memory information.
        let memory = Memory {
            total: sys.total_memory(),
            available: sys.available_memory(),
            used: sys.used_memory(),
            used_percent: (sys.used_memory() / sys.total_memory()) as f64,

            // TODO Get the process used memory.
            process_used_percent: 0 as f64,
            free: sys.free_memory(),
        };

        // Get the network information.
        let network = Network {
            // TODO Get the count of the tcp connection.
            tcp_connection_count: 0,

            // TODO Get the count of the upload tcp connection.
            upload_tcp_connection_count: 0,
            idc: self.config.host.idc.clone(),
            location: self.config.host.location.clone(),
        };

        // Get the build information.
        let build = Build {
            git_version: CARGO_PKG_VERSION.to_string(),
            git_commit: Some(GIT_HASH.unwrap_or_default().to_string()),
            go_version: None,
            rust_version: Some(CARGO_PKG_RUSTC_VERSION.to_string()),
            platform: None,
        };

        // Struct the host information.
        let host = Host {
            id: self.host_id.to_string(),
            r#type: host_type as u32,
            hostname: self.config.host.hostname.clone(),
            ip: self.config.host.ip.unwrap().to_string(),
            port: self.config.upload.server.port as i32,
            download_port: self.config.upload.server.port as i32,
            object_storage_port: self.config.object_storage.port as i32,
            os: env::consts::OS.to_string(),
            platform: env::consts::OS.to_string(),
            platform_family: env::consts::FAMILY.to_string(),
            platform_version: sys.os_version().unwrap_or_default(),
            kernel_version: sys.kernel_version().unwrap_or_default(),
            cpu: Some(cpu),
            memory: Some(memory),
            network: Some(network),

            // TODO Get the disk information.
            disk: None,
            build: Some(build),

            // TODO Get scheduler cluster id from dynconfig.
            scheduler_cluster_id: 0,
        };

        Ok(AnnounceHostRequest { host: Some(host) })
    }
}
