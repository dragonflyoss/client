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

use crate::grpc::scheduler::SchedulerClient;
use dragonfly_api::common::v2::{Build, Cpu, Disk, Host, Memory, Network};
use dragonfly_api::scheduler::v2::{AnnounceHostRequest, DeleteHostRequest};
use dragonfly_client_config::{
    dfdaemon::{Config, HostType},
    CARGO_PKG_RUSTC_VERSION, CARGO_PKG_VERSION, GIT_COMMIT_SHORT_HASH,
};
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::Result;
use dragonfly_client_util::{net::Interface, shutdown};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::System;
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument};

/// Announcer is used to announce the dfdaemon information to the manager and scheduler.
pub struct SchedulerAnnouncer {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// host_id is the id of the host.
    host_id: String,

    /// scheduler_client is the grpc client of the scheduler.
    scheduler_client: Arc<SchedulerClient>,

    /// interface is the network interface.
    interface: Arc<Interface>,

    /// shutdown is used to shutdown the announcer.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the announcer is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// SchedulerAnnouncer implements the scheduler announcer of the dfdaemon.
impl SchedulerAnnouncer {
    /// new creates a new scheduler announcer.
    pub async fn new(
        config: Arc<Config>,
        host_id: String,
        scheduler_client: Arc<SchedulerClient>,
        interface: Arc<Interface>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Result<Self> {
        let announcer = Self {
            config,
            host_id,
            scheduler_client,
            interface,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        };

        // Initialize the scheduler announcer.
        announcer
            .scheduler_client
            .init_announce_host(announcer.make_announce_host_request(Duration::ZERO).await?)
            .await?;
        Ok(announcer)
    }

    /// run announces the dfdaemon information to the scheduler.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Start the scheduler announcer.
        let mut interval = tokio::time::interval(self.config.scheduler.announce_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let request = match self.make_announce_host_request(interval.period()).await {
                        Ok(request) => request,
                        Err(err) => {
                            error!("make announce host request failed: {}", err);
                            continue;
                        }
                    };

                    if let Err(err) = self.scheduler_client.announce_host(request).await {
                        error!("announce host to scheduler failed: {}", err);
                    };
                }
                _ = shutdown.recv() => {
                    // Announce to scheduler shutting down with signals.
                    if let Err(err) = self.scheduler_client.delete_host(DeleteHostRequest{
                        host_id: self.host_id.clone(),
                    }).await {
                        error!("delete host from scheduler failed: {}", err);
                    }

                    info!("announce to scheduler shutting down");
                    return
                }
            }
        }
    }

    /// make_announce_host_request makes the announce host request.
    #[instrument(skip_all)]
    async fn make_announce_host_request(&self, interval: Duration) -> Result<AnnounceHostRequest> {
        // If the seed peer is enabled, we should announce the seed peer to the scheduler.
        let host_type = if self.config.seed_peer.enable {
            self.config.seed_peer.kind
        } else {
            HostType::Normal
        };

        // Refresh the system information.
        let mut sys = System::new_all();
        sys.refresh_all();

        // Get the process information.
        let process = sys.process(sysinfo::get_current_pid().unwrap()).unwrap();

        // Get the cpu information.
        let cpu = Cpu {
            logical_count: sys.physical_core_count().unwrap_or_default() as u32,
            physical_count: sys.physical_core_count().unwrap_or_default() as u32,
            percent: sys.global_cpu_usage() as f64,
            process_percent: process.cpu_usage() as f64,

            // TODO: Get the cpu times.
            times: None,
        };

        // Get the memory information.
        let memory = Memory {
            total: sys.total_memory(),
            available: sys.available_memory(),
            used: sys.used_memory(),
            used_percent: (sys.used_memory() / sys.total_memory()) as f64,
            process_used_percent: (process.memory() / sys.total_memory()) as f64,
            free: sys.free_memory(),
        };

        // Wait for getting the network data.
        let network_data = self.interface.get_network_data().await;
        debug!(
            "network data: rx bandwidth {}/{} bps, tx bandwidth {}/{} bps",
            network_data.rx_bandwidth.unwrap_or(0),
            network_data.max_rx_bandwidth,
            network_data.tx_bandwidth.unwrap_or(0),
            network_data.max_tx_bandwidth
        );

        // Get the network information.
        let network = Network {
            idc: self.config.host.idc.clone(),
            location: self.config.host.location.clone(),
            max_rx_bandwidth: network_data.max_rx_bandwidth,
            rx_bandwidth: network_data.rx_bandwidth,
            max_tx_bandwidth: network_data.max_tx_bandwidth,
            tx_bandwidth: network_data.tx_bandwidth,
            ..Default::default()
        };

        // Get the disk information.
        let stats = fs2::statvfs(self.config.storage.dir.as_path())?;
        let total_space = stats.total_space();
        let available_space = stats.available_space();
        let used_space = total_space - available_space;
        let used_percent = (used_space as f64 / (total_space) as f64) * 100.0;

        let mut write_bandwidth = 0;
        let mut read_bandwidth = 0;
        if interval != Duration::ZERO {
            let disk_usage = process.disk_usage();
            write_bandwidth = disk_usage.written_bytes / interval.as_secs();
            read_bandwidth = disk_usage.read_bytes / interval.as_secs();
        };

        let disk = Disk {
            total: total_space,
            free: available_space,
            used: used_space,
            used_percent,
            write_bandwidth,
            read_bandwidth,

            // TODO: Get the disk inodes information.
            inodes_total: 0,
            inodes_used: 0,
            inodes_free: 0,
            inodes_used_percent: 0.0,
        };

        // Get the build information.
        let build = Build {
            git_version: CARGO_PKG_VERSION.to_string(),
            git_commit: Some(GIT_COMMIT_SHORT_HASH.to_string()),
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
            os: env::consts::OS.to_string(),
            platform: env::consts::OS.to_string(),
            platform_family: env::consts::FAMILY.to_string(),
            platform_version: System::os_version().unwrap_or_default(),
            kernel_version: System::kernel_version().unwrap_or_default(),
            cpu: Some(cpu),
            memory: Some(memory),
            network: Some(network),
            disk: Some(disk),
            build: Some(build),
            scheduler_cluster_id: self.config.host.scheduler_cluster_id.unwrap_or_default(),
            disable_shared: self.config.upload.disable_shared,
            proxy_port: self.config.proxy.server.port as i32,
        };

        Ok(AnnounceHostRequest {
            host: Some(host),
            interval: Some(
                prost_wkt_types::Duration::try_from(self.config.scheduler.announce_interval)
                    .or_err(ErrorType::ParseError)?,
            ),
        })
    }
}
