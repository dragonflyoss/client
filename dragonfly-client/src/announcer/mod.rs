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
    CARGO_PKG_RUSTC_VERSION, CARGO_PKG_VERSION, GIT_COMMIT_SHORT_HASH, INSTANCE_NAME,
};
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::Result;
use dragonfly_client_util::{shutdown, sysinfo::SystemMonitor};
use std::env;
use std::sync::Arc;
use sysinfo::System;
use tokio::sync::mpsc;
use tracing::{debug, error, info, instrument};

/// Announcer for broadcasting dfdaemon information to the manager and scheduler.
pub struct SchedulerAnnouncer {
    /// Configuration of the dfdaemon.
    config: Arc<Config>,

    /// ID of the host.
    host_id: String,

    /// gRPC client for the scheduler.
    scheduler_client: Arc<SchedulerClient>,

    /// System interface for monitoring.
    system_monitor: Arc<SystemMonitor>,

    /// Used to shut down the announcer.
    shutdown: shutdown::Shutdown,

    /// Used to notify that the announcer shutdown is complete.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// SchedulerAnnouncer implements the scheduler announcer of the dfdaemon.
impl SchedulerAnnouncer {
    /// Creates a new scheduler announcer.
    pub async fn new(
        config: Arc<Config>,
        host_id: String,
        scheduler_client: Arc<SchedulerClient>,
        system_monitor: Arc<SystemMonitor>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Result<Self> {
        let announcer = Self {
            config,
            host_id,
            scheduler_client,
            system_monitor,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        };

        // Initialize the scheduler announcer.
        announcer
            .scheduler_client
            .init_announce_host(announcer.make_announce_host_request().await?)
            .await?;
        Ok(announcer)
    }

    /// Announces the dfdaemon information to the scheduler.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Start the scheduler announcer.
        let mut interval = tokio::time::interval(self.config.scheduler.announce_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let request = match self.make_announce_host_request().await {
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

    /// Makes the announce host request.
    #[instrument(skip_all)]
    async fn make_announce_host_request(&self) -> Result<AnnounceHostRequest> {
        // If the seed peer is enabled, we should announce the seed peer to the scheduler.
        let host_type = if self.config.seed_peer.enable {
            self.config.seed_peer.kind
        } else {
            HostType::Normal
        };
        let pid = std::process::id();

        // Get the cpu information.
        let cpu_stats = self.system_monitor.cpu.get_stats();
        let process_cpu_stats = self.system_monitor.cpu.get_process_stats(pid);
        let cpu = Cpu {
            logical_count: cpu_stats.logical_core_count,
            physical_count: cpu_stats.physical_core_count,
            percent: cpu_stats.used_percent,
            process_percent: process_cpu_stats.used_percent,

            // TODO: Get the cpu times.
            times: None,
        };

        // Get the memory information.
        let memory_stats = self.system_monitor.memory.get_stats();
        let process_memory_stats = self.system_monitor.memory.get_process_stats(pid);
        let memory = Memory {
            total: memory_stats.total,
            free: memory_stats.free,
            available: memory_stats.available,
            used: memory_stats.usage,
            used_percent: memory_stats.used_percent,
            process_used_percent: process_memory_stats.used_percent,
        };

        // Wait for getting the network data.
        let network_stats = self.system_monitor.network.get_stats().await;
        debug!(
            "network data: rx bandwidth {}/{} bps, tx bandwidth {}/{} bps",
            network_stats.rx_bandwidth.unwrap_or(0),
            network_stats.max_rx_bandwidth,
            network_stats.tx_bandwidth.unwrap_or(0),
            network_stats.max_tx_bandwidth
        );

        // Get the network information.
        let network = Network {
            idc: self.config.host.idc.clone(),
            location: self.config.host.location.clone(),
            max_rx_bandwidth: network_stats.max_rx_bandwidth,
            rx_bandwidth: network_stats.rx_bandwidth,
            max_tx_bandwidth: network_stats.max_tx_bandwidth,
            tx_bandwidth: network_stats.tx_bandwidth,
            ..Default::default()
        };

        // Get the disk information.
        let disk_stats = self
            .system_monitor
            .disk
            .get_stats(self.config.storage.dir.as_path())?;
        let process_disk_stats = self.system_monitor.disk.get_process_stats(pid).await;
        let disk = Disk {
            total: disk_stats.total,
            free: disk_stats.free,
            used: disk_stats.usage,
            used_percent: disk_stats.used_percent,
            write_bandwidth: process_disk_stats.write_bandwidth,
            read_bandwidth: process_disk_stats.read_bandwidth,

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
            name: INSTANCE_NAME.clone(),
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
