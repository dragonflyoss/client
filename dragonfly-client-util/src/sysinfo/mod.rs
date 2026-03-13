/*
 *     Copyright 2026 The Dragonfly Authors
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

use bytesize::ByteSize;
use std::net::IpAddr;

pub mod cpu;
pub mod disk;
pub mod memory;
pub mod network;

/// SystemMonitor resource monitor.
///
/// This struct aggregates all system resource monitors including CPU, memory,
/// disk, and network. It provides a unified interface for tracking and
/// querying system resource usage.
pub struct SystemMonitor {
    /// CPU resource monitor for tracking processor usage.
    pub cpu: cpu::CPU,

    /// Memory resource monitor for tracking RAM usage.
    pub memory: memory::Memory,

    /// Disk resource monitor for tracking storage I/O and usage.
    pub disk: disk::Disk,

    /// Network resource monitor for tracking bandwidth and traffic.
    pub network: network::Network,
}

/// Implementation of the System resource monitor.
impl SystemMonitor {
    /// Constructs a new System resource monitor
    ///
    /// Initializes all resource monitors with default values except for the
    /// network monitor, which requires specific configuration parameters.
    ///
    /// # Arguments
    ///
    /// * `ip` - The IP address to monitor for network traffic
    /// * `network_bandwidth_limit` - The bandwidth limit for network monitoring
    pub fn new(ip: IpAddr, network_bandwidth_limit: ByteSize) -> Self {
        Self {
            cpu: cpu::CPU::new(),
            memory: memory::Memory::default(),
            disk: disk::Disk::new(),
            network: network::Network::new(ip, network_bandwidth_limit),
        }
    }
}
