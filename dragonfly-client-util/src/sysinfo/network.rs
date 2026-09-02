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
use pnet::datalink::{self, NetworkInterface};
use std::cmp::min;
use std::net::IpAddr;
use std::time::Duration;
use sysinfo::Networks;
use tokio::sync::watch;
use tracing::{debug, info, warn};

/// Represents the network statistics for a specific interface.
#[derive(Debug, Clone, Default)]
pub struct NetworkStats {
    /// The maximum receive bandwidth of the interface in bps.
    pub max_rx_bandwidth: u64,

    /// The current receive bandwidth of the interface in bps.
    pub rx_bandwidth: Option<u64>,

    /// The maximum transmit bandwidth of the interface in bps.
    pub max_tx_bandwidth: u64,

    /// The current transmit bandwidth of the interface in bps.
    pub tx_bandwidth: Option<u64>,
}

/// Represents a network interface with its latest statistics.
#[derive(Debug, Clone)]
pub struct Network {
    // The latest statistics published by the collector, None until the first collection.
    stats: watch::Receiver<Option<NetworkStats>>,
}

/// Implementation of network monitoring functionality.
///
/// Provides methods to retrieve network interface information and statistics,
/// including bandwidth measurements and traffic monitoring.
impl Network {
    /// Creates a new Network instance based on the provided IP address and rate limit,
    /// and spawns the statistics collector, so it must be called within a tokio runtime.
    ///
    /// # Arguments
    /// * `ip` - The IP address to identify the network interface.
    /// * `rate_limit` - The rate limit as a ByteSize value.
    ///
    /// # Returns
    /// A new Network instance with the identified interface and calculated bandwidth.
    pub fn new(ip: IpAddr, rate_limit: ByteSize) -> Network {
        let rate_limit = Self::byte_size_to_bits(rate_limit); // convert to bps
        let Some(interface) = Self::get_network_interface_by_ip(ip) else {
            warn!(
            "can not find interface for IP address {}, network interface unknown with bandwidth {} bps",
            ip, rate_limit
        );
            return Self::spawn("unknown".to_string(), rate_limit);
        };

        match Self::get_speed(&interface.name) {
            Some(speed) => {
                let bandwidth = min(Self::megabits_to_bits(speed), rate_limit);
                info!(
                    "network interface {} with bandwidth {} bps",
                    interface.name, bandwidth
                );

                Self::spawn(interface.name, bandwidth)
            }
            None => {
                warn!(
                    "can not get speed, network interface {} with bandwidth {} bps",
                    interface.name, rate_limit
                );

                Self::spawn(interface.name, rate_limit)
            }
        }
    }

    /// Spawns the statistics collector for the interface and returns the Network instance.
    fn spawn(interface_name: String, bandwidth: u64) -> Network {
        let (tx, rx) = watch::channel(None);
        tokio::spawn(
            StatsCollector {
                interface_name,
                bandwidth,
            }
            .run(tx),
        );

        Self { stats: rx }
    }

    /// Retrieves the next network statistics not yet seen by this instance, so a fresh
    /// clone returns the latest statistics right away, or None once the collector has stopped.
    ///
    /// # Returns
    /// NetworkStats containing maximum and current bandwidth information.
    pub async fn get_stats(&mut self) -> Option<NetworkStats> {
        self.stats.changed().await.ok()?;
        self.stats.borrow_and_update().clone()
    }

    /// Retrieves the speed of the network interface in megabits per second (Mbps).
    ///
    /// # Arguments
    /// * `name` - The name of the network interface.
    ///
    /// # Returns
    /// Some(u64) containing the interface speed in Mbps if available,
    /// None otherwise or on non-Linux platforms.
    pub fn get_speed(name: &str) -> Option<u64> {
        #[cfg(target_os = "linux")]
        {
            let speed_path = format!("/sys/class/net/{name}/speed");
            std::fs::read_to_string(&speed_path)
                .ok()
                .and_then(|speed_str| speed_str.trim().parse::<u64>().ok())
        }

        #[cfg(not(target_os = "linux"))]
        {
            warn!("can not get interface {} speed on non-linux platform", name);
            None
        }
    }

    /// Retrieves the network interface that has the specified IP address.
    ///
    /// # Arguments
    /// * `ip` - The IP address to search for.
    ///
    /// # Returns
    /// Some(NetworkInterface) if an interface with the specified IP is found,
    /// None otherwise.
    pub fn get_network_interface_by_ip(ip: IpAddr) -> Option<NetworkInterface> {
        datalink::interfaces()
            .into_iter()
            .find(|interface| interface.ips.iter().any(|ip_net| ip_net.ip() == ip))
    }

    /// Converts a ByteSize value to bits.
    ///
    /// # Arguments
    /// * `size` - The ByteSize value to convert.
    ///
    /// # Returns
    /// The equivalent value in bits.
    pub fn byte_size_to_bits(size: ByteSize) -> u64 {
        size.as_u64() * 8
    }

    /// Converts megabits to bits.
    ///
    /// # Arguments
    /// * `size` - The value in megabits.
    ///
    /// # Returns
    /// The equivalent value in bits (1 Mbit = 1,000,000 bits).
    pub fn megabits_to_bits(size: u64) -> u64 {
        size * 1_000_000 // 1 Mbit = 1,000,000 bits
    }

    /// Converts bytes to bits.
    ///
    /// # Arguments
    /// * `size` - The value in bytes.
    ///
    /// # Returns
    /// The equivalent value in bits (1 byte = 8 bits).
    pub fn bytes_to_bits(size: u64) -> u64 {
        size * 8 // 1 byte = 8 bits
    }
}

/// Collects the traffic statistics of a network interface.
#[derive(Debug)]
struct StatsCollector {
    /// The name of the network interface.
    interface_name: String,

    /// The bandwidth of the network interface in bits per second (bps).
    bandwidth: u64,
}

impl StatsCollector {
    /// Default interval for refreshing network statistics.
    const DEFAULT_NETWORK_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

    /// Collects the statistics continuously and publishes them until every receiver is dropped.
    async fn run(self, tx: watch::Sender<Option<NetworkStats>>) {
        loop {
            let stats = self.collect().await;
            if tx.send(Some(stats)).is_err() {
                return;
            }
        }
    }

    /// Measures network traffic over DEFAULT_NETWORK_REFRESH_INTERVAL to calculate
    /// current receive and transmit bandwidth.
    async fn collect(&self) -> NetworkStats {
        // Initialize sysinfo network.
        let mut networks = Networks::new_with_refreshed_list();

        // Sleep to calculate the network traffic difference over
        // the DEFAULT_NETWORK_REFRESH_INTERVAL.
        tokio::time::sleep(Self::DEFAULT_NETWORK_REFRESH_INTERVAL).await;

        // Refresh network information to get updated statistics.
        networks.refresh(true);
        let Some(network_stats) = networks.get(self.interface_name.as_str()) else {
            warn!(
                "can not find network data for interface {}",
                self.interface_name
            );
            return NetworkStats {
                max_rx_bandwidth: self.bandwidth,
                max_tx_bandwidth: self.bandwidth,
                ..Default::default()
            };
        };

        // Calculate the receive bandwidth in bits per second.
        let rx_bandwidth = (Network::bytes_to_bits(network_stats.received()) as f64
            / Self::DEFAULT_NETWORK_REFRESH_INTERVAL.as_secs_f64())
        .round() as u64;

        // Calculate the transmit bandwidth in bits per second.
        let tx_bandwidth = (Network::bytes_to_bits(network_stats.transmitted()) as f64
            / Self::DEFAULT_NETWORK_REFRESH_INTERVAL.as_secs_f64())
        .round() as u64;

        debug!(
            "network interface {} max receive bandwidth: {} bps, receive bandwidth: {} bps, max transmit bandwidth: {} bps, transmit bandwidth: {} bps",
            self.interface_name, self.bandwidth, rx_bandwidth, self.bandwidth, tx_bandwidth
        );

        NetworkStats {
            max_rx_bandwidth: self.bandwidth,
            rx_bandwidth: Some(rx_bandwidth),
            max_tx_bandwidth: self.bandwidth,
            tx_bandwidth: Some(tx_bandwidth),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytesize::ByteSize;
    use std::net::Ipv4Addr;
    use std::time::Instant;
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn test_get_stats() {
        let mut network = Network::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ByteSize::mb(100));

        let start = Instant::now();
        let mut join_set = JoinSet::new();
        for _ in 0..10 {
            let mut network = network.clone();
            join_set.spawn(async move { network.get_stats().await });
        }
        while let Some(stats) = join_set.join_next().await {
            assert!(stats.unwrap().is_some());
        }
        assert!(start.elapsed() < StatsCollector::DEFAULT_NETWORK_REFRESH_INTERVAL * 2);

        let start = Instant::now();
        assert!(network.get_stats().await.is_some());
        assert!(start.elapsed() < StatsCollector::DEFAULT_NETWORK_REFRESH_INTERVAL / 2);

        let start = Instant::now();
        assert!(network.get_stats().await.is_some());
        assert!(start.elapsed() >= StatsCollector::DEFAULT_NETWORK_REFRESH_INTERVAL / 2);
        assert!(start.elapsed() < StatsCollector::DEFAULT_NETWORK_REFRESH_INTERVAL * 2);
    }

    #[test]
    fn test_byte_size_to_bits() {
        let test_cases = vec![
            (ByteSize::kb(1), 8_000u64),
            (ByteSize::mb(1), 8_000_000u64),
            (ByteSize::gb(1), 8_000_000_000u64),
            (ByteSize::b(0), 0u64),
        ];

        for (input, expected) in test_cases {
            let result = Network::byte_size_to_bits(input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_megabits_to_bits() {
        let test_cases = vec![
            (1u64, 1_000_000u64),
            (1000u64, 1_000_000_000u64),
            (0u64, 0u64),
        ];

        for (input, expected) in test_cases {
            let result = Network::megabits_to_bits(input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_bytes_to_bits() {
        let test_cases = vec![(1u64, 8u64), (1000u64, 8_000u64), (0u64, 0u64)];

        for (input, expected) in test_cases {
            let result = Network::bytes_to_bits(input);
            assert_eq!(result, expected);
        }
    }
}
