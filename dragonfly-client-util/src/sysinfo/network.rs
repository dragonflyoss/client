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
use std::sync::Arc;
use std::time::Duration;
use sysinfo::Networks;
use tokio::sync::Mutex;
use tracing::{info, warn};

/// Network represents a network interface with its information.
#[derive(Debug, Clone, Default)]
pub struct Network {
    // The name of the network interface.
    interface_name: String,

    // The bandwidth of the network interface in bits per second (bps).
    bandwidth: u64,

    // Mutex to protect concurrent access to network statistics.
    mutex: Arc<Mutex<()>>,
}

/// NetworkStats represents the network statistics for a specific interface.
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

/// Implementation of network monitoring functionality.
///
/// Provides methods to retrieve network interface information and statistics,
/// including bandwidth measurements and traffic monitoring.
impl Network {
    /// Default interval for refreshing network statistics.
    const DEFAULT_NETWORK_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

    /// Creates a new Network instance based on the provided IP address and rate limit.
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
            return Network {
                interface_name: "unknown".to_string(),
                bandwidth: rate_limit,
                mutex: Arc::new(Mutex::new(())),
            };
        };

        match Self::get_speed(&interface.name) {
            Some(speed) => {
                let bandwidth = min(Self::megabits_to_bits(speed), rate_limit);
                info!(
                    "network interface {} with bandwidth {} bps",
                    interface.name, bandwidth
                );

                Network {
                    interface_name: interface.name,
                    bandwidth,
                    mutex: Arc::new(Mutex::new(())),
                }
            }
            None => {
                warn!(
                    "can not get speed, network interface {} with bandwidth {} bps",
                    interface.name, rate_limit
                );

                Network {
                    interface_name: interface.name,
                    bandwidth: rate_limit,
                    mutex: Arc::new(Mutex::new(())),
                }
            }
        }
    }

    /// Retrieves the network statistics for the interface.
    ///
    /// This method measures network traffic over a time interval (DEFAULT_NETWORK_REFRESH_INTERVAL)
    /// to calculate current receive and transmit bandwidth.
    ///
    /// # Returns
    /// NetworkStats containing maximum and current bandwidth information.
    pub async fn get_stats(&self) -> NetworkStats {
        // Lock the mutex to ensure exclusive access to network stats.
        let _guard = self.mutex.lock().await;

        // Initialize sysinfo network.
        let mut networks = Networks::new_with_refreshed_list();

        // Sleep to calculate the network traffic difference over
        // the DEFAULT_NETWORK_REFRESH_INTERVAL.
        tokio::time::sleep(Self::DEFAULT_NETWORK_REFRESH_INTERVAL).await;

        // Refresh network information to get updated statistics.
        networks.refresh();
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
        let rx_bandwidth = (Self::bytes_to_bits(network_stats.received()) as f64
            / Self::DEFAULT_NETWORK_REFRESH_INTERVAL.as_secs_f64())
        .round() as u64;

        // Calculate the transmit bandwidth in bits per second.
        let tx_bandwidth = (Self::bytes_to_bits(network_stats.transmitted()) as f64
            / Self::DEFAULT_NETWORK_REFRESH_INTERVAL.as_secs_f64())
        .round() as u64;

        NetworkStats {
            max_rx_bandwidth: self.bandwidth,
            rx_bandwidth: Some(rx_bandwidth),
            max_tx_bandwidth: self.bandwidth,
            tx_bandwidth: Some(tx_bandwidth),
        }
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
            let speed_path = format!("/sys/class/net/{}/speed", name);
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

#[cfg(test)]
mod tests {
    use super::*;
    use bytesize::ByteSize;

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
