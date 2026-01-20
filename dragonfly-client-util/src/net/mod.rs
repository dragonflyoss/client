/*
 *     Copyright 2025 The Dragonfly Authors
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
use local_ip_address::{local_ip, local_ipv6};
use pnet::datalink::{self, NetworkInterface};
use std::cmp::min;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::Networks;
use tokio::sync::Mutex;
use tracing::{info, warn};

#[cfg(target_os = "linux")]
use std::{io, mem, os::unix::io::RawFd};

/// join_host_port formats a host and port into `host:port`, adding brackets for IPv6 literals.
///
/// Examples:
/// - ("127.0.0.1", 80) -> "127.0.0.1:80"
/// - ("::1", 80) -> "[::1]:80"
pub fn join_host_port(host: &str, port: u16) -> String {
    if host.is_empty() {
        return format!(":{}", port);
    }

    // If already bracketed, don't double-bracket.
    if host.starts_with('[') && host.ends_with(']') {
        return format!("{}:{}", host, port);
    }

    if host.contains(':') {
        format!("[{}]:{}", host, port)
    } else {
        format!("{}:{}", host, port)
    }
}

/// join_url formats a scheme + host + port into a URL, adding brackets for IPv6 literals.
///
/// Examples:
/// - ("http", "127.0.0.1", 80) -> "http://127.0.0.1:80"
/// - ("http", "::1", 80) -> "http://[::1]:80"
pub fn join_url(scheme: &str, host: &str, port: u16) -> String {
    format!("{}://{}", scheme, join_host_port(host, port))
}

/// Get the local IP address of the machine.
///
/// Attempts to retrieve the local IPv4 address first. If unavailable or if the
/// operation fails, falls back to attempting IPv6 address retrieval.
pub fn preferred_local_ip() -> Option<IpAddr> {
    local_ip_address::local_ip()
        .ok()
        .or_else(|| local_ipv6().ok())
}

/// Interface represents a network interface with its information.
#[derive(Debug, Clone, Default)]
pub struct Interface {
    /// name is the name of the network interface.
    pub name: String,

    /// bandwidth is the bandwidth of the network interface in bps.
    pub bandwidth: u64,

    // network_data_mutex is a mutex to protect access to network data.
    network_data_mutex: Arc<Mutex<()>>,
}

/// NetworkData represents the network data for a specific interface,
#[derive(Debug, Clone, Default)]
pub struct NetworkData {
    /// max_rx_bandwidth is the maximum receive bandwidth of the interface in bps.
    pub max_rx_bandwidth: u64,

    /// rx_bandwidth is the current receive bandwidth of the interface in bps.
    pub rx_bandwidth: Option<u64>,

    /// max_tx_bandwidth is the maximum transmit bandwidth of the interface in bps.
    pub max_tx_bandwidth: u64,

    /// tx_bandwidth is the current transmit bandwidth of the interface in bps.
    pub tx_bandwidth: Option<u64>,
}

/// Interface methods provide functionality to get network interface information.
impl Interface {
    /// DEFAULT_NETWORKS_REFRESH_INTERVAL is the default interval for refreshing network data.
    const DEFAULT_NETWORKS_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

    /// new creates a new Interface instance based on the provided IP address and rate limit.
    pub fn new(ip: IpAddr, rate_limit: ByteSize) -> Interface {
        let rate_limit = Self::byte_size_to_bits(rate_limit); // convert to bps
        let Some(interface) = Self::get_network_interface_by_ip(ip) else {
            warn!(
            "can not find interface for IP address {}, network interface unknown with bandwidth {} bps",
            ip, rate_limit
        );
            return Interface {
                name: "unknown".to_string(),
                bandwidth: rate_limit,
                network_data_mutex: Arc::new(Mutex::new(())),
            };
        };

        match Self::get_speed(&interface.name) {
            Some(speed) => {
                let bandwidth = min(Self::megabits_to_bits(speed), rate_limit);
                info!(
                    "network interface {} with bandwidth {} bps",
                    interface.name, bandwidth
                );

                Interface {
                    name: interface.name,
                    bandwidth,
                    network_data_mutex: Arc::new(Mutex::new(())),
                }
            }
            None => {
                warn!(
                    "can not get speed, network interface {} with bandwidth {} bps",
                    interface.name, rate_limit
                );

                Interface {
                    name: interface.name,
                    bandwidth: rate_limit,
                    network_data_mutex: Arc::new(Mutex::new(())),
                }
            }
        }
    }

    /// get_network_data retrieves the network data for the interface.
    pub async fn get_network_data(&self) -> NetworkData {
        // Lock the mutex to ensure exclusive access to network data.
        let _guard = self.network_data_mutex.lock().await;

        // Initialize sysinfo network.
        let mut networks = Networks::new_with_refreshed_list();

        // Sleep to calculate the network traffic difference over
        // the DEFAULT_NETWORKS_REFRESH_INTERVAL.
        tokio::time::sleep(Self::DEFAULT_NETWORKS_REFRESH_INTERVAL).await;

        // Refresh network information.
        networks.refresh();
        let Some(network_data) = networks.get(self.name.as_str()) else {
            warn!("can not find network data for interface {}", self.name);
            return NetworkData {
                max_rx_bandwidth: self.bandwidth,
                max_tx_bandwidth: self.bandwidth,
                ..Default::default()
            };
        };

        // Calculate the receive and transmit bandwidth in bits per second.
        let rx_bandwidth = (Self::bytes_to_bits(network_data.received()) as f64
            / Self::DEFAULT_NETWORKS_REFRESH_INTERVAL.as_secs_f64())
        .round() as u64;

        // Calculate the transmit bandwidth in bits per second.
        let tx_bandwidth = (Self::bytes_to_bits(network_data.transmitted()) as f64
            / Self::DEFAULT_NETWORKS_REFRESH_INTERVAL.as_secs_f64())
        .round() as u64;

        NetworkData {
            max_rx_bandwidth: self.bandwidth,
            rx_bandwidth: Some(rx_bandwidth),
            max_tx_bandwidth: self.bandwidth,
            tx_bandwidth: Some(tx_bandwidth),
        }
    }

    /// get_speed returns the speed of the network interface in Mbps.
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

    /// get_network_interface_by_ip returns the network interface that has the specified
    /// IP address.
    pub fn get_network_interface_by_ip(ip: IpAddr) -> Option<NetworkInterface> {
        datalink::interfaces()
            .into_iter()
            .find(|interface| interface.ips.iter().any(|ip_net| ip_net.ip() == ip))
    }

    /// byte_size_to_bits converts a ByteSize to bits.
    pub fn byte_size_to_bits(size: ByteSize) -> u64 {
        size.as_u64() * 8
    }

    /// megabits_to_bit converts megabits to bits.
    pub fn megabits_to_bits(size: u64) -> u64 {
        size * 1_000_000 // 1 Mbit = 1,000,000 bits
    }

    /// bytes_to_bits converts bytes to bits.
    pub fn bytes_to_bits(size: u64) -> u64 {
        size * 8 // 1 byte = 8 bits
    }
}

/// set_tcp_fastopen_connect enables TCP Fast Open for client connections on the given socket file
/// descriptor.
#[cfg(target_os = "linux")]
pub fn set_tcp_fastopen_connect(fd: RawFd) -> io::Result<()> {
    let enable: libc::c_int = 1;

    unsafe {
        let ret = libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_FASTOPEN_CONNECT,
            &enable as *const _ as *const libc::c_void,
            mem::size_of_val(&enable) as libc::socklen_t,
        );

        if ret != 0 {
            let err = std::io::Error::last_os_error();
            return Err(err);
        }
    }

    Ok(())
}

/// set_tcp_fastopen enables TCP Fast Open for server connections on the given socket file
/// descriptor.
#[cfg(target_os = "linux")]
pub fn set_tcp_fastopen(fd: RawFd) -> io::Result<()> {
    let queue: libc::c_int = 1024;

    unsafe {
        let ret = libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_FASTOPEN,
            &queue as *const _ as *const libc::c_void,
            mem::size_of_val(&queue) as libc::socklen_t,
        );

        if ret != 0 {
            let err = std::io::Error::last_os_error();
            return Err(err);
        }
    }

    Ok(())
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
            let result = Interface::byte_size_to_bits(input);
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
            let result = Interface::megabits_to_bits(input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_bytes_to_bits() {
        let test_cases = vec![(1u64, 8u64), (1000u64, 8_000u64), (0u64, 0u64)];

        for (input, expected) in test_cases {
            let result = Interface::bytes_to_bits(input);
            assert_eq!(result, expected);
        }
    }

    #[test]
    fn test_join_host_port_ipv4() {
        assert_eq!(join_host_port("127.0.0.1", 80), "127.0.0.1:80");
        assert_eq!(join_host_port("192.168.1.1", 8080), "192.168.1.1:8080");
    }

    #[test]
    fn test_join_host_port_ipv6() {
        assert_eq!(join_host_port("::1", 80), "[::1]:80");
        assert_eq!(join_host_port("2001:db8::1", 8080), "[2001:db8::1]:8080");
        assert_eq!(join_host_port("fe80::1%eth0", 443), "[fe80::1%eth0]:443");
    }

    #[test]
    fn test_join_host_port_already_bracketed() {
        assert_eq!(join_host_port("[::1]", 80), "[::1]:80");
        assert_eq!(join_host_port("[2001:db8::1]", 8080), "[2001:db8::1]:8080");
    }

    #[test]
    fn test_join_host_port_empty() {
        assert_eq!(join_host_port("", 80), ":80");
    }

    #[test]
    fn test_join_host_port_hostname() {
        assert_eq!(join_host_port("example.com", 80), "example.com:80");
        assert_eq!(join_host_port("localhost", 8080), "localhost:8080");
    }

    #[test]
    fn test_join_url_ipv4() {
        assert_eq!(join_url("http", "127.0.0.1", 80), "http://127.0.0.1:80");
        assert_eq!(
            join_url("https", "192.168.1.1", 443),
            "https://192.168.1.1:443"
        );
    }

    #[test]
    fn test_join_url_ipv6() {
        assert_eq!(join_url("http", "::1", 80), "http://[::1]:80");
        assert_eq!(
            join_url("https", "2001:db8::1", 443),
            "https://[2001:db8::1]:443"
        );
    }

    #[test]
    fn test_preferred_local_ip() {
        let ip = preferred_local_ip();
        assert!(ip.is_some());
    }
}
