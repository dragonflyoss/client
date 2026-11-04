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
use std::time::{Duration, Instant};
use tokio::sync::watch;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{debug, error, info, warn};

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
    // The latest statistics published by the collector.
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
    /// The rate limit is published right away as the first statistics, so callers of
    /// get_stats do not wait for the interface lookup.
    ///
    /// # Arguments
    /// * `ip` - The IP address to identify the network interface.
    /// * `rate_limit` - The rate limit as a ByteSize value.
    ///
    /// # Returns
    /// A new Network instance whose collector measures the identified interface.
    pub fn new(ip: IpAddr, rate_limit: ByteSize) -> Network {
        let rate_limit = Self::byte_size_to_bits(rate_limit); // convert to bps
        let (tx, rx) = watch::channel(None);
        tx.send_replace(Some(NetworkStats {
            max_rx_bandwidth: rate_limit,
            max_tx_bandwidth: rate_limit,
            ..Default::default()
        }));

        tokio::spawn(async move {
            // The interface lookup and the speed read take the kernel RTNL lock, which
            // can be held for minutes on hosts that add and remove interfaces often, so
            // they must not block the runtime workers.
            let (interface_name, bandwidth) =
                match tokio::task::spawn_blocking(move || Self::resolve(ip, rate_limit)).await {
                    Ok(resolved) => resolved,
                    Err(err) => {
                        error!("resolve network interface failed: {}", err);
                        (None, rate_limit)
                    }
                };

            StatsCollector {
                interface_name,
                bandwidth,
            }
            .run(tx)
            .await
        });

        Self { stats: rx }
    }

    /// Finds the name of the interface that has the IP address and its bandwidth,
    /// capped by the rate limit. It blocks, so it must run on the blocking pool.
    fn resolve(ip: IpAddr, rate_limit: u64) -> (Option<String>, u64) {
        let Some(interface) = Self::get_network_interface_by_ip(ip) else {
            warn!(
                "can not find interface for IP address {}, network interface unknown with bandwidth {} bps",
                ip, rate_limit
            );
            return (None, rate_limit);
        };

        match Self::get_speed(&interface.name) {
            Some(speed) => {
                let bandwidth = min(Self::megabits_to_bits(speed), rate_limit);
                info!(
                    "network interface {} with bandwidth {} bps",
                    interface.name, bandwidth
                );

                (Some(interface.name), bandwidth)
            }
            None => {
                warn!(
                    "can not get speed, network interface {} with bandwidth {} bps",
                    interface.name, rate_limit
                );

                (Some(interface.name), rate_limit)
            }
        }
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

/// Cumulative traffic counters of a network interface at a point in time.
#[derive(Debug, Clone, Copy)]
struct Counters {
    /// The time the counters were read.
    read_at: Instant,

    /// The total number of bytes received.
    rx_bytes: u64,

    /// The total number of bytes transmitted.
    tx_bytes: u64,
}

/// Collects the traffic statistics of a network interface.
#[derive(Debug)]
struct StatsCollector {
    /// The name of the network interface, None if it is unknown.
    interface_name: Option<String>,

    /// The bandwidth of the network interface in bits per second (bps).
    bandwidth: u64,
}

impl StatsCollector {
    /// Default interval for refreshing network statistics.
    const DEFAULT_NETWORK_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

    /// Reads the counters every DEFAULT_NETWORK_REFRESH_INTERVAL and publishes the bandwidth
    /// over the last interval, until every receiver is dropped.
    async fn run(self, tx: watch::Sender<Option<NetworkStats>>) {
        let mut ticker = interval(Self::DEFAULT_NETWORK_REFRESH_INTERVAL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

        // The first tick completes immediately.
        ticker.tick().await;
        let mut previous = self.read_counters();
        loop {
            ticker.tick().await;
            let current = self.read_counters();
            if tx.send(Some(self.calculate(previous, current))).is_err() {
                return;
            }

            previous = current;
        }
    }

    /// Calculates the receive and transmit bandwidth between two readings of the counters.
    fn calculate(&self, previous: Option<Counters>, current: Option<Counters>) -> NetworkStats {
        let mut stats = NetworkStats {
            max_rx_bandwidth: self.bandwidth,
            max_tx_bandwidth: self.bandwidth,
            ..Default::default()
        };

        let (Some(previous), Some(current)) = (previous, current) else {
            return stats;
        };

        let elapsed = current
            .read_at
            .duration_since(previous.read_at)
            .as_secs_f64();
        let bandwidth = |previous_bytes: u64, current_bytes: u64| {
            // The counters start over when the interface is recreated.
            let bytes = current_bytes.saturating_sub(previous_bytes);
            (Network::bytes_to_bits(bytes) as f64 / elapsed).round() as u64
        };

        stats.rx_bandwidth = Some(bandwidth(previous.rx_bytes, current.rx_bytes));
        stats.tx_bandwidth = Some(bandwidth(previous.tx_bytes, current.tx_bytes));
        debug!(
            "network interface {:?} max bandwidth: {} bps, receive bandwidth: {:?} bps, transmit bandwidth: {:?} bps",
            self.interface_name, self.bandwidth, stats.rx_bandwidth, stats.tx_bandwidth
        );

        stats
    }

    /// Reads the counters of the interface from sysfs. Unlike getifaddrs, which sysinfo
    /// uses, this does not take the kernel RTNL lock.
    fn read_counters(&self) -> Option<Counters> {
        #[cfg(target_os = "linux")]
        {
            let interface_name = self.interface_name.as_deref()?;
            let read = |counter: &str| {
                let path = format!("/sys/class/net/{interface_name}/statistics/{counter}");
                std::fs::read_to_string(&path)
                    .inspect_err(|err| warn!("can not read {}: {}", path, err))
                    .ok()?
                    .trim()
                    .parse::<u64>()
                    .ok()
            };

            Some(Counters {
                read_at: Instant::now(),
                rx_bytes: read("rx_bytes")?,
                tx_bytes: read("tx_bytes")?,
            })
        }

        #[cfg(not(target_os = "linux"))]
        {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::type_complexity)]

    use super::*;
    use bytesize::ByteSize;
    use std::net::Ipv4Addr;
    use tokio::task::JoinSet;

    #[tokio::test]
    async fn get_stats_shares_one_collector_across_clones() {
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

    #[tokio::test]
    async fn new_falls_back_to_the_rate_limit_for_an_unknown_interface() {
        let mut network =
            Network::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)), ByteSize::mb(100));

        let stats = network.get_stats().await.unwrap();
        assert_eq!(stats.max_rx_bandwidth, 800_000_000);
        assert_eq!(stats.max_tx_bandwidth, 800_000_000);
        assert_eq!(stats.rx_bandwidth, None);
        assert_eq!(stats.tx_bandwidth, None);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn collector_measures_loopback_traffic() {
        use std::net::UdpSocket;

        let mut network = Network::new(IpAddr::V4(Ipv4Addr::LOCALHOST), ByteSize::gb(10));

        let first = network.get_stats().await.unwrap();
        assert_eq!(first.rx_bandwidth, None);
        assert_eq!(first.tx_bandwidth, None);

        let receiver = UdpSocket::bind("127.0.0.1:0").unwrap();
        let sender = UdpSocket::bind("127.0.0.1:0").unwrap();
        let payload = vec![0u8; 1024];
        let deadline = Instant::now() + StatsCollector::DEFAULT_NETWORK_REFRESH_INTERVAL * 5;
        while Instant::now() < deadline {
            for _ in 0..100 {
                sender
                    .send_to(&payload, receiver.local_addr().unwrap())
                    .unwrap();
            }

            let stats = network.get_stats().await.unwrap();
            if stats.rx_bandwidth.is_some_and(|rx| rx > 0) {
                assert!(stats.tx_bandwidth.unwrap() > 0);
                return;
            }
        }

        panic!("loopback traffic was not measured");
    }

    #[test]
    fn calculate_returns_the_bandwidth_between_two_readings() {
        let collector = StatsCollector {
            interface_name: Some("eth0".to_string()),
            bandwidth: 1_000,
        };
        let start = Instant::now();
        let counters = |seconds: u64, rx_bytes: u64, tx_bytes: u64| Counters {
            read_at: start + Duration::from_secs(seconds),
            rx_bytes,
            tx_bytes,
        };

        let test_cases = vec![
            (
                Some(counters(0, 1_000, 2_000)),
                Some(counters(2, 3_000, 2_500)),
                Some(8_000),
                Some(2_000),
            ),
            (
                Some(counters(0, 5_000, 5_000)),
                Some(counters(1, 1_000, 6_000)),
                Some(0),
                Some(8_000),
            ),
            (None, Some(counters(1, 1_000, 1_000)), None, None),
            (Some(counters(0, 1_000, 1_000)), None, None, None),
        ];

        for (previous, current, rx_bandwidth, tx_bandwidth) in test_cases {
            let stats = collector.calculate(previous, current);
            assert_eq!(stats.max_rx_bandwidth, 1_000);
            assert_eq!(stats.max_tx_bandwidth, 1_000);
            assert_eq!(stats.rx_bandwidth, rx_bandwidth);
            assert_eq!(stats.tx_bandwidth, tx_bandwidth);
        }
    }

    #[test]
    fn get_network_interface_by_ip_finds_only_bound_addresses() {
        let test_cases: Vec<(IpAddr, fn(Option<NetworkInterface>))> = vec![
            (IpAddr::V4(Ipv4Addr::LOCALHOST), |interface| {
                assert!(interface.unwrap().is_loopback());
            }),
            (IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)), |interface| {
                assert!(interface.is_none());
            }),
        ];

        for (ip, expect) in test_cases {
            expect(Network::get_network_interface_by_ip(ip));
        }
    }

    #[test]
    fn byte_size_to_bits_multiplies_by_eight() {
        let test_cases = vec![
            (ByteSize::kb(1), 8_000),
            (ByteSize::mb(1), 8_000_000),
            (ByteSize::gb(1), 8_000_000_000),
            (ByteSize::b(0), 0),
        ];

        for (size, expected) in test_cases {
            assert_eq!(Network::byte_size_to_bits(size), expected);
        }
    }

    #[test]
    fn megabits_to_bits_multiplies_by_a_million() {
        let test_cases = vec![(1, 1_000_000), (1000, 1_000_000_000), (0, 0)];

        for (size, expected) in test_cases {
            assert_eq!(Network::megabits_to_bits(size), expected);
        }
    }

    #[test]
    fn bytes_to_bits_multiplies_by_eight() {
        let test_cases = vec![(1, 8), (1000, 8_000), (0, 0)];

        for (size, expected) in test_cases {
            assert_eq!(Network::bytes_to_bits(size), expected);
        }
    }
}
