/*
 * Copyright 2025 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use dragonfly_api::common::v2::Host;
use dragonfly_api::scheduler::v2::scheduler_client::SchedulerClient;
use dragonfly_client_core::{Error, Result};
use futures::stream::{self, StreamExt};
use hashring::HashRing;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::time::Duration;
use tokio::sync::RwLock;
use tonic::transport::{Channel, Endpoint};
use tonic_health::pb::{
    health_client::HealthClient as HealthGRPCClient, HealthCheckRequest, HealthCheckResponse,
};
use tracing::error;

/// Selector is the interface for selecting item from a list of items by a specific criteria.
#[tonic::async_trait]
pub trait Selector: Send + Sync {
    async fn select(&self, task_id: String, replicas: u32) -> Result<Vec<Host>>;
}

/// DEFAULT_SEED_PEERS_HEALTHCHECK_INTERVAL is the default interval(5m) of healthcheck for seed peers.
const DEFAULT_SEED_PEERS_HEALTHCHECK_INTERVAL: Duration = Duration::from_secs(300);

/// SEED_PEERS_HEALTHCHECK_INTERVAL_RANGE is the range of healthcheck interval(1m~10m) for seed peers.
const SEED_PEERS_HEALTHCHECK_INTERVAL_RANGE: RangeInclusive<Duration> =
    RangeInclusive::new(Duration::from_secs(60), Duration::from_secs(600));

/// SEED_PEERS_HEALTH_CHECK_TIMEOUT is the health check timeout for seed peers.
const SEED_PEERS_HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

struct SeedPeerData {
    /// hosts is the list of seed peers.
    hosts: HashMap<String, Host>,

    /// hashring is the hashring constructed from the hosts.
    hashring: HashRing<String>,
}

pub struct SeedPeerSelector {
    /// healthcheck_interval is the interval of healthcheck for seed peers.
    healthcheck_interval: Duration,

    /// scheduler_client is the client to communicate with the scheduler service.
    scheduler_client: SchedulerClient<Channel>,

    /// data is the data of the seed peer selector.
    data: RwLock<SeedPeerData>,
}

impl SeedPeerSelector {
    /// new creates a new seed peer selector.
    pub fn new(scheduler_channel: Channel, healthcheck_interval: Option<Duration>) -> Result<Self> {
        let healthcheck_interval =
            healthcheck_interval.unwrap_or(DEFAULT_SEED_PEERS_HEALTHCHECK_INTERVAL);

        // Validate seed_peers_healthcheck_interval.
        if !SEED_PEERS_HEALTHCHECK_INTERVAL_RANGE.contains(&healthcheck_interval) {
            return Err(Error::ValidationError(format!(
                "seed_peers_healthcheck_interval is not in range {:?}",
                SEED_PEERS_HEALTHCHECK_INTERVAL_RANGE
            )));
        }

        let scheduler_client = SchedulerClient::new(scheduler_channel);

        Ok(Self {
            healthcheck_interval,
            scheduler_client,
            data: RwLock::new(SeedPeerData {
                hosts: HashMap::new(),
                hashring: HashRing::new(),
            }),
        })
    }

    /// run starts the seed peer selector service.
    pub async fn run(&self) {
        // Perform an initial refresh immediately before starting the loop.
        self.refresh().await;

        let mut interval = tokio::time::interval(self.healthcheck_interval);
        loop {
            interval.tick().await;
            self.refresh().await;
        }
    }

    /// refresh updates the seed peer data.
    async fn refresh(&self) {
        let seed_peers = match self.list_seed_peers().await {
            Ok(peers) => peers,
            Err(err) => {
                error!("failed to list seed peers: {}", err);
                return;
            }
        };

        // Process health checks concurrently using streams.
        let healthy_peers: Vec<(String, Host)> = stream::iter(seed_peers)
            .filter_map(|peer| async move {
                let addr = format!("http://{}:{}", peer.ip, peer.port);
                match self.check_health(&addr).await {
                    Ok(_) => Some((addr, peer)),
                    Err(err) => {
                        error!("health check failed for peer {}: {}", addr, err);
                        None
                    }
                }
            })
            .collect()
            .await;

        let mut hosts = HashMap::with_capacity(healthy_peers.len());
        let mut hashring = HashRing::new();

        for (addr, peer) in healthy_peers {
            hosts.insert(addr.clone(), peer);
            hashring.add(addr);
        }

        // The write lock is held for a very short time.
        let mut data = self.data.write().await;
        data.hosts = hosts;
        data.hashring = hashring;
    }

    /// list_seed_peers lists the seed peers from scheduler.
    async fn list_seed_peers(&self) -> Result<Vec<Host>> {
        let request = tonic::Request::new(());
        let response = self.scheduler_client.clone().list_hosts(request).await?;
        let hosts = response.into_inner().hosts;

        // Filter for seed peer types.
        let seed_peers = hosts.into_iter().filter(|host| host.r#type != 0).collect();

        Ok(seed_peers)
    }

    /// check_health checks the health of each seed peer.
    async fn check_health(&self, addr: &str) -> Result<HealthCheckResponse> {
        let endpoint =
            Endpoint::from_shared(addr.to_string())?.timeout(SEED_PEERS_HEALTH_CHECK_TIMEOUT);

        let channel = endpoint.connect().await?;
        let mut client = HealthGRPCClient::new(channel);

        let request = tonic::Request::new(HealthCheckRequest {
            service: "".to_string(),
        });

        let response = client.check(request).await?;
        Ok(response.into_inner())
    }
}

#[tonic::async_trait]
impl Selector for SeedPeerSelector {
    async fn select(&self, task_id: String, replicas: u32) -> Result<Vec<Host>> {
        // Acquire a read lock and perform all logic within it.
        let data = self.data.read().await;

        if data.hosts.is_empty() {
            return Err(Error::Unknown(
                "cannot find any healthy seed peer in selector".to_string(),
            ));
        }

        // The number of replicas cannot exceed the total number of seed peers.
        let desired_replicas = std::cmp::min(replicas as usize, data.hashring.len());

        // Get replica nodes from the hash ring.
        let addrs = data
            .hashring
            .get_with_replicas(&task_id, desired_replicas)
            .unwrap_or_default();

        let seed_peers = addrs
            .into_iter()
            .filter_map(|addr| data.hosts.get(&addr).cloned())
            .collect();

        Ok(seed_peers)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_api::common::v2::Host;
    use std::collections::HashMap;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    fn create_test_host(id: &str, ip: &str, port: i32, host_type: u32) -> Host {
        Host {
            id: id.to_string(),
            ip: ip.to_string(),
            port,
            r#type: host_type,
            hostname: format!("host-{}", id),
            ..Default::default()
        }
    }

    async fn create_test_selector() -> SeedPeerSelector {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                drop(stream);
            }
        });

        let channel = Channel::from_shared(format!("http://{}", addr))
            .unwrap()
            .connect()
            .await
            .unwrap();

        SeedPeerSelector {
            healthcheck_interval: DEFAULT_SEED_PEERS_HEALTHCHECK_INTERVAL,
            scheduler_client: SchedulerClient::new(channel),
            data: RwLock::new(SeedPeerData {
                hosts: HashMap::new(),
                hashring: HashRing::new(),
            }),
        }
    }

    async fn add_test_host(selector: &SeedPeerSelector, addr: String, host: Host) {
        let mut data = selector.data.write().await;
        data.hosts.insert(addr.clone(), host);
        data.hashring.add(addr);
    }

    #[test]
    fn test_constants() {
        assert_eq!(
            DEFAULT_SEED_PEERS_HEALTHCHECK_INTERVAL,
            Duration::from_secs(300)
        );
        assert_eq!(SEED_PEERS_HEALTH_CHECK_TIMEOUT, Duration::from_secs(5));
        assert_eq!(
            SEED_PEERS_HEALTHCHECK_INTERVAL_RANGE.start(),
            &Duration::from_secs(60)
        );
        assert_eq!(
            SEED_PEERS_HEALTHCHECK_INTERVAL_RANGE.end(),
            &Duration::from_secs(600)
        );
    }

    #[test]
    fn test_healthcheck_interval_validation() {
        // Test valid intervals.
        assert!(SEED_PEERS_HEALTHCHECK_INTERVAL_RANGE.contains(&Duration::from_secs(60)));
        assert!(SEED_PEERS_HEALTHCHECK_INTERVAL_RANGE.contains(&Duration::from_secs(300)));
        assert!(SEED_PEERS_HEALTHCHECK_INTERVAL_RANGE.contains(&Duration::from_secs(600)));

        // Test invalid intervals.
        assert!(!SEED_PEERS_HEALTHCHECK_INTERVAL_RANGE.contains(&Duration::from_secs(30)));
        assert!(!SEED_PEERS_HEALTHCHECK_INTERVAL_RANGE.contains(&Duration::from_secs(700)));
    }

    #[tokio::test]
    async fn test_select_with_no_hosts() {
        let selector = create_test_selector().await;

        let result = selector.select("test-task".to_string(), 2).await;
        assert!(result.is_err());

        match result {
            Err(Error::Unknown(msg)) => {
                assert_eq!(msg, "cannot find any healthy seed peer in selector");
            }
            _ => panic!("Expected Unknown error"),
        }
    }

    #[tokio::test]
    async fn test_select_with_single_host() {
        let selector = create_test_selector().await;

        let host = create_test_host("1", "192.168.1.1", 8080, 1);
        let addr = "http://192.168.1.1:8080".to_string();
        add_test_host(&selector, addr, host).await;

        let result = selector.select("test-task".to_string(), 1).await;
        assert!(result.is_ok());

        let hosts = result.unwrap();
        assert_eq!(hosts.len(), 2);
        assert_eq!(hosts[0].id, "1");
        assert_eq!(hosts[0].ip, "192.168.1.1");
        assert_eq!(hosts[1].id, "1");
        assert_eq!(hosts[1].ip, "192.168.1.1");
    }

    #[tokio::test]
    async fn test_select_with_multiple_hosts() {
        let selector = create_test_selector().await;

        for i in 1..=5 {
            let host = create_test_host(&i.to_string(), &format!("192.168.1.{}", i), 8080, 1);
            let addr = format!("http://192.168.1.{}:8080", i);
            add_test_host(&selector, addr, host).await;
        }

        let result = selector.select("test-task".to_string(), 3).await;
        assert!(result.is_ok());

        let hosts = result.unwrap();
        assert!(hosts.len() <= 4);
        assert!(!hosts.is_empty());
    }

    #[tokio::test]
    async fn test_select_replicas_exceeds_available() {
        let selector = create_test_selector().await;

        for i in 1..=2 {
            let host = create_test_host(&i.to_string(), &format!("192.168.1.{}", i), 8080, 1);
            let addr = format!("http://192.168.1.{}:8080", i);
            add_test_host(&selector, addr, host).await;
        }

        let result = selector.select("test-task".to_string(), 5).await;
        assert!(result.is_ok());

        let hosts = result.unwrap();
        assert_eq!(hosts.len(), 3);
    }

    #[tokio::test]
    async fn test_select_consistency() {
        let selector = create_test_selector().await;

        for i in 1..=5 {
            let host = create_test_host(&i.to_string(), &format!("192.168.1.{}", i), 8080, 1);
            let addr = format!("http://192.168.1.{}:8080", i);
            add_test_host(&selector, addr, host).await;
        }

        let task_id = "consistent-task".to_string();

        let result1 = selector.select(task_id.clone(), 3).await.unwrap();
        let result2 = selector.select(task_id.clone(), 3).await.unwrap();
        let result3 = selector.select(task_id, 3).await.unwrap();

        assert_eq!(result1.len(), result2.len());
        assert_eq!(result2.len(), result3.len());

        let ids1: Vec<_> = result1.iter().map(|h| &h.id).collect();
        let ids2: Vec<_> = result2.iter().map(|h| &h.id).collect();
        let ids3: Vec<_> = result3.iter().map(|h| &h.id).collect();

        assert_eq!(ids1, ids2);
        assert_eq!(ids2, ids3);
    }

    #[tokio::test]
    async fn test_concurrent_select() {
        let selector = Arc::new(create_test_selector().await);

        for i in 1..=5 {
            let host = create_test_host(&i.to_string(), &format!("192.168.1.{}", i), 8080, 1);
            let addr = format!("http://192.168.1.{}:8080", i);
            add_test_host(&selector, addr, host).await;
        }

        let mut handles = vec![];

        for i in 0..10 {
            let selector = Arc::clone(&selector);
            let handle = tokio::spawn(async move {
                let task_id = format!("concurrent-task-{}", i);
                selector.select(task_id, 2).await
            });
            handles.push(handle);
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert!(result.is_ok());
            let hosts = result.unwrap();
            assert_eq!(hosts.len(), 3);
        }
    }

    #[tokio::test]
    async fn test_no_duplicate_hosts_in_selection() {
        let selector = create_test_selector().await;

        for i in 1..=20 {
            let host = create_test_host(&i.to_string(), &format!("192.168.1.{}", i), 8080, 1);
            let addr = format!("http://192.168.1.{}:8080", i);
            add_test_host(&selector, addr, host).await;
        }

        let result = selector.select("unique-task".to_string(), 10).await;
        assert!(result.is_ok());

        let hosts = result.unwrap();
        assert_eq!(hosts.len(), 11);

        let mut seen_ids = std::collections::HashSet::new();
        for host in hosts {
            assert!(
                seen_ids.insert(host.id.clone()),
                "Found duplicate host ID: {}",
                host.id
            );
        }
    }
}
