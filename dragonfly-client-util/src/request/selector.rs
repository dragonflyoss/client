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

use crate::hashring::VNodeHashRing;
use crate::shutdown;
use dragonfly_api::common::v2::Host;
use dragonfly_api::scheduler::v2::{scheduler_client::SchedulerClient, ListHostsRequest};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};
use tokio::task::JoinSet;
use tonic::transport::{Channel, Endpoint};
use tonic_health::pb::{
    health_client::HealthClient as HealthGRPCClient, HealthCheckRequest, HealthCheckResponse,
};
use tracing::{debug, error, info, Instrument};

/// Selector is the interface for selecting item from a list of items by a specific criteria.
#[tonic::async_trait]
pub trait Selector: Send + Sync {
    /// select selects items based on the given task_id and number of replicas.
    async fn select(&self, task_id: String, replicas: u32) -> Result<Vec<Host>>;
}

/// SEED_PEERS_HEALTH_CHECK_TIMEOUT is the health check timeout for seed peers.
const SEED_PEERS_HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

/// DEFAULT_VNODES_PER_HOST is the default number of virtual nodes per host.
const DEFAULT_VNODES_PER_HOST: usize = 3;

/// SeedPeers holds the data of seed peers.
struct SeedPeers {
    /// hosts is the list of seed peers.
    hosts: HashMap<String, Host>,

    /// hashring is the hashring constructed from the hosts.
    hashring: VNodeHashRing,
}

/// SeedPeerSelector is the implementation of Selector for seed peers.
pub struct SeedPeerSelector {
    /// health_check_interval is the interval of health check for seed peers.
    health_check_interval: Duration,

    /// scheduler_client is the client to communicate with the scheduler service.
    scheduler_client: SchedulerClient<Channel>,

    /// seed_peers is the data of the seed peer selector.
    seed_peers: RwLock<SeedPeers>,

    /// mutex is used to protect hot refresh.
    mutex: Mutex<()>,
}

/// SeedPeerSelector implements a selector that selects seed peers from the scheduler service.
impl SeedPeerSelector {
    /// new creates a new seed peer selector.
    pub async fn new(
        scheduler_client: SchedulerClient<Channel>,
        health_check_interval: Duration,
    ) -> Result<Self> {
        let seed_peer_selector = Self {
            health_check_interval,
            scheduler_client,
            seed_peers: RwLock::new(SeedPeers {
                hosts: HashMap::new(),
                hashring: VNodeHashRing::new(DEFAULT_VNODES_PER_HOST),
            }),
            mutex: Mutex::new(()),
        };

        seed_peer_selector.refresh().await?;
        Ok(seed_peer_selector)
    }

    /// run starts the seed peer selector service.
    pub async fn run(&self) {
        let mut interval = tokio::time::interval(self.health_check_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    match self.refresh().await {
                        Err(err) => error!("failed to refresh seed peers: {}", err),
                        Ok(_) => debug!("succeed to refresh seed peers"),
                    }
                }
                _ = shutdown::shutdown_signal() => {
                    info!("seed peer selector service is shutting down");
                    return;
                }
            }
        }
    }

    /// refresh updates the seed peers data.
    async fn refresh(&self) -> Result<()> {
        // Only one refresh can be running at a time.
        let Ok(_guard) = self.mutex.try_lock() else {
            info!("refresh is already running");
            return Ok(());
        };

        let seed_peers = self.list_seed_peers().await?;
        let seed_peers_length = seed_peers.len();

        // Process health checks concurrently using streams.
        // The number of seed peers in a cluster is usually small,
        // so here we directly use the number of seed peers as the concurrency for simultaneous health checks.
        let mut join_set = JoinSet::new();
        for peer in seed_peers {
            let addr = format!("http://{}:{}", peer.ip, peer.port);
            join_set.spawn(
                async move {
                    match Self::check_health(&addr).await {
                        Ok(_) => Ok(peer),
                        Err(err) => Err(err),
                    }
                }
                .in_current_span(),
            );
        }

        let mut hosts = HashMap::with_capacity(seed_peers_length);
        let mut hashring = VNodeHashRing::new(DEFAULT_VNODES_PER_HOST);
        while let Some(result) = join_set.join_next().await {
            match result {
                Ok(Ok(peer)) => {
                    let addr: SocketAddr = format!("{}:{}", peer.ip, peer.port)
                        .parse()
                        .or_err(ErrorType::ParseError)?;

                    hashring.add(addr);
                    hosts.insert(addr.to_string(), peer);
                }
                Ok(Err(err)) => {
                    error!("health check error: {}", err);
                }
                Err(join_err) => {
                    error!("task join error: {}", join_err);
                }
            }
        }

        // The write lock is held for a very short time.
        let mut seed_peers = self.seed_peers.write().await;
        seed_peers.hosts = hosts;
        seed_peers.hashring = hashring;

        Ok(())
    }

    /// list_seed_peers lists the seed peers from scheduler.
    async fn list_seed_peers(&self) -> Result<Vec<Host>> {
        // Filter for seed peer types, seed peer type is 1.
        // refer to dragonfly-client-config/src/dfdaemon.rs#HostType.
        let request = tonic::Request::new(ListHostsRequest { r#type: Some(1) });
        let response = self.scheduler_client.clone().list_hosts(request).await?;
        let seed_peers = response.into_inner().hosts.into_iter().collect();

        Ok(seed_peers)
    }

    /// check_health checks the health of each seed peer.
    async fn check_health(addr: &str) -> Result<HealthCheckResponse> {
        let channel = Endpoint::from_shared(addr.to_string())?
            .connect_timeout(SEED_PEERS_HEALTH_CHECK_TIMEOUT)
            .connect()
            .await?;

        let mut client = HealthGRPCClient::new(channel);
        let mut request = tonic::Request::new(HealthCheckRequest {
            service: "".to_string(),
        });
        request.set_timeout(SEED_PEERS_HEALTH_CHECK_TIMEOUT);
        let response = client.check(request).await?;
        Ok(response.into_inner())
    }
}

#[tonic::async_trait]
impl Selector for SeedPeerSelector {
    async fn select(&self, task_id: String, replicas: u32) -> Result<Vec<Host>> {
        // Acquire a read lock and perform all logic within it.
        let seed_peers = self.seed_peers.read().await;
        if seed_peers.hosts.is_empty() {
            return Err(Error::HostNotFound("seed peers".to_string()));
        }

        // The number of replicas cannot exceed the total number of seed peers.
        let expected_replicas = std::cmp::min(replicas as usize, seed_peers.hashring.len());
        debug!("task {} expected replicas: {}", task_id, expected_replicas);

        // Get replica nodes from the hash ring.
        let vnodes = seed_peers
            .hashring
            .get_with_replicas(&task_id, expected_replicas)
            .unwrap_or_default();

        let seed_peers: Vec<Host> = vnodes
            .into_iter()
            .filter_map(|vnode| {
                seed_peers
                    .hosts
                    .get(vnode.addr().to_string().as_str())
                    .cloned()
            })
            .collect();

        if seed_peers.is_empty() {
            return Err(Error::HostNotFound("selected seed peers".to_string()));
        }

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
            health_check_interval: Duration::from_secs(10),
            scheduler_client: SchedulerClient::new(channel),
            seed_peers: RwLock::new(SeedPeers {
                hosts: HashMap::new(),
                hashring: VNodeHashRing::new(1),
            }),
            mutex: Mutex::new(()),
        }
    }

    async fn add_test_host(selector: &SeedPeerSelector, host: Host) {
        let mut seed_peers = selector.seed_peers.write().await;
        seed_peers
            .hosts
            .insert(format!("{}:{}", host.ip, host.port), host.clone());
        seed_peers
            .hashring
            .add(format!("{}:{}", host.ip, host.port).parse().unwrap());
    }

    #[tokio::test]
    async fn test_select_with_no_hosts() {
        let selector = create_test_selector().await;

        let result = selector.select("test-task".to_string(), 2).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::HostNotFound(_))));
    }

    #[tokio::test]
    async fn test_select_with_single_host() {
        let selector = create_test_selector().await;

        let host = create_test_host("1", "192.168.1.1", 8080, 1);
        add_test_host(&selector, host).await;

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
            add_test_host(&selector, host).await;
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
            add_test_host(&selector, host).await;
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
            add_test_host(&selector, host).await;
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
            add_test_host(&selector, host).await;
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
            add_test_host(&selector, host).await;
        }

        let result = selector.select("unique-task".to_string(), 10).await;
        assert!(result.is_ok());

        let hosts = result.unwrap();
        assert_eq!(hosts.len(), 11);

        let mut seen_ids = std::collections::HashSet::new();
        for host in hosts {
            assert!(
                seen_ids.insert(host.id.clone()),
                "found duplicate host ID: {}",
                host.id
            );
        }
    }
}
