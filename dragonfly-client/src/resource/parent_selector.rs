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
use crate::grpc::dfdaemon_upload::DfdaemonUploadClient;
use crate::resource::piece_collector::CollectedParent;
use crate::shutdown::Shutdown;
use dashmap::DashMap;
use dragonfly_api::common::v2::{Host, Network};
use dragonfly_api::dfdaemon::v2::SyncHostRequest;
use dragonfly_client_config::dfdaemon;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::Error;
use dragonfly_client_core::Result;
use dragonfly_client_util::id_generator::IDGenerator;
use lru::LruCache;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::net::IpAddr;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, instrument, Instrument};
use validator::HasLen;

#[allow(dead_code)]
const DEFAULT_SYNC_HOST_TIMEOUT: u32 = 5;

/// ThreadManager is the manager to control sync_host thread.
#[derive(Clone)]
struct ThreadManager {
    /// parent_id is the id of the parent this thread sync with.
    parent_id: String,

    /// shutdown used to stop the thread.
    shutdown: Shutdown,
}

/// ParentSelector represents a parent selector.
#[allow(dead_code)]
pub struct ParentSelector {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// sync_interval represents the time interval between two refreshing probability operations.
    sync_interval: Duration,

    /// cache is the lru cache to store sync host thread.
    cache: Arc<Mutex<LruCache<String, ThreadManager>>>,

    /// id_generator is a IDGenerator.
    id_generator: Arc<IDGenerator>,

    /// hosts_info is the latest host info of different parents.
    hosts_info: Arc<DashMap<String, Host>>,
}

/// TaskParentSelector implements the task parent selector.
#[allow(dead_code)]
impl ParentSelector {
    /// new returns a ParentSelector.
    #[instrument(skip_all)]
    pub fn new(config: Arc<Config>, id_generator: Arc<IDGenerator>) -> ParentSelector {
        let config = config.clone();
        let sync_interval = config.download.parent_selector.sync_interval;
        let parent_cache = LruCache::new(
            NonZeroUsize::try_from(config.download.parent_selector.capacity).unwrap(),
        );
        let id_generator = id_generator.clone();
        let hosts_info = Arc::new(DashMap::new());

        ParentSelector {
            config,
            sync_interval,
            cache: Arc::new(Mutex::new(parent_cache)),
            id_generator,
            hosts_info,
        }
    }

    /// register_parents registers task and it's parents.
    #[instrument(skip_all)]
    pub fn register_parents(&self, add_parents: &Vec<CollectedParent>) -> Result<()> {
        // If not enable.
        if !self.config.download.parent_selector.enable {
            return Ok(());
        }

        // No parents, skip.
        if add_parents.length() == 0 {
            info!("register failed, parents length = 0");
            return Err(Error::Unknown(
                "register failed, parents length = 0".to_string(),
            ));
        }

        // Get LRU cache.
        let cache = self.cache.clone();
        let cache = cache.lock();
        let config = self.config.clone();
        let hosts_info = self.hosts_info.clone();

        if let Ok(mut cache) = cache {
            for parent in add_parents {
                // already contains parent.id, move to head and skip.
                if cache.get(&parent.id).is_some() {
                    continue;
                }

                // Create shutdown to control thread.
                let shutdown = Shutdown::new();
                let thread_manager = ThreadManager {
                    parent_id: parent.id.clone(),
                    shutdown,
                };

                // Push new parent to the LRU cache.
                if let Some(manager) = cache.put(parent.id.clone(), thread_manager.clone()) {
                    // Shutdown popped sync_host thread.
                    manager.shutdown.trigger();
                    let parent_id = manager.parent_id.clone();
                    hosts_info.remove(&parent_id);
                }

                // Start new sync_host thread.
                let config = config.clone();
                let host_id = self.id_generator.host_id();
                let peer_id = self.id_generator.peer_id();
                let parent = parent.clone();
                let shutdown = thread_manager.shutdown.clone();
                let hosts_info = hosts_info.clone();
                let sync_host_timeout = self.sync_interval * DEFAULT_SYNC_HOST_TIMEOUT;
                tokio::spawn(
                    async move {
                        let _ = Self::sync_host(
                            config,
                            host_id,
                            peer_id,
                            parent,
                            hosts_info,
                            shutdown,
                            sync_host_timeout,
                        )
                        .await;
                    }
                    .in_current_span(),
                );
            }
        }
        Ok(())
    }

    /// sync_host is a sub thread to sync host info from the parent.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all)]
    async fn sync_host(
        config: Arc<Config>,
        host_id: String,
        peer_id: String,
        parent: CollectedParent,
        hosts_info: Arc<DashMap<String, Host>>,
        shutdown: Shutdown,
        sync_host_timeout: Duration,
    ) -> Result<()> {
        info!("sync host info from parent {}", parent.id);

        // If parent.host is None, skip it.
        let host = parent.host.clone().ok_or_else(|| {
            error!("peer {:?} host is empty", parent);
            Error::InvalidPeer(parent.id.clone())
        })?;

        // Create a dfdaemon upload client.
        let dfdaemon_upload_client =
            DfdaemonUploadClient::new(config, format!("http://{}:{}", host.ip, host.port))
                .await
                .inspect_err(|err| {
                    error!(
                        "create dfdaemon upload client from parent {} failed: {}",
                        parent.id, err
                    );
                })
                .unwrap();

        let response = dfdaemon_upload_client
            .sync_host(SyncHostRequest { host_id, peer_id })
            .await
            .inspect_err(|err| {
                error!("sync host info from parent {} failed: {}", parent.id, err);
            })
            .unwrap();

        // If the response repeating timeout exceeds the piece download timeout,
        // the stream will return error.
        let out_stream = response.into_inner().timeout(sync_host_timeout);
        tokio::pin!(out_stream);

        let hosts_info = hosts_info.clone();
        while let Some(message) = out_stream.try_next().await.or_err(ErrorType::StreamError)? {
            // Check shutdown.
            if shutdown.is_shutdown() {
                hosts_info.remove(&parent.id.clone());
                break;
            }
            // Deal with massage.
            match message {
                Ok(message) => {
                    // Update the parent's host info if exists.
                    debug!(
                        "sync host info from parent {} message {:?}",
                        parent.id, message
                    );
                    hosts_info.insert(parent.id.clone(), message);
                }
                Err(err) => {
                    // Err, return
                    error!("sync host info from parent {} error {}", parent.id, err);
                    break;
                }
            }
        }
        Ok(())
    }

    /// select_parent select an optimal parent for the task.
    #[instrument(skip_all)]
    pub fn select_parent(&self, parents: &[CollectedParent]) -> Result<CollectedParent> {
        // No parents, error.
        if parents.is_empty() {
            error!("parents' length is 0");
            return Err(Error::Unknown("parents' length is 0".to_string()));
        }

        let mut probability = Vec::with_capacity(parents.len());
        let hosts_info = self.hosts_info.clone();
        let cache = self.cache.clone();

        let mut sum: f64 = 0f64;

        // Update parent host available capacity.
        let mut cache = cache.lock().unwrap();
        parents
            .iter()
            .for_each(|parent| match hosts_info.get(&parent.id) {
                None => {
                    cache.promote(&parent.id);
                    probability.push(0f64);
                }
                Some(host) => {
                    cache.promote(&parent.id);
                    match Self::available_capacity(host.value()) {
                        Ok(capacity) => {
                            probability.push(capacity);
                            sum += capacity;
                        }
                        Err(_) => {
                            probability.push(0f64);
                        }
                    }
                }
            });
        drop(cache);
        // Update probability.
        probability.iter_mut().for_each(|p| {
            if sum > 0f64 {
                *p /= sum;
            } else {
                *p = 1f64 / parents.len() as f64;
            }
        });

        // Get random value.
        let mut rng = StdRng::seed_from_u64(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        );

        let random_num: f64 = rng.gen();
        // Return the first parent_id that the sum is bigger than random value.
        let mut sum: f64 = 0f64;
        for (idx, v) in probability.iter().enumerate() {
            sum += v;
            if sum >= random_num {
                return Ok(parents[idx].clone());
            }
        }
        Ok(parents[parents.len() - 1].clone())
    }

    /// available_capacity return the available capacity of the host.
    fn available_capacity(host: &Host) -> Result<f64> {
        match host.network.clone() {
            None => Ok(0f64),
            Some(network) => Ok(network.upload_rate as f64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_register_parents() {
        let mut config = Config::default();
        config.download.parent_selector = dfdaemon::ParentSelector {
            enable: false,
            sync_interval: Duration::from_millis(500),
            capacity: 20,
        };
        config.host.ip = Some(IpAddr::V4("127.0.0.1".parse().unwrap()));
        config.host.hostname = "localhost".to_string();

        let id_generator = IDGenerator::new(
            config.host.ip.unwrap().to_string(),
            config.host.hostname.clone(),
            config.seed_peer.enable,
        );
        let id_generator = Arc::new(id_generator);

        // Test normal case.
        config.download.parent_selector.enable = true;
        let test_config = Arc::new(config.clone());
        let parent_selector = ParentSelector::new(test_config.clone(), id_generator.clone());
        let parents = vec![
            CollectedParent {
                id: "1".to_string(),
                host: None,
            },
            CollectedParent {
                id: "2".to_string(),
                host: None,
            },
            CollectedParent {
                id: "3".to_string(),
                host: None,
            },
        ];
        let result = parent_selector.register_parents(&parents);

        assert!(result.is_ok());
        assert_eq!(parent_selector.cache.clone().lock().unwrap().len(), 3);

        // Test disable case.
        config.download.parent_selector.enable = false;
        let test_config = Arc::new(config.clone());
        let parent_selector = ParentSelector::new(test_config.clone(), id_generator.clone());
        let parents = vec![CollectedParent {
            id: "".to_string(),
            host: None,
        }];
        let result = parent_selector.register_parents(&parents);

        assert!(result.is_ok());
        assert_eq!(parent_selector.cache.clone().lock().unwrap().len(), 0);

        // Test empty parents case.
        config.download.parent_selector.enable = true;
        let test_config = Arc::new(config.clone());
        let parent_selector = ParentSelector::new(test_config.clone(), id_generator.clone());
        let parents = vec![];
        let result = parent_selector.register_parents(&parents);

        assert!(result.is_err());
        assert_eq!(parent_selector.cache.clone().lock().unwrap().len(), 0);

        // Test cache overflow case.
        config.download.parent_selector.enable = true;
        config.download.parent_selector.capacity = 3;
        let test_config = Arc::new(config.clone());
        let parent_selector = ParentSelector::new(test_config.clone(), id_generator.clone());
        let parents = vec![
            CollectedParent {
                id: "1".to_string(),
                host: None,
            },
            CollectedParent {
                id: "2".to_string(),
                host: None,
            },
        ];
        let result = parent_selector.register_parents(&parents);

        assert!(result.is_ok());
        assert_eq!(parent_selector.cache.clone().lock().unwrap().len(), 2);

        let parents = vec![
            CollectedParent {
                id: "3".to_string(),
                host: None,
            },
            CollectedParent {
                id: "4".to_string(),
                host: None,
            },
        ];
        let result = parent_selector.register_parents(&parents);

        assert!(result.is_ok());
        assert_eq!(parent_selector.cache.clone().lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn test_optimal_parent() {
        let mut config = Config::default();
        config.download.parent_selector = dfdaemon::ParentSelector {
            enable: false,
            sync_interval: Duration::from_millis(500),
            capacity: 20,
        };
        config.host.ip = Some(IpAddr::V4("127.0.0.1".parse().unwrap()));
        config.host.hostname = "localhost".to_string();

        let id_generator = IDGenerator::new(
            config.host.ip.unwrap().to_string(),
            config.host.hostname.clone(),
            config.seed_peer.enable,
        );
        let id_generator = Arc::new(id_generator);

        // Test normal case.
        config.download.parent_selector.enable = true;
        let test_config = Arc::new(config.clone());
        let mut parent_selector = ParentSelector::new(test_config.clone(), id_generator.clone());
        let hosts_info = DashMap::new();
        hosts_info.insert(
            "1".to_string(),
            Host {
                network: Some(Network {
                    upload_rate: 100,
                    ..Network::default()
                }),
                ..Host::default()
            },
        );
        hosts_info.insert(
            "2".to_string(),
            Host {
                network: Some(Network {
                    upload_rate: 0,
                    ..Network::default()
                }),
                ..Host::default()
            },
        );
        hosts_info.insert(
            "3".to_string(),
            Host {
                network: Some(Network {
                    upload_rate: 0,
                    ..Network::default()
                }),
                ..Host::default()
            },
        );
        parent_selector.hosts_info = Arc::new(hosts_info);
        let parents = vec![
            CollectedParent {
                id: "1".to_string(),
                host: None,
            },
            CollectedParent {
                id: "2".to_string(),
                host: None,
            },
            CollectedParent {
                id: "3".to_string(),
                host: None,
            },
        ];
        let result = parent_selector.select_parent(&parents);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().id, "1");

        // Test parents length is 0 case.
        config.download.parent_selector.enable = true;
        let test_config = Arc::new(config.clone());
        let parent_selector = ParentSelector::new(test_config.clone(), id_generator.clone());
        let parents = vec![];
        let result = parent_selector.select_parent(&parents);

        assert!(result.is_err());

        // Test parent is not in cache case.
        config.download.parent_selector.enable = true;
        let test_config = Arc::new(config.clone());
        let mut parent_selector = ParentSelector::new(test_config.clone(), id_generator.clone());
        let hosts_info = DashMap::new();
        hosts_info.insert(
            "1".to_string(),
            Host {
                network: Some(Network {
                    upload_rate: 100,
                    ..Network::default()
                }),
                ..Host::default()
            },
        );
        parent_selector.hosts_info = Arc::new(hosts_info);
        let parents = vec![
            CollectedParent {
                id: "1".to_string(),
                host: None,
            },
            CollectedParent {
                id: "2".to_string(),
                host: None,
            },
            CollectedParent {
                id: "3".to_string(),
                host: None,
            },
        ];
        let result = parent_selector.select_parent(&parents);

        assert!(result.is_ok());
        assert_eq!(result.unwrap().id, "1");
    }

    #[tokio::test]
    async fn test_available_capacity() {
        // Test network is None case.
        let host = Host::default();
        let capacity = ParentSelector::available_capacity(&host);

        assert!(capacity.is_ok());
        assert_eq!(capacity.unwrap(), 0f64);

        // Test normal case.
        let host = Host {
            network: Some(Network {
                upload_rate: 5000,
                ..Default::default()
            }),
            ..Default::default()
        };
        let capacity = ParentSelector::available_capacity(&host.clone());

        assert!(capacity.is_ok());
        assert_eq!(capacity.unwrap(), 5000f64);
    }
}
