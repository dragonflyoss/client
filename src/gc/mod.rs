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

use crate::config::dfdaemon::Config;
use crate::shutdown;
use crate::storage::Storage;
use dragonfly_client_core::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info};

// GC is the garbage collector of dfdaemon.
pub struct GC {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

    // storage is the local storage.
    storage: Arc<Storage>,

    // shutdown is used to shutdown the garbage collector.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the garbage collector is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

impl GC {
    // new creates a new GC.
    pub fn new(
        config: Arc<Config>,
        storage: Arc<Storage>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        GC {
            config,
            storage,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    // run runs the garbage collector.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Start the collect loop.
        let mut interval = tokio::time::interval(self.config.gc.interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Evict the cache by task ttl.
                    if let Err(err) = self.evict_by_task_ttl().await {
                        info!("failed to evict by task ttl: {}", err);
                    }

                    // Evict the cache by disk usage.
                    if let Err(err) = self.evict_by_disk_usage().await {
                        info!("failed to evict by disk usage: {}", err);
                    }
                }
                _ = shutdown.recv() => {
                    // Shutdown the garbage collector.
                    info!("garbage collector shutting down");
                    return;
                }
            }
        }
    }

    // evict_by_task_ttl evicts the cache by task ttl.
    async fn evict_by_task_ttl(&self) -> Result<()> {
        info!("start to evict by task ttl");
        for task in self.storage.get_tasks()? {
            // If the task is expired and not uploading, evict the task.
            if task.is_expired(self.config.gc.policy.task_ttl) && !task.is_uploading() {
                self.storage
                    .delete_task(&task.id)
                    .await
                    .unwrap_or_else(|err| {
                        info!("failed to evict task {}: {}", task.id, err);
                    });
                info!("evict task {}", task.id);
            }
        }

        Ok(())
    }

    // evict_by_disk_usage evicts the cache by disk usage.
    async fn evict_by_disk_usage(&self) -> Result<()> {
        let stats = fs2::statvfs(self.config.storage.dir.as_path())?;
        let available_space = stats.available_space();
        let total_space = stats.total_space();

        // Calculate the usage percent.
        let usage_percent = (100 - available_space * 100 / total_space) as u8;
        if usage_percent >= self.config.gc.policy.dist_high_threshold_percent {
            info!(
                "start to evict by disk usage, disk usage {}% is higher than high threshold {}%",
                usage_percent, self.config.gc.policy.dist_high_threshold_percent
            );

            // Calculate the need evict space.
            let need_evict_space = total_space as f64
                * ((usage_percent - self.config.gc.policy.dist_low_threshold_percent) as f64
                    / 100.0);

            // Evict the cache by the need evict space.
            if let Err(err) = self.evict_space(need_evict_space as u64).await {
                info!("failed to evict by disk usage: {}", err);
            }
        }

        Ok(())
    }

    // evict_space evicts the cache by the given space.
    async fn evict_space(&self, need_evict_space: u64) -> Result<()> {
        let mut tasks = self.storage.get_tasks()?;
        tasks.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));

        let mut evicted_space = 0;
        for task in tasks {
            // Evict enough space.
            if evicted_space >= need_evict_space {
                break;
            }

            // If the task is started, skip it.
            if task.is_started() {
                continue;
            }

            // If the task is uploading, skip it.
            if task.is_uploading() {
                continue;
            }

            // If the task has no content length, skip it.
            let task_space = match task.content_length() {
                Some(content_length) => content_length,
                None => {
                    error!("task {} has no content length", task.id);
                    continue;
                }
            };

            // Evict the task.
            if let Err(err) = self.storage.delete_task(&task.id).await {
                info!("failed to evict task {}: {}", task.id, err);
                continue;
            }

            // Update the evicted space.
            evicted_space += task_space;
            info!("evict task {} size {}", task.id, task_space);
        }

        info!("evict total size {}", evicted_space);
        Ok(())
    }
}
