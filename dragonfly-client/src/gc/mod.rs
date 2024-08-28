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
use crate::shutdown;
use dragonfly_api::scheduler::v2::{DeleteCacheTaskRequest, DeleteTaskRequest};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Result;
use dragonfly_client_storage::{metadata, Storage};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, instrument};

// GC is the garbage collector of dfdaemon.
pub struct GC {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

    // host_id is the id of the host.
    host_id: String,

    // storage is the local storage.
    storage: Arc<Storage>,

    // scheduler_client is the grpc client of the scheduler.
    scheduler_client: Arc<SchedulerClient>,

    // shutdown is used to shutdown the garbage collector.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the garbage collector is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

impl GC {
    // new creates a new GC.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        host_id: String,
        storage: Arc<Storage>,
        scheduler_client: Arc<SchedulerClient>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        GC {
            config,
            host_id,
            storage,
            scheduler_client,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    // run runs the garbage collector.
    #[instrument(skip_all)]
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Start the collect loop.
        let mut interval = tokio::time::interval(self.config.gc.interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    // Evict the cache task by ttl.
                    if let Err(err) = self.evict_cache_task_by_ttl().await {
                        info!("failed to evict cache task by ttl: {}", err);
                    }

                    // Evict the cache by disk usage.
                    if let Err(err) = self.evict_cache_task_by_disk_usage().await {
                        info!("failed to evict cache task by disk usage: {}", err);
                    }

                    // Evict the task by ttl.
                    if let Err(err) = self.evict_task_by_ttl().await {
                        info!("failed to evict task by ttl: {}", err);
                    }

                    // Evict the cache by disk usage.
                    if let Err(err) = self.evict_task_by_disk_usage().await {
                        info!("failed to evict task by disk usage: {}", err);
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

    // evict_task_by_ttl evicts the task by ttl.
    #[instrument(skip_all)]
    async fn evict_task_by_ttl(&self) -> Result<()> {
        info!("start to evict by task ttl");
        for task in self.storage.get_tasks()? {
            // If the task is expired and not uploading, evict the task.
            if task.is_expired(self.config.gc.policy.task_ttl) {
                self.storage.delete_task(&task.id).await;
                info!("evict task {}", task.id);

                self.delete_task_from_scheduler(task.clone()).await;
                info!("delete task {} from scheduler", task.id);
            }
        }

        Ok(())
    }

    // evict_task_by_disk_usage evicts the task by disk usage.
    #[instrument(skip_all)]
    async fn evict_task_by_disk_usage(&self) -> Result<()> {
        let stats = fs2::statvfs(self.config.storage.dir.as_path())?;
        let available_space = stats.available_space();
        let total_space = stats.total_space();

        // Calculate the usage percent.
        let usage_percent = (100 - available_space * 100 / total_space) as u8;
        if usage_percent >= self.config.gc.policy.dist_high_threshold_percent {
            info!(
                "start to evict task by disk usage, disk usage {}% is higher than high threshold {}%",
                usage_percent, self.config.gc.policy.dist_high_threshold_percent
            );

            // Calculate the need evict space.
            let need_evict_space = total_space as f64
                * ((usage_percent - self.config.gc.policy.dist_low_threshold_percent) as f64
                    / 100.0);

            // Evict the task by the need evict space.
            if let Err(err) = self.evict_task_space(need_evict_space as u64).await {
                info!("failed to evict task by disk usage: {}", err);
            }
        }

        Ok(())
    }

    // evict_task_space evicts the task by the given space.
    #[instrument(skip_all)]
    async fn evict_task_space(&self, need_evict_space: u64) -> Result<()> {
        let mut tasks = self.storage.get_tasks()?;
        tasks.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));

        let mut evicted_space = 0;
        for task in tasks {
            // Evict enough space.
            if evicted_space >= need_evict_space {
                break;
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
            self.storage.delete_task(&task.id).await;

            // Update the evicted space.
            evicted_space += task_space;
            info!("evict task {} size {}", task.id, task_space);

            self.delete_task_from_scheduler(task.clone()).await;
            info!("delete task {} from scheduler", task.id);
        }

        info!("evict total size {}", evicted_space);
        Ok(())
    }

    // delete_task_from_scheduler deletes the task from the scheduler.
    #[instrument(skip_all)]
    async fn delete_task_from_scheduler(&self, task: metadata::Task) {
        self.scheduler_client
            .delete_task(DeleteTaskRequest {
                host_id: self.host_id.clone(),
                task_id: task.id.clone(),
            })
            .await
            .unwrap_or_else(|err| {
                error!("failed to delete peer {}: {}", task.id, err);
            });
    }

    // evict_cache_task_by_ttl evicts the cache task by ttl.
    #[instrument(skip_all)]
    async fn evict_cache_task_by_ttl(&self) -> Result<()> {
        info!("start to evict by cache task ttl * 2");
        for task in self.storage.get_cache_tasks()? {
            // If the cache task is expired and not uploading, evict the cache task.
            if task.is_expired() {
                self.storage.delete_cache_task(&task.id).await;
                info!("evict cache task {}", task.id);

                self.delete_cache_task_from_scheduler(task.clone()).await;
                info!("delete cache task {} from scheduler", task.id);
            }
        }

        Ok(())
    }

    // evict_cache_task_by_disk_usage evicts the cache task by disk usage.
    #[instrument(skip_all)]
    async fn evict_cache_task_by_disk_usage(&self) -> Result<()> {
        let stats = fs2::statvfs(self.config.storage.dir.as_path())?;
        let available_space = stats.available_space();
        let total_space = stats.total_space();

        // Calculate the usage percent.
        let usage_percent = (100 - available_space * 100 / total_space) as u8;
        if usage_percent >= self.config.gc.policy.dist_high_threshold_percent {
            info!(
                "start to evict cache task by disk usage, disk usage {}% is higher than high threshold {}%",
                usage_percent, self.config.gc.policy.dist_high_threshold_percent
            );

            // Calculate the need evict space.
            let need_evict_space = total_space as f64
                * ((usage_percent - self.config.gc.policy.dist_low_threshold_percent) as f64
                    / 100.0);

            // Evict the cache task by the need evict space.
            if let Err(err) = self.evict_cache_task_space(need_evict_space as u64).await {
                info!("failed to evict task by disk usage: {}", err);
            }
        }

        Ok(())
    }

    // evict_cache_task_space evicts the cache task by the given space.
    #[instrument(skip_all)]
    async fn evict_cache_task_space(&self, need_evict_space: u64) -> Result<()> {
        let mut tasks = self.storage.get_cache_tasks()?;
        tasks.sort_by(|a, b| a.updated_at.cmp(&b.updated_at));

        let mut evicted_space = 0;
        for task in tasks {
            // Evict enough space.
            if evicted_space >= need_evict_space {
                break;
            }

            // If the cache task is persistent, skip it.
            if task.is_persistent() {
                continue;
            }

            let task_space = task.content_length();

            // Evict the task.
            self.storage.delete_task(&task.id).await;

            // Update the evicted space.
            evicted_space += task_space;
            info!("evict cache task {} size {}", task.id, task_space);

            self.delete_cache_task_from_scheduler(task.clone()).await;
            info!("delete cache task {} from scheduler", task.id);
        }

        info!("evict total size {}", evicted_space);
        Ok(())
    }

    // delete_cache_task_from_scheduler deletes the cache task from the scheduler.
    #[instrument(skip_all)]
    async fn delete_cache_task_from_scheduler(&self, task: metadata::CacheTask) {
        self.scheduler_client
            .delete_cache_task(DeleteCacheTaskRequest {
                host_id: self.host_id.clone(),
                task_id: task.id.clone(),
            })
            .await
            .unwrap_or_else(|err| {
                error!("failed to delete cache peer {}: {}", task.id, err);
            });
    }
}
