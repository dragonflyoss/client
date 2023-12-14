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
use crate::Result;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::info;

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
                    // Evict the cache by disk usage.
                    self.evict_by_disk_usage();


                    // Evict the cache by task ttl.
                    if let Err(err) = self.evict_by_task_ttl() {
                        info!("failed to evict by task ttl: {}", err);
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
    fn evict_by_task_ttl(&self) -> Result<()> {
        info!("start to evict by task ttl");
        for task in self.storage.get_tasks()? {
            // If the task is expired and not uploading, evict the task.
            if task.is_expired(self.config.gc.policy.task_ttl) && !task.is_uploading() {
                self.storage.delete_task(&task.id).unwrap_or_else(|err| {
                    info!("failed to evict task {}: {}", task.id, err);
                });
                info!("evict task {}", task.id);
            }
        }

        Ok(())
    }

    // evict_by_disk_usage evicts the cache by disk usage.
    fn evict_by_disk_usage(&self) {}
}
