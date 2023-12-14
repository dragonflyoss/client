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
                    // Run the garbage collector.
                    // TODO: implement the garbage collector.
                    info!("garbage collector running {:?} {:?}", self.config.gc, self.storage.get_task("1"));
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
    // fn evict_by_task_ttl(&self) {
    // }

    // evict_by_disk_usage evicts the cache by disk usage.
    // fn evict_by_disk_usage(&self) {
    // }
}
