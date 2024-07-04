/*
 *     Copyright 2024 The Dragonfly Authors
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
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Result as ClientResult;
use dragonfly_client_storage::metadata;
use dragonfly_client_storage::Storage;
use dragonfly_client_util::id_generator::IDGenerator;
use std::path::Path;
use std::sync::Arc;

// CacheTask represents a cache task manager.
pub struct CacheTask {
    // config is the configuration of the dfdaemon.
    _config: Arc<Config>,

    // id_generator is the id generator.
    pub id_generator: Arc<IDGenerator>,

    // storage is the local storage.
    storage: Arc<Storage>,

    // scheduler_client is the grpc client of the scheduler.
    pub scheduler_client: Arc<SchedulerClient>,
}

// CacheTask is the implementation of CacheTask.
impl CacheTask {
    // new creates a new CacheTask.
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        scheduler_client: Arc<SchedulerClient>,
    ) -> Self {
        CacheTask {
            _config: config,
            id_generator,
            storage,
            scheduler_client,
        }
    }

    // TODO: Implement this.
    // create_persistent_cache_task creates a persistent cache task from local.
    pub async fn create_persistent_cache_task(
        &self,
        id: &str,
        path: &Path,
        piece_length: u64,
        digest: &str,
    ) -> ClientResult<metadata::CacheTask> {
        self.storage
            .create_persistent_cache_task(id, path, piece_length, digest)
            .await
    }
}
