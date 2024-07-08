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
use dragonfly_api::common::v2::CacheTask as CommonCacheTask;
use dragonfly_api::dfdaemon::v2::UploadCacheTaskRequest;
use dragonfly_api::scheduler::v2::{
    DeleteCacheTaskRequest, StatCacheTaskRequest, UploadCacheTaskFailedRequest,
    UploadCacheTaskFinishedRequest, UploadCacheTaskStartedRequest,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Result as ClientResult;
use dragonfly_client_storage::Storage;
use dragonfly_client_util::id_generator::IDGenerator;
use std::path::Path;
use std::sync::Arc;
use tracing::error;

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

    // create_persistent_cache_task creates a persistent cache task from local.
    pub async fn create_persistent_cache_task(
        &self,
        task_id: &str,
        host_id: &str,
        peer_id: &str,
        path: &Path,
        digest: &str,
        request: UploadCacheTaskRequest,
    ) -> ClientResult<CommonCacheTask> {
        // Notify the scheduler that the cache task is started.
        self.scheduler_client
            .upload_cache_task_started(UploadCacheTaskStartedRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
                peer_id: peer_id.to_string(),
                persistent_replica_count: request.persistent_replica_count,
                tag: request.tag.clone(),
                application: request.application.clone(),
                piece_length: request.piece_length,
                ttl: request.ttl.clone(),
                timeout: request.timeout,
            })
            .await
            .map_err(|err| {
                error!("upload cache task started: {}", err);
                err
            })?;

        // Create the persistent cache task.
        match self
            .storage
            .create_persistent_cache_task(task_id, path, request.piece_length, digest)
            .await
        {
            Ok(metadata) => {
                let response = match self
                    .scheduler_client
                    .upload_cache_task_finished(UploadCacheTaskFinishedRequest {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        peer_id: peer_id.to_string(),
                    })
                    .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        // Notify the scheduler that the cache task is failed.
                        self.scheduler_client
                            .upload_cache_task_failed(UploadCacheTaskFailedRequest {
                                host_id: host_id.to_string(),
                                task_id: task_id.to_string(),
                                peer_id: peer_id.to_string(),
                                description: Some(err.to_string()),
                            })
                            .await
                            .map_err(|err| {
                                error!("upload cache task failed: {}", err);
                                err
                            })?;

                        // Delete the cache task.
                        self.storage.delete_cache_task(task_id).await;
                        return Err(err);
                    }
                };

                Ok(CommonCacheTask {
                    id: task_id.to_string(),
                    persistent_replica_count: request.persistent_replica_count,
                    replica_count: response.replica_count,
                    digest: digest.to_string(),
                    tag: request.tag,
                    application: request.application,
                    piece_length: request.piece_length,
                    content_length: metadata.content_length,
                    piece_count: response.piece_count,
                    state: response.state,
                    ttl: request.ttl,
                    created_at: response.created_at,
                    updated_at: response.updated_at,
                })
            }
            Err(err) => {
                // Notify the scheduler that the cache task is failed.
                self.scheduler_client
                    .upload_cache_task_failed(UploadCacheTaskFailedRequest {
                        host_id: host_id.to_string(),
                        task_id: task_id.to_string(),
                        peer_id: peer_id.to_string(),
                        description: Some(err.to_string()),
                    })
                    .await
                    .map_err(|err| {
                        error!("upload cache task failed: {}", err);
                        err
                    })?;

                // Delete the cache task.
                self.storage.delete_cache_task(task_id).await;
                Err(err)
            }
        }
    }

    // stat stats the cache task from the scheduler.
    pub async fn stat(&self, task_id: &str, host_id: &str) -> ClientResult<CommonCacheTask> {
        self.scheduler_client
            .stat_cache_task(StatCacheTaskRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
            })
            .await
    }

    // delete_cache_task deletes a cache task.
    pub async fn delete(&self, task_id: &str, host_id: &str) -> ClientResult<()> {
        self.scheduler_client
            .delete_cache_task(DeleteCacheTaskRequest {
                host_id: host_id.to_string(),
                task_id: task_id.to_string(),
            })
            .await
            .map_err(|err| {
                error!("delete cache task: {}", err);
                err
            })?;

        self.storage.delete_cache_task(task_id).await;
        Ok(())
    }
}
