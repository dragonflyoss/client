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

use crate::backend::http::HTTP;
use crate::grpc::scheduler::SchedulerClient;
use crate::storage::{metadata, Storage};
use crate::Result;
use std::sync::Arc;

pub mod piece;

// Task represents a task manager.
pub struct Task {
    // manager_client is the grpc client of the manager.
    storage: Arc<Storage>,

    // scheduler_client is the grpc client of the scheduler.
    pub scheduler_client: Arc<SchedulerClient>,

    // piece is the piece manager.
    pub piece: piece::Piece,
}

// Task implements the task manager.
impl Task {
    // new returns a new Task.
    pub fn new(
        storage: Arc<Storage>,
        scheduler_client: Arc<SchedulerClient>,
        http_client: Arc<HTTP>,
    ) -> Self {
        Self {
            storage: storage.clone(),
            scheduler_client: scheduler_client.clone(),
            piece: piece::Piece::new(
                storage.clone(),
                scheduler_client.clone(),
                http_client.clone(),
            ),
        }
    }

    // get_task gets a task from the local storage.
    pub fn get_task(&self, task_id: &str) -> Result<metadata::Task> {
        self.storage.get_task(task_id)
    }
}
