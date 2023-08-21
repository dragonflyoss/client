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

// Dynconfig supports dynamic configuration of the client.
pub struct Dynconfig {
    // manager_client is the grpc client of the manager.
    manager_client: ManagerClient,

    // shutdown is used to shutdown the announcer.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the announcer is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

impl Dynconfig {
    pub fn new(
        manager_client: ManagerClient,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Dynconfig {}
    }
}
