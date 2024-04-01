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

use dragonfly_client_config::dfinit::Config;
use dragonfly_client_core::Result;
use std::sync::Arc;
use tracing::info;

pub mod containerd;
pub mod docker;

// ContainerRuntime represents the container runtime manager.
pub struct ContainerRuntime {
    // containerd is the containerd runtime manager.
    containerd: containerd::Containerd,

    // docker is the docker runtime manager.
    docker: docker::Docker,
}

// ContainerRuntime implements the container runtime manager.
impl ContainerRuntime {
    // new creates a new container runtime manager.
    pub fn new(config: Arc<Config>) -> Self {
        Self {
            containerd: containerd::Containerd::new(
                config.container_runtime.containerd.clone(),
                config.proxy.clone(),
            ),
            docker: docker::Docker::new(
                config.container_runtime.docker.clone(),
                config.proxy.clone(),
            ),
        }
    }

    // run runs the container runtime to initialize runtime environment for the dfdaemon.
    pub async fn run(&self) -> Result<()> {
        // If containerd is enabled, override the default containerd
        // configuration.
        if self.containerd.is_enabled() {
            info!("containerd feature is enabled");
            self.containerd.run().await?;
        }

        // If docker is enabled, override the default docker configuration.
        if self.docker.is_enabled() {
            info!("docker feature is enabled");
            self.docker.run().await?;
        }

        Ok(())
    }
}
