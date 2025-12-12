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

use dragonfly_client_config::dfinit::{Config, ContainerRuntimeConfig};
use dragonfly_client_core::Result;
use tracing::info;

pub mod containerd;
pub mod crio;
pub mod docker;
pub mod podman;

/// Engine represents config of the container runtime engine.
#[derive(Debug, Clone)]
enum Engine {
    Containerd(containerd::Containerd),
    Docker(docker::Docker),
    Crio(crio::CRIO),
    Podman(podman::Podman),
}

/// ContainerRuntime represents the container runtime manager.
pub struct ContainerRuntime {
    engine: Option<Engine>,
}

/// ContainerRuntime implements the container runtime manager.
impl ContainerRuntime {
    /// new creates a new container runtime manager.

    pub fn new(config: &Config) -> Self {
        Self {
            engine: Self::get_engine(config),
        }
    }

    /// run runs the container runtime to initialize runtime environment for the dfdaemon.

    pub async fn run(&self) -> Result<()> {
        match &self.engine {
            None => Ok(()),
            Some(Engine::Containerd(containerd)) => containerd.run().await,
            Some(Engine::Docker(docker)) => docker.run().await,
            Some(Engine::Crio(crio)) => crio.run().await,
            Some(Engine::Podman(podman)) => podman.run().await,
        }
    }

    /// get_engine returns the runtime engine from the config.

    fn get_engine(config: &Config) -> Option<Engine> {
        if let Some(ref container_runtime_config) = config.container_runtime.config {
            let engine = match container_runtime_config {
                ContainerRuntimeConfig::Containerd(containerd) => Engine::Containerd(
                    containerd::Containerd::new(containerd.clone(), config.proxy.clone()),
                ),
                ContainerRuntimeConfig::Docker(docker) => {
                    Engine::Docker(docker::Docker::new(docker.clone(), config.proxy.clone()))
                }
                ContainerRuntimeConfig::CRIO(crio) => {
                    Engine::Crio(crio::CRIO::new(crio.clone(), config.proxy.clone()))
                }
                ContainerRuntimeConfig::Podman(podman) => {
                    Engine::Podman(podman::Podman::new(podman.clone(), config.proxy.clone()))
                }
            };

            info!("container runtime engine is {:?}", engine);
            return Some(engine);
        }

        info!("container runtime engine is not set");
        None
    }
}

#[cfg(test)]
mod test {
    use dragonfly_client_config::dfinit::Containerd;

    use super::*;

    #[tokio::test]
    async fn should_return_ok_if_container_runtime_not_set() {
        let runtime = ContainerRuntime::new(&Config {
            ..Default::default()
        });
        assert!(runtime.run().await.is_ok());
    }

    #[test]
    fn should_get_engine_from_config() {
        let runtime = ContainerRuntime::new(&Config {
            container_runtime: dragonfly_client_config::dfinit::ContainerRuntime {
                config: Some(ContainerRuntimeConfig::Containerd(Containerd {
                    ..Default::default()
                })),
            },
            ..Default::default()
        });
        assert!(runtime.engine.is_some());

        let runtime = ContainerRuntime::new(&Config {
            container_runtime: dragonfly_client_config::dfinit::ContainerRuntime {
                config: Some(ContainerRuntimeConfig::CRIO(Default::default())),
            },
            ..Default::default()
        });
        assert!(runtime.engine.is_some());
    }
}
