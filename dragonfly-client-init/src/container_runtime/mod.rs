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

pub mod containerd;
pub mod crio;
pub mod docker;

enum RuntimeEngine {
    Containerd(containerd::Containerd),
    Docker(docker::Docker),
}

// ContainerRuntime represents the container runtime manager.
pub struct ContainerRuntime {
    engine: Option<RuntimeEngine>,
}

// ContainerRuntime implements the container runtime manager.
impl ContainerRuntime {
    // new creates a new container runtime manager.
    pub fn new(config: &Config) -> Self {
        Self { engine: Self::get_runtime_engine(&config) }
    }

    // run runs the container runtime to initialize runtime environment for the dfdaemon.
    pub async fn run(&self) -> Result<()> {
        // If containerd is enabled, override the default containerd
        // configuration.
        match &self.engine {
            None => return Ok(()),
            Some(RuntimeEngine::Containerd(containerd)) => containerd.run().await,
            Some(RuntimeEngine::Docker(docker)) => docker.run().await,
        }
    }

    fn get_runtime_engine(config: &Config) -> Option<RuntimeEngine> {
        use dragonfly_client_config::dfinit::ContainerRuntimeConfig;
        let Some(ref cfg) = config.container_runtime.config else {
            return None;
        };
        let engine = match cfg {
            ContainerRuntimeConfig::Containerd(containerd) => RuntimeEngine::Containerd(
                containerd::Containerd::new(containerd.clone(), config.proxy.clone()),
            ),
            ContainerRuntimeConfig::Docker(docker) => {
                RuntimeEngine::Docker(docker::Docker::new(docker.clone(), config.proxy.clone()))
            }
        };
        Some(engine)
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
    fn should_get_runtime_engine_from_config() {
        let runtime = ContainerRuntime::new(&Config {
            container_runtime: dragonfly_client_config::dfinit::ContainerRuntime { 
                config: Some(dragonfly_client_config::dfinit::ContainerRuntimeConfig::Containerd(Containerd {
                    ..Default::default()
                }))
            },
            ..Default::default()
        });
        assert!(runtime.engine.is_some());
    }
}