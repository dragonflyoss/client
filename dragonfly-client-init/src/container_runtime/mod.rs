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
use std::path::PathBuf;
use tokio::fs;
use tracing::{info, instrument};

pub mod containerd;
pub mod crio;
pub mod docker;
pub mod podman;

/// Represents config of the container runtime engine.
#[derive(Debug, Clone)]
enum Engine {
    Containerd(containerd::Containerd),
    Docker(docker::Docker),
    Crio(crio::CRIO),
    Podman(podman::Podman),
}

/// Represents the container runtime manager.
pub struct ContainerRuntime {
    /// The container runtime engine to initialize, if configured.
    engine: Option<Engine>,

    /// The path of the sentinel file created when the container runtime has to be
    /// restarted for the configuration to take effect, like /run/reboot-required
    /// on Debian. It is consumed by the restart-container-runtime init container.
    restart_sentinel: PathBuf,
}

/// Implements the container runtime manager.
impl ContainerRuntime {
    /// Creates a new container runtime manager.
    #[instrument(skip_all)]
    pub fn new(config: &Config, restart_sentinel: PathBuf) -> Self {
        Self {
            engine: Self::get_engine(config),
            restart_sentinel,
        }
    }

    /// Runs the container runtime to initialize runtime environment for the dfdaemon, and
    /// creates the restart sentinel only when a configuration file the runtime does not
    /// reload was rewritten, so an unchanged runtime is never restarted.
    #[instrument(skip_all)]
    pub async fn run(&self) -> Result<()> {
        let restart_required = match &self.engine {
            None => false,
            Some(Engine::Containerd(containerd)) => containerd.run().await?,
            Some(Engine::Docker(docker)) => docker.run().await?,
            Some(Engine::Crio(crio)) => crio.run().await?,
            Some(Engine::Podman(podman)) => podman.run().await?,
        };

        if restart_required {
            if let Some(parent) = self.restart_sentinel.parent() {
                fs::create_dir_all(parent).await?;
            }
            fs::write(&self.restart_sentinel, b"").await?;
            info!(
                "container runtime restart required, created sentinel {}",
                self.restart_sentinel.display()
            );
        }

        Ok(())
    }

    /// Returns the runtime engine from the config.
    #[instrument(skip_all)]
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
mod tests {
    #![allow(clippy::type_complexity)]

    use super::*;
    use dragonfly_client_config::dfinit;

    #[test]
    fn new_picks_engine_matching_config() {
        let test_cases: Vec<(Option<ContainerRuntimeConfig>, fn(Option<&Engine>))> = vec![
            (None, |engine| assert!(engine.is_none())),
            (
                Some(ContainerRuntimeConfig::Containerd(Default::default())),
                |engine| assert!(matches!(engine, Some(Engine::Containerd(_)))),
            ),
            (
                Some(ContainerRuntimeConfig::Docker(Default::default())),
                |engine| assert!(matches!(engine, Some(Engine::Docker(_)))),
            ),
            (
                Some(ContainerRuntimeConfig::CRIO(Default::default())),
                |engine| assert!(matches!(engine, Some(Engine::Crio(_)))),
            ),
            (
                Some(ContainerRuntimeConfig::Podman(Default::default())),
                |engine| assert!(matches!(engine, Some(Engine::Podman(_)))),
            ),
        ];

        for (container_runtime_config, expect) in test_cases {
            let runtime = ContainerRuntime::new(
                &Config {
                    container_runtime: dfinit::ContainerRuntime {
                        config: container_runtime_config,
                    },
                    ..Default::default()
                },
                PathBuf::default(),
            );
            expect(runtime.engine.as_ref());
        }
    }

    #[tokio::test]
    async fn run_returns_ok_without_engine() {
        let temp_dir = tempfile::tempdir().unwrap();
        let restart_sentinel = temp_dir.path().join("restart-required");
        let runtime = ContainerRuntime::new(&Config::default(), restart_sentinel.clone());
        assert!(runtime.run().await.is_ok());
        assert!(!restart_sentinel.exists());
    }

    #[tokio::test]
    async fn run_creates_restart_sentinel_only_when_config_rewritten() {
        // (containerd config, expected sentinel)
        let test_cases = vec![
            // config_path is set, hosts.toml is reloaded by containerd without a restart.
            (
                r#"
[plugins."io.containerd.grpc.v1.cri".registry]
  config_path = "{certs_dir}"
"#,
                false,
            ),
            // config_path is not set, config.toml is rewritten and containerd must restart.
            (
                r#"
[plugins."io.containerd.grpc.v1.cri"]
"#,
                true,
            ),
        ];

        for (containerd_config, expected) in test_cases {
            let temp_dir = tempfile::tempdir().unwrap();
            let config_path = temp_dir.path().join("config.toml");
            let certs_dir = temp_dir.path().join("certs.d");
            fs::write(
                &config_path,
                containerd_config.replace("{certs_dir}", certs_dir.to_str().unwrap()),
            )
            .await
            .unwrap();
            let restart_sentinel = temp_dir.path().join("dfinit").join("restart-required");

            let runtime = ContainerRuntime::new(
                &Config {
                    container_runtime: dfinit::ContainerRuntime {
                        config: Some(ContainerRuntimeConfig::Containerd(dfinit::Containerd {
                            config_path,
                            cri_plugin_id: None,
                            registries: vec![],
                            proxy_all_registries: false,
                        })),
                    },
                    ..Default::default()
                },
                restart_sentinel.clone(),
            );
            runtime.run().await.unwrap();
            assert_eq!(restart_sentinel.exists(), expected);
        }
    }
}
