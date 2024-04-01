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

use dragonfly_client::proxy::header::DRAGONFLY_REGISTRY_HEADER;
use dragonfly_client_config::dfinit::{self, Registry};
use dragonfly_client_core::{Error, Result};
use std::path::PathBuf;
use tokio::{self, fs};
use toml_edit::{value, Array, DocumentMut, Item, Table, Value};
use tracing::info;

// Containerd represents the containerd runtime manager.
pub struct Containerd {
    // config is the configuration for initializing
    // runtime environment for the dfdaemon.
    config: dfinit::Containerd,

    // proxy_config is the configuration for the dfdaemon's proxy server.
    proxy_config: dfinit::Proxy,
}

// Containerd implements the containerd runtime manager.
impl Containerd {
    // new creates a new containerd runtime manager.
    pub fn new(config: dfinit::Containerd, proxy_config: dfinit::Proxy) -> Self {
        Self {
            config,
            proxy_config,
        }
    }

    // run runs the containerd runtime to initialize
    // runtime environment for the dfdaemon.
    pub async fn run(&self) -> Result<()> {
        let content = fs::read_to_string(&self.config.config_path).await?;
        let mut containerd_config = content.parse::<DocumentMut>()?;

        // If containerd supports config_path mode and config_path is not empty,
        // add registries to the certs.d directory.
        if let Some(config_path) =
            containerd_config["plugins"]["io.containerd.grpc.v1.cri"]["registry"].get("config_path")
        {
            if let Some(config_path) = config_path.as_str() {
                if !config_path.is_empty() {
                    info!(
                        "containerd supports config_path mode, config_path: {}",
                        config_path.to_string()
                    );
                    return self
                        .add_registries(
                            config_path,
                            self.config.registries.clone(),
                            self.proxy_config.clone(),
                        )
                        .await;
                }
            }
        }

        // If containerd is old version and supports mirror mode, add registries to the
        // registry mirrors in containerd configuration.
        info!("containerd not supports config_path mode, use mirror mode to add registries");
        containerd_config = self.add_registries_by_mirrors(
            self.config.registries.clone(),
            self.proxy_config.clone(),
            containerd_config,
        )?;

        // Override containerd configuration.
        info!("override containerd configuration");
        fs::write(
            &self.config.config_path,
            containerd_config.to_string().as_bytes(),
        )
        .await?;
        Ok(())
    }

    // add_registries adds registries to the containerd configuration, when containerd supports
    // config_path mode and config_path is not empty.
    pub async fn add_registries(
        &self,
        config_path: &str,
        registries: Vec<Registry>,
        proxy_config: dfinit::Proxy,
    ) -> Result<()> {
        for registry in registries {
            info!("add registry: {:?}", registry);
            let mut registry_table = toml_edit::DocumentMut::new();
            registry_table.set_implicit(true);
            registry_table.insert("server", value(registry.server_addr.clone()));

            let mut host_config_table = Table::new();
            host_config_table.set_implicit(true);

            // Add capabilities to the host configuration.
            let mut capabilities = Array::default();
            for capability in registry.capabilities {
                capabilities.push(Value::from(capability));
            }
            host_config_table.insert("capabilities", value(capabilities));

            // Add X-Dragonfly-Registry header to the host configuration.
            let mut headers_table = Table::new();
            headers_table.insert(DRAGONFLY_REGISTRY_HEADER, value(registry.server_addr));
            host_config_table.insert("header", Item::Table(headers_table));

            // Add host configuration to the registry table.
            let mut host_table = Table::new();
            host_table.set_implicit(true);
            host_table.insert(proxy_config.addr.as_str(), Item::Table(host_config_table));
            registry_table.insert("host", Item::Table(host_table));

            let registry_config_dir = PathBuf::from(config_path).join(registry.host_namespace);
            fs::create_dir_all(registry_config_dir.as_os_str()).await?;
            fs::write(
                registry_config_dir.join("hosts.toml").as_os_str(),
                registry_table.to_string().as_bytes(),
            )
            .await?;
        }

        Ok(())
    }

    // add_registries_by_mirrors adds registries to the containerd configuration, when containerd
    // supports mirror mode with old version.
    pub fn add_registries_by_mirrors(
        &self,
        registries: Vec<Registry>,
        proxy_config: dfinit::Proxy,
        mut containerd_config: DocumentMut,
    ) -> Result<DocumentMut> {
        let mut mirrors_table = Table::new();
        mirrors_table.set_implicit(true);

        for registry in registries {
            info!("add registry: {:?}", registry);

            // Add endpoints to the mirror configuration.
            let mut endpoints = Array::default();
            endpoints.push(Value::from(proxy_config.addr.clone()));
            endpoints.push(Value::from(registry.server_addr));

            let mut mirror_table = Table::new();
            mirror_table.insert("endpoint", value(endpoints));

            mirrors_table.insert(&registry.host_namespace, Item::Table(mirror_table));
        }

        let registry_table = containerd_config["plugins"]["io.containerd.grpc.v1.cri"]["registry"]
            .as_table_mut()
            .ok_or(Error::Unknown("registry field not found".to_string()))?;
        registry_table.insert("mirrors", Item::Table(mirrors_table));

        Ok(containerd_config)
    }

    // is_enabled returns true if containerd feature is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enable
    }
}
