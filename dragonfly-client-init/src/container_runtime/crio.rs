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

use dragonfly_client_config::dfinit;
use dragonfly_client_core::{error::{ErrorType, OrErr}, Error, Result};
use tokio::{self, fs};
use toml_edit::{value, Array, ArrayOfTables, Item, Table, Value};
use tracing::info;
use url::Url;

// CRIO represents the cri-o runtime manager.
#[derive(Debug, Clone)]
pub struct CRIO {
    // config is the configuration for initializing
    // runtime environment for the dfdaemon.
    config: dfinit::CRIO,

    // proxy_config is the configuration for the dfdaemon's proxy server.
    proxy_config: dfinit::Proxy,
}

// CRIO implements the cri-o runtime manager.
impl CRIO {
    // new creates a new cri-o runtime manager.
    pub fn new(config: dfinit::CRIO, proxy_config: dfinit::Proxy) -> Self {
        Self {
            config,
            proxy_config,
        }
    }

    // run runs the cri-o runtime to initialize
    // runtime environment for the dfdaemon.
    pub async fn run(&self) -> Result<()> {
        let mut registries_config_table = toml_edit::DocumentMut::new();
        registries_config_table.set_implicit(true);

        // Add unqualified-search-registries to registries config.
        let mut unqualified_search_registries = Array::default();
        for unqualified_search_registry in self.config.unqualified_search_registries.clone() {
            unqualified_search_registries.push(Value::from(unqualified_search_registry));
        }
        registries_config_table.insert(
            "unqualified-search-registries",
            value(unqualified_search_registries),
        );

        // Parse proxy address to get host and port.
        let proxy_url = Url::parse(self.proxy_config.addr.as_str()).or_err(ErrorType::ParseError)?;
        let proxy_host = proxy_url
            .host_str()
            .ok_or(Error::Unknown("host not found".to_string()))?;
        let proxy_port = proxy_url
            .port_or_known_default()
            .ok_or(Error::Unknown("port not found".to_string()))?;
        let proxy_locaiton = format!("{}:{}", proxy_host, proxy_port);

        // Add registries to the registries config.
        let mut registries_table = ArrayOfTables::new();
        for registry in self.config.registries.clone() {
            info!("add registry: {:?}", registry);
            let mut registry_mirror_table = Table::new();
            registry_mirror_table.set_implicit(true);
            registry_mirror_table.insert("insecure", value(true));
            registry_mirror_table.insert("location", value(proxy_locaiton.as_str()));

            let mut registry_mirrors_table = ArrayOfTables::new();
            registry_mirrors_table.push(registry_mirror_table);

            let mut registry_table = Table::new();
            registry_table.set_implicit(true);
            registry_table.insert("prefix", value(registry.prefix));
            registry_table.insert("location", value(registry.location));
            registry_table.insert("mirror", Item::ArrayOfTables(registry_mirrors_table));

            registries_table.push(registry_table);
        }
        registries_config_table.insert("registry", Item::ArrayOfTables(registries_table));

        let registries_config_dir = self
            .config
            .config_path
            .parent()
            .ok_or(Error::Unknown("invalid config path".to_string()))?;
        fs::create_dir_all(registries_config_dir.as_os_str()).await?;
        fs::write(
            self.config.config_path.as_os_str(),
            registries_config_table.to_string().as_bytes(),
        )
        .await?;

        Ok(())
    }
}
