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
use dragonfly_client_config::dfinit::{
    self, default_container_runtime_containerd_registry_capabilities, ContainerdRegistry,
};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use std::path::PathBuf;
use tokio::{self, fs};
use toml_edit::{value, Array, DocumentMut, Item, Table, Value};
use tracing::{info, instrument};

/// Containerd represents the containerd runtime manager.
#[derive(Debug, Clone)]
pub struct Containerd {
    /// config is the configuration for initializing
    /// runtime environment for the dfdaemon.
    config: dfinit::Containerd,

    /// proxy_config is the configuration for the dfdaemon's proxy server.
    proxy_config: dfinit::Proxy,
}

/// Containerd implements the containerd runtime manager.
impl Containerd {
    /// new creates a new containerd runtime manager.
    #[instrument(skip_all)]
    pub fn new(config: dfinit::Containerd, proxy_config: dfinit::Proxy) -> Self {
        Self {
            config,
            proxy_config,
        }
    }

    /// run runs the containerd runtime to initialize
    /// runtime environment for the dfdaemon.
    #[instrument(skip_all)]
    pub async fn run(&self) -> Result<()> {
        let content = fs::read_to_string(&self.config.config_path).await?;
        let mut containerd_config = content
            .parse::<DocumentMut>()
            .or_err(ErrorType::ParseError)?;

        // Get the containerd version for config_path parsing, default to containerd 1.x if not set.
        // https://github.com/containerd/containerd/blob/main/docs/hosts.md#cri.
        let version = containerd_config
            .get("version")
            .and_then(|v| v.as_integer())
            .unwrap_or(2);
        info!("containerd version: {}", version);

        let plugin_id = if version == 3 {
            "io.containerd.cri.v1.images"
        } else {
            "io.containerd.grpc.v1.cri"
        };

        // If containerd supports config_path mode and config_path is not empty,
        // add registries to the certs.d directory.
        if let Some(config_path) = containerd_config
            .get("plugins")
            .and_then(|plugins| plugins.get(plugin_id))
            .and_then(|cri| cri.get("registry"))
            .and_then(|registry| registry.get("config_path"))
            .and_then(|config_path| config_path.as_str())
            .filter(|config_path| !config_path.is_empty())
        {
            // Rebind config_path to the first entry if multiple paths are present
            let config_path = config_path.split(':').next().unwrap_or(config_path);

            info!(
                "containerd supports config_path mode, config_path: {}",
                config_path.to_string()
            );

            self.add_registries(
                config_path,
                self.config.registries.clone(),
                self.proxy_config.clone(),
            )
            .await?;

            if self.config.proxy_all_registries {
                self.add_default_registry(config_path, self.proxy_config.clone())
                    .await?;
            }

            return Ok(());
        }

        // If containerd does not support mirror mode and config_path not set, create a new
        // config_path for the registries.
        info!("containerd not supports mirror mode and config_path not set");
        let config_path = "/etc/containerd/certs.d";

        // Add config_path to the containerd configuration.
        let mut registry_table = Table::new();
        registry_table.set_implicit(true);
        registry_table.insert("config_path", value(config_path));
        containerd_config["plugins"][plugin_id]
            .as_table_mut()
            .ok_or(Error::Unknown(format!("{} not found", plugin_id)))?
            .insert("registry", Item::Table(registry_table));

        // Override containerd configuration.
        info!("override containerd configuration");
        fs::write(
            &self.config.config_path,
            containerd_config.to_string().as_bytes(),
        )
        .await?;

        self.add_registries(
            config_path,
            self.config.registries.clone(),
            self.proxy_config.clone(),
        )
        .await?;

        if self.config.proxy_all_registries {
            self.add_default_registry(config_path, self.proxy_config.clone())
                .await?;
        }

        Ok(())
    }

    /// add_registries adds registries to the containerd configuration, when containerd supports
    /// config_path mode and config_path is not empty.
    #[instrument(skip_all)]
    pub async fn add_registries(
        &self,
        config_path: &str,
        registries: Vec<ContainerdRegistry>,
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

            // Add insecure to the host configuration.
            if let Some(skip_verify) = registry.skip_verify {
                host_config_table.insert("skip_verify", value(skip_verify));
            }

            // Add ca to the host configuration.
            let mut certs = Array::default();
            if let Some(ca) = registry.ca {
                for cert in ca {
                    certs.push(Value::from(cert));
                }
                host_config_table.insert("ca", Item::Value(Value::Array(certs)));
            }

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

    /// add_default_registry writes a catch-all `_default/hosts.toml` under the containerd
    /// config_path so that registries not explicitly listed in `registries` are still
    /// proxied through dfdaemon. The dfdaemon infers the upstream registry from the `ns=`
    /// query parameter that containerd appends when resolving via the `_default` fallback,
    /// so no `X-Dragonfly-Registry` header and no top-level `server` field are set.
    /// Explicitly configured registries keep their own `hosts.toml` and take precedence.
    #[instrument(skip_all)]
    pub async fn add_default_registry(
        &self,
        config_path: &str,
        proxy_config: dfinit::Proxy,
    ) -> Result<()> {
        info!(
            "add _default catch-all mirror pointing at {}",
            proxy_config.addr
        );

        let mut host_config_table = Table::new();
        host_config_table.set_implicit(true);

        let mut capabilities = Array::default();
        for capability in default_container_runtime_containerd_registry_capabilities() {
            capabilities.push(Value::from(capability));
        }
        host_config_table.insert("capabilities", value(capabilities));

        let mut host_table = Table::new();
        host_table.set_implicit(true);
        host_table.insert(proxy_config.addr.as_str(), Item::Table(host_config_table));

        let mut default_table = toml_edit::DocumentMut::new();
        default_table.set_implicit(true);
        default_table.insert("host", Item::Table(host_table));

        let default_config_dir = PathBuf::from(config_path).join("_default");
        fs::create_dir_all(default_config_dir.as_os_str()).await?;
        fs::write(
            default_config_dir.join("hosts.toml").as_os_str(),
            default_table.to_string().as_bytes(),
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use tokio::fs;

    #[tokio::test]
    async fn test_containerd_config_with_v2_config_path() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        let certs_dir = temp_dir.path().join("certs.d");
        let certs_dir_str = certs_dir.to_str().unwrap();

        // Create initial containerd config with config_path
        let initial_config = format!(
            r#"
[plugins]
  [plugins."io.containerd.grpc.v1.cri"]
    [plugins."io.containerd.grpc.v1.cri".registry]
      config_path = "{}"
"#,
            certs_dir_str
        );
        fs::write(&config_path, initial_config).await.unwrap();

        // Create Containerd instance
        let containerd = Containerd::new(
            dfinit::Containerd {
                config_path: config_path.clone(),
                registries: vec![ContainerdRegistry {
                    host_namespace: "docker.io".into(),
                    server_addr: "https://registry.example.com".into(),
                    skip_verify: Some(true),
                    ca: Some(vec!["test-ca-cert".into()]),
                    capabilities: vec!["pull".into(), "resolve".into()],
                }],
                proxy_all_registries: false,
            },
            dfinit::Proxy {
                addr: "http://127.0.0.1:65001".into(),
            },
        );

        // Run containerd configuration
        let result = containerd.run().await;
        if let Err(e) = &result {
            println!("Error: {:?}", e);
            if let Ok(contents) = fs::read_to_string(&config_path).await {
                println!("Current config file contents:\n{}", contents);
            }
        }
        assert!(result.is_ok());

        // Verify the hosts.toml file content
        let hosts_file_path = certs_dir.join("docker.io").join("hosts.toml");
        let contents = fs::read_to_string(&hosts_file_path).await.unwrap();

        let expected_contents = r#"server = "https://registry.example.com"

[host."http://127.0.0.1:65001"]
capabilities = ["pull", "resolve"]
skip_verify = true
ca = ["test-ca-cert"]

[host."http://127.0.0.1:65001".header]
X-Dragonfly-Registry = "https://registry.example.com"
"#;

        assert_eq!(contents.trim(), expected_contents.trim());
    }

    #[tokio::test]
    async fn test_containerd_config_with_proxy_all_registries() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        let certs_dir = temp_dir.path().join("certs.d");
        let certs_dir_str = certs_dir.to_str().unwrap();

        let initial_config = format!(
            r#"
[plugins]
  [plugins."io.containerd.grpc.v1.cri"]
    [plugins."io.containerd.grpc.v1.cri".registry]
      config_path = "{}"
"#,
            certs_dir_str
        );
        fs::write(&config_path, initial_config).await.unwrap();

        let containerd = Containerd::new(
            dfinit::Containerd {
                config_path: config_path.clone(),
                registries: vec![ContainerdRegistry {
                    host_namespace: "docker.io".into(),
                    server_addr: "https://registry.example.com".into(),
                    skip_verify: None,
                    ca: None,
                    capabilities: vec!["pull".into(), "resolve".into()],
                }],
                proxy_all_registries: true,
            },
            dfinit::Proxy {
                addr: "http://127.0.0.1:65001".into(),
            },
        );

        let result = containerd.run().await;
        assert!(result.is_ok(), "containerd.run() failed: {:?}", result);

        // Explicitly configured registry still gets its own hosts.toml with the registry header.
        let explicit_hosts = fs::read_to_string(certs_dir.join("docker.io").join("hosts.toml"))
            .await
            .unwrap();
        assert!(explicit_hosts.contains("X-Dragonfly-Registry = \"https://registry.example.com\""));

        // _default catch-all is written, without a top-level `server` or X-Dragonfly-Registry
        // header — dfdaemon infers the upstream from the containerd `ns=` query parameter.
        let default_hosts = fs::read_to_string(certs_dir.join("_default").join("hosts.toml"))
            .await
            .unwrap();
        let expected = r#"[host."http://127.0.0.1:65001"]
capabilities = ["pull", "resolve"]
"#;
        assert_eq!(default_hosts.trim(), expected.trim());
    }

    #[tokio::test]
    async fn test_containerd_config_without_proxy_all_registries() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        let certs_dir = temp_dir.path().join("certs.d");
        let certs_dir_str = certs_dir.to_str().unwrap();

        let initial_config = format!(
            r#"
[plugins]
  [plugins."io.containerd.grpc.v1.cri"]
    [plugins."io.containerd.grpc.v1.cri".registry]
      config_path = "{}"
"#,
            certs_dir_str
        );
        fs::write(&config_path, initial_config).await.unwrap();

        let containerd = Containerd::new(
            dfinit::Containerd {
                config_path: config_path.clone(),
                registries: vec![],
                proxy_all_registries: false,
            },
            dfinit::Proxy {
                addr: "http://127.0.0.1:65001".into(),
            },
        );

        assert!(containerd.run().await.is_ok());
        assert!(!certs_dir.join("_default").join("hosts.toml").exists());
    }

    #[tokio::test]
    async fn test_containerd_config_with_v3_config_path() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.toml");
        let certs_dir = temp_dir.path().join("certs.d");
        let certs_dir_str = certs_dir.to_str().unwrap();

        // Create initial containerd config with version = 3 and config_path
        let initial_config = format!(
            r#"
version = 3

[plugins]
  [plugins."io.containerd.cri.v1.images"]
    [plugins."io.containerd.cri.v1.images".registry]
      config_path = "{}"
"#,
            certs_dir_str
        );
        fs::write(&config_path, initial_config).await.unwrap();

        // Create Containerd instance
        let containerd = Containerd::new(
            dfinit::Containerd {
                config_path: config_path.clone(),
                registries: vec![ContainerdRegistry {
                    host_namespace: "docker.io".into(),
                    server_addr: "https://registry.example.com".into(),
                    skip_verify: Some(true),
                    ca: Some(vec!["test-ca-cert".into()]),
                    capabilities: vec!["pull".into(), "resolve".into()],
                }],
                proxy_all_registries: false,
            },
            dfinit::Proxy {
                addr: "http://127.0.0.1:65001".into(),
            },
        );

        // Run containerd configuration
        let result = containerd.run().await;
        if let Err(e) = &result {
            println!("Error: {:?}", e);
            if let Ok(contents) = fs::read_to_string(&config_path).await {
                println!("Current config file contents:\n{}", contents);
            }
        }
        assert!(result.is_ok());

        // Verify the hosts.toml file content
        let hosts_file_path = certs_dir.join("docker.io").join("hosts.toml");
        let contents = fs::read_to_string(&hosts_file_path).await.unwrap();

        let expected_contents = r#"server = "https://registry.example.com"

[host."http://127.0.0.1:65001"]
capabilities = ["pull", "resolve"]
skip_verify = true
ca = ["test-ca-cert"]

[host."http://127.0.0.1:65001".header]
X-Dragonfly-Registry = "https://registry.example.com"
"#;

        assert_eq!(contents.trim(), expected_contents.trim());
    }
}
