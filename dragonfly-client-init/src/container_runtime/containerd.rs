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

/// Represents the containerd runtime manager.
#[derive(Debug, Clone)]
pub struct Containerd {
    /// The configuration for initializing
    /// runtime environment for the dfdaemon.
    config: dfinit::Containerd,

    /// The configuration for the dfdaemon's proxy server.
    proxy_config: dfinit::Proxy,
}

/// Implements the containerd runtime manager.
impl Containerd {
    /// Creates a new containerd runtime manager.
    #[instrument(skip_all)]
    pub fn new(config: dfinit::Containerd, proxy_config: dfinit::Proxy) -> Self {
        Self {
            config,
            proxy_config,
        }
    }

    /// Runs the containerd runtime to initialize
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

        let plugin_id = self
            .config
            .cri_plugin_id
            .as_deref()
            .unwrap_or_else(|| Self::get_cri_plugin_id(&containerd_config, version));
        info!("containerd CRI plugin: {}", plugin_id);

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
            .ok_or(Error::Unknown(format!("{plugin_id} not found")))?
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

    /// Gets the CRI plugin id owning the registry configuration: the first plugin table
    /// present in the config, checked in version-preferred order. Version 2 configurations
    /// default to "io.containerd.grpc.v1.cri" and version 3 to "io.containerd.cri.v1.images",
    /// but e.g. AKS ships version 2 configurations with the containerd 2.x images plugin.
    #[instrument(skip_all)]
    fn get_cri_plugin_id(containerd_config: &DocumentMut, version: i64) -> &'static str {
        let candidates = match version {
            ..=2 => ["io.containerd.grpc.v1.cri", "io.containerd.cri.v1.images"],
            _ => ["io.containerd.cri.v1.images", "io.containerd.grpc.v1.cri"],
        };

        candidates
            .into_iter()
            .find(|&plugin_id| {
                containerd_config
                    .get("plugins")
                    .and_then(|plugins| plugins.get(plugin_id))
                    .is_some()
            })
            .unwrap_or(candidates[0])
    }

    /// Adds registries to the containerd configuration, when containerd supports
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

    /// Writes a catch-all `_default/hosts.toml` under the containerd
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
    use std::path::Path;
    use tempfile::TempDir;

    type ExpectRun = fn(Result<()>);

    const GRPC_CRI: &str = "io.containerd.grpc.v1.cri";
    const CRI_IMAGES: &str = "io.containerd.cri.v1.images";

    const GRPC_CRI_CONFIG: &str = r#"
[plugins]
  [plugins."io.containerd.grpc.v1.cri"]
    [plugins."io.containerd.grpc.v1.cri".registry]
      config_path = "{certs_dir}"
"#;

    const HOSTS_TOML: &str = r#"server = "https://registry.example.com"

[host."http://127.0.0.1:65001"]
capabilities = ["pull", "resolve"]

[host."http://127.0.0.1:65001".header]
X-Dragonfly-Registry = "https://registry.example.com"
"#;

    const TLS_HOSTS_TOML: &str = r#"server = "https://registry.example.com"

[host."http://127.0.0.1:65001"]
capabilities = ["pull", "resolve"]
skip_verify = true
ca = ["test-ca-cert"]

[host."http://127.0.0.1:65001".header]
X-Dragonfly-Registry = "https://registry.example.com"
"#;

    fn plugins_config(plugin_ids: &[&str]) -> DocumentMut {
        plugin_ids
            .iter()
            .map(|plugin_id| format!("[plugins.\"{plugin_id}\"]\n"))
            .collect::<String>()
            .parse()
            .unwrap()
    }

    fn registry(skip_verify: Option<bool>, ca: Option<Vec<String>>) -> ContainerdRegistry {
        ContainerdRegistry {
            host_namespace: "docker.io".into(),
            server_addr: "https://registry.example.com".into(),
            skip_verify,
            ca,
            capabilities: vec!["pull".into(), "resolve".into()],
        }
    }

    fn containerd(
        config_path: &Path,
        cri_plugin_id: Option<&str>,
        registries: Vec<ContainerdRegistry>,
        proxy_all_registries: bool,
    ) -> Containerd {
        Containerd::new(
            dfinit::Containerd {
                config_path: config_path.to_path_buf(),
                cri_plugin_id: cri_plugin_id.map(str::to_string),
                registries,
                proxy_all_registries,
            },
            dfinit::Proxy {
                addr: "http://127.0.0.1:65001".into(),
            },
        )
    }

    async fn write_config(temp_dir: &TempDir, config: &str) -> (PathBuf, PathBuf) {
        let config_path = temp_dir.path().join("config.toml");
        let certs_dir = temp_dir.path().join("certs.d");
        let config = config.replace("{certs_dir}", certs_dir.to_str().unwrap());
        fs::write(&config_path, config).await.unwrap();
        (config_path, certs_dir)
    }

    fn registry_config_path(config: &str, plugin_id: &str) -> Option<String> {
        let containerd_config = config.parse::<DocumentMut>().unwrap();
        containerd_config
            .get("plugins")?
            .get(plugin_id)?
            .get("registry")?
            .get("config_path")?
            .as_str()
            .map(str::to_string)
    }

    #[test]
    fn get_cri_plugin_id_follows_version_preferred_order() {
        let test_cases = vec![
            (vec![], 2, GRPC_CRI),
            (vec![], 3, CRI_IMAGES),
            (vec![CRI_IMAGES], 2, CRI_IMAGES),
            (vec![GRPC_CRI], 3, GRPC_CRI),
            (vec![GRPC_CRI, CRI_IMAGES], 2, GRPC_CRI),
            (vec![GRPC_CRI, CRI_IMAGES], 3, CRI_IMAGES),
        ];

        for (plugin_ids, version, expected) in test_cases {
            let containerd_config = plugins_config(&plugin_ids);
            assert_eq!(
                Containerd::get_cri_plugin_id(&containerd_config, version),
                expected
            );
        }
    }

    #[tokio::test]
    async fn run_writes_hosts_toml_under_config_path() {
        let test_cases = vec![
            (
                GRPC_CRI_CONFIG,
                None,
                registry(Some(true), Some(vec!["test-ca-cert".into()])),
                TLS_HOSTS_TOML,
            ),
            (
                r#"
version = 2

[plugins]
  [plugins."io.containerd.cri.v1.images"]
    [plugins."io.containerd.cri.v1.images".registry]
      config_path = "{certs_dir}"
"#,
                None,
                registry(None, None),
                HOSTS_TOML,
            ),
            (
                r#"
version = 2

[plugins]
  [plugins."io.containerd.grpc.v1.cri"]
  [plugins."io.containerd.cri.v1.images"]
    [plugins."io.containerd.cri.v1.images".registry]
      config_path = "{certs_dir}"
"#,
                Some(CRI_IMAGES),
                registry(None, None),
                HOSTS_TOML,
            ),
            (
                r#"
version = 3

[plugins]
  [plugins."io.containerd.cri.v1.images"]
    [plugins."io.containerd.cri.v1.images".registry]
      config_path = "{certs_dir}"
"#,
                None,
                registry(Some(true), Some(vec!["test-ca-cert".into()])),
                TLS_HOSTS_TOML,
            ),
            (
                r#"
[plugins]
  [plugins."io.containerd.grpc.v1.cri"]
    [plugins."io.containerd.grpc.v1.cri".registry]
      config_path = "{certs_dir}:/etc/containerd/certs.d"
"#,
                None,
                registry(None, None),
                HOSTS_TOML,
            ),
        ];

        for (config, cri_plugin_id, registry, expected) in test_cases {
            let temp_dir = TempDir::new().unwrap();
            let (config_path, certs_dir) = write_config(&temp_dir, config).await;
            let initial_config = fs::read_to_string(&config_path).await.unwrap();

            let containerd = containerd(&config_path, cri_plugin_id, vec![registry], false);
            let result = containerd.run().await;
            assert!(result.is_ok());

            let hosts = fs::read_to_string(certs_dir.join("docker.io").join("hosts.toml"))
                .await
                .unwrap();
            assert_eq!(hosts, expected);
            assert_eq!(
                fs::read_to_string(&config_path).await.unwrap(),
                initial_config
            );
        }
    }

    #[tokio::test]
    async fn run_writes_default_hosts_only_when_proxy_all_registries() {
        let test_cases = vec![
            (
                true,
                Some(
                    r#"[host."http://127.0.0.1:65001"]
capabilities = ["pull", "resolve"]
"#,
                ),
            ),
            (false, None),
        ];

        for (proxy_all_registries, expected) in test_cases {
            let temp_dir = TempDir::new().unwrap();
            let (config_path, certs_dir) = write_config(&temp_dir, GRPC_CRI_CONFIG).await;
            let containerd = containerd(
                &config_path,
                None,
                vec![registry(None, None)],
                proxy_all_registries,
            );
            let result = containerd.run().await;
            assert!(result.is_ok());

            let hosts = fs::read_to_string(certs_dir.join("docker.io").join("hosts.toml"))
                .await
                .unwrap();
            assert_eq!(hosts, HOSTS_TOML);

            let default_hosts = fs::read_to_string(certs_dir.join("_default").join("hosts.toml"))
                .await
                .ok();
            assert_eq!(default_hosts.as_deref(), expected);
        }
    }

    #[tokio::test]
    async fn run_sets_config_path_when_registry_has_none() {
        let test_cases = vec![
            (
                r#"
[plugins]
  [plugins."io.containerd.grpc.v1.cri"]
"#,
                GRPC_CRI,
            ),
            (
                r#"
version = 3

[plugins]
  [plugins."io.containerd.cri.v1.images"]
    [plugins."io.containerd.cri.v1.images".registry]
      config_path = ""
"#,
                CRI_IMAGES,
            ),
        ];

        for (config, plugin_id) in test_cases {
            let temp_dir = TempDir::new().unwrap();
            let (config_path, _) = write_config(&temp_dir, config).await;
            let containerd = containerd(&config_path, None, vec![], false);
            let result = containerd.run().await;
            assert!(result.is_ok());

            let rewritten_config = fs::read_to_string(&config_path).await.unwrap();
            assert_eq!(
                registry_config_path(&rewritten_config, plugin_id).as_deref(),
                Some("/etc/containerd/certs.d")
            );
        }
    }

    #[tokio::test]
    async fn run_fails_on_invalid_config_or_missing_cri_plugin() {
        let test_cases: Vec<(&str, ExpectRun)> = vec![
            ("version = [", |result| {
                assert!(
                    matches!(result, Err(Error::ExternalError(ref err)) if err.etype == ErrorType::ParseError)
                );
            }),
            ("[plugins]", |result| {
                assert!(
                    matches!(result, Err(Error::Unknown(ref msg)) if msg == "io.containerd.grpc.v1.cri not found")
                );
            }),
        ];

        for (config, expect) in test_cases {
            let temp_dir = TempDir::new().unwrap();
            let (config_path, _) = write_config(&temp_dir, config).await;
            expect(containerd(&config_path, None, vec![], false).run().await);
        }
    }
}
