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
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use serde_json::{json, Value};
use tokio::{self, fs};
use tracing::{info, instrument};
use url::Url;

/// Represents the docker runtime manager.
#[derive(Debug, Clone)]
pub struct Docker {
    /// The configuration for initializing
    /// runtime environment for the dfdaemon.
    config: dfinit::Docker,

    /// The configuration for the dfdaemon's proxy server.
    proxy_config: dfinit::Proxy,
}

/// Implements the docker runtime manager.
impl Docker {
    /// Creates a new docker runtime manager.
    #[instrument(skip_all)]
    pub fn new(config: dfinit::Docker, proxy_config: dfinit::Proxy) -> Self {
        Self {
            config,
            proxy_config,
        }
    }

    /// Runs the docker runtime to initialize
    /// runtime environment for the dfdaemon.
    #[instrument(skip_all)]
    pub async fn run(&self) -> Result<()> {
        info!(
            "docker feature is enabled, proxy_addr: {}, config_path: {:?}",
            self.proxy_config.addr, self.config.config_path,
        );

        // Parse proxy address to get host and port.
        let proxy_url = Url::parse(&self.proxy_config.addr).or_err(ErrorType::ParseError)?;
        let proxy_host = proxy_url
            .host_str()
            .ok_or(Error::Unknown("host not found".to_string()))?;
        let proxy_port = proxy_url
            .port_or_known_default()
            .ok_or(Error::Unknown("port not found".to_string()))?;
        let proxy_location = format!("{proxy_host}:{proxy_port}");

        // Prepare proxies configuration.
        let mut proxies_map = serde_json::Map::new();
        proxies_map.insert(
            "http-proxy".to_string(),
            json!(format!("http://{}", proxy_location)),
        );
        proxies_map.insert(
            "https-proxy".to_string(),
            json!(format!("http://{}", proxy_location)),
        );

        let config_path = &self.config.config_path;
        let mut docker_config: serde_json::Map<String, Value> = if config_path.exists() {
            let contents = fs::read_to_string(config_path).await?;
            if contents.trim().is_empty() {
                serde_json::Map::new()
            } else {
                serde_json::from_str(&contents).or_err(ErrorType::ParseError)?
            }
        } else {
            serde_json::Map::new()
        };

        // Insert or update proxies configuration.
        docker_config.insert("proxies".to_string(), Value::Object(proxies_map));

        // Create config directory if it doesn't exist.
        let config_dir = config_path
            .parent()
            .ok_or(Error::Unknown("invalid config path".to_string()))?;
        fs::create_dir_all(config_dir).await?;

        // Write configuration to file.
        fs::write(
            config_path,
            serde_json::to_string_pretty(&Value::Object(docker_config))
                .or_err(ErrorType::SerializeError)?,
        )
        .await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;

    const INVALID_DAEMON_JSON: &str = r#"
        {
            "log-driver": "json-file",
            "experimental": true,
        }
        "#;

    fn docker(config_path: &Path, proxy_addr: &str) -> Docker {
        Docker::new(
            dfinit::Docker {
                config_path: config_path.to_path_buf(),
            },
            dfinit::Proxy {
                addr: proxy_addr.into(),
            },
        )
    }

    async fn write_daemon_json(temp_dir: &TempDir, contents: Option<&str>) -> PathBuf {
        let config_path = temp_dir.path().join("docker").join("daemon.json");
        if let Some(contents) = contents {
            fs::create_dir_all(config_path.parent().unwrap())
                .await
                .unwrap();
            fs::write(&config_path, contents).await.unwrap();
        }
        config_path
    }

    #[tokio::test]
    async fn run_writes_proxies_into_daemon_json() {
        let proxies = json!({
            "http-proxy": "http://127.0.0.1:5000",
            "https-proxy": "http://127.0.0.1:5000",
        });

        let test_cases = vec![
            (None, json!({ "proxies": proxies })),
            (Some(""), json!({ "proxies": proxies })),
            (
                Some(r#"{ "log-driver": "json-file", "experimental": true }"#),
                json!({
                    "log-driver": "json-file",
                    "experimental": true,
                    "proxies": proxies,
                }),
            ),
            (
                Some(
                    r#"{
                        "proxies": {
                            "http-proxy": "http://old-proxy:3128",
                            "https-proxy": "https://old-proxy:3129",
                            "no-proxy": "old-no-proxy"
                        },
                        "log-driver": "json-file"
                    }"#,
                ),
                json!({ "log-driver": "json-file", "proxies": proxies }),
            ),
        ];

        for (initial_config, expected) in test_cases {
            let temp_dir = TempDir::new().unwrap();
            let config_path = write_daemon_json(&temp_dir, initial_config).await;
            let result = docker(&config_path, "http://127.0.0.1:5000").run().await;
            assert!(result.is_ok());

            let contents = fs::read_to_string(&config_path).await.unwrap();
            let config: Value = serde_json::from_str(&contents).unwrap();
            assert_eq!(config, expected);
        }
    }

    #[tokio::test]
    async fn run_fails_on_invalid_daemon_json_or_proxy_addr() {
        let test_cases = vec![
            (
                Some(INVALID_DAEMON_JSON),
                "http://127.0.0.1:5000",
                "ParseError cause: trailing comma at line 5 column 9",
            ),
            (
                None,
                "127.0.0.1:5000",
                "ParseError cause: relative URL without a base",
            ),
            (
                None,
                "unix:/var/run/dfdaemon.sock",
                "unknown host not found",
            ),
            (None, "dfdaemon://127.0.0.1", "unknown port not found"),
        ];

        for (initial_config, proxy_addr, expected) in test_cases {
            let temp_dir = TempDir::new().unwrap();
            let config_path = write_daemon_json(&temp_dir, initial_config).await;
            let result = docker(&config_path, proxy_addr).run().await;
            assert_eq!(result.unwrap_err().to_string(), expected);
        }
    }
}
