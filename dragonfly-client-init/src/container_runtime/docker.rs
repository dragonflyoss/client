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
use tracing::info;
use url::Url;

/// Docker represents the docker runtime manager.
#[derive(Debug, Clone)]
pub struct Docker {
    /// config is the configuration for initializing
    /// runtime environment for the dfdaemon.
    config: dfinit::Docker,

    /// proxy_config is the configuration for the dfdaemon's proxy server.
    proxy_config: dfinit::Proxy,
}

/// Docker implements the docker runtime manager.
impl Docker {
    /// new creates a new docker runtime manager.

    pub fn new(config: dfinit::Docker, proxy_config: dfinit::Proxy) -> Self {
        Self {
            config,
            proxy_config,
        }
    }

    /// run runs the docker runtime to initialize
    /// runtime environment for the dfdaemon.

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
        let proxy_location = format!("{}:{}", proxy_host, proxy_port);

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
    use tempfile::NamedTempFile;
    use tokio::fs;

    #[tokio::test]
    async fn test_docker_config_empty() {
        let docker_config_file = NamedTempFile::new().unwrap();
        let docker = Docker::new(
            dfinit::Docker {
                config_path: docker_config_file.path().to_path_buf(),
            },
            dfinit::Proxy {
                addr: "http://127.0.0.1:5000".into(),
            },
        );

        let result = docker.run().await;
        println!("{:?}", result);
        assert!(result.is_ok());

        // Read and verify configuration.
        let contents = fs::read_to_string(docker_config_file.path()).await.unwrap();
        let config: serde_json::Value = serde_json::from_str(&contents).unwrap();

        // Verify proxies configuration.
        assert_eq!(config["proxies"]["http-proxy"], "http://127.0.0.1:5000");
        assert_eq!(config["proxies"]["https-proxy"], "http://127.0.0.1:5000");
    }

    #[tokio::test]
    async fn test_docker_config_existing() {
        let docker_config_file = NamedTempFile::new().unwrap();
        let initial_config = r#"
        {
            "log-driver": "json-file",
            "experimental": true
        }
        "#;
        fs::write(docker_config_file.path(), initial_config)
            .await
            .unwrap();

        let docker = Docker::new(
            dfinit::Docker {
                config_path: docker_config_file.path().to_path_buf(),
            },
            dfinit::Proxy {
                addr: "http://127.0.0.1:5000".into(),
            },
        );

        let result = docker.run().await;
        assert!(result.is_ok());

        // Read and verify configuration.
        let contents = fs::read_to_string(docker_config_file.path()).await.unwrap();
        let config: serde_json::Value = serde_json::from_str(&contents).unwrap();

        // Verify existing configurations.
        assert_eq!(config["log-driver"], "json-file");
        assert_eq!(config["experimental"], true);

        // Verify proxies configuration.
        assert_eq!(config["proxies"]["http-proxy"], "http://127.0.0.1:5000");
        assert_eq!(config["proxies"]["https-proxy"], "http://127.0.0.1:5000");
    }

    #[tokio::test]
    async fn test_docker_config_invalid_json() {
        let docker_config_file = NamedTempFile::new().unwrap();
        let invalid_config = r#"
        {
            "log-driver": "json-file",
            "experimental": true,
        }
        "#;
        fs::write(docker_config_file.path(), invalid_config)
            .await
            .unwrap();

        let docker = Docker::new(
            dfinit::Docker {
                config_path: docker_config_file.path().to_path_buf(),
            },
            dfinit::Proxy {
                addr: "http://127.0.0.1:5000".into(),
            },
        );

        let result = docker.run().await;
        assert!(result.is_err());
        if let Err(e) = result {
            assert_eq!(
                format!("{}", e),
                "ParseError cause: trailing comma at line 5 column 9"
            );
        }
    }

    #[tokio::test]
    async fn test_docker_config_proxies_existing() {
        let docker_config_file = NamedTempFile::new().unwrap();
        let existing_proxies = r#"
        {
            "proxies": {
                "http-proxy": "http://old-proxy:3128",
                "https-proxy": "https://old-proxy:3129",
                "no-proxy": "old-no-proxy"
            },
            "log-driver": "json-file"
        }
        "#;
        fs::write(docker_config_file.path(), existing_proxies)
            .await
            .unwrap();

        let docker = Docker::new(
            dfinit::Docker {
                config_path: docker_config_file.path().to_path_buf(),
            },
            dfinit::Proxy {
                addr: "http://127.0.0.1:5000".into(),
            },
        );

        let result = docker.run().await;
        assert!(result.is_ok());

        // Read and verify configuration.
        let contents = fs::read_to_string(docker_config_file.path()).await.unwrap();
        let config: serde_json::Value = serde_json::from_str(&contents).unwrap();

        // Verify existing configurations.
        assert_eq!(config["log-driver"], "json-file");

        // Verify proxies configuration.
        assert_eq!(config["proxies"]["http-proxy"], "http://127.0.0.1:5000");
        assert_eq!(config["proxies"]["https-proxy"], "http://127.0.0.1:5000");
    }
}
