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

use crate::dfdaemon::default_proxy_server_port;
use dragonfly_client_core::Result;
use serde::{Deserialize, Serialize};
use std::fs;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use tracing::info;
use validator::Validate;

// NAME is the name of dfinit.
pub const NAME: &str = "dfinit";

// default_dfinit_config_path is the default config path for dfinit.
#[inline]
pub fn default_dfinit_config_path() -> PathBuf {
    super::default_config_dir().join("dfinit.yaml")
}

// default_dfinit_log_dir is the default log directory for dfinit.
pub fn default_dfinit_log_dir() -> PathBuf {
    super::default_log_dir().join(NAME)
}

// default_container_runtime_containerd_config_path is the default containerd configuration path.
#[inline]
fn default_container_runtime_containerd_config_path() -> PathBuf {
    PathBuf::from("/etc/containerd/config.toml")
}

// default_container_runtime_docker_config_path is the default docker configuration path.
#[inline]
fn default_container_runtime_docker_config_path() -> PathBuf {
    PathBuf::from("/etc/docker/daemon.json")
}

// default_container_runtime_crio_config_path is the default cri-o configuration path.
#[inline]
fn default_container_runtime_crio_config_path() -> PathBuf {
    PathBuf::from("/etc/containers/registries.conf")
}

// default_container_runtime_crio_unqualified_search_registries is the default unqualified search registries of cri-o,
// refer to https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#global-settings.
#[inline]
fn default_container_runtime_crio_unqualified_search_registries() -> Vec<String> {
    vec![
        "registry.fedoraproject.org".to_string(),
        "registry.access.redhat.com".to_string(),
        "docker.io".to_string(),
    ]
}

// default_proxy_addr is the default proxy address of dfdaemon.
#[inline]
fn default_proxy_addr() -> String {
    format!(
        "http://{}:{}",
        Ipv4Addr::LOCALHOST,
        default_proxy_server_port()
    )
}

// default_container_runtime_containerd_registry_host_capabilities is the default
// capabilities of the containerd registry.
#[inline]
fn default_container_runtime_containerd_registry_capabilities() -> Vec<String> {
    vec!["pull".to_string(), "resolve".to_string()]
}

// Registry is the registry configuration for containerd.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ContainerdRegistry {
    // host_namespace is the location where container images and artifacts are sourced,
    // refer to https://github.com/containerd/containerd/blob/main/docs/hosts.md#registry-host-namespace.
    // The registry host namespace portion is [registry_host_name|IP address][:port], such as
    // docker.io, ghcr.io, gcr.io, etc.
    pub host_namespace: String,

    // server_addr specifies the default server for this registry host namespace, refer to
    // https://github.com/containerd/containerd/blob/main/docs/hosts.md#server-field.
    pub server_addr: String,

    // capabilities is the list of capabilities in containerd configuration, refer to
    // https://github.com/containerd/containerd/blob/main/docs/hosts.md#capabilities-field.
    #[serde(default = "default_container_runtime_containerd_registry_capabilities")]
    pub capabilities: Vec<String>,
}

// Containerd is the containerd configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Containerd {
    // config_path is the path of containerd configuration file.
    #[serde(default = "default_container_runtime_containerd_config_path")]
    pub config_path: PathBuf,

    // registries is the list of containerd registries.
    pub registries: Vec<ContainerdRegistry>,
}

// CRIORegistry is the registry configuration for cri-o.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CRIORegistry {
    // prefix is the prefix of the user-specified image name, refer to
    // https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#choosing-a-registry-toml-table.
    pub prefix: String,

    // location accepts the same format as the prefix field, and specifies the physical location of the prefix-rooted namespace,
    // refer to https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#remapping-and-mirroring-registries.
    pub location: String,
}

// CRIO is the cri-o configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CRIO {
    // enable is a flag to enable cri-o feature.
    pub enable: bool,

    // config_path is the path of cri-o registries's configuration file.
    #[serde(default = "default_container_runtime_crio_config_path")]
    pub config_path: PathBuf,

    // unqualified_search_registries is an array of host[:port] registries to try when pulling an unqualified image, in order.
    // Refer to https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#global-settings.
    #[serde(default = "default_container_runtime_crio_unqualified_search_registries")]
    pub unqualified_search_registries: Vec<String>,

    // registries is the list of cri-o registries, refer to
    // https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#namespaced-registry-settings.
    pub registries: Vec<CRIORegistry>,
}

// Docker is the docker configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Docker {
    // config_path is the path of docker configuration file.
    #[serde(default = "default_container_runtime_docker_config_path")]
    pub config_path: PathBuf,
}

// ContainerRuntime is the container runtime configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ContainerRuntime {
    #[serde(flatten)]
    pub config: Option<ContainerRuntimeConfig>,
}

#[derive(Debug, Clone)]
pub enum ContainerRuntimeConfig {
    Containerd(Containerd),
    Docker(Docker),
}

impl Serialize for ContainerRuntimeConfig {
    fn serialize<S>(&self, serializer: S) -> std::prelude::v1::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        match *self {
            ContainerRuntimeConfig::Containerd(ref cfg) => {
                let mut state = serializer.serialize_struct("containerd", 1)?;
                state.serialize_field("containerd", &cfg)?;
                state.end()
            }
            ContainerRuntimeConfig::Docker(ref cfg) => {
                let mut state = serializer.serialize_struct("docker", 1)?;
                state.serialize_field("docker", &cfg)?;
                state.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for ContainerRuntimeConfig {
    fn deserialize<D>(deserializer: D) -> std::prelude::v1::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ContainerRuntimeHelper {
            containerd: Option<Containerd>,
            docker: Option<Docker>,
        }

        let helper = ContainerRuntimeHelper::deserialize(deserializer)?;
        if helper.containerd.is_some() {
            Ok(ContainerRuntimeConfig::Containerd(
                helper.containerd.unwrap(),
            ))
        } else if helper.docker.is_some() {
            Ok(ContainerRuntimeConfig::Docker(helper.docker.unwrap()))
        } else {
            use serde::de::Error;
            Err(D::Error::custom("expected containerd or docker"))
        }
    }
}

// Proxy is the proxy server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Proxy {
    // addr is the proxy server address of dfdaemon.
    #[serde(default = "default_proxy_addr")]
    pub addr: String,
}

// Proxy implements Default.
impl Default for Proxy {
    fn default() -> Self {
        Self {
            addr: default_proxy_addr(),
        }
    }
}

// Config is the configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    // proxy is the configuration of the dfdaemon's HTTP/HTTPS proxy.
    #[validate]
    pub proxy: Proxy,

    // container_runtime is the container runtime configuration.
    #[validate]
    pub container_runtime: ContainerRuntime,
}

// Config implements the config operation of dfinit.
impl Config {
    // load loads configuration from file.
    pub fn load(path: &PathBuf) -> Result<Config> {
        // Load configuration from file.
        let content = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content)?;
        info!("load config from {}", path.display());

        // Validate configuration.
        config.validate()?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seralize_container_runtime() {
        let cfg = ContainerRuntimeConfig::Containerd(Containerd {
            ..Default::default()
        });
        let res = serde_yaml::to_string(&cfg).unwrap();
        let expected = r#"
containerd:
  configPath: ''
  registries: []"#;
        assert_eq!(expected.trim(), res.trim());

        let runtime_cfg = ContainerRuntimeConfig::Docker(Docker {
            config_path: PathBuf::from("/root/.dragonfly/config/dfinit/yaml"),
        });
        let cfg = Config {
            container_runtime: ContainerRuntime {
                config: Some(runtime_cfg),
            },
            proxy: Proxy {
                addr: String::from("hello"),
            },
        };

        let res = serde_yaml::to_string(&cfg).unwrap();
        let expected = r#"
proxy:
  addr: hello
containerRuntime:
  docker:
    configPath: /root/.dragonfly/config/dfinit/yaml"#;
        assert_eq!(expected.trim(), res.trim());

        let runtime_cfg = ContainerRuntimeConfig::Containerd(Containerd {
            config_path: PathBuf::from("/root/.dragonfly/config/dfinit/yaml"),
            ..Default::default()
        });
        let cfg = Config {
            container_runtime: ContainerRuntime {
                config: Some(runtime_cfg),
            },
            proxy: Proxy {
                addr: String::from("hello"),
            },
        };
        let res = serde_yaml::to_string(&cfg).unwrap();
        let expected = r#"
proxy:
  addr: hello
containerRuntime:
  containerd:
    configPath: /root/.dragonfly/config/dfinit/yaml
    registries: []"#;
        assert_eq!(expected.trim(), res.trim());
    }

    #[test]
    fn deserialize_container_runtime() {
        let raw_data = r#"
            proxy: 
                addr: "hello"
        "#;
        let cfg: Config = serde_yaml::from_str(&raw_data).unwrap();
        assert!(cfg.container_runtime.config.is_none());
        assert_eq!("hello".to_string(), cfg.proxy.addr);

        let raw_data = r#"
            proxy: 
                addr: "hello"
            containerRuntime:
                containerd:
                    configPath: "test_path"
        "#;
        let cfg: Config = serde_yaml::from_str(&raw_data).unwrap();
        assert!(cfg.container_runtime.config.is_some());
        match cfg.container_runtime.config {
            Some(ContainerRuntimeConfig::Containerd(c)) => {
                assert_eq!(PathBuf::from("test_path"), c.config_path);
            }
            _ => assert!(false)
        }
        assert_eq!("hello".to_string(), cfg.proxy.addr);
    }
}
