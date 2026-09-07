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
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::Result;
use serde::{ser::SerializeStruct, Deserialize, Serialize};
use std::fs;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use tracing::{info, instrument};
use validator::Validate;

/// The name of dfinit.
pub const NAME: &str = "dfinit";

/// Returns the default config path for dfinit.
#[inline]
pub fn default_dfinit_config_path() -> PathBuf {
    crate::default_config_dir().join("dfinit.yaml")
}

/// Returns the default containerd configuration path.
#[inline]
fn default_container_runtime_containerd_config_path() -> PathBuf {
    PathBuf::from("/etc/containerd/config.toml")
}

/// Returns the default docker configuration path.
#[inline]
fn default_container_runtime_docker_config_path() -> PathBuf {
    PathBuf::from("/etc/docker/daemon.json")
}

/// Returns the default cri-o configuration path.
#[inline]
fn default_container_runtime_crio_config_path() -> PathBuf {
    PathBuf::from("/etc/containers/registries.conf")
}

/// Returns the default podman configuration path.
#[inline]
fn default_container_runtime_podman_config_path() -> PathBuf {
    PathBuf::from("/etc/containers/registries.conf")
}

/// Returns the default unqualified search registries of cri-o,
/// refer to https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#global-settings.
#[inline]
fn default_container_runtime_crio_unqualified_search_registries() -> Vec<String> {
    vec![
        "registry.fedoraproject.org".to_string(),
        "registry.access.redhat.com".to_string(),
        "docker.io".to_string(),
    ]
}

/// Returns the default unqualified search registries of podman,
/// refer to https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#global-settings.
#[inline]
fn default_container_runtime_podman_unqualified_search_registries() -> Vec<String> {
    vec![
        "registry.fedoraproject.org".to_string(),
        "registry.access.redhat.com".to_string(),
        "docker.io".to_string(),
    ]
}

/// Returns the default proxy address of dfdaemon.
#[inline]
fn default_proxy_addr() -> String {
    format!(
        "http://{}:{}",
        Ipv4Addr::LOCALHOST,
        default_proxy_server_port()
    )
}

/// Returns the default capabilities of the containerd registry.
#[inline]
pub fn default_container_runtime_containerd_registry_capabilities() -> Vec<String> {
    vec!["pull".to_string(), "resolve".to_string()]
}

/// Returns the default value of whether to proxy all registries through dfdaemon via a
/// catch-all `_default/hosts.toml`.
#[inline]
fn default_container_runtime_containerd_proxy_all_registries() -> bool {
    true
}

/// The registry configuration for containerd.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ContainerdRegistry {
    /// The location where container images and artifacts are sourced,
    /// refer to https://github.com/containerd/containerd/blob/main/docs/hosts.md#registry-host-namespace.
    /// The registry host namespace portion is [registry_host_name|IP address][:port], such as
    /// docker.io, ghcr.io, gcr.io, etc.
    pub host_namespace: String,

    /// Specifies the default server for this registry host namespace, refer to
    /// https://github.com/containerd/containerd/blob/main/docs/hosts.md#server-field.
    pub server_addr: String,

    /// The list of capabilities in containerd configuration, refer to
    /// https://github.com/containerd/containerd/blob/main/docs/hosts.md#capabilities-field.
    #[serde(default = "default_container_runtime_containerd_registry_capabilities")]
    pub capabilities: Vec<String>,

    /// The flag to skip verifying the server's certificate, refer to
    /// https://github.com/containerd/containerd/blob/main/docs/hosts.md#bypass-tls-verification-example.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skip_verify: Option<bool>,

    /// The Certificate Authority Certification, which can be set to a path or an array of paths each pointing
    /// to a ca file for use in authenticating with the registry namespace, refer to
    /// https://github.com/containerd/containerd/blob/main/docs/hosts.md#ca-field.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ca: Option<Vec<String>>,
}

/// The containerd configuration for dfinit.
#[derive(Debug, Clone, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Containerd {
    /// The path of containerd configuration file.
    #[serde(default = "default_container_runtime_containerd_config_path")]
    pub config_path: PathBuf,

    /// The CRI plugin id owning the registry configuration, e.g. "io.containerd.grpc.v1.cri"
    /// or "io.containerd.cri.v1.images". If not set, the plugin table present in the
    /// containerd configuration is used, preferring "io.containerd.grpc.v1.cri" for
    /// version 2 configurations and "io.containerd.cri.v1.images" for version 3.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cri_plugin_id: Option<String>,

    /// The list of containerd registries.
    pub registries: Vec<ContainerdRegistry>,

    /// Enables a catch-all `_default/hosts.toml` entry so that any
    /// registry not explicitly listed in `registries` is still proxied through dfdaemon.
    /// The dfdaemon infers the upstream registry from the `ns=` query parameter that
    /// containerd appends when using a `_default` fallback mirror. Explicitly configured
    /// registries continue to use their own `hosts.toml` and take precedence.
    #[serde(default = "default_container_runtime_containerd_proxy_all_registries")]
    pub proxy_all_registries: bool,
}

impl Default for Containerd {
    fn default() -> Self {
        Self {
            config_path: PathBuf::default(),
            cri_plugin_id: None,
            registries: Vec::default(),
            proxy_all_registries: default_container_runtime_containerd_proxy_all_registries(),
        }
    }
}

/// The registry configuration for cri-o.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct CRIORegistry {
    /// The prefix of the user-specified image name, refer to
    /// https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#choosing-a-registry-toml-table.
    pub prefix: String,

    /// Accepts the same format as the prefix field, and specifies the physical location of the prefix-rooted namespace,
    /// refer to https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#remapping-and-mirroring-registries.
    pub location: String,
}

/// The cri-o configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CRIO {
    /// The path of cri-o registries's configuration file.
    #[serde(default = "default_container_runtime_crio_config_path")]
    pub config_path: PathBuf,

    /// An array of host[:port] registries to try when pulling an unqualified image, in order.
    /// Refer to https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#global-settings.
    #[serde(default = "default_container_runtime_crio_unqualified_search_registries")]
    pub unqualified_search_registries: Vec<String>,

    /// The list of cri-o registries, refer to
    /// https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#namespaced-registry-settings.
    pub registries: Vec<CRIORegistry>,
}

/// The registry configuration for podman.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize, PartialEq, Eq)]
#[serde(default, rename_all = "camelCase")]
pub struct PodmanRegistry {
    /// The prefix of the user-specified image name, refer to
    /// https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#choosing-a-registry-toml-table.
    pub prefix: String,

    /// Accepts the same format as the prefix field, and specifies the physical location of the prefix-rooted namespace,
    /// refer to https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#remapping-and-mirroring-registries.
    pub location: String,
}

/// The podman configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Podman {
    /// The path of podman registries's configuration file.
    #[serde(default = "default_container_runtime_podman_config_path")]
    pub config_path: PathBuf,

    /// An array of host[:port] registries to try when pulling an unqualified image, in order.
    /// Refer to https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#global-settings.
    #[serde(default = "default_container_runtime_podman_unqualified_search_registries")]
    pub unqualified_search_registries: Vec<String>,

    /// The list of podman registries, refer to
    /// https://github.com/containers/image/blob/main/docs/containers-registries.conf.5.md#namespaced-registry-settings.
    pub registries: Vec<PodmanRegistry>,
}

/// The docker configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Docker {
    /// The path of docker configuration file.
    #[serde(default = "default_container_runtime_docker_config_path")]
    pub config_path: PathBuf,
}

/// The container runtime configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ContainerRuntime {
    #[serde(flatten)]
    pub config: Option<ContainerRuntimeConfig>,
}

/// The container runtime configuration for dfinit.
#[derive(Debug, Clone)]
pub enum ContainerRuntimeConfig {
    Containerd(Containerd),
    Docker(Docker),
    CRIO(CRIO),
    Podman(Podman),
}

/// Implement Serialize for ContainerRuntimeConfig.
impl Serialize for ContainerRuntimeConfig {
    fn serialize<S>(&self, serializer: S) -> std::prelude::v1::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
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
            ContainerRuntimeConfig::CRIO(ref cfg) => {
                let mut state = serializer.serialize_struct("crio", 1)?;
                state.serialize_field("crio", &cfg)?;
                state.end()
            }
            ContainerRuntimeConfig::Podman(ref cfg) => {
                let mut state = serializer.serialize_struct("podman", 1)?;
                state.serialize_field("podman", &cfg)?;
                state.end()
            }
        }
    }
}

/// Implement Deserialize for ContainerRuntimeConfig.
impl<'de> Deserialize<'de> for ContainerRuntimeConfig {
    fn deserialize<D>(deserializer: D) -> std::prelude::v1::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ContainerRuntimeHelper {
            containerd: Option<Containerd>,
            docker: Option<Docker>,
            crio: Option<CRIO>,
            podman: Option<Podman>,
        }

        let helper = ContainerRuntimeHelper::deserialize(deserializer)?;
        match helper {
            ContainerRuntimeHelper {
                containerd: Some(containerd),
                ..
            } => Ok(ContainerRuntimeConfig::Containerd(containerd)),
            ContainerRuntimeHelper {
                docker: Some(docker),
                ..
            } => Ok(ContainerRuntimeConfig::Docker(docker)),
            ContainerRuntimeHelper {
                crio: Some(crio), ..
            } => Ok(ContainerRuntimeConfig::CRIO(crio)),
            ContainerRuntimeHelper {
                podman: Some(podman),
                ..
            } => Ok(ContainerRuntimeConfig::Podman(podman)),
            _ => {
                use serde::de::Error;
                Err(D::Error::custom(
                    "expected containerd or docker or crio or podman",
                ))
            }
        }
    }
}

/// The proxy server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Proxy {
    /// The proxy server address of dfdaemon.
    #[serde(default = "default_proxy_addr")]
    pub addr: String,
}

/// Implement Default for Proxy.
impl Default for Proxy {
    fn default() -> Self {
        Self {
            addr: default_proxy_addr(),
        }
    }
}

/// The configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize, Serialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    /// The configuration of the dfdaemon's HTTP/HTTPS proxy.
    #[validate]
    pub proxy: Proxy,

    /// The container runtime configuration.
    #[validate]
    pub container_runtime: ContainerRuntime,
}

/// Implement the config operation of dfinit.
impl Config {
    /// Loads configuration from file.
    #[instrument(skip_all)]
    pub fn load(path: &PathBuf) -> Result<Config> {
        // Load configuration from file.
        let content = fs::read_to_string(path)?;
        let config: Config = serde_yaml::from_str(&content).or_err(ErrorType::ConfigError)?;
        info!("load config from {}", path.display());

        // Validate configuration.
        config.validate().or_err(ErrorType::ValidationError)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_client_core::Error;
    use std::path::Path;

    type ExpectConfig = fn(&Config);
    type ExpectLoad = fn(Result<Config>);

    fn config(runtime_config: Option<ContainerRuntimeConfig>) -> Config {
        Config {
            proxy: Proxy {
                addr: "hello".to_string(),
            },
            container_runtime: ContainerRuntime {
                config: runtime_config,
            },
        }
    }

    fn containerd(config: &Config) -> &Containerd {
        let Some(ContainerRuntimeConfig::Containerd(containerd)) = &config.container_runtime.config
        else {
            unreachable!()
        };
        containerd
    }

    fn docker(config: &Config) -> &Docker {
        let Some(ContainerRuntimeConfig::Docker(docker)) = &config.container_runtime.config else {
            unreachable!()
        };
        docker
    }

    fn crio(config: &Config) -> &CRIO {
        let Some(ContainerRuntimeConfig::CRIO(crio)) = &config.container_runtime.config else {
            unreachable!()
        };
        crio
    }

    fn podman(config: &Config) -> &Podman {
        let Some(ContainerRuntimeConfig::Podman(podman)) = &config.container_runtime.config else {
            unreachable!()
        };
        podman
    }

    #[test]
    fn default_dfinit_config_path_joins_config_dir() {
        assert_eq!(
            default_dfinit_config_path(),
            crate::default_config_dir().join("dfinit.yaml")
        );
    }

    #[test]
    fn config_serializes_container_runtime_under_runtime_key() {
        let test_cases = vec![
            (
                None,
                r#"
proxy:
  addr: hello
containerRuntime: {}"#,
            ),
            (
                Some(ContainerRuntimeConfig::Containerd(Containerd::default())),
                r#"
proxy:
  addr: hello
containerRuntime:
  containerd:
    configPath: ''
    registries: []
    proxyAllRegistries: true"#,
            ),
            (
                Some(ContainerRuntimeConfig::Containerd(Containerd {
                    config_path: PathBuf::from("/root/.dragonfly/config/dfinit/yaml"),
                    ..Default::default()
                })),
                r#"
proxy:
  addr: hello
containerRuntime:
  containerd:
    configPath: /root/.dragonfly/config/dfinit/yaml
    registries: []
    proxyAllRegistries: true"#,
            ),
            (
                Some(ContainerRuntimeConfig::Docker(Docker {
                    config_path: PathBuf::from("/root/.dragonfly/config/dfinit/yaml"),
                })),
                r#"
proxy:
  addr: hello
containerRuntime:
  docker:
    configPath: /root/.dragonfly/config/dfinit/yaml"#,
            ),
            (
                Some(ContainerRuntimeConfig::CRIO(CRIO::default())),
                r#"
proxy:
  addr: hello
containerRuntime:
  crio:
    configPath: ''
    unqualifiedSearchRegistries: []
    registries: []"#,
            ),
            (
                Some(ContainerRuntimeConfig::Podman(Podman::default())),
                r#"
proxy:
  addr: hello
containerRuntime:
  podman:
    configPath: ''
    unqualifiedSearchRegistries: []
    registries: []"#,
            ),
        ];

        for (runtime_config, expected) in test_cases {
            let config = config(runtime_config);
            let yaml = serde_yaml::to_string(&config).unwrap();
            assert_eq!(yaml.trim(), expected.trim());
        }
    }

    #[test]
    fn config_deserializes_container_runtime_variant() {
        let test_cases: Vec<(&str, ExpectConfig)> = vec![
            ("{}", |config| {
                assert_eq!(config.proxy.addr, "http://127.0.0.1:4001");
                assert!(config.container_runtime.config.is_none());
            }),
            ("proxy:\n  addr: hello\n", |config| {
                assert_eq!(config.proxy.addr, "hello");
                assert!(config.container_runtime.config.is_none());
            }),
            ("containerRuntime:\n  unknown: {}\n", |config| {
                assert!(config.container_runtime.config.is_none());
            }),
            (
                r#"
                proxy:
                  addr: hello
                containerRuntime:
                  containerd:
                    configPath: test_path
                    criPluginId: io.containerd.cri.v1.images
                    proxyAllRegistries: false
                    registries:
                      - hostNamespace: docker.io
                        serverAddr: https://index.docker.io
                        skipVerify: true
                        ca:
                          - /etc/ssl/certs/ca.crt
                      - hostNamespace: ghcr.io
                        serverAddr: https://ghcr.io
                        capabilities:
                          - pull
                "#,
                |config| {
                    assert_eq!(config.proxy.addr, "hello");
                    let containerd = containerd(config);
                    assert_eq!(containerd.config_path, PathBuf::from("test_path"));
                    assert_eq!(
                        containerd.cri_plugin_id,
                        Some("io.containerd.cri.v1.images".to_string())
                    );
                    assert!(!containerd.proxy_all_registries);
                    assert_eq!(containerd.registries.len(), 2);
                    assert_eq!(containerd.registries[0].host_namespace, "docker.io");
                    assert_eq!(
                        containerd.registries[0].server_addr,
                        "https://index.docker.io"
                    );
                    assert_eq!(
                        containerd.registries[0].capabilities,
                        vec!["pull", "resolve"]
                    );
                    assert_eq!(containerd.registries[0].skip_verify, Some(true));
                    assert_eq!(
                        containerd.registries[0].ca,
                        Some(vec!["/etc/ssl/certs/ca.crt".to_string()])
                    );
                    assert_eq!(containerd.registries[1].capabilities, vec!["pull"]);
                    assert!(containerd.registries[1].skip_verify.is_none());
                    assert!(containerd.registries[1].ca.is_none());
                },
            ),
            ("containerRuntime:\n  containerd: {}\n", |config| {
                let containerd = containerd(config);
                assert_eq!(
                    containerd.config_path,
                    Path::new("/etc/containerd/config.toml")
                );
                assert!(containerd.cri_plugin_id.is_none());
                assert!(containerd.registries.is_empty());
                assert!(containerd.proxy_all_registries);
            }),
            (
                "containerRuntime:\n  docker:\n    configPath: test_path\n",
                |config| {
                    assert_eq!(docker(config).config_path, PathBuf::from("test_path"));
                },
            ),
            ("containerRuntime:\n  docker: {}\n", |config| {
                assert_eq!(
                    docker(config).config_path,
                    Path::new("/etc/docker/daemon.json")
                );
            }),
            (
                r#"
                containerRuntime:
                  crio:
                    configPath: test_path
                    unqualifiedSearchRegistries:
                      - reg1
                      - reg2
                    registries:
                      - prefix: prefix1
                        location: location1
                      - prefix: prefix2
                        location: location2
                "#,
                |config| {
                    let crio = crio(config);
                    assert_eq!(crio.config_path, PathBuf::from("test_path"));
                    assert_eq!(crio.unqualified_search_registries, vec!["reg1", "reg2"]);
                    assert_eq!(
                        crio.registries,
                        vec![
                            CRIORegistry {
                                prefix: "prefix1".to_string(),
                                location: "location1".to_string(),
                            },
                            CRIORegistry {
                                prefix: "prefix2".to_string(),
                                location: "location2".to_string(),
                            },
                        ]
                    );
                },
            ),
            ("containerRuntime:\n  crio: {}\n", |config| {
                let crio = crio(config);
                assert_eq!(
                    crio.config_path,
                    Path::new("/etc/containers/registries.conf")
                );
                assert_eq!(
                    crio.unqualified_search_registries,
                    vec![
                        "registry.fedoraproject.org",
                        "registry.access.redhat.com",
                        "docker.io"
                    ]
                );
                assert!(crio.registries.is_empty());
            }),
            (
                r#"
                containerRuntime:
                  podman:
                    configPath: test_path
                    unqualifiedSearchRegistries:
                      - reg1
                      - reg2
                    registries:
                      - prefix: prefix1
                        location: location1
                      - prefix: prefix2
                        location: location2
                "#,
                |config| {
                    let podman = podman(config);
                    assert_eq!(podman.config_path, PathBuf::from("test_path"));
                    assert_eq!(podman.unqualified_search_registries, vec!["reg1", "reg2"]);
                    assert_eq!(
                        podman.registries,
                        vec![
                            PodmanRegistry {
                                prefix: "prefix1".to_string(),
                                location: "location1".to_string(),
                            },
                            PodmanRegistry {
                                prefix: "prefix2".to_string(),
                                location: "location2".to_string(),
                            },
                        ]
                    );
                },
            ),
            ("containerRuntime:\n  podman: {}\n", |config| {
                let podman = podman(config);
                assert_eq!(
                    podman.config_path,
                    Path::new("/etc/containers/registries.conf")
                );
                assert_eq!(
                    podman.unqualified_search_registries,
                    vec![
                        "registry.fedoraproject.org",
                        "registry.access.redhat.com",
                        "docker.io"
                    ]
                );
                assert!(podman.registries.is_empty());
            }),
        ];

        for (yaml, expect) in test_cases {
            let config: Config = serde_yaml::from_str(yaml).unwrap();
            expect(&config);
        }
    }

    #[test]
    fn load_reads_file_and_wraps_parse_errors() {
        let test_cases: Vec<(&str, ExpectLoad)> = vec![
            (
                "containerRuntime:\n  docker:\n    configPath: test_path\n",
                |result| {
                    let config = result.unwrap();
                    assert_eq!(docker(&config).config_path, PathBuf::from("test_path"));
                },
            ),
            ("containerRuntime: [", |result| {
                assert!(
                    matches!(result, Err(Error::ExternalError(ref err)) if err.etype == ErrorType::ConfigError)
                );
            }),
        ];

        for (content, expect) in test_cases {
            let file = tempfile::NamedTempFile::new().unwrap();
            fs::write(file.path(), content).unwrap();
            expect(Config::load(&file.path().to_path_buf()));
        }
    }
}
