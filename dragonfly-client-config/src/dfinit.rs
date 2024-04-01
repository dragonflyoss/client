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
use serde::Deserialize;
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

// default_proxy_addr is the default proxy address of dfdaemon.
#[inline]
fn default_proxy_addr() -> String {
    format!(
        "http://{}:{}",
        Ipv4Addr::LOCALHOST,
        default_proxy_server_port()
    )
}

// Host is the host configuration for registry.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Host {
    // host is the mirror host.
    pub host: String,
}

// Registry is the registry configuration for containerd.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Registry {
    // host_namespace is the location where container images and artifacts are sourced,
    // refer to https://github.com/containerd/containerd/blob/main/docs/hosts.md#registry-host-namespace.
    // The registry host namespace portion is [registry_host_name|IP address][:port], such as
    // docker.io, ghcr.io, gcr.io, etc.
    pub host_namespace: String,

    // hosts is the list of mirror hosts. When host(s) are specified,
    // the hosts will be tried first in the order listed, refer to
    // https://github.com/containerd/containerd/blob/main/docs/hosts.md#registry-configuration---examples.
    pub hosts: Vec<Host>,
}

// Containerd is the containerd configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Containerd {
    // enable is a flag to enable containerd feature.
    pub enable: bool,

    // config_path is the path of containerd configuration file.
    #[serde(default = "default_container_runtime_containerd_config_path")]
    pub config_path: PathBuf,

    // registries is the list of containerd registries.
    pub registries: Vec<Registry>,
}

// ContainerRuntime is the container runtime configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ContainerRuntime {
    // containerd is the containerd configuration.
    pub containerd: Containerd,
}

// Proxy is the proxy server configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
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
#[derive(Debug, Clone, Default, Validate, Deserialize)]
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
