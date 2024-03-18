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

use dragonfly_client_core::Result;
use serde::Deserialize;
use std::fs;
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

// Config is the configuration for dfinit.
#[derive(Debug, Clone, Default, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
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
