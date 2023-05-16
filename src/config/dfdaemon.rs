/*
 *     Copyright 2023 The Dragonfly Authors
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

use crate::config::{
    default_cache_dir, default_config_dir, default_data_dir, default_lock_dir, default_log_dir,
    default_plugin_dir, default_root_dir,
};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use tracing::info;
use validator::Validate;

// NAME is the name of dfdaemon.
pub const NAME: &str = "dfdaemon";

// default_dfdaemon_config_path is the default config path for dfdaemon.
pub fn default_dfdaemon_config_path() -> PathBuf {
    default_config_dir().join("dfdaemon.yaml")
}

// default_dfdaemon_log_dir is the default log directory for dfdaemon.
pub fn default_dfdaemon_log_dir() -> PathBuf {
    default_log_dir().join(NAME)
}

// default_dfdaemon_content_dir is the default content directory for dfdaemon.
pub fn default_dfdaemon_content_dir() -> PathBuf {
    default_data_dir().join(NAME).join("content")
}

// default_dfdaemon_metadata_dir is the default metadata directory for dfdaemon.
pub fn default_dfdaemon_metadata_dir() -> PathBuf {
    default_data_dir().join(NAME).join("metadata")
}

// default_dfdaemon_plugin_dir is the default plugin directory for dfdaemon.
pub fn default_dfdaemon_plugin_dir() -> PathBuf {
    default_plugin_dir().join(NAME)
}

// default_dfdaemon_cache_dir is the default cache directory for dfdaemon.
pub fn default_dfdaemon_cache_dir() -> PathBuf {
    default_cache_dir().join(NAME)
}

// default_dfdaemon_unix_socket_path is the default unix socket path for dfdaemon GRPC service.
pub fn default_dfdaemon_unix_socket_path() -> PathBuf {
    default_root_dir().join("dfdaemon.sock")
}

// default_dfdaemon_lock_path is the default file lock path for dfdaemon service.
pub fn default_dfdaemon_lock_path() -> PathBuf {
    default_lock_dir().join("dfdaemon.lock")
}

// Error is the error for Config.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    IO(#[from] std::io::Error),

    #[error(transparent)]
    YAML(#[from] serde_yaml::Error),
}

// Config is the configuration for dfdaemon.
#[derive(Debug, Clone, Validate, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Config {
    // data_dir is the directory to store task's metadata and content.
    pub data_dir: PathBuf,

    // plugin_dir is the directory to store plugins.
    pub plugin_dir: PathBuf,

    // cache_dir is the directory to store cache files.
    pub cache_dir: PathBuf,

    // root_dir is the root directory for dfdaemon.
    pub root_dir: PathBuf,

    // lock_path is the file lock path for dfdaemon service.
    pub lock_dir: PathBuf,
}

// Default implements default value for Config.
impl Default for Config {
    fn default() -> Self {
        Config {
            data_dir: default_dfdaemon_metadata_dir(),
            plugin_dir: default_dfdaemon_plugin_dir(),
            cache_dir: default_dfdaemon_cache_dir(),
            root_dir: default_root_dir(),
            lock_dir: default_lock_dir(),
        }
    }
}

// Config implements Config.
impl Config {
    // load loads configuration from file.
    pub fn load(path: &PathBuf) -> Result<Config, Error> {
        if path.exists() {
            let content = fs::read_to_string(path)?;
            let config: Config = serde_yaml::from_str(&content)?;
            info!("load config from {}", path.display());
            Ok(config)
        } else {
            info!(
                "config file {} not found, use default config",
                path.display()
            );
            Ok(Self::default())
        }
    }
}
