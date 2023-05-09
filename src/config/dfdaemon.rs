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
use std::path::PathBuf;

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
