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

use std::path::PathBuf;

pub mod daemon;
pub mod dfget;
pub mod dfstore;

// default_config_dir is the default config directory for dfdaemon.
pub fn default_config_dir() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/etc/dragonfly/");

    #[cfg(target_os = "macos")]
    return home::home_dir().unwrap().join(".dragonfly").join("config");
}

// default_log_dir is the default log directory for dfdaemon.
pub fn default_log_dir() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/var/log/dragonfly/");

    #[cfg(target_os = "macos")]
    return home::home_dir().unwrap().join(".dragonfly").join("logs");
}

// default_data_dir is the default data directory for dfdaemon.
pub fn default_data_dir() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/var/lib/dragonfly/");

    #[cfg(target_os = "macos")]
    return home::home_dir().unwrap().join(".dragonfly").join("data");
}

// default_plugin_dir is the default plugin directory for dfdaemon.
pub fn default_plugin_dir() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/usr/local/dragonfly/plugins/");

    #[cfg(target_os = "macos")]
    return home::home_dir().unwrap().join(".dragonfly").join("plugins");
}

// default_cache_dir is the default cache directory for dfdaemon.
pub fn default_cache_dir() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/var/cache/dragonfly/");

    #[cfg(target_os = "macos")]
    return home::home_dir().unwrap().join(".dragonfly").join("cache");
}
