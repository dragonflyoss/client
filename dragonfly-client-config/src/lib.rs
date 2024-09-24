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

use std::path::PathBuf;

pub mod dfcache;
pub mod dfdaemon;
pub mod dfget;
pub mod dfinit;
pub mod dfstore;

/// SERVICE_NAME is the name of the service.
pub const SERVICE_NAME: &str = "dragonfly";

/// NAME is the name of the package.
pub const NAME: &str = "client";

/// CARGO_PKG_VERSION is the version of the cargo package.
pub const CARGO_PKG_VERSION: &str = env!("CARGO_PKG_VERSION");

/// CARGO_PKG_RUSTC_VERSION is the minimum Rust version supported by the package, not the current Rust version.
pub const CARGO_PKG_RUSTC_VERSION: &str = env!("CARGO_PKG_RUST_VERSION");

/// GIT_HASH is the git hash of the package.
pub const GIT_HASH: Option<&str> = option_env!("GIT_HASH");

/// default_root_dir is the default root directory for client.
pub fn default_root_dir() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/var/run/dragonfly/");

    #[cfg(target_os = "macos")]
    return home::home_dir().unwrap().join(".dragonfly");
}

/// default_config_dir is the default config directory for client.
pub fn default_config_dir() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/etc/dragonfly/");

    #[cfg(target_os = "macos")]
    return home::home_dir().unwrap().join(".dragonfly").join("config");
}

/// default_log_dir is the default log directory for client.
pub fn default_log_dir() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/var/log/dragonfly/");

    #[cfg(target_os = "macos")]
    return home::home_dir().unwrap().join(".dragonfly").join("logs");
}

/// default_storage_dir is the default storage directory for client.
pub fn default_storage_dir() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/var/lib/dragonfly/");

    #[cfg(target_os = "macos")]
    return home::home_dir().unwrap().join(".dragonfly").join("storage");
}

/// default_lock_dir is the default lock directory for client.
pub fn default_lock_dir() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/var/lock/dragonfly/");

    #[cfg(target_os = "macos")]
    return home::home_dir().unwrap().join(".dragonfly");
}

/// default_plugin_dir is the default plugin directory for client.
pub fn default_plugin_dir() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/var/lib/dragonfly/plugins/");

    #[cfg(target_os = "macos")]
    return home::home_dir().unwrap().join(".dragonfly").join("plugins");
}

/// default_cache_dir is the default cache directory for client.
pub fn default_cache_dir() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/var/cache/dragonfly/");

    #[cfg(target_os = "macos")]
    return home::home_dir().unwrap().join(".dragonfly").join("cache");
}
