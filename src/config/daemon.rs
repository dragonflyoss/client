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

// default_daemon_unix_socket_path is the default unix socket path for daemon GRPC service.
pub fn default_daemon_unix_socket_path() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/var/run/dragonfly/daemon.socket");

    #[cfg(target_os = "macos")]
    return home::home_dir()
        .unwrap()
        .join(".dragonfly")
        .join("daemon.sock");
}

// default_daemon_lock_path is the default file lock path for daemon service.
pub fn default_daemon_lock_path() -> PathBuf {
    #[cfg(target_os = "linux")]
    return PathBuf::from("/var/lock/dragonfly/daemon.lock");

    #[cfg(target_os = "macos")]
    return home::home_dir()
        .unwrap()
        .join(".dragonfly")
        .join("daemon.lock");
}
