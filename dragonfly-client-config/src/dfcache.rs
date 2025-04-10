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

/// NAME is the name of dfcache.
pub const NAME: &str = "dfcache";

// DEFAULT_OUTPUT_FILE_MODE defines the default file mode for output files when downloading with dfcache
// using the `--transfer-from-dfdaemon=true` option.
pub const DEFAULT_OUTPUT_FILE_MODE: u32 = 0o644;

/// default_dfcache_log_dir is the default log directory for dfcache.
#[inline]
pub fn default_dfcache_log_dir() -> PathBuf {
    crate::default_log_dir().join(NAME)
}

/// default_dfcache_persistent_replica_count is the default replica count of the persistent cache task.
#[inline]
pub fn default_dfcache_persistent_replica_count() -> u64 {
    2
}

#[cfg(test)]
mod tests {
    use super::*;
    use home;
    use std::env::consts::OS;
    use std::path::PathBuf;

    #[test]
    fn test_default_dfcache_log_dir() {
        let log_dir: PathBuf = default_dfcache_log_dir();

        let expected: PathBuf = if OS == "linux" {
            PathBuf::from("/var/log/dragonfly/dfcache")
        } else if OS == "macos" {
            let mut path: PathBuf = home::home_dir().unwrap();
            path.push(".dragonfly/logs/dfcache");
            path
        } else {
            panic!("Unsupported OS: {OS}")
        };

        assert_eq!(
            log_dir, expected,
            "Log directory does not match expected path for {}",
            OS
        );
    }

    #[test]
    fn default_persistent_replica_count() {
        assert_eq!(default_dfcache_persistent_replica_count(), 2);
    }
}
