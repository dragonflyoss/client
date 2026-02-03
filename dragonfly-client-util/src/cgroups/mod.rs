/*
 *     Copyright 2026 The Dragonfly Authors
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

use cgroups_rs::fs::{cgroup::get_cgroups_relative_paths_by_pid, hierarchies, Cgroup};
use dragonfly_client_core::{Error, Result};
use std::fs;
use std::path::{Path, PathBuf};

/// Retrieves the cgroup associated with a given process ID, supporting both cgroup v1 and v2.
///
/// This function fetches the cgroup path for the specified PID
/// and loads the corresponding cgroup hierarchy automatically.
///
/// # Arguments
///
/// * `pid` - The process ID to look up cgroups for
///
/// # Returns
///
/// * `Ok(Cgroup)` - The cgroup associated with the process
/// * `Err` - If the process doesn't exist or cgroup lookup fails
///
/// # Examples
///
/// ```rust
/// use cgroups_rs::get_cgroup_by_pid;
///
/// // Get cgroup for the current process
/// let cgroup = get_cgroup_by_pid(std::process::id()).unwrap();
/// ```
///
/// # Errors
///
/// Returns an error if:
/// - The process with the given PID does not exist
/// - Permission denied when reading cgroup information
/// - The cgroup filesystem is not mounted
pub fn get_cgroup_by_pid(pid: u32) -> Result<Cgroup> {
    // Automatically detect the cgroup hierarchy (v1 or v2)
    // and load the cgroup configuration for the specified PID.
    let hierarchies = hierarchies::auto();
    if hierarchies.v2() {
        let path = get_cgroup_v2_path_by_pid(pid)?;
        Ok(Cgroup::load(hierarchies, path))
    } else {
        let relative_paths = get_cgroups_relative_paths_by_pid(pid)?;
        Ok(Cgroup::load_with_relative_paths(
            hierarchies::auto(),
            Path::new("."),
            relative_paths,
        ))
    }
}

/// Retrieves the cgroup v2 filesystem path for a given process ID.
///
/// This function reads the cgroup information from `/proc/{pid}/cgroup` and
/// constructs the absolute path to the cgroup v2 directory in `/sys/fs/cgroup`.
///
/// # Arguments
/// * `pid` - The process ID to query
///
/// # Returns
/// * `Ok(PathBuf)` - The absolute path to the cgroup v2 directory
/// * `Err(Error)` - If the process doesn't exist or cgroup v2 is not available
///
/// # Example
/// ```
/// let cgroup_path = get_cgroup_v2_path_by_pid(1)?;
/// // Returns something like: /sys/fs/cgroup/system.slice/service.scope
/// ```
pub fn get_cgroup_v2_path_by_pid(pid: u32) -> Result<PathBuf> {
    let content = fs::read_to_string(format!("/proc/{}/cgroup", pid))?;
    let first_line = content.lines().next().ok_or_else(|| {
        Error::ValidationError(format!("No cgroup information found for PID {}", pid))
    })?;

    // Extract the cgroup v2 relative path by removing "0::" prefix and leading "/".
    let relative_path = first_line
        .strip_prefix("0::")
        .ok_or_else(|| Error::ValidationError(format!("Cgroup v2 not found for PID {}", pid)))?
        .trim_start_matches('/');
    Ok(PathBuf::from("/sys/fs/cgroup").join(relative_path))
}
