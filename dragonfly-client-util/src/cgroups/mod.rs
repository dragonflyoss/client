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
    let content = fs::read_to_string(format!("/proc/{pid}/cgroup"))?;
    let relative_path = parse_cgroup_v2_relative_path(&content, pid)?;
    Ok(PathBuf::from("/sys/fs/cgroup").join(relative_path))
}

/// Extracts the cgroup v2 relative path from the content of `/proc/{pid}/cgroup`.
///
/// On a pure cgroup v2 host the file holds a single `0::<path>` line. A cgroup v1 named
/// hierarchy can coexist with the unified hierarchy (for example `1:name=systemd:/`, which
/// lingers on a host after a container mounted one), and the kernel lists such hierarchies
/// before the `0::` entry. The unified-hierarchy line is therefore located by its `0::`
/// prefix instead of being assumed to be the first line.
///
/// # Arguments
/// * `content` - The content of `/proc/{pid}/cgroup`
/// * `pid` - The process ID, used for error messages only
///
/// # Returns
/// * `Ok(String)` - The cgroup v2 path relative to `/sys/fs/cgroup`, without a leading `/`
/// * `Err(Error)` - If the content is empty or has no cgroup v2 entry
fn parse_cgroup_v2_relative_path(content: &str, pid: u32) -> Result<String> {
    if content.lines().all(|line| line.trim().is_empty()) {
        return Err(Error::ValidationError(format!(
            "No cgroup information found for PID {pid}"
        )));
    }

    // Extract the cgroup v2 relative path by removing the "0::" prefix and leading "/".
    content
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .map(|path| path.trim_start_matches('/').to_string())
        .ok_or_else(|| Error::ValidationError(format!("Cgroup v2 not found for PID {pid}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cgroup_v2_relative_path_unified_hierarchy_only() {
        let content = "0::/kubepods.slice/kubepods-pod1.slice/cri-containerd-abc.scope\n";
        assert_eq!(
            parse_cgroup_v2_relative_path(content, 1).unwrap(),
            "kubepods.slice/kubepods-pod1.slice/cri-containerd-abc.scope"
        );
    }

    #[test]
    fn test_parse_cgroup_v2_relative_path_root_cgroup() {
        assert_eq!(parse_cgroup_v2_relative_path("0::/\n", 1).unwrap(), "");
    }

    #[test]
    fn test_parse_cgroup_v2_relative_path_with_named_v1_hierarchy() {
        // A lingering cgroup v1 named hierarchy is listed before the unified hierarchy.
        let content = "1:name=systemd:/\n0::/kubepods.slice/cri-containerd-abc.scope\n";
        assert_eq!(
            parse_cgroup_v2_relative_path(content, 1).unwrap(),
            "kubepods.slice/cri-containerd-abc.scope"
        );
    }

    #[test]
    fn test_parse_cgroup_v2_relative_path_v1_only() {
        let content = "12:memory:/foo\n1:name=systemd:/\n";
        match parse_cgroup_v2_relative_path(content, 42).unwrap_err() {
            Error::ValidationError(msg) => assert_eq!(msg, "Cgroup v2 not found for PID 42"),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_parse_cgroup_v2_relative_path_empty() {
        match parse_cgroup_v2_relative_path("", 42).unwrap_err() {
            Error::ValidationError(msg) => {
                assert_eq!(msg, "No cgroup information found for PID 42")
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
