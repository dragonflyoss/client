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

use std::fs;

/// Detects if the current process is running inside a container environment.
///
/// # Detection Methods
/// This function uses multiple heuristics to detect various container runtimes:
/// - Docker
/// - containerd
/// - LXC/LXD
/// - Kubernetes pods
///
/// # Returns
/// `true` if running inside a container, `false` otherwise.
///
/// # Note
/// This detection is best-effort and may not catch all container environments,
/// especially heavily customized ones.
pub fn is_running_in_container() -> bool {
    // Check for the presence of the Docker environment file.
    if std::path::Path::new("/.dockerenv").exists() {
        return true;
    }

    // Check cgroup information for container indicators.
    if let Ok(cgroup) = fs::read_to_string("/proc/self/cgroup") {
        if cgroup.contains("/docker/")
            || cgroup.contains("/containerd/")
            || cgroup.contains("/podman/")
            || cgroup.contains("/lxc/")
            || cgroup.contains("/kubepods/")
        {
            return true;
        }
    }

    // Check for common container-related environment variables.
    const CONTAINER_ENV_VARS: &[&str] = &[
        "KUBERNETES_SERVICE_HOST",
        "DOCKER_CONTAINER",
        "container",
        "PODMAN_CONTAINER",
    ];

    if CONTAINER_ENV_VARS
        .iter()
        .any(|var| std::env::var(var).is_ok())
    {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_running_in_container() {
        let _ = is_running_in_container();
    }
}
