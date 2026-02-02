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

use sysinfo::{MemoryRefreshKind, Pid, ProcessRefreshKind, RefreshKind, System};

/// Memory represents a memory interface for monitoring memory statistics.
#[derive(Debug, Clone, Default)]
pub struct Memory {}

/// Represents system-wide memory statistics.
#[derive(Debug, Clone, Default)]
pub struct MemoryStats {
    /// Total physical memory in bytes.
    pub total: u64,

    /// Free memory in bytes (does not include cached/buffered memory).
    pub free: u64,

    /// Available memory in bytes (includes reclaimable cache/buffers).
    pub available: u64,

    /// Used memory in bytes.
    pub usage: u64,

    /// Memory usage percentage (0.0 - 100.0).
    pub used_percent: f64,
}

/// Represents memory statistics for a specific process.
#[derive(Debug, Clone, Default)]
pub struct ProcessMemoryStats {
    /// Memory usage percentage of the process relative to total system memory (0.0 - 100.0).
    pub used_percent: f64,
}

/// Represents memory statistics from cgroup (Linux containers/resource limits).
#[derive(Debug, Clone, Default)]
pub struct CgroupMemoryStats {
    /// Memory limit in bytes (-1 means unlimited).
    pub limit: i64,

    /// Current memory usage in bytes.
    pub usage: u64,

    /// Memory usage percentage relative to the cgroup limit (0.0 - 100.0).
    pub used_percent: f64,
}

/// Implementation of memory monitoring functionality.
///
/// Provides methods to retrieve memory statistics at three different levels:
/// - System-wide: Overall memory usage, availability, and capacity.
/// - Process-level: Memory usage for a specific process.
/// - Cgroup-level: Memory resource limits and usage (Linux only).
impl Memory {
    /// Retrieves system-wide memory statistics.
    ///
    /// # Returns
    /// MemoryStats containing total, free, available, used memory and usage percentage.
    pub fn get_stats(&self) -> MemoryStats {
        let sys =
            System::new_with_specifics(RefreshKind::new().with_memory(MemoryRefreshKind::new()));

        MemoryStats {
            total: sys.total_memory(),
            free: sys.free_memory(),
            available: sys.available_memory(),
            usage: sys.used_memory(),
            used_percent: (sys.used_memory() as f64 / sys.total_memory() as f64) * 100.0,
        }
    }

    /// Retrieves memory statistics for the process with the given PID.
    ///
    /// # Arguments
    /// * `pid` - Process ID to query memory statistics for.
    ///
    /// # Returns
    /// ProcessMemoryStats containing the process's memory usage percentage.
    pub fn get_process_stats(&self, pid: u32) -> ProcessMemoryStats {
        let sys = System::new_with_specifics(
            RefreshKind::new().with_processes(ProcessRefreshKind::new().with_memory()),
        );

        ProcessMemoryStats {
            used_percent: (sys.process(Pid::from_u32(pid)).unwrap().memory() as f64
                / sys.total_memory() as f64)
                * 100.0,
        }
    }

    /// Retrieves memory statistics from the cgroup (Linux only).
    ///
    /// # Arguments
    /// * `pid` - Process ID used to determine which cgroup to query.
    ///
    /// # Returns
    /// Some(CgroupMemoryStats) if cgroup memory controller is available and accessible,
    /// None otherwise or on non-Linux platforms.
    #[allow(unused_variables)]
    pub fn get_cgroup_stats(&self, pid: u32) -> Option<CgroupMemoryStats> {
        #[cfg(target_os = "linux")]
        {
            use crate::cgroups::get_cgroup_by_pid;
            use crate::container::is_running_in_container;
            use cgroups_rs::fs::memory::MemController;
            use tracing::error;

            if !is_running_in_container() {
                return None;
            }

            match get_cgroup_by_pid(pid) {
                Ok(cgroup) => {
                    if let Some(memory_controller) = cgroup.controller_of::<MemController>() {
                        let memory_stats = memory_controller.memory_stats();
                        return Some(CgroupMemoryStats {
                            limit: memory_stats.limit_in_bytes,
                            usage: memory_stats.usage_in_bytes,
                            used_percent: if memory_stats.limit_in_bytes == -1 {
                                (memory_stats.usage_in_bytes as f64 / self.get_stats().total as f64)
                                    * 100.0
                            } else {
                                (memory_stats.usage_in_bytes as f64
                                    / memory_stats.limit_in_bytes as f64)
                                    * 100.0
                            },
                        });
                    }
                }
                Err(err) => {
                    error!("failed to get cgroup for pid {}: {}", pid, err);
                    return None;
                }
            }
        }

        None
    }
}
