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

use sysinfo::{CpuRefreshKind, Pid, ProcessRefreshKind, RefreshKind, System};

/// CPU represents a cpu interface with its information.
#[derive(Debug, Clone, Default)]
pub struct CPU {}

/// Represents system-wide CPU statistics.
#[derive(Debug, Clone, Default)]
pub struct CPUStats {
    /// Number of physical CPU cores available on the system.
    pub physical_core_count: u32,

    /// Overall CPU usage percentage across all cores (0.0 - 100.0).
    pub used_percent: f64,
}

/// Represents CPU statistics for the process.
#[derive(Debug, Clone, Default)]
pub struct ProcessCPUStats {
    /// CPU usage percentage of the process (0.0 - 100.0).
    pub used_percent: f64,
}

/// Represents CPU statistics from cgroup (Linux containers/resource limits).
#[derive(Debug, Clone, Default)]
pub struct CgroupCPUStats {
    /// CFS period in microseconds.
    pub period: u64,

    /// CFS quota in microseconds (-1 means unlimited).
    pub quota: i64,
}

/// Implementation of CPU monitoring functionality.
///
/// Provides methods to retrieve CPU statistics at three different levels:
/// - System-wide: Overall CPU usage and core count.
/// - Process-level: CPU usage for a specific process.
/// - Cgroup-level: CPU resource limits and quotas (Linux only).
impl CPU {
    /// Retrieves system-wide CPU statistics.
    ///
    /// # Returns
    /// CPUStats containing physical core count and global CPU usage percentage.
    pub fn get_stats(&self) -> CPUStats {
        let sys = System::new_with_specifics(
            RefreshKind::new().with_cpu(CpuRefreshKind::new().with_cpu_usage()),
        );

        CPUStats {
            physical_core_count: sys.physical_core_count().unwrap_or_default() as u32,
            used_percent: sys.global_cpu_usage() as f64,
        }
    }

    /// Retrieves CPU statistics for the process with the given PID.
    ///
    /// # Returns
    /// ProcessCPUStats containing the current process's CPU usage.
    pub fn get_process_stats(&self, pid: u32) -> ProcessCPUStats {
        let sys = System::new_with_specifics(
            RefreshKind::new().with_processes(ProcessRefreshKind::new().with_cpu()),
        );

        ProcessCPUStats {
            used_percent: sys.process(Pid::from_u32(pid)).unwrap().cpu_usage() as f64,
        }
    }

    /// Retrieves CPU statistics from the cgroup (Linux only).
    ///
    /// # Arguments
    /// * `pid` - Process ID (currently unused, cgroup is determined at construction).
    ///
    /// # Returns
    /// Some(CgroupCPUStats) if cgroup CPU controller is available and accessible,
    /// None otherwise or on non-Linux platforms.
    #[allow(unused_variables)]
    pub fn get_cgroup_stats(&self, pid: u32) -> Option<CgroupCPUStats> {
        #[cfg(target_os = "linux")]
        {
            use crate::cgroups::get_cgroup_by_pid;
            use crate::container::is_running_in_container;
            use cgroups_rs::fs::cpu::CpuController;
            use tracing::error;

            if !is_running_in_container() {
                return None;
            }

            match get_cgroup_by_pid(pid) {
                Ok(cgroup) => {
                    if let Ok(cpu_controller) = cgroup.controller_of::<CpuController>() {
                        let (Ok(period), Ok(quota)) =
                            (cpu_controller.period(), cpu_controller.quota())
                        else {
                            return None;
                        };

                        return Some(CgroupCPUStats { period, quota });
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
