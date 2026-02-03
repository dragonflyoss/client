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

use std::sync::Arc;
use std::time::Duration;
use sysinfo::{CpuRefreshKind, Pid, ProcessRefreshKind, RefreshKind, System};
use tokio::sync::Mutex;

/// Represents system-wide CPU statistics.
#[derive(Debug, Clone, Default)]
pub struct CPUStats {
    /// Number of physical CPU cores available on the system.
    pub physical_core_count: u32,

    /// Number of logical CPU cores (including hyperthreads) available on the system.
    pub logical_core_count: u32,

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

    /// Calculated CPU usage percentage within the cgroup (0.0 - 100.0).
    pub used_percent: f64,
}

/// CPU represents a cpu interface with its information.
#[derive(Debug, Clone, Default)]
pub struct CPU {
    /// Mutex to protect concurrent access to cgroup CPU statistics.
    mutex: Arc<Mutex<()>>,
}

/// Implementation of CPU monitoring functionality.
///
/// Provides methods to retrieve CPU statistics at three different levels:
/// - System-wide: Overall CPU usage and core count.
/// - Process-level: CPU usage for a specific process.
/// - Cgroup-level: CPU resource limits and quotas (Linux only).
impl CPU {
    /// Default interval for refreshing cgroup CPU statistics.
    const DEFAULT_CPU_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

    /// Creates a new CPU instance.
    pub fn new() -> Self {
        Self {
            mutex: Arc::new(Mutex::new(())),
        }
    }

    /// Retrieves system-wide CPU statistics.
    ///
    /// # Returns
    /// CPUStats containing physical core count and global CPU usage percentage.
    pub fn get_stats(&self) -> CPUStats {
        let sys = System::new_with_specifics(
            RefreshKind::new().with_cpu(CpuRefreshKind::new().with_cpu_usage()),
        );

        CPUStats {
            physical_core_count: num_cpus::get_physical() as u32,
            logical_core_count: num_cpus::get() as u32,
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
    /// * `pid` - Process ID used to determine the cgroup for which CPU statistics are collected.
    ///
    /// # Returns
    /// Some(CgroupCPUStats) if cgroup CPU controller is available and accessible,
    /// None otherwise or on non-Linux platforms.
    #[allow(unused_variables)]
    pub async fn get_cgroup_stats(&self, pid: u32) -> Option<CgroupCPUStats> {
        // Retrieve and compute cgroup CPU statistics while holding the mutex.
        let _guard = self.mutex.lock().await;

        #[cfg(target_os = "linux")]
        {
            use crate::cgroups::get_cgroup_by_pid;
            use crate::container::is_running_in_container;
            use cgroups_rs::fs::cpu::CpuController;
            use tracing::error;

            if !is_running_in_container() {
                return None;
            }

            // Lock the mutex to ensure exclusive access to cgroup stats.
            match get_cgroup_by_pid(pid) {
                Ok(cgroup) => {
                    let cpu_controller = cgroup.controller_of::<CpuController>()?;
                    let (Ok(period), Ok(quota)) =
                        (cpu_controller.cfs_period(), cpu_controller.cfs_quota())
                    else {
                        return None;
                    };

                    // Get CPU usage percentage.
                    let used_percent = self
                        .calculate_cgroup_used_percent(&cgroup, period, quota)
                        .await?;

                    Some(CgroupCPUStats {
                        period,
                        quota,
                        used_percent,
                    })
                }
                Err(err) => {
                    error!("failed to get cgroup for pid {}: {}", pid, err);
                    None
                }
            }
        }

        #[cfg(not(target_os = "linux"))]
        None
    }

    /// Calculates the CPU usage percentage within a cgroup.
    ///
    /// This method takes two samples of CPU usage and calculates the percentage
    /// based on the difference over the sampling interval.
    ///
    /// # Arguments
    /// * `cgroup` - Reference to the cgroup.
    /// * `period` - CFS period in microseconds.
    /// * `quota` - CFS quota in microseconds (-1 means unlimited).
    ///
    /// # Returns
    /// Some(f64) containing the CPU usage percentage if calculable,
    /// None if unable to read CPU usage or if quota is unlimited.
    #[cfg(target_os = "linux")]
    async fn calculate_cgroup_used_percent(
        &self,
        cgroup: &cgroups_rs::fs::Cgroup,
        period: u64,
        quota: i64,
    ) -> Option<f64> {
        // Get initial and final CPU usage samples and calculate the difference.
        let initial_usage = self.get_cgroup_usage(cgroup)?;
        tokio::time::sleep(Self::DEFAULT_CPU_REFRESH_INTERVAL).await;
        let final_usage = self.get_cgroup_usage(cgroup)?;
        let consumed = final_usage.saturating_sub(initial_usage);

        // Calculate available CPU time based on quota.
        let interval_ns = Self::DEFAULT_CPU_REFRESH_INTERVAL.as_nanos() as u64;
        let capacity = if quota > 0 {
            // If quota is set, calculate based on quota/period ratio
            // quota and period are in microseconds, convert to nanoseconds.
            let quota_ns = (quota as u64) * 1000;
            let period_ns = period * 1000;
            (interval_ns * quota_ns) / period_ns
        } else {
            // If quota is unlimited (-1), use all available CPU cores.
            let logical_core_count = num_cpus::get() as u64;
            interval_ns * logical_core_count
        };

        // Calculate usage percentage.
        if capacity > 0 {
            let percent = (consumed as f64 / capacity as f64) * 100.0;
            Some(percent.clamp(0.0, 100.0))
        } else {
            None
        }
    }

    /// Gets the current CPU usage from the cgroup.
    ///
    /// Supports both cgroup v1 (cpuacct.usage) and cgroup v2 (cpu.stat usage_usec).
    ///
    /// # Arguments
    /// * `cgroup` - Reference to the cgroup.
    ///
    /// # Returns
    /// Some(u64) containing the CPU usage in nanoseconds,
    /// None if unable to read the value.
    #[cfg(target_os = "linux")]
    fn get_cgroup_usage(&self, cgroup: &cgroups_rs::fs::Cgroup) -> Option<u64> {
        use cgroups_rs::fs::{cpu::CpuController, cpuacct::CpuAcctController};

        // Try cgroup v1 first (cpuacct controller).
        if let Some(cpuacct) = cgroup.controller_of::<CpuAcctController>() {
            // cpuacct.usage returns nanoseconds
            return Some(cpuacct.cpuacct().usage);
        }

        // Fall back to cgroup v2 (read cpu.stat directly).
        if let Some(cpu) = cgroup.controller_of::<CpuController>() {
            // For cgroup v2, we need to read cpu.stat file
            // The usage_usec field contains total CPU time in microseconds.
            let stat = cpu.cpu().stat;
            if let Some(usage_usec) = self.parse_usage_usec(&stat) {
                // Convert microseconds to nanoseconds for consistency
                return Some(usage_usec * 1000);
            }
        }

        None
    }

    /// Parses the usage_usec value from cpu.stat content.
    ///
    /// The cpu.stat file format (cgroup v2) looks like:
    /// ```
    /// usage_usec 123456789
    /// user_usec 100000000
    /// system_usec 23456789
    /// nr_periods 0
    /// nr_throttled 0
    /// throttled_usec 0
    /// ```
    ///
    /// # Arguments
    /// * `stat` - The content of cpu.stat file as a string.
    ///
    /// # Returns
    /// Some(u64) containing the usage_usec value in microseconds,
    /// None if the value cannot be parsed.
    #[cfg(target_os = "linux")]
    fn parse_usage_usec(&self, stat: &str) -> Option<u64> {
        for line in stat.lines() {
            let line = line.trim();
            if line.starts_with("usage_usec") {
                // Split by whitespace and get the second part.
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 2 {
                    return parts[1].parse::<u64>().ok();
                }
            }
        }

        None
    }
}
