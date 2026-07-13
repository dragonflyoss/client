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

use dragonfly_client_core::Result;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
use tokio::sync::Mutex;
use tracing::debug;

/// Represents the disk statistics for a specific path.
#[derive(Debug, Clone, Default)]
pub struct DiskStats {
    /// The total disk space in bytes.
    pub total: u64,

    /// The free disk space available in bytes.
    pub free: u64,

    /// The used disk space in bytes.
    pub usage: u64,

    /// The percentage of disk space used (0.0 to 100.0).
    pub used_percent: f64,
}

/// Represents the disk I/O statistics for a specific process.
#[derive(Debug, Clone, Default)]
pub struct ProcessDiskStats {
    /// The write bandwidth of the process in bytes per second.
    pub write_bandwidth: u64,

    /// The read bandwidth of the process in bytes per second.
    pub read_bandwidth: u64,
}

/// Represents disk I/O statistics from cgroup (Linux containers/resource limits).
#[derive(Debug, Clone, Default)]
pub struct CgroupDiskStats {
    /// The write bandwidth of the cgroup at the block-device level in bytes per second.
    pub write_bandwidth: u64,

    /// The read bandwidth of the cgroup at the block-device level in bytes per second.
    pub read_bandwidth: u64,
}

/// Represents a disk interface for monitoring disk statistics.
#[derive(Debug, Clone, Default)]
pub struct Disk {
    // Mutex to protect concurrent access to disk statistics.
    mutex: Arc<Mutex<()>>,
}

/// Implementation of disk monitoring functionality.
///
/// Provides methods to retrieve disk statistics at three different levels:
/// - Path-level: Total, free and used space of the filesystem.
/// - Process-level: I/O bandwidth of a specific process.
/// - Cgroup-level: Block-device I/O bandwidth of the cgroup (Linux only).
impl Disk {
    /// Default interval for refreshing disk statistics.
    const DEFAULT_DISK_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

    /// Creates a new Disk instance.
    ///
    /// # Returns
    /// A new Disk instance with an initialized mutex for thread-safe access.
    pub fn new() -> Self {
        Self {
            mutex: Arc::new(Mutex::new(())),
        }
    }

    /// Retrieves the disk statistics for the specified path.
    ///
    /// This method queries the filesystem to obtain total space, available space,
    /// used space, and the percentage of disk usage.
    ///
    /// # Arguments
    /// * `path` - The filesystem path to query for disk statistics.
    ///
    /// # Returns
    /// Result<DiskStats> containing disk space information if successful,
    /// or an error if the operation fails.
    pub fn get_stats(&self, path: &Path) -> Result<DiskStats> {
        let stats = fs2::statvfs(path)?;
        let total_space = stats.total_space();
        let available_space = stats.available_space();
        let usage_space = total_space - available_space;
        let used_percent = (usage_space as f64 / (total_space) as f64) * 100.0;

        debug!(
            "disk total space: {} bytes, available space: {} bytes, usage space: {} bytes, used percent: {}%",
            total_space, available_space, usage_space, used_percent
        );

        Ok(DiskStats {
            total: total_space,
            free: available_space,
            usage: usage_space,
            used_percent: used_percent.clamp(0.0, 100.0),
        })
    }

    /// Retrieves the disk I/O statistics for the process with the given PID.
    ///
    /// This method measures the process's /proc I/O counters over a time
    /// interval (DEFAULT_DISK_REFRESH_INTERVAL) to calculate read and write
    /// bandwidth. Writes are accounted at page-dirtying time rather than at
    /// the block device.
    ///
    /// # Arguments
    /// * `pid` - The process ID to monitor for disk I/O statistics.
    ///
    /// # Returns
    /// ProcessDiskStats containing read and write bandwidth information in bytes per second.
    pub async fn get_process_stats(&self, pid: u32) -> ProcessDiskStats {
        // Lock the mutex to ensure exclusive access to disk stats.
        let _guard = self.mutex.lock().await;

        let mut sys = System::new();
        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[Pid::from_u32(pid)]),
            false,
            ProcessRefreshKind::nothing().with_disk_usage(),
        );

        // Sleep to calculate the disk traffic difference over
        // the DEFAULT_DISK_REFRESH_INTERVAL.
        tokio::time::sleep(Self::DEFAULT_DISK_REFRESH_INTERVAL).await;

        sys.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[Pid::from_u32(pid)]),
            false,
            ProcessRefreshKind::nothing().with_disk_usage(),
        );

        let disk_usage = sys.process(Pid::from_u32(pid)).unwrap().disk_usage();
        let write_bandwidth =
            disk_usage.written_bytes / Self::DEFAULT_DISK_REFRESH_INTERVAL.as_secs();
        let read_bandwidth = disk_usage.read_bytes / Self::DEFAULT_DISK_REFRESH_INTERVAL.as_secs();

        debug!(
            "process {} disk write bandwidth: {} bytes/s, read bandwidth: {} bytes/s",
            pid, write_bandwidth, read_bandwidth
        );

        ProcessDiskStats {
            write_bandwidth,
            read_bandwidth,
        }
    }

    /// Retrieves disk I/O statistics from the cgroup (Linux only).
    ///
    /// This method measures the cumulative block-device I/O counters of the
    /// cgroup the process belongs to over a time interval
    /// (DEFAULT_DISK_REFRESH_INTERVAL) to calculate device-level read and
    /// write bandwidth. cgroup v2 supports writeback attribution, so flushed
    /// page cache writes are charged to the cgroup that dirtied the pages,
    /// while cgroup v1 only accounts direct and synchronous I/O.
    ///
    /// # Arguments
    /// * `pid` - Process ID used to determine which cgroup to query.
    ///
    /// # Returns
    /// Some(CgroupDiskStats) if cgroup I/O statistics are available and accessible,
    /// None otherwise or on non-Linux platforms.
    #[allow(unused_variables)]
    pub async fn get_cgroup_stats(&self, pid: u32) -> Option<CgroupDiskStats> {
        #[cfg(target_os = "linux")]
        {
            // Lock the mutex to ensure exclusive access to disk stats.
            let _guard = self.mutex.lock().await;

            // Take a baseline of the cumulative block-device I/O counters.
            let (baseline_read_bytes, baseline_written_bytes) = Self::get_cgroup_io_counters(pid)?;

            // Sleep to calculate the disk traffic difference over
            // the DEFAULT_DISK_REFRESH_INTERVAL.
            tokio::time::sleep(Self::DEFAULT_DISK_REFRESH_INTERVAL).await;

            let (read_bytes, written_bytes) = Self::get_cgroup_io_counters(pid)?;

            // Calculate the write bandwidth in bytes per second.
            let write_bandwidth = (written_bytes.saturating_sub(baseline_written_bytes) as f64
                / Self::DEFAULT_DISK_REFRESH_INTERVAL.as_secs_f64())
            .round() as u64;

            // Calculate the read bandwidth in bytes per second.
            let read_bandwidth = (read_bytes.saturating_sub(baseline_read_bytes) as f64
                / Self::DEFAULT_DISK_REFRESH_INTERVAL.as_secs_f64())
            .round() as u64;

            debug!(
                "process {} cgroup disk write bandwidth: {} bytes/s, read bandwidth: {} bytes/s",
                pid, write_bandwidth, read_bandwidth
            );

            return Some(CgroupDiskStats {
                write_bandwidth,
                read_bandwidth,
            });
        }

        #[cfg(not(target_os = "linux"))]
        None
    }

    /// Retrieves the cumulative bytes read from and written to the block
    /// devices by the cgroup the process belongs to.
    ///
    /// # Arguments
    /// * `pid` - The process ID used to determine which cgroup to read.
    ///
    /// # Returns
    /// Some((read_bytes, written_bytes)) summed across all block devices if
    /// the cgroup I/O statistics are accessible, None otherwise.
    #[cfg(target_os = "linux")]
    fn get_cgroup_io_counters(pid: u32) -> Option<(u64, u64)> {
        use cgroups_rs::fs::hierarchies;

        if hierarchies::auto().v2() {
            Self::get_cgroup_v2_io_counters(pid)
        } else {
            Self::get_cgroup_v1_io_counters(pid)
        }
    }

    /// Retrieves the cumulative block-device I/O counters from the cgroup v2
    /// io.stat file.
    ///
    /// # Arguments
    /// * `pid` - The process ID used to determine which cgroup to read.
    ///
    /// # Returns
    /// Some((read_bytes, written_bytes)) summed across all block devices,
    /// None if the io.stat file is unavailable.
    #[cfg(target_os = "linux")]
    fn get_cgroup_v2_io_counters(pid: u32) -> Option<(u64, u64)> {
        use crate::cgroups::get_cgroup_v2_path_by_pid;
        use tracing::error;

        let path = match get_cgroup_v2_path_by_pid(pid) {
            Ok(path) => path.join("io.stat"),
            Err(err) => {
                error!("failed to get cgroup v2 path for pid {}: {}", pid, err);
                return None;
            }
        };

        match std::fs::read_to_string(&path) {
            Ok(content) => Some(Self::parse_cgroup_v2_io_stat(&content)),
            Err(err) => {
                error!("failed to read {}: {}", path.display(), err);
                None
            }
        }
    }

    /// Parses the content of a cgroup v2 io.stat file and sums the read and
    /// written bytes across all block devices.
    ///
    /// Each line is `MAJ:MIN rbytes=N wbytes=N rios=N wios=N dbytes=N dios=N`.
    ///
    /// # Arguments
    /// * `content` - The content of the io.stat file.
    ///
    /// # Returns
    /// (read_bytes, written_bytes) summed across all block devices.
    #[cfg(target_os = "linux")]
    fn parse_cgroup_v2_io_stat(content: &str) -> (u64, u64) {
        let (mut read_bytes, mut written_bytes) = (0u64, 0u64);
        for part in content.split_whitespace() {
            if let Some(value) = part.strip_prefix("rbytes=") {
                read_bytes = read_bytes.saturating_add(value.parse().unwrap_or(0));
            } else if let Some(value) = part.strip_prefix("wbytes=") {
                written_bytes = written_bytes.saturating_add(value.parse().unwrap_or(0));
            }
        }

        (read_bytes, written_bytes)
    }

    /// Retrieves the cumulative block-device I/O counters from the cgroup v1
    /// blkio controller.
    ///
    /// cgroup v1 charges page cache writeback to the root cgroup, so only
    /// direct and synchronous I/O are accounted here.
    ///
    /// # Arguments
    /// * `pid` - The process ID used to determine which cgroup to read.
    ///
    /// # Returns
    /// Some((read_bytes, written_bytes)) summed across all block devices,
    /// None if the blkio controller is unavailable.
    #[cfg(target_os = "linux")]
    fn get_cgroup_v1_io_counters(pid: u32) -> Option<(u64, u64)> {
        use crate::cgroups::get_cgroup_by_pid;
        use cgroups_rs::fs::blkio::BlkIoController;
        use tracing::error;

        let cgroup = match get_cgroup_by_pid(pid) {
            Ok(cgroup) => cgroup,
            Err(err) => {
                error!("failed to get cgroup for pid {}: {}", pid, err);
                return None;
            }
        };

        let Some(blkio_controller) = cgroup.controller_of::<BlkIoController>() else {
            error!("no blkio controller found for pid {}", pid);
            return None;
        };

        // blkio.throttle.io_service_bytes is maintained by any I/O scheduler,
        // while blkio.io_service_bytes is only maintained by CFQ.
        let blkio = blkio_controller.blkio();
        let io_service_bytes = if !blkio.throttle.io_service_bytes.is_empty() {
            blkio.throttle.io_service_bytes
        } else {
            blkio.io_service_bytes
        };

        Some(Self::parse_cgroup_v1_io_stat(&io_service_bytes))
    }

    /// Parses the cgroup v1 blkio IoService entries and sums the read and
    /// written bytes across all block devices.
    ///
    /// # Arguments
    /// * `io_service_bytes` - The per-device IoService entries from
    ///   blkio.throttle.io_service_bytes or blkio.io_service_bytes.
    ///
    /// # Returns
    /// (read_bytes, written_bytes) summed across all block devices.
    #[cfg(target_os = "linux")]
    fn parse_cgroup_v1_io_stat(
        io_service_bytes: &[cgroups_rs::fs::blkio::IoService],
    ) -> (u64, u64) {
        io_service_bytes
            .iter()
            .fold((0u64, 0u64), |(read_bytes, written_bytes), io_service| {
                (
                    read_bytes.saturating_add(io_service.read),
                    written_bytes.saturating_add(io_service.write),
                )
            })
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cgroup_v2_io_stat() {
        let content = "8:0 rbytes=90430464 wbytes=299008000 rios=8950 wios=1252 dbytes=50331648 dios=3021\n253:0 rbytes=1459200 wbytes=314773504 rios=192 wios=353 dbytes=0 dios=0";
        assert_eq!(
            Disk::parse_cgroup_v2_io_stat(content),
            (90430464 + 1459200, 299008000 + 314773504)
        );

        assert_eq!(Disk::parse_cgroup_v2_io_stat(""), (0, 0));
    }

    #[test]
    fn test_parse_cgroup_v1_io_stat() {
        use cgroups_rs::fs::blkio::IoService;

        let io_service_bytes = vec![
            IoService {
                major: 8,
                minor: 0,
                read: 90430464,
                write: 299008000,
                ..Default::default()
            },
            IoService {
                major: 253,
                minor: 0,
                read: 1459200,
                write: 314773504,
                ..Default::default()
            },
        ];
        assert_eq!(
            Disk::parse_cgroup_v1_io_stat(&io_service_bytes),
            (90430464 + 1459200, 299008000 + 314773504)
        );

        assert_eq!(Disk::parse_cgroup_v1_io_stat(&[]), (0, 0));
    }
}
