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
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, RefreshKind, System};
use tokio::sync::Mutex;

/// Disk represents a disk interface for monitoring disk statistics.
#[derive(Debug, Clone, Default)]
pub struct Disk {
    // Mutex to protect concurrent access to disk statistics.
    mutex: Arc<Mutex<()>>,
}

/// DiskStats represents the disk statistics for a specific path.
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

/// ProcessDiskStats represents the disk I/O statistics for a specific process.
#[derive(Debug, Clone, Default)]
pub struct ProcessDiskStats {
    /// The write bandwidth of the process in bytes per second.
    pub write_bandwidth: u64,

    /// The read bandwidth of the process in bytes per second.
    pub read_bandwidth: u64,
}

/// Implementation of disk monitoring functionality.
///
/// Provides methods to retrieve disk space information and process-specific
/// disk I/O statistics, including read/write bandwidth measurements.
impl Disk {
    /// Default interval for refreshing disk statistics.
    const DEFAULT_DISK_REFRESH_INTERVAL: Duration = Duration::from_secs(1);

    /// Creates a new Disk instance.
    ///
    /// # Returns
    /// A new Disk instance with an initialized mutex for thread-safe access.
    pub fn new() -> Self {
        Disk {
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

        Ok(DiskStats {
            total: total_space,
            free: available_space,
            usage: usage_space,
            used_percent,
        })
    }

    /// Retrieves the disk I/O statistics for a specific process.
    ///
    /// This method measures disk I/O activity over a time interval
    /// (DEFAULT_DISK_REFRESH_INTERVAL) to calculate read and write bandwidth
    /// for the specified process.
    ///
    /// # Arguments
    /// * `pid` - The process ID to monitor for disk I/O statistics.
    ///
    /// # Returns
    /// ProcessDiskStats containing read and write bandwidth information in bytes per second.
    pub async fn get_process_stats(&self, pid: u32) -> ProcessDiskStats {
        // Lock the mutex to ensure exclusive access to disk stats.
        let _guard = self.mutex.lock().await;

        let mut sys = System::new_with_specifics(
            RefreshKind::new().with_processes(ProcessRefreshKind::new().with_disk_usage()),
        );

        // Sleep to calculate the disk traffic difference over
        // the DEFAULT_DISK_REFRESH_INTERVAL.
        tokio::time::sleep(Self::DEFAULT_DISK_REFRESH_INTERVAL).await;

        sys.refresh_processes_specifics(
            ProcessesToUpdate::All,
            true,
            ProcessRefreshKind::new().with_disk_usage(),
        );

        let disk_usage = sys.process(Pid::from_u32(pid)).unwrap().disk_usage();
        ProcessDiskStats {
            write_bandwidth: disk_usage.written_bytes
                / Self::DEFAULT_DISK_REFRESH_INTERVAL.as_secs(),
            read_bandwidth: disk_usage.read_bytes / Self::DEFAULT_DISK_REFRESH_INTERVAL.as_secs(),
        }
    }
}
