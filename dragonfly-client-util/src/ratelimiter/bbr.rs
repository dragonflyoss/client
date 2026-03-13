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

use crate::container::is_running_in_container;
use crate::shutdown;
use crate::sysinfo::{cpu::CPU, memory::Memory};
use parking_lot::Mutex;
use ringbuf::traits::*;
use ringbuf::HeapRb;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Default bucket count for the rolling window.
#[inline]
pub fn default_bucket_count() -> u32 {
    50
}

/// Default bucket interval for the rolling window (e.g., 200ms).
#[inline]
pub fn default_bucket_interval() -> Duration {
    Duration::from_millis(200)
}

/// Default CPU usage threshold (percentage) for overload detection.
#[inline]
pub fn default_cpu_threshold() -> u8 {
    85
}

/// Default memory usage threshold (percentage) for overload detection.
#[inline]
pub fn default_memory_threshold() -> u8 {
    85
}

/// Default cooldown duration after shedding a request, during which subsequent requests will also
/// be shed.
#[inline]
pub fn default_shed_cooldown() -> Duration {
    Duration::from_secs(5)
}

/// Default interval for the background collector to sample CPU and memory usage.
#[inline]
pub fn default_collect_interval() -> Duration {
    Duration::from_secs(3)
}

/// RAII guard that automatically records request metrics and decrements
/// the in-flight counter when the request completes (guard is dropped).
///
/// Obtained via [`BBR::acquire`]. Tracks the request's response time from
/// creation to drop, feeding it back into the rolling window for adaptive
/// rate limit calculations.
pub struct RequestGuard<'a> {
    /// Reference to the BBR limiter; used to update metrics and decrement
    /// the in-flight count when this guard is dropped.
    limiter: &'a BBR,

    /// Timestamp captured at guard creation; used to compute the request's
    /// elapsed response time upon drop.
    created_at: Instant,
}

/// When the guard is dropped, it calculates the request's response time and
/// updates the rolling window metrics accordingly. It also decrements the
/// in-flight counter to reflect that the request has completed.
impl Drop for RequestGuard<'_> {
    /// Drops the guard, recording the request's response time and updating the rolling window
    /// metrics.
    fn drop(&mut self) {
        let rt = self.created_at.elapsed().as_millis().max(1) as u64;
        self.limiter.rolling_window.add(rt);
        self.limiter.rolling_window.sub_in_flight();
    }
}

/// Configuration for the BBR-based adaptive rate limiter.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct BBRConfig {
    /// Number of time buckets in the rolling window for metric aggregation.
    #[serde(default = "default_bucket_count")]
    pub bucket_count: u32,

    /// Duration of each time bucket (e.g., 200ms).
    #[serde(default = "default_bucket_interval")]
    pub bucket_interval: Duration,

    /// CPU usage percentage threshold (0–100) above which the system is
    /// considered overloaded.
    #[serde(default = "default_cpu_threshold")]
    pub cpu_threshold: u8,

    /// Memory usage percentage threshold (0–100) above which the system is
    /// considered overloaded.
    #[serde(default = "default_memory_threshold")]
    pub memory_threshold: u8,

    /// Duration to continue shedding incoming requests after the first drop
    /// event, preventing rapid oscillation between shedding and accepting.
    #[serde(default = "default_shed_cooldown")]
    pub shed_cooldown: Duration,

    /// How often the background task collects CPU/memory usage metrics.
    #[serde(default = "default_collect_interval")]
    pub collect_interval: Duration,
}

/// Provides a default configuration for BBR with reasonable defaults for typical use cases.
impl Default for BBRConfig {
    /// Default values are chosen based on common practices for BBR-style rate limiters:
    fn default() -> Self {
        Self {
            bucket_count: default_bucket_count(),
            bucket_interval: default_bucket_interval(),
            cpu_threshold: default_cpu_threshold(),
            memory_threshold: default_memory_threshold(),
            shed_cooldown: default_shed_cooldown(),
            collect_interval: default_collect_interval(),
        }
    }
}

/// BBR-inspired adaptive rate limiter that combines system resource monitoring
/// (CPU/memory) with request-level metrics (throughput, latency, in-flight count)
/// to perform congestion-based load shedding.
///
/// # Algorithm
///
/// The limiter only activates when system resources exceed configured thresholds.
/// When overloaded, it estimates the system's capacity using the formula:
///
/// ```text
/// estimated_limit = max_pass * min_rt * bucket_count / 1000
/// ```
///
/// Where:
/// - `max_pass`: Peak per-bucket throughput observed in the rolling window.
/// - `min_rt`: Minimum response time (ms) observed in the rolling window.
/// - `bucket_count`: Number of buckets in the rolling window.
///
/// If the current in-flight request count exceeds `estimated_limit`, the
/// incoming request is shed (dropped). A cooldown mechanism prevents
/// oscillation after a drop event.
///
/// # Usage
///
/// ```ignore
/// let bbr = BBR::new(BBRConfig::default()).await;
///
/// // In your request handler:
/// let guard = match bbr.acquire() {
///     Some(guard) => guard,
///     None => return Err(Status::resource_exhausted("overloaded")),
/// };
/// // ... handle request ...
/// // guard is dropped automatically, recording metrics
/// ```
pub struct BBR {
    /// Rolling window that aggregates per-bucket throughput, latency, and
    /// in-flight request counts over a sliding time horizon.
    rolling_window: RollingWindow,

    /// Shared handle to the background collector that periodically samples
    /// CPU and memory usage and maintains an atomic overload flag.
    overload_collector: Arc<OverloadCollector>,

    /// How long to keep shedding requests after the most recent drop event,
    /// preventing rapid on/off oscillation.
    shed_cooldown: Duration,

    /// Timestamp of the most recent load-shedding event; `None` if no drop
    /// has occurred yet. Protected by a mutex for interior mutability.
    shed_at: Mutex<Option<Instant>>,
}

/// BBR implementation with methods for creating a new limiter and acquiring request guards.
impl BBR {
    /// Constructs a new BBR rate limiter, performs an initial synchronous
    /// resource collection, and spawns the background collection loop.
    pub async fn new(config: BBRConfig) -> Self {
        let overload_collector = Arc::new(OverloadCollector::new(config.clone()));

        // Perform initial collection so metrics are available immediately.
        overload_collector.collect_overloaded().await;

        // Spawn the background collection loop.
        let overload_collector_clone = overload_collector.clone();
        tokio::spawn(async move {
            overload_collector_clone.run().await;
        });

        Self {
            rolling_window: RollingWindow::new(config.bucket_count, config.bucket_interval),
            overload_collector,
            shed_cooldown: config.shed_cooldown,
            shed_at: Mutex::new(None),
        }
    }

    /// Attempts to acquire a request permit. Returns `Some(RequestGuard)` if
    /// the request is allowed, or `None` if it should be shed due to overload.
    /// Increments the in-flight counter immediately; decrements it if shed.
    pub async fn acquire(&self) -> Option<RequestGuard<'_>> {
        self.rolling_window.add_in_flight();
        if self.should_shed().await {
            self.rolling_window.sub_in_flight();
            return None;
        }

        Some(RequestGuard {
            limiter: self,
            created_at: Instant::now(),
        })
    }

    /// Evaluates whether the current request should be dropped. Checks
    /// cooldown status, system overload, data sufficiency, and whether
    /// in-flight count exceeds the estimated capacity limit.
    async fn should_shed(&self) -> bool {
        if self.is_in_cooldown() {
            return true;
        }

        if !self.overload_collector.is_overloaded() {
            return false;
        }

        let (max_pass, min_rt, in_flight) = self.rolling_window.get_stats();
        if max_pass == 0 || in_flight == 0 {
            return false;
        }

        let estimated_limit =
            (max_pass as f64 * min_rt as f64 * self.rolling_window.bucket_count() as f64 / 1000.0)
                .round() as u64;
        if estimated_limit >= in_flight {
            return false;
        }

        warn!(
            "overloaded: cpu={}%, memory={}%, estimated_limit={}, in_flight={}",
            self.overload_collector.cpu_used_percent(),
            self.overload_collector.memory_used_percent(),
            estimated_limit,
            in_flight
        );

        self.shed_at.lock().replace(Instant::now());
        true
    }

    /// Returns `true` if a recent shed event is still within the cooldown
    /// window, indicating that requests should continue to be dropped.
    #[inline]
    fn is_in_cooldown(&self) -> bool {
        self.shed_at
            .lock()
            .map_or(false, |shed_at| shed_at.elapsed() < self.shed_cooldown)
    }
}

/// Background task that periodically samples system CPU and memory usage,
/// maintaining an atomic flag indicating whether the system is currently
/// overloaded. Supports both containerized (cgroup) and bare-metal environments.
struct OverloadCollector {
    // Atomic flag: `true` when CPU or memory exceeds their respective thresholds.
    is_overloaded: AtomicBool,

    // CPU sampler used to read usage from cgroup or process-level stats.
    cpu: CPU,

    // CPU usage percentage at or above which the system is deemed overloaded.
    cpu_threshold: u8,

    // Most recently observed CPU usage percentage, stored atomically for
    // lock-free reads in logging and diagnostics.
    cpu_used_percent: AtomicU8,

    // Memory sampler used to read usage from cgroup or process-level stats.
    memory: Memory,

    // Memory usage percentage at or above which the system is deemed overloaded.
    memory_threshold: u8,

    // Most recently observed memory usage percentage, stored atomically.
    memory_used_percent: AtomicU8,

    // Interval between successive resource usage samples.
    collect_interval: Duration,

    // PID of the current process, used to query per-process resource stats.
    pid: u32,

    // Whether the process is running inside a container (cgroup-aware path).
    is_running_in_container: bool,
}

/// Implementation of the overload collector, responsible for periodically sampling CPU and memory
/// usage, updating the overload status, and providing methods to check current usage and overload
/// state.
impl OverloadCollector {
    /// Creates a new collector from the given config, capturing the current PID
    /// and detecting whether the process runs inside a container.
    pub fn new(config: BBRConfig) -> Self {
        Self {
            is_overloaded: AtomicBool::new(false),
            cpu: CPU::new(),
            cpu_threshold: config.cpu_threshold,
            cpu_used_percent: AtomicU8::new(0),
            memory: Memory::default(),
            memory_threshold: config.memory_threshold,
            memory_used_percent: AtomicU8::new(0),
            collect_interval: config.collect_interval,
            pid: std::process::id(),
            is_running_in_container: is_running_in_container(),
        }
    }

    /// Runs the periodic collection loop until a shutdown signal is received.
    pub async fn run(&self) {
        // Start the collect loop.
        let mut interval = tokio::time::interval(self.collect_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    self.collect_overloaded().await;
                }
                _ = shutdown::shutdown_signal() => {
                    // Collector server shutting down with signals.
                    info!("ratelimiter's collecting server shutting down");
                    return
                }
            }
        }
    }

    /// Performs a single collection cycle: samples CPU and memory, then updates
    /// the atomic `is_overloaded` flag.
    pub async fn collect_overloaded(&self) {
        self.is_overloaded.store(
            self.is_cpu_overloaded().await || self.is_memory_overloaded(),
            Ordering::Relaxed,
        );
    }

    /// Returns the current overload status (lock-free atomic read).
    pub fn is_overloaded(&self) -> bool {
        self.is_overloaded.load(Ordering::Relaxed)
    }

    /// Returns the most recently sampled CPU usage percentage.
    pub fn cpu_used_percent(&self) -> u8 {
        self.cpu_used_percent.load(Ordering::Relaxed)
    }

    /// Returns the most recently sampled memory usage percentage.
    pub fn memory_used_percent(&self) -> u8 {
        self.memory_used_percent.load(Ordering::Relaxed)
    }

    /// Checks whether memory usage meets or exceeds the configured threshold.
    /// Reads cgroup stats in containers, falling back to process-level stats.
    #[inline]
    fn is_memory_overloaded(&self) -> bool {
        let used_percent = if self.is_running_in_container {
            match self.memory.get_cgroup_stats(self.pid) {
                Some(stats) => (stats.used_percent * 100.0).round() as u8,
                None => {
                    warn!("container detected but cgroup memory stats unavailable, falling back to process stats");
                    (self.memory.get_process_stats(self.pid).used_percent * 100.0).round() as u8
                }
            }
        } else {
            (self.memory.get_process_stats(self.pid).used_percent * 100.0).round() as u8
        };

        self.memory_used_percent
            .store(used_percent, Ordering::Relaxed);
        used_percent >= self.memory_threshold
    }

    /// Checks whether CPU usage meets or exceeds the configured threshold.
    /// Reads cgroup stats in containers, falling back to process-level stats.
    #[inline]
    async fn is_cpu_overloaded(&self) -> bool {
        let used_percent = if self.is_running_in_container {
            match self.cpu.get_cgroup_stats(self.pid).await {
                Some(stats) => stats.used_percent.round() as u8,
                None => {
                    warn!("container detected but cgroup CPU stats unavailable, falling back to process stats");
                    self.cpu.get_process_stats(self.pid).used_percent.round() as u8
                }
            }
        } else {
            self.cpu.get_process_stats(self.pid).used_percent.round() as u8
        };

        self.cpu_used_percent.store(used_percent, Ordering::Relaxed);
        used_percent >= self.cpu_threshold
    }
}

/// Represents a single time bucket sample containing aggregated metrics.
#[derive(Clone, Copy)]
struct Sample {
    // Number of requests passed in this bucket.
    pass: u64,

    // Minimum response time observed in this bucket (in milliseconds).
    min_rt: u64,
}

/// A thread-safe rolling window for tracking request metrics over time.
///
/// Uses a ring buffer to maintain time-bucketed samples, enabling calculation
/// of max throughput, min response time, and in-flight request count for
/// adaptive rate limiting (e.g., BBR-style congestion control).
pub struct RollingWindow {
    // Ring buffer storing historical samples.
    ring: Mutex<HeapRb<Sample>>,

    // Number of buckets to maintain in the rolling window.
    bucket_count: u32,

    // Duration of each time bucket.
    bucket_interval: Duration,

    // Current active bucket: (started_at, pass, min_rt).
    current_bucket: Mutex<(Instant, u64, u64)>,

    // Current number of in-flight requests.
    in_flight: AtomicU64,
}

/// RollingWindow implement the methods for RollingWindow.
impl RollingWindow {
    /// Creates a new RollingWindow with the specified number of buckets and bucket interval.
    ///
    /// # Arguments
    /// * `bucket_count` - Number of buckets to maintain in the rolling window.
    /// * `bucket_interval` - Duration of each bucket (e.g., 100ms).
    ///
    /// # Returns
    /// A new RollingWindow instance with an empty ring buffer.
    pub fn new(bucket_count: u32, bucket_interval: Duration) -> Self {
        Self {
            ring: Mutex::new(HeapRb::new(bucket_count as usize)),
            bucket_count,
            bucket_interval,
            current_bucket: Mutex::new((Instant::now(), 0, u64::MAX)),
            in_flight: AtomicU64::new(0),
        }
    }

    /// Records a completed request with its response time.
    ///
    /// If the current bucket has expired, it flushes the bucket to the ring buffer
    /// and starts a new bucket. Updates the pass count and tracks the minimum
    /// response time within the current bucket.
    ///
    /// # Arguments
    /// * `rt` - Response time of the request in milliseconds.
    pub fn add(&self, rt: u64) {
        let now = Instant::now();
        let mut current_bucket = self.current_bucket.lock();
        if now.duration_since(current_bucket.0) >= self.bucket_interval {
            // Flush the current bucket to the ring buffer if it has any recorded passes, when the
            // bucket interval has elapsed.
            if current_bucket.1 > 0 {
                let mut ring = self.ring.lock();
                ring.push_overwrite(Sample {
                    pass: current_bucket.1,
                    min_rt: current_bucket.2,
                });
            }

            // Start a new bucket, when the current bucket has expired.
            *current_bucket = (now, 0, u64::MAX);
        }

        // Update the current bucket with the new request's metrics.
        current_bucket.1 += 1;
        current_bucket.2 = current_bucket.2.min(rt);
    }

    /// Retrieves all key statistics in a single lock acquisition for efficiency.
    ///
    /// # Returns
    /// A tuple of (max_pass, min_rt, in_flight):
    /// - `max_pass`: Maximum throughput observed in any bucket.
    /// - `min_rt`: Minimum response time across all buckets (defaults to 1 if none).
    /// - `in_flight`: Current number of in-flight requests.
    pub fn get_stats(&self) -> (u64, u64, u64) {
        let ring = self.ring.lock();
        let (max_pass, min_rt) = ring.iter().fold((0, u64::MAX), |(max_pass, min_rt), s| {
            (max_pass.max(s.pass), min_rt.min(s.min_rt))
        });

        let min_rt = if min_rt == u64::MAX { 1 } else { min_rt };
        (max_pass, min_rt, self.in_flight())
    }

    /// Returns the current number of in-flight (pending) requests.
    ///
    /// Uses relaxed ordering as this is a best-effort metric for rate limiting.
    #[inline]
    pub fn in_flight(&self) -> u64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    /// Increments the in-flight counter when a new request starts.
    ///
    /// # Returns
    /// The new in-flight count after incrementing (useful for limit checking).
    #[inline]
    pub fn add_in_flight(&self) -> u64 {
        self.in_flight.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Decrements the in-flight counter when a request completes.
    ///
    /// Should be called in a finally/drop block to ensure proper cleanup.
    #[inline]
    pub fn sub_in_flight(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }

    /// Returns the configured number of buckets in the rolling window.
    #[inline]
    pub fn bucket_count(&self) -> u32 {
        self.bucket_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_new_rolling_window() {
        let window = RollingWindow::new(5, Duration::from_millis(50));
        assert_eq!(window.in_flight(), 0);

        let (max_pass, min_rt, in_flight) = window.get_stats();
        assert_eq!(max_pass, 0);
        assert_eq!(min_rt, 1);
        assert_eq!(in_flight, 0);
    }

    #[test]
    fn test_add_single_request() {
        let window = RollingWindow::new(10, Duration::from_millis(100));
        window.add(100);

        let current_bucket = window.current_bucket.lock();
        assert_eq!(current_bucket.1, 1);
        assert_eq!(current_bucket.2, 100);
    }

    #[test]
    fn test_add_multiple_requests_same_bucket() {
        let window = RollingWindow::new(10, Duration::from_millis(100));
        window.add(100);
        window.add(50);
        window.add(200);

        let current_bucket = window.current_bucket.lock();
        assert_eq!(current_bucket.1, 3);
        assert_eq!(current_bucket.2, 50);
    }

    #[test]
    fn test_bucket_rotation() {
        let window = RollingWindow::new(10, Duration::from_millis(100));
        window.add(100);
        window.add(80);
        thread::sleep(Duration::from_millis(150));
        window.add(150);

        let ring = window.ring.lock();
        assert_eq!(ring.occupied_len(), 1);

        let sample = ring.iter().next().unwrap();
        assert_eq!(sample.pass, 2);
        assert_eq!(sample.min_rt, 80);
    }

    #[test]
    fn test_in_flight_initial_value() {
        let window = RollingWindow::new(10, Duration::from_millis(100));
        assert_eq!(window.in_flight(), 0);
    }

    #[test]
    fn test_add_in_flight() {
        let window = RollingWindow::new(10, Duration::from_millis(100));

        assert_eq!(window.add_in_flight(), 1);
        assert_eq!(window.add_in_flight(), 2);
        assert_eq!(window.add_in_flight(), 3);
        assert_eq!(window.in_flight(), 3);
    }

    #[test]
    fn test_sub_in_flight() {
        let window = RollingWindow::new(10, Duration::from_millis(100));
        window.add_in_flight();
        window.add_in_flight();
        window.add_in_flight();

        window.sub_in_flight();
        assert_eq!(window.in_flight(), 2);

        window.sub_in_flight();
        assert_eq!(window.in_flight(), 1);

        window.sub_in_flight();
        assert_eq!(window.in_flight(), 0);
    }

    #[test]
    fn test_get_stats_empty_window() {
        let window = RollingWindow::new(10, Duration::from_millis(100));

        let (max_pass, min_rt, in_flight) = window.get_stats();
        assert_eq!(max_pass, 0);
        assert_eq!(min_rt, 1);
        assert_eq!(in_flight, 0);
    }

    #[test]
    fn test_get_stats_with_data() {
        let window = RollingWindow::new(10, Duration::from_millis(100));
        window.add(100);
        window.add(50);
        thread::sleep(Duration::from_millis(120));

        window.add(200);
        window.add(150);
        window.add(120);
        thread::sleep(Duration::from_millis(110));

        window.add(80);
        window.add_in_flight();
        window.add_in_flight();

        let (max_pass, min_rt, in_flight) = window.get_stats();
        assert_eq!(max_pass, 3);
        assert_eq!(min_rt, 50);
        assert_eq!(in_flight, 2);
    }

    #[test]
    fn test_get_stats_includes_in_flight() {
        let window = RollingWindow::new(10, Duration::from_millis(100));

        window.add_in_flight();
        window.add_in_flight();
        window.add_in_flight();
        let (_, _, in_flight) = window.get_stats();
        assert_eq!(in_flight, 3);
    }

    #[test]
    fn test_expired_samples_filtered() {
        let window = RollingWindow::new(3, Duration::from_millis(100));
        window.add(100);
        thread::sleep(Duration::from_millis(120));

        window.add(80);
        thread::sleep(Duration::from_millis(110));

        window.add(60);
        thread::sleep(Duration::from_millis(110));

        window.add(40);
        thread::sleep(Duration::from_millis(200));

        window.add(30);
        let (max_pass, min_rt, _) = window.get_stats();

        assert_eq!(max_pass, 1);
        assert!(min_rt == 40);
    }

    #[test]
    fn test_zero_response_time() {
        let window = RollingWindow::new(10, Duration::from_millis(100));
        window.add(0);

        let current_bucket = window.current_bucket.lock();
        assert_eq!(current_bucket.1, 1);
        assert_eq!(current_bucket.2, 0);
    }

    #[test]
    fn test_max_response_time() {
        let window = RollingWindow::new(10, Duration::from_millis(100));
        window.add(u64::MAX - 1);

        let current_bucket = window.current_bucket.lock();
        assert_eq!(current_bucket.1, 1);
        assert_eq!(current_bucket.2, u64::MAX - 1);
    }

    #[test]
    fn test_single_bucket_window() {
        let window = RollingWindow::new(1, Duration::from_millis(100));
        window.add(100);
        thread::sleep(Duration::from_millis(150));
        window.add(50);

        let ring = window.ring.lock();
        assert_eq!(ring.occupied_len(), 1);
    }

    #[test]
    fn test_empty_bucket_not_flushed() {
        let window = RollingWindow::new(10, Duration::from_millis(100));
        thread::sleep(Duration::from_millis(120));
        window.add(100);
        let ring = window.ring.lock();
        assert_eq!(ring.occupied_len(), 0);
    }

    #[test]
    fn test_concurrent_add_requests() {
        let window = Arc::new(RollingWindow::new(10, Duration::from_millis(100)));
        let mut handles = vec![];
        for _ in 0..4 {
            let w = window.clone();
            handles.push(thread::spawn(move || {
                for i in 0..100 {
                    w.add(i as u64);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        let current_bucket = window.current_bucket.lock();
        assert!(current_bucket.1 > 0);
        assert!(current_bucket.2 <= 99);
    }

    #[tokio::test]
    async fn test_concurrent_add_and_stats() {
        let window = Arc::new(RollingWindow::new(10, Duration::from_millis(100)));
        let mut handles = vec![];
        for _ in 0..2 {
            let w = window.clone();
            handles.push(tokio::spawn(async move {
                for i in 0..50 {
                    w.add(i as u64 + 10);
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            }));
        }

        for _ in 0..2 {
            let w = window.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..50 {
                    let (max_pass, min_rt, _) = w.get_stats();
                    assert!(min_rt > 0);
                    assert!(max_pass <= 100);
                    tokio::time::sleep(Duration::from_micros(100)).await;
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }
    }

    #[test]
    fn test_bbr_style_usage() {
        let window = RollingWindow::new(10, Duration::from_millis(100));
        for rt in [100, 80, 120, 60, 90, 70, 110, 50, 85, 95] {
            let _ = window.add_in_flight();
            window.add(rt);
            window.sub_in_flight();
        }

        thread::sleep(Duration::from_millis(110));
        for rt in [40, 55, 45, 60, 50] {
            let _ = window.add_in_flight();
            window.add(rt);
            window.sub_in_flight();
        }

        let (max_pass, min_rt, _) = window.get_stats();
        let estimated_limit = max_pass as f64 * min_rt as f64 / 1000.0;
        assert!(estimated_limit == 0.5 || max_pass == 0);
    }
}
