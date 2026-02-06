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

use parking_lot::Mutex;
use ringbuf::traits::*;
use ringbuf::HeapRb;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Represents a single time bucket sample containing aggregated metrics.
#[derive(Clone, Copy)]
struct Sample {
    // Number of requests passed in this bucket.
    pass: u64,

    // Minimum response time observed in this bucket (in milliseconds).
    min_rt: u64,

    // Timestamp when this bucket was created.
    created_at: Instant,
}

/// A thread-safe rolling window for tracking request metrics over time.
///
/// Uses a ring buffer to maintain time-bucketed samples, enabling calculation
/// of max throughput, min response time, and in-flight request count for
/// adaptive rate limiting (e.g., BBR-style congestion control).
pub struct RollingWindow {
    // Ring buffer storing historical samples.
    ring: Mutex<HeapRb<Sample>>,

    // Duration of each time bucket.
    bucket_interval: Duration,

    // Current active bucket: (started_at, pass, min_rt).
    current_bucket: Mutex<(Instant, u64, u64)>,

    // Total window duration (bucket_interval * bucket_count).
    window: Duration,

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
            window: bucket_interval * bucket_count,
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
                    created_at: current_bucket.0,
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
        let now = Instant::now();
        let (max_pass, min_rt) = ring
            .iter()
            .filter(|s| now.duration_since(s.created_at) <= self.window)
            .fold((0, u64::MAX), |(max_p, min_r), s| {
                (max_p.max(s.pass), min_r.min(s.min_rt))
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
        assert_eq!(window.window, Duration::from_millis(250));

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
