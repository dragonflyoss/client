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

use ringbuf::traits::*;
use ringbuf::HeapRb;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

#[derive(Clone, Copy)]
struct Sample {
    pass: u64,
    min_rt: u64,
    created_at: Instant,
}

pub struct RollingWindow {
    ring: Mutex<HeapRb<Sample>>,
    window_duration: Duration,
    bucket_duration: Duration,
    current: Mutex<(Instant, u64, u64)>, // (start_time, pass, min_rt)
    in_flight: AtomicI64,
}

impl RollingWindow {
    pub fn new(bucket_count: u32, bucket_duration: Duration) -> Self {
        Self {
            ring: Mutex::new(HeapRb::new(bucket_count as usize)),
            window_duration: bucket_duration * bucket_count,
            bucket_duration,
            current: Mutex::new((Instant::now(), 0, u64::MAX)),
            in_flight: AtomicI64::new(0),
        }
    }

    pub async fn add(&self, rt_ms: u64) {
        let now = Instant::now();
        let mut current = self.current.lock().await;
        if now.duration_since(current.0) >= self.bucket_duration {
            if current.1 > 0 {
                let sample = Sample {
                    created_at: current.0,
                    pass: current.1,
                    min_rt: current.2,
                };

                let mut ring = self.ring.lock().await;
                ring.push_overwrite(sample);
            }

            *current = (now, 0, u64::MAX);
        }

        current.1 += 1;
        current.2 = current.2.min(rt_ms);
    }

    pub async fn max_pass(&self) -> u64 {
        let ring = self.ring.lock().await;
        let now = Instant::now();
        ring.iter()
            .filter(|s| now.duration_since(s.created_at) <= self.window_duration)
            .map(|s| s.pass)
            .max()
            .unwrap_or(1)
    }

    pub async fn min_rt(&self) -> u64 {
        let ring = self.ring.lock().await;
        let now = Instant::now();
        ring.iter()
            .filter(|s| now.duration_since(s.created_at) <= self.window_duration)
            .filter(|s| s.min_rt < u64::MAX)
            .map(|s| s.min_rt)
            .min()
            .unwrap_or(1)
    }

    pub fn in_flight(&self) -> i64 {
        self.in_flight.load(Ordering::Relaxed)
    }

    pub fn add_in_flight(&self) -> i64 {
        self.in_flight.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn sub_in_flight(&self) {
        self.in_flight.fetch_sub(1, Ordering::Relaxed);
    }
}
