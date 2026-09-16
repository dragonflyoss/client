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

use bytesize::ByteSize;
use leaky_bucket::RateLimiter;
use std::time::Duration;

pub mod bbr;

/// The refills per second of the bandwidth limiters, bounding a burst to a tenth of a
/// second of bandwidth instead of a full second.
const BANDWIDTH_LIMITER_REFILLS_PER_SECOND: u32 = 10;

/// Creates a leaky bucket limiter for the bandwidth limit in bytes per second.
pub fn new_bandwidth_limiter(limit: ByteSize) -> RateLimiter {
    let refill = (limit.as_u64() / BANDWIDTH_LIMITER_REFILLS_PER_SECOND as u64) as usize;
    RateLimiter::builder()
        .initial(refill)
        .refill(refill)
        .max(refill)
        .interval(Duration::from_secs(1) / BANDWIDTH_LIMITER_REFILLS_PER_SECOND)
        .fair(false)
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_bandwidth_limiter_refills_a_tenth_of_the_limit_every_100ms() {
        let test_cases = vec![(ByteSize::b(1000), 100), (ByteSize::gb(1), 100_000_000)];

        for (limit, refill) in test_cases {
            let limiter = new_bandwidth_limiter(limit);
            assert_eq!(limiter.refill(), refill);
            assert_eq!(limiter.max(), refill);
            assert_eq!(limiter.balance(), refill);
            assert_eq!(limiter.interval(), Duration::from_millis(100));
        }
    }
}
