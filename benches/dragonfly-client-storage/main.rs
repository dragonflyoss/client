/*
 *     Copyright 2025 The Dragonfly Authors
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

use criterion::{criterion_group, criterion_main};

mod cache;
mod lru_cache;

criterion_group!(
    benches,
    cache::put_task_in_cache,
    cache::write_piece_in_cache,
    cache::read_piece_from_cache,
    lru_cache::lru_put,
    lru_cache::lru_get,
    lru_cache::lru_peek,
    lru_cache::lru_contains,
    lru_cache::lru_pop_lru,
);

criterion_main!(benches);
