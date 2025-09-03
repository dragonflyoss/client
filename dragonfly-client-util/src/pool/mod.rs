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

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, info};

/// RequestGuard automatically tracks active requests for a client.
pub struct RequestGuard {
    active_requests: Arc<AtomicUsize>,
}

impl RequestGuard {
    fn new(active_requests: Arc<AtomicUsize>) -> Self {
        active_requests.fetch_add(1, Ordering::SeqCst);
        Self { active_requests }
    }
}

impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.active_requests.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Entry wrapper for clients in the pool.
#[derive(Clone)]
pub struct Entry<T> {
    /// client is the generic client instance.
    pub client: T,

    /// active_requests is the number of the active requests.
    active_requests: Arc<AtomicUsize>,

    /// actived_at is the time when the client is the last active time.
    actived_at: Arc<std::sync::Mutex<Instant>>,
}

impl<T> Entry<T> {
    fn new(client: T) -> Self {
        Self {
            client,
            active_requests: Arc::new(AtomicUsize::new(0)),
            actived_at: Arc::new(std::sync::Mutex::new(Instant::now())),
        }
    }

    /// Create a request guard to track active requests.
    pub fn request_guard(&self) -> RequestGuard {
        RequestGuard::new(self.active_requests.clone())
    }

    /// Update the last active time.
    fn update_actived_at(&self) {
        *self.actived_at.lock().unwrap() = Instant::now();
    }

    /// Check if the client has active requests.
    fn has_active_requests(&self) -> bool {
        self.active_requests.load(Ordering::SeqCst) > 0
    }

    /// Get the idle duration since last active.
    fn idle_duration(&self) -> Duration {
        let actived_at = self.actived_at.lock().unwrap();
        Instant::now().duration_since(*actived_at)
    }
}

/// Factory trait for creating new clients.
#[tonic::async_trait]
pub trait Factory<K, T> {
    type Error;

    async fn make_entry(&self, key: &K) -> Result<T, Self::Error>;
}

/// Generic client pool for managing reusable clients with automatic cleanup.
pub struct Pool<K, T, F> {
    /// factory is the factory for creating new clients.
    factory: F,

    /// clients is the map of clients.
    clients: Arc<Mutex<HashMap<K, Entry<T>>>>,

    /// capacity is the capacity of the clients. If the number of the
    /// clients exceeds the capacity, it will clean up the idle clients.
    capacity: usize,

    /// client_idle_timeout is the idle timeout for the client. If the client is idle for a long
    /// time, it will be removed when cleaning up the idle clients.
    idle_timeout: Duration,

    /// cleanup_at is the time when the client is the last cleanup time.
    cleanup_at: Arc<Mutex<Instant>>,
}

impl<K, T, F> Pool<K, T, F>
where
    K: Clone + Eq + Hash + std::fmt::Display,
    T: Clone,
    F: Factory<K, T>,
{
    /// Create a new client pool.
    pub fn new(factory: F, capacity: usize, idle_timeout: Duration) -> Self {
        Self {
            factory,
            clients: Arc::new(Mutex::new(HashMap::new())),
            capacity,
            idle_timeout,
            cleanup_at: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// Get or create a client entry for the given key.
    pub async fn get_or_create(&self, key: &K) -> Result<Entry<T>, F::Error> {
        // Cleanup idle clients first.
        self.cleanup_idle_clients().await;

        // Try to get existing client.
        {
            let clients = self.clients.lock().await;
            if let Some(entry) = clients.get(key) {
                debug!("reusing client: {}", key);
                entry.update_actived_at();
                return Ok(entry.clone());
            }
        }

        // Create new client.
        debug!("creating client: {}", key);
        let client = self.factory.make_entry(key).await?;

        let mut clients = self.clients.lock().await;
        let entry = clients
            .entry(key.clone())
            .or_insert_with(|| Entry::new(client));

        entry.update_actived_at();
        Ok(entry.clone())
    }

    /// Remove a client entry if it has no active requests.
    pub async fn remove_if_idle(&self, key: &K) {
        let mut clients = self.clients.lock().await;
        if let Some(entry) = clients.get(key) {
            if !entry.has_active_requests() {
                clients.remove(key);
            }
        }
    }

    /// Cleanup idle clients that exceed capacity or idle timeout.
    async fn cleanup_idle_clients(&self) {
        let now = Instant::now();

        // Avoid hot cleanup.
        {
            let cleanup_at = self.cleanup_at.lock().await;
            let interval = self.idle_timeout / 2;
            if now.duration_since(*cleanup_at) < interval {
                debug!("avoid hot cleanup");
                return;
            }
        }

        let mut clients = self.clients.lock().await;
        let exceeds_capacity = clients.len() > self.capacity;

        clients.retain(|key, entry| {
            let has_active_requests = entry.has_active_requests();
            let idle_duration = entry.idle_duration();
            let is_recent = idle_duration <= self.idle_timeout;

            let should_retain = has_active_requests || (!exceeds_capacity && is_recent);

            if !should_retain {
                info!(
                    "removing idle client: {}, exceeds_capacity: {}, idle_duration: {}s",
                    key,
                    exceeds_capacity,
                    idle_duration.as_secs(),
                );
            }

            should_retain
        });

        *self.cleanup_at.lock().await = now;
    }

    /// Get current pool size.
    pub async fn size(&self) -> usize {
        self.clients.lock().await.len()
    }

    /// Clear all clients from the pool.
    pub async fn clear(&self) {
        self.clients.lock().await.clear();
    }
}
