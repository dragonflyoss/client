/*
 *     Copyright 2024 The Dragonfly Authors
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

use dragonfly_client_util::shutdown;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tracing::{info, instrument};
use warp::{Filter, Rejection, Reply};

/// Health check server.
#[derive(Debug)]
pub struct Health {
    /// Address of the health server.
    addr: SocketAddr,

    /// Used to shut down the health server.
    shutdown: shutdown::Shutdown,

    /// Used to notify that the health server shutdown is complete.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// Health implements the health server.
impl Health {
    /// Creates a new health server.
    pub fn new(
        addr: SocketAddr,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            addr,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    /// Starts the health server.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Create the health route.
        let health_route = warp::path!("healthy")
            .and(warp::get())
            .and(warp::path::end())
            .and_then(Self::health_handler);

        // Start the health server and wait for it to finish.
        info!("health server listening on {}", self.addr);
        tokio::select! {
            _ = warp::serve(health_route).run(self.addr) => {
                // Health server ended.
                info!("health server ended");
            }
            _ = shutdown.recv() => {
                // Health server shutting down with signals.
                info!("health server shutting down");
            }
        }
    }

    /// Handles the health check request.
    #[instrument(skip_all)]
    async fn health_handler() -> Result<impl Reply, Rejection> {
        Ok(warp::reply())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_health_new() {
        // Create a test address.
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);

        // Create shutdown signal.
        let shutdown = shutdown::Shutdown::new();

        // Create shutdown complete channel.
        let (shutdown_complete_tx, _shutdown_complete_rx) = mpsc::unbounded_channel();

        // Create health server.
        let health = Health::new(addr, shutdown, shutdown_complete_tx);

        // Verify the address is set correctly.
        assert_eq!(health.addr, addr);
    }

    #[tokio::test]
    async fn test_health_handler() {
        // Call the health handler.
        let result = Health::health_handler().await;

        // Verify the handler returns Ok.
        assert!(result.is_ok());
    }
}
