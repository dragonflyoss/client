/*
 *     Copyright 2023 The Dragonfly Authors
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

use crate::shutdown;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tracing::info;
use warp::{Filter, Rejection, Reply};

// Health is the health server.
#[derive(Debug)]
pub struct Health {
    // addr is the address of the health server.
    addr: SocketAddr,

    // shutdown is used to shutdown the health server.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the health server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// Health implements the health server.
impl Health {
    // new creates a new Health.
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

    // run starts the health server.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Create the health route.
        let health_route = warp::path!("healthy")
            .and(warp::get())
            .and(warp::path::end())
            .and_then(Self::health_handler);

        // Start the health server and wait for it to finish.
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

    // health_handler handles the health check request.
    async fn health_handler() -> Result<impl Reply, Rejection> {
        Ok(warp::reply())
    }
}
