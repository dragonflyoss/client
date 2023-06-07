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

use std::future::Future;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use tracing::info;
use warp::{Filter, Rejection, Reply};

// Health is the health server.
#[derive(Debug, Clone)]
pub struct Health {
    // addr is the address of the health server.
    pub addr: SocketAddr,
}

// Health implements the health server.
impl Health {
    // new creates a new Metrics.
    pub fn new(enable_ipv6: bool) -> Self {
        if enable_ipv6 {
            return Self {
                addr: SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 40901),
            };
        }

        Self {
            addr: SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 40901),
        }
    }

    // run starts the metrics server.
    pub async fn run(&self, shutdown: impl Future) {
        let health_route = warp::path!("healthy")
            .and(warp::get())
            .and(warp::path::end())
            .and_then(Self::health_handler);

        tokio::select! {
            _ = warp::serve(health_route).run(self.addr) => {
                // Health server ended.
                info!("health server ended");
            }
            _ = shutdown => {
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
