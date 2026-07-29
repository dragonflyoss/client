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

use bytes::Bytes;
use dragonfly_client_util::shutdown;
use http_body_util::Full;
use hyper::server::conn::http1::Builder as ServerBuilder;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use std::convert::Infallible;
use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{error, info, instrument};

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

/// Implements the health server.
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

        // Start the health server and wait for it to finish.
        info!("health server listening on {}", self.addr);
        let listener = TcpListener::bind(self.addr).await.unwrap();
        loop {
            tokio::select! {
                tcp_accepted = listener.accept() => {
                    let (tcp, remote_address) = match tcp_accepted {
                        Ok(tcp_accepted) => tcp_accepted,
                        Err(err) => {
                            error!("failed to accept connection: {}", err);
                            continue;
                        }
                    };

                    let io = TokioIo::new(tcp);
                    tokio::spawn(async move {
                        if let Err(err) = ServerBuilder::new()
                            .serve_connection(io, service_fn(Self::handler))
                            .await
                        {
                            error!("failed to serve connection from {}: {}", remote_address, err);
                        }
                    });
                }
                _ = shutdown.recv() => {
                    // Health server shutting down with signals.
                    info!("health server shutting down");
                    return;
                }
            }
        }
    }

    /// Handles the health check request.
    #[instrument(skip_all)]
    async fn handler<T>(request: Request<T>) -> Result<Response<Full<Bytes>>, Infallible> {
        match (request.method(), request.uri().path()) {
            (&Method::GET, "/healthy") => Ok(Response::new(Full::default())),
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::default())
                .unwrap()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_health_new() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let shutdown = shutdown::Shutdown::new();
        let (shutdown_complete_tx, _shutdown_complete_rx) = mpsc::unbounded_channel();
        let health = Health::new(addr, shutdown, shutdown_complete_tx);

        assert_eq!(health.addr, addr);
    }

    #[tokio::test]
    async fn test_handler() {
        let request = Request::builder()
            .method(Method::GET)
            .uri("/healthy")
            .body(())
            .unwrap();
        let response = Health::handler(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let request = Request::builder()
            .method(Method::GET)
            .uri("/unknown")
            .body(())
            .unwrap();
        let response = Health::handler(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}
