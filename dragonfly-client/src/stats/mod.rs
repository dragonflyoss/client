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
use pprof::protos::Message;
use pprof::ProfilerGuard;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tracing::{error, info, instrument};

/// The default seconds to start profiling.
const DEFAULT_PROFILER_SECONDS: u64 = 10;

/// The default frequency to start profiling.
const DEFAULT_PROFILER_FREQUENCY: i32 = 1000;

/// The query params to start profiling.
pub struct PProfProfileQueryParams {
    /// The seconds to start profiling.
    pub seconds: u64,

    /// The frequency to start profiling.
    pub frequency: i32,
}

/// Implements the default.
impl Default for PProfProfileQueryParams {
    fn default() -> Self {
        Self {
            seconds: DEFAULT_PROFILER_SECONDS,
            frequency: DEFAULT_PROFILER_FREQUENCY,
        }
    }
}

/// Implements the query params.
impl PProfProfileQueryParams {
    /// Parses the query params from the query string.
    fn from_query(query: &str) -> Self {
        let mut params = Self::default();
        for (key, value) in query.split('&').filter_map(|pair| pair.split_once('=')) {
            match key {
                "seconds" => params.seconds = value.parse().unwrap_or(params.seconds),
                "frequency" => params.frequency = value.parse().unwrap_or(params.frequency),
                _ => {}
            }
        }

        params
    }
}

/// The stats server.
#[derive(Debug)]
pub struct Stats {
    /// The address of the stats server.
    addr: SocketAddr,

    /// Used to shutdown the stats server.
    shutdown: shutdown::Shutdown,

    /// Used to notify the stats server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// Implements the stats server.
impl Stats {
    /// Creates a new Stats.
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

    /// Starts the stats server.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Start the stats server and wait for it to finish.
        info!("stats server listening on {}", self.addr);
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
                    // Stats server shutting down with signals.
                    info!("stats server shutting down");
                    return;
                }
            }
        }
    }

    /// Handles the stats request.
    #[instrument(skip_all)]
    async fn handler<T>(request: Request<T>) -> Result<Response<Full<Bytes>>, Infallible> {
        match (request.method(), request.uri().path()) {
            (&Method::GET, "/debug/pprof/profile") => {
                let query_params = request
                    .uri()
                    .query()
                    .map(PProfProfileQueryParams::from_query)
                    .unwrap_or_default();

                match Self::pprof_profile_handler(query_params).await {
                    Ok(body) => Ok(Response::new(Full::new(Bytes::from(body)))),
                    Err(()) => Ok(Response::builder()
                        .status(StatusCode::INTERNAL_SERVER_ERROR)
                        .body(Full::default())
                        .unwrap()),
                }
            }
            (&Method::GET, "/debug/pprof/heap") => match Self::pprof_heap_handler().await {
                Ok(body) => Ok(Response::new(Full::new(Bytes::from(body)))),
                Err(()) => Ok(Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Full::default())
                    .unwrap()),
            },
            _ => Ok(Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::default())
                .unwrap()),
        }
    }

    /// Handles the pprof profile request.
    #[instrument(skip_all)]
    async fn pprof_profile_handler(query_params: PProfProfileQueryParams) -> Result<Vec<u8>, ()> {
        info!(
            "start profiling for {} seconds with {} frequency",
            query_params.seconds, query_params.frequency
        );

        let guard = ProfilerGuard::new(query_params.frequency).map_err(|err| {
            error!("failed to create profiler guard: {}", err);
        })?;

        tokio::time::sleep(Duration::from_secs(query_params.seconds)).await;
        let report = guard.report().build().map_err(|err| {
            error!("failed to build profiler report: {}", err);
        })?;

        let profile = report.pprof().map_err(|err| {
            error!("failed to get pprof profile: {}", err);
        })?;

        let mut body: Vec<u8> = Vec::new();
        profile.write_to_vec(&mut body).map_err(|err| {
            error!("failed to write pprof profile: {}", err);
        })?;

        Ok(body)
    }

    /// Handles the pprof heap request.
    #[instrument(skip_all)]
    async fn pprof_heap_handler() -> Result<Vec<u8>, ()> {
        info!("start heap profiling");
        #[cfg(target_os = "linux")]
        {
            // PROF_CTL is None when jemalloc profiling is disabled
            // (release builds set prof:false).
            let Some(prof_ctl) = jemalloc_pprof::PROF_CTL.as_ref() else {
                return Err(());
            };

            let mut prof_ctl = prof_ctl.lock().await;
            if !prof_ctl.activated() {
                return Err(());
            }

            let pprof = prof_ctl.dump_pprof().map_err(|err| {
                error!("failed to dump pprof: {}", err);
            })?;

            Ok(pprof)
        }

        #[cfg(not(target_os = "linux"))]
        Err(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn pprof_profile_query_params_from_query_falls_back_to_defaults() {
        let test_cases = vec![
            ("seconds=25&frequency=1500", 25, 1500),
            ("seconds=25", 25, DEFAULT_PROFILER_FREQUENCY),
            ("frequency=1500", DEFAULT_PROFILER_SECONDS, 1500),
            (
                "seconds=invalid&unknown=1",
                DEFAULT_PROFILER_SECONDS,
                DEFAULT_PROFILER_FREQUENCY,
            ),
            ("", DEFAULT_PROFILER_SECONDS, DEFAULT_PROFILER_FREQUENCY),
        ];

        for (query, expected_seconds, expected_frequency) in test_cases {
            let params = PProfProfileQueryParams::from_query(query);
            assert_eq!(params.seconds, expected_seconds);
            assert_eq!(params.frequency, expected_frequency);
        }
    }

    #[test]
    fn stats_new_keeps_the_addr() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080);
        let shutdown = shutdown::Shutdown::new();
        let (shutdown_complete_tx, _shutdown_complete_rx) = mpsc::unbounded_channel();
        let stats = Stats::new(addr, shutdown, shutdown_complete_tx);

        assert_eq!(stats.addr, addr);
    }

    #[tokio::test]
    async fn handler_returns_not_found_for_unknown_routes() {
        let test_cases = vec![
            (Method::GET, "/unknown"),
            (Method::POST, "/debug/pprof/profile"),
            (Method::POST, "/debug/pprof/heap"),
        ];

        for (method, path) in test_cases {
            let request = Request::builder()
                .method(method.clone())
                .uri(path)
                .body(())
                .unwrap();
            let response = Stats::handler(request).await.unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
    }

    #[tokio::test]
    async fn pprof_profile_handler_builds_a_profile_for_each_frequency() {
        for frequency in [1000, 500] {
            let params = PProfProfileQueryParams {
                seconds: 0,
                frequency,
            };
            let result = Stats::pprof_profile_handler(params).await;
            assert!(result.is_ok());
        }
    }

    #[cfg(not(target_os = "linux"))]
    #[tokio::test]
    async fn pprof_heap_handler_fails_on_non_linux() {
        let result = Stats::pprof_heap_handler().await;
        assert!(result.is_err());
    }
}
