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

use crate::shutdown;
use pprof::protos::Message;
use pprof::ProfilerGuard;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{error, info};
use warp::{Filter, Rejection, Reply};

// DEFAULT_PROFILER_SECONDS is the default seconds to start profiling.
const DEFAULT_PROFILER_SECONDS: u64 = 10;

// DEFAULT_PROFILER_FREQUENCY is the default frequency to start profiling.
const DEFAULT_PROFILER_FREQUENCY: i32 = 1000;

// PProfQueryParams is the query params to start profiling.
#[derive(Deserialize, Serialize)]
#[serde(default)]
pub struct PProfQueryParams {
    // seconds is the seconds to start profiling.
    pub seconds: u64,

    // frequency is the frequency to start profiling.
    pub frequency: i32,
}

// PProfQueryParams implements the default.
impl Default for PProfQueryParams {
    fn default() -> Self {
        Self {
            seconds: DEFAULT_PROFILER_SECONDS,
            frequency: DEFAULT_PROFILER_FREQUENCY,
        }
    }
}

// Stats is the stats server.
#[derive(Debug)]
pub struct Stats {
    // addr is the address of the stats server.
    addr: SocketAddr,

    // shutdown is used to shutdown the stats server.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the stats server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// Stats implements the stats server.
impl Stats {
    // new creates a new Stats.
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

    // run starts the stats server.
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Create the stats route.
        let stats_route = warp::path!("debug" / "pprof" / "profile")
            .and(warp::get())
            .and(warp::query::<PProfQueryParams>())
            .and_then(Self::stats_handler);

        // Start the stats server and wait for it to finish.
        info!("stats server listening on {}", self.addr);
        tokio::select! {
            _ = warp::serve(stats_route).run(self.addr) => {
                // Stats server ended.
                info!("stats server ended");
            }
            _ = shutdown.recv() => {
                // Stats server shutting down with signals.
                info!("stats server shutting down");
            }
        }
    }

    // stats_handler handles the stats request.
    async fn stats_handler(query_params: PProfQueryParams) -> Result<impl Reply, Rejection> {
        info!(
            "start profiling for {} seconds with {} frequency",
            query_params.seconds, query_params.frequency
        );
        let guard = match ProfilerGuard::new(query_params.frequency) {
            Ok(guard) => guard,
            Err(err) => {
                error!("failed to create profiler guard: {}", err);
                return Err(warp::reject::reject());
            }
        };

        tokio::time::sleep(Duration::from_secs(query_params.seconds)).await;
        let report = match guard.report().build() {
            Ok(report) => report,
            Err(err) => {
                error!("failed to build profiler report: {}", err);
                return Err(warp::reject::reject());
            }
        };

        let profile = match report.pprof() {
            Ok(profile) => profile,
            Err(err) => {
                error!("failed to get pprof profile: {}", err);
                return Err(warp::reject::reject());
            }
        };

        let mut body: Vec<u8> = Vec::new();
        profile.write_to_vec(&mut body).map_err(|err| {
            error!("failed to write pprof profile: {}", err);
            warp::reject::reject()
        })?;

        Ok(body)
    }
}
