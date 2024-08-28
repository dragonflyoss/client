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
use tracing::{error, info, instrument};
use warp::{Filter, Rejection, Reply};

// DEFAULT_PROFILER_SECONDS is the default seconds to start profiling.
const DEFAULT_PROFILER_SECONDS: u64 = 10;

// DEFAULT_PROFILER_FREQUENCY is the default frequency to start profiling.
const DEFAULT_PROFILER_FREQUENCY: i32 = 1000;

// PProfProfileQueryParams is the query params to start profiling.
#[derive(Deserialize, Serialize)]
#[serde(default)]
pub struct PProfProfileQueryParams {
    // seconds is the seconds to start profiling.
    pub seconds: u64,

    // frequency is the frequency to start profiling.
    pub frequency: i32,
}

// PProfProfileQueryParams implements the default.
impl Default for PProfProfileQueryParams {
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
    #[instrument(skip_all)]
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
    #[instrument(skip_all)]
    pub async fn run(&self) {
        // Clone the shutdown channel.
        let mut shutdown = self.shutdown.clone();

        // Create the pprof profile route.
        let pprof_profile_route = warp::path!("debug" / "pprof" / "profile")
            .and(warp::get())
            .and(warp::query::<PProfProfileQueryParams>())
            .and_then(Self::pprof_profile_handler);

        // Create the pprof heap route.
        let pprof_heap_route = warp::path!("debug" / "pprof" / "heap")
            .and(warp::get())
            .and_then(Self::pprof_heap_handler);

        // Create the pprof routes.
        let pprof_routes = pprof_profile_route.or(pprof_heap_route);

        // Start the stats server and wait for it to finish.
        info!("stats server listening on {}", self.addr);
        tokio::select! {
            _ = warp::serve(pprof_routes).run(self.addr) => {
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
    #[instrument(skip_all)]
    async fn pprof_profile_handler(
        query_params: PProfProfileQueryParams,
    ) -> Result<impl Reply, Rejection> {
        info!(
            "start profiling for {} seconds with {} frequency",
            query_params.seconds, query_params.frequency
        );

        let guard = ProfilerGuard::new(query_params.frequency).map_err(|err| {
            error!("failed to create profiler guard: {}", err);
            warp::reject::reject()
        })?;

        tokio::time::sleep(Duration::from_secs(query_params.seconds)).await;
        let report = guard.report().build().map_err(|err| {
            error!("failed to build profiler report: {}", err);
            warp::reject::reject()
        })?;

        let profile = report.pprof().map_err(|err| {
            error!("failed to get pprof profile: {}", err);
            warp::reject::reject()
        })?;

        let mut body: Vec<u8> = Vec::new();
        profile.write_to_vec(&mut body).map_err(|err| {
            error!("failed to write pprof profile: {}", err);
            warp::reject::reject()
        })?;

        Ok(body)
    }

    // pprof_heap_handler handles the pprof heap request.
    #[instrument(skip_all)]
    async fn pprof_heap_handler() -> Result<impl Reply, Rejection> {
        info!("start heap profiling");
        #[cfg(target_os = "linux")]
        {
            let mut prof_ctl = jemalloc_pprof::PROF_CTL.as_ref().unwrap().lock().await;
            if !prof_ctl.activated() {
                return Err(warp::reject::reject());
            }

            let pprof = prof_ctl.dump_pprof().map_err(|err| {
                error!("failed to dump pprof: {}", err);
                warp::reject::reject()
            })?;

            Ok(pprof)
        }

        #[cfg(not(target_os = "linux"))]
        Err::<warp::http::Error, Rejection>(warp::reject::reject())
    }
}
