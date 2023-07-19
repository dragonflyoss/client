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

use crate::config;
use crate::shutdown;
use lazy_static::lazy_static;
use prometheus::{gather, Encoder, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tracing::{error, info};
use warp::{Filter, Rejection, Reply};

lazy_static! {
    // REGISTRY is used to register all metrics.
    pub static ref REGISTRY: Registry = Registry::new();

    // VERSION_GAUGE is used to record the version info of the service.
    pub static ref VERSION_GAUGE: IntGaugeVec =
        IntGaugeVec::new(
            Opts::new("version", "Version info of the service.").namespace(config::SERVICE_NAME).subsystem(config::NAME),
            &["major", "minor", "git_version", "git_commit", "platform", "build_time"]
        ).expect("metric can be created");

    // DOWNLOAD_PEER_COUNT is used to count the number of download peers.
    pub static ref DOWNLOAD_PEER_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("download_peer_total", "Counter of the number of the download peer.").namespace(config::SERVICE_NAME).subsystem(config::NAME),
            &["task_type"]
        ).expect("metric can be created");
}

// Metrics is the metrics server.
#[derive(Debug)]
pub struct Metrics {
    // addr is the address of the metrics server.
    addr: SocketAddr,

    // shutdown is used to shutdown the metrics server.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the metrics server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// Metrics implements the metrics server.
impl Metrics {
    // new creates a new Metrics.
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

    // run starts the metrics server.
    pub async fn run(&mut self) {
        self.register_custom_metrics();

        let metrics_route = warp::path!("metrics")
            .and(warp::get())
            .and(warp::path::end())
            .and_then(Self::metrics_handler);

        // Start the metrics server and wait for it to finish.
        tokio::select! {
            _ = warp::serve(metrics_route).run(self.addr) => {
                // Metrics server ended.
                info!("metrics server ended");
            }
            _ = self.shutdown.recv() => {
                // Metrics server shutting down with signals.
                info!("metrics server shutting down");
            }
        }
    }

    // register_custom_metrics registers all custom metrics.
    fn register_custom_metrics(&self) {
        REGISTRY
            .register(Box::new(VERSION_GAUGE.clone()))
            .expect("metric can be registered");

        REGISTRY
            .register(Box::new(DOWNLOAD_PEER_COUNT.clone()))
            .expect("metric can be registered");
    }

    // metrics_handler handles the metrics request.
    async fn metrics_handler() -> Result<impl Reply, Rejection> {
        let encoder = TextEncoder::new();

        // Encode custom metrics.
        let mut buf = Vec::new();
        if let Err(err) = encoder.encode(&REGISTRY.gather(), &mut buf) {
            error!("could not encode custom metrics: {}", err);
        };

        let mut res = match String::from_utf8(buf.clone()) {
            Ok(v) => v,
            Err(err) => {
                error!("custom metrics could not be from_utf8'd: {}", err);
                String::default()
            }
        };
        buf.clear();

        // Encode prometheus metrics.
        let mut buf = Vec::new();
        if let Err(err) = encoder.encode(&gather(), &mut buf) {
            error!("could not encode prometheus metrics: {}", err);
        };

        let res_custom = match String::from_utf8(buf.clone()) {
            Ok(v) => v,
            Err(err) => {
                error!("prometheus metrics could not be from_utf8'd: {}", err);
                String::default()
            }
        };
        buf.clear();

        res.push_str(&res_custom);
        Ok(res)
    }
}
