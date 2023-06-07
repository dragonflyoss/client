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

use crate::config::{NAME, SERVICE_NAME};
use lazy_static::lazy_static;
use prometheus::{gather, Encoder, IntCounterVec, IntGaugeVec, Opts, Registry, TextEncoder};
use std::future::Future;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use tracing::error;
use tracing::info;
use warp::{Filter, Rejection, Reply};

lazy_static! {
    // REGISTRY is used to register all metrics.
    pub static ref REGISTRY: Registry = Registry::new();

    // VERSION_GAUGE is used to record the version info of the service.
    pub static ref VERSION_GAUGE: IntGaugeVec =
        IntGaugeVec::new(
            Opts::new("version", "Version info of the service.").namespace(SERVICE_NAME).subsystem(NAME),
            &["major", "minor", "git_version", "git_commit", "platform", "build_time"]
        ).expect("metric can be created");

    // DOWNLOAD_PEER_COUNT is used to count the number of download peers.
    pub static ref DOWNLOAD_PEER_COUNT: IntCounterVec =
        IntCounterVec::new(
            Opts::new("download_peer_total", "Counter of the number of the download peer.").namespace(SERVICE_NAME).subsystem(NAME),
            &["task_type"]
        ).expect("metric can be created");
}

// Metrics is the metrics server.
#[derive(Debug, Clone)]
pub struct Metrics {
    // addr is the address of the metrics server.
    addr: SocketAddr,
}

// Metrics implements the metrics server.
impl Metrics {
    // new creates a new Metrics.
    pub fn new(enable_ipv6: bool) -> Self {
        if enable_ipv6 {
            return Self {
                addr: SocketAddr::new(Ipv6Addr::UNSPECIFIED.into(), 8000),
            };
        }

        Self {
            addr: SocketAddr::new(Ipv4Addr::UNSPECIFIED.into(), 8000),
        }
    }

    // run starts the metrics server.
    pub async fn run(&self, shutdown: impl Future) {
        self.register_custom_metrics();

        let metrics_route = warp::path!("metrics").and_then(Self::metrics_handler);
        tokio::select! {
            _ = warp::serve(metrics_route).run(self.addr) => {
                // Metrics server ended.
                info!("metrics server ended");
            }
            _ = shutdown => {
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

        let mut buffer = Vec::new();
        if let Err(e) = encoder.encode(&REGISTRY.gather(), &mut buffer) {
            error!("could not encode custom metrics: {}", e);
        };
        let mut res = match String::from_utf8(buffer.clone()) {
            Ok(v) => v,
            Err(e) => {
                error!("custom metrics could not be from_utf8'd: {}", e);
                String::default()
            }
        };
        buffer.clear();

        let mut buffer = Vec::new();
        if let Err(e) = encoder.encode(&gather(), &mut buffer) {
            error!("could not encode prometheus metrics: {}", e);
        };
        let res_custom = match String::from_utf8(buffer.clone()) {
            Ok(v) => v,
            Err(e) => {
                error!("prometheus metrics could not be from_utf8'd: {}", e);
                String::default()
            }
        };
        buffer.clear();

        res.push_str(&res_custom);
        Ok(res)
    }
}
