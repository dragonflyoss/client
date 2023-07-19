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

use opentelemetry::sdk::propagation::TraceContextPropagator;
use std::net::SocketAddr;
use std::path::PathBuf;
use tracing::Level;
use tracing_appender::non_blocking::WorkerGuard;
use tracing_appender::rolling::{RollingFileAppender, Rotation};
use tracing_log::LogTracer;
use tracing_subscriber::{fmt::Layer, prelude::*, EnvFilter, Registry};

pub fn init_tracing(
    name: &str,
    log_dir: &PathBuf,
    log_level: Level,
    jaeger_addr: Option<SocketAddr>,
) -> Vec<WorkerGuard> {
    let mut guards = vec![];

    // Setup stdout layer.
    let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
    let stdout_logging_layer = Layer::new().with_writer(stdout_writer);
    guards.push(stdout_guard);

    // Setup file layer.
    let rolling_appender = RollingFileAppender::new(Rotation::DAILY, log_dir, name);
    let (rolling_writer, rolling_writer_guard) = tracing_appender::non_blocking(rolling_appender);
    let file_logging_layer = Layer::new().with_writer(rolling_writer).with_ansi(false);
    guards.push(rolling_writer_guard);

    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::default().add_directive(log_level.into()));

    let subscriber = Registry::default()
        .with(env_filter)
        .with(stdout_logging_layer)
        .with(file_logging_layer);

    // Setup jaeger layer.
    if let Some(jaeger_addr) = jaeger_addr {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let tracer = opentelemetry_jaeger::new_agent_pipeline()
            .with_service_name(name)
            .with_endpoint(jaeger_addr.to_string())
            .install_batch(opentelemetry::runtime::Tokio)
            .expect("install");
        let jaeger_layer = tracing_opentelemetry::layer().with_tracer(tracer);
        let subscriber = subscriber.with(jaeger_layer);

        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set global subscriber");
    } else {
        tracing::subscriber::set_global_default(subscriber)
            .expect("failed to set global subscriber");
    }

    LogTracer::init().expect("failed to init LogTracer");

    guards
}
