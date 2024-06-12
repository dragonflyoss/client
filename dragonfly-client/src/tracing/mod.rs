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
use rolling_file::*;
use std::fs;
use std::fs::OpenOptions;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use tracing::{info, Level};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_log::LogTracer;
use tracing_subscriber::{
    filter::LevelFilter,
    fmt::{time::ChronoLocal, Layer},
    prelude::*,
    EnvFilter, Registry,
};

pub fn init_tracing(
    name: &str,
    log_dir: &PathBuf,
    log_level: Level,
    log_max_files: usize,
    jaeger_addr: Option<String>,
    redirect_stderr: bool,
    verbose: bool,
) -> Vec<WorkerGuard> {
    let mut guards = vec![];

    // Setup stdout layer.
    let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());

    // Initialize stdout layer.
    let stdout_filter = if verbose {
        LevelFilter::DEBUG
    } else {
        LevelFilter::OFF
    };
    let stdout_logging_layer = Layer::new()
        .with_writer(stdout_writer)
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_thread_names(false)
        .with_thread_ids(false)
        .with_timer(ChronoLocal::rfc_3339())
        .pretty()
        .with_filter(stdout_filter);
    guards.push(stdout_guard);

    // Setup file layer.
    fs::create_dir_all(log_dir).expect("failed to create log directory");
    let rolling_appender = BasicRollingFileAppender::new(
        log_dir.join(name).with_extension("log"),
        RollingConditionBasic::new().hourly(),
        log_max_files,
    )
    .expect("failed to create rolling file appender");

    let (rolling_writer, rolling_writer_guard) = tracing_appender::non_blocking(rolling_appender);
    let file_logging_layer = Layer::new()
        .with_writer(rolling_writer)
        .with_ansi(false)
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_thread_names(false)
        .with_thread_ids(false)
        .with_timer(ChronoLocal::rfc_3339())
        .compact();
    guards.push(rolling_writer_guard);

    // Setup env filter for log level.
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::default().add_directive(log_level.into()));

    let subscriber = Registry::default()
        .with(env_filter)
        .with(file_logging_layer)
        .with(stdout_logging_layer);

    // Setup jaeger layer.
    if let Some(jaeger_addr) = jaeger_addr {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());
        let tracer = opentelemetry_jaeger::new_agent_pipeline()
            .with_service_name(name)
            .with_endpoint(jaeger_addr)
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

    info!(
        "tracing initialized directory: {}, level: {}",
        log_dir.as_path().display(),
        log_level
    );

    // Redirect stderr to file.
    if redirect_stderr {
        redirect_stderr_to_file(log_dir);
    }

    guards
}

// Redirect stderr to file.
fn redirect_stderr_to_file(log_dir: &Path) {
    let log_path = log_dir.join("stderr.log");
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_path)
        .unwrap();

    unsafe {
        libc::dup2(file.as_raw_fd(), libc::STDERR_FILENO);
    }
}
