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

use dragonfly_client_config::dfdaemon::Host;
use opentelemetry::{global, trace::TracerProvider, KeyValue};
use opentelemetry_otlp::{WithExportConfig, WithTonicConfig};
use opentelemetry_sdk::{propagation::TraceContextPropagator, Resource};
use rolling_file::*;
use std::fs;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use tonic::metadata::{MetadataKey, MetadataMap, MetadataValue};
use tracing::{info, Level};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_opentelemetry::OpenTelemetryLayer;
use tracing_subscriber::{
    filter::LevelFilter,
    fmt::{time::ChronoLocal, Layer},
    prelude::*,
    EnvFilter, Registry,
};

/// SPAN_EXPORTER_TIMEOUT is the timeout for the span exporter.
const SPAN_EXPORTER_TIMEOUT: Duration = Duration::from_secs(10);

/// init_tracing initializes the tracing system.
#[allow(clippy::too_many_arguments)]
pub fn init_tracing(
    name: &str,
    log_dir: PathBuf,
    log_level: Level,
    log_max_files: usize,
    otel_protocol: Option<String>,
    otel_endpoint: Option<String>,
    otel_path: Option<PathBuf>,
    otel_headers: Option<reqwest::header::HeaderMap>,
    host: Option<Host>,
    is_seed_peer: bool,
    console: bool,
) -> Vec<WorkerGuard> {
    let mut guards = vec![];

    // Setup stdout layer.
    let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
    guards.push(stdout_guard);

    // Initialize stdout layer.
    let stdout_filter = if console {
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

    // Setup file layer.
    fs::create_dir_all(log_dir.clone()).expect("failed to create log directory");
    let rolling_appender = BasicRollingFileAppender::new(
        log_dir.join(name).with_extension("log"),
        RollingConditionBasic::new().hourly(),
        log_max_files,
    )
    .expect("failed to create rolling file appender");

    let (rolling_writer, rolling_writer_guard) = tracing_appender::non_blocking(rolling_appender);
    guards.push(rolling_writer_guard);

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

    // Setup env filter for log level.
    let env_filter = EnvFilter::from_default_env().add_directive(log_level.into());

    // Enable console subscriber layer for tracing spawn tasks on `127.0.0.1:6669` when log level is TRACE.
    let console_subscriber_layer = if log_level == Level::TRACE {
        Some(console_subscriber::spawn())
    } else {
        None
    };

    let subscriber = Registry::default()
        .with(env_filter)
        .with(console_subscriber_layer)
        .with(file_logging_layer)
        .with(stdout_logging_layer);

    // If OTLP protocol and endpoint are provided, set up OpenTelemetry tracing.
    if let (Some(protocol), Some(endpoint)) = (otel_protocol, otel_endpoint) {
        let otlp_exporter = match protocol.as_str() {
            "grpc" => {
                let mut metadata = MetadataMap::new();
                if let Some(headers) = otel_headers {
                    for (key, value) in headers.iter() {
                        metadata.insert(
                            MetadataKey::from_str(key.as_str())
                                .expect("failed to create metadata key"),
                            MetadataValue::from_str(value.to_str().unwrap())
                                .expect("failed to create metadata value"),
                        );
                    }
                }

                let endpoint_url = url::Url::parse(&format!("http://{}", endpoint))
                    .expect("failed to parse OTLP endpoint URL");

                opentelemetry_otlp::SpanExporter::builder()
                    .with_tonic()
                    .with_endpoint(endpoint_url)
                    .with_timeout(SPAN_EXPORTER_TIMEOUT)
                    .with_metadata(metadata)
                    .build()
                    .expect("failed to create OTLP exporter")
            }
            "http" | "https" => {
                let mut endpoint_url = url::Url::parse(&format!("{}://{}", protocol, endpoint))
                    .expect("failed to parse OTLP endpoint URL");

                if let Some(path) = otel_path {
                    endpoint_url = endpoint_url
                        .join(path.to_str().unwrap())
                        .expect("failed to join OTLP endpoint path");
                }

                opentelemetry_otlp::SpanExporter::builder()
                    .with_http()
                    .with_endpoint(endpoint_url.as_str())
                    .with_protocol(opentelemetry_otlp::Protocol::HttpJson)
                    .with_timeout(SPAN_EXPORTER_TIMEOUT)
                    .build()
                    .expect("failed to create OTLP exporter")
            }
            _ => {
                panic!("unsupported OTLP protocol: {}", protocol);
            }
        };

        let host = host.unwrap();
        let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
            .with_batch_exporter(otlp_exporter)
            .with_resource(
                Resource::builder()
                    .with_service_name(format!("{}-{}", name, host.ip.unwrap()))
                    .with_schema_url(
                        [
                            KeyValue::new(
                                opentelemetry_semantic_conventions::attribute::SERVICE_NAMESPACE,
                                "dragonfly",
                            ),
                            KeyValue::new(
                                opentelemetry_semantic_conventions::attribute::HOST_NAME,
                                host.hostname,
                            ),
                            KeyValue::new(
                                opentelemetry_semantic_conventions::attribute::HOST_IP,
                                host.ip.unwrap().to_string(),
                            ),
                        ],
                        opentelemetry_semantic_conventions::SCHEMA_URL,
                    )
                    .with_attribute(opentelemetry::KeyValue::new(
                        "host.idc",
                        host.idc.unwrap_or_default(),
                    ))
                    .with_attribute(opentelemetry::KeyValue::new(
                        "host.location",
                        host.location.unwrap_or_default(),
                    ))
                    .with_attribute(opentelemetry::KeyValue::new("host.seed_peer", is_seed_peer))
                    .build(),
            )
            .build();

        let tracer = provider.tracer(name.to_string());
        global::set_tracer_provider(provider.clone());
        global::set_text_map_propagator(TraceContextPropagator::new());

        let jaeger_layer = OpenTelemetryLayer::new(tracer);
        subscriber.with(jaeger_layer).init();
    } else {
        subscriber.init();
    }

    std::panic::set_hook(Box::new(tracing_panic::panic_hook));
    info!(
        "tracing initialized directory: {}, level: {}",
        log_dir.as_path().display(),
        log_level
    );

    guards
}

/// init_command_tracing initializes the tracing system for command line tools.
#[allow(clippy::too_many_arguments)]
pub fn init_command_tracing(log_level: Level, console: bool) -> Vec<WorkerGuard> {
    let mut guards = vec![];

    // Setup stdout layer.
    let (stdout_writer, stdout_guard) = tracing_appender::non_blocking(std::io::stdout());
    guards.push(stdout_guard);

    // Initialize stdout layer.
    let stdout_filter = if console {
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

    // Setup env filter for log level.
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::default().add_directive(log_level.into()));

    // Enable console subscriber layer for tracing spawn tasks on `127.0.0.1:6669` when log level is TRACE.
    let console_subscriber_layer = if log_level == Level::TRACE {
        Some(console_subscriber::spawn())
    } else {
        None
    };

    let subscriber = Registry::default()
        .with(env_filter)
        .with(console_subscriber_layer)
        .with(stdout_logging_layer);
    subscriber.init();

    std::panic::set_hook(Box::new(tracing_panic::panic_hook));
    info!("tracing initialized level: {}", log_level);

    guards
}
