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

use tonic::{metadata, service::Interceptor, Request, Status};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// Tracing metadata map container for span context.
struct MetadataMap<'a>(&'a mut metadata::MetadataMap);

/// Implements the otel tracing Extractor.
impl opentelemetry::propagation::Extractor for MetadataMap<'_> {
    /// Gets a value for a key from the `MetadataMap`.  If the value can't be converted to &str, returns None
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|metadata| metadata.to_str().ok())
    }

    /// Collects all the keys from the `MetadataMap`.
    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .map(|key| match key {
                tonic::metadata::KeyRef::Ascii(v) => v.as_str(),
                tonic::metadata::KeyRef::Binary(v) => v.as_str(),
            })
            .collect::<Vec<_>>()
    }
}

/// Implements the otel tracing Injector.
impl opentelemetry::propagation::Injector for MetadataMap<'_> {
    /// Sets a key-value pair to the injector.
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(val) = metadata::MetadataValue::try_from(&value) {
                self.0.insert(key, val);
            }
        }
    }
}

/// Auto-inject tracing gRPC interceptor.
#[derive(Clone)]
pub struct InjectTracingInterceptor;

/// Implements the tonic Interceptor interface.
impl Interceptor for InjectTracingInterceptor {
    /// Calls and injects tracing context into global propagator.
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        let context = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|prop| {
            prop.inject_context(&context, &mut MetadataMap(request.metadata_mut()));
        });

        Ok(request)
    }
}

/// Auto-extract tracing gRPC interceptor.
#[derive(Clone)]
pub struct ExtractTracingInterceptor;

/// Implements the tonic Interceptor interface.
impl Interceptor for ExtractTracingInterceptor {
    /// Calls and injects tracing context into global propagator.
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        let parent_cx = opentelemetry::global::get_text_map_propagator(|prop| {
            prop.extract(&MetadataMap(request.metadata_mut()))
        });

        request.extensions_mut().insert(parent_cx);
        Ok(request)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::type_complexity)]

    use super::*;
    use opentelemetry::propagation::{Extractor, Injector};
    use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceId};
    use opentelemetry_sdk::propagation::TraceContextPropagator;

    const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";

    #[test]
    fn metadata_map_set_drops_entries_tonic_rejects() {
        let test_cases: Vec<(&str, &str, fn(&MetadataMap))> = vec![
            ("traceparent", TRACEPARENT, |map| {
                assert_eq!(map.get("traceparent"), Some(TRACEPARENT));
            }),
            ("Trace-Parent", TRACEPARENT, |map| {
                assert_eq!(map.keys(), vec!["trace-parent"]);
            }),
            ("bad key", TRACEPARENT, |map| assert!(map.keys().is_empty())),
            ("traceparent-bin", TRACEPARENT, |map| {
                assert!(map.keys().is_empty());
            }),
            ("traceparent", "bad\nvalue", |map| {
                assert!(map.keys().is_empty());
            }),
        ];

        for (key, value, expect) in test_cases {
            let mut metadata = metadata::MetadataMap::new();
            let mut map = MetadataMap(&mut metadata);
            map.set(key, value.to_string());
            expect(&map);
        }
    }

    #[test]
    fn metadata_map_extractor_lists_ascii_and_binary_keys() {
        let mut metadata = metadata::MetadataMap::new();
        metadata.insert("traceparent", TRACEPARENT.parse().unwrap());
        metadata.insert_bin("baggage-bin", metadata::MetadataValue::from_bytes(b"value"));

        let map = MetadataMap(&mut metadata);
        assert_eq!(map.get("traceparent"), Some(TRACEPARENT));
        assert_eq!(map.get("baggage-bin"), None);
        assert_eq!(map.keys(), vec!["traceparent", "baggage-bin"]);
    }

    #[test]
    fn extract_tracing_interceptor_inserts_the_propagated_context() {
        opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

        let test_cases: Vec<(Option<&str>, fn(&SpanContext))> = vec![
            (Some(TRACEPARENT), |span_context| {
                assert!(span_context.is_remote());
                assert!(span_context.is_sampled());
                assert_eq!(
                    span_context.trace_id(),
                    TraceId::from_hex("0af7651916cd43dd8448eb211c80319c").unwrap()
                );
                assert_eq!(
                    span_context.span_id(),
                    SpanId::from_hex("b7ad6b7169203331").unwrap()
                );
            }),
            (None, |span_context| assert!(!span_context.is_valid())),
        ];

        for (traceparent, expect) in test_cases {
            let mut request = Request::new(());
            if let Some(traceparent) = traceparent {
                request
                    .metadata_mut()
                    .insert("traceparent", traceparent.parse().unwrap());
            }

            let request = ExtractTracingInterceptor.call(request).unwrap();
            let context = request
                .extensions()
                .get::<opentelemetry::Context>()
                .unwrap();
            expect(context.span().span_context());
        }
    }
}
