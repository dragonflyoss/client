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

/// MetadataMap implements the otel tracing Extractor.
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

/// MetadataMap implements the otel tracing Injector.
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

/// InjectTracingInterceptor implements the tonic Interceptor interface.
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

/// ExtractTracingInterceptor implements the tonic Interceptor interface.
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
