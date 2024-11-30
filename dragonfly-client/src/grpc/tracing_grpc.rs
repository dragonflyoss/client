use {tonic::{metadata, service::Interceptor, Request, Status}, tracing_opentelemetry::OpenTelemetrySpanExt};

/// MetadataMap is a tracing meda data map container.
struct MetadataMap<'a>(&'a mut metadata::MetadataMap);

/// MetadataMap implements the otel tracing Injector.
impl<'a> opentelemetry::propagation::Injector for MetadataMap<'a> {
    /// set a key-value pair to the injector.
    fn set(&mut self, key: &str, value: String) {
        if let Ok(key) = metadata::MetadataKey::from_bytes(key.as_bytes()) {
            if let Ok(val) = metadata::MetadataValue::try_from(&value) {
                self.0.insert(key, val);
            }
        }
    }
}

/// TracingInterceptor is a auto-inject tracing gRPC interceptor.
#[derive(Clone)]
pub struct TracingInterceptor;

/// TracingInterceptor implements the tonic Interceptor interface.
impl Interceptor for TracingInterceptor {
    /// call and inject tracing context into lgobal propagator.
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        let context = tracing::Span::current().context();
        opentelemetry::global::get_text_map_propagator(|prop| 
            prop.inject_context(&context, &mut MetadataMap(request.metadata_mut())));
    
        Ok(request)
    }
}