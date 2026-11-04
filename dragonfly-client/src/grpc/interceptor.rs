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

use dragonfly_client_config::dfdaemon::Jwt;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use tonic::{metadata, service::Interceptor, Request, Status};
use tracing::warn;
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

/// Token loaded from the dfdaemon JWT configuration.
#[derive(Clone, Default)]
enum TokenSource {
    /// Authentication is disabled for local development.
    #[default]
    Missing,

    /// A configured JWT to attach to outbound requests.
    Token(String),

    /// A configuration or token file error to report as an authentication error.
    Invalid(String),
}

/// Injects a JWT into outbound gRPC request metadata.
#[derive(Clone, Default)]
pub struct AuthInterceptor {
    token_source: TokenSource,
}

impl AuthInterceptor {
    /// Creates an authentication interceptor from the JWT configuration.
    pub fn new(config: &Jwt) -> Self {
        let token_source = match (&config.token, &config.token_path) {
            (Some(_), Some(_)) => {
                TokenSource::Invalid("JWT token and tokenPath are mutually exclusive".to_string())
            }
            (Some(token), None) => TokenSource::Token(token.trim().to_string()),
            (None, Some(path)) => match std::fs::read_to_string(path) {
                Ok(token) => TokenSource::Token(token.trim().to_string()),
                Err(err) => TokenSource::Invalid(format!(
                    "failed to read JWT token file {}: {}",
                    path.display(),
                    err
                )),
            },
            (None, None) => {
                warn!(
                    "JWT authentication token is not configured; sending gRPC requests without authorization metadata"
                );
                TokenSource::Missing
            }
        };

        Self { token_source }
    }

    /// Validates the token's structure and lifetime before sending it. Signature
    /// verification is deliberately left to the receiving server, which owns the
    /// verification secret.
    fn validate_token(token: &str) -> std::result::Result<(), Status> {
        let header = decode_header(token)
            .map_err(|err| Status::unauthenticated(format!("invalid JWT header: {err}")))?;
        if header.alg != Algorithm::HS256 {
            return Err(Status::unauthenticated(
                "invalid JWT algorithm: only HS256 is supported",
            ));
        }

        let mut validation = jwt_validation();
        // This is only an early client-side format and expiry check. The server
        // always performs full HS256 signature verification before accepting a call.
        validation.insecure_disable_signature_validation();
        decode::<serde_json::Value>(token, &DecodingKey::from_secret(&[]), &validation)
            .map(|_| ())
            .map_err(jwt_status)
    }
}

/// Implements the tonic Interceptor interface for outbound authentication.
impl Interceptor for AuthInterceptor {
    fn call(&mut self, mut request: Request<()>) -> std::result::Result<Request<()>, Status> {
        match &self.token_source {
            TokenSource::Missing => Ok(request),
            TokenSource::Invalid(message) => Err(Status::unauthenticated(message.clone())),
            TokenSource::Token(token) => {
                Self::validate_token(token)?;
                let value = metadata::MetadataValue::try_from(format!("Bearer {token}"))
                    .map_err(|_| Status::unauthenticated("JWT is invalid gRPC metadata"))?;
                request.metadata_mut().insert("authorization", value);
                Ok(request)
            }
        }
    }
}

/// Validates JWT authentication on inbound gRPC requests.
#[derive(Clone, Default)]
pub struct AuthValidator {
    decoding_key: Option<DecodingKey>,
    configuration_error: Option<String>,
}

impl AuthValidator {
    /// Creates a request validator from the JWT configuration.
    pub fn new(config: &Jwt) -> Self {
        match &config.secret {
            Some(secret) if secret.is_empty() => Self {
                decoding_key: None,
                configuration_error: Some("JWT HS256 secret must not be empty".to_string()),
            },
            Some(secret) => Self {
                decoding_key: Some(DecodingKey::from_secret(secret.as_bytes())),
                configuration_error: None,
            },
            None => {
                warn!(
                    "JWT server authentication is not configured; allowing unauthenticated gRPC requests"
                );
                Self::default()
            }
        }
    }
}

/// Implements the tonic Interceptor interface for inbound authentication.
impl Interceptor for AuthValidator {
    fn call(&mut self, request: Request<()>) -> std::result::Result<Request<()>, Status> {
        if let Some(message) = &self.configuration_error {
            return Err(Status::unauthenticated(message.clone()));
        }

        let Some(decoding_key) = &self.decoding_key else {
            return Ok(request);
        };

        let authorization = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("missing authorization metadata"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization metadata is not valid ASCII"))?;
        let token = authorization
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("authorization scheme must be Bearer"))?;

        decode::<serde_json::Value>(token, decoding_key, &jwt_validation())
            .map(|_| request)
            .map_err(jwt_status)
    }
}

/// Returns the common JWT validation policy.
fn jwt_validation() -> Validation {
    let mut validation = Validation::new(Algorithm::HS256);
    validation.leeway = 0;
    validation.validate_nbf = true;
    // No audience is configured, so do not reject otherwise valid tokens that
    // contain an audience claim. The HS256 signature and lifetime are enforced.
    validation.validate_aud = false;
    validation.set_required_spec_claims(&["exp"]);
    validation
}

/// Converts JWT validation failures into an identifiable gRPC authentication status.
fn jwt_status(err: jsonwebtoken::errors::Error) -> Status {
    match err.kind() {
        jsonwebtoken::errors::ErrorKind::ExpiredSignature => {
            Status::unauthenticated("JWT has expired")
        }
        _ => Status::unauthenticated(format!("invalid JWT: {err}")),
    }
}

/// Combines tracing and JWT authentication for outbound gRPC clients.
#[derive(Clone)]
pub struct ClientInterceptor {
    auth: AuthInterceptor,
    tracing: InjectTracingInterceptor,
}

impl ClientInterceptor {
    /// Creates a client interceptor from the JWT configuration.
    pub fn new(config: &Jwt) -> Self {
        Self {
            auth: AuthInterceptor::new(config),
            tracing: InjectTracingInterceptor,
        }
    }
}

impl Default for ClientInterceptor {
    fn default() -> Self {
        Self {
            auth: AuthInterceptor::default(),
            tracing: InjectTracingInterceptor,
        }
    }
}

/// Implements the tonic Interceptor interface for outbound clients.
impl Interceptor for ClientInterceptor {
    fn call(&mut self, request: Request<()>) -> std::result::Result<Request<()>, Status> {
        self.tracing.call(self.auth.call(request)?)
    }
}

/// Combines JWT authentication and tracing for inbound gRPC servers.
#[derive(Clone)]
pub struct ServerInterceptor {
    auth: AuthValidator,
    tracing: ExtractTracingInterceptor,
}

impl ServerInterceptor {
    /// Creates a server interceptor from the JWT configuration.
    pub fn new(config: &Jwt) -> Self {
        Self {
            auth: AuthValidator::new(config),
            tracing: ExtractTracingInterceptor,
        }
    }
}

/// Implements the tonic Interceptor interface for inbound servers.
impl Interceptor for ServerInterceptor {
    fn call(&mut self, request: Request<()>) -> std::result::Result<Request<()>, Status> {
        self.tracing.call(self.auth.call(request)?)
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::type_complexity)]

    use super::*;
    use jsonwebtoken::{encode, get_current_timestamp, EncodingKey, Header};
    use opentelemetry::propagation::{Extractor, Injector};
    use opentelemetry::trace::{SpanContext, SpanId, TraceContextExt, TraceId};
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    use serde::Serialize;
    use std::io::{self, Write};
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tempfile::NamedTempFile;
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::transport::{Channel, Server};
    use tonic::{Code, Response};
    use tonic_health::pb::health_check_response::ServingStatus;
    use tonic_health::pb::health_client::HealthClient;
    use tonic_health::pb::health_server::{Health, HealthServer};
    use tonic_health::pb::{HealthCheckRequest, HealthCheckResponse};

    const TRACEPARENT: &str = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    const TEST_SECRET: &str = "dfdaemon-grpc-test-secret";

    #[derive(Clone, Default)]
    struct RecordingHealth {
        request_count: Arc<AtomicUsize>,
        authorization: Arc<Mutex<Option<String>>>,
    }

    #[tonic::async_trait]
    impl Health for RecordingHealth {
        async fn check(
            &self,
            request: Request<HealthCheckRequest>,
        ) -> std::result::Result<Response<HealthCheckResponse>, Status> {
            self.request_count.fetch_add(1, Ordering::SeqCst);
            *self.authorization.lock().unwrap() = request
                .metadata()
                .get("authorization")
                .map(|value| value.to_str().unwrap().to_string());

            Ok(Response::new(HealthCheckResponse {
                status: ServingStatus::Serving as i32,
            }))
        }

        type WatchStream = ReceiverStream<std::result::Result<HealthCheckResponse, Status>>;

        async fn watch(
            &self,
            _request: Request<HealthCheckRequest>,
        ) -> std::result::Result<Response<Self::WatchStream>, Status> {
            Err(Status::unimplemented("watch is not used by these tests"))
        }
    }

    #[derive(Clone, Default)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    impl LogBuffer {
        fn contents(&self) -> String {
            String::from_utf8_lossy(&self.0.lock().unwrap()).into_owned()
        }
    }

    impl Write for LogBuffer {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[derive(Serialize)]
    struct TestClaims<'a> {
        sub: &'a str,
        exp: u64,
    }

    fn signed_token(exp: u64) -> String {
        encode(
            &Header::new(Algorithm::HS256),
            &TestClaims {
                sub: "dfdaemon-test",
                exp,
            },
            &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
        )
        .unwrap()
    }

    fn authenticated_config(token: String) -> Jwt {
        Jwt {
            token: Some(token),
            secret: Some(TEST_SECRET.to_string()),
            ..Default::default()
        }
    }

    async fn spawn_auth_server(config: &Jwt) -> (SocketAddr, RecordingHealth) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let health = RecordingHealth::default();
        let recorded_health = health.clone();
        let service = HealthServer::with_interceptor(health, ServerInterceptor::new(config));

        tokio::spawn(async move {
            Server::builder()
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });

        (addr, recorded_health)
    }

    async fn check_with_interceptor(
        addr: SocketAddr,
        interceptor: ClientInterceptor,
    ) -> std::result::Result<Response<HealthCheckResponse>, Status> {
        let channel = Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        let mut client = HealthClient::with_interceptor(channel, interceptor);
        client
            .check(Request::new(HealthCheckRequest {
                service: String::new(),
            }))
            .await
    }

    async fn check_without_interceptor(
        addr: SocketAddr,
        token: Option<String>,
    ) -> std::result::Result<Response<HealthCheckResponse>, Status> {
        let mut client = HealthClient::connect(format!("http://{addr}"))
            .await
            .unwrap();
        let mut request = Request::new(HealthCheckRequest {
            service: String::new(),
        });
        if let Some(token) = token {
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
        }

        client.check(request).await
    }

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

    #[tokio::test]
    async fn grpc_auth_injects_valid_bearer_token() {
        let token = signed_token(get_current_timestamp() + 3600);
        let config = authenticated_config(token.clone());
        let (addr, health) = spawn_auth_server(&config).await;

        let response = check_with_interceptor(addr, ClientInterceptor::new(&config))
            .await
            .unwrap();

        assert_eq!(response.into_inner().status, ServingStatus::Serving as i32);
        assert_eq!(health.request_count.load(Ordering::SeqCst), 1);
        assert_eq!(
            *health.authorization.lock().unwrap(),
            Some(format!("Bearer {token}"))
        );
    }

    #[tokio::test]
    async fn grpc_auth_accepts_an_audience_claim_without_an_audience_policy() {
        let token = encode(
            &Header::new(Algorithm::HS256),
            &serde_json::json!({
                "sub": "dfdaemon-test",
                "exp": get_current_timestamp() + 3600,
                "aud": "dragonfly",
            }),
            &EncodingKey::from_secret(TEST_SECRET.as_bytes()),
        )
        .unwrap();
        let config = authenticated_config(token);
        let (addr, health) = spawn_auth_server(&config).await;

        check_with_interceptor(addr, ClientInterceptor::new(&config))
            .await
            .unwrap();

        assert_eq!(health.request_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn grpc_auth_reads_token_from_file() {
        let token = signed_token(get_current_timestamp() + 3600);
        let token_file = NamedTempFile::new().unwrap();
        std::fs::write(token_file.path(), format!("\n{token}\n")).unwrap();
        let config = Jwt {
            token_path: Some(token_file.path().to_path_buf()),
            secret: Some(TEST_SECRET.to_string()),
            ..Default::default()
        };
        let (addr, health) = spawn_auth_server(&config).await;

        check_with_interceptor(addr, ClientInterceptor::new(&config))
            .await
            .unwrap();

        assert_eq!(
            *health.authorization.lock().unwrap(),
            Some(format!("Bearer {token}"))
        );
    }

    #[tokio::test]
    async fn grpc_auth_without_token_preserves_development_mode_and_warns() {
        let (addr, health) = spawn_auth_server(&Jwt::default()).await;
        let logs = LogBuffer::default();
        let writer = logs.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .without_time()
            .with_target(false)
            .with_writer(move || writer.clone())
            .finish();
        let interceptor = tracing::subscriber::with_default(subscriber, || {
            ClientInterceptor::new(&Jwt::default())
        });

        check_with_interceptor(addr, interceptor).await.unwrap();

        assert_eq!(*health.authorization.lock().unwrap(), None);
        assert!(logs.contents().contains("WARN"));
        assert!(logs
            .contents()
            .contains("JWT authentication token is not configured"));
    }

    #[tokio::test]
    async fn grpc_auth_server_rejects_missing_token() {
        let config = authenticated_config(signed_token(get_current_timestamp() + 3600));
        let (addr, health) = spawn_auth_server(&config).await;

        let status = check_without_interceptor(addr, None).await.unwrap_err();

        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(health.request_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn grpc_auth_client_rejects_expired_token() {
        let config = authenticated_config(signed_token(get_current_timestamp() - 3600));
        let (addr, health) = spawn_auth_server(&config).await;

        let status = check_with_interceptor(addr, ClientInterceptor::new(&config))
            .await
            .unwrap_err();

        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(health.request_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn grpc_auth_server_rejects_expired_token() {
        let token = signed_token(get_current_timestamp() - 3600);
        let config = authenticated_config(token.clone());
        let (addr, health) = spawn_auth_server(&config).await;

        let status = check_without_interceptor(addr, Some(token))
            .await
            .unwrap_err();

        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(health.request_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn grpc_auth_rejects_tampered_signature() {
        let mut token = signed_token(get_current_timestamp() + 3600);
        let last = token.pop().unwrap();
        token.push(if last == 'a' { 'b' } else { 'a' });
        let config = authenticated_config(token);
        let (addr, health) = spawn_auth_server(&config).await;

        let status = check_with_interceptor(addr, ClientInterceptor::new(&config))
            .await
            .unwrap_err();

        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(health.request_count.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn grpc_auth_rejects_malformed_token() {
        let config = authenticated_config("not-a-jwt".to_string());
        let (addr, health) = spawn_auth_server(&config).await;

        let status = check_with_interceptor(addr, ClientInterceptor::new(&config))
            .await
            .unwrap_err();

        assert_eq!(status.code(), Code::Unauthenticated);
        assert_eq!(health.request_count.load(Ordering::SeqCst), 0);
    }
}
