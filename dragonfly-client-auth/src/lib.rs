/*
 *     Copyright 2026 The Dragonfly Authors
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

use base64::{engine::general_purpose::STANDARD, Engine as _};
use jsonwebtoken::{
    decode, decode_header, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation,
};
use prometheus::{IntCounterVec, Opts};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, LazyLock, Mutex, OnceLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use thiserror::Error;
use tonic::metadata::{Ascii, MetadataMap, MetadataValue};
use tonic::service::Interceptor;
use tonic::{Request, Status};
use validator::{Validate, ValidationError, ValidationErrors};

/// JWT type used for Dragonfly inter-component gRPC authentication.
pub const TOKEN_TYPE: &str = "dragonfly-grpc+jwt";

/// Audience expected by Manager gRPC servers.
pub const AUDIENCE_MANAGER: &str = "urn:dragonfly:grpc:manager";

/// Audience expected by Scheduler gRPC servers.
pub const AUDIENCE_SCHEDULER: &str = "urn:dragonfly:grpc:scheduler";

/// Audience expected by dfdaemon gRPC servers.
pub const AUDIENCE_DFDAEMON: &str = "urn:dragonfly:grpc:dfdaemon";

const DEFAULT_ISSUER: &str = "dragonfly-internal";
const AUTHORIZATION_METADATA_KEY: &str = "authorization";
const BEARER_SCHEME: &str = "Bearer";
const MAX_CREDENTIAL_LENGTH: usize = 4 * 1024;
const MINIMUM_KEY_SIZE: usize = 32;
const AUTHENTICATION_ERROR_MESSAGE: &str = "invalid authentication credentials";

const REASON_NONE: &str = "none";
const REASON_MISSING: &str = "missing";
const REASON_MALFORMED: &str = "malformed";
const REASON_UNSUPPORTED_ALG: &str = "unsupported_alg";
const REASON_INVALID_TYPE: &str = "invalid_type";
const REASON_UNKNOWN_KEY_ID: &str = "unknown_kid";
const REASON_INVALID_SIGNATURE: &str = "invalid_signature";
const REASON_INVALID_ISSUER: &str = "invalid_issuer";
const REASON_INVALID_AUDIENCE: &str = "invalid_audience";
const REASON_EXPIRED: &str = "expired";
const REASON_INVALID_ISSUED_AT: &str = "invalid_iat";
const REASON_TTL_EXCEEDED: &str = "ttl_exceeded";

/// Server authentication metrics shared with dragonfly-client-metric.
pub static GRPC_AUTH_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "requests_total",
            "Total number of inter-component gRPC authentication attempts.",
        )
        .namespace("dragonfly")
        .subsystem("grpc_auth"),
        &["audience", "mode", "result", "reason"],
    )
    .expect("metric can be created")
});

/// Client token generation and cache metrics shared with dragonfly-client-metric.
pub static GRPC_AUTH_CLIENT_TOKEN_EVENTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new(
            "client_token_events_total",
            "Total number of inter-component gRPC client JWT cache and generation events.",
        )
        .namespace("dragonfly")
        .subsystem("grpc_auth"),
        &["audience", "event"],
    )
    .expect("metric can be created")
});

fn default_token_ttl() -> Duration {
    Duration::from_secs(10 * 60)
}

fn default_max_token_ttl() -> Duration {
    Duration::from_secs(15 * 60)
}

fn default_clock_skew() -> Duration {
    Duration::from_secs(30)
}

fn default_refresh_before() -> Duration {
    Duration::from_secs(60)
}

fn default_issuer() -> String {
    DEFAULT_ISSUER.to_string()
}

/// Inter-component gRPC authentication mode.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Do not send or verify JWTs.
    #[default]
    Disabled,

    /// Send JWTs and allow missing credentials during a rolling upgrade.
    Permissive,

    /// Require a valid JWT on every protected RPC.
    Required,
}

impl Mode {
    /// Returns the configuration representation of the mode.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Disabled => "disabled",
            Self::Permissive => "permissive",
            Self::Required => "required",
        }
    }
}

/// A shared HMAC key file.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct KeyConfig {
    /// Key identifier serialized in the JWT `kid` header.
    pub id: String,

    /// File containing an RFC 4648 standard Base64-encoded key.
    pub secret_file: PathBuf,
}

/// JWT signing and validation configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct JwtConfig {
    /// Exact issuer accepted by the verifier.
    #[serde(default = "default_issuer")]
    pub issuer: String,

    /// Lifetime of generated tokens.
    #[serde(default = "default_token_ttl", with = "humantime_serde")]
    pub token_ttl: Duration,

    /// Maximum lifetime accepted by the verifier.
    #[serde(default = "default_max_token_ttl", with = "humantime_serde")]
    pub max_token_ttl: Duration,

    /// Clock skew accepted by the verifier.
    #[serde(default = "default_clock_skew", with = "humantime_serde")]
    pub clock_skew: Duration,

    /// Duration before expiration at which a cached token is refreshed.
    #[serde(default = "default_refresh_before", with = "humantime_serde")]
    pub refresh_before: Duration,

    /// Key used to sign new tokens.
    #[serde(rename = "activeKeyID")]
    pub active_key_id: String,

    /// Keyring trusted by the verifier.
    pub keys: Vec<KeyConfig>,
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            issuer: default_issuer(),
            token_ttl: default_token_ttl(),
            max_token_ttl: default_max_token_ttl(),
            clock_skew: default_clock_skew(),
            refresh_before: default_refresh_before(),
            active_key_id: String::new(),
            keys: Vec::new(),
        }
    }
}

#[derive(Clone)]
struct CachedToken {
    value: String,
    expires_at: u64,
}

#[derive(Default)]
struct Runtime {
    keys: OnceLock<Arc<HashMap<String, Vec<u8>>>>,
    tokens: Mutex<HashMap<String, CachedToken>>,
}

/// Inter-component gRPC authentication configuration and shared runtime cache.
#[derive(Clone, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct GrpcAuth {
    /// Client and server authentication mode.
    pub mode: Mode,

    /// Refuse to attach bearer credentials to plaintext transports.
    pub require_transport_security: bool,

    /// JWT configuration.
    pub jwt: JwtConfig,

    #[serde(skip)]
    runtime: Arc<Runtime>,
}

impl Default for GrpcAuth {
    fn default() -> Self {
        Self {
            mode: Mode::Disabled,
            require_transport_security: false,
            jwt: JwtConfig::default(),
            runtime: Arc::new(Runtime::default()),
        }
    }
}

impl fmt::Debug for GrpcAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GrpcAuth")
            .field("mode", &self.mode)
            .field(
                "require_transport_security",
                &self.require_transport_security,
            )
            .field("jwt", &self.jwt)
            .finish_non_exhaustive()
    }
}

impl Validate for GrpcAuth {
    fn validate(&self) -> Result<(), ValidationErrors> {
        self.ensure_keyring().map(|_| ()).map_err(|error| {
            let mut validation_error = ValidationError::new("grpc_auth");
            validation_error.message = Some(Cow::Owned(error.to_string()));
            let mut validation_errors = ValidationErrors::new();
            validation_errors.add("grpcAuth", validation_error);
            validation_errors
        })
    }
}

/// Authentication decision for an incoming RPC.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthenticationOutcome {
    /// Authentication is disabled.
    Disabled,

    /// The request carried a valid JWT.
    Authenticated,

    /// Missing credentials were allowed in permissive mode.
    PermissiveMissing,
}

/// A bounded authentication failure.
#[derive(Debug, Error)]
#[error("{detail}")]
pub struct AuthError {
    reason: &'static str,
    detail: String,
}

impl AuthError {
    fn new(reason: &'static str, detail: impl Into<String>) -> Self {
        Self {
            reason,
            detail: detail.into(),
        }
    }

    /// Returns the bounded metric reason.
    pub fn reason(&self) -> &'static str {
        self.reason
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Claims {
    iss: String,
    aud: String,
    iat: i64,
    exp: i64,
}

impl GrpcAuth {
    /// Returns whether authentication is enabled.
    pub fn enabled(&self) -> bool {
        self.mode != Mode::Disabled
    }

    /// Validates that credentials may be sent on the selected transport.
    pub fn ensure_transport_security(&self, secure: bool) -> Result<(), AuthError> {
        if self.enabled() && self.require_transport_security && !secure {
            return Err(AuthError::new(
                REASON_MALFORMED,
                "gRPC authentication requires transport security",
            ));
        }

        Ok(())
    }

    /// Generates or reuses a JWT for the target audience.
    pub fn token(&self, audience: &str) -> Result<Option<String>, AuthError> {
        self.token_at(audience, unix_time()?)
    }

    fn token_at(&self, audience: &str, now: u64) -> Result<Option<String>, AuthError> {
        if !self.enabled() {
            return Ok(None);
        }

        validate_audience(audience)?;
        let keys = self.ensure_keyring()?;
        let cache_key = format!("{}\0{}", audience, self.jwt.active_key_id);
        let mut tokens =
            self.runtime.tokens.lock().map_err(|_| {
                AuthError::new(REASON_MALFORMED, "JWT token cache lock is poisoned")
            })?;

        if let Some(cached) = tokens.get(&cache_key) {
            if cached.expires_at.saturating_sub(now) > self.jwt.refresh_before.as_secs() {
                GRPC_AUTH_CLIENT_TOKEN_EVENTS
                    .with_label_values(&[audience, "cache_hit"])
                    .inc();
                return Ok(Some(cached.value.clone()));
            }
        }

        GRPC_AUTH_CLIENT_TOKEN_EVENTS
            .with_label_values(&[audience, "cache_miss"])
            .inc();
        let key = keys.get(&self.jwt.active_key_id).ok_or_else(|| {
            AuthError::new(REASON_UNKNOWN_KEY_ID, "active JWT key is unavailable")
        })?;
        let expires_at = now
            .checked_add(self.jwt.token_ttl.as_secs())
            .ok_or_else(|| AuthError::new(REASON_TTL_EXCEEDED, "JWT expiration time overflows"))?;
        let issued_at = i64::try_from(now).map_err(|_| {
            AuthError::new(REASON_INVALID_ISSUED_AT, "JWT issued-at time overflows")
        })?;
        let expiration = i64::try_from(expires_at)
            .map_err(|_| AuthError::new(REASON_TTL_EXCEEDED, "JWT expiration time overflows"))?;
        let claims = Claims {
            iss: self.jwt.issuer.clone(),
            aud: audience.to_string(),
            iat: issued_at,
            exp: expiration,
        };
        let mut header = Header::new(Algorithm::HS256);
        header.typ = Some(TOKEN_TYPE.to_string());
        header.kid = Some(self.jwt.active_key_id.clone());
        let token = encode(&header, &claims, &EncodingKey::from_secret(key)).map_err(|error| {
            AuthError::new(REASON_MALFORMED, format!("failed to sign JWT: {error}"))
        })?;

        tokens.insert(
            cache_key,
            CachedToken {
                value: token.clone(),
                expires_at,
            },
        );
        GRPC_AUTH_CLIENT_TOKEN_EVENTS
            .with_label_values(&[audience, "generated"])
            .inc();
        Ok(Some(token))
    }

    /// Authenticates incoming gRPC metadata for the target audience.
    pub fn authenticate(
        &self,
        metadata: &MetadataMap,
        audience: &str,
    ) -> Result<AuthenticationOutcome, AuthError> {
        self.authenticate_at(metadata, audience, unix_time()?)
    }

    fn authenticate_at(
        &self,
        metadata: &MetadataMap,
        audience: &str,
        now: u64,
    ) -> Result<AuthenticationOutcome, AuthError> {
        if !self.enabled() {
            return Ok(AuthenticationOutcome::Disabled);
        }

        validate_audience(audience)?;
        let values: Vec<_> = metadata
            .get_all(AUTHORIZATION_METADATA_KEY)
            .iter()
            .collect();
        if values.is_empty() {
            if self.mode == Mode::Permissive {
                return Ok(AuthenticationOutcome::PermissiveMissing);
            }

            return Err(AuthError::new(
                REASON_MISSING,
                "authorization metadata is missing",
            ));
        }

        if values.len() != 1 {
            return Err(AuthError::new(
                REASON_MALFORMED,
                "multiple authorization metadata values",
            ));
        }

        let credential = values[0]
            .to_str()
            .map_err(|_| AuthError::new(REASON_MALFORMED, "authorization metadata is not ASCII"))?;
        if credential.len() > MAX_CREDENTIAL_LENGTH {
            return Err(AuthError::new(
                REASON_MALFORMED,
                "authorization metadata is too large",
            ));
        }

        let parts: Vec<_> = credential.split_whitespace().collect();
        if parts.len() != 2 || !parts[0].eq_ignore_ascii_case(BEARER_SCHEME) || parts[1].is_empty()
        {
            return Err(AuthError::new(
                REASON_MALFORMED,
                "malformed bearer credential",
            ));
        }

        self.verify_at(parts[1], audience, now)?;
        Ok(AuthenticationOutcome::Authenticated)
    }

    fn verify_at(&self, raw_token: &str, audience: &str, now: u64) -> Result<(), AuthError> {
        let header = decode_header(raw_token).map_err(|error| {
            AuthError::new(
                REASON_MALFORMED,
                format!("failed to parse JWT header: {error}"),
            )
        })?;
        if header.alg != Algorithm::HS256 {
            return Err(AuthError::new(
                REASON_UNSUPPORTED_ALG,
                "JWT algorithm is not HS256",
            ));
        }

        if header.typ.as_deref() != Some(TOKEN_TYPE) {
            return Err(AuthError::new(REASON_INVALID_TYPE, "JWT type is invalid"));
        }

        let key_id = header
            .kid
            .as_deref()
            .filter(|key_id| !key_id.is_empty())
            .ok_or_else(|| AuthError::new(REASON_UNKNOWN_KEY_ID, "JWT key id is missing"))?;
        let keys = self.ensure_keyring()?;
        let key = keys
            .get(key_id)
            .ok_or_else(|| AuthError::new(REASON_UNKNOWN_KEY_ID, "JWT key id is not trusted"))?;

        let mut validation = Validation::new(Algorithm::HS256);
        validation.validate_exp = false;
        validation.validate_nbf = false;
        validation.validate_aud = false;
        validation.required_spec_claims.clear();
        let token = decode::<Claims>(raw_token, &DecodingKey::from_secret(key), &validation)
            .map_err(|_| AuthError::new(REASON_INVALID_SIGNATURE, "JWT signature is invalid"))?;
        let claims = token.claims;

        if claims.iss != self.jwt.issuer {
            return Err(AuthError::new(
                REASON_INVALID_ISSUER,
                "JWT issuer is invalid",
            ));
        }

        if claims.aud != audience {
            return Err(AuthError::new(
                REASON_INVALID_AUDIENCE,
                "JWT audience is invalid",
            ));
        }

        if claims.iat <= 0 {
            return Err(AuthError::new(
                REASON_INVALID_ISSUED_AT,
                "JWT issued-at time is missing",
            ));
        }

        if claims.exp <= 0 {
            return Err(AuthError::new(
                REASON_EXPIRED,
                "JWT expiration time is missing",
            ));
        }

        let now = i64::try_from(now)
            .map_err(|_| AuthError::new(REASON_INVALID_ISSUED_AT, "server time overflows"))?;
        let skew = i64::try_from(self.jwt.clock_skew.as_secs())
            .map_err(|_| AuthError::new(REASON_TTL_EXCEEDED, "JWT clock skew overflows"))?;
        if claims.iat > now.saturating_add(skew) {
            return Err(AuthError::new(
                REASON_INVALID_ISSUED_AT,
                "JWT issued-at time is in the future",
            ));
        }

        if claims.exp <= now.saturating_sub(skew) {
            return Err(AuthError::new(REASON_EXPIRED, "JWT is expired"));
        }

        if claims.exp <= claims.iat {
            return Err(AuthError::new(
                REASON_TTL_EXCEEDED,
                "JWT lifetime is not positive",
            ));
        }

        let max_token_ttl = i64::try_from(self.jwt.max_token_ttl.as_secs())
            .map_err(|_| AuthError::new(REASON_TTL_EXCEEDED, "JWT maximum lifetime overflows"))?;
        if claims.exp - claims.iat > max_token_ttl {
            return Err(AuthError::new(
                REASON_TTL_EXCEEDED,
                "JWT lifetime exceeds the configured maximum",
            ));
        }

        Ok(())
    }

    fn ensure_keyring(&self) -> Result<Arc<HashMap<String, Vec<u8>>>, AuthError> {
        if let Some(keys) = self.runtime.keys.get() {
            return Ok(keys.clone());
        }

        let keys = Arc::new(self.load_keyring()?);
        let _ = self.runtime.keys.set(keys.clone());
        Ok(self.runtime.keys.get().cloned().unwrap_or(keys))
    }

    fn load_keyring(&self) -> Result<HashMap<String, Vec<u8>>, AuthError> {
        if !self.enabled() {
            return Ok(HashMap::new());
        }

        if self.jwt.issuer.is_empty() {
            return Err(AuthError::new(REASON_MALFORMED, "JWT issuer is required"));
        }

        validate_duration(self.jwt.token_ttl, "tokenTTL", true)?;
        validate_duration(self.jwt.max_token_ttl, "maxTokenTTL", true)?;
        validate_duration(self.jwt.clock_skew, "clockSkew", false)?;
        validate_duration(self.jwt.refresh_before, "refreshBefore", true)?;
        if self.jwt.token_ttl > self.jwt.max_token_ttl {
            return Err(AuthError::new(
                REASON_MALFORMED,
                "JWT tokenTTL must not exceed maxTokenTTL",
            ));
        }

        if self.jwt.refresh_before >= self.jwt.token_ttl {
            return Err(AuthError::new(
                REASON_MALFORMED,
                "JWT refreshBefore must be less than tokenTTL",
            ));
        }

        if self.jwt.active_key_id.is_empty() {
            return Err(AuthError::new(
                REASON_UNKNOWN_KEY_ID,
                "JWT activeKeyID is required",
            ));
        }

        if self.jwt.keys.is_empty() {
            return Err(AuthError::new(
                REASON_UNKNOWN_KEY_ID,
                "JWT keyring must not be empty",
            ));
        }

        let mut keys = HashMap::with_capacity(self.jwt.keys.len());
        for key_config in &self.jwt.keys {
            if key_config.id.is_empty() {
                return Err(AuthError::new(
                    REASON_UNKNOWN_KEY_ID,
                    "JWT key id is required",
                ));
            }

            if keys.contains_key(&key_config.id) {
                return Err(AuthError::new(
                    REASON_UNKNOWN_KEY_ID,
                    format!("JWT key id {:?} is duplicated", key_config.id),
                ));
            }

            let metadata = fs::metadata(&key_config.secret_file).map_err(|error| {
                AuthError::new(
                    REASON_UNKNOWN_KEY_ID,
                    format!("failed to stat JWT secret file: {error}"),
                )
            })?;
            if !metadata.is_file() {
                return Err(AuthError::new(
                    REASON_UNKNOWN_KEY_ID,
                    "JWT secret file is not a regular file",
                ));
            }

            let encoded = fs::read_to_string(&key_config.secret_file).map_err(|error| {
                AuthError::new(
                    REASON_UNKNOWN_KEY_ID,
                    format!("failed to read JWT secret file: {error}"),
                )
            })?;
            let secret = STANDARD.decode(encoded.trim()).map_err(|_| {
                AuthError::new(
                    REASON_UNKNOWN_KEY_ID,
                    "JWT secret file is not valid standard Base64",
                )
            })?;
            if secret.len() < MINIMUM_KEY_SIZE {
                return Err(AuthError::new(
                    REASON_UNKNOWN_KEY_ID,
                    format!("decoded JWT secret must contain at least {MINIMUM_KEY_SIZE} bytes"),
                ));
            }

            keys.insert(key_config.id.clone(), secret);
        }

        if !keys.contains_key(&self.jwt.active_key_id) {
            return Err(AuthError::new(
                REASON_UNKNOWN_KEY_ID,
                "JWT active key is not trusted",
            ));
        }

        Ok(keys)
    }

    fn record_server_result(&self, audience: &str, result: &str, reason: &str) {
        GRPC_AUTH_REQUESTS
            .with_label_values(&[audience, self.mode.as_str(), result, reason])
            .inc();
    }
}

/// Tonic client interceptor that attaches a JWT to an RPC.
#[derive(Clone)]
pub struct ClientInterceptor {
    auth: GrpcAuth,
    audience: &'static str,
}

impl ClientInterceptor {
    /// Creates a client interceptor for a fixed audience and transport.
    pub fn new(auth: GrpcAuth, audience: &'static str, secure: bool) -> Result<Self, AuthError> {
        validate_audience(audience)?;
        auth.ensure_transport_security(secure)?;
        auth.ensure_keyring()?;
        Ok(Self { auth, audience })
    }
}

impl Interceptor for ClientInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        match self.auth.token(self.audience) {
            Ok(Some(token)) => {
                let value: MetadataValue<Ascii> = format!("{BEARER_SCHEME} {token}")
                    .parse()
                    .map_err(|_| Status::unauthenticated(AUTHENTICATION_ERROR_MESSAGE))?;
                request
                    .metadata_mut()
                    .insert(AUTHORIZATION_METADATA_KEY, value);
            }
            Ok(None) => {}
            Err(_) => {
                GRPC_AUTH_CLIENT_TOKEN_EVENTS
                    .with_label_values(&[self.audience, "generation_failed"])
                    .inc();
                return Err(Status::unauthenticated(AUTHENTICATION_ERROR_MESSAGE));
            }
        }

        Ok(request)
    }
}

/// Tonic server interceptor that validates a JWT on an RPC.
#[derive(Clone)]
pub struct ServerInterceptor {
    auth: GrpcAuth,
    audience: &'static str,
}

impl ServerInterceptor {
    /// Creates a server interceptor for a fixed audience.
    pub fn new(auth: GrpcAuth, audience: &'static str) -> Self {
        Self { auth, audience }
    }
}

impl Interceptor for ServerInterceptor {
    fn call(&mut self, request: Request<()>) -> Result<Request<()>, Status> {
        match self.auth.authenticate(request.metadata(), self.audience) {
            Ok(AuthenticationOutcome::Disabled) => Ok(request),
            Ok(AuthenticationOutcome::Authenticated) => {
                self.auth
                    .record_server_result(self.audience, "success", REASON_NONE);
                Ok(request)
            }
            Ok(AuthenticationOutcome::PermissiveMissing) => {
                self.auth
                    .record_server_result(self.audience, "allowed", REASON_MISSING);
                Ok(request)
            }
            Err(error) => {
                self.auth
                    .record_server_result(self.audience, "failure", error.reason());
                Err(Status::unauthenticated(AUTHENTICATION_ERROR_MESSAGE))
            }
        }
    }
}

fn validate_duration(duration: Duration, name: &str, positive: bool) -> Result<(), AuthError> {
    if duration.subsec_nanos() != 0 {
        return Err(AuthError::new(
            REASON_MALFORMED,
            format!("JWT {name} must use whole seconds"),
        ));
    }

    if positive && duration < Duration::from_secs(1) {
        return Err(AuthError::new(
            REASON_MALFORMED,
            format!("JWT {name} must be at least one second"),
        ));
    }

    if duration.as_secs() > i64::MAX as u64 {
        return Err(AuthError::new(
            REASON_MALFORMED,
            format!("JWT {name} is too large"),
        ));
    }

    Ok(())
}

fn validate_audience(audience: &str) -> Result<(), AuthError> {
    if matches!(
        audience,
        AUDIENCE_MANAGER | AUDIENCE_SCHEDULER | AUDIENCE_DFDAEMON
    ) {
        return Ok(());
    }

    Err(AuthError::new(
        REASON_INVALID_AUDIENCE,
        "unsupported JWT audience",
    ))
}

fn unix_time() -> Result<u64, AuthError> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .map_err(|_| AuthError::new(REASON_INVALID_ISSUED_AT, "system time precedes Unix epoch"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::ops::{Deref, DerefMut};
    use tempfile::NamedTempFile;

    const NOW: u64 = 1_786_435_200;

    #[derive(Deserialize)]
    struct InteroperabilityFixture {
        #[serde(rename = "secretBase64")]
        secret_base64: String,
        #[serde(rename = "keyID")]
        key_id: String,
        issuer: String,
        audience: String,
        #[serde(rename = "issuedAt")]
        issued_at: u64,
        #[serde(rename = "expiresAt")]
        expires_at: u64,
        #[serde(rename = "goToken")]
        go_token: String,
        #[serde(rename = "rustToken")]
        rust_token: String,
    }

    struct TestAuth {
        auth: GrpcAuth,
        _secret_file: NamedTempFile,
    }

    impl Deref for TestAuth {
        type Target = GrpcAuth;

        fn deref(&self) -> &Self::Target {
            &self.auth
        }
    }

    impl DerefMut for TestAuth {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.auth
        }
    }

    fn enabled_auth(mode: Mode) -> TestAuth {
        let mut secret_file = NamedTempFile::new().unwrap();
        writeln!(
            secret_file,
            "{}",
            STANDARD.encode(b"0123456789abcdef0123456789abcdef")
        )
        .unwrap();
        TestAuth {
            auth: GrpcAuth {
                mode,
                jwt: JwtConfig {
                    active_key_id: "test-key".to_string(),
                    keys: vec![KeyConfig {
                        id: "test-key".to_string(),
                        secret_file: secret_file.path().to_path_buf(),
                    }],
                    ..Default::default()
                },
                ..Default::default()
            },
            _secret_file: secret_file,
        }
    }

    fn interoperability_fixture() -> InteroperabilityFixture {
        serde_json::from_str(include_str!("../testdata/interop.json")).unwrap()
    }

    #[test]
    fn defaults_are_disabled() {
        let auth = GrpcAuth::default();
        assert_eq!(auth.mode, Mode::Disabled);
        assert_eq!(auth.jwt.issuer, DEFAULT_ISSUER);
        assert_eq!(auth.jwt.token_ttl, default_token_ttl());
        assert!(auth.validate().is_ok());
    }

    #[test]
    fn deserializes_shared_component_configuration() {
        let mut secret_file = NamedTempFile::new().unwrap();
        writeln!(
            secret_file,
            "{}",
            STANDARD.encode(b"0123456789abcdef0123456789abcdef")
        )
        .unwrap();
        let yaml = format!(
            r#"
mode: required
requireTransportSecurity: true
jwt:
  issuer: dragonfly-internal
  activeKeyID: test-key
  tokenTTL: 10m
  maxTokenTTL: 15m
  clockSkew: 30s
  refreshBefore: 1m
  keys:
    - id: test-key
      secretFile: {}
"#,
            secret_file.path().display()
        );

        let auth: GrpcAuth = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(auth.mode, Mode::Required);
        assert!(auth.require_transport_security);
        assert_eq!(auth.jwt.active_key_id, "test-key");
        assert!(auth.validate().is_ok());
    }

    #[test]
    fn validates_go_interoperability_fixture() {
        let fixture = interoperability_fixture();
        let auth = enabled_auth(Mode::Required);
        assert_eq!(auth.jwt.active_key_id, fixture.key_id);
        assert_eq!(auth.jwt.issuer, fixture.issuer);
        assert_eq!(fixture.audience, AUDIENCE_SCHEDULER);
        assert_eq!(
            fixture.secret_base64,
            STANDARD.encode(b"0123456789abcdef0123456789abcdef")
        );
        assert_eq!(
            fixture.expires_at - fixture.issued_at,
            auth.jwt.token_ttl.as_secs()
        );
        assert!(auth
            .verify_at(&fixture.go_token, AUDIENCE_SCHEDULER, fixture.issued_at)
            .is_ok());
        assert!(auth
            .verify_at(&fixture.rust_token, AUDIENCE_SCHEDULER, fixture.issued_at)
            .is_ok());
        assert_eq!(
            auth.token_at(AUDIENCE_SCHEDULER, fixture.issued_at)
                .unwrap()
                .unwrap(),
            fixture.rust_token
        );
    }

    #[test]
    fn generates_and_caches_token() {
        let auth = enabled_auth(Mode::Required);
        let first = auth.token_at(AUDIENCE_MANAGER, NOW).unwrap().unwrap();
        let second = auth.token_at(AUDIENCE_MANAGER, NOW).unwrap().unwrap();
        assert_eq!(first, second);
        assert!(auth.verify_at(&first, AUDIENCE_MANAGER, NOW).is_ok());
    }

    #[test]
    fn overlapping_keys_support_rotation() {
        let mut auth = enabled_auth(Mode::Required);
        let mut old_secret_file = NamedTempFile::new().unwrap();
        let old_secret = b"old-key-material-old-key-material-";
        writeln!(old_secret_file, "{}", STANDARD.encode(old_secret)).unwrap();
        auth.jwt.keys.push(KeyConfig {
            id: "old-key".to_string(),
            secret_file: old_secret_file.path().to_path_buf(),
        });

        let claims = Claims {
            iss: DEFAULT_ISSUER.to_string(),
            aud: AUDIENCE_SCHEDULER.to_string(),
            iat: NOW as i64,
            exp: (NOW + default_token_ttl().as_secs()) as i64,
        };
        let mut old_header = Header::new(Algorithm::HS256);
        old_header.typ = Some(TOKEN_TYPE.to_string());
        old_header.kid = Some("old-key".to_string());
        let old_token =
            encode(&old_header, &claims, &EncodingKey::from_secret(old_secret)).unwrap();
        assert!(auth.verify_at(&old_token, AUDIENCE_SCHEDULER, NOW).is_ok());

        let new_token = auth.token_at(AUDIENCE_SCHEDULER, NOW).unwrap().unwrap();
        assert_eq!(
            decode_header(&new_token).unwrap().kid.as_deref(),
            Some("test-key")
        );
    }

    #[test]
    fn caches_token_across_concurrent_clones() {
        let auth = enabled_auth(Mode::Required);
        let barrier = Arc::new(std::sync::Barrier::new(16));
        let handles: Vec<_> = (0..16)
            .map(|_| {
                let auth = auth.auth.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || {
                    barrier.wait();
                    auth.token_at(AUDIENCE_SCHEDULER, NOW).unwrap().unwrap()
                })
            })
            .collect();
        let tokens: Vec<_> = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .collect();

        assert!(tokens.windows(2).all(|pair| pair[0] == pair[1]));
    }

    #[test]
    fn enforces_server_modes() {
        let disabled = GrpcAuth::default();
        assert_eq!(
            disabled
                .authenticate_at(&MetadataMap::new(), AUDIENCE_DFDAEMON, NOW)
                .unwrap(),
            AuthenticationOutcome::Disabled
        );

        let permissive = enabled_auth(Mode::Permissive);
        assert_eq!(
            permissive
                .authenticate_at(&MetadataMap::new(), AUDIENCE_DFDAEMON, NOW)
                .unwrap(),
            AuthenticationOutcome::PermissiveMissing
        );

        let required = enabled_auth(Mode::Required);
        assert_eq!(
            required
                .authenticate_at(&MetadataMap::new(), AUDIENCE_DFDAEMON, NOW)
                .unwrap_err()
                .reason(),
            REASON_MISSING
        );
    }

    #[test]
    fn rejects_wrong_audience_and_expired_token() {
        let fixture = interoperability_fixture();
        let auth = enabled_auth(Mode::Required);
        assert_eq!(
            auth.verify_at(&fixture.go_token, AUDIENCE_MANAGER, NOW)
                .unwrap_err()
                .reason(),
            REASON_INVALID_AUDIENCE
        );
        assert_eq!(
            auth.verify_at(
                &fixture.go_token,
                AUDIENCE_SCHEDULER,
                NOW + default_token_ttl().as_secs() + default_clock_skew().as_secs()
            )
            .unwrap_err()
            .reason(),
            REASON_EXPIRED
        );
    }

    #[test]
    fn rejects_invalid_key_configuration() {
        let mut auth = enabled_auth(Mode::Required);
        auth.jwt.active_key_id = "missing".to_string();
        assert!(auth.validate().is_err());
    }

    #[test]
    fn rejects_plaintext_when_required() {
        let mut auth = enabled_auth(Mode::Required);
        auth.require_transport_security = true;
        assert!(auth.ensure_transport_security(false).is_err());
        assert!(auth.ensure_transport_security(true).is_ok());
    }
}
