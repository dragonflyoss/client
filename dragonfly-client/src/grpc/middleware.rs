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

use dragonfly_client_util::ratelimiter::bbr::{BBRConfig, BBR};
use hyper::body::Body;
use std::sync::Arc;
use tonic::codegen::http::{Request, Response};
use tonic::Status;
use tonic_middleware::{Middleware, ServiceBound};
use tracing::warn;

/// gRPC middleware that performs BBR-based adaptive rate limiting.
///
/// Wraps the [`BBR`] rate limiter to integrate with `tonic-middleware`.
/// When the system is overloaded (CPU/memory thresholds exceeded and
/// in-flight requests surpass the estimated capacity), incoming gRPC
/// requests are rejected with `RESOURCE_EXHAUSTED` status.
///
/// # Usage
///
/// ```ignore
/// use dragonfly_client_util::ratelimiter::middleware::BBRMiddleware;
/// use dragonfly_client_util::ratelimiter::bbr::BBRConfig;
/// use tonic_middleware::MiddlewareFor;
///
/// let bbr_middleware = BBRMiddleware::new(BBRConfig::default()).await;
///
/// // Apply to an individual gRPC service:
/// let service_with_bbr = MiddlewareFor::new(grpc_service, bbr_middleware);
///
/// // Or apply to all services via layer:
/// use tonic_middleware::MiddlewareLayer;
/// Server::builder()
///     .layer(MiddlewareLayer::new(bbr_middleware))
///     .add_service(grpc_service)
///     .serve(addr)
///     .await?;
/// ```
#[derive(Clone)]
pub struct BBRMiddleware {
    /// Shared BBR rate limiter instance. Wrapped in `Arc` so the middleware
    /// can be cheaply cloned (required by `tonic-middleware`).
    bbr: Arc<BBR>,
}

/// BBRMiddleware implementation.
impl BBRMiddleware {
    /// Creates a new `BBRMiddleware` with the given BBR configuration.
    ///
    /// This performs an initial synchronous resource collection and spawns
    /// the background CPU/memory sampling loop (via [`BBR::new`]).
    pub async fn new(config: BBRConfig) -> Self {
        Self {
            bbr: Arc::new(BBR::new(config).await),
        }
    }

    /// Creates a new `BBRMiddleware` from an existing [`BBR`] instance.
    ///
    /// Useful when you want to share a single BBR limiter across multiple
    /// middleware instances or other components.
    pub fn from_bbr(bbr: Arc<BBR>) -> Self {
        Self { bbr }
    }
}

#[tonic::async_trait]
impl<S> Middleware<S> for BBRMiddleware
where
    S: ServiceBound,
    S::Future: Send,
{
    /// Attempts to acquire a BBR permit before forwarding the request.
    ///
    /// If the system is overloaded and the request is shed, returns a
    /// `RESOURCE_EXHAUSTED` gRPC status immediately without calling the
    /// inner service. Otherwise, the request is forwarded and the guard
    /// is held until the response completes, ensuring accurate in-flight
    /// tracking and response time measurement.
    async fn call(&self, req: Request<Body>, mut service: S) -> Result<Response<Body>, S::Error> {
        // Try to acquire a BBR permit. If the system is overloaded,
        // the permit will be None and we reject the request.
        let _guard = match self.bbr.acquire().await {
            Some(guard) => guard,
            None => {
                warn!("BBR rate limiter rejected gRPC request: system overloaded");

                // Build a RESOURCE_EXHAUSTED response and convert it to
                // the http::Response<Body> expected by tonic-middleware.
                let status = Status::resource_exhausted("server is overloaded, please retry later");
                let response = status.into_http();
                return Ok(response);
            }
        };

        // Forward the request to the inner service. The guard remains alive
        // for the duration of the call, so in-flight count and response
        // time are tracked accurately.
        service.call(req).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_bbr_middleware_creation() {
        let middleware = BBRMiddleware::new(BBRConfig::default()).await;
        // Verify the middleware can be cloned (required by tonic-middleware).
        let _cloned = middleware.clone();
    }

    #[tokio::test]
    async fn test_bbr_middleware_from_bbr() {
        let bbr = Arc::new(BBR::new(BBRConfig::default()).await);
        let middleware = BBRMiddleware::from_bbr(bbr.clone());
        let _cloned = middleware.clone();
    }
}
