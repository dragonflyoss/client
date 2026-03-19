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

use dragonfly_client_util::ratelimiter::bbr::BBR;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tonic::body::Body;
use tonic::codegen::http::{Request, Response};
use tonic::Status;
use tower::{Layer, Service};

/// gRPC middleware that performs BBR-based adaptive rate limiting.
///
/// Wraps the [`BBR`] rate limiter to integrate with tower::Layer and tower::Service, allowing it
/// to be easily applied to gRPC services. When the system is overloaded (CPU/memory thresholds
/// exceeded and in-flight requests surpass the estimated capacity), incoming gRPC
/// requests are rejected with `RESOURCE_EXHAUSTED` status.
///
/// # Usage
///
/// ```ignore
/// use dragonfly_client_util::ratelimiter::middleware::{BBRLayer, BBRConfig};
///
/// let bbr = Arc::new(BBR::new(BBRConfig::default()).await);
///
/// Server::builder()
///     .layer(BBRLayer::new(bbr))
///     .add_service(grpc_service)
///     .serve(addr)
///     .await?;
/// ```
#[derive(Clone)]
pub struct BBRLayer {
    bbr: Arc<BBR>,
}

/// BBRLayer is a simple wrapper around a shared BBR instance. It implements the Tower Layer trait
/// to create a BBRService for each inner service. The BBRService is where the actual rate limiting
/// logic is applied to incoming gRPC requests.
impl BBRLayer {
    /// Creates a new `BBRLayer` from a shared [`BBR`] instance.
    pub fn new(bbr: Arc<BBR>) -> Self {
        Self { bbr }
    }
}

/// Tower layer that applies BBR-based rate limiting to gRPC requests.
impl<S> Layer<S> for BBRLayer {
    type Service = BBRService<S>;

    /// Wraps the inner service with BBR-based rate limiting. The returned
    /// service will attempt to acquire a BBR permit for each incoming request,
    /// rejecting requests immediately if the system is overloaded.
    fn layer(&self, inner: S) -> Self::Service {
        BBRService {
            inner,
            bbr: self.bbr.clone(),
        }
    }
}

/// Tower service that applies BBR-based rate limiting to gRPC requests.
#[derive(Clone)]
pub struct BBRService<S> {
    inner: S,
    bbr: Arc<BBR>,
}

/// Service implementation that attempts to acquire a BBR permit before forwarding each request. If the system is overloaded, it returns a
/// `RESOURCE_EXHAUSTED` status immediately without calling the inner service. Otherwise, it
/// forwards the request and holds the guard until the response completes, ensuring accurate
/// in-flight tracking and response time measurement.
impl<S> Service<Request<Body>> for BBRService<S>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    /// Polls the inner service to check if it's ready to accept a request. This is a standard part
    /// of the Tower service lifecycle and is required before calling the service. The BBR rate
    /// limiter does not affect the readiness of the inner service, so we simply delegate to it.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    /// Attempts to acquire a BBR permit before forwarding the request.
    ///
    /// If the system is overloaded and the request is shed, returns a
    /// `RESOURCE_EXHAUSTED` gRPC status immediately without calling the
    /// inner service. Otherwise, the request is forwarded and the guard
    /// is held until the response completes, ensuring accurate in-flight
    /// tracking and response time measurement.
    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let bbr = self.bbr.clone();
        let inner_clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, inner_clone);
        Box::pin(async move {
            // Try to acquire a BBR permit. If the system is overloaded,
            // the permit will be None and we reject the request.
            let _guard = match bbr.acquire().await {
                Some(guard) => guard,
                None => {
                    return Ok(Status::resource_exhausted(
                        "server is overloaded: CPU/memory thresholds are exceeded, please retry later",
                    )
                    .into_http());
                }
            };

            inner.call(req).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_client_util::ratelimiter::bbr::BBRConfig;

    #[tokio::test]
    async fn test_bbr_layer_creation() {
        let bbr = Arc::new(BBR::new(BBRConfig::default()).await);
        let layer = BBRLayer::new(bbr);
        let _ = layer.clone();
    }
}
