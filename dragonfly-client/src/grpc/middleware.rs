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
use tower::{BoxError, Layer, Service, ServiceExt};

/// HEALTH_CHECK_PATH_PREFIX is the request path prefix of the gRPC health checking
/// service (`grpc.health.v1.Health`), which is used by the kubelet liveness/readiness
/// probes via `grpc_health_probe`.
const HEALTH_CHECK_PATH_PREFIX: &str = "/grpc.health.v1.Health/";

/// Tower layer that lets gRPC health checking requests bypass the wrapped layer.
///
/// The health checking service is registered on the same gRPC server as the business
/// services. When the request rate limiting, buffering and load shedding layers are
/// applied to the whole server, a burst of business traffic makes the health checks fail
/// with `RESOURCE_EXHAUSTED`, so kubelet restarts a process that is only busy, not broken.
/// This layer forwards requests whose path starts with `/grpc.health.v1.Health/` directly
/// to the inner service, while every other request still goes through the wrapped layer.
///
/// # Usage
///
/// ```ignore
/// Server::builder()
///     .layer(HealthCheckBypassLayer::new(
///         ServiceBuilder::new()
///             .layer(LoadShedLayer::new())
///             .layer(BufferLayer::new(request_buffer_size))
///             .layer(RateLimitLayer::new(request_rate_limit, Duration::from_secs(1))),
///     ))
///     .add_service(health_service)
///     .add_service(grpc_service)
///     .serve(addr)
///     .await?;
/// ```
#[derive(Clone)]
pub struct HealthCheckBypassLayer<L> {
    layer: L,
}

/// Wraps the layer that health checking requests should bypass.
impl<L> HealthCheckBypassLayer<L> {
    /// Creates a new `HealthCheckBypassLayer` wrapping the given layer.
    pub fn new(layer: L) -> Self {
        Self { layer }
    }
}

/// Tower layer that builds a [`HealthCheckBypassService`] from a cloneable inner service.
impl<S, L> Layer<S> for HealthCheckBypassLayer<L>
where
    S: Clone,
    L: Layer<S>,
{
    type Service = HealthCheckBypassService<S, L::Service>;

    /// One clone of the inner service is served directly to health checking requests and
    /// another clone is wrapped by the bypassed layer for all other requests.
    fn layer(&self, inner: S) -> Self::Service {
        HealthCheckBypassService {
            limited: self.layer.layer(inner.clone()),
            inner,
        }
    }
}

/// Tower service that routes gRPC health checking requests directly to the inner service
/// and every other request through the wrapped (limited) service.
#[derive(Clone)]
pub struct HealthCheckBypassService<S, T> {
    inner: S,
    limited: T,
}

/// Service implementation that dispatches requests by their path. Health checking requests
/// are forwarded to the inner service, other requests are forwarded to the limited service.
impl<S, T> Service<Request<Body>> for HealthCheckBypassService<S, T>
where
    S: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<BoxError> + Send + 'static,
    T: Service<Request<Body>, Response = Response<Body>> + Clone + Send + 'static,
    T::Future: Send + 'static,
    T::Error: Into<BoxError> + Send + 'static,
{
    type Response = Response<Body>;
    type Error = BoxError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    /// Polls only the inner service. The limited service is driven in `call` with a oneshot,
    /// so health checking requests never reserve capacity in the limited service.
    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx).map_err(Into::into)
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        if req.uri().path().starts_with(HEALTH_CHECK_PATH_PREFIX) {
            let inner_clone = self.inner.clone();
            let mut inner = std::mem::replace(&mut self.inner, inner_clone);
            return Box::pin(async move { inner.call(req).await.map_err(Into::into) });
        }

        let limited = self.limited.clone();
        Box::pin(async move { limited.oneshot(req).await.map_err(Into::into) })
    }
}

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

/// A simple wrapper around a shared BBR instance. It implements the Tower Layer trait
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
    use std::convert::Infallible;
    use std::future::{ready, Ready};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::Waker;
    use std::time::Duration;
    use tonic::Code;
    use tower::{
        buffer::BufferLayer,
        limit::rate::RateLimitLayer,
        load_shed::{error::Overloaded, LoadShedLayer},
        service_fn, ServiceBuilder, ServiceExt,
    };

    #[derive(Clone)]
    struct RejectLayer {
        calls: Arc<AtomicUsize>,
    }

    impl<S> Layer<S> for RejectLayer {
        type Service = RejectService;

        fn layer(&self, _inner: S) -> Self::Service {
            RejectService {
                calls: self.calls.clone(),
            }
        }
    }

    #[derive(Clone)]
    struct RejectService {
        calls: Arc<AtomicUsize>,
    }

    impl Service<Request<Body>> for RejectService {
        type Response = Response<Body>;
        type Error = Status;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<Body>) -> Self::Future {
            self.calls.fetch_add(1, Ordering::SeqCst);
            ready(Err(Status::resource_exhausted("rejected")))
        }
    }

    fn request(path: &str) -> Request<Body> {
        Request::builder().uri(path).body(Body::empty()).unwrap()
    }

    // A concrete service type is used instead of `service_fn` so that its `Future`
    // associated type is nameable and provably `Send`, which the bypass service requires.
    #[derive(Clone)]
    struct OkService;

    impl Service<Request<Body>> for OkService {
        type Response = Response<Body>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;

        fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }

        fn call(&mut self, _req: Request<Body>) -> Self::Future {
            ready(Ok(Response::new(Body::empty())))
        }
    }

    fn ok_service() -> OkService {
        OkService
    }

    fn assert_ok(response: &Response<Body>) {
        assert_eq!(response.status(), http::StatusCode::OK);
        assert!(response.headers().get("grpc-status").is_none());
    }

    fn assert_resource_exhausted(err: &BoxError, message: &str) {
        let status = err
            .downcast_ref::<Status>()
            .expect("error should be a Status");
        assert_eq!(status.code(), Code::ResourceExhausted);
        assert_eq!(status.message(), message);
    }

    #[tokio::test]
    async fn health_check_bypass_service_skips_wrapped_layer_for_health_requests() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut service = HealthCheckBypassLayer::new(RejectLayer {
            calls: calls.clone(),
        })
        .layer(ok_service());

        let response = service
            .ready()
            .await
            .unwrap()
            .call(request("/grpc.health.v1.Health/Check"))
            .await
            .unwrap();
        assert_ok(&response);
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let err = service
            .ready()
            .await
            .unwrap()
            .call(request("/dfdaemon.v2.DfdaemonDownload/DownloadTask"))
            .await
            .unwrap_err();
        assert_resource_exhausted(&err, "rejected");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn health_check_bypass_service_keeps_health_checks_serving_when_rate_limited() {
        let mut service = HealthCheckBypassLayer::new(
            ServiceBuilder::new()
                .map_err(|err: BoxError| {
                    if err.is::<Overloaded>() {
                        Status::resource_exhausted(
                            "server is overloaded: too many requests, please retry later",
                        )
                    } else {
                        Status::internal(err.to_string())
                    }
                })
                .layer(LoadShedLayer::new())
                .layer(BufferLayer::new(1))
                .layer(RateLimitLayer::new(1, Duration::from_secs(3600))),
        )
        .layer(ok_service());

        // Consume the only token of the rate limit window with a business request.
        let response = service
            .ready()
            .await
            .unwrap()
            .call(request("/dfdaemon.v2.DfdaemonDownload/DownloadTask"))
            .await
            .unwrap();
        assert_ok(&response);

        // Poll further business requests without yielding so the buffer fills up and the
        // load shedder starts rejecting.
        let mut cx = Context::from_waker(Waker::noop());
        let mut pending = Vec::new();
        let mut shed = false;
        for _ in 0..10 {
            let mut future = service
                .ready()
                .await
                .unwrap()
                .call(request("/dfdaemon.v2.DfdaemonDownload/DownloadTask"));
            match future.as_mut().poll(&mut cx) {
                Poll::Ready(Err(err)) => {
                    assert_resource_exhausted(
                        &err,
                        "server is overloaded: too many requests, please retry later",
                    );
                    shed = true;
                    break;
                }
                Poll::Ready(Ok(_)) => panic!("rate limited request should not succeed"),
                Poll::Pending => pending.push(future),
            }
        }
        assert!(shed, "business requests should be shed when overloaded");

        for _ in 0..5 {
            let response = service
                .ready()
                .await
                .unwrap()
                .call(request("/grpc.health.v1.Health/Check"))
                .await
                .unwrap();
            assert_ok(&response);
        }
    }

    #[tokio::test]
    async fn bbr_service_forwards_requests_when_not_overloaded() {
        let bbr = Arc::new(
            BBR::new(BBRConfig {
                cpu_threshold: 100,
                memory_threshold: 100,
                ..Default::default()
            })
            .await,
        );
        let mut service = BBRLayer::new(bbr).layer(service_fn(|_request: Request<Body>| async {
            Ok::<_, Infallible>(Response::new(Body::empty()))
        }));

        let response = service
            .ready()
            .await
            .unwrap()
            .call(Request::new(Body::empty()))
            .await
            .unwrap();
        assert_eq!(response.status(), http::StatusCode::OK);
        assert!(response.headers().get("grpc-status").is_none());
    }
}
