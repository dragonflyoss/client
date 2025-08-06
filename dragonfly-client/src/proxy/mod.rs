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

use crate::grpc::{dfdaemon_download::DfdaemonDownloadClient, REQUEST_TIMEOUT};
use crate::metrics::{
    collect_proxy_request_failure_metrics, collect_proxy_request_started_metrics,
    collect_proxy_request_via_dfdaemon_metrics,
};
use crate::resource::{piece::MIN_PIECE_LENGTH, task::Task};
use crate::shutdown;
use bytes::Bytes;
use dragonfly_api::common::v2::{Download, TaskType};
use dragonfly_api::dfdaemon::v2::{
    download_task_response, DownloadTaskRequest, DownloadTaskStartedResponse,
};
use dragonfly_api::errordetails::v2::Backend;
use dragonfly_client_config::dfdaemon::{Config, Rule};
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use dragonfly_client_util::{
    http::{hashmap_to_headermap, headermap_to_hashmap},
    tls::{generate_self_signed_certs_by_ca_cert, generate_simple_self_signed_certs, NoVerifier},
};
use futures::TryStreamExt;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, StreamBody};
use hyper::body::Frame;
use hyper::client::conn::http1::Builder as ClientBuilder;
use hyper::server::conn::http1::Builder as ServerBuilder;
use hyper::service::service_fn;
use hyper::upgrade::Upgraded;
use hyper::{Method, Request};
use hyper_util::{
    client::legacy::Client,
    rt::{tokio::TokioIo, TokioExecutor},
};
use lazy_static::lazy_static;
use rcgen::Certificate;
use rustls::{RootCertStore, ServerConfig};
use rustls_pki_types::CertificateDer;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Barrier};
use tokio_rustls::TlsAcceptor;
use tokio_util::io::ReaderStream;
use tracing::{debug, error, info, instrument, Instrument, Span};

pub mod header;

lazy_static! {
  /// SUPPORTED_HTTP_PROTOCOLS is the supported HTTP protocols, including http/1.1 and http/1.0.
  static ref SUPPORTED_HTTP_PROTOCOLS: Vec<Vec<u8>> = vec![b"http/1.1".to_vec(), b"http/1.0".to_vec()];
}

/// Response is the response of the proxy server.
pub type Response = hyper::Response<BoxBody<Bytes, ClientError>>;

/// Proxy is the proxy server.
pub struct Proxy {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// task is the task manager.
    task: Arc<Task>,

    /// addr is the address of the proxy server.
    addr: SocketAddr,

    /// registry_cert is the certificate of the client for the registry.
    registry_cert: Arc<Option<Vec<CertificateDer<'static>>>>,

    /// server_ca_cert is the CA certificate of the proxy server to
    /// sign the self-signed certificate.
    server_ca_cert: Arc<Option<Certificate>>,

    /// shutdown is used to shutdown the proxy server.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the proxy server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

/// Proxy implements the proxy server.
impl Proxy {
    /// new creates a new Proxy.
    pub fn new(
        config: Arc<Config>,
        task: Arc<Task>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        let mut proxy = Self {
            config: config.clone(),
            task: task.clone(),
            addr: SocketAddr::new(config.proxy.server.ip.unwrap(), config.proxy.server.port),
            registry_cert: Arc::new(None),
            server_ca_cert: Arc::new(None),
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        };

        // Load and generate the registry certificates from the PEM format file.
        proxy.registry_cert = match config.proxy.registry_mirror.load_cert_der() {
            Ok(registry_cert) => {
                info!("load registry cert success");
                Arc::new(registry_cert)
            }
            Err(err) => {
                error!("load registry cert failed: {}", err);
                Arc::new(None)
            }
        };

        // Generate the CA certificate and key from the PEM format files.
        proxy.server_ca_cert = match config.proxy.server.load_cert() {
            Ok(server_ca_cert) => {
                info!("load proxy ca cert and key success");
                Arc::new(server_ca_cert)
            }
            Err(err) => {
                error!("load proxy ca cert and key failed: {}", err);
                Arc::new(None)
            }
        };

        proxy
    }

    /// run starts the proxy server.
    pub async fn run(&self, grpc_server_started_barrier: Arc<Barrier>) -> ClientResult<()> {
        let mut shutdown = self.shutdown.clone();
        let read_buffer_size = self.config.proxy.read_buffer_size;

        // When the grpc server is started, notify the barrier. If the shutdown signal is received
        // before barrier is waited successfully, the server will shutdown immediately.
        tokio::select! {
            // Wait for starting the proxy server
            _ = grpc_server_started_barrier.wait() => {
                info!("proxy server is ready to start");
            }
            _ = shutdown.recv() => {
                // Proxy server shutting down with signals.
                info!("proxy server shutting down");
                return Ok(());
            }
        }

        let dfdaemon_download_client =
            DfdaemonDownloadClient::new_unix(self.config.download.server.socket_path.clone())
                .await?;

        #[derive(Clone)]
        struct Context {
            config: Arc<Config>,
            task: Arc<Task>,
            dfdaemon_download_client: DfdaemonDownloadClient,
            registry_cert: Arc<Option<Vec<CertificateDer<'static>>>>,
            server_ca_cert: Arc<Option<Certificate>>,
        }

        let context = Context {
            config: self.config.clone(),
            task: self.task.clone(),
            dfdaemon_download_client,
            registry_cert: self.registry_cert.clone(),
            server_ca_cert: self.server_ca_cert.clone(),
        };

        let listener = TcpListener::bind(self.addr).await?;
        info!("proxy server listening on {}", self.addr);

        loop {
            // Wait for a client connection.
            tokio::select! {
                tcp_accepted = listener.accept() => {
                    // A new client connection has been established.
                    let (tcp, remote_address) = tcp_accepted?;

                    // Spawn a task to handle the connection.
                    let io = TokioIo::new(tcp);
                    debug!("accepted connection from {}", remote_address);

                    let context = context.clone();
                    tokio::task::spawn(async move {
                        if let Err(err) = ServerBuilder::new()
                            .keep_alive(true)
                            .max_buf_size(read_buffer_size)
                            .preserve_header_case(true)
                            .title_case_headers(true)
                            .serve_connection(
                                io,
                                service_fn(move |request|{
                                    let context = context.clone();
                                    async move {
                                        handler(context.config, context.task, request, context.dfdaemon_download_client, context.registry_cert, context.server_ca_cert, remote_address.ip()).await
                                    }
                                } ),
                                )
                            .with_upgrades()
                            .await
                        {
                            collect_proxy_request_failure_metrics();
                            error!("failed to serve connection from {}: {}", remote_address, err);
                        }
                    });
                }
                _ = shutdown.recv() => {
                    // Proxy server shutting down with signals.
                    info!("proxy server shutting down");
                    return Ok(());
                }
            }
        }
    }
}

/// handler handles the request from the client.
#[instrument(skip_all, fields(url, method, remote_ip))]
pub async fn handler(
    config: Arc<Config>,
    task: Arc<Task>,
    request: Request<hyper::body::Incoming>,
    dfdaemon_download_client: DfdaemonDownloadClient,
    registry_cert: Arc<Option<Vec<CertificateDer<'static>>>>,
    server_ca_cert: Arc<Option<Certificate>>,
    remote_ip: std::net::IpAddr,
) -> ClientResult<Response> {
    // Span record the url and method.
    Span::current().record("url", request.uri().to_string().as_str());
    Span::current().record("method", request.method().as_str());
    Span::current().record("remote_ip", remote_ip.to_string().as_str());

    // Record the proxy request started metrics. The metrics will be recorded
    // when the request is kept alive.
    collect_proxy_request_started_metrics();

    // If host is not set, it is the mirror request.
    if request.uri().host().is_none() {
        // Handle CONNECT request.
        if Method::CONNECT == request.method() {
            return registry_mirror_https_handler(
                config,
                task,
                request,
                remote_ip,
                dfdaemon_download_client,
                registry_cert,
                server_ca_cert,
            )
            .await;
        }

        return registry_mirror_http_handler(
            config,
            task,
            request,
            remote_ip,
            dfdaemon_download_client,
            registry_cert,
        )
        .await;
    }

    // Handle CONNECT request.
    if Method::CONNECT == request.method() {
        return https_handler(
            config,
            task,
            request,
            remote_ip,
            dfdaemon_download_client,
            registry_cert,
            server_ca_cert,
        )
        .await;
    }

    http_handler(
        config,
        task,
        request,
        remote_ip,
        dfdaemon_download_client,
        registry_cert,
    )
    .await
}

/// registry_mirror_http_handler handles the http request for the registry mirror by client.
#[instrument(skip_all)]
pub async fn registry_mirror_http_handler(
    config: Arc<Config>,
    task: Arc<Task>,
    request: Request<hyper::body::Incoming>,
    remote_ip: std::net::IpAddr,
    dfdaemon_download_client: DfdaemonDownloadClient,
    registry_cert: Arc<Option<Vec<CertificateDer<'static>>>>,
) -> ClientResult<Response> {
    let request = make_registry_mirror_request(config.clone(), request)?;
    return http_handler(
        config,
        task,
        request,
        remote_ip,
        dfdaemon_download_client,
        registry_cert,
    )
    .await;
}

/// registry_mirror_https_handler handles the https request for the registry mirror by client.
#[instrument(skip_all)]
pub async fn registry_mirror_https_handler(
    config: Arc<Config>,
    task: Arc<Task>,
    request: Request<hyper::body::Incoming>,
    remote_ip: std::net::IpAddr,
    dfdaemon_download_client: DfdaemonDownloadClient,
    registry_cert: Arc<Option<Vec<CertificateDer<'static>>>>,
    server_ca_cert: Arc<Option<Certificate>>,
) -> ClientResult<Response> {
    let request = make_registry_mirror_request(config.clone(), request)?;
    return https_handler(
        config,
        task,
        request,
        remote_ip,
        dfdaemon_download_client,
        registry_cert,
        server_ca_cert,
    )
    .await;
}

/// http_handler handles the http request by client.
#[instrument(skip_all)]
pub async fn http_handler(
    config: Arc<Config>,
    task: Arc<Task>,
    request: Request<hyper::body::Incoming>,
    remote_ip: std::net::IpAddr,
    dfdaemon_download_client: DfdaemonDownloadClient,
    registry_cert: Arc<Option<Vec<CertificateDer<'static>>>>,
) -> ClientResult<Response> {
    info!("handle HTTP request: {:?}", request);

    // Authenticate the request with the basic auth.
    if let Some(basic_auth) = config.proxy.server.basic_auth.as_ref() {
        match basic_auth.credentials().verify(request.headers()) {
            Ok(_) => {}
            Err(ClientError::Unauthorized) => {
                error!("basic auth failed");
                return Ok(make_error_response(http::StatusCode::UNAUTHORIZED, None));
            }
            Err(err) => {
                error!("verify basic auth failed: {}", err);
                return Ok(make_error_response(http::StatusCode::BAD_REQUEST, None));
            }
        }
    }

    // If find the matching rule, proxy the request via the dfdaemon.
    let request_uri = request.uri();
    if let Some(rule) = find_matching_rule(
        config.proxy.rules.as_deref(),
        request_uri.to_string().as_str(),
    ) {
        info!(
            "proxy HTTP request via dfdaemon by rule config for method: {}, uri: {}",
            request.method(),
            request_uri
        );
        return proxy_via_dfdaemon(
            config,
            task,
            &rule,
            request,
            remote_ip,
            dfdaemon_download_client,
        )
        .await;
    }

    // If the request header contains the X-Dragonfly-Use-P2P header, proxy the request via the
    // dfdaemon.
    if header::get_use_p2p(request.headers()) {
        info!(
            "proxy HTTP request via dfdaemon by X-Dragonfly-Use-P2P header for method: {}, uri: {}",
            request.method(),
            request_uri
        );
        return proxy_via_dfdaemon(
            config,
            task,
            &Rule::default(),
            request,
            remote_ip,
            dfdaemon_download_client,
        )
        .await;
    }

    if request.uri().scheme().cloned() == Some(http::uri::Scheme::HTTPS) {
        info!(
            "proxy HTTPS request directly to remote server for method: {}, uri: {}",
            request.method(),
            request.uri()
        );
        return proxy_via_https(request, registry_cert).await;
    }

    info!(
        "proxy HTTP request directly to remote server for method: {}, uri: {}",
        request.method(),
        request.uri()
    );
    return proxy_via_http(request).await;
}

/// https_handler handles the https request by client.
#[instrument(skip_all)]
pub async fn https_handler(
    config: Arc<Config>,
    task: Arc<Task>,
    request: Request<hyper::body::Incoming>,
    remote_ip: std::net::IpAddr,
    dfdaemon_download_client: DfdaemonDownloadClient,
    registry_cert: Arc<Option<Vec<CertificateDer<'static>>>>,
    server_ca_cert: Arc<Option<Certificate>>,
) -> ClientResult<Response> {
    info!("handle HTTPS request: {:?}", request);

    // Proxy the request directly  to the remote server.
    if let Some(host) = request.uri().host() {
        let host = host.to_string();
        let port = request.uri().port_u16().unwrap_or(443);
        tokio::task::spawn(async move {
            match hyper::upgrade::on(request).await {
                Ok(upgraded) => {
                    if let Err(e) = upgraded_tunnel(
                        config,
                        task,
                        upgraded,
                        host,
                        port,
                        remote_ip,
                        dfdaemon_download_client,
                        registry_cert,
                        server_ca_cert,
                    )
                    .await
                    {
                        error!("server io error: {}", e);
                    };
                }
                Err(e) => error!("upgrade error: {}", e),
            }
        });

        Ok(Response::new(empty()))
    } else {
        return Ok(make_error_response(http::StatusCode::BAD_REQUEST, None));
    }
}

/// upgraded_tunnel handles the upgraded connection. If the ca_cert is not set, use the
/// self-signed certificate. Otherwise, use the CA certificate to sign the
/// self-signed certificate.
#[allow(clippy::too_many_arguments)]
#[instrument(skip_all)]
async fn upgraded_tunnel(
    config: Arc<Config>,
    task: Arc<Task>,
    upgraded: Upgraded,
    host: String,
    port: u16,
    remote_ip: std::net::IpAddr,
    dfdaemon_download_client: DfdaemonDownloadClient,
    registry_cert: Arc<Option<Vec<CertificateDer<'static>>>>,
    server_ca_cert: Arc<Option<Certificate>>,
) -> ClientResult<()> {
    // Generate the self-signed certificate by the given host. If the ca_cert
    // is not set, use the self-signed certificate. Otherwise, use the CA
    // certificate to sign the self-signed certificate.
    let subject_alt_names = vec![host.to_string()];
    let (server_certs, server_key) = match server_ca_cert.as_ref() {
        Some(server_ca_cert) => {
            info!("generate self-signed certificate by CA certificate");
            generate_self_signed_certs_by_ca_cert(server_ca_cert, host.as_ref(), subject_alt_names)?
        }
        None => {
            info!("generate simple self-signed certificate");
            generate_simple_self_signed_certs(host.as_ref(), subject_alt_names)?
        }
    };

    // Build TLS configuration.
    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(server_certs, server_key)
        .or_err(ErrorType::TLSConfigError)?;
    server_config.alpn_protocols = SUPPORTED_HTTP_PROTOCOLS.clone();

    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));
    let tls_stream = tls_acceptor.accept(TokioIo::new(upgraded)).await?;

    // Serve the connection with the TLS stream.
    // Ensure the connection uses HTTP/1 to prevent version mismatch errors, such as:
    //
    // user => proxy => backend
    //    http2    http1
    //
    // user => proxy => backend
    //    http1    http2
    if let Err(err) = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
        .http1_only()
        .serve_connection(
            TokioIo::new(tls_stream),
            service_fn(move |request| {
                upgraded_handler(
                    config.clone(),
                    task.clone(),
                    host.clone(),
                    port,
                    request,
                    remote_ip,
                    dfdaemon_download_client.clone(),
                    registry_cert.clone(),
                )
            }),
        )
        .await
    {
        error!("failed to serve connection: {}", err);
        return Err(ClientError::Unknown(err.to_string()));
    }

    Ok(())
}

/// upgraded_handler handles the upgraded https request from the client.
#[allow(clippy::too_many_arguments)]
#[instrument(skip_all, fields(url, method))]
pub async fn upgraded_handler(
    config: Arc<Config>,
    task: Arc<Task>,
    host: String,
    port: u16,
    mut request: Request<hyper::body::Incoming>,
    remote_ip: std::net::IpAddr,
    dfdaemon_download_client: DfdaemonDownloadClient,
    registry_cert: Arc<Option<Vec<CertificateDer<'static>>>>,
) -> ClientResult<Response> {
    // Span record the url and method.
    Span::current().record("url", request.uri().to_string().as_str());
    Span::current().record("method", request.method().as_str());

    // Authenticate the request with the basic auth.
    if let Some(basic_auth) = config.proxy.server.basic_auth.as_ref() {
        match basic_auth.credentials().verify(request.headers()) {
            Ok(_) => {}
            Err(ClientError::Unauthorized) => {
                return Ok(make_error_response(http::StatusCode::UNAUTHORIZED, None));
            }
            Err(err) => {
                error!("verify basic auth failed: {}", err);
                return Ok(make_error_response(http::StatusCode::BAD_REQUEST, None));
            }
        }
    }

    // If the scheme is not set, set the scheme to https.
    if request.uri().scheme().is_none() {
        let builder = http::uri::Builder::new();
        *request.uri_mut() = builder
            .scheme("https")
            .authority(format!("{}:{}", host, port))
            .path_and_query(
                request
                    .uri()
                    .path_and_query()
                    .map(|v| v.as_str())
                    .unwrap_or("/"),
            )
            .build()
            .or_err(ErrorType::ParseError)?;
    }

    // If find the matching rule, proxy the request via the dfdaemon.
    let request_uri = request.uri();
    if let Some(rule) = find_matching_rule(
        config.proxy.rules.as_deref(),
        request_uri.to_string().as_str(),
    ) {
        info!(
            "proxy HTTPS request via dfdaemon by rule config for method: {}, uri: {}",
            request.method(),
            request_uri
        );
        return proxy_via_dfdaemon(
            config,
            task,
            &rule,
            request,
            remote_ip,
            dfdaemon_download_client,
        )
        .await;
    }

    // If the request header contains the X-Dragonfly-Use-P2P header, proxy the request via the
    // dfdaemon.
    if header::get_use_p2p(request.headers()) {
        info!(
            "proxy HTTP request via dfdaemon by X-Dragonfly-Use-P2P header for method: {}, uri: {}",
            request.method(),
            request_uri
        );
        return proxy_via_dfdaemon(
            config,
            task,
            &Rule::default(),
            request,
            remote_ip,
            dfdaemon_download_client,
        )
        .await;
    }

    if request.uri().scheme().cloned() == Some(http::uri::Scheme::HTTPS) {
        info!(
            "proxy HTTPS request directly to remote server for method: {}, uri: {}",
            request.method(),
            request.uri()
        );
        return proxy_via_https(request, registry_cert).await;
    }

    info!(
        "proxy HTTP request directly to remote server for method: {}, uri: {}",
        request.method(),
        request.uri()
    );
    return proxy_via_http(request).await;
}

/// proxy_via_dfdaemon proxies the request via the dfdaemon.
#[instrument(skip_all, fields(host_id, task_id, peer_id))]
async fn proxy_via_dfdaemon(
    config: Arc<Config>,
    task: Arc<Task>,
    rule: &Rule,
    request: Request<hyper::body::Incoming>,
    remote_ip: std::net::IpAddr,
    dfdaemon_download_client: DfdaemonDownloadClient,
) -> ClientResult<Response> {
    // Collect the metrics for the proxy request via dfdaemon.
    collect_proxy_request_via_dfdaemon_metrics();

    // Make the download task request.
    let download_task_request =
        match make_download_task_request(config.clone(), rule, request, remote_ip) {
            Ok(download_task_request) => download_task_request,
            Err(err) => {
                error!("make download task request failed: {}", err);
                return Ok(make_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    None,
                ));
            }
        };

    // Download the task by the dfdaemon download client.
    let response = match dfdaemon_download_client
        .download_task(download_task_request)
        .await
    {
        Ok(response) => response,
        Err(err) => match err {
            ClientError::TonicStatus(err) => {
                match serde_json::from_slice::<Backend>(err.details()) {
                    Ok(backend) => {
                        error!("download task failed: {:?}", backend);
                        return Ok(make_error_response(
                            http::StatusCode::from_u16(
                                backend.status_code.unwrap_or_default() as u16
                            )
                            .unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR),
                            Some(hashmap_to_headermap(&backend.header)?),
                        ));
                    }
                    Err(_) => {
                        error!("download task failed: {}", err);
                        return Ok(make_error_response(
                            http::StatusCode::INTERNAL_SERVER_ERROR,
                            None,
                        ));
                    }
                };
            }
            _ => {
                error!("download task failed: {}", err);
                return Ok(make_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    None,
                ));
            }
        },
    };

    // Handle the response from the download grpc server.
    let mut out_stream = response.into_inner();
    let Ok(Some(message)) = out_stream.message().await else {
        error!("response message failed");
        return Ok(make_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            None,
        ));
    };

    // Span record the host_id, task_id, and peer_id.
    Span::current().record("host_id", message.host_id.as_str());
    Span::current().record("task_id", message.task_id.as_str());
    Span::current().record("peer_id", message.peer_id.as_str());

    // Handle the download task started response.
    let Some(download_task_response::Response::DownloadTaskStartedResponse(
        download_task_started_response,
    )) = message.response
    else {
        error!("response is not started");
        return Ok(make_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            None,
        ));
    };

    // Write the status code to the writer.
    let (sender, mut receiver) = mpsc::channel(10 * 1024);

    // Get the read buffer size from the config.
    let read_buffer_size = config.proxy.read_buffer_size;

    // Write the task data to the reader.
    let (reader, writer) = tokio::io::duplex(read_buffer_size);
    let mut writer = BufWriter::with_capacity(read_buffer_size, writer);
    let reader_stream = ReaderStream::with_capacity(reader, read_buffer_size);

    // Construct the response body.
    let stream_body = StreamBody::new(reader_stream.map_ok(Frame::data).map_err(ClientError::from));
    let boxed_body = stream_body.boxed();

    // Construct the response.
    let mut response = Response::new(boxed_body);
    *response.headers_mut() = make_response_headers(
        message.task_id.as_str(),
        download_task_started_response.clone(),
    )?;
    *response.status_mut() = http::StatusCode::OK;

    // Return the response if the client return the first piece.
    let mut initialized = false;

    // Write task data to pipe. If grpc received error message,
    // shutdown the writer.
    tokio::spawn(
        async move {
            // Initialize the hashmap of the finished piece readers and pieces.
            let mut finished_piece_readers = HashMap::new();

            // Get the first piece number from the started response.
            let Some(first_piece) = download_task_started_response.pieces.first() else {
                error!("response pieces is empty");
                if let Err(err) = writer.shutdown().await {
                    error!("writer shutdown error: {}", err);
                }

                return;
            };
            let mut need_piece_number = first_piece.number;

            // Read piece data from stream and write to pipe. If the piece data is
            // not in order, store it in the hashmap, and write it to the pipe
            // when the previous piece data is written.
            loop {
                match out_stream.message().await {
                    Ok(Some(message)) => {
                        if let Some(
                            download_task_response::Response::DownloadPieceFinishedResponse(
                                download_task_response,
                            ),
                        ) = message.response
                        {
                            // Send the none response to the client, if the first piece is received.
                            if !initialized {
                                debug!("first piece received, send response");
                                sender
                                    .send_timeout(None, REQUEST_TIMEOUT)
                                    .await
                                    .unwrap_or_default();
                                initialized = true;
                            }

                            let Some(piece) = download_task_response.piece else {
                                error!("response piece is empty");
                                writer.shutdown().await.unwrap_or_else(|err| {
                                    error!("writer shutdown error: {}", err);
                                });

                                return;
                            };

                            let piece_range_reader = match task
                                .piece
                                .download_from_local_into_async_read(
                                    task.piece
                                        .id(message.task_id.as_str(), piece.number)
                                        .as_str(),
                                    message.task_id.as_str(),
                                    piece.length,
                                    download_task_started_response.range,
                                    true,
                                    false,
                                )
                                .await
                            {
                                Ok(piece_range_reader) => piece_range_reader,
                                Err(err) => {
                                    error!("download piece reader error: {}", err);
                                    if let Err(err) = writer.shutdown().await {
                                        error!("writer shutdown error: {}", err);
                                    }

                                    return;
                                }
                            };

                            // Use a buffer to read the piece.
                            let piece_range_reader =
                                BufReader::with_capacity(read_buffer_size, piece_range_reader);

                            // Write the piece data to the pipe in order.
                            finished_piece_readers.insert(piece.number, piece_range_reader);
                            while let Some(mut piece_range_reader) =
                                finished_piece_readers.remove(&need_piece_number)
                            {
                                debug!("copy piece {} to stream", need_piece_number);
                                if let Err(err) =
                                    tokio::io::copy(&mut piece_range_reader, &mut writer).await
                                {
                                    error!("download piece reader error: {}", err);
                                    if let Err(err) = writer.shutdown().await {
                                        error!("writer shutdown error: {}", err);
                                    }

                                    return;
                                }

                                need_piece_number += 1;
                            }
                        } else {
                            error!("response unknown message");
                            writer.shutdown().await.unwrap_or_else(|err| {
                                error!("writer shutdown error: {}", err);
                            });

                            return;
                        }
                    }
                    Ok(None) => {
                        info!("message is none");
                        if let Err(err) = writer.flush().await {
                            error!("writer flush error: {}", err);
                        }

                        return;
                    }
                    Err(err) => {
                        if initialized {
                            error!("stream error: {}", err);
                            if let Err(err) = writer.flush().await {
                                error!("writer flush error: {}", err);
                            }

                            return;
                        }

                        match serde_json::from_slice::<Backend>(err.details()) {
                            Ok(backend) => {
                                error!("download task failed: {:?}", backend);
                                sender
                                    .send_timeout(
                                        Some(make_error_response(
                                            http::StatusCode::from_u16(
                                                backend.status_code.unwrap_or_default() as u16,
                                            )
                                            .unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR),
                                            Some(
                                                hashmap_to_headermap(&backend.header)
                                                    .unwrap_or_default(),
                                            ),
                                        )),
                                        REQUEST_TIMEOUT,
                                    )
                                    .await
                                    .unwrap_or_default();
                            }
                            Err(_) => {
                                error!("download task failed: {}", err);
                                sender
                                    .send_timeout(
                                        Some(make_error_response(
                                            http::StatusCode::INTERNAL_SERVER_ERROR,
                                            None,
                                        )),
                                        REQUEST_TIMEOUT,
                                    )
                                    .await
                                    .unwrap_or_default();
                            }
                        }

                        return;
                    }
                };
            }
        }
        .in_current_span(),
    );

    match receiver.recv().await {
        Some(Some(response)) => Ok(response),
        Some(None) => return Ok(response),
        None => Ok(make_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            None,
        )),
    }
}

/// proxy_via_http proxies the HTTP request directly to the remote server.
#[instrument(skip_all)]
async fn proxy_via_http(request: Request<hyper::body::Incoming>) -> ClientResult<Response> {
    let Some(host) = request.uri().host() else {
        error!("CONNECT host is not socket addr: {:?}", request.uri());
        return Ok(make_error_response(http::StatusCode::BAD_REQUEST, None));
    };
    let port = request.uri().port_u16().unwrap_or(80);

    let stream = TcpStream::connect((host, port)).await?;
    let io = TokioIo::new(stream);
    let (mut client, conn) = ClientBuilder::new()
        .preserve_header_case(true)
        .title_case_headers(true)
        .handshake(io)
        .await?;

    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            error!("connection failed: {:?}", err);
        }
    });

    let response = client.send_request(request).await?;
    Ok(response.map(|b| b.map_err(ClientError::from).boxed()))
}

/// proxy_via_https proxies the HTTPS request directly to the remote server.
#[instrument(skip_all)]
async fn proxy_via_https(
    request: Request<hyper::body::Incoming>,
    registry_cert: Arc<Option<Vec<CertificateDer<'static>>>>,
) -> ClientResult<Response> {
    let client_config_builder = match registry_cert.as_ref() {
        Some(registry_cert) => {
            let mut root_cert_store = RootCertStore::empty();
            root_cert_store.add_parsable_certificates(registry_cert.to_owned());

            // TLS client config using the custom CA store for lookups.
            rustls::ClientConfig::builder()
                .with_root_certificates(root_cert_store)
                .with_no_client_auth()
        }
        // Default TLS client config with native roots.
        None => rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(NoVerifier::new())
            .with_no_client_auth(),
    };

    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(client_config_builder)
        .https_or_http()
        .enable_http1()
        .build();

    let client = Client::builder(TokioExecutor::new()).build(https);
    let response = client.request(request).await.inspect_err(|err| {
        error!("request failed: {:?}", err);
    })?;

    Ok(response.map(|b| b.map_err(ClientError::from).boxed()))
}

/// make_registry_mirror_request makes a registry mirror request by the request.
fn make_registry_mirror_request(
    config: Arc<Config>,
    mut request: Request<hyper::body::Incoming>,
) -> ClientResult<Request<hyper::body::Incoming>> {
    let header = request.headers().clone();
    let registry_mirror_uri = match header::get_registry(&header) {
        Some(registry) => format!("{}{}", registry, request.uri().path())
            .parse::<http::Uri>()
            .or_err(ErrorType::ParseError)?,
        None => format!(
            "{}{}",
            config.proxy.registry_mirror.addr,
            request.uri().path()
        )
        .parse::<http::Uri>()
        .or_err(ErrorType::ParseError)?,
    };
    header::get_registry(&header);

    *request.uri_mut() = registry_mirror_uri.clone();
    request.headers_mut().insert(
        hyper::header::HOST,
        registry_mirror_uri
            .host()
            .ok_or_else(|| ClientError::Unknown("registry mirror host is not set".to_string()))?
            .parse()
            .or_err(ErrorType::ParseError)?,
    );

    Ok(request)
}

/// make_download_task_request makes a download task request by the request.
fn make_download_task_request(
    config: Arc<Config>,
    rule: &Rule,
    request: Request<hyper::body::Incoming>,
    remote_ip: std::net::IpAddr,
) -> ClientResult<DownloadTaskRequest> {
    // Convert the Reqwest header to the Hyper header.
    let mut header = request.headers().clone();

    // Registry will return the 403 status code if the Host header is set.
    header.remove(reqwest::header::HOST);

    // Validate the request arguments.
    let piece_length = header::get_piece_length(&header).map(|piece_length| piece_length.as_u64());
    if let Some(piece_length) = piece_length {
        if piece_length < MIN_PIECE_LENGTH {
            return Err(ClientError::ValidationError(format!(
                "piece length {} is less than the minimum piece length {}",
                piece_length, MIN_PIECE_LENGTH
            )));
        }
    }

    Ok(DownloadTaskRequest {
        download: Some(Download {
            url: make_download_url(request.uri(), rule.use_tls, rule.redirect.clone())?,
            digest: None,
            // Download range use header range in HTTP protocol.
            range: None,
            r#type: TaskType::Standard as i32,
            tag: header::get_tag(&header),
            application: header::get_application(&header),
            priority: header::get_priority(&header),
            filtered_query_params: header::get_filtered_query_params(
                &header,
                rule.filtered_query_params.clone(),
            ),
            request_header: headermap_to_hashmap(&header),
            piece_length,
            // Need the absolute path.
            output_path: header::get_output_path(&header),
            timeout: None,
            need_back_to_source: false,
            disable_back_to_source: config.proxy.disable_back_to_source,
            certificate_chain: Vec::new(),
            prefetch: need_prefetch(config.clone(), &header),
            object_storage: None,
            hdfs: None,
            is_prefetch: false,
            need_piece_content: false,
            force_hard_link: header::get_force_hard_link(&header),
            content_for_calculating_task_id: header::get_content_for_calculating_task_id(&header),
            remote_ip: Some(remote_ip.to_string()),
        }),
    })
}

/// need_prefetch returns whether the prefetch is needed by the configuration and the request
/// header.
fn need_prefetch(config: Arc<Config>, header: &http::HeaderMap) -> bool {
    // If the header not contains the range header, the request does not need prefetch.
    if !header.contains_key(reqwest::header::RANGE) {
        return false;
    }

    // If the header contains the X-Dragonfly-Prefetch header, return the value.
    // Because the X-Dragonfly-Prefetch header has the highest priority.
    if let Some(prefetch) = header::get_prefetch(header) {
        return prefetch;
    }

    // Return the prefetch value from the configuration.
    config.proxy.prefetch
}

/// make_download_url makes a download url by the given uri.
fn make_download_url(
    uri: &hyper::Uri,
    use_tls: bool,
    redirect: Option<String>,
) -> ClientResult<String> {
    let mut parts = http::uri::Parts::from(uri.clone());

    // Set the scheme to https if the rule uses tls.
    if use_tls {
        parts.scheme = Some(http::uri::Scheme::HTTPS);
    }

    // Set the authority to the redirect address.
    if let Some(redirect) = redirect {
        parts.authority =
            Some(http::uri::Authority::try_from(redirect).or_err(ErrorType::ParseError)?);
    }

    Ok(http::Uri::from_parts(parts)
        .or_err(ErrorType::ParseError)?
        .to_string())
}

/// make_response_headers makes the response headers.
fn make_response_headers(
    task_id: &str,
    mut download_task_started_response: DownloadTaskStartedResponse,
) -> ClientResult<hyper::header::HeaderMap> {
    // Insert the content range header to the response header.
    if let Some(range) = download_task_started_response.range.as_ref() {
        download_task_started_response.response_header.insert(
            reqwest::header::CONTENT_RANGE.to_string(),
            format!(
                "bytes {}-{}/{}",
                range.start,
                range.start + range.length - 1,
                download_task_started_response.content_length
            ),
        );

        download_task_started_response.response_header.insert(
            reqwest::header::CONTENT_LENGTH.to_string(),
            range.length.to_string(),
        );
    };

    if download_task_started_response.is_finished {
        download_task_started_response.response_header.insert(
            header::DRAGONFLY_TASK_DOWNLOAD_FINISHED_HEADER.to_string(),
            "true".to_string(),
        );
    }

    download_task_started_response.response_header.insert(
        header::DRAGONFLY_TASK_ID_HEADER.to_string(),
        task_id.to_string(),
    );

    hashmap_to_headermap(&download_task_started_response.response_header)
}

/// find_matching_rule returns whether the dfdaemon should be used to download the task.
/// If the dfdaemon should be used, return the matched rule.
fn find_matching_rule(rules: Option<&[Rule]>, url: &str) -> Option<Rule> {
    rules?.iter().find(|rule| rule.regex.is_match(url)).cloned()
}

/// make_error_response makes an error response with the given status and message.
fn make_error_response(status: http::StatusCode, header: Option<http::HeaderMap>) -> Response {
    let mut response = Response::new(empty());
    *response.status_mut() = status;
    if let Some(header) = header {
        for (k, v) in header.iter() {
            response.headers_mut().insert(k, v.clone());
        }
    }

    response
}

/// empty returns an empty body.
fn empty() -> BoxBody<Bytes, ClientError> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}
