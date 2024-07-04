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

use crate::grpc::dfdaemon_download::DfdaemonDownloadClient;
use crate::metrics::{
    collect_proxy_request_failure_metrics, collect_proxy_request_started_metrics,
};
use crate::resource::task::Task;
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
    http::{
        hashmap_to_hyper_header_map, hyper_headermap_to_reqwest_headermap,
        reqwest_headermap_to_hashmap,
    },
    tls::{
        generate_ca_cert_from_pem, generate_certs_from_pem, generate_self_signed_certs_by_ca_cert,
        generate_simple_self_signed_certs, NoVerifier,
    },
};
use futures_util::TryStreamExt;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full, StreamBody};
use hyper::body::Frame;
use hyper::client::conn::http1::Builder;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::upgrade::Upgraded;
use hyper::{Method, Request};
use hyper_util::{
    client::legacy::Client,
    rt::{tokio::TokioIo, TokioExecutor},
};
use rcgen::Certificate;
use rustls::{RootCertStore, ServerConfig};
use rustls_pki_types::CertificateDer;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_rustls::TlsAcceptor;
use tokio_util::io::ReaderStream;
use tracing::{error, info, instrument, Span};

pub mod header;

// Response is the response of the proxy server.
pub type Response = hyper::Response<BoxBody<Bytes, ClientError>>;

// Proxy is the proxy server.
pub struct Proxy {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

    // task is the task manager.
    task: Arc<Task>,

    // addr is the address of the proxy server.
    addr: SocketAddr,

    // registry_certs is the certificate of the client for the registry.
    registry_certs: Arc<Option<Vec<CertificateDer<'static>>>>,

    // server_ca_cert is the CA certificate of the proxy server to
    // sign the self-signed certificate.
    server_ca_cert: Arc<Option<Certificate>>,

    // shutdown is used to shutdown the proxy server.
    shutdown: shutdown::Shutdown,

    // _shutdown_complete is used to notify the proxy server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,
}

// Proxy implements the proxy server.
impl Proxy {
    // new creates a new Proxy.
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
            registry_certs: Arc::new(None),
            server_ca_cert: Arc::new(None),
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        };

        // Load and generate the registry certificates from the PEM format file.
        proxy.registry_certs = match config.proxy.registry_mirror.certs.clone() {
            Some(certs_path) => match generate_certs_from_pem(&certs_path) {
                Ok(certs) => Arc::new(Some(certs)),
                Err(err) => {
                    error!("generate registry cert from pem failed: {}", err);
                    Arc::new(None)
                }
            },
            None => Arc::new(None),
        };

        // Load the CA certificate and key from the PEM format files.
        let Some(server_ca_cert_path) = config.proxy.server.ca_cert.clone() else {
            info!("ca_cert is not set, use self-signed certificate");
            return proxy;
        };

        // Load the CA certificate and key from the PEM format files.
        let Some(server_ca_key_path) = config.proxy.server.ca_key.clone() else {
            info!("ca_key is not set, use self-signed certificate");
            return proxy;
        };

        // Generate the CA certificate and key from the PEM format files.
        proxy.server_ca_cert =
            match generate_ca_cert_from_pem(&server_ca_cert_path, &server_ca_key_path) {
                Ok(server_ca_cert) => Arc::new(Some(server_ca_cert)),
                Err(err) => {
                    error!("generate ca cert and key from pem failed: {}", err);
                    Arc::new(None)
                }
            };

        proxy
    }

    // run starts the proxy server.
    #[instrument(skip_all)]
    pub async fn run(&self) -> ClientResult<()> {
        let listener = TcpListener::bind(self.addr).await?;
        info!("proxy server listening on {}", self.addr);

        loop {
            // Clone the shutdown channel.
            let mut shutdown = self.shutdown.clone();

            // Wait for a client connection.
            tokio::select! {
                tcp_accepted = listener.accept() => {
                    // A new client connection has been established.
                    let (tcp, remote_address) = tcp_accepted?;

                    // Spawn a task to handle the connection.
                    let io = TokioIo::new(tcp);
                    info!("accepted connection from {}", remote_address);

                    let config = self.config.clone();
                    let task = self.task.clone();
                    let registry_certs = self.registry_certs.clone();
                    let server_ca_cert = self.server_ca_cert.clone();
                    tokio::task::spawn(async move {
                        if let Err(err) = http1::Builder::new()
                            .keep_alive(true)
                            .preserve_header_case(true)
                            .title_case_headers(true)
                            .serve_connection(
                                io,
                                service_fn(move |request| handler(config.clone(), task.clone(), request, registry_certs.clone(), server_ca_cert.clone())),
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

// handler handles the request from the client.
#[instrument(skip_all, fields(uri, method))]
pub async fn handler(
    config: Arc<Config>,
    task: Arc<Task>,
    request: Request<hyper::body::Incoming>,
    registry_certs: Arc<Option<Vec<CertificateDer<'static>>>>,
    server_ca_cert: Arc<Option<Certificate>>,
) -> ClientResult<Response> {
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
                registry_certs,
                server_ca_cert,
            )
            .await;
        }

        return registry_mirror_http_handler(config, task, request, registry_certs).await;
    }

    // Span record the uri and method.
    Span::current().record("uri", request.uri().to_string().as_str());
    Span::current().record("method", request.method().as_str());

    // Handle CONNECT request.
    if Method::CONNECT == request.method() {
        return https_handler(config, task, request, registry_certs, server_ca_cert).await;
    }

    return http_handler(config, task, request, registry_certs).await;
}

// registry_mirror_http_handler handles the http request for the registry mirror by client.
#[instrument(skip_all)]
pub async fn registry_mirror_http_handler(
    config: Arc<Config>,
    task: Arc<Task>,
    request: Request<hyper::body::Incoming>,
    registry_certs: Arc<Option<Vec<CertificateDer<'static>>>>,
) -> ClientResult<Response> {
    let request = make_registry_mirror_request(config.clone(), request)?;
    return http_handler(config, task, request, registry_certs).await;
}

// registry_mirror_https_handler handles the https request for the registry mirror by client.
#[instrument(skip_all)]
pub async fn registry_mirror_https_handler(
    config: Arc<Config>,
    task: Arc<Task>,
    request: Request<hyper::body::Incoming>,
    registry_certs: Arc<Option<Vec<CertificateDer<'static>>>>,
    server_ca_cert: Arc<Option<Certificate>>,
) -> ClientResult<Response> {
    let request = make_registry_mirror_request(config.clone(), request)?;
    return https_handler(config, task, request, registry_certs, server_ca_cert).await;
}

// http_handler handles the http request by client.
#[instrument(skip_all)]
pub async fn http_handler(
    config: Arc<Config>,
    task: Arc<Task>,
    request: Request<hyper::body::Incoming>,
    registry_certs: Arc<Option<Vec<CertificateDer<'static>>>>,
) -> ClientResult<Response> {
    info!("handle HTTP request: {:?}", request);

    // If find the matching rule, proxy the request via the dfdaemon.
    let request_uri = request.uri();
    if let Some(rule) =
        find_matching_rule(config.proxy.rules.clone(), request_uri.to_string().as_str())
    {
        info!(
            "proxy HTTP request via dfdaemon for method: {}, uri: {}",
            request.method(),
            request_uri
        );
        return proxy_by_dfdaemon(config, task, rule.clone(), request).await;
    }

    if request.uri().scheme().cloned() == Some(http::uri::Scheme::HTTPS) {
        info!(
            "proxy HTTPS request directly to remote server for method: {}, uri: {}",
            request.method(),
            request.uri()
        );
        return proxy_https(request, registry_certs).await;
    }

    info!(
        "proxy HTTP request directly to remote server for method: {}, uri: {}",
        request.method(),
        request.uri()
    );
    return proxy_http(request).await;
}

// https_handler handles the https request by client.
#[instrument(skip_all)]
pub async fn https_handler(
    config: Arc<Config>,
    task: Arc<Task>,
    request: Request<hyper::body::Incoming>,
    registry_certs: Arc<Option<Vec<CertificateDer<'static>>>>,
    server_ca_cert: Arc<Option<Certificate>>,
) -> ClientResult<Response> {
    info!("handle HTTPS request: {:?}", request);

    // Proxy the request directly  to the remote server.
    if let Some(host) = request.uri().host() {
        let host = host.to_string();
        tokio::task::spawn(async move {
            match hyper::upgrade::on(request).await {
                Ok(upgraded) => {
                    if let Err(e) = upgraded_tunnel(
                        config,
                        task,
                        upgraded,
                        host,
                        registry_certs,
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
        return Ok(make_error_response(
            http::StatusCode::BAD_REQUEST,
            "CONNECT must be to a socket address",
            None,
        ));
    }
}

// upgraded_tunnel handles the upgraded connection. If the ca_cert is not set, use the
// self-signed certificate. Otherwise, use the CA certificate to sign the
// self-signed certificate.
#[instrument(skip_all)]
async fn upgraded_tunnel(
    config: Arc<Config>,
    task: Arc<Task>,
    upgraded: Upgraded,
    host: String,
    registry_certs: Arc<Option<Vec<CertificateDer<'static>>>>,
    server_ca_cert: Arc<Option<Certificate>>,
) -> ClientResult<()> {
    // Initialize the tcp stream to the remote server.
    let upgraded = TokioIo::new(upgraded);

    // Generate the self-signed certificate by the given host. If the ca_cert
    // is not set, use the self-signed certificate. Otherwise, use the CA
    // certificate to sign the self-signed certificate.
    let subject_alt_names = vec![host.to_string()];
    let (server_certs, server_key) = match server_ca_cert.as_ref() {
        Some(server_ca_cert) => {
            info!("generate self-signed certificate by CA certificate");
            generate_self_signed_certs_by_ca_cert(server_ca_cert, subject_alt_names)?
        }
        None => {
            info!("generate simple self-signed certificate");
            generate_simple_self_signed_certs(subject_alt_names)?
        }
    };

    // Build TLS configuration.
    let mut server_config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(server_certs, server_key)
        .or_err(ErrorType::TLSConfigError)?;
    server_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec(), b"http/1.0".to_vec()];
    let tls_acceptor = TlsAcceptor::from(Arc::new(server_config));
    let tls_stream = tls_acceptor.accept(upgraded).await?;

    // Serve the connection with the TLS stream.
    if let Err(err) = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
        .serve_connection(
            TokioIo::new(tls_stream),
            service_fn(move |request| {
                upgraded_handler(
                    config.clone(),
                    task.clone(),
                    host.clone(),
                    request,
                    registry_certs.clone(),
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

// upgraded_handler handles the upgraded https request from the client.
#[instrument(skip_all, fields(uri, method))]
pub async fn upgraded_handler(
    config: Arc<Config>,
    task: Arc<Task>,
    host: String,
    mut request: Request<hyper::body::Incoming>,
    registry_certs: Arc<Option<Vec<CertificateDer<'static>>>>,
) -> ClientResult<Response> {
    // Span record the uri and method.
    Span::current().record("uri", request.uri().to_string().as_str());
    Span::current().record("method", request.method().as_str());

    *request.uri_mut() = format!("https://{}{}", host, request.uri())
        .parse()
        .or_err(ErrorType::ParseError)?;

    // If find the matching rule, proxy the request via the dfdaemon.
    let request_uri = request.uri();
    if let Some(rule) =
        find_matching_rule(config.proxy.rules.clone(), request_uri.to_string().as_str())
    {
        info!(
            "proxy HTTPS request via dfdaemon for method: {}, uri: {}",
            request.method(),
            request_uri
        );
        return proxy_by_dfdaemon(config, task, rule.clone(), request).await;
    }

    if request.uri().scheme().cloned() == Some(http::uri::Scheme::HTTPS) {
        info!(
            "proxy HTTPS request directly to remote server for method: {}, uri: {}",
            request.method(),
            request.uri()
        );
        return proxy_https(request, registry_certs).await;
    }

    info!(
        "proxy HTTP request directly to remote server for method: {}, uri: {}",
        request.method(),
        request.uri()
    );
    return proxy_http(request).await;
}

// proxy_by_dfdaemon proxies the request via the dfdaemon.
#[instrument(skip_all)]
async fn proxy_by_dfdaemon(
    config: Arc<Config>,
    task: Arc<Task>,
    rule: Rule,
    request: Request<hyper::body::Incoming>,
) -> ClientResult<Response> {
    // Initialize the dfdaemon download client.
    let dfdaemon_download_client =
        match DfdaemonDownloadClient::new_unix(config.download.server.socket_path.clone()).await {
            Ok(client) => client,
            Err(err) => {
                error!("create dfdaemon download client failed: {}", err);
                return Ok(make_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    err.to_string().as_str(),
                    None,
                ));
            }
        };

    // Make the download task request.
    let download_task_request = match make_download_task_request(config.clone(), rule, request) {
        Ok(download_task_request) => download_task_request,
        Err(err) => {
            error!("make download task request failed: {}", err);
            return Ok(make_error_response(
                http::StatusCode::INTERNAL_SERVER_ERROR,
                err.to_string().as_str(),
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
                            http::StatusCode::from_u16(backend.status_code as u16)
                                .unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR),
                            "download task failed",
                            Some(hashmap_to_hyper_header_map(&backend.header)?),
                        ));
                    }
                    Err(err) => {
                        error!("download task failed by tonic status: {}", err.to_string());
                        return Ok(make_error_response(
                            http::StatusCode::INTERNAL_SERVER_ERROR,
                            err.to_string().as_str(),
                            None,
                        ));
                    }
                };
            }
            _ => {
                error!("download task failed: {}", err);
                return Ok(make_error_response(
                    http::StatusCode::INTERNAL_SERVER_ERROR,
                    err.to_string().as_str(),
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
            "response message failed",
            None,
        ));
    };

    // Handle the download task started response.
    let Some(download_task_response::Response::DownloadTaskStartedResponse(
        download_task_started_response,
    )) = message.response
    else {
        error!("response is not started");
        return Ok(make_error_response(
            http::StatusCode::INTERNAL_SERVER_ERROR,
            "response is not started",
            None,
        ));
    };

    // Write the task data to the reader.
    let (reader, mut writer) = tokio::io::duplex(1024);

    // Construct the response body.
    let reader_stream = ReaderStream::new(reader);
    let stream_body = StreamBody::new(reader_stream.map_ok(Frame::data).map_err(ClientError::from));
    let boxed_body = stream_body.boxed();

    // Construct the response.
    let mut response = Response::new(boxed_body);
    *response.headers_mut() = make_response_headers(download_task_started_response.clone())?;
    *response.status_mut() = http::StatusCode::OK;

    // Get the read buffer size from the config.
    let read_buffer_size = config.proxy.read_buffer_size;

    // Write task data to pipe. If grpc received error message,
    // shutdown the writer.
    tokio::spawn(async move {
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
        while let Ok(Some(message)) = out_stream.message().await {
            if let Some(download_task_response::Response::DownloadPieceFinishedResponse(response)) =
                message.response
            {
                let Some(piece) = response.piece else {
                    error!("response piece is empty");
                    if let Err(err) = writer.shutdown().await {
                        error!("writer shutdown error: {}", err);
                    }

                    return;
                };

                let piece_reader = match task
                    .piece
                    .download_from_local_peer_into_async_read(
                        message.task_id.as_str(),
                        piece.number,
                        piece.length,
                        download_task_started_response.range.clone(),
                        true,
                    )
                    .await
                {
                    Ok(piece_reader) => piece_reader,
                    Err(err) => {
                        error!("download piece reader error: {}", err);
                        if let Err(err) = writer.shutdown().await {
                            error!("writer shutdown error: {}", err);
                        }

                        return;
                    }
                };

                // Use a buffer to read the piece.
                let piece_reader = BufReader::with_capacity(read_buffer_size, piece_reader);

                // Write the piece data to the pipe in order.
                finished_piece_readers.insert(piece.number, piece_reader);
                while let Some(piece_reader) = finished_piece_readers.get_mut(&need_piece_number) {
                    info!("copy piece {} to stream", need_piece_number);
                    if let Err(err) = tokio::io::copy(piece_reader, &mut writer).await {
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
                if let Err(err) = writer.shutdown().await {
                    error!("writer shutdown error: {}", err);
                }

                return;
            }
        }

        info!("copy finished");
        if let Err(err) = writer.flush().await {
            error!("writer flush error: {}", err);
        }
    });

    Ok(response)
}

// proxy_http proxies the HTTP request directly to the remote server.
#[instrument(skip_all)]
async fn proxy_http(request: Request<hyper::body::Incoming>) -> ClientResult<Response> {
    let Some(host) = request.uri().host() else {
        error!("CONNECT host is not socket addr: {:?}", request.uri());
        return Ok(make_error_response(
            http::StatusCode::BAD_REQUEST,
            "CONNECT must be to a socket address",
            None,
        ));
    };
    let port = request.uri().port_u16().unwrap_or(80);

    let stream = TcpStream::connect((host, port)).await?;
    let io = TokioIo::new(stream);
    let (mut client, conn) = Builder::new()
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

// proxy_https proxies the HTTPS request directly to the remote server.
#[instrument(skip_all)]
async fn proxy_https(
    request: Request<hyper::body::Incoming>,
    registry_certs: Arc<Option<Vec<CertificateDer<'static>>>>,
) -> ClientResult<Response> {
    let client_config_builder = match registry_certs.as_ref() {
        Some(registry_certs) => {
            let mut root_cert_store = RootCertStore::empty();
            root_cert_store.add_parsable_certificates(registry_certs.to_owned());

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
        .enable_http2()
        .build();

    let client = Client::builder(TokioExecutor::new()).build::<_, hyper::body::Incoming>(https);
    let response = client.request(request).await?;
    Ok(response.map(|b| b.map_err(ClientError::from).boxed()))
}

// make_registry_mirror_request makes a registry mirror request by the request.
#[instrument(skip_all)]
fn make_registry_mirror_request(
    config: Arc<Config>,
    mut request: Request<hyper::body::Incoming>,
) -> ClientResult<Request<hyper::body::Incoming>> {
    // Convert the Reqwest header to the Hyper header.
    let reqwest_request_header = hyper_headermap_to_reqwest_headermap(request.headers());
    let registry_mirror_uri = match header::get_registry(&reqwest_request_header) {
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

    header::get_registry(&reqwest_request_header);

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

// make_download_task_request makes a download task request by the request.
#[instrument(skip_all)]
fn make_download_task_request(
    config: Arc<Config>,
    rule: Rule,
    request: Request<hyper::body::Incoming>,
) -> ClientResult<DownloadTaskRequest> {
    // Convert the Reqwest header to the Hyper header.
    let mut reqwest_request_header = hyper_headermap_to_reqwest_headermap(request.headers());

    // Registry will return the 403 status code if the Host header is set.
    reqwest_request_header.remove(reqwest::header::HOST);

    Ok(DownloadTaskRequest {
        download: Some(Download {
            url: make_download_url(request.uri(), rule.use_tls, rule.redirect.clone())?,
            digest: None,
            // Download range use header range in HTTP protocol.
            range: None,
            r#type: TaskType::Dfdaemon as i32,
            tag: header::get_tag(&reqwest_request_header),
            application: header::get_application(&reqwest_request_header),
            priority: header::get_priority(&reqwest_request_header),
            filtered_query_params: header::get_filtered_query_params(
                &reqwest_request_header,
                rule.filtered_query_params.clone(),
            ),
            request_header: reqwest_headermap_to_hashmap(&reqwest_request_header),
            piece_length: header::get_piece_length(&reqwest_request_header),
            output_path: None,
            timeout: None,
            need_back_to_source: false,
            disable_back_to_source: config.proxy.disable_back_to_source,
            certificate_chain: Vec::new(),
            // Prefetch is set to true if the request header contains the range header and the
            // prefetch is enabled in the configuration.
            prefetch: config.proxy.prefetch
                && reqwest_request_header.contains_key(reqwest::header::RANGE),
        }),
    })
}

// make_download_url makes a download url by the given uri.
#[instrument(skip_all)]
fn make_download_url(
    uri: &hyper::Uri,
    use_tls: bool,
    redirect: Option<String>,
) -> ClientResult<String> {
    let mut parts = uri.clone().into_parts();

    // Set the scheme to https if the rule uses tls.
    if use_tls {
        parts.scheme = Some(http::uri::Scheme::HTTPS);
    }

    // Set the authority to the redirect address.
    if let Some(redirect) = redirect {
        parts.authority = Some(http::uri::Authority::from_static(Box::leak(
            redirect.into_boxed_str(),
        )));
    }

    Ok(http::Uri::from_parts(parts)
        .or_err(ErrorType::ParseError)?
        .to_string())
}

// make_response_headers makes the response headers.
#[instrument(skip_all)]
fn make_response_headers(
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

    hashmap_to_hyper_header_map(&download_task_started_response.response_header)
}

// find_matching_rule returns whether the dfdaemon should be used to download the task.
// If the dfdaemon should be used, return the matched rule.
#[instrument(skip_all)]
fn find_matching_rule(rules: Option<Vec<Rule>>, url: &str) -> Option<Rule> {
    rules?.iter().find(|rule| rule.regex.is_match(url)).cloned()
}

// make_error_response makes an error response with the given status and message.
#[instrument(skip_all)]
fn make_error_response(
    status: http::StatusCode,
    message: &str,
    header: Option<http::HeaderMap>,
) -> Response {
    let mut response = Response::new(full(message.as_bytes().to_vec()));
    *response.status_mut() = status;
    if let Some(header) = header {
        for (k, v) in header.iter() {
            response.headers_mut().insert(k, v.clone());
        }
    }

    response
}

// empty returns an empty body.
#[instrument(skip_all)]
fn empty() -> BoxBody<Bytes, ClientError> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

// full returns a body with the given chunk.
#[instrument(skip_all)]
fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, ClientError> {
    Full::new(chunk.into())
        .map_err(|never| match never {})
        .boxed()
}
