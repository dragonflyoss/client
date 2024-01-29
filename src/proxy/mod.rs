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

use crate::config::dfdaemon::Config;
use crate::grpc::dfdaemon_download::DfdaemonDownloadClient;
use crate::shutdown;
use crate::task::Task;
use crate::utils::http::{
    hashmap_to_hyper_header_map, reqwest_headermap_to_hashmap, hyper_headermap_to_reqwest_headermap,
};
use crate::{Error as ClientError, Result as ClientResult};
use bytes::Bytes;
use dragonfly_api::common::v2::{Download, TaskType};
use dragonfly_api::dfdaemon::v2::{download_task_response, DownloadTaskRequest};
use futures_util::TryStreamExt;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full, StreamBody};
use hyper::body::Frame;
use hyper::client::conn::http1::Builder;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::upgrade::Upgraded;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::tokio::TokioIo;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::io::ReaderStream;
use tracing::{error, info, instrument, Span};

pub mod header;

pub type Response = hyper::Response<BoxBody<Bytes, ClientError>>;

// Proxy is the proxy server.
pub struct Proxy {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

    // task is the task manager.
    task: Arc<Task>,

    // addr is the address of the proxy server.
    addr: SocketAddr,

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
        Self {
            config: config.clone(),
            task: task.clone(),
            addr: SocketAddr::new(config.proxy.server.ip.unwrap(), config.proxy.server.port),
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
        }
    }

    // run starts the proxy server.
    #[instrument(skip_all)]
    pub async fn run(&self) -> ClientResult<()> {
        let listener = TcpListener::bind(self.addr).await?;

        // Start the proxy server and wait for it to finish.
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
                    tokio::task::spawn(async move {
                        if let Err(err) = http1::Builder::new()
                            .preserve_header_case(true)
                            .title_case_headers(true)
                            .serve_connection(
                                io,
                                service_fn(move |request| handler(config.clone(), task.clone(), request)),
                                )
                            .with_upgrades()
                            .await
                        {
                            error!("failed to serve connection: {}", err);
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

// handle starts to handle the request.
#[instrument(skip_all, fields(uri, method))]
pub async fn handler(
    config: Arc<Config>,
    task: Arc<Task>,
    request: Request<hyper::body::Incoming>,
) -> ClientResult<Response> {
    info!("handle request: {:?}", request);

    // Span record the uri and method.
    Span::current().record("uri", request.uri().to_string().as_str());
    Span::current().record("method", request.method().as_str());

    // Handle CONNECT request.
    if Method::CONNECT == request.method() {
        return https_handler(config, request).await;
    }

    return http_handler(config, task, request).await;
}

// http_handler handles the http request.
#[instrument(skip_all)]
pub async fn http_handler(
    config: Arc<Config>,
    task: Arc<Task>,
    request: Request<hyper::body::Incoming>,
) -> ClientResult<Response> {
    let Some(host) = request.uri().host() else {
        error!("CONNECT host is not socket addr: {:?}", request.uri());
        let mut response = Response::new(full("CONNECT must be to a socket address"));
        *response.status_mut() = http::StatusCode::BAD_REQUEST;
        return Ok(response);
    };
    let port = request.uri().port_u16().unwrap_or(80);

    if let Some(rules) = config.proxy.rules.clone() {
        for rule in rules.iter() {
            if rule.regex.is_match(request.uri().to_string().as_str()) {
                // Convert the Reqwest header to the Hyper header.
                let request_header = hyper_headermap_to_reqwest_headermap(request.headers());

                // Construct the download url.
                let url =
                    match make_download_url(request.uri(), rule.use_tls, rule.redirect.clone()) {
                        Ok(url) => url,
                        Err(err) => {
                            let mut response = Response::new(full(
                                err.to_string().to_string().as_bytes().to_vec(),
                            ));
                            *response.status_mut() = http::StatusCode::BAD_REQUEST;
                            return Ok(response);
                        }
                    };

                // Get parameters from the header.
                let tag = header::get_tag(&request_header);
                let application = header::get_application(&request_header);
                let priority = header::get_priority(&request_header);
                let piece_length = header::get_piece_length(&request_header);
                let filtered_query_params = header::get_filtered_query_params(
                    &request_header,
                    rule.filtered_query_params.clone(),
                );
                let request_header = reqwest_headermap_to_hashmap(&request_header);

                // Initialize the dfdaemon download client.
                let dfdaemon_download_client = match DfdaemonDownloadClient::new_unix(
                    config.download.server.socket_path.clone(),
                )
                .await
                {
                    Ok(client) => client,
                    Err(err) => {
                        let mut response =
                            Response::new(full(err.to_string().to_string().as_bytes().to_vec()));
                        *response.status_mut() = http::StatusCode::BAD_REQUEST;
                        return Ok(response);
                    }
                };

                // Download the task by the dfdaemon download client.
                let response = match dfdaemon_download_client
                    .download_task(DownloadTaskRequest {
                        download: Some(Download {
                            url,
                            digest: None,
                            // Download range use header range in HTTP protocol.
                            range: None,
                            r#type: TaskType::Dfdaemon as i32,
                            tag,
                            application,
                            priority,
                            filtered_query_params,
                            request_header,
                            piece_length,
                            output_path: None,
                            timeout: None,
                            need_back_to_source: false,
                        }),
                    })
                    .await
                {
                    Ok(response) => response,
                    Err(err) => {
                        let mut response =
                            Response::new(full(err.to_string().to_string().as_bytes().to_vec()));
                        *response.status_mut() = http::StatusCode::BAD_REQUEST;
                        return Ok(response);
                    }
                };

                // Construct the response header.
                let mut response_header = HashMap::new();

                // Write the task data to the reader.
                let (reader, mut writer) = tokio::io::duplex(1024);

                // Handle the response from the download grpc server.
                let mut out_stream = response.into_inner();
                while let Some(message) = match out_stream.message().await {
                    Ok(message) => message,
                    Err(err) => {
                        let mut response =
                            Response::new(full(err.to_string().to_string().as_bytes().to_vec()));
                        *response.status_mut() = http::StatusCode::BAD_REQUEST;
                        return Ok(response);
                    }
                } {
                    match message.response {
                        Some(download_task_response::Response::DownloadTaskStartedResponse(
                            response,
                        )) => {
                            response_header = response.response_header;
                            break;
                        }
                        Some(download_task_response::Response::DownloadTaskFinishedResponse(_)) => {
                            info!("download task finished");
                        }
                        Some(download_task_response::Response::DownloadPieceFinishedResponse(
                            response,
                        )) => {
                            let piece = match response.piece {
                                Some(piece) => piece,
                                None => {
                                    let mut response = Response::new(full(
                                        "download task response piece is empty",
                                    ));
                                    *response.status_mut() = http::StatusCode::BAD_REQUEST;
                                    return Ok(response);
                                }
                            };

                            let mut need_piece_number = 0;
                            let piece_reader = match task
                                .piece
                                .download_from_local_peer_into_async_read(
                                    message.task_id.as_str(),
                                    piece.number,
                                    piece.length,
                                    true,
                                )
                                .await
                            {
                                Ok(reader) => reader,
                                Err(err) => {
                                    let mut response = Response::new(full(
                                        err.to_string().to_string().as_bytes().to_vec(),
                                    ));
                                    *response.status_mut() = http::StatusCode::BAD_REQUEST;
                                    return Ok(response);
                                }
                            };

                            // Sort by piece number and return to reader in order.
                            let mut finished_piece_readers = HashMap::new();
                            finished_piece_readers.insert(piece.number, piece_reader);
                            while let Some(piece_reader) =
                                finished_piece_readers.get_mut(&need_piece_number)
                            {
                                if let Err(err) = tokio::io::copy(piece_reader, &mut writer).await {
                                    let mut response = Response::new(full(
                                        err.to_string().to_string().as_bytes().to_vec(),
                                    ));
                                    *response.status_mut() =
                                        http::StatusCode::INTERNAL_SERVER_ERROR;
                                    return Ok(response);
                                }
                                need_piece_number += 1;
                            }
                        }
                        None => {
                            let mut response = Response::new(full(
                                "download task response is empty".as_bytes().to_vec(),
                            ));
                            *response.status_mut() = http::StatusCode::INTERNAL_SERVER_ERROR;
                            return Ok(response);
                        }
                    }
                }

                // Construct the response body.
                let reader_stream = ReaderStream::new(reader);
                let stream_body =
                    StreamBody::new(reader_stream.map_ok(Frame::data).map_err(ClientError::from));
                let boxed_body = stream_body.boxed();

                // Construct the response.
                let mut response = Response::new(boxed_body);
                *response.headers_mut() = hashmap_to_hyper_header_map(&response_header)?;
                *response.status_mut() = http::StatusCode::OK;

                // Send response
                return Ok(response);
            }
        }
    }

    // Proxy the request to the remote server directly.
    info!("proxy http request to remote server directly");
    let stream = TcpStream::connect((host, port)).await?;
    let io = TokioIo::new(stream);
    let (mut sender, conn) = Builder::new()
        .preserve_header_case(true)
        .title_case_headers(true)
        .handshake(io)
        .await?;

    tokio::task::spawn(async move {
        if let Err(err) = conn.await {
            error!("connection failed: {:?}", err);
        }
    });

    let response = sender.send_request(request).await?;
    Ok(response.map(|b| b.map_err(ClientError::from).boxed()))
}

// https_handler handles the https request.
#[instrument(skip_all)]
pub async fn https_handler(
    config: Arc<Config>,
    request: Request<hyper::body::Incoming>,
) -> ClientResult<Response> {
    if let Some(rules) = config.proxy.rules.clone() {
        for rule in rules.iter() {
            if rule.regex.is_match(request.uri().to_string().as_str()) {
                // TODO: handle https request.
                let mut response = Response::new(full("CONNECT must be to a socket address"));
                *response.status_mut() = http::StatusCode::BAD_REQUEST;
                return Ok(response);
            }
        }
    }

    // Proxy the request to the remote server directly.
    info!("proxy https request to remote server directly");
    if let Some(addr) = host_addr(request.uri()) {
        tokio::task::spawn(async move {
            match hyper::upgrade::on(request).await {
                Ok(upgraded) => {
                    if let Err(e) = tunnel(upgraded, addr).await {
                        error!("server io error: {}", e);
                    };
                }
                Err(e) => error!("upgrade error: {}", e),
            }
        });

        Ok(Response::new(empty()))
    } else {
        error!("CONNECT host is not socket addr: {:?}", request.uri());
        let mut response = Response::new(full("CONNECT must be to a socket address"));
        *response.status_mut() = http::StatusCode::BAD_REQUEST;

        Ok(response)
    }
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

    Ok(http::Uri::from_parts(parts)?.to_string())
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

// host_addr returns the host address of the uri.
#[instrument(skip_all)]
fn host_addr(uri: &hyper::Uri) -> Option<String> {
    uri.authority().map(|auth| auth.to_string())
}

// tunnel proxies the data between the client and the remote server.
#[instrument(skip_all)]
async fn tunnel(upgraded: Upgraded, addr: String) -> std::io::Result<()> {
    // Connect to remote server.
    let mut server = TcpStream::connect(addr).await?;
    let mut upgraded = TokioIo::new(upgraded);

    // Proxying data.
    let (from_client, from_server) =
        tokio::io::copy_bidirectional(&mut upgraded, &mut server).await?;

    // Print message when done.
    info!(
        "client wrote {} bytes and received {} bytes",
        from_client, from_server
    );

    Ok(())
}
