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
use crate::shutdown;
use crate::Result as ClientResult;
use bytes::Bytes;
use http_body_util::{combinators::BoxBody, BodyExt, Empty, Full};
use hyper::client::conn::http1::Builder;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::upgrade::Upgraded;
use hyper::{Method, Request, Response};
use hyper_util::rt::tokio::TokioIo;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tracing::{error, info, instrument, Span};

// Proxy is the proxy server.
pub struct Proxy {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

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
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            config: config.clone(),
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

                    // Clone the config.
                    let config = self.config.clone();

                    tokio::task::spawn(async move {
                        if let Err(err) = http1::Builder::new()
                            .preserve_header_case(true)
                            .title_case_headers(true)
                            .serve_connection(
                                io,
                                service_fn(move |request| handler(config.clone(), request)),
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
    request: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
    info!("handle request: {:?}", request);

    // Span record the uri and method.
    Span::current().record("uri", request.uri().to_string().as_str());
    Span::current().record("method", request.method().as_str());

    // Handle CONNECT request.
    if Method::CONNECT == request.method() {
        return https_handler(config, request).await;
    }

    return http_handler(config, request).await;
}

// http_handler handles the http request.
#[instrument(skip_all)]
pub async fn http_handler(
    config: Arc<Config>,
    request: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
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
                // TODO: handle https request.
                let mut response = Response::new(full("CONNECT must be to a socket address"));
                *response.status_mut() = http::StatusCode::BAD_REQUEST;
                return Ok(response);
            }
        }
    }

    // Proxy the request to the remote server directly.
    info!("proxy http request to remote server directly");
    let stream = TcpStream::connect((host, port)).await.unwrap();
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
    Ok(response.map(|b| b.boxed()))
}

// https_handler handles the https request.
#[instrument(skip_all)]
pub async fn https_handler(
    config: Arc<Config>,
    request: Request<hyper::body::Incoming>,
) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
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

// empty returns an empty body.
#[instrument(skip_all)]
fn empty() -> BoxBody<Bytes, hyper::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

// full returns a body with the given chunk.
#[instrument(skip_all)]
fn full<T: Into<Bytes>>(chunk: T) -> BoxBody<Bytes, hyper::Error> {
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
