/*
 *     Copyright 2025 The Dragonfly Authors
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

use crate::client::Client;
use bytes::Bytes;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Result as ClientResult;
use socket2::{SockRef, TcpKeepalive};
use std::sync::Arc;
use tokio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use tracing::{error, instrument};

/// TCPClient is a TCP-based client for tcp storage service.
#[derive(Clone)]
pub struct TCPClient {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// addr is the address of the TCP server.
    addr: String,
}

/// TCPClient implements the TCP-based client for tcp storage service.
impl TCPClient {
    /// Creates a new TCPClient instance.
    pub fn new(config: Arc<Config>, addr: String) -> Self {
        Self { config, addr }
    }
}

#[tonic::async_trait]
impl Client for TCPClient {
    /// Establishes TCP connection and writes a vortex protocol request.
    ///
    /// This is a low-level utility function that handles the TCP connection
    /// lifecycle and request transmission. It ensures proper error handling
    /// and connection cleanup.
    #[instrument(skip_all)]
    async fn connect_and_write_request(
        &self,
        request: Bytes,
    ) -> ClientResult<(
        Box<dyn AsyncRead + Send + Unpin>,
        Box<dyn AsyncWrite + Send + Unpin>,
    )> {
        let stream = tokio::net::TcpStream::connect(self.addr.clone()).await?;
        let socket = SockRef::from(&stream);
        socket.set_tcp_nodelay(true)?;
        socket.set_nonblocking(true)?;
        socket.set_send_buffer_size(super::DEFAULT_SEND_BUFFER_SIZE)?;
        socket.set_recv_buffer_size(super::DEFAULT_RECV_BUFFER_SIZE)?;
        socket.set_tcp_keepalive(
            &TcpKeepalive::new().with_interval(super::DEFAULT_KEEPALIVE_INTERVAL),
        )?;

        let (reader, mut writer) = stream.into_split();
        writer.write_all(&request).await.inspect_err(|err| {
            error!("failed to send request: {}", err);
        })?;

        writer.flush().await.inspect_err(|err| {
            error!("failed to flush request: {}", err);
        })?;

        Ok((Box::new(reader), Box::new(writer)))
    }

    /// Access to client configuration.
    fn config(&self) -> &Arc<Config> {
        &self.config
    }

    /// Access to client address.
    fn addr(&self) -> &str {
        &self.addr
    }
}
