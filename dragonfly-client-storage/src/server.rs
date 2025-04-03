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

use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
#[cfg(target_os = "linux")]
use ibverbs::{devices, CompletionQueue};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tracing::error;

const MAX_CQ_SIZE: u64 = 16 * 1024;

#[tonic::async_trait]
pub trait Server: Send + Sync {
    async fn run(self: Arc<Self>) -> Result<()>;
}

pub struct ServerFactory {
    server: Arc<dyn Server + Send + Sync>,
}

/// DownloadFactory implements the DownloadFactory trait.
impl ServerFactory {
    /// new returns a new DownloadFactory.
    pub fn new(protocol: &str, addr: SocketAddr, config: Arc<Config>) -> Result<Self> {
        match protocol {
            "rdma" => {
                #[cfg(target_os = "linux")]
                {
                    Ok(Self {
                        server: Arc::new(RDMAServer::new(
                            addr,
                            config.storage.server.device.clone(),
                        )?),
                    })
                }

                #[cfg(not(target_os = "linux"))]
                {
                    error!("RDMA is only supported on Linux");
                    Err(Error::InvalidParameter)
                }
            }
            _ => {
                error!("unsupported protocol: {}", protocol);
                Err(Error::InvalidParameter)
            }
        }
    }

    pub fn build(&self) -> Result<Arc<dyn Server>> {
        Ok(self.server.clone())
    }
}

#[cfg(target_os = "linux")]
pub struct RDMAServer {
    ctx: Arc<ibverbs::Context>,

    addr: SocketAddr,

    cp_id_counter: AtomicU64,
}

#[cfg(target_os = "linux")]
impl RDMAServer {
    pub fn new(addr: SocketAddr, device: Option<String>) -> Result<Self> {
        let devices = devices()?;
        let ctx = match device {
            Some(device_name) => devices
                .iter()
                .find(|d| d.name().unwrap().to_bytes() == device_name.as_bytes())
                .map(|d| d.open())
                .transpose()?
                .ok_or_else(|| Error::InvalidParameter)?,
            None => devices
                .iter()
                .next()
                .ok_or_else(|| Error::InvalidParameter)?
                .open()?,
        };

        Ok(Self {
            ctx: Arc::new(ctx),
            addr,
            cp_id_counter: AtomicU64::new(0),
        })
    }

    async fn get_cp_id(&self) -> Result<u64> {
        let cp_id = self.cp_id_counter.fetch_add(1, Ordering::SeqCst);
        if cp_id >= MAX_CQ_SIZE {
            self.cp_id_counter.store(0, Ordering::SeqCst);
            return Ok(0);
        }

        Ok(cp_id)
    }

    async fn handle_connection(&self, stream: TcpStream, ctx: Arc<ibverbs::Context>) -> Result<()> {
        Ok(())
    }
}

#[cfg(target_os = "linux")]
#[tonic::async_trait]
impl Server for RDMAServer {
    async fn run(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let ctx = self.ctx.clone();
            let self_clone = self.clone();
            tokio::spawn(async move {
                if let Err(err) = self_clone.handle_connection(stream, ctx).await {
                    error!("failed to handle connection: {}", err);
                }
            });
        }
    }
}
