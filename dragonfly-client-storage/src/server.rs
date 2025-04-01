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
use ibverbs::devices;
use std::sync::Arc;
use tracing::error;

#[tonic::async_trait]
pub trait Server: Send + Sync {
    async fn run(&self) -> Result<()>;

    async fn create_qp(&self) -> Result<()>;
}

pub struct ServerFactory {
    server: Arc<dyn Server + Send + Sync>,
}

/// DownloadFactory implements the DownloadFactory trait.
impl ServerFactory {
    /// new returns a new DownloadFactory.
    pub fn new(protocol: &str, config: Arc<Config>) -> Result<Self> {
        match protocol {
            "rdma" => {
                #[cfg(target_os = "linux")]
                {
                    Ok(Self {
                        server: Arc::new(RDMAServer::new(config)?),
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
    config: Arc<Config>,
    ctx: Arc<ibverbs::Context>,
}

#[cfg(target_os = "linux")]
impl RDMAServer {
    pub fn new(config: Arc<Config>) -> Result<Self> {
        let devices = devices()?;
        let ctx = match config.storage.server.device.clone() {
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
            config,
            ctx: Arc::new(ctx),
        })
    }
}

#[cfg(target_os = "linux")]
#[tonic::async_trait]
impl Server for RDMAServer {
    async fn run(&self) -> Result<()> {
        Ok(())
    }

    async fn create_qp(&self) -> Result<()> {
        let cp = self.ctx.create_cq(1024, 0)?;
        let pd = self.ctx.alloc_pd()?;
        Ok(())
    }
}
