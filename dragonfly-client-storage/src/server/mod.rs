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
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::error;

pub mod rdma;

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
                        server: Arc::new(rdma::RDMAServer::new(
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
