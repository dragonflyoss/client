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
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, error, instrument};

#[tonic::async_trait]
pub trait Server: Send + Sync {
    async fn run(&self) -> Result<()>;
}

pub struct ServerFactory {
    server: Arc<dyn Server + Send + Sync>,
}

/// DownloadFactory implements the DownloadFactory trait.
impl ServerFactory {
    /// new returns a new DownloadFactory.
    pub fn new(protocol: &str, config: Arc<Config>) -> Result<Self> {
        match protocol {
            "rdma" => Ok(Self {
                server: Arc::new(RDMAServer::new(config)),
            }),
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

pub struct RDMAServer {
    config: Arc<Config>,
}

impl RDMAServer {
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }
}

#[tonic::async_trait]
impl Server for RDMAServer {
    async fn run(&self) -> Result<()> {
        Ok(())
    }
}
