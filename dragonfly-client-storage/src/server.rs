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
use dragonfly_client_core::Result;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::{debug, error, instrument};

/// DownloaderFactory is the factory for creating different downloaders by different protocols.
pub struct ServerFactory {
    config: Arc<Config>,
}

/// DownloadFactory implements the DownloadFactory trait.
impl ServerFactory {
    /// new returns a new DownloadFactory.
    #[instrument(skip_all)]
    pub fn new(protocol: &str, config: Arc<Config>) -> Result<Self> {
        Ok(Self { config })
    }

    pub async fn run(&self) -> Result<()> {
        Ok(())
    }
}

pub struct RDMAServer {
    config: Arc<Config>,
}
