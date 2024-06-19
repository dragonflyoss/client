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

use dragonfly_client_config::dfinit;
use dragonfly_client_core::{Error, Result};
use tracing::info;

// Docker represents the docker runtime manager.
#[derive(Debug, Clone)]
pub struct Docker {
    // config is the configuration for initializing
    // runtime environment for the dfdaemon.
    config: dfinit::Docker,

    // proxy_config is the configuration for the dfdaemon's proxy server.
    proxy_config: dfinit::Proxy,
}

// Docker implements the docker runtime manager.
impl Docker {
    // new creates a new docker runtime manager.
    pub fn new(config: dfinit::Docker, proxy_config: dfinit::Proxy) -> Self {
        Self {
            config,
            proxy_config,
        }
    }

    // TODO: Implement the run method for Docker.
    //
    // run runs the docker runtime to initialize
    // runtime environment for the dfdaemon.
    pub async fn run(&self) -> Result<()> {
        info!(
            "docker feature is enabled, proxy_addr: {}, config_path: {:?}",
            self.proxy_config.addr, self.config.config_path,
        );
        Err(Error::Unimplemented)
    }
}
