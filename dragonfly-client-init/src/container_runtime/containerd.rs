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
use dragonfly_client_core::Result;
use tokio::fs;
use toml::Table;
use tracing::info;

// Containerd represents the containerd runtime manager.
pub struct Containerd {
    // config is the configuration for initializing
    // runtime environment for the dfdaemon.
    config: dfinit::Containerd,
}

// Containerd implements the containerd runtime manager.
impl Containerd {
    // new creates a new containerd runtime manager.
    pub fn new(config: dfinit::Containerd) -> Self {
        Self { config }
    }

    // run runs the containerd runtime to initialize
    // runtime environment for the dfdaemon.
    pub async fn run(&self) -> Result<()> {
        let content = fs::read_to_string(&self.config.config_path).await?;
        let containerd_config: Table = content.parse()?;
        info!("containerd config: {:?}", containerd_config);

        Ok(())
    }

    // is_enabled returns true if containerd feature is enabled.
    pub fn is_enabled(&self) -> bool {
        self.config.enable
    }
}
