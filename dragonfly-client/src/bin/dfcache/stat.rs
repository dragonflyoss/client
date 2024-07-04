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

use clap::Parser;
use dragonfly_client_core::Result;
use std::path::Path;

// StatCommand is the subcommand of stat.
#[derive(Debug, Clone, Parser)]
pub struct StatCommand {
    #[arg(help = "Specify the cache task ID to stat")]
    id: String,
}

// Implement the execute for StatCommand.
impl StatCommand {
    pub async fn execute(&self, _endpoint: &Path) -> Result<()> {
        println!("StatCommand is executed!");
        Ok(())
    }
}
