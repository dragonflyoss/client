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
use dragonfly_client_config::default_piece_length;
use dragonfly_client_core::Result;
use std::path::{Path, PathBuf};
use std::time::Duration;

// ExportCommand is the subcommand of export.
#[derive(Debug, Clone, Parser)]
pub struct ExportCommand {
    #[arg(help = "Specify the cache task ID to export")]
    id: String,

    #[arg(
        long = "application",
        default_value = "",
        help = "Caller application which is used for statistics and access control"
    )]
    application: String,

    #[arg(
        long = "tag",
        default_value = "",
        help = "Different tags for the same file will be divided into different cache tasks"
    )]
    tag: String,

    #[arg(
        long = "piece-length",
        default_value_t = default_piece_length(),
        help = "Specify the byte length of the piece"
    )]
    piece_length: u64,

    #[arg(
        short = 'O',
        long = "output",
        help = "Specify the output path of exporting file"
    )]
    output: PathBuf,

    #[arg(
        long = "timeout",
        value_parser= humantime::parse_duration,
        default_value = "2h",
        help = "Specify the timeout for exporting a file"
    )]
    timeout: Duration,
}

// Implement the execute for ExportCommand.
impl ExportCommand {
    pub async fn execute(&self, _endpoint: &Path) -> Result<()> {
        println!("ExportCommand is executed!");
        Ok(())
    }
}
