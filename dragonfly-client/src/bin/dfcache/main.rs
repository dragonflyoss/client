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

use clap::{Parser, Subcommand};
use dragonfly_client::tracing::init_tracing;
use dragonfly_client_config::{dfcache, dfdaemon};
use dragonfly_client_core::Result;
use std::path::{Path, PathBuf};
use tracing::Level;

pub mod export;
pub mod import;
pub mod remove;
pub mod stat;

#[derive(Debug, Parser)]
#[command(
    name = dfcache::NAME,
    author,
    version,
    about = "dfcache is a cache command line based on P2P technology in Dragonfly.",
    long_about = "A cache command line based on P2P technology in Dragonfly that can import file and export file in P2P network, \
    and it can copy multiple replicas during import. P2P cache is effectively used for fast read and write cache."
)]
struct Args {
    #[arg(
        short = 'e',
        long = "endpoint",
        default_value_os_t = dfdaemon::default_download_unix_socket_path(),
        help = "Endpoint of dfdaemon's GRPC server"
    )]
    endpoint: PathBuf,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        help = "Specify the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_os_t = dfcache::default_dfcache_log_dir(),
        help = "Specify the log directory"
    )]
    log_dir: PathBuf,

    #[arg(
        long,
        default_value_t = 6,
        help = "Specify the max number of log files"
    )]
    log_max_files: usize,

    #[arg(
        long = "verbose",
        default_value_t = false,
        help = "Specify whether to print log"
    )]
    verbose: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Subcommand)]
#[command(args_conflicts_with_subcommands = true)]
pub enum Command {
    #[command(
        name = "import",
        author,
        version,
        about = "Import a file into Dragonfly P2P network",
        long_about = "Import a local file into Dragonfly P2P network and copy multiple replicas during import. If import successfully, it will return a task ID."
    )]
    Import(import::ImportCommand),

    #[command(
        name = "export",
        author,
        version,
        about = "Export a file from Dragonfly P2P network",
        long_about = "Export a file from Dragonfly P2P network by task ID. If export successfully, it will return the local file path."
    )]
    Export(export::ExportCommand),

    #[command(
        name = "stat",
        author,
        version,
        about = "Stat a file in Dragonfly P2P network",
        long_about = "Stat a file in Dragonfly P2P network by task ID. If stat successfully, it will return the file information."
    )]
    Stat(stat::StatCommand),

    #[command(
        name = "rm",
        author,
        version,
        about = "Remove a file from Dragonfly P2P network",
        long_about = "Remove the P2P cache in Dragonfly P2P network by task ID."
    )]
    Remove(remove::RemoveCommand),
}

// Implement the execute for Command.
impl Command {
    #[allow(unused)]
    pub async fn execute(self, endpoint: &Path) -> Result<()> {
        match self {
            Self::Import(cmd) => cmd.execute(endpoint).await,
            Self::Export(cmd) => cmd.execute(endpoint).await,
            Self::Stat(cmd) => cmd.execute(endpoint).await,
            Self::Remove(cmd) => cmd.execute(endpoint).await,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments.
    let args = Args::parse();

    // Initialize tracing.
    let _guards = init_tracing(
        dfcache::NAME,
        &args.log_dir,
        args.log_level,
        args.log_max_files,
        None,
        false,
        args.verbose,
    );

    // Execute the command.
    args.command.execute(&args.endpoint).await?;
    Ok(())
}
