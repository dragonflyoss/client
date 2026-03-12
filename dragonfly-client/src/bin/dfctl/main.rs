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
use dragonfly_client::grpc::dfdaemon_download::DfdaemonDownloadClient;
use dragonfly_client::grpc::health::HealthClient;
use dragonfly_client::tracing::init_command_tracing;
use dragonfly_client_config::VersionValueParser;
use dragonfly_client_config::{dfctl, dfdaemon};
use dragonfly_client_core::Result;
use std::path::PathBuf;
use tracing::Level;

pub mod persistent_cache_task;
pub mod persistent_task;
pub mod task;

#[derive(Debug, Parser)]
#[command(
    name = dfctl::NAME,
    author,
    version,
    about = "dfctl is a command line tool for managing Dragonfly tasks.",
    long_about = "A command line tool for managing tasks, persistent tasks, and persistent cache tasks in \
    Dragonfly. It provides listing and removal capabilities through a unified CLI interface.",
    disable_version_flag = true
)]
struct Args {
    #[arg(
        short = 'V',
        long = "version",
        help = "Print version information",
        default_value_t = false,
        action = clap::ArgAction::SetTrue,
        value_parser = VersionValueParser
    )]
    version: bool,

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

    #[arg(long, default_value_t = false, help = "Specify whether to print log")]
    console: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Subcommand)]
#[command(args_conflicts_with_subcommands = true)]
pub enum Command {
    #[command(
        name = "task",
        author,
        version,
        about = "Manage tasks in Dragonfly",
        long_about = "Manage standard tasks in Dragonfly, including listing and removing tasks."
    )]
    Task(task::TaskCommand),

    #[command(
        name = "persistent-task",
        author,
        version,
        about = "Manage persistent tasks in Dragonfly",
        long_about = "Manage persistent tasks in Dragonfly, including listing and removing persistent tasks."
    )]
    PersistentTask(persistent_task::PersistentTaskCommand),

    #[command(
        name = "persistent-cache-task",
        author,
        version,
        about = "Manage persistent cache tasks in Dragonfly",
        long_about = "Manage persistent cache tasks in Dragonfly, including listing and removing persistent cache tasks."
    )]
    PersistentCacheTask(persistent_cache_task::PersistentCacheTaskCommand),
}

/// Implement the execute for Command.
impl Command {
    pub async fn execute(self, endpoint: PathBuf, log_level: Level, console: bool) -> Result<()> {
        match self {
            Self::Task(cmd) => cmd.execute(endpoint, log_level, console).await,
            Self::PersistentTask(cmd) => cmd.execute(endpoint, log_level, console).await,
            Self::PersistentCacheTask(cmd) => cmd.execute(endpoint, log_level, console).await,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments.
    let args = Args::parse();

    // Execute the command.
    args.command
        .execute(args.endpoint, args.log_level, args.console)
        .await?;
    Ok(())
}

/// Creates and validates a dfdaemon download client with health checking.
///
/// This function establishes a connection to the dfdaemon service via Unix domain socket
/// and performs a health check to ensure the service is running and ready to handle
/// requests. Only after successful health verification does it return the download client
/// for actual use.
pub async fn get_dfdaemon_download_client(endpoint: PathBuf) -> Result<DfdaemonDownloadClient> {
    // Check dfdaemon's health.
    let health_client = HealthClient::new_unix(endpoint.clone()).await?;
    health_client.check_dfdaemon_download().await?;

    // Get dfdaemon download client.
    let dfdaemon_download_client = DfdaemonDownloadClient::new_unix(endpoint).await?;
    Ok(dfdaemon_download_client)
}
