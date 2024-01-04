/*
 *     Copyright 2023 The Dragonfly Authors
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
use dragonfly_client::config::dfdaemon;
use dragonfly_client::config::dfstore;
use dragonfly_client::tracing::init_tracing;
use std::path::PathBuf;
use tracing::Level;

#[derive(Debug, Parser)]
#[command(
    name = dfstore::NAME,
    author,
    version,
    about = "dfstore is a storage command line based on P2P technology in Dragonfly.",
    long_about = "A storage command line based on P2P technology in Dragonfly that can rely on different types of object storage, \
    such as S3 or OSS, to provide stable object storage capabilities. It uses the entire P2P network as a cache when storing objects. \
    Rely on S3 or OSS as the backend to ensure storage reliability. In the process of object storage, \
    P2P cache is effectively used for fast read and write storage."
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
        default_value_os_t = dfstore::default_dfstore_log_dir(),
        help = "Specify the log directory"
    )]
    log_dir: PathBuf,

    #[arg(
        long,
        default_value_t = 24,
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
#[command()]
pub enum Command {
    #[command(
        name = "cp",
        author,
        version,
        about = "Download or upload files using object storage in Dragonfly",
        long_about = "Download a file from object storage in Dragonfly or upload a local file to object storage in Dragonfly"
    )]
    Copy(CopyCommand),

    #[command(
        name = "rm",
        author,
        version,
        about = "Remove a file from Dragonfly object storage",
        long_about = "Remove the P2P cache in Dragonfly and remove the file stored in the object storage."
    )]
    Remove(RemoveCommand),
}

// Download or upload files using object storage in Dragonfly.
#[derive(Debug, Clone, Parser)]
pub struct CopyCommand {}

// Remove a file from Dragonfly object storage.
#[derive(Debug, Clone, Parser)]
pub struct RemoveCommand {}

fn main() {
    // Parse command line arguments.
    let args = Args::parse();

    // Initialize tracing.
    let _guards = init_tracing(
        dfstore::NAME,
        &args.log_dir,
        args.log_level,
        args.log_max_files,
        None,
        args.verbose,
    );
}
