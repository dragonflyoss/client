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

use clap::Parser;
use client::config::dfdaemon::default_dfdaemon_unix_socket_path;
use client::config::dfget::{default_dfget_log_dir, NAME};
use client::tracing::init_tracing;
use std::path::PathBuf;
use std::time::Duration;
use tracing::Level;

#[derive(Debug, Parser)]
#[command(
    name = NAME,
    author,
    version,
    about = "dfget is a download command line based on P2P technology",
    long_about = "A download command line based on P2P technology in Dragonfly that can download resources of different protocols."
)]
struct Args {
    #[arg(
        short = 'o',
        long = "output",
        help = "Specify the output path of downloading file"
    )]
    output: PathBuf,

    #[arg(
        short = 'e',
        long = "endpoint",
        default_value_os_t = default_dfdaemon_unix_socket_path(),
        help = "Endpoint of dfdaemon's GRPC server"
    )]
    endpoint: PathBuf,

    #[arg(
        long = "timeout",
        value_parser= humantime::parse_duration,
        default_value = "0s",
        help = "Set the timeout for downloading a file"
    )]
    timeout: Duration,

    #[arg(
        short = 'd',
        long = "digest",
        default_value = "",
        help = "Verify the integrity of the downloaded file using the specified digest, e.g. md5:86d3f3a95c324c9479bd8986968f4327"
    )]
    digest: String,

    #[arg(
        short = 'p',
        long = "priority",
        default_value_t = 6,
        help = "Set the priority for scheduling task"
    )]
    priority: u32,

    #[arg(
        long = "application",
        default_value = "",
        help = "Caller application which is used for statistics and access control"
    )]
    application: String,

    #[arg(
        long = "tag",
        default_value = "",
        help = "Different tags for the same url will be divided into different tasks"
    )]
    tag: String,

    #[arg(
        long = "header",
        required = false,
        help = "Set the header for downloading file, e.g. --header='Content-Type: application/json' --header='Accept: application/json'"
    )]
    header: Vec<String>,

    #[arg(
        long = "filter",
        required = false,
        help = "Filter the query parameters of the downloaded URL. If the download URL is the same, it will be scheduled as the same task, e.g. --filter='signature' --filter='timeout'"
    )]
    filter: Vec<String>,

    #[arg(
        long = "disable-back-to-source",
        default_value_t = false,
        help = "Disable back-to-source download when dfget download failed"
    )]
    disable_back_to_source: bool,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        help = "Set the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_os_t = default_dfget_log_dir(),
        help = "Specify the log directory"
    )]
    log_dir: PathBuf,
}

fn main() {
    // Parse command line arguments.
    let args = Args::parse();

    // Initialize tracting.
    let _guards = init_tracing(NAME, &args.log_dir, args.log_level);
}
