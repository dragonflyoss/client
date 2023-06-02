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
use client::config::dfdaemon::{
    default_dfdaemon_config_path, default_dfdaemon_log_dir, Config, NAME,
};
use client::metrics::Metrics;
use client::tracing::init_tracing;
use std::path::PathBuf;
use tracing::Level;

#[derive(Debug, Parser)]
#[command(
    name = NAME,
    author,
    version,
    about = "dfdaemon is a high performance P2P download daemon",
    long_about = "A high performance P2P download daemon in Dragonfly that can download resources of different protocols. \
    When user triggers a file downloading task, dfdaemon will download the pieces of file from other peers. \
    Meanwhile, it will act as an uploader to support other peers to download pieces from it if it owns them."
)]
struct Args {
    #[arg(
        short = 'c',
        long = "config",
        default_value_os_t = default_dfdaemon_config_path(),
        help = "Specify config file to use")
    ]
    config: PathBuf,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        help = "Set the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_os_t = default_dfdaemon_log_dir(),
        help = "Specify the log directory"
    )]
    log_dir: PathBuf,
}

#[tokio::main]
async fn main() {
    // Parse command line arguments.
    let args = Args::parse();

    // Initialize tracing.
    let _guards = init_tracing(NAME, &args.log_dir, args.log_level, None);

    // Load config.
    let config = Config::load(&args.config).unwrap().convert().unwrap();

    let metrics = Metrics::new(config.network.enable_ipv6);
    metrics.serve().await;
}
