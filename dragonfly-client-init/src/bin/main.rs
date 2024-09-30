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
use dragonfly_client::tracing::init_tracing;
use dragonfly_client_config::dfinit;
use dragonfly_client_config::VersionValueParser;
use dragonfly_client_init::container_runtime;
use std::path::PathBuf;
use tracing::{error, Level};

#[derive(Debug, Parser)]
#[command(
    name = dfinit::NAME,
    author,
    version,
    about = "dfinit is a command line for initializing runtime environment of the dfdaemon",
    long_about = "A command line for initializing runtime environment of the dfdaemon, \
    For example, if the container's runtime is containerd, then dfinit will modify the mirror configuration of containerd and restart the containerd service. \
    It also supports to change configuration of the other container's runtime, such as cri-o, docker, etc.",
    disable_version_flag = true
)]
struct Args {
    #[arg(
        short = 'c',
        long = "config",
        default_value_os_t = dfinit::default_dfinit_config_path(),
        help = "Specify config file to use")
    ]
    config: PathBuf,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        help = "Specify the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_os_t = dfinit::default_dfinit_log_dir(),
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

    #[arg(
        short = 'V',
        long = "version",
        help = "Print version information",
        default_value_t = false,
        action = clap::ArgAction::SetTrue,
        value_parser = VersionValueParser
    )]
    version: bool,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Parse command line arguments.
    let args = Args::parse();

    // Initialize tracing.
    let _guards = init_tracing(
        dfinit::NAME,
        args.log_dir,
        args.log_level,
        args.log_max_files,
        None,
        false,
        false,
        args.verbose,
    );

    // Load config.
    let config = dfinit::Config::load(&args.config).map_err(|err| {
        error!("failed to load config: {}", err);
        err
    })?;

    // Handle features of the container runtime.
    let container_runtime = container_runtime::ContainerRuntime::new(&config);
    container_runtime.run().await.map_err(|err| {
        error!("failed to run container runtime: {}", err);
        err
    })?;

    Ok(())
}
