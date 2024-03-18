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
use dragonfly_client_config::dfinit;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = dfinit::NAME,
    author,
    version,
    about = "dfinit is a command line for initializing runtime environment of the dfdaemon",
    long_about = "A command line for initializing runtime environment of the dfdaemon, \
    For example, if the container's runtime is containerd, then dfinit will modify the mirror configuration of containerd and restart the containerd service. \
    It also supports to change configuration of the other container's runtime, such as cri-o, docker, etc."
)]
struct Args {
    #[arg(
        short = 'c',
        long = "config",
        default_value_os_t = dfinit::default_dfinit_config_path(),
        help = "Specify config file to use")
    ]
    config: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Parse command line arguments.
    let args = Args::parse();

    // Load config.
    let config = dfinit::Config::load(&args.config)?;
    println!("{:?}", config);

    Ok(())
}
