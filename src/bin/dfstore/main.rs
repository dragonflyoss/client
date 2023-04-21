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
use client::config::dfstore::default_dfstore_log_dir;
use std::path::PathBuf;
use tracing::Level;

#[derive(Debug, Parser)]
#[command(
    name = "dfstore",
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
        default_value_os_t = default_dfdaemon_unix_socket_path(),
        help = "Endpoint of dfdaemon's GRPC server"
    )]
    endpoint: PathBuf,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        help = "Set the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_os_t = default_dfstore_log_dir(),
        help = "Specify the log directory"
    )]
    log_dir: PathBuf,
}

fn main() {
    let args = Args::parse();
    print!("{:?}", args.endpoint);
}
