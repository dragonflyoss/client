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
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "dfget",
    author,
    version,
    about = "Dragonfly client written in Rust",
    long_about = "A download client based on P2P technology in Dragonfly that can download resources of different protocols."
)]
struct Args {
    #[arg(
        short = 'o',
        long = "output",
        help = "Specify the output path of downloading file."
    )]
    output: PathBuf,

    #[arg(short = 'c', long = "config", help = "Specify config file to use.")]
    config: PathBuf,
}

fn main() {
    let args = Args::parse();
    print!("{:?}", args.output)
}
