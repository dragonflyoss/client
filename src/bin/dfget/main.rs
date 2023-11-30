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
use dragonfly_api::common::v2::Download;
use dragonfly_api::common::v2::TaskType;
use dragonfly_api::dfdaemon::v2::DownloadTaskRequest;
use dragonfly_client::config::dfdaemon;
use dragonfly_client::config::dfget;
use dragonfly_client::grpc::dfdaemon::DfdaemonClient;
use dragonfly_client::tracing::init_tracing;
use dragonfly_client::Error;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Duration;
use std::{cmp::min, fmt::Write};
use tracing::Level;
use url::Url;

#[derive(Debug, Parser)]
#[command(
    name = dfget::NAME,
    author,
    version,
    about = "dfget is a download command line based on P2P technology",
    long_about = "A download command line based on P2P technology in Dragonfly that can download resources of different protocols."
)]
struct Args {
    #[arg(help = "Specify the URL to download")]
    url: Url,

    #[arg(
        short = 'o',
        long = "output",
        help = "Specify the output path of downloading file"
    )]
    output: PathBuf,

    #[arg(
        short = 'e',
        long = "endpoint",
        default_value_os_t = dfdaemon::default_download_unix_socket_path(),
        help = "Endpoint of dfdaemon's GRPC server"
    )]
    endpoint: PathBuf,

    #[arg(
        long = "timeout",
        value_parser= humantime::parse_duration,
        default_value = "2h",
        help = "Set the timeout for downloading a file"
    )]
    timeout: Duration,

    #[arg(
        long = "piece-length",
        default_value_t = 4194304,
        help = "Set the byte length of the piece"
    )]
    piece_length: u64,

    #[arg(
        long = "download-rate-limit",
        default_value_t = 2147483648,
        help = "Set the rate limit of the downloading in bytes per second"
    )]
    download_rate_limit: u64,

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
    priority: i32,

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
        short = 'H',
        long = "header",
        required = false,
        help = "Set the header for downloading file, e.g. --header='Content-Type: application/json' --header='Accept: application/json'"
    )]
    header: Option<Vec<String>>,

    #[arg(
        long = "filter",
        required = false,
        help = "Filter the query parameters of the downloaded URL. If the download URL is the same, it will be scheduled as the same task, e.g. --filter='signature' --filter='timeout'"
    )]
    filters: Option<Vec<String>>,

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
        default_value_os_t = dfget::default_dfget_log_dir(),
        help = "Specify the log directory"
    )]
    log_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Parse command line arguments.
    let args = Args::parse();

    // Initialize tracting.
    let _guards = init_tracing(dfget::NAME, &args.log_dir, args.log_level, None);

    // Create dfdaemon client.
    let client = DfdaemonClient::new_unix(args.endpoint).await.unwrap();
    let response = client
        .download_task(DownloadTaskRequest {
            download: Some(Download {
                url: args.url.to_string(),
                digest: Some(args.digest),
                range: None,
                r#type: TaskType::Dfdaemon as i32,
                tag: Some(args.tag),
                application: Some(args.application),
                priority: args.priority,
                filters: args.filters.unwrap_or_default(),
                header: parse_header(args.header.unwrap_or_default())?,
                piece_length: args.piece_length,
                output_path: args.output.into_os_string().into_string().unwrap(),
                timeout: Some(prost_wkt_types::Duration::try_from(args.timeout)?),
                download_rate_limit: Some(args.download_rate_limit),
                need_back_to_source: false,
            }),
        })
        .await?;

    // Initialize progress bar.
    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{wide_bar}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
        )
        .unwrap()
        .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
            write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
        })
        .progress_chars("#>-"),
    );

    //  Dwonload file.
    let mut downloaded = 0;
    let mut out_stream = response.into_inner();
    while let Some(message) = out_stream.message().await? {
        let piece = message.piece.ok_or(Error::InvalidParameter())?;
        pb.set_length(message.content_length);

        downloaded += piece.length;
        let position = min(downloaded + piece.length, message.content_length);
        pb.set_position(position);
    }

    pb.finish_with_message("downloaded");
    Ok(())
}

// parse_header parses the header strings to a hash map.
fn parse_header(raw_header: Vec<String>) -> Result<HashMap<String, String>, Error> {
    let mut header = HashMap::new();
    for h in raw_header {
        let mut parts = h.splitn(2, ':');
        let key = parts.next().unwrap().trim();
        let value = parts.next().unwrap().trim();
        header.insert(key.to_string(), value.to_string());
    }

    Ok(header)
}
