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
use dragonfly_client::grpc::dfdaemon_download::DfdaemonDownloadClient;
use dragonfly_client::grpc::health::HealthClient;
use dragonfly_client::tracing::init_tracing;
use dragonfly_client::Error;
use fslock::LockFile;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use std::{cmp::min, fmt::Write};
use tokio::process::{Child, Command};
use tracing::{debug, error, info, Level};
use url::Url;

// DEFAULT_DFDAEMON_CHECK_HEALTH_INTERVAL is the default interval of checking dfdaemon's health.
const DEFAULT_DFDAEMON_CHECK_HEALTH_INTERVAL: Duration = Duration::from_millis(200);

// DEFAULT_DFDAEMON_CHECK_HEALTH_TIMEOUT is the default timeout of checking dfdaemon's health.
const DEFAULT_DFDAEMON_CHECK_HEALTH_TIMEOUT: Duration = Duration::from_secs(10);

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

    #[arg(
        short = 'c',
        long = "dfdaemon-config",
        default_value_os_t = dfdaemon::default_dfdaemon_config_path(),
        help = "Specify dfdaemon's config file to use")
    ]
    dfdaemon_config: PathBuf,

    #[arg(
        long = "dfdaemon-lock-path",
        default_value_os_t = dfdaemon::default_dfdaemon_lock_path(),
        help = "Specify the dfdaemon's lock file path"
    )]
    dfdaemon_lock_path: PathBuf,

    #[arg(
        long = "dfdaemon-log-level",
        default_value = "info",
        help = "Set the dfdaemon's logging level [trace, debug, info, warn, error]"
    )]
    dfdaemon_log_level: Level,

    #[arg(
        long = "dfdaemon-log-dir",
        default_value_os_t = dfdaemon::default_dfdaemon_log_dir(),
        help = "Specify the dfdaemon's log directory"
    )]
    dfdaemon_log_dir: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Parse command line arguments.
    let args = Args::parse();

    // Initialize tracting.
    let _guards = init_tracing(dfget::NAME, &args.log_dir, args.log_level, None);

    // Get or create dfdaemon download client.
    let dfdaemon_download_client = get_or_create_dfdaemon_download_client(
        args.dfdaemon_config,
        args.endpoint.clone(),
        args.dfdaemon_log_dir,
        args.dfdaemon_log_level,
        args.dfdaemon_lock_path,
    )
    .await
    .map_err(|err| {
        error!("initialize dfdaemon download client failed: {}", err);
        err
    })?;

    // Create dfdaemon client.
    let response = dfdaemon_download_client
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
        .await
        .map_err(|err| {
            error!("download task failed: {}", err);
            err
        })?;

    // Initialize progress bar.
    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] [{wide_bar}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta})",
        )?
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

// get_or_create_dfdaemon_download_client gets a dfdaemon download client or creates a new one.
async fn get_or_create_dfdaemon_download_client(
    config_path: PathBuf,
    endpoint: PathBuf,
    log_dir: PathBuf,
    log_level: Level,
    lock_path: PathBuf,
) -> Result<DfdaemonDownloadClient, anyhow::Error> {
    // Get dfdaemon download client and check its health.
    match get_dfdaemon_download_client(endpoint.clone()).await {
        Ok(dfdaemon_download_client) => return Ok(dfdaemon_download_client),
        Err(err) => debug!("get dfdaemon download client failed: {}", err),
    }

    // Create a lock file to prevent multiple dfdaemon processes from being created.
    let mut f = LockFile::open(lock_path.as_path())?;
    f.lock()?;

    // Check dfdaemon download client again.
    match get_dfdaemon_download_client(endpoint.clone()).await {
        Ok(dfdaemon_download_client) => return Ok(dfdaemon_download_client),
        Err(err) => debug!("get dfdaemon download client failed: {}", err),
    }

    // Spawn a dfdaemon process.
    let child = spawn_dfdaemon(config_path, log_dir, log_level)?;
    info!("spawn dfdaemon process: {:?}", child);

    // Initialize the timeout of checking dfdaemon's health.
    let check_health_timeout = tokio::time::sleep(DEFAULT_DFDAEMON_CHECK_HEALTH_TIMEOUT);
    tokio::pin!(check_health_timeout);

    // Wait for dfdaemon's health.
    let mut interval = tokio::time::interval(DEFAULT_DFDAEMON_CHECK_HEALTH_INTERVAL);
    loop {
        tokio::select! {
            _ = interval.tick() => {
                match get_dfdaemon_download_client(endpoint.clone()).await {
                    Ok(dfdaemon_download_client) => {
                        f.unlock()?;
                        return Ok(dfdaemon_download_client);
                    }
                    Err(err) => debug!("get dfdaemon download client failed: {}", err),
                }
            }
            _ = &mut check_health_timeout => {
                return Err(anyhow::anyhow!("get dfdaemon download client timeout"));
            }
        }
    }
}

// get_and_check_dfdaemon_download_client gets a dfdaemon download client and checks its health.
async fn get_dfdaemon_download_client(
    endpoint: PathBuf,
) -> Result<DfdaemonDownloadClient, anyhow::Error> {
    // Check dfdaemon's health.
    let health_client = HealthClient::new_unix(endpoint.clone()).await?;
    health_client.check_dfdaemon_download().await?;

    // Get dfdaemon download client.
    let dfdaemon_download_client = DfdaemonDownloadClient::new_unix(endpoint).await?;
    Ok(dfdaemon_download_client)
}

// spawn_dfdaemon spawns a dfdaemon process in the background.
fn spawn_dfdaemon(
    config_path: PathBuf,
    log_dir: PathBuf,
    log_level: Level,
) -> Result<Child, anyhow::Error> {
    // Create dfdaemon command.
    let mut cmd = Command::new("dfdaemon");

    // Set command line arguments.
    cmd.arg("--config")
        .arg(config_path)
        .arg("--log-dir")
        .arg(log_dir)
        .arg("--log-level")
        .arg(log_level.to_string());

    // Redirect stdin, stdout, stderr to /dev/null.
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    // Create a new session for dfdaemon by calling setsid.
    unsafe {
        cmd.pre_exec(|| {
            libc::setsid();
            Ok(())
        });
    }

    let child = cmd.spawn()?;
    Ok(child)
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
