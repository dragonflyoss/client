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
use dragonfly_api::common::v2::{Download, ObjectStorage, TaskType};
use dragonfly_api::dfdaemon::v2::{download_task_response, DownloadTaskRequest};
use dragonfly_api::errordetails::v2::Backend;
use dragonfly_client::grpc::dfdaemon_download::DfdaemonDownloadClient;
use dragonfly_client::grpc::health::HealthClient;
use dragonfly_client::tracing::init_tracing;
use dragonfly_client_config::{self, default_piece_length, dfdaemon, dfget};
use dragonfly_client_core::{
    error::{ErrorType, ExternalError, OrErr},
    Error, Result,
};
use dragonfly_client_util::http::header_vec_to_hashmap;
use fslock::LockFile;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;
use std::{cmp::min, fmt::Write};
use termion::{color, style};
use tokio::process::{Child, Command};
use tracing::{error, info, Level};
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
        short = 'O',
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
        help = "Specify the timeout for downloading a file"
    )]
    timeout: Duration,

    #[arg(
        long = "piece-length",
        default_value_t = default_piece_length(),
        help = "Specify the byte length of the piece"
    )]
    piece_length: u64,

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
        help = "Specify the priority for scheduling task"
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
        help = "Specify the header for downloading file, e.g. --header='Content-Type: application/json' --header='Accept: application/json'"
    )]
    header: Option<Vec<String>>,

    #[arg(
        long = "filtered-query-param",
        required = false,
        help = "Filter the query parameters of the downloaded URL. If the download URL is the same, it will be scheduled as the same task, e.g. --filtered-query-param='signature' --filtered-query-param='timeout'"
    )]
    filtered_query_params: Option<Vec<String>>,

    #[arg(
        long = "disable-back-to-source",
        default_value_t = false,
        help = "Disable back-to-source download when dfget download failed"
    )]
    disable_back_to_source: bool,

    #[arg(
        long,
        help = "Specify the access key ID for the object storage service"
    )]
    access_key_id: Option<String>,

    #[arg(
        long,
        help = "Specify the access key secret for the object storage service"
    )]
    access_key_secret: Option<String>,

    #[arg(
        long, 
        help = "Specify the session token for AWS S3"
    )]
    session_token: Option<String>,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        help = "Specify the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_os_t = dfget::default_dfget_log_dir(),
        help = "Specify the log directory"
    )]
    log_dir: PathBuf,

    #[arg(
        long,
        default_value_t = 6,
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
        help = "Specify the dfdaemon's logging level [trace, debug, info, warn, error]"
    )]
    dfdaemon_log_level: Level,

    #[arg(
        long = "dfdaemon-log-dir",
        default_value_os_t = dfdaemon::default_dfdaemon_log_dir(),
        help = "Specify the dfdaemon's log directory"
    )]
    dfdaemon_log_dir: PathBuf,

    #[arg(
        long,
        default_value_t = 6,
        help = "Specify the dfdaemon's max number of log files"
    )]
    dfdaemon_log_max_files: usize,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments.
    let args = Args::parse();

    // Initialize tracing.
    let _guards = init_tracing(
        dfget::NAME,
        &args.log_dir,
        args.log_level,
        args.log_max_files,
        None,
        false,
        args.verbose,
    );

    // Run dfget command.
    if let Err(err) = run(args).await {
        match err {
            Error::TonicStatus(status) => {
                let details = status.details();
                if let Ok(backend_err) = serde_json::from_slice::<Backend>(details) {
                    eprintln!(
                        "{}{}{}Downloading Failed!{}",
                        color::Fg(color::Red),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );

                    eprintln!(
                        "{}{}{}****************************************{}",
                        color::Fg(color::Black),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );

                    if let Some(status_code) = backend_err.status_code {
                        eprintln!(
                            "{}{}{}Bad status code:{} {}",
                            color::Fg(color::Red),
                            style::Italic,
                            style::Bold,
                            style::Reset,
                            status_code
                        );
                    }

                    eprintln!(
                        "{}{}{}Message:{} {}",
                        color::Fg(color::Cyan),
                        style::Italic,
                        style::Bold,
                        style::Reset,
                        backend_err.message
                    );

                    if !backend_err.header.is_empty() {
                        eprintln!(
                            "{}{}{}Header:{}",
                            color::Fg(color::Cyan),
                            style::Italic,
                            style::Bold,
                            style::Reset
                        );
                        for (key, value) in backend_err.header.iter() {
                            eprintln!("  [{}]: {}", key.as_str(), value.as_str());
                        }
                    }

                    eprintln!(
                        "{}{}{}****************************************{}",
                        color::Fg(color::Black),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );
                } else {
                    eprintln!(
                        "{}{}{}Downloading Failed!{}",
                        color::Fg(color::Red),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );

                    eprintln!(
                        "{}{}{}*********************************{}",
                        color::Fg(color::Black),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );

                    eprintln!(
                        "{}{}{}Bad code:{} {}",
                        color::Fg(color::Red),
                        style::Italic,
                        style::Bold,
                        style::Reset,
                        status.code()
                    );

                    eprintln!(
                        "{}{}{}Message:{} {}",
                        color::Fg(color::Cyan),
                        style::Italic,
                        style::Bold,
                        style::Reset,
                        status.message()
                    );

                    if !status.details().is_empty() {
                        eprintln!(
                            "{}{}{}Details:{} {}",
                            color::Fg(color::Cyan),
                            style::Italic,
                            style::Bold,
                            style::Reset,
                            std::str::from_utf8(status.details()).unwrap()
                        );
                    }

                    eprintln!(
                        "{}{}{}*********************************{}",
                        color::Fg(color::Black),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );
                }
            }
            Error::BackendError(err) => {
                eprintln!(
                    "{}{}{}Downloading Failed!{}",
                    color::Fg(color::Red),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );

                eprintln!(
                    "{}{}{}****************************************{}",
                    color::Fg(color::Black),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );

                eprintln!(
                    "{}{}{}Message:{} {}",
                    color::Fg(color::Red),
                    style::Italic,
                    style::Bold,
                    style::Reset,
                    err.message
                );

                if err.header.is_some() {
                    eprintln!(
                        "{}{}{}Header:{}",
                        color::Fg(color::Cyan),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );
                    for (key, value) in err.header.unwrap_or_default().iter() {
                        eprintln!("  [{}]: {}", key.as_str(), value.to_str().unwrap());
                    }
                }

                eprintln!(
                    "{}{}{}****************************************{}",
                    color::Fg(color::Black),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );
            }
            err => {
                eprintln!(
                    "{}{}{}Downloading Failed!{}",
                    color::Fg(color::Red),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );

                eprintln!(
                    "{}{}{}****************************************{}",
                    color::Fg(color::Black),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );

                eprintln!(
                    "{}{}{}Message:{} {}",
                    color::Fg(color::Red),
                    style::Italic,
                    style::Bold,
                    style::Reset,
                    err
                );

                eprintln!(
                    "{}{}{}****************************************{}",
                    color::Fg(color::Black),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );
            }
        }

        std::process::exit(1);
    }

    Ok(())
}

// run runs the dfget command.
async fn run(args: Args) -> Result<()> {
    // Get or create dfdaemon download client.
    let dfdaemon_download_client = get_or_create_dfdaemon_download_client(
        args.dfdaemon_config,
        args.endpoint.clone(),
        args.dfdaemon_log_dir,
        args.dfdaemon_log_level,
        args.dfdaemon_log_max_files,
        args.dfdaemon_lock_path,
    )
    .await
    .map_err(|err| {
        error!("initialize dfdaemon download client failed: {}", err);
        err
    })?;

    // Only when the `access_key_id` and `access_key_secret` are provided at the same time,
    // they will be pass to the `DownloadTaskRequest`.
    let mut object_storage = None;
    if let (Some(access_key_id), Some(access_key_secret)) =
        (args.access_key_id, args.access_key_secret)
    {
        object_storage = Some(ObjectStorage {
            access_key_id,
            access_key_secret,
            session_token: None,
        });
    }

    // Create dfdaemon client.
    let response = dfdaemon_download_client
        .download_task(DownloadTaskRequest {
            download: Some(Download {
                url: args.url.to_string(),
                digest: Some(args.digest),
                // NOTE: Dfget does not support range download.
                range: None,
                r#type: TaskType::Dfdaemon as i32,
                tag: Some(args.tag),
                application: Some(args.application),
                priority: args.priority,
                filtered_query_params: args.filtered_query_params.unwrap_or_default(),
                request_header: header_vec_to_hashmap(args.header.unwrap_or_default())?,
                piece_length: args.piece_length,
                output_path: Some(args.output.into_os_string().into_string().unwrap()),
                timeout: Some(
                    prost_wkt_types::Duration::try_from(args.timeout)
                        .or_err(ErrorType::ParseError)?,
                ),
                need_back_to_source: false,
                disable_back_to_source: args.disable_back_to_source,
                certificate_chain: Vec::new(),
                prefetch: false,
                object_storage,
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
        )
        .or_err(ErrorType::ParseError)?
        .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
            write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
        })
        .progress_chars("#>-"),
    );

    //  Download file.
    let mut downloaded = 0;
    let mut out_stream = response.into_inner();
    while let Some(message) = out_stream.message().await.map_err(|err| {
        error!("get message failed: {}", err);
        err
    })? {
        match message.response {
            Some(download_task_response::Response::DownloadTaskStartedResponse(response)) => {
                pb.set_length(response.content_length);
            }
            Some(download_task_response::Response::DownloadPieceFinishedResponse(response)) => {
                let piece = response.piece.ok_or(Error::InvalidParameter)?;

                downloaded += piece.length;
                let position = min(downloaded + piece.length, pb.length().unwrap_or(0));
                pb.set_position(position);
            }
            None => {}
        }
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
    log_max_files: usize,
    lock_path: PathBuf,
) -> Result<DfdaemonDownloadClient> {
    // Get dfdaemon download client and check its health.
    match get_dfdaemon_download_client(endpoint.clone()).await {
        Ok(dfdaemon_download_client) => return Ok(dfdaemon_download_client),
        Err(err) => error!("get dfdaemon download client failed: {}", err),
    }

    // Create a lock file to prevent multiple dfdaemon processes from being created.
    let mut f = LockFile::open(lock_path.as_path())?;
    f.lock()?;

    // Check dfdaemon download client again.
    match get_dfdaemon_download_client(endpoint.clone()).await {
        Ok(dfdaemon_download_client) => return Ok(dfdaemon_download_client),
        Err(err) => error!("get dfdaemon download client failed: {}", err),
    }

    // Spawn a dfdaemon process.
    let child = spawn_dfdaemon(config_path, log_dir, log_level, log_max_files).map_err(|err| {
        error!("spawn dfdaemon process failed: {}", err);
        ExternalError::new(ErrorType::ConfigError).with_context(format!(
            "Dfdaemon's unix socket is not exist in path: {:?}. dfget will spawn a dfdaemon process automatically, but spawn failed: {}. If you need to use the existing dfdaemon, please set the correct path of the dfdaemon's unix socket with --endpoint option.",
            endpoint.to_string_lossy(),
            err
        ))
    })?;
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
                    Err(err) => error!("get dfdaemon download client failed: {}", err),
                }
            }
            _ = &mut check_health_timeout => {
                return Err(Error::Unknown("get dfdaemon download client timeout".to_string()));
            }
        }
    }
}

// get_and_check_dfdaemon_download_client gets a dfdaemon download client and checks its health.
async fn get_dfdaemon_download_client(endpoint: PathBuf) -> Result<DfdaemonDownloadClient> {
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
    log_max_files: usize,
) -> Result<Child> {
    // Create dfdaemon command.
    let mut cmd = Command::new("dfdaemon");

    // Set command line arguments.
    cmd.arg("--config")
        .arg(config_path)
        .arg("--log-dir")
        .arg(log_dir)
        .arg("--log-level")
        .arg(log_level.to_string())
        .arg("--log-max-files")
        .arg(log_max_files.to_string());

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
