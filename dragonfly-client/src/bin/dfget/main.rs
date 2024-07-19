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
use dragonfly_client_backend::{BackendFactory, HeadRequest};
use dragonfly_client_config::{self, default_piece_length, dfdaemon, dfget};
use dragonfly_client_core::error::{BackendError, ErrorType, OrErr};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::http::header_vec_to_hashmap;
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use path_absolutize::*;
use percent_encoding::percent_decode_str;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use std::{cmp::min, fmt::Write};
use termion::{color, style};
use tokio::sync::Semaphore;
use tracing::{error, info, warn, Level};
use url::Url;

const LONG_ABOUT: &str = r#"
A download command line based on P2P technology in Dragonfly that can download resources of different protocols.

The full documentation is here: https://d7y.io/docs/next/reference/commands/client/dfget/.

Examples:
  # Download a file from HTTP server.
  $ dfget https://<host>:<port>/<path> -O /tmp/file.txt
  
  # Download a file from Amazon Simple Storage Service(S3).
  $ dfget s3://<bucket>/<path> -O /tmp/file.txt --storage-access-key-id=<access_key_id> --storage-access-key-secret=<access_key_secret>
  
  # Download a file from Google Cloud Storage Service(GCS).
  $ dfget gcs://<bucket>/<path> -O /tmp/file.txt --storage-credential=<credential> --storage-endpoint=<endpoint>
  
  # Download a file from Azure Blob Storage Service(ABS).
  $ dfget abs://<container>/<path> -O /tmp/file.txt --storage-access-key-id=<account_name> --storage-access-key-secret=<account_key> --storage-endpoint=<endpoint>
  
  # Download a file from Aliyun Object Storage Service(OSS).
  $ dfget oss://<bucket>/<path> -O /tmp/file.txt --storage-access-key-id=<access_key_id> --storage-access-key-secret=<access_key_secret> --storage-endpoint=<endpoint>
  
  # Download a file from Huawei Cloud Object Storage Service(OBS).
  $ dfget obs://<bucket>/<path> -O /tmp/file.txt --storage-access-key-id=<access_key_id> --storage-access-key-secret=<access_key_secret> --storage-endpoint=<endpoint>
  
  # Download a file from Tencent Cloud Object Storage Service(COS).
  $ dfget cos://<bucket>/<path> -O /tmp/file.txt --storage-access-key-id=<access_key_id> --storage-access-key-secret=<access_key_secret> --storage-endpoint=<endpoint>
"#;

const DFGET_HEAD_REQUEST_TASK_ID: &str = "dfget";

#[derive(Debug, Parser, Clone)]
#[command(
    name = dfget::NAME,
    author,
    version,
    about = "dfget is a download command line based on P2P technology",
    long_about = LONG_ABOUT,
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

    #[arg(long, help = "Specify the region for the Object Storage Service")]
    storage_region: Option<String>,

    #[arg(long, help = "Specify the endpoint for the Object Storage Service")]
    storage_endpoint: Option<String>,

    #[arg(
        long,
        help = "Specify the access key ID for the Object Storage Service"
    )]
    storage_access_key_id: Option<String>,

    #[arg(
        long,
        help = "Specify the access key secret for the Object Storage Service"
    )]
    storage_access_key_secret: Option<String>,

    #[arg(
        long,
        help = "Specify the session token for Amazon Simple Storage Service(S3)"
    )]
    storage_session_token: Option<String>,

    #[arg(
        long,
        help = "Specify the credential for Google Cloud Storage Service(GCS)"
    )]
    storage_credential: Option<String>,

    #[arg(
        long,
        default_value = "publicRead",
        help = "Specify the predefined ACL for Google Cloud Storage Service(GCS)"
    )]
    storage_predefined_acl: Option<String>,

    #[arg(
        long,
        default_value_t = 8,
        help = "Specify the max number of file to download"
    )]
    download_max_files: usize,

    #[arg(
        long,
        default_value_t = 5,
        help = "Specify the concurrent count of download tasks"
    )]
    download_concurrent_count: usize,

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
                            "{}{}{}Bad Status Code:{} {}",
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
                        "{}{}{}Bad Code:{} {}",
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
async fn run(mut args: Args) -> Result<()> {
    let dfdaemon_download_client = get_dfdaemon_download_client(args.endpoint.to_path_buf())
        .await
        .map_err(|err| {
            error!("initialize dfdaemon download client failed: {}", err);
            err
        })?;

    // Get the absolute path of the output file.
    let absolute_path = Path::new(&args.output).absolutize()?;
    info!("download file to: {}", absolute_path.to_string_lossy());

    args.output = absolute_path.into();

    // If url ends with '/', treat it as a directory and download the whole directory.
    if args.url.path().ends_with('/') {
        download_tasks(args, dfdaemon_download_client).await
    } else {
        // Download single file.
        let progress_bar = ProgressBar::new(0);
        download_task(args, progress_bar, dfdaemon_download_client).await
    }
}

async fn download_tasks(args: Args, download_client: DfdaemonDownloadClient) -> Result<()> {
    // Only when the `access_key_id` and `access_key_secret` are provided at the same time,
    // they will be passed to the `DownloadTaskRequest`.
    let mut object_storage = None;
    if let (Some(access_key_id), Some(access_key_secret)) = (
        args.storage_access_key_id.clone(),
        args.storage_access_key_secret.clone(),
    ) {
        object_storage = Some(ObjectStorage {
            access_key_id,
            access_key_secret,
            session_token: args.storage_session_token.clone(),
            region: args.storage_region.clone(),
            endpoint: args.storage_endpoint.clone(),
            credential: args.storage_credential.clone(),
            predefined_acl: args.storage_predefined_acl.clone(),
        });
    }

    // Init the backend factory to choose which backend to use for sending head request.
    let backend_factory = BackendFactory::new(None)?;

    // Get the actual backend to send head request.
    let backend = backend_factory.build(args.url.as_str())?;

    // Send head request.
    let head_response = backend
        .head(HeadRequest {
            task_id: DFGET_HEAD_REQUEST_TASK_ID.into(),
            url: args.url.to_string(),
            http_header: None,
            timeout: args.timeout,
            client_certs: None,
            object_storage,
        })
        .await?;

    // Return error when response is failed.
    if !head_response.success {
        return Err(Error::BackendError(BackendError {
            message: head_response.error_message.unwrap_or_default(),
            status_code: Some(head_response.http_status_code.unwrap_or_default()),
            header: Some(head_response.http_header.unwrap_or_default()),
        }));
    }

    // If target directory is empty, then just return.
    let Some(entries) = head_response.entries else {
        warn!("no file is found in {}", args.url);
        return Ok(());
    };

    // Calc the total file count and compare it with args to decide whether to execute download task.
    let file_count = entries.iter().filter(|e| !e.is_dir).count();
    if file_count > args.download_max_files {
        return Err(Error::MaxDownloadFileCountExceeded(file_count));
    }

    // Due to the root_dir always ends with '/', but the args.output may end with '/', so
    // append '/' to output_root_dir if need.
    // These two variable root_dir and output_root_dir will be used to build the actual output
    // directory.
    // For example, if root_dir is '/test/' and output_root_dir is '/path/to/target/', the actual output
    // directory will be '/path/to/target/file-to-download', so, if output_root_dir is not suffix with
    // '/', it's necessary to append a '/' to it.
    let root_dir = args.url.path();
    let output_root_dir = if args.output.to_string_lossy().ends_with('/') {
        args.output.to_string_lossy().to_string()
    } else {
        format!("{}/", args.output.to_string_lossy())
    };

    let multi_progress_bar = MultiProgress::new();

    // Use the semaphore to control the concurrent download task number.
    // The initial value of semaphore is taken from the user input.
    let concurrent_control = Arc::new(Semaphore::new(args.download_concurrent_count));

    // To store the download task handler.
    let mut handlers = Vec::with_capacity(file_count);

    for entry in entries {
        // The url in the entry is returned by head request which must be a value url and will
        //  not panic, so just use .expect() to get the url.
        let url: Url = entry.url.parse().expect("unexpected url");

        // If entry is a directory, then create it, or execute the download task.
        if entry.is_dir {
            // The url in the entry is percentage encoded, so we should decode it to get right path.
            let decoded_url_path = percent_decode_str(url.path()).decode_utf8_lossy();
            // Get the actual path.
            let output_dir = decoded_url_path.replacen(root_dir, &output_root_dir, 1);

            tokio::fs::create_dir(&output_dir).await.map_err(|e| {
                error!("create {} failed: {}", output_dir, e);
                e
            })?;
        } else {
            let mut args = args.clone();
            // The url in the entry is percentage encoded, so we should decode it to get right path.
            let decoded_url_path = percent_decode_str(url.path()).decode_utf8_lossy();
            // Get the actual path.
            args.output = decoded_url_path
                .replacen(root_dir, &output_root_dir, 1)
                .into();
            args.url = url;

            let progress_bar = multi_progress_bar.add(ProgressBar::new(0));
            let client = download_client.clone();
            let semaphore = concurrent_control.clone();

            handlers.push(tokio::spawn(async move {
                // This is used for concurrent control.
                // semaphore will live until all the download task finished, it should
                // remain open when the download task executing, so we can expect it directly.
                let _permit = semaphore.acquire().await.expect("semaphore closed");
                download_task(args, progress_bar, client).await
            }));
        }
    }

    // Wait for all download tasks finished.
    for handler in handlers {
        handler.await.map_err(Error::TokioJoinError)??;
    }

    Ok(())
}

async fn download_task(
    args: Args,
    progress_bar: ProgressBar,
    download_client: DfdaemonDownloadClient,
) -> Result<()> {
    // Only when the `access_key_id` and `access_key_secret` are provided at the same time,
    // they will be passed to the `DownloadTaskRequest`.
    let mut object_storage = None;
    if let (Some(access_key_id), Some(access_key_secret)) = (
        args.storage_access_key_id.clone(),
        args.storage_access_key_secret.clone(),
    ) {
        object_storage = Some(ObjectStorage {
            access_key_id,
            access_key_secret,
            session_token: args.storage_session_token.clone(),
            region: args.storage_region.clone(),
            endpoint: args.storage_endpoint.clone(),
            credential: args.storage_credential.clone(),
            predefined_acl: args.storage_predefined_acl.clone(),
        });
    }

    // Create dfdaemon client.
    let response = download_client
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
                output_path: Some(args.output.to_string_lossy().to_string()),
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

    // Get actual path rather than percentage encoded path as task name.
    let task_name = percent_decode_str(args.url.path()).decode_utf8_lossy();

    progress_bar.set_style(
        ProgressStyle::with_template(
            "{msg:.bold}\n[{elapsed_precise}] [{bar:60.green/red}] {percent:3}% ({bytes_per_sec:.red}, {eta:.cyan})",
        )
        .or_err(ErrorType::ParseError)?
        .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
            write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
        })
        .progress_chars("━-"),
    );
    progress_bar.set_message(task_name.to_string());

    // Download file.
    let mut downloaded = 0;
    let mut out_stream = response.into_inner();
    while let Some(message) = out_stream.message().await.map_err(|err| {
        error!("get message failed: {}", err);
        err
    })? {
        match message.response {
            Some(download_task_response::Response::DownloadTaskStartedResponse(response)) => {
                progress_bar.set_length(response.content_length);
            }
            Some(download_task_response::Response::DownloadPieceFinishedResponse(response)) => {
                let piece = response.piece.ok_or(Error::InvalidParameter)?;

                downloaded += piece.length;
                let position = min(
                    downloaded + piece.length,
                    progress_bar.length().unwrap_or(0),
                );
                progress_bar.set_position(position);
            }
            None => {}
        }
    }

    progress_bar.finish_with_message(format!("{} downloaded", task_name));
    Ok(())
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
