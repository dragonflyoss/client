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
use dragonfly_client_backend::{object_storage, BackendFactory, DirEntry, HeadRequest};
use dragonfly_client_config::{self, default_piece_length, dfdaemon, dfget};
use dragonfly_client_core::error::{BackendError, ErrorType, OrErr};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::http::{header_vec_to_hashmap, header_vec_to_reqwest_headermap};
use indicatif::{MultiProgress, ProgressBar, ProgressState, ProgressStyle};
use path_absolutize::*;
use percent_encoding::percent_decode_str;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{cmp::min, fmt::Write};
use termion::{color, style};
use tokio::fs;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinSet;
use tracing::{error, info, warn, Level};
use url::Url;
use uuid::Uuid;

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
        default_value_t = 10,
        help = "Specify the max count of file to download when downloading a directory. If the actual file count is greater than this value, the downloading will be rejected"
    )]
    max_files: usize,

    #[arg(
        long,
        default_value_t = 5,
        help = "Specify the max count of concurrent download files when downloading a directory"
    )]
    max_concurrent_requests: usize,

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
    args.output = Path::new(&args.output).absolutize()?.into();
    info!("download file to: {}", args.output.to_string_lossy());

    // If download from object storage, the path has end with '/', then download all files in then
    // directory. Otherwise, download the single file. Only object storage protocol supports
    // directory download.
    let scheme = args.url.scheme();
    if object_storage::Scheme::from_str(scheme).is_err() && args.url.path().ends_with('/') {
        return Err(Error::Unsupported(format!("{} download directory", scheme)));
    };

    if args.url.path().ends_with('/') {
        return download_dir(args, dfdaemon_download_client).await;
    };

    download(args, ProgressBar::new(0), dfdaemon_download_client).await
}

// download_dir downloads all files in the directory.
async fn download_dir(args: Args, download_client: DfdaemonDownloadClient) -> Result<()> {
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

    // Get all entries in the directory. If the directory is empty, then return directly.
    let entries = get_entries(args.clone(), object_storage.clone()).await?;
    if entries.is_empty() {
        warn!("directory {} is empty", args.url);
        return Ok(());
    };

    // If the actual file count is greater than the max_files, then reject the downloading.
    let count = entries.iter().filter(|entry| !entry.is_dir).count();
    if count > args.max_files {
        return Err(Error::MaxDownloadFilesExceeded(count));
    }

    // Initialize the multi progress bar.
    let multi_progress_bar = MultiProgress::new();

    // Initialize the join set.
    let mut join_set = JoinSet::new();
    let semaphore = Arc::new(Semaphore::new(args.max_concurrent_requests));

    // Iterate all entries in the directory.
    for entry in entries {
        let entry_url: Url = entry.url.parse().or_err(ErrorType::ParseError)?;

        // If entry is a directory, then create the output directory. If entry is a file,
        // then download the file to the output directory.
        if entry.is_dir {
            let output_dir = make_output_by_entry(args.url.clone(), &args.output, entry)?;
            fs::create_dir_all(&output_dir).await.map_err(|err| {
                error!("create {} failed: {}", output_dir.to_string_lossy(), err);
                err
            })?;
        } else {
            let mut entry_args = args.clone();
            entry_args.output = make_output_by_entry(args.url.clone(), &args.output, entry)?;
            entry_args.url = entry_url;

            let progress_bar = multi_progress_bar.add(ProgressBar::new(0));
            async fn download_entry(
                args: Args,
                progress_bar: ProgressBar,
                download_client: DfdaemonDownloadClient,
                _permit: OwnedSemaphorePermit,
            ) -> Result<()> {
                download(args, progress_bar, download_client).await
            }

            let permit = semaphore.clone().acquire_owned().await.unwrap();
            join_set.spawn(download_entry(
                entry_args,
                progress_bar,
                download_client.clone(),
                permit,
            ));
        }
    }

    // Wait for all download tasks finished.
    while let Some(message) = join_set
        .join_next()
        .await
        .transpose()
        .or_err(ErrorType::AsyncRuntimeError)?
    {
        match message {
            Ok(_) => continue,
            Err(err) => {
                error!("download entry failed: {}", err);
                return Err(err);
            }
        }
    }

    Ok(())
}

// download downloads the single file.
async fn download(
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

    // Get actual path rather than percentage encoded path as download path.
    let download_path = percent_decode_str(args.url.path()).decode_utf8_lossy();
    progress_bar.set_style(
        ProgressStyle::with_template(
            "{msg:.bold}\n[{elapsed_precise}] [{bar:60.green/red}] {percent:3}% ({bytes_per_sec:.red}, {eta:.cyan})",
        )
        .or_err(ErrorType::ParseError)?
        .with_key("eta", |state: &ProgressState, w: &mut dyn Write| {
            write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap()
        })
        .progress_chars("=>-"),
    );
    progress_bar.set_message(download_path.to_string());

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

    progress_bar.finish_with_message(format!("{} downloaded", download_path));
    Ok(())
}

// get_entries gets all entries in the directory.
async fn get_entries(args: Args, object_storage: Option<ObjectStorage>) -> Result<Vec<DirEntry>> {
    // Initialize backend factory and build backend.
    let backend_factory = BackendFactory::new(None)?;
    let backend = backend_factory.build(args.url.as_str())?;

    // Send head request.
    let response = backend
        .head(HeadRequest {
            // NOTE: Mock a task id for head request.
            task_id: Uuid::new_v4().to_string(),
            url: args.url.to_string(),
            http_header: Some(header_vec_to_reqwest_headermap(
                args.header.clone().unwrap_or_default(),
            )?),
            timeout: args.timeout,
            client_certs: None,
            object_storage,
        })
        .await?;

    // Return error when response is failed.
    if !response.success {
        return Err(Error::BackendError(BackendError {
            message: response.error_message.unwrap_or_default(),
            status_code: Some(response.http_status_code.unwrap_or_default()),
            header: Some(response.http_header.unwrap_or_default()),
        }));
    }

    Ok(response.entries)
}

// make_output_by_entry makes the output path by the entry information.
fn make_output_by_entry(url: Url, output: &Path, entry: DirEntry) -> Result<PathBuf> {
    // Get the root directory of the download directory and the output root directory.
    let root_dir = url.path().to_string();
    let mut output_root_dir = output.to_string_lossy().to_string();
    if !output_root_dir.ends_with('/') {
        output_root_dir.push('/');
    };

    // The url in the entry is percentage encoded, so we should decode it to get right path and
    // replace the root directory with the output root directory.
    let entry_url: Url = entry.url.parse().or_err(ErrorType::ParseError)?;
    let decoded_entry_url = percent_decode_str(entry_url.path()).decode_utf8_lossy();
    Ok(decoded_entry_url
        .replace(root_dir.as_str(), output_root_dir.as_str())
        .into())
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
