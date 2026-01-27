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

use bytesize::ByteSize;
use clap::Parser;
use dragonfly_api::common::v2::{Download, Hdfs, ObjectStorage, TaskType};
use dragonfly_api::dfdaemon::v2::{
    download_task_response, DownloadTaskRequest, ListTaskEntriesRequest,
};
use dragonfly_api::errordetails::v2::Backend;
use dragonfly_client::grpc::dfdaemon_download::DfdaemonDownloadClient;
use dragonfly_client::grpc::health::HealthClient;
use dragonfly_client::resource::piece::MIN_PIECE_LENGTH;
use dragonfly_client::tracing::init_command_tracing;
use dragonfly_client_backend::{hdfs, object_storage, BackendFactory, DirEntry};
use dragonfly_client_config::VersionValueParser;
use dragonfly_client_config::{self, dfdaemon, dfget};
use dragonfly_client_core::error::{ErrorType, OrErr};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::net::preferred_local_ip;
use dragonfly_client_util::{
    fs::fallocate, http::header_vec_to_hashmap,
    http::query_params::default_proxy_rule_filtered_query_params,
};
use glob::Pattern;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressState, ProgressStyle};
use path_absolutize::*;
use percent_encoding::percent_decode_str;
use std::collections::HashSet;
use std::path::{Component, Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{cmp::min, fmt::Write};
use termion::{color, style};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::{debug, error, info, warn, Instrument, Level};
use url::Url;
use uuid::Uuid;

const LONG_ABOUT: &str = r#"
A download command line based on P2P technology in Dragonfly that can download resources of different protocols.

The full documentation is here: https://d7y.io/docs/next/reference/commands/client/dfget/.

Examples:
  # Download a file from HTTP server.
  $ dfget https://<host>:<port>/<path> -O /tmp/file.txt

  # Download a file from HDFS.
  $ dfget hdfs://<host>:<port>/<path> -O /tmp/file.txt --hdfs-delegation-token=<delegation_token>

  # Download a file from Amazon Simple Storage Service(S3).
  $ dfget s3://<bucket>/<path> -O /tmp/file.txt --storage-access-key-id=<access_key_id> --storage-access-key-secret=<access_key_secret>

  # Download a file from Google Cloud Storage Service(GCS).
  $ dfget gs://<bucket>/<path> -O /tmp/file.txt --storage-credential-path=<credential_path>

  # Download a file from Azure Blob Storage Service(ABS).
  $ dfget abs://<container>/<path> -O /tmp/file.txt --storage-access-key-id=<account_name> --storage-access-key-secret=<account_key>

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
    disable_version_flag = true,
)]
struct Args {
    #[arg(help = "Specify the URL to download")]
    url: Url,

    #[arg(
        long = "transfer-from-dfdaemon",
        default_value_t = false,
        help = "Specify whether to transfer the content of downloading file from dfdaemon's unix domain socket. If it is true, dfget will call dfdaemon to download the file, and dfdaemon will return the content of downloading file to dfget via unix domain socket, and dfget will copy the content to the output path. If it is false, dfdaemon will download the file and hardlink or copy the file to the output path."
    )]
    transfer_from_dfdaemon: bool,

    #[arg(
        long = "overwrite",
        default_value_t = false,
        help = "Specify whether to overwrite the output file if it already exists. If it is true, dfget will overwrite the output file. If it is false, dfget will return an error if the output file already exists. Cannot be used with `--force-hard-link=true`"
    )]
    overwrite: bool,

    #[arg(
        long = "force-hard-link",
        default_value_t = false,
        help = "Specify whether the download file must be hard linked to the output path. If hard link is failed, download will be failed. If it is false, dfdaemon will copy the file to the output path if hard link is failed."
    )]
    force_hard_link: bool,

    #[arg(
        long = "content-for-calculating-task-id",
        help = "Specify the content used to calculate the task ID. If it is set, use its value to calculate the task ID, Otherwise, calculate the task ID based on URL, piece-length, tag, application, and filtered-query-params."
    )]
    content_for_calculating_task_id: Option<String>,

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
        short = 'r',
        long = "recursive",
        default_value_t = false,
        help = "Specify whether to download the directory recursively. If it is true, dfget will download all files in the directory. If it is false, dfget will download the single file specified by the URL."
    )]
    recursive: bool,

    #[arg(
        long = "timeout",
        value_parser= humantime::parse_duration,
        default_value = "2h",
        help = "Specify the timeout for downloading a file"
    )]
    timeout: Duration,

    #[arg(
        long = "digest",
        required = false,
        help = "Verify the integrity of the downloaded file using the specified digest, support sha256, sha512, crc32. If the digest is not specified, the downloaded file will not be verified. Format: <algorithm>:<digest>. Examples: sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef, crc32:12345678"
    )]
    digest: Option<String>,

    #[arg(
        short = 'p',
        long = "priority",
        default_value_t = 6,
        help = "Specify the priority for scheduling task"
    )]
    priority: i32,

    #[arg(
        long = "piece-length",
        required = false,
        help = "Specify the piece length for downloading file. If the piece length is not specified, the piece length will be calculated according to the file size. Different piece lengths will be divided into different tasks. The value needs to be set with human readable format and needs to be greater than or equal to 4mib, for example: 4mib, 1gib"
    )]
    piece_length: Option<ByteSize>,

    #[arg(
        long = "application",
        default_value = "",
        help = "Different applications for the same URL will be divided into different tasks"
    )]
    application: String,

    #[arg(
        long = "tag",
        default_value = "",
        help = "Different tags for the same URL will be divided into different tasks"
    )]
    tag: String,

    #[arg(
        short = 'H',
        long = "header",
        required = false,
        help = "Specify the header for downloading file. Examples: --header='Content-Type: application/json' --header='Accept: application/json'"
    )]
    header: Option<Vec<String>>,

    #[arg(
        long = "filtered-query-param",
        required = false,
        help = "Filter the query parameters of the downloaded URL. If the download URL is the same, it will be scheduled as the same task. Examples: --filtered-query-param='signature' --filtered-query-param='timeout'"
    )]
    filtered_query_params: Option<Vec<String>>,

    #[arg(
        short = 'I',
        long = "include-files",
        required = false,
        help = "Filter files to download in a directory using glob patterns relative to the root URL's path. Examples: --include-files file.txt --include-files subdir/file.txt --include-files subdir/dir/"
    )]
    include_files: Option<Vec<String>>,

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
        help = "Specify the security token for the Object Storage Service"
    )]
    storage_security_token: Option<String>,

    #[arg(
        long,
        help = "Specify the session token for Amazon Simple Storage Service(S3)"
    )]
    storage_session_token: Option<String>,

    #[arg(
        long,
        help = "Specify the local path to the credential file which is used for OAuth2 authentication for Google Cloud Storage Service(GCS)"
    )]
    storage_credential_path: Option<String>,

    #[arg(
        long,
        default_value = "publicRead",
        help = "Specify the predefined ACL for Google Cloud Storage Service(GCS)"
    )]
    storage_predefined_acl: Option<String>,

    #[arg(
        long,
        help = "Specify whether to skip verify TLS certification for object storage service"
    )]
    storage_insecure_tls: Option<bool>,

    #[arg(
        long,
        help = "Specify the delegation token for Hadoop Distributed File System(HDFS)"
    )]
    hdfs_delegation_token: Option<String>,

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
        long,
        default_value_t = false,
        help = "Specify whether to disable the progress bar display"
    )]
    no_progress: bool,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        help = "Specify the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(long, default_value_t = false, help = "Specify whether to print log")]
    console: bool,

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
async fn main() -> anyhow::Result<()> {
    // Parse command line arguments.
    let args = convert_args(Args::parse());

    // Initialize tracing.
    let _guards = init_command_tracing(args.log_level, args.console);

    // Validate command line arguments.
    if let Err(err) = validate_args(&args) {
        println!(
            "{}{}{}Validating Failed!{}",
            color::Fg(color::Red),
            style::Italic,
            style::Bold,
            style::Reset
        );

        println!(
            "{}{}{}****************************************{}",
            color::Fg(color::Black),
            style::Italic,
            style::Bold,
            style::Reset
        );

        println!(
            "{}{}{}Message:{} {}",
            color::Fg(color::Cyan),
            style::Italic,
            style::Bold,
            style::Reset,
            err,
        );

        println!(
            "{}{}{}****************************************{}",
            color::Fg(color::Black),
            style::Italic,
            style::Bold,
            style::Reset
        );

        std::process::exit(1);
    }

    // Get dfdaemon download client.
    let dfdaemon_download_client =
        match get_dfdaemon_download_client(args.endpoint.to_path_buf()).await {
            Ok(client) => client,
            Err(err) => {
                println!(
                    "{}{}{}Connect Dfdaemon Failed!{}",
                    color::Fg(color::Red),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );

                println!(
                    "{}{}{}****************************************{}",
                    color::Fg(color::Black),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );

                println!(
                    "{}{}{}Message:{}, can not connect {}, please check the unix socket {}",
                    color::Fg(color::Cyan),
                    style::Italic,
                    style::Bold,
                    style::Reset,
                    err,
                    args.endpoint.to_string_lossy(),
                );

                println!(
                    "{}{}{}****************************************{}",
                    color::Fg(color::Black),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );

                std::process::exit(1);
            }
        };

    // Run dfget command.
    if let Err(err) = run(args, dfdaemon_download_client).await {
        match err {
            Error::TonicStatus(status) => {
                let details = status.details();
                if let Ok(backend_err) = serde_json::from_slice::<Backend>(details) {
                    println!(
                        "{}{}{}Downloading Failed!{}",
                        color::Fg(color::Red),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );

                    println!(
                        "{}{}{}****************************************{}",
                        color::Fg(color::Black),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );

                    if let Some(status_code) = backend_err.status_code {
                        println!(
                            "{}{}{}Bad Status Code:{} {}",
                            color::Fg(color::Red),
                            style::Italic,
                            style::Bold,
                            style::Reset,
                            status_code
                        );
                    }

                    println!(
                        "{}{}{}Message:{} {}",
                        color::Fg(color::Cyan),
                        style::Italic,
                        style::Bold,
                        style::Reset,
                        backend_err.message
                    );

                    if !backend_err.header.is_empty() {
                        println!(
                            "{}{}{}Header:{}",
                            color::Fg(color::Cyan),
                            style::Italic,
                            style::Bold,
                            style::Reset
                        );
                        for (key, value) in backend_err.header.iter() {
                            println!("  [{}]: {}", key.as_str(), value.as_str());
                        }
                    }

                    println!(
                        "{}{}{}****************************************{}",
                        color::Fg(color::Black),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );
                } else {
                    println!(
                        "{}{}{}Downloading Failed!{}",
                        color::Fg(color::Red),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );

                    println!(
                        "{}{}{}*********************************{}",
                        color::Fg(color::Black),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );

                    println!(
                        "{}{}{}Bad Code:{} {}",
                        color::Fg(color::Red),
                        style::Italic,
                        style::Bold,
                        style::Reset,
                        status.code()
                    );

                    println!(
                        "{}{}{}Message:{} {}",
                        color::Fg(color::Cyan),
                        style::Italic,
                        style::Bold,
                        style::Reset,
                        status.message()
                    );

                    if !status.details().is_empty() {
                        println!(
                            "{}{}{}Details:{} {}",
                            color::Fg(color::Cyan),
                            style::Italic,
                            style::Bold,
                            style::Reset,
                            std::str::from_utf8(status.details()).unwrap()
                        );
                    }

                    println!(
                        "{}{}{}*********************************{}",
                        color::Fg(color::Black),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );
                }
            }
            Error::BackendError(err) => {
                println!(
                    "{}{}{}Downloading Failed!{}",
                    color::Fg(color::Red),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );

                println!(
                    "{}{}{}****************************************{}",
                    color::Fg(color::Black),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );

                println!(
                    "{}{}{}Message:{} {}",
                    color::Fg(color::Red),
                    style::Italic,
                    style::Bold,
                    style::Reset,
                    err.message
                );

                if err.header.is_some() {
                    println!(
                        "{}{}{}Header:{}",
                        color::Fg(color::Cyan),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );
                    for (key, value) in err.header.unwrap_or_default().iter() {
                        println!("  [{}]: {}", key.as_str(), value.to_str().unwrap());
                    }
                }

                println!(
                    "{}{}{}****************************************{}",
                    color::Fg(color::Black),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );
            }
            err => {
                println!(
                    "{}{}{}Downloading Failed!{}",
                    color::Fg(color::Red),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );

                println!(
                    "{}{}{}****************************************{}",
                    color::Fg(color::Black),
                    style::Italic,
                    style::Bold,
                    style::Reset
                );

                println!(
                    "{}{}{}Message:{} {}",
                    color::Fg(color::Red),
                    style::Italic,
                    style::Bold,
                    style::Reset,
                    err
                );

                println!(
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

/// Runs the dfget command to download files or directories from a given URL.
///
/// This function serves as the main entry point for the dfget download operation.
/// It handles both single file downloads and directory downloads based on the URL format.
/// The function performs path normalization, validates the URL scheme's capabilities,
/// and delegates to the appropriate download handler.
async fn run(mut args: Args, dfdaemon_download_client: DfdaemonDownloadClient) -> Result<()> {
    // Get the absolute path of the output file.
    args.output = Path::new(&args.output).absolutize()?.into();
    info!("download file to: {}", args.output.to_string_lossy());

    // If the path has end with '/' and the scheme supports directory download,
    // then download all files in the directory. Otherwise, download the single file.
    let scheme = args.url.scheme();
    if args.url.path().ends_with('/') {
        if BackendFactory::unsupported_download_directory(scheme) {
            return Err(Error::Unsupported(format!("{} download directory", scheme)));
        };

        return download_dir(args, dfdaemon_download_client).await;
    };

    let progress_bar = if args.no_progress {
        ProgressBar::hidden()
    } else {
        ProgressBar::new(0)
    };

    download(args, progress_bar, dfdaemon_download_client).await
}

/// Downloads all files in a directory from various storage backends (object storage, HDFS, etc.).
///
/// This function handles directory-based downloads by recursively fetching all entries
/// in the specified directory. It supports filtering files based on include patterns,
/// enforces download limits, and performs concurrent downloads with configurable
/// concurrency control. The function creates the necessary directory structure
/// locally and downloads files while preserving the remote directory hierarchy.
async fn download_dir(args: Args, download_client: DfdaemonDownloadClient) -> Result<()> {
    // Initialize the object storage config and the hdfs config.
    let object_storage = Some(ObjectStorage {
        access_key_id: args.storage_access_key_id.clone(),
        access_key_secret: args.storage_access_key_secret.clone(),
        security_token: args.storage_security_token.clone(),
        session_token: args.storage_session_token.clone(),
        region: args.storage_region.clone(),
        endpoint: args.storage_endpoint.clone(),
        credential_path: args.storage_credential_path.clone(),
        predefined_acl: args.storage_predefined_acl.clone(),
        insecure_skip_verify: args.storage_insecure_tls,
    });

    let hdfs = Some(Hdfs {
        delegation_token: args.hdfs_delegation_token.clone(),
    });

    // Get all entries in the directory with include files filter.
    let entries: Vec<DirEntry> = get_all_entries(
        &args.url,
        args.header.clone(),
        args.include_files.clone(),
        object_storage,
        hdfs,
        download_client.clone(),
    )
    .await?;

    // If the entries is empty, then return directly.
    if entries.is_empty() {
        warn!("no entries found in directory {}", args.url);
        return Ok(());
    }

    // If the actual file count is greater than the max_files, then reject the downloading.
    let count = entries.iter().filter(|entry| !entry.is_dir).count();
    if count > args.max_files {
        return Err(Error::MaxDownloadFilesExceeded(count));
    }

    // Initialize the multi progress bar.
    let multi_progress_bar = if args.no_progress {
        let multi_progress = MultiProgress::new();
        multi_progress.set_draw_target(ProgressDrawTarget::hidden());
        multi_progress
    } else {
        MultiProgress::new()
    };

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
            fs::create_dir_all(&output_dir).await.inspect_err(|err| {
                error!("create {} failed: {}", output_dir.to_string_lossy(), err);
            })?;
        } else {
            let mut entry_args = args.clone();
            entry_args.output = make_output_by_entry(args.url.clone(), &args.output, entry)?;
            entry_args.url = entry_url;

            let progress_bar = multi_progress_bar.add(ProgressBar::new(0));
            let download_client = download_client.clone();
            let permit = semaphore.clone().acquire_owned().await.unwrap();
            join_set.spawn(
                async move {
                    let _permit = permit;
                    download(entry_args, progress_bar, download_client).await
                }
                .in_current_span(),
            );
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
                join_set.shutdown().await;
                return Err(err);
            }
        }
    }

    Ok(())
}

/// Get all entries in the directory with include files filter.
async fn get_all_entries(
    base_url: &Url,
    header: Option<Vec<String>>,
    include_files: Option<Vec<String>>,
    object_storage: Option<ObjectStorage>,
    hdfs: Option<Hdfs>,
    download_client: DfdaemonDownloadClient,
) -> Result<Vec<DirEntry>> {
    let urls: HashSet<Url> = match include_files {
        Some(files) => {
            let mut urls = HashSet::with_capacity(files.len());
            for file in files {
                let url = base_url.join(&file).or_err(ErrorType::ParseError)?;
                urls.insert(url);
            }

            urls
        }
        None => {
            let mut urls = HashSet::with_capacity(1);
            urls.insert(base_url.clone());
            urls
        }
    };
    info!(
        "make urls by args: {:?}",
        urls.iter().map(|u| u.as_str()).collect::<Vec<&str>>()
    );

    let mut entries = HashSet::new();
    for url in &urls {
        if !url.to_string().ends_with('/') {
            entries.insert(DirEntry {
                url: url.to_string(),
                content_length: 0,
                is_dir: false,
            });

            let parent = url.join(".").or_err(ErrorType::ParseError)?;
            if parent.path() != base_url.path() {
                entries.insert(DirEntry {
                    url: parent.to_string(),
                    content_length: 0,
                    is_dir: true,
                });
            }

            info!("add entries {:?} by url: {}", entries, url);
            continue;
        };

        let mut dir_entries = get_entries(
            url,
            header.clone(),
            object_storage.clone(),
            hdfs.clone(),
            download_client.clone(),
        )
        .await
        .inspect_err(|err| {
            error!("get dir entries for {} failed: {}", url, err);
        })?;

        for dir_entry in dir_entries.clone() {
            if dir_entry.is_dir {
                continue;
            }

            let parent = Url::parse(&dir_entry.url)
                .or_err(ErrorType::ParseError)?
                .join(".")
                .or_err(ErrorType::ParseError)?;

            if parent.path() != base_url.path() {
                dir_entries.push(DirEntry {
                    url: parent.to_string(),
                    content_length: 0,
                    is_dir: true,
                });
            }
        }

        let mut seen = HashSet::new();
        entries.retain(|entry| seen.insert(entry.clone()));
        entries.extend(dir_entries.clone());
        info!("add entries {:?} by dir url: {}", entries, url);
    }

    // Sort directories before files to ensure parent directories exist
    // before attempting to create their children.
    let mut sorted_entries = Vec::from_iter(entries);
    sorted_entries.sort_unstable_by(|a, b| b.is_dir.cmp(&a.is_dir));
    info!("get all entries: {:?}", sorted_entries);

    Ok(sorted_entries)
}

/// Downloads a single file from various storage backends using the dfdaemon service.
///
/// This function handles single file downloads by communicating with a dfdaemon client.
/// It supports multiple storage protocols (object storage, HDFS, HTTP/HTTPS) and provides
/// two transfer modes: direct download by dfdaemon or streaming piece content through
/// the client. The function includes progress tracking, file creation, and proper error
/// handling throughout the download process.
async fn download(
    args: Args,
    progress_bar: ProgressBar,
    download_client: DfdaemonDownloadClient,
) -> Result<()> {
    // Only initialize object storage when the scheme is an object storage protocol.
    let object_storage = match object_storage::Scheme::from_str(args.url.scheme()) {
        Ok(_) => Some(ObjectStorage {
            access_key_id: args.storage_access_key_id.clone(),
            access_key_secret: args.storage_access_key_secret.clone(),
            security_token: args.storage_security_token.clone(),
            session_token: args.storage_session_token.clone(),
            region: args.storage_region.clone(),
            endpoint: args.storage_endpoint.clone(),
            credential_path: args.storage_credential_path.clone(),
            predefined_acl: args.storage_predefined_acl.clone(),
            insecure_skip_verify: args.storage_insecure_tls,
        }),
        Err(_) => None,
    };

    // Only initialize HDFS when the scheme is HDFS protocol.
    let hdfs = match args.url.scheme() {
        hdfs::HDFS_SCHEME => Some(Hdfs {
            delegation_token: args.hdfs_delegation_token.clone(),
        }),
        _ => None,
    };

    // If the `filtered_query_params` is not provided, then use the default value.
    let filtered_query_params = args
        .filtered_query_params
        .unwrap_or_else(default_proxy_rule_filtered_query_params);

    // Dfget needs to notify dfdaemon to transfer the piece content of downloading file via unix domain socket
    // when the `transfer_from_dfdaemon` is true. Otherwise, dfdaemon will download the file and hardlink or
    // copy the file to the output path.
    let (output_path, need_piece_content) = if args.transfer_from_dfdaemon {
        (None, true)
    } else {
        (Some(args.output.to_string_lossy().to_string()), false)
    };

    // Create dfdaemon client.
    let response = download_client
        .download_task(DownloadTaskRequest {
            download: Some(Download {
                url: args.url.to_string(),
                digest: args.digest,
                // NOTE: Dfget does not support range download.
                range: None,
                r#type: TaskType::Standard as i32,
                tag: Some(args.tag),
                application: Some(args.application),
                priority: args.priority,
                filtered_query_params,
                request_header: header_vec_to_hashmap(args.header.unwrap_or_default())?,
                piece_length: args.piece_length.map(|piece_length| piece_length.as_u64()),
                output_path,
                timeout: Some(
                    prost_wkt_types::Duration::try_from(args.timeout)
                        .or_err(ErrorType::ParseError)?,
                ),
                need_back_to_source: false,
                disable_back_to_source: args.disable_back_to_source,
                certificate_chain: Vec::new(),
                prefetch: false,
                is_prefetch: false,
                need_piece_content,
                object_storage,
                hdfs,
                force_hard_link: args.force_hard_link,
                content_for_calculating_task_id: args.content_for_calculating_task_id,
                remote_ip: preferred_local_ip().map(|ip| ip.to_string()),
                concurrent_piece_count: None,
                overwrite: args.overwrite,
                actual_piece_length: None,
                actual_content_length: None,
                actual_piece_count: None,
                enable_task_id_based_blob_digest: false,
            }),
        })
        .await
        .inspect_err(|err| {
            error!("download task failed: {}", err);
        })?;

    // If transfer_from_dfdaemon is true, then dfget needs to create the output file and write the
    // piece content to the output file.
    let mut f = if args.transfer_from_dfdaemon {
        if let Some(parent) = args.output.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent).await.inspect_err(|err| {
                    error!("failed to create directory {:?}: {}", parent, err);
                })?;
            }
        }

        let f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .mode(dfget::DEFAULT_OUTPUT_FILE_MODE)
            .open(&args.output)
            .await
            .inspect_err(|err| {
                error!("open file {:?} failed: {}", args.output, err);
            })?;

        Some(f)
    } else {
        None
    };

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
    loop {
        match out_stream.message().await {
            Ok(Some(message)) => {
                match message.response {
                    Some(download_task_response::Response::DownloadTaskStartedResponse(
                        response,
                    )) => {
                        if let Some(f) = &f {
                            if let Err(err) = fallocate(f, response.content_length).await {
                                error!("fallocate {:?} failed: {}", args.output, err);
                                fs::remove_file(&args.output).await.inspect_err(|err| {
                                    error!("remove file {:?} failed: {}", args.output, err);
                                })?;

                                return Err(err);
                            }
                        }

                        progress_bar.set_length(response.content_length);
                    }
                    Some(download_task_response::Response::DownloadPieceFinishedResponse(
                        response,
                    )) => {
                        let piece = match response.piece {
                            Some(piece) => piece,
                            None => {
                                error!("response piece is missing");
                                fs::remove_file(&args.output).await.inspect_err(|err| {
                                    error!("remove file {:?} failed: {}", args.output, err);
                                })?;

                                return Err(Error::InvalidParameter);
                            }
                        };

                        // Dfget needs to write the piece content to the output file.
                        if let Some(f) = &mut f {
                            debug!("copy piece {} to {:?} started", piece.number, args.output);
                            if let Err(err) = f.seek(SeekFrom::Start(piece.offset)).await {
                                error!("seek {:?} failed: {}", args.output, err);
                                fs::remove_file(&args.output).await.inspect_err(|err| {
                                    error!("remove file {:?} failed: {}", args.output, err);
                                })?;

                                return Err(Error::IO(err));
                            }

                            let content = match piece.content {
                                Some(content) => content,
                                None => {
                                    error!("piece content is missing");
                                    fs::remove_file(&args.output).await.inspect_err(|err| {
                                        error!("remove file {:?} failed: {}", args.output, err);
                                    })?;

                                    return Err(Error::InvalidParameter);
                                }
                            };

                            if let Err(err) = f.write_all(&content).await {
                                error!(
                                    "write piece {} to {:?} failed: {}",
                                    piece.number, args.output, err
                                );
                                fs::remove_file(&args.output).await.inspect_err(|err| {
                                    error!("remove file {:?} failed: {}", args.output, err);
                                })?;

                                return Err(Error::IO(err));
                            }

                            debug!("copy piece {} to {:?} success", piece.number, args.output);
                        }

                        downloaded += piece.length;
                        let position = min(
                            downloaded + piece.length,
                            progress_bar.length().unwrap_or(0),
                        );
                        progress_bar.set_position(position);
                    }
                    None => {
                        error!("response is missing");
                        fs::remove_file(&args.output).await.inspect_err(|err| {
                            error!("remove file {:?} failed: {}", args.output, err);
                        })?;

                        return Err(Error::UnexpectedResponse);
                    }
                }
            }
            Ok(None) => break,
            Err(err) => {
                error!("get message failed: {}", err);
                fs::remove_file(&args.output).await.inspect_err(|err| {
                    error!("remove file {:?} failed: {}", args.output, err);
                })?;

                return Err(Error::TonicStatus(err));
            }
        }
    }

    if let Some(f) = &mut f {
        if let Err(err) = f.flush().await {
            error!("flush {:?} failed: {}", args.output, err);
            fs::remove_file(&args.output).await.inspect_err(|err| {
                error!("remove file {:?} failed: {}", args.output, err);
            })?;

            return Err(Error::IO(err));
        }
    };
    info!("flush {:?} success", args.output);

    progress_bar.finish();
    Ok(())
}

/// Retrieves all directory entries from a remote storage location.
///
/// This function communicates with the dfdaemon service to list all entries
/// (files and subdirectories) in the specified directory URL. It supports
/// various storage backends including object storage and HDFS by passing
/// the appropriate credentials and configuration. The function converts
/// the gRPC response into a local `DirEntry` format for further processing.
async fn get_entries(
    url: &Url,
    header: Option<Vec<String>>,
    object_storage: Option<ObjectStorage>,
    hdfs: Option<Hdfs>,
    download_client: DfdaemonDownloadClient,
) -> Result<Vec<DirEntry>> {
    info!("list task entries: {:?}", url);
    let response = download_client
        .list_task_entries(ListTaskEntriesRequest {
            task_id: Uuid::new_v4().to_string(),
            url: url.to_string(),
            request_header: header_vec_to_hashmap(header.clone().unwrap_or_default())?,
            timeout: None,
            certificate_chain: Vec::new(),
            object_storage,
            hdfs,
            remote_ip: preferred_local_ip().map(|ip| ip.to_string()),
        })
        .await
        .inspect_err(|err| {
            error!("list task entries failed: {}", err);
        })?;

    info!("list task entries response: {:?}", response.entries);
    Ok(response
        .entries
        .into_iter()
        .map(|entry| DirEntry {
            url: entry.url,
            content_length: entry.content_length as usize,
            is_dir: entry.is_dir,
        })
        .collect())
}

/// Constructs the local output path for a directory entry based on its remote URL.
///
/// This function maps a remote directory entry to its corresponding local file system
/// path by replacing the remote root directory with the local output directory.
/// It handles URL percent-decoding to ensure proper path construction and maintains
/// the relative directory structure from the remote source.
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
        .replacen(root_dir.as_str(), output_root_dir.as_str(), 1)
        .into())
}

/// Creates and validates a dfdaemon download client with health checking.
///
/// This function establishes a connection to the dfdaemon service via Unix domain socket
/// and performs a health check to ensure the service is running and ready to handle
/// download requests. Only after successful health verification does it return the
/// download client for actual use.
async fn get_dfdaemon_download_client(endpoint: PathBuf) -> Result<DfdaemonDownloadClient> {
    // Check dfdaemon's health.
    let health_client = HealthClient::new_unix(endpoint.clone()).await?;
    health_client.check_dfdaemon_download().await?;

    // Get dfdaemon download client.
    let dfdaemon_download_client = DfdaemonDownloadClient::new_unix(endpoint).await?;
    Ok(dfdaemon_download_client)
}

/// Converts command line arguments for download operations.
///
/// This function modifies the command line arguments to ensure
/// the arguments are in a suitable format for processing.
fn convert_args(mut args: Args) -> Args {
    // If the URL is a directory and the recursive flag is set, ensure the URL ends with '/'.
    // This is necessary to ensure that the URL is treated as a directory, can be downloaded recursively.
    if args.recursive && !args.url.path().ends_with('/') {
        let mut path = args.url.path().to_string();
        path.push('/');
        args.url.set_path(&path);
    }
    args
}

/// Validates command line arguments for consistency and safety requirements.
///
/// This function performs comprehensive validation of the download arguments to ensure
/// they are logically consistent and safe to execute. It checks URL-output path matching,
/// directory existence, file conflicts, piece length constraints, and glob pattern validity.
/// The validation prevents common user errors and potential security issues before
/// starting the download process.
fn validate_args(args: &Args) -> Result<()> {
    // If the URL is a directory, the output path should be a directory.
    if args.url.path().ends_with('/') && !args.output.is_dir() {
        return Err(Error::ValidationError(format!(
            "output path {} is not a directory",
            args.output.to_string_lossy()
        )));
    }

    // If the URL is a file, the output path should be a file and the parent directory should
    // exist.
    if !args.url.path().ends_with('/') {
        let absolute_path = Path::new(&args.output).absolutize()?;
        match absolute_path.parent() {
            Some(parent_path) => {
                if !parent_path.is_dir() {
                    return Err(Error::ValidationError(format!(
                        "output path {} is not a directory",
                        parent_path.to_string_lossy()
                    )));
                }
            }
            None => {
                return Err(Error::ValidationError(format!(
                    "output path {} is not exist",
                    args.output.to_string_lossy()
                )));
            }
        }

        if !args.overwrite && absolute_path.exists() {
            return Err(Error::ValidationError(format!(
                "output path {} is already exist",
                args.output.to_string_lossy()
            )));
        }
    }

    if let Some(piece_length) = args.piece_length {
        if piece_length.as_u64() < MIN_PIECE_LENGTH {
            return Err(Error::ValidationError(format!(
                "piece length {} bytes is less than the minimum piece length {} bytes",
                piece_length.as_u64(),
                MIN_PIECE_LENGTH
            )));
        }
    }

    if let Some(ref include_files) = args.include_files {
        for include_file in include_files {
            if Pattern::new(include_file).is_err() {
                return Err(Error::ValidationError(format!(
                    "invalid glob pattern in include_files: '{}'",
                    include_file
                )));
            }

            if !is_normal_relative_path(include_file) {
                return Err(Error::ValidationError(format!(
                    "path is not a normal relative path in include_files: '{}'. It must not contain '..', '.', or start with '/'.",
                    include_file
                )));
            }
        }
    }

    Ok(())
}

/// Validates that a path string is a normal relative path without unsafe components.
///
/// This function ensures that a given path is both relative (doesn't start with '/')
/// and contains only normal path components. It rejects paths with parent directory
/// references ('..'), current directory references ('.'), or any other special
/// path components that could be used for directory traversal attacks or unexpected
/// file system navigation.
fn is_normal_relative_path(path: &str) -> bool {
    let path = Path::new(path);
    path.is_relative()
        && path
            .components()
            .all(|comp| matches!(comp, Component::Normal(_)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_api::dfdaemon::v2::{Entry, ListTaskEntriesResponse};
    use mocktail::prelude::*;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[test]
    fn should_convert_args() {
        let tempdir = tempfile::tempdir().unwrap();

        let test_cases = vec![
            (
                Args::parse_from(vec![
                    "dfget",
                    "http://test.local/test.txt",
                    "--output",
                    tempdir
                        .path()
                        .join("test.txt")
                        .as_os_str()
                        .to_str()
                        .unwrap(),
                ]),
                "http://test.local/test.txt",
            ),
            (
                Args::parse_from(vec![
                    "dfget",
                    "http://test.local/test-dir",
                    "--recursive",
                    "--output",
                    tempdir.path().as_os_str().to_str().unwrap(),
                ]),
                "http://test.local/test-dir/",
            ),
            (
                Args::parse_from(vec![
                    "dfget",
                    "http://test.local/test-dir/",
                    "--recursive",
                    "--output",
                    tempdir.path().as_os_str().to_str().unwrap(),
                ]),
                "http://test.local/test-dir/",
            ),
            (
                Args::parse_from(vec![
                    "dfget",
                    "http://test.local/test-dir/",
                    "--output",
                    tempdir.path().as_os_str().to_str().unwrap(),
                ]),
                "http://test.local/test-dir/",
            ),
        ];

        for (args, expected_url) in test_cases {
            let args = convert_args(args);
            assert!(args.url.to_string() == expected_url);
        }
    }

    #[test]
    fn should_validate_args() {
        let tempdir = tempfile::tempdir().unwrap();

        // Download file.
        let output_file_path = tempdir.path().join("test.txt");
        let args = Args::parse_from(vec![
            "dfget",
            "http://test.local/test.txt",
            "--output",
            output_file_path.as_os_str().to_str().unwrap(),
        ]);

        let result = validate_args(&args);
        assert!(result.is_ok());

        // Download directory.
        let output_dir_path = tempdir.path();
        let args = Args::parse_from(vec![
            "dfget",
            "http://test.local/test-dir/",
            "--output",
            output_dir_path.as_os_str().to_str().unwrap(),
        ]);

        let result = validate_args(&args);
        assert!(result.is_ok());
    }

    #[test]
    fn should_return_error_when_args_is_not_valid() {
        let tempdir = tempfile::tempdir().unwrap();
        let non_exist_dir_path = tempdir.path().join("non_exist");
        let non_exist_file_path = non_exist_dir_path.join("non_exist.txt");

        let file_path = tempdir.path().join("test.txt");
        std::fs::File::create(&file_path).unwrap();

        let test_cases = vec![
            (
                Args::parse_from(vec![
                    "dfget",
                    "http://test.local/test-dir/",
                    "--output",
                    file_path.as_os_str().to_str().unwrap(),
                ]),
                format!("output path {} is not a directory", file_path.display()),
            ),
            (
                Args::parse_from(vec![
                    "dfget",
                    "http://test.local/test-dir/",
                    "--output",
                    non_exist_dir_path.as_os_str().to_str().unwrap(),
                ]),
                format!(
                    "output path {} is not a directory",
                    non_exist_dir_path.display()
                ),
            ),
            (
                Args::parse_from(vec![
                    "dfget",
                    "http://test.local/test.txt",
                    "--output",
                    file_path.as_os_str().to_str().unwrap(),
                ]),
                format!("output path {} is already exist", file_path.display()),
            ),
            (
                Args::parse_from(vec![
                    "dfget",
                    "http://test.local/test.txt",
                    "--output",
                    non_exist_file_path.as_os_str().to_str().unwrap(),
                ]),
                format!(
                    "output path {} is not a directory",
                    non_exist_dir_path.display()
                ),
            ),
            (
                Args::parse_from(vec!["dfget", "http://test.local/test.txt", "--output", "/"]),
                "output path / is not exist".to_string(),
            ),
        ];

        for (args, error_message) in test_cases {
            let result = validate_args(&args);
            assert!(result.is_err());
            assert_eq!(
                result.unwrap_err().to_string(),
                Error::ValidationError(error_message).to_string()
            )
        }
    }

    #[test]
    fn should_make_output_by_entry() {
        let url = Url::parse("http://example.com/root/").unwrap();
        let temp_dir = tempdir().unwrap();
        let output_path = temp_dir.path();

        let entry = DirEntry {
            url: Url::parse("http://example.com/root/dir/file.txt")
                .unwrap()
                .to_string(),
            content_length: 100,
            is_dir: false,
        };

        let result = make_output_by_entry(url, output_path, entry);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), output_path.join("dir/file.txt"));
    }

    #[test]
    fn should_make_output_by_entry_no_trailing_slash_in_output() {
        let url = Url::parse("http://example.com/root/").unwrap();
        let temp_dir = tempdir().unwrap();
        let output_path: PathBuf = temp_dir
            .path()
            .to_string_lossy()
            .to_string()
            .trim_end_matches('/')
            .parse()
            .unwrap();

        let entry = DirEntry {
            url: Url::parse("http://example.com/root/dir/file.txt")
                .unwrap()
                .to_string(),
            content_length: 100,
            is_dir: false,
        };

        let result = make_output_by_entry(url, &output_path, entry);
        assert!(result.is_ok());

        let path = result.unwrap();
        assert_eq!(path, output_path.join("dir/file.txt"));
    }

    #[test]
    fn should_return_error_when_make_output_with_invalid_url() {
        let url = Url::parse("http://example.com/root/dir/file.txt").unwrap();
        let temp_dir = tempdir().unwrap();
        let output = temp_dir.path();

        let entry = DirEntry {
            url: "invalid_url".to_string(),
            content_length: 100,
            is_dir: false,
        };

        let result = make_output_by_entry(url, output, entry);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn should_get_empty_entries() {
        let mut mocks = MockSet::new();
        mocks.mock(|when, then| {
            when.path("/dfdaemon.v2.DfdaemonDownload/ListTaskEntries");
            then.pb(ListTaskEntriesResponse {
                content_length: 0,
                response_header: HashMap::new(),
                status_code: None,
                entries: vec![],
            });
        });

        let server = MockServer::new_grpc("dfdaemon.v2.DfdaemonDownload").with_mocks(mocks);
        server.start().await.unwrap();

        let dfdaemon_download_client = DfdaemonDownloadClient::new(
            Arc::new(dfdaemon::Config::default()),
            format!("http://0.0.0.0:{}", server.port().unwrap()),
        )
        .await
        .unwrap();

        let entries = get_all_entries(
            &Url::parse("http://example.com/root/").unwrap(),
            None,
            None,
            None,
            None,
            dfdaemon_download_client,
        )
        .await
        .unwrap();

        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn should_get_all_entries_in_subdir() {
        let mut mocks = MockSet::new();
        mocks.mock(|when, then| {
            when.path("/dfdaemon.v2.DfdaemonDownload/ListTaskEntries");
            then.pb(ListTaskEntriesResponse {
                content_length: 0,
                response_header: HashMap::new(),
                status_code: None,
                entries: vec![
                    Entry {
                        url: "http://example.com/root/dir1/file1.txt".to_string(),
                        content_length: 100,
                        is_dir: false,
                    },
                    Entry {
                        url: "http://example.com/root/dir1/file2.txt".to_string(),
                        content_length: 100,
                        is_dir: false,
                    },
                    Entry {
                        url: "http://example.com/root/dir2/file1.txt".to_string(),
                        content_length: 200,
                        is_dir: false,
                    },
                    Entry {
                        url: "http://example.com/root/dir2/file2.txt".to_string(),
                        content_length: 200,
                        is_dir: false,
                    },
                ],
            });
        });

        let server = MockServer::new_grpc("dfdaemon.v2.DfdaemonDownload").with_mocks(mocks);
        server.start().await.unwrap();

        let dfdaemon_download_client = DfdaemonDownloadClient::new(
            Arc::new(dfdaemon::Config::default()),
            format!("http://0.0.0.0:{}", server.port().unwrap()),
        )
        .await
        .unwrap();

        let entries = get_all_entries(
            &Url::parse("http://example.com/root/").unwrap(),
            None,
            None,
            None,
            None,
            dfdaemon_download_client,
        )
        .await
        .unwrap();

        assert_eq!(
            entries.into_iter().collect::<HashSet<_>>(),
            vec![
                DirEntry {
                    url: "http://example.com/root/dir1/file1.txt".to_string(),
                    content_length: 100,
                    is_dir: false,
                },
                DirEntry {
                    url: "http://example.com/root/dir1/file2.txt".to_string(),
                    content_length: 100,
                    is_dir: false,
                },
                DirEntry {
                    url: "http://example.com/root/dir1/".to_string(),
                    content_length: 0,
                    is_dir: true,
                },
                DirEntry {
                    url: "http://example.com/root/dir2/file1.txt".to_string(),
                    content_length: 200,
                    is_dir: false,
                },
                DirEntry {
                    url: "http://example.com/root/dir2/file2.txt".to_string(),
                    content_length: 200,
                    is_dir: false,
                },
                DirEntry {
                    url: "http://example.com/root/dir2/".to_string(),
                    content_length: 0,
                    is_dir: true,
                },
            ]
            .into_iter()
            .collect::<HashSet<_>>()
        );
    }

    #[tokio::test]
    async fn should_get_all_entries_in_rootdir() {
        let mut mocks = MockSet::new();
        mocks.mock(|when, then| {
            when.path("/dfdaemon.v2.DfdaemonDownload/ListTaskEntries");
            then.pb(ListTaskEntriesResponse {
                content_length: 0,
                response_header: HashMap::new(),
                status_code: None,
                entries: vec![
                    Entry {
                        url: "http://example.com/root/file1.txt".to_string(),
                        content_length: 100,
                        is_dir: false,
                    },
                    Entry {
                        url: "http://example.com/root/file2.txt".to_string(),
                        content_length: 200,
                        is_dir: false,
                    },
                ],
            });
        });

        let server = MockServer::new_grpc("dfdaemon.v2.DfdaemonDownload").with_mocks(mocks);
        server.start().await.unwrap();

        let dfdaemon_download_client = DfdaemonDownloadClient::new(
            Arc::new(dfdaemon::Config::default()),
            format!("http://0.0.0.0:{}", server.port().unwrap()),
        )
        .await
        .unwrap();

        let entries = get_all_entries(
            &Url::parse("http://example.com/root/").unwrap(),
            None,
            None,
            None,
            None,
            dfdaemon_download_client,
        )
        .await
        .unwrap();

        assert_eq!(
            entries.into_iter().collect::<HashSet<_>>(),
            vec![
                DirEntry {
                    url: "http://example.com/root/file1.txt".to_string(),
                    content_length: 100,
                    is_dir: false,
                },
                DirEntry {
                    url: "http://example.com/root/file2.txt".to_string(),
                    content_length: 200,
                    is_dir: false,
                },
            ]
            .into_iter()
            .collect::<HashSet<_>>()
        );
    }

    #[tokio::test]
    async fn should_get_all_entries_in_rootdir_and_subdir() {
        let mut mocks = MockSet::new();
        mocks.mock(|when, then| {
            when.path("/dfdaemon.v2.DfdaemonDownload/ListTaskEntries");
            then.pb(ListTaskEntriesResponse {
                content_length: 0,
                response_header: HashMap::new(),
                status_code: None,
                entries: vec![
                    Entry {
                        url: "http://example.com/root/file1.txt".to_string(),
                        content_length: 100,
                        is_dir: false,
                    },
                    Entry {
                        url: "http://example.com/root/file2.txt".to_string(),
                        content_length: 200,
                        is_dir: false,
                    },
                    Entry {
                        url: "http://example.com/root/dir1/file1.txt".to_string(),
                        content_length: 100,
                        is_dir: false,
                    },
                    Entry {
                        url: "http://example.com/root/dir1/file2.txt".to_string(),
                        content_length: 100,
                        is_dir: false,
                    },
                    Entry {
                        url: "http://example.com/root/dir2/file1.txt".to_string(),
                        content_length: 200,
                        is_dir: false,
                    },
                    Entry {
                        url: "http://example.com/root/dir2/file2.txt".to_string(),
                        content_length: 200,
                        is_dir: false,
                    },
                ],
            });
        });

        let server = MockServer::new_grpc("dfdaemon.v2.DfdaemonDownload").with_mocks(mocks);
        server.start().await.unwrap();

        let dfdaemon_download_client = DfdaemonDownloadClient::new(
            Arc::new(dfdaemon::Config::default()),
            format!("http://0.0.0.0:{}", server.port().unwrap()),
        )
        .await
        .unwrap();

        let entries = get_all_entries(
            &Url::parse("http://example.com/root/").unwrap(),
            None,
            None,
            None,
            None,
            dfdaemon_download_client,
        )
        .await
        .unwrap();

        assert_eq!(
            entries.into_iter().collect::<HashSet<_>>(),
            vec![
                DirEntry {
                    url: "http://example.com/root/file1.txt".to_string(),
                    content_length: 100,
                    is_dir: false,
                },
                DirEntry {
                    url: "http://example.com/root/file2.txt".to_string(),
                    content_length: 200,
                    is_dir: false,
                },
                DirEntry {
                    url: "http://example.com/root/dir1/file1.txt".to_string(),
                    content_length: 100,
                    is_dir: false,
                },
                DirEntry {
                    url: "http://example.com/root/dir1/file2.txt".to_string(),
                    content_length: 100,
                    is_dir: false,
                },
                DirEntry {
                    url: "http://example.com/root/dir1/".to_string(),
                    content_length: 0,
                    is_dir: true,
                },
                DirEntry {
                    url: "http://example.com/root/dir2/file1.txt".to_string(),
                    content_length: 200,
                    is_dir: false,
                },
                DirEntry {
                    url: "http://example.com/root/dir2/file2.txt".to_string(),
                    content_length: 200,
                    is_dir: false,
                },
                DirEntry {
                    url: "http://example.com/root/dir2/".to_string(),
                    content_length: 0,
                    is_dir: true,
                },
            ]
            .into_iter()
            .collect::<HashSet<_>>()
        );
    }
}
