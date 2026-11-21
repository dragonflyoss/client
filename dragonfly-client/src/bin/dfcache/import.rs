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

use bytesize::ByteSize;
use clap::Parser;
use dragonfly_api::dfdaemon::v2::UploadPersistentCacheTaskRequest;
use dragonfly_client::grpc::dfdaemon_download::{
    UploadPersistentCacheTaskContentMetadata, UploadPersistentCacheTaskContentRequest,
    UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE,
};
use dragonfly_client::resource::piece::MIN_PIECE_LENGTH;
use dragonfly_client::terminal;
use dragonfly_client_config::dfcache::default_dfcache_persistent_replica_count;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use dragonfly_client_util::net::preferred_local_ip;
use futures::Stream;
use indicatif::{ProgressBar, ProgressStyle};
use path_absolutize::*;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::fs::File;
use tokio_stream::StreamExt;
use tokio_util::io::ReaderStream;
use tracing::{error, info};

use super::*;

/// The default steady tick interval of progress bar.
const DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL: Duration = Duration::from_millis(80);

/// The subcommand of import.
#[derive(Debug, Clone, Parser)]
pub struct ImportCommand {
    #[arg(help = "Specify the path of the file to import")]
    path: PathBuf,

    #[arg(
        long = "content-for-calculating-task-id",
        env = "DFCACHE_IMPORT_CONTENT_FOR_CALCULATING_TASK_ID",
        help = "Specify the content used to calculate the persistent cache task ID. If it is set, use its value to calculate the task ID. Otherwise, calculate the persistent cache task ID by computing SHA256 hash of file content. Note: SHA256 computation takes longer for large files."
    )]
    content_for_calculating_task_id: Option<String>,

    #[arg(
        long = "persistent-replica-count",
        default_value_t = default_dfcache_persistent_replica_count(),
        env = "DFCACHE_IMPORT_PERSISTENT_REPLICA_COUNT",
        help = "Specify the replica count of the persistent cache task"
    )]
    persistent_replica_count: u64,

    #[arg(
        long = "piece-length",
        required = false,
        env = "DFCACHE_IMPORT_PIECE_LENGTH",
        help = "Specify the piece length for downloading file. If the piece length is not specified, the piece length will be calculated according to the file size. Different piece lengths will be divided into different persistent cache tasks. The value needs to be set with human readable format and needs to be greater than or equal to 4mib, for example: 4mib, 1gib"
    )]
    piece_length: Option<ByteSize>,

    #[arg(
        long = "application",
        required = false,
        env = "DFCACHE_IMPORT_APPLICATION",
        help = "Different applications for the same url will be divided into different persistent cache tasks"
    )]
    application: Option<String>,

    #[arg(
        long = "tag",
        required = false,
        env = "DFCACHE_IMPORT_TAG",
        help = "Different tags for the same file will be divided into different persistent cache tasks"
    )]
    tag: Option<String>,

    #[arg(
        long = "ttl",
        value_parser= humantime::parse_duration,
        default_value = "1h",
        env = "DFCACHE_IMPORT_TTL",
        help = "Specify the ttl of the persistent cache task, maximum is 7d and minimum is 1m"
    )]
    ttl: Duration,

    #[arg(
        long = "timeout",
        value_parser= humantime::parse_duration,
        default_value = "30m",
        env = "DFCACHE_IMPORT_TIMEOUT",
        help = "Specify the timeout for importing a file"
    )]
    timeout: Duration,

    #[arg(
        short = 'e',
        long = "endpoint",
        default_value_os_t = dfdaemon::default_download_unix_socket_path(),
        env = "DFCACHE_IMPORT_DFDAEMON_ENDPOINT",
        help = "Endpoint of dfdaemon's GRPC server"
    )]
    endpoint: PathBuf,

    #[arg(
        long = "transfer-to-dfdaemon",
        default_value_t = false,
        env = "DFCACHE_IMPORT_TRANSFER_TO_DFDAEMON",
        help = "Stream file content to dfdaemon over the unix domain socket. Enable this when the source path is not visible in dfdaemon's mount namespace, such as when dfcache runs in a Kubernetes Pod with only the socket mounted. By default, dfcache preserves the legacy behavior and sends only the absolute path."
    )]
    transfer_to_dfdaemon: bool,

    #[arg(
        long,
        default_value_t = false,
        env = "DFCACHE_IMPORT_NO_PROGRESS",
        help = "Specify whether to disable the progress bar display"
    )]
    no_progress: bool,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        env = "DFCACHE_IMPORT_LOG_LEVEL",
        help = "Specify the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_t = false,
        env = "DFCACHE_IMPORT_CONSOLE",
        help = "Specify whether to print log"
    )]
    console: bool,
}

#[derive(Clone, Default)]
struct ContentReadError(Arc<Mutex<Option<std::io::Error>>>);

impl ContentReadError {
    fn set(&self, error: std::io::Error) {
        *self.0.lock().unwrap() = Some(error);
    }

    fn take(&self) -> Option<std::io::Error> {
        self.0.lock().unwrap().take()
    }
}

fn content_chunk_stream(
    file: File,
) -> (
    impl Stream<Item = UploadPersistentCacheTaskContentRequest>,
    ContentReadError,
) {
    let read_error = ContentReadError::default();
    let stream_read_error = read_error.clone();
    let stream = ReaderStream::with_capacity(file, UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE)
        .map_while(move |result| match result {
            Ok(chunk) => Some(UploadPersistentCacheTaskContentRequest::chunk(chunk)),
            Err(err) => {
                error!("read import file content failed: {}", err);
                stream_read_error.set(err);
                None
            }
        });
    (stream, read_error)
}

/// Implement the execute for ImportCommand.
impl ImportCommand {
    /// Executes the import sub command with comprehensive validation and error handling.
    ///
    /// This function serves as the main entry point for the dfcache import command execution.
    /// It handles the complete workflow including argument parsing, validation, logging setup,
    /// dfdaemon client connection, and import operation execution. The function provides
    /// detailed error reporting with colored terminal output and follows a fail-fast approach
    /// with immediate process termination on any critical failures.
    pub async fn execute(&self) -> Result<()> {
        // Parse command line arguments.
        Args::parse();

        // Initialize tracing.
        let _guards = init_command_tracing(self.log_level, self.console);

        // Validate the command line arguments.
        if let Err(err) = self.validate_args() {
            terminal::error("Validating Failed!");
            terminal::separator();
            terminal::field("Message:", err);
            terminal::separator();

            std::process::exit(1);
        }

        // Get dfdaemon download client.
        let dfdaemon_download_client =
            match get_dfdaemon_download_client(self.endpoint.to_path_buf()).await {
                Ok(client) => client,
                Err(err) => {
                    terminal::error("Connect Dfdaemon Failed!");
                    terminal::separator();
                    terminal::field(
                        "Message:",
                        format!(
                            "can not connect {}, please check the unix socket {}",
                            err,
                            self.endpoint.to_string_lossy()
                        ),
                    );
                    terminal::separator();

                    std::process::exit(1);
                }
            };

        // Warn user if content_for_calculating_task_id is not set, which may lead to slow SHA256
        // computation.
        if self.content_for_calculating_task_id.is_none() {
            terminal::warn("Warning: SHA256 hash computation from file content is slow for large files. Use --content-for-calculating-task-id to improve performance.");
        }

        // Run import sub command.
        if let Err(err) = self.run(dfdaemon_download_client).await {
            match err {
                Error::TonicStatus(status) => {
                    terminal::error("Importing Failed!");
                    terminal::separator();
                    terminal::error_field("Bad Code:", status.code());
                    terminal::field("Message:", status.message());
                    terminal::field("Details:", std::str::from_utf8(status.details()).unwrap());
                    terminal::separator();
                }
                err => {
                    terminal::error("Importing Failed!");
                    terminal::separator();
                    terminal::error_field("Message:", err);
                    terminal::separator();
                }
            }

            std::process::exit(1);
        }

        Ok(())
    }

    /// Executes the cache import operation by uploading a file to the persistent cache system.
    ///
    /// This function handles the core import functionality by uploading a local file to the
    /// dfdaemon persistent cache system. It provides visual feedback through a progress spinner,
    /// converts the file path to absolute format, and configures the cache task with specified
    /// parameters including TTL, replica count, and piece length. The operation is asynchronous
    /// and provides completion feedback with the generated task ID.
    async fn run(&self, dfdaemon_download_client: DfdaemonDownloadClient) -> Result<()> {
        let absolute_path = Path::new(&self.path).absolutize()?;
        info!("import file: {}", absolute_path.to_string_lossy());

        let progress_bar = if self.no_progress {
            ProgressBar::hidden()
        } else {
            ProgressBar::new_spinner()
        };

        progress_bar.enable_steady_tick(DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL);
        progress_bar.set_style(
            ProgressStyle::with_template("{spinner:.blue} {msg}")
                .unwrap()
                .tick_strings(&["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"]),
        );
        progress_bar.set_message("Importing...");

        let request = UploadPersistentCacheTaskRequest {
            content_for_calculating_task_id: self.content_for_calculating_task_id.clone(),
            path: absolute_path.to_string_lossy().to_string(),
            persistent_replica_count: self.persistent_replica_count,
            tag: self.tag.clone(),
            application: self.application.clone(),
            piece_length: self.piece_length.map(|piece_length| piece_length.as_u64()),
            ttl: Some(prost_wkt_types::Duration::try_from(self.ttl).or_err(ErrorType::ParseError)?),
            timeout: Some(
                prost_wkt_types::Duration::try_from(self.timeout).or_err(ErrorType::ParseError)?,
            ),
            remote_ip: preferred_local_ip().map(|ip| ip.to_string()),
        };

        let persistent_cache_task = if self.transfer_to_dfdaemon {
            let file = File::open(absolute_path.as_ref()).await?;
            let content_length = file.metadata().await?.len();
            let metadata = UploadPersistentCacheTaskContentMetadata {
                content_for_calculating_task_id: request.content_for_calculating_task_id,
                content_length,
                persistent_replica_count: request.persistent_replica_count,
                tag: request.tag,
                application: request.application,
                piece_length: request.piece_length,
                ttl: request.ttl,
                timeout: request.timeout,
                remote_ip: request.remote_ip,
            };
            let (content, read_error) = content_chunk_stream(file);
            let result = dfdaemon_download_client
                .upload_persistent_cache_task_content(metadata, content)
                .await;
            if let Some(err) = read_error.take() {
                return Err(err.into());
            }
            result?
        } else {
            dfdaemon_download_client
                .upload_persistent_cache_task(request)
                .await?
        };

        progress_bar.finish_with_message(format!("Done: {}", persistent_cache_task.id));
        Ok(())
    }

    /// Validates command line arguments for the import operation to ensure safe and correct execution.
    ///
    /// This function performs comprehensive validation of import-specific parameters to prevent
    /// invalid operations and ensure the import request meets all system requirements. It validates
    /// TTL boundaries, file existence and type, and piece length constraints before allowing the
    /// import operation to proceed.
    fn validate_args(&self) -> Result<()> {
        if self.ttl < Duration::from_secs(5 * 60)
            || self.ttl > Duration::from_secs(7 * 24 * 60 * 60)
        {
            return Err(Error::ValidationError(format!(
                "ttl must be between 5 minutes and 7 days, but got {}",
                self.ttl.as_secs()
            )));
        }

        if self.path.is_dir() {
            return Err(Error::ValidationError(format!(
                "path {} is a directory",
                self.path.display()
            )));
        }

        if !self.path.exists() {
            return Err(Error::ValidationError(format!(
                "path {} does not exist",
                self.path.display()
            )));
        }

        if let Some(piece_length) = self.piece_length {
            if piece_length.as_u64() < MIN_PIECE_LENGTH {
                return Err(Error::ValidationError(format!(
                    "piece length {} bytes is less than the minimum piece length {} bytes",
                    piece_length.as_u64(),
                    MIN_PIECE_LENGTH
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use dragonfly_client::grpc::dfdaemon_download::upload_persistent_cache_task_content_request;
    use tempfile::tempdir;

    async fn read_content_chunks(path: &Path) -> Vec<Bytes> {
        let file = File::open(path).await.unwrap();
        let (stream, _) = content_chunk_stream(file);
        stream
            .map(|request| match request.request {
                Some(upload_persistent_cache_task_content_request::Request::Chunk(chunk)) => chunk,
                _ => panic!("file stream produced a non-chunk message"),
            })
            .collect()
            .await
    }

    #[test]
    fn validate_args_checks_ttl_path_and_piece_length() {
        let tempdir = tempdir().unwrap();
        let dir = tempdir.path().to_str().unwrap();
        let existing_file = format!("{dir}/existing.txt");
        let missing_file = format!("{dir}/missing.txt");
        std::fs::File::create(&existing_file).unwrap();

        let test_cases: Vec<(Vec<&str>, Result<()>)> = vec![
            (vec![existing_file.as_str(), "--ttl", "5m"], Ok(())),
            (vec![existing_file.as_str(), "--ttl", "7d"], Ok(())),
            (vec![existing_file.as_str(), "--piece-length", "4mib"], Ok(())),
            (
                vec![existing_file.as_str(), "--ttl", "299s"],
                Err(Error::ValidationError("ttl must be between 5 minutes and 7 days, but got 299".to_string())),
            ),
            (
                vec![existing_file.as_str(), "--ttl", "604801s"],
                Err(Error::ValidationError("ttl must be between 5 minutes and 7 days, but got 604801".to_string())),
            ),
            (
                vec![dir],
                Err(Error::ValidationError(format!("path {dir} is a directory"))),
            ),
            (
                vec![missing_file.as_str()],
                Err(Error::ValidationError(format!("path {missing_file} does not exist"))),
            ),
            (
                vec![existing_file.as_str(), "--piece-length", "1mib"],
                Err(Error::ValidationError(format!(
                    "piece length 1048576 bytes is less than the minimum piece length {MIN_PIECE_LENGTH} bytes"
                ))),
            ),
        ];

        for (argv, expected) in test_cases {
            let command =
                ImportCommand::parse_from(std::iter::once("dfcache").chain(argv.iter().copied()));
            let result = command.validate_args();
            assert_eq!(
                result.map_err(|err| err.to_string()),
                expected.map_err(|err| err.to_string())
            );
        }
    }

    #[tokio::test]
    async fn content_chunk_stream_covers_empty_and_large_file_boundaries() {
        let tempdir = tempdir().unwrap();
        let path = tempdir.path().join("content.bin");
        let lengths = [
            0,
            UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE - 1,
            UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE,
            UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE + 1,
            2 * UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE + 17,
        ];

        for length in lengths {
            let content: Vec<u8> = (0..length).map(|index| (index % 251) as u8).collect();
            tokio::fs::write(&path, &content).await.unwrap();
            let chunks = read_content_chunks(&path).await;
            let expected_chunks = length.div_ceil(UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE);
            assert_eq!(chunks.len(), expected_chunks);
            assert!(chunks.iter().all(|chunk| {
                !chunk.is_empty() && chunk.len() <= UPLOAD_PERSISTENT_CACHE_TASK_CONTENT_CHUNK_SIZE
            }));
            assert_eq!(chunks.concat(), content);
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn content_chunk_stream_supports_non_utf8_filename() {
        use std::ffi::OsString;
        use std::os::unix::ffi::OsStringExt;

        let tempdir = tempdir().unwrap();
        let path = tempdir
            .path()
            .join(OsString::from_vec(vec![b'f', 0x80, 0xff, b'e']));
        let content = [0x00, 0xff, 0x80, 0x41];
        tokio::fs::write(&path, content).await.unwrap();
        let chunks = read_content_chunks(&path).await;
        assert_eq!(chunks.concat(), content);
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn content_chunk_stream_retains_source_read_error() {
        let tempdir = tempdir().unwrap();
        let file = File::open(tempdir.path()).await.unwrap();
        let (stream, read_error) = content_chunk_stream(file);
        let chunks: Vec<_> = stream.collect().await;
        assert!(chunks.is_empty());
        assert!(read_error.take().is_some());
    }

    #[test]
    fn transfer_to_dfdaemon_is_opt_in() {
        let tempdir = tempdir().unwrap();
        let path = tempdir.path().join("content.bin");
        std::fs::write(&path, b"content").unwrap();
        let path = path.to_str().unwrap();

        let command = ImportCommand::parse_from(["dfcache", path]);
        assert!(!command.transfer_to_dfdaemon);
        let command = ImportCommand::parse_from(["dfcache", path, "--transfer-to-dfdaemon"]);
        assert!(command.transfer_to_dfdaemon);
    }
}
