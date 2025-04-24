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
use dragonfly_client::resource::piece::MIN_PIECE_LENGTH;
use dragonfly_client_config::dfcache::default_dfcache_persistent_replica_count;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use indicatif::{ProgressBar, ProgressStyle};
use path_absolutize::*;
use std::path::{Path, PathBuf};
use std::time::Duration;
use termion::{color, style};
use tracing::info;

use super::*;

/// DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL is the default steady tick interval of progress bar.
const DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL: Duration = Duration::from_millis(80);

/// ImportCommand is the subcommand of import.
#[derive(Debug, Clone, Parser)]
pub struct ImportCommand {
    #[arg(help = "Specify the path of the file to import")]
    path: PathBuf,

    #[arg(
        long = "content-for-calculating-task-id",
        help = "Specify the content used to calculate the persistent cache task ID. If it is set, use its value to calculate the task ID, Otherwise, calculate the persistent cache task ID based on url, piece-length, tag, application, and filtered-query-params."
    )]
    content_for_calculating_task_id: Option<String>,

    #[arg(
        long = "persistent-replica-count",
        default_value_t = default_dfcache_persistent_replica_count(),
        help = "Specify the replica count of the persistent cache task"
    )]
    persistent_replica_count: u64,

    #[arg(
        long = "piece-length",
        required = false,
        help = "Specify the piece length for downloading file. If the piece length is not specified, the piece length will be calculated according to the file size. Different piece lengths will be divided into different persistent cache tasks. The value needs to be set with human readable format and needs to be greater than or equal to 4mib, for example: 4mib, 1gib"
    )]
    piece_length: Option<ByteSize>,

    #[arg(
        long = "application",
        required = false,
        help = "Different applications for the same url will be divided into different persistent cache tasks"
    )]
    application: Option<String>,

    #[arg(
        long = "tag",
        required = false,
        help = "Different tags for the same file will be divided into different persistent cache tasks"
    )]
    tag: Option<String>,

    #[arg(
        long = "ttl",
        value_parser= humantime::parse_duration,
        default_value = "1h",
        help = "Specify the ttl of the persistent cache task, maximum is 7d and minimum is 1m"
    )]
    ttl: Duration,

    #[arg(
        long = "timeout",
        value_parser= humantime::parse_duration,
        default_value = "30m",
        help = "Specify the timeout for importing a file"
    )]
    timeout: Duration,

    #[arg(
        short = 'e',
        long = "endpoint",
        default_value_os_t = dfdaemon::default_download_unix_socket_path(),
        help = "Endpoint of dfdaemon's GRPC server"
    )]
    endpoint: PathBuf,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        help = "Specify the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_os_t = dfcache::default_dfcache_log_dir(),
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

/// Implement the execute for ImportCommand.
impl ImportCommand {
    /// execute executes the import sub command.
    pub async fn execute(&self) -> Result<()> {
        // Parse command line arguments.
        Args::parse();

        // Initialize tracing.
        let _guards = init_tracing(
            dfcache::NAME,
            self.log_dir.clone(),
            self.log_level,
            self.log_max_files,
            None,
            self.verbose,
        );

        // Validate the command line arguments.
        if let Err(err) = self.validate_args() {
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
            match get_dfdaemon_download_client(self.endpoint.to_path_buf()).await {
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
                        self.endpoint.to_string_lossy(),
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

        // Run import sub command.
        if let Err(err) = self.run(dfdaemon_download_client).await {
            match err {
                Error::TonicStatus(status) => {
                    println!(
                        "{}{}{}Importing Failed!{}",
                        color::Fg(color::Red),
                        style::Italic,
                        style::Bold,
                        style::Reset,
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

                    println!(
                        "{}{}{}Details:{} {}",
                        color::Fg(color::Cyan),
                        style::Italic,
                        style::Bold,
                        style::Reset,
                        std::str::from_utf8(status.details()).unwrap()
                    );

                    println!(
                        "{}{}{}*********************************{}",
                        color::Fg(color::Black),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );
                }
                err => {
                    println!(
                        "{}{}{}Importing Failed!{}",
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

    /// run runs the import sub command.
    async fn run(&self, dfdaemon_download_client: DfdaemonDownloadClient) -> Result<()> {
        let absolute_path = Path::new(&self.path).absolutize()?;
        info!("import file: {}", absolute_path.to_string_lossy());

        let pb = ProgressBar::new_spinner();
        pb.enable_steady_tick(DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL);
        pb.set_style(
            ProgressStyle::with_template("{spinner:.blue} {msg}")
                .unwrap()
                .tick_strings(&["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"]),
        );
        pb.set_message("Importing...");

        let persistent_cache_task = dfdaemon_download_client
            .upload_persistent_cache_task(UploadPersistentCacheTaskRequest {
                content_for_calculating_task_id: self.content_for_calculating_task_id.clone(),
                path: absolute_path.to_string_lossy().to_string(),
                persistent_replica_count: self.persistent_replica_count,
                tag: self.tag.clone(),
                application: self.application.clone(),
                piece_length: self.piece_length.map(|piece_length| piece_length.as_u64()),
                ttl: Some(
                    prost_wkt_types::Duration::try_from(self.ttl).or_err(ErrorType::ParseError)?,
                ),
                timeout: Some(
                    prost_wkt_types::Duration::try_from(self.timeout)
                        .or_err(ErrorType::ParseError)?,
                ),
            })
            .await?;

        pb.finish_with_message(format!("Done: {}", persistent_cache_task.id));
        Ok(())
    }

    /// validate_args validates the command line arguments.
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
