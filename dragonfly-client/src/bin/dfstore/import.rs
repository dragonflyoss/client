/*
 *     Copyright 2025 The Dragonfly Authors
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
use dragonfly_api::common::v2::ObjectStorage;
use dragonfly_api::dfdaemon::v2::UploadPersistentTaskRequest;
use dragonfly_client_config::dfstore::default_dfstore_persistent_replica_count;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use dragonfly_client_util::net::preferred_local_ip;
use indicatif::{ProgressBar, ProgressStyle};
use path_absolutize::*;
use std::path::{Path, PathBuf};
use std::time::Duration;
use termion::{color, style};
use tracing::info;
use url::Url;

use super::*;

/// DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL is the default steady tick interval of progress bar.
const DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL: Duration = Duration::from_millis(80);

/// ImportCommand is the subcommand of import.
#[derive(Debug, Clone, Parser)]
pub struct ImportCommand {
    #[arg(help = "Specify the path of the file to import")]
    path: PathBuf,

    #[arg(
        long,
        help = "Specify the URL for copying data to object storage. Format: scheme://<bucket>/<path>. Examples: s3://<bucket>/<path>, abs://<bucket>/<path>"
    )]
    url: Url,

    #[arg(
        long = "persistent-replica-count",
        default_value_t = default_dfstore_persistent_replica_count(),
        help = "Specify the replica count of the persistent cache task"
    )]
    persistent_replica_count: u64,

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
}

/// Implement the execute for ImportCommand.
impl ImportCommand {
    /// Executes the import sub command with comprehensive validation and error handling.
    ///
    /// This function serves as the main entry point for the dfstore import command execution.
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

    /// Executes the storage import operation by uploading a file to the persistent system.
    ///
    /// This function handles the core import functionality by copying a local file to the
    /// dfdaemon persistent system and provides persistence by copying data to object storage.
    /// It provides visual feedback through a progress spinner, converts the file path to
    /// absolute format, and configures the cache task with specified parameters including TTL,
    /// replica count, and piece length. The operation is asynchronous and provides completion
    /// feedback with the generated task ID.
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

        dfdaemon_download_client
            .upload_persistent_task(UploadPersistentTaskRequest {
                url: self.url.to_string(),
                object_storage: Some(ObjectStorage {
                    region: self.storage_region.clone(),
                    endpoint: self.storage_endpoint.clone(),
                    access_key_id: self.storage_access_key_id.clone(),
                    access_key_secret: self.storage_access_key_secret.clone(),
                    security_token: self.storage_security_token.clone(),
                    session_token: self.storage_session_token.clone(),
                    credential_path: self.storage_credential_path.clone(),
                    predefined_acl: self.storage_predefined_acl.clone(),
                }),
                path: absolute_path.to_string_lossy().to_string(),
                persistent_replica_count: self.persistent_replica_count,
                ttl: Some(
                    prost_wkt_types::Duration::try_from(self.ttl).or_err(ErrorType::ParseError)?,
                ),
                timeout: Some(
                    prost_wkt_types::Duration::try_from(self.timeout)
                        .or_err(ErrorType::ParseError)?,
                ),
                remote_ip: preferred_local_ip(),
            })
            .await?;

        progress_bar.finish_with_message(format!("Done: {}", self.url));
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

        Ok(())
    }
}
