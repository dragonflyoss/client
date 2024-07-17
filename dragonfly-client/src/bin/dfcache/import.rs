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

use clap::Parser;
use dragonfly_api::dfdaemon::v2::UploadCacheTaskRequest;
use dragonfly_client::grpc::dfdaemon_download::DfdaemonDownloadClient;
use dragonfly_client::grpc::health::HealthClient;
use dragonfly_client_config::{
    default_piece_length, dfcache::default_dfcache_persistent_replica_count,
};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::{Path, PathBuf};
use std::time::Duration;
use termion::{color, style};
use tracing::error;

// DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL is the default steady tick interval of progress bar.
const DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL: Duration = Duration::from_millis(80);

// ImportCommand is the subcommand of import.
#[derive(Debug, Clone, Parser)]
pub struct ImportCommand {
    #[arg(help = "Specify the path of the file to import")]
    path: PathBuf,

    #[arg(
        long = "persistent-replica-count",
        default_value_t = default_dfcache_persistent_replica_count(),
        help = "Specify the replica count of the persistent cache task"
    )]
    persistent_replica_count: u64,

    #[arg(
        long = "application",
        required = false,
        help = "Caller application which is used for statistics and access control"
    )]
    application: Option<String>,

    #[arg(
        long = "tag",
        required = false,
        help = "Different tags for the same file will be divided into different cache tasks"
    )]
    tag: Option<String>,

    #[arg(
        long = "piece-length",
        default_value_t = default_piece_length(),
        help = "Specify the byte length of the piece"
    )]
    piece_length: u64,

    #[arg(
        long = "ttl",
        value_parser= humantime::parse_duration,
        default_value = "1h",
        help = "Specify the ttl of the cache task, maximum is 7d and minimum is 1m"
    )]
    ttl: Duration,

    #[arg(
        long = "timeout",
        value_parser= humantime::parse_duration,
        default_value = "30m",
        help = "Specify the timeout for importing a file"
    )]
    timeout: Duration,
}

// Implement the execute for ImportCommand.
impl ImportCommand {
    // execute executes the import sub command.
    pub async fn execute(&self, endpoint: &Path) -> Result<()> {
        // Run import sub command.
        if let Err(err) = self.run(endpoint).await {
            match err {
                Error::TonicStatus(status) => {
                    eprintln!(
                        "{}{}{}Importing Failed!{}",
                        color::Fg(color::Red),
                        style::Italic,
                        style::Bold,
                        style::Reset,
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

                    eprintln!(
                        "{}{}{}Details:{} {}",
                        color::Fg(color::Cyan),
                        style::Italic,
                        style::Bold,
                        style::Reset,
                        std::str::from_utf8(status.details()).unwrap()
                    );

                    eprintln!(
                        "{}{}{}*********************************{}",
                        color::Fg(color::Black),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );
                }
                err => {
                    eprintln!(
                        "{}{}{}Importing Failed!{}",
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

    // run runs the import sub command.
    async fn run(&self, endpoint: &Path) -> Result<()> {
        let dfdaemon_download_client = self
            .get_dfdaemon_download_client(endpoint.to_path_buf())
            .await
            .map_err(|err| {
                error!("initialize dfdaemon download client failed: {}", err);
                err
            })?;

        let pb = ProgressBar::new_spinner();
        pb.enable_steady_tick(DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL);
        pb.set_style(
            ProgressStyle::with_template("{spinner:.blue} {msg}")
                .unwrap()
                .tick_strings(&["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"]),
        );
        pb.set_message("Importing...");

        dfdaemon_download_client
            .upload_cache_task(UploadCacheTaskRequest {
                path: self.path.clone().into_os_string().into_string().unwrap(),
                persistent_replica_count: self.persistent_replica_count,
                tag: self.tag.clone(),
                application: self.application.clone(),
                piece_length: self.piece_length,
                ttl: Some(
                    prost_wkt_types::Duration::try_from(self.ttl).or_err(ErrorType::ParseError)?,
                ),
                timeout: Some(
                    prost_wkt_types::Duration::try_from(self.timeout)
                        .or_err(ErrorType::ParseError)?,
                ),
            })
            .await?;

        pb.finish_with_message("Done");
        Ok(())
    }

    // get_and_check_dfdaemon_download_client gets a dfdaemon download client and checks its health.
    async fn get_dfdaemon_download_client(
        &self,
        endpoint: PathBuf,
    ) -> Result<DfdaemonDownloadClient> {
        // Check dfdaemon's health.
        let health_client = HealthClient::new_unix(endpoint.clone()).await?;
        health_client.check_dfdaemon_download().await?;

        // Get dfdaemon download client.
        let dfdaemon_download_client = DfdaemonDownloadClient::new_unix(endpoint).await?;
        Ok(dfdaemon_download_client)
    }
}
