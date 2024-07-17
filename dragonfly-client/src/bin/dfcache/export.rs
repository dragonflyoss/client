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
use dragonfly_api::dfdaemon::v2::{download_cache_task_response, DownloadCacheTaskRequest};
use dragonfly_api::errordetails::v2::Backend;
use dragonfly_client_config::default_piece_length;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{cmp::min, fmt::Write};
use termion::{color, style};
use tracing::error;

use super::*;

// ExportCommand is the subcommand of export.
#[derive(Debug, Clone, Parser)]
pub struct ExportCommand {
    #[arg(help = "Specify the cache task ID to export")]
    id: String,

    #[arg(
        long = "application",
        default_value = "",
        help = "Caller application which is used for statistics and access control"
    )]
    application: String,

    #[arg(
        long = "tag",
        default_value = "",
        help = "Different tags for the same file will be divided into different cache tasks"
    )]
    tag: String,

    #[arg(
        long = "piece-length",
        default_value_t = default_piece_length(),
        help = "Specify the byte length of the piece"
    )]
    piece_length: u64,

    #[arg(
        short = 'O',
        long = "output",
        help = "Specify the output path of exporting file"
    )]
    output: PathBuf,

    #[arg(
        long = "timeout",
        value_parser= humantime::parse_duration,
        default_value = "2h",
        help = "Specify the timeout for exporting a file"
    )]
    timeout: Duration,
}

// Implement the execute for ExportCommand.
impl ExportCommand {
    // execute executes the export command.
    pub async fn execute(&self, endpoint: &Path) -> Result<()> {
        // Run export command.
        if let Err(err) = self.run(endpoint).await {
            match err {
                Error::TonicStatus(status) => {
                    let details = status.details();
                    if let Ok(backend_err) = serde_json::from_slice::<Backend>(details) {
                        eprintln!(
                            "{}{}{}Exporting Failed!{}",
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
                            "{}{}{}Exporting Failed!{}",
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
                        "{}{}{}Exporting Failed!{}",
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
                        "{}{}{}Exporting Failed!{}",
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

    // run runs the export command.
    async fn run(&self, endpoint: &Path) -> Result<()> {
        let dfdaemon_download_client = get_dfdaemon_download_client(endpoint.to_path_buf())
            .await
            .map_err(|err| {
            error!("initialize dfdaemon download client failed: {}", err);
            err
        })?;

        // Create dfdaemon client.
        let response = dfdaemon_download_client
            .download_cache_task(DownloadCacheTaskRequest {
                task_id: self.id.clone(),
                // When scheduler triggers the export task, it will set true. If the export task is
                // triggered by the user, it will set false.
                persistent: false,
                tag: Some(self.tag),
                application: Some(self.application),
                piece_length: self.piece_length,
                output_path: self
                    .output
                    .to_str()
                    .ok_or(Error::InvalidParameter)?
                    .to_string(),
                timeout: Some(
                    prost_wkt_types::Duration::try_from(self.timeout)
                        .or_err(ErrorType::ParseError)?,
                ),
            })
            .await
            .map_err(|err| {
                error!("download cache task failed: {}", err);
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
                Some(download_cache_task_response::Response::DownloadCacheTaskStartedResponse(
                    response,
                )) => {
                    pb.set_length(response.content_length);
                }
                Some(download_cache_task_response::Response::DownloadPieceFinishedResponse(
                    response,
                )) => {
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
}
