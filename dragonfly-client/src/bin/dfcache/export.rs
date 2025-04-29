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
use dragonfly_api::dfdaemon::v2::{
    download_persistent_cache_task_response, DownloadPersistentCacheTaskRequest,
};
use dragonfly_api::errordetails::v2::Backend;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use dragonfly_client_util::fs::fallocate;
use indicatif::{ProgressBar, ProgressState, ProgressStyle};
use path_absolutize::*;
use std::path::{Path, PathBuf};
use std::time::Duration;
use std::{cmp::min, fmt::Write};
use termion::{color, style};
use tokio::fs::{self, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tracing::{debug, error, info};

use super::*;

/// ExportCommand is the subcommand of export.
#[derive(Debug, Clone, Parser)]
pub struct ExportCommand {
    #[arg(help = "Specify the persistent cache task ID to export")]
    id: String,

    #[arg(
        long = "transfer-from-dfdaemon",
        default_value_t = false,
        help = "Specify whether to transfer the content of downloading file from dfdaemon's unix domain socket. If it is true, dfcache will call dfdaemon to download the file, and dfdaemon will return the content of downloading file to dfcache via unix domain socket, and dfcache will copy the content to the output path. If it is false, dfdaemon will download the file and hardlink or copy the file to the output path."
    )]
    transfer_from_dfdaemon: bool,

    #[arg(
        long = "force-hard-link",
        default_value_t = false,
        help = "Specify whether the download file must be hard linked to the output path. If hard link is failed, download will be failed. If it is false, dfdaemon will copy the file to the output path if hard link is failed."
    )]
    force_hard_link: bool,

    #[arg(
        long = "application",
        default_value = "",
        help = "Caller application which is used for statistics and access control"
    )]
    application: String,

    #[arg(
        long = "tag",
        default_value = "",
        help = "Different tags for the same file will be divided into different persistent cache tasks"
    )]
    tag: String,

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

/// Implement the execute for ExportCommand.
impl ExportCommand {
    /// execute executes the export command.
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

        // Run export command.
        if let Err(err) = self.run(dfdaemon_download_client).await {
            match err {
                Error::TonicStatus(status) => {
                    let details = status.details();
                    if let Ok(backend_err) = serde_json::from_slice::<Backend>(details) {
                        println!(
                            "{}{}{}Exporting Failed!{}",
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
                            "{}{}{}Exporting Failed!{}",
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
                        "{}{}{}Exporting Failed!{}",
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
                        "{}{}{}Exporting Failed!{}",
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

    /// run runs the export command.
    async fn run(&self, dfdaemon_download_client: DfdaemonDownloadClient) -> Result<()> {
        // Dfcache needs to notify dfdaemon to transfer the piece content of downloading file via unix domain socket
        // when the `transfer_from_dfdaemon` is true. Otherwise, dfdaemon will download the file and hardlink or
        // copy the file to the output path.
        let (output_path, need_piece_content) = if self.transfer_from_dfdaemon {
            (None, true)
        } else {
            let absolute_path = Path::new(&self.output).absolutize()?;
            info!("export file to: {}", absolute_path.to_string_lossy());
            (Some(absolute_path.to_string_lossy().to_string()), false)
        };

        // Create dfdaemon client.
        let response = dfdaemon_download_client
            .download_persistent_cache_task(DownloadPersistentCacheTaskRequest {
                task_id: self.id.clone(),
                // When scheduler triggers the export task, it will set true. If the export task is
                // triggered by the user, it will set false.
                persistent: false,
                tag: Some(self.tag.clone()),
                application: Some(self.application.clone()),
                output_path,
                timeout: Some(
                    prost_wkt_types::Duration::try_from(self.timeout)
                        .or_err(ErrorType::ParseError)?,
                ),
                need_piece_content,
                force_hard_link: self.force_hard_link,
            })
            .await
            .inspect_err(|err| {
                error!("download persistent cache task failed: {}", err);
            })?;

        // If transfer_from_dfdaemon is true, then dfcache needs to create the output file and write the
        // piece content to the output file.
        let mut f = if self.transfer_from_dfdaemon {
            if let Some(parent) = self.output.parent() {
                if !parent.exists() {
                    fs::create_dir_all(parent).await.inspect_err(|err| {
                        error!("failed to create directory {:?}: {}", parent, err);
                    })?;
                }
            }

            let f = OpenOptions::new()
                .create_new(true)
                .write(true)
                .mode(dfcache::DEFAULT_OUTPUT_FILE_MODE)
                .open(&self.output)
                .await
                .inspect_err(|err| {
                    error!("open file {:?} failed: {}", self.output, err);
                })?;

            Some(f)
        } else {
            None
        };

        // Initialize progress bar.
        let progress_bar = ProgressBar::new(0);
        progress_bar.set_style(
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
        while let Some(message) = out_stream.message().await.inspect_err(|err| {
            error!("get message failed: {}", err);
        })? {
            match message.response {
                Some(download_persistent_cache_task_response::Response::DownloadPersistentCacheTaskStartedResponse(
                    response,
                )) => {
                    if let Some(f) = &f {
                        fallocate(f, response.content_length)
                            .await
                            .inspect_err(|err| {
                                error!("fallocate {:?} failed: {}", self.output, err);
                            })?;
                    }

                    progress_bar.set_length(response.content_length);
                }
                Some(download_persistent_cache_task_response::Response::DownloadPieceFinishedResponse(
                    response,
                )) => {
                    let piece = response.piece.ok_or(Error::InvalidParameter)?;

                    // Dfcache needs to write the piece content to the output file.
                    if let Some(f) = &mut f {
                        f.seek(SeekFrom::Start(piece.offset))
                            .await
                            .inspect_err(|err| {
                                error!("seek {:?} failed: {}", self.output, err);
                            })?;

                        let content = piece.content.ok_or(Error::InvalidParameter)?;
                        f.write_all(&content).await.inspect_err(|err| {
                            error!("write {:?} failed: {}", self.output, err);
                        })?;

                        debug!("copy piece {} to {:?} success", piece.number, self.output);
                    };

                    downloaded += piece.length;
                    let position = min(downloaded + piece.length, progress_bar.length().unwrap_or(0));
                    progress_bar.set_position(position);
                }
                None => {}
            }
        }

        progress_bar.finish_with_message("downloaded");
        Ok(())
    }

    /// validate_args validates the command line arguments.
    fn validate_args(&self) -> Result<()> {
        let absolute_path = Path::new(&self.output).absolutize()?;
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
                    self.output.to_string_lossy()
                )));
            }
        }

        if absolute_path.exists() {
            return Err(Error::ValidationError(format!(
                "output path {} is already exist",
                self.output.to_string_lossy()
            )));
        }

        Ok(())
    }
}
