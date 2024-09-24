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
use dragonfly_api::dfdaemon::v2::DeleteCacheTaskRequest;
use dragonfly_client_core::{Error, Result};
use indicatif::{ProgressBar, ProgressStyle};
use std::path::Path;
use std::time::Duration;
use termion::{color, style};

use super::*;

/// DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL is the default steady tick interval of progress bar.
const DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL: Duration = Duration::from_millis(80);

/// RemoveCommand is the subcommand of remove.
#[derive(Debug, Clone, Parser)]
pub struct RemoveCommand {
    #[arg(help = "Specify the cache task ID to remove")]
    id: String,
}

/// Implement the execute for RemoveCommand.
impl RemoveCommand {
    /// execute executes the delete command.
    pub async fn execute(&self, endpoint: &Path) -> Result<()> {
        // Get dfdaemon download client.
        let dfdaemon_download_client =
            match get_dfdaemon_download_client(endpoint.to_path_buf()).await {
                Ok(client) => client,
                Err(err) => {
                    eprintln!(
                        "{}{}{}Connect Dfdaemon Failed!{}",
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
                        "{}{}{}Message:{}, can not connect {}, please check the unix socket {}",
                        color::Fg(color::Cyan),
                        style::Italic,
                        style::Bold,
                        style::Reset,
                        err,
                        endpoint.to_string_lossy(),
                    );

                    eprintln!(
                        "{}{}{}****************************************{}",
                        color::Fg(color::Black),
                        style::Italic,
                        style::Bold,
                        style::Reset
                    );

                    std::process::exit(1);
                }
            };

        // Run delete sub command.
        if let Err(err) = self.run(dfdaemon_download_client).await {
            match err {
                Error::TonicStatus(status) => {
                    eprintln!(
                        "{}{}{}Removing Failed!{}",
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
                        "{}{}{}Removing Failed!{}",
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

    /// run runs the delete command.
    async fn run(&self, dfdaemon_download_client: DfdaemonDownloadClient) -> Result<()> {
        let pb = ProgressBar::new_spinner();
        pb.enable_steady_tick(DEFAULT_PROGRESS_BAR_STEADY_TICK_INTERVAL);
        pb.set_style(
            ProgressStyle::with_template("{spinner:.blue} {msg}")
                .unwrap()
                .tick_strings(&["⣾", "⣽", "⣻", "⢿", "⡿", "⣟", "⣯", "⣷"]),
        );
        pb.set_message("Removing...");

        dfdaemon_download_client
            .delete_cache_task(DeleteCacheTaskRequest {
                task_id: self.id.clone(),
            })
            .await?;

        pb.finish_with_message("Done");
        Ok(())
    }
}
