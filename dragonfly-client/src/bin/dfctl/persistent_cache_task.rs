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

use chrono::{DateTime, Local};
use clap::{Parser, Subcommand};
use dragonfly_api::dfdaemon::v2::ListLocalPersistentCacheTasksRequest;
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::net::preferred_local_ip;
use std::path::PathBuf;
use tabled::{
    settings::{object::Rows, Alignment, Modify, Style},
    Table, Tabled,
};
use termion::{color, style};
use tracing::Level;

use super::*;

/// PersistentCacheTaskCommand is the subcommand of persistent-cache-task.
#[derive(Debug, Clone, Parser)]
pub struct PersistentCacheTaskCommand {
    #[command(subcommand)]
    pub subcommand: PersistentCacheTaskSubCommand,
}

/// PersistentCacheTaskSubCommand is the subcommand of persistent-cache-task.
#[derive(Debug, Clone, Subcommand)]
pub enum PersistentCacheTaskSubCommand {
    #[command(
        name = "ls",
        author,
        version,
        about = "List persistent cache tasks",
        long_about = "List all persistent cache tasks managed by the local dfdaemon."
    )]
    Ls(LsCommand),
}

/// Implement the execute for PersistentCacheTaskCommand.
impl PersistentCacheTaskCommand {
    pub async fn execute(self) -> Result<()> {
        match self.subcommand {
            PersistentCacheTaskSubCommand::Ls(cmd) => cmd.execute().await,
        }
    }
}

/// LsCommand is the subcommand of persistent-cache-task ls.
#[derive(Debug, Clone, Parser)]
pub struct LsCommand {
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

    #[arg(long, default_value_t = false, help = "Specify whether to print log")]
    console: bool,
}

/// Implement the execute for LsCommand.
impl LsCommand {
    /// Executes the ls command to list all persistent cache tasks.
    pub async fn execute(&self) -> Result<()> {
        // Initialize tracing.
        let _guards = init_command_tracing(self.log_level, self.console);

        // Get dfdaemon download client.
        let dfdaemon_download_client =
            match get_dfdaemon_download_client(self.endpoint.clone()).await {
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
                        "{}{}{}Message:{} can not connect {}, please check the unix socket {}",
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

        // Run ls sub command.
        if let Err(err) = self.run(dfdaemon_download_client).await {
            match err {
                Error::TonicStatus(status) => {
                    println!(
                        "{}{}{}Listing Persistent Cache Tasks Failed!{}",
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
                        "{}{}{}Listing Persistent Cache Tasks Failed!{}",
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

    /// Runs the ls command to retrieve and display all persistent cache tasks.
    async fn run(
        &self,
        dfdaemon_download_client: dragonfly_client::grpc::dfdaemon_download::DfdaemonDownloadClient,
    ) -> Result<()> {
        let response = dfdaemon_download_client
            .list_local_persistent_cache_tasks(ListLocalPersistentCacheTasksRequest {
                remote_ip: preferred_local_ip().map(|ip| ip.to_string()),
            })
            .await?;

        // Define the table structure for printing.
        #[derive(Debug, Default, Tabled)]
        #[tabled(rename_all = "UPPERCASE")]
        struct PersistentCacheTaskRow {
            id: String,
            persistent: bool,
            ttl: String,
            #[tabled(rename = "PIECE LENGTH")]
            piece_length: String,
            #[tabled(rename = "CONTENT LENGTH")]
            content_length: String,
            #[tabled(rename = "CREATED")]
            created_at: String,
            #[tabled(rename = "FINISHED")]
            finished_at: String,
            #[tabled(rename = "FAILED")]
            failed_at: String,
            #[tabled(rename = "UPDATED")]
            updated_at: String,
        }

        let mut rows: Vec<PersistentCacheTaskRow> = Vec::new();
        for task in response.tasks {
            let mut row = PersistentCacheTaskRow {
                id: task.task_id.clone(),
                persistent: task.persistent,
                ttl: humantime::format_duration(
                    task.ttl
                        .and_then(|d| std::time::Duration::try_from(d).ok())
                        .unwrap_or_default(),
                )
                .to_string(),
                piece_length: bytesize::to_string(task.piece_length.unwrap_or_default(), true),
                content_length: bytesize::to_string(task.content_length.unwrap_or_default(), true),
                created_at: "-".to_string(),
                finished_at: "-".to_string(),
                failed_at: "-".to_string(),
                updated_at: "-".to_string(),
            };

            // Convert created_at to human readable format.
            if let Some(ts) = task.created_at {
                if let Some(dt) = DateTime::from_timestamp(ts.seconds, ts.nanos as u32) {
                    row.created_at = dt
                        .with_timezone(&Local)
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string();
                }
            }

            // Convert finished_at to human readable format.
            if let Some(ts) = task.finished_at {
                if let Some(dt) = DateTime::from_timestamp(ts.seconds, ts.nanos as u32) {
                    row.finished_at = dt
                        .with_timezone(&Local)
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string();
                }
            }

            // Convert failed_at to human readable format.
            if let Some(ts) = task.failed_at {
                if let Some(dt) = DateTime::from_timestamp(ts.seconds, ts.nanos as u32) {
                    row.failed_at = dt
                        .with_timezone(&Local)
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string();
                }
            }

            // Convert updated_at to human readable format.
            if let Some(ts) = task.updated_at {
                if let Some(dt) = DateTime::from_timestamp(ts.seconds, ts.nanos as u32) {
                    row.updated_at = dt
                        .with_timezone(&Local)
                        .format("%Y-%m-%d %H:%M:%S")
                        .to_string();
                }
            }

            rows.push(row);
        }

        // Build and display the table.
        let mut table = Table::new(rows);
        table
            .with(Style::blank())
            .with(Modify::new(Rows::first()).with(Alignment::left()));
        println!("{table}");

        Ok(())
    }
}
