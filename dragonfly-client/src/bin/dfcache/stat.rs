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
use clap::Parser;
use dragonfly_api::dfdaemon::v2::StatPersistentCacheTaskRequest;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use humantime::format_duration;
use std::time::Duration;
use tabled::{
    settings::{object::Rows, Alignment, Modify, Style},
    Table, Tabled,
};
use termion::{color, style};

use super::*;

/// StatCommand is the subcommand of stat.
#[derive(Debug, Clone, Parser)]
pub struct StatCommand {
    #[arg(help = "Specify the persistent cache task ID to stat")]
    id: String,

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

    #[arg(long, default_value_t = false, help = "Specify whether to print log")]
    console: bool,
}

/// Implement the execute for StatCommand.
impl StatCommand {
    /// execute executes the stat command.
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
            None,
            false,
            self.console,
        );

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

        // Run stat sub command.
        if let Err(err) = self.run(dfdaemon_download_client).await {
            match err {
                Error::TonicStatus(status) => {
                    println!(
                        "{}{}{}Stating Failed!{}",
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
                        "{}{}{}Stating Failed!{}",
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

    /// run runs the stat command.
    async fn run(&self, dfdaemon_download_client: DfdaemonDownloadClient) -> Result<()> {
        let task = dfdaemon_download_client
            .stat_persistent_cache_task(StatPersistentCacheTaskRequest {
                task_id: self.id.clone(),
            })
            .await?;

        // Define the table struct for printing.
        #[derive(Debug, Default, Tabled)]
        #[tabled(rename_all = "UPPERCASE")]
        struct TableTask {
            id: String,
            state: String,
            #[tabled(rename = "CONTENT LENGTH")]
            content_length: String,
            #[tabled(rename = "PIECE LENGTH")]
            piece_length: String,
            #[tabled(rename = "PERSISTENT REPLICA COUNT")]
            persistent_replica_count: u64,
            ttl: String,
            #[tabled(rename = "CREATED")]
            created_at: String,
            #[tabled(rename = "UPDATED")]
            updated_at: String,
        }

        let mut table_task = TableTask {
            id: task.id,
            state: task.state,
            // Convert content_length to human readable format.
            content_length: bytesize::to_string(task.content_length, true),
            // Convert piece_length to human readable format.
            piece_length: bytesize::to_string(task.piece_length, true),
            persistent_replica_count: task.persistent_replica_count,
            ..Default::default()
        };

        // Convert ttl to human readable format.
        let ttl = Duration::try_from(task.ttl.ok_or(Error::InvalidParameter)?)
            .or_err(ErrorType::ParseError)?;
        table_task.ttl = format_duration(ttl).to_string();

        // Convert created_at to human readable format.
        if let Some(created_at) = task.created_at {
            if let Some(date_time) =
                DateTime::from_timestamp(created_at.seconds, created_at.nanos as u32)
            {
                table_task.created_at = date_time
                    .with_timezone(&Local)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string();
            }
        }

        // Convert updated_at to human readable format.
        if let Some(updated_at) = task.updated_at {
            if let Some(date_time) =
                DateTime::from_timestamp(updated_at.seconds, updated_at.nanos as u32)
            {
                table_task.updated_at = date_time
                    .with_timezone(&Local)
                    .format("%Y-%m-%d %H:%M:%S")
                    .to_string();
            }
        }

        // Create a table and print it.
        let mut table = Table::new(vec![table_task]);
        table
            .with(Style::blank())
            .with(Modify::new(Rows::first()).with(Alignment::center()));
        println!("{table}");

        Ok(())
    }
}
