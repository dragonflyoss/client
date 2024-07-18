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
use dragonfly_api::dfdaemon::v2::StatCacheTaskRequest;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use humantime::format_duration;
use std::path::Path;
use std::time::Duration;
use tabled::{
    settings::{object::Rows, Alignment, Modify, Style},
    Table, Tabled,
};
use termion::{color, style};
use tracing::error;

use super::*;

// StatCommand is the subcommand of stat.
#[derive(Debug, Clone, Parser)]
pub struct StatCommand {
    #[arg(help = "Specify the cache task ID to stat")]
    id: String,
}

// Implement the execute for StatCommand.
impl StatCommand {
    // execute executes the stat command.
    pub async fn execute(&self, endpoint: &Path) -> Result<()> {
        // Run stat sub command.
        if let Err(err) = self.run(endpoint).await {
            match err {
                Error::TonicStatus(status) => {
                    eprintln!(
                        "{}{}{}Stating Failed!{}",
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
                        "{}{}{}Stating Failed!{}",
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

    // run runs the stat command.
    async fn run(&self, endpoint: &Path) -> Result<()> {
        let dfdaemon_download_client = get_dfdaemon_download_client(endpoint.to_path_buf())
            .await
            .map_err(|err| {
            error!("initialize dfdaemon download client failed: {}", err);
            err
        })?;

        let task = dfdaemon_download_client
            .stat_cache_task(StatCacheTaskRequest {
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
            #[tabled(rename = "REPLICA COUNT")]
            replica_count: u64,
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
            replica_count: task.replica_count,
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
