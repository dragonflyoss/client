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
use dragonfly_api::common::v2::Task;
use dragonfly_api::dfdaemon::v2::DeleteTaskRequest;
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

/// TaskCommand is the subcommand of task.
#[derive(Debug, Clone, Parser)]
pub struct TaskCommand {
    #[command(subcommand)]
    pub subcommand: TaskSubCommand,
}

/// TaskSubCommand is the subcommand of task.
#[derive(Debug, Clone, Subcommand)]
pub enum TaskSubCommand {
    #[command(
        name = "ls",
        author,
        version,
        about = "List tasks",
        long_about = "List all tasks managed by the local dfdaemon."
    )]
    Ls(LsCommand),

    #[command(
        name = "rm",
        author,
        version,
        about = "Remove a task",
        long_about = "Remove a specific task by ID from the local dfdaemon."
    )]
    Rm(RmCommand),
}

/// Implement the execute for TaskCommand.
impl TaskCommand {
    pub async fn execute(self, endpoint: PathBuf, log_level: Level, console: bool) -> Result<()> {
        match self.subcommand {
            TaskSubCommand::Ls(cmd) => cmd.execute(endpoint, log_level, console).await,
            TaskSubCommand::Rm(cmd) => cmd.execute(endpoint, log_level, console).await,
        }
    }
}

/// LsCommand is the subcommand of task ls.
#[derive(Debug, Clone, Parser)]
pub struct LsCommand {}

/// Implement the execute for LsCommand.
impl LsCommand {
    /// Executes the ls command to list all tasks.
    ///
    /// This function lists all tasks managed by the local dfdaemon. It connects to the
    /// dfdaemon gRPC server and retrieves the list of tasks, then displays them in a
    /// tabular format.
    pub async fn execute(
        &self,
        endpoint: PathBuf,
        log_level: Level,
        console: bool,
    ) -> Result<()> {
        // Initialize tracing.
        let _guards = init_command_tracing(log_level, console);

        // Get dfdaemon download client.
        let dfdaemon_download_client =
            match get_dfdaemon_download_client(endpoint.clone()).await {
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
                        endpoint.to_string_lossy(),
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
                        "{}{}{}Listing Tasks Failed!{}",
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
                        "{}{}{}Listing Tasks Failed!{}",
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

    /// Runs the ls command to retrieve and display all tasks.
    async fn run(
        &self,
        dfdaemon_download_client: dragonfly_client::grpc::dfdaemon_download::DfdaemonDownloadClient,
    ) -> Result<()> {
        let tasks = dfdaemon_download_client.get_tasks().await?;

        // Define the table structure for printing.
        #[derive(Debug, Default, Tabled)]
        #[tabled(rename_all = "UPPERCASE")]
        struct TaskRow {
            id: String,
            state: String,
            #[tabled(rename = "CONTENT LENGTH")]
            content_length: String,
            #[tabled(rename = "CREATED AT")]
            created_at: String,
            #[tabled(rename = "UPDATED AT")]
            updated_at: String,
        }

        let mut rows: Vec<TaskRow> = Vec::new();
        for task in tasks {
            let mut row = TaskRow {
                id: task.id.clone(),
                state: task.state.clone(),
                content_length: bytesize::to_string(task.content_length, true),
                ..Default::default()
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
            .with(Modify::new(Rows::first()).with(Alignment::center()));
        println!("{table}");

        Ok(())
    }
}

/// RmCommand is the subcommand of task rm.
#[derive(Debug, Clone, Parser)]
pub struct RmCommand {
    #[arg(help = "Specify the task ID to remove")]
    id: String,
}

/// Implement the execute for RmCommand.
impl RmCommand {
    /// Executes the rm command to remove a task.
    ///
    /// This function serves as the main entry point for the dfctl task rm command execution.
    /// It handles the complete lifecycle including argument parsing, logging initialization,
    /// dfdaemon client setup, and command execution with detailed error reporting.
    pub async fn execute(
        &self,
        endpoint: PathBuf,
        log_level: Level,
        console: bool,
    ) -> Result<()> {
        // Initialize tracing.
        let _guards = init_command_tracing(log_level, console);

        // Get dfdaemon download client.
        let dfdaemon_download_client =
            match get_dfdaemon_download_client(endpoint.clone()).await {
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
                        endpoint.to_string_lossy(),
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

        // Run rm sub command.
        if let Err(err) = self.run(dfdaemon_download_client).await {
            match err {
                Error::TonicStatus(status) => {
                    println!(
                        "{}{}{}Removing Task Failed!{}",
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
                        "{}{}{}Removing Task Failed!{}",
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

    /// Runs the rm command to delete a task by ID.
    async fn run(
        &self,
        dfdaemon_download_client: dragonfly_client::grpc::dfdaemon_download::DfdaemonDownloadClient,
    ) -> Result<()> {
        dfdaemon_download_client
            .delete_task(DeleteTaskRequest {
                task_id: self.id.clone(),
                remote_ip: preferred_local_ip().map(|ip| ip.to_string()),
            })
            .await?;

        println!(
            "{}{}{}Task Removed Successfully!{}",
            color::Fg(color::Green),
            style::Italic,
            style::Bold,
            style::Reset
        );

        Ok(())
    }
}
