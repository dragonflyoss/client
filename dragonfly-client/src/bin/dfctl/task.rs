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
use dragonfly_api::dfdaemon::v2::{DeleteTaskRequest, ListLocalTasksRequest};
use dragonfly_api::scheduler::v2::{
    scheduler_client::SchedulerClient as SchedulerGRPCClient, PreheatFileRequest,
    PreheatImageRequest,
};
use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::net::preferred_local_ip;
use dragonfly_client_util::request::{GetRequest, PreheatRequest, Proxy, Request};
use oci_client::Reference;
use std::path::PathBuf;
use std::time::Duration;
use tabled::{
    settings::{object::Rows, Alignment, Modify, Style},
    Table, Tabled,
};
use termion::{color, style};
use tonic::transport::Channel;
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

    #[command(
        name = "preheat",
        author,
        version,
        about = "Preheat an image or file",
        long_about = "Preheat an OCI image or file via the Dragonfly scheduler. Use oci:// prefix for images \
        (e.g., oci://docker.io/library/nginx:latest) and http:// or https:// for files \
        (e.g., https://example.com/file.tar.gz). By default, uses scheduler gRPC directly. \
        Use --request-sdk to preheat via the Dragonfly SDK proxy instead."
    )]
    Preheat(PreheatCommand),
}

/// Implement the execute for TaskCommand.
impl TaskCommand {
    pub async fn execute(self) -> Result<()> {
        match self.subcommand {
            TaskSubCommand::Ls(cmd) => cmd.execute().await,
            TaskSubCommand::Rm(cmd) => cmd.execute().await,
            TaskSubCommand::Preheat(cmd) => cmd.execute().await,
        }
    }
}

/// LsCommand is the subcommand of task ls.
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
    /// Executes the ls command to list all tasks.
    ///
    /// This function lists all tasks managed by the local dfdaemon. It connects to the
    /// dfdaemon gRPC server and retrieves the list of tasks, then displays them in a
    /// tabular format.
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
        let response = dfdaemon_download_client
            .list_local_tasks(ListLocalTasksRequest {
                remote_ip: preferred_local_ip().map(|ip| ip.to_string()),
            })
            .await?;

        // Define the table structure for printing.
        #[derive(Debug, Default, Tabled)]
        #[tabled(rename_all = "UPPERCASE")]
        struct TaskRow {
            id: String,
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

        let mut rows: Vec<TaskRow> = Vec::new();
        for task in response.tasks {
            let mut row = TaskRow {
                id: task.task_id.clone(),
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

/// RmCommand is the subcommand of task rm.
#[derive(Debug, Clone, Parser)]
pub struct RmCommand {
    #[arg(help = "Specify the task ID to remove")]
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

    #[arg(long, default_value_t = false, help = "Specify whether to print log")]
    console: bool,
}

/// Implement the execute for RmCommand.
impl RmCommand {
    /// Executes the rm command to remove a task.
    ///
    /// This function serves as the main entry point for the dfctl task rm command execution.
    /// It handles the complete lifecycle including argument parsing, logging initialization,
    /// dfdaemon client setup, and command execution with detailed error reporting.
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
            "{}{}{}Task Removed!{}",
            color::Fg(color::Green),
            style::Italic,
            style::Bold,
            style::Reset
        );

        Ok(())
    }
}

/// PreheatCommand is the subcommand of task preheat.
#[derive(Debug, Clone, Parser)]
pub struct PreheatCommand {
    #[arg(
        help = "URL to preheat. Use oci:// prefix for images (e.g., oci://docker.io/library/nginx:latest), \
        http:// or https:// for files (e.g., https://example.com/file.tar.gz)"
    )]
    url: String,

    #[arg(
        short = 's',
        long = "scheduler",
        default_value = "http://127.0.0.1:8002",
        help = "Scheduler endpoint address"
    )]
    scheduler: String,

    #[arg(
        long = "request-sdk",
        default_value_t = false,
        help = "Use SDK mode for preheat via the Dragonfly proxy"
    )]
    request_sdk: bool,

    #[arg(long, help = "Platform for image preheat (e.g., linux/amd64)")]
    platform: Option<String>,

    #[arg(long, help = "Username for registry authentication")]
    username: Option<String>,

    #[arg(long, help = "Password for registry authentication")]
    password: Option<String>,

    #[arg(long, help = "Piece length in bytes")]
    piece_length: Option<u64>,

    #[arg(long, help = "Tag for task identification")]
    tag: Option<String>,

    #[arg(long, help = "Application for task identification")]
    application: Option<String>,

    #[arg(
        long,
        help = "Timeout in seconds, default 1800 for gRPC mode, 300 for SDK mode"
    )]
    timeout: Option<u64>,

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

/// Implement the execute for PreheatCommand.
impl PreheatCommand {
    /// Executes the preheat command to preheat an image or file.
    ///
    /// This function preheats content via the Dragonfly scheduler. It supports two modes:
    /// - gRPC mode (default): directly calls the scheduler's preheat RPC
    /// - SDK mode (--request-sdk): uses the Dragonfly SDK proxy for preheat
    pub async fn execute(&self) -> Result<()> {
        // Initialize tracing.
        let _guards = init_command_tracing(self.log_level, self.console);

        // Run preheat sub command.
        if let Err(err) = self.run().await {
            match err {
                Error::TonicStatus(status) => {
                    println!(
                        "{}{}{}Preheat Failed!{}",
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
                        "{}{}{}Preheat Failed!{}",
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

    /// Runs the preheat command by detecting the URL type and dispatching to the appropriate
    /// preheat handler.
    async fn run(&self) -> Result<()> {
        if self.url.starts_with("oci://") {
            if self.request_sdk {
                self.preheat_image_sdk().await
            } else {
                self.preheat_image_grpc().await
            }
        } else if self.url.starts_with("http://") || self.url.starts_with("https://") {
            if self.request_sdk {
                self.preheat_file_sdk().await
            } else {
                self.preheat_file_grpc().await
            }
        } else {
            Err(Error::Unknown(format!(
                "unsupported URL scheme: {}. Use oci:// for images or http(s):// for files",
                self.url
            )))
        }
    }

    /// Preheats an OCI image via the scheduler's gRPC PreheatImage RPC.
    async fn preheat_image_grpc(&self) -> Result<()> {
        let image_ref = self.url.strip_prefix("oci://").ok_or_else(|| {
            Error::Unknown("URL must start with oci:// for image preheat".to_string())
        })?;

        // Parse the OCI reference to construct a manifest URL.
        let reference: Reference = image_ref
            .parse()
            .map_err(|err| Error::Unknown(format!("invalid OCI image reference: {}", err)))?;

        let registry = reference.resolve_registry();
        let repository = reference.repository();
        let tag = reference.tag().unwrap_or("latest");
        let manifest_url = format!("https://{}/v2/{}/manifests/{}", registry, repository, tag);

        // Connect to the scheduler gRPC endpoint.
        let channel = Channel::from_shared(self.scheduler.clone())
            .map_err(|_| Error::InvalidURI(self.scheduler.clone()))?
            .connect()
            .await?;
        let mut client = SchedulerGRPCClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);

        let timeout = self.timeout.unwrap_or(1800);
        let request = PreheatImageRequest {
            url: manifest_url,
            piece_length: self.piece_length,
            tag: self.tag.clone(),
            application: self.application.clone(),
            platform: self.platform.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            timeout: Some(prost_wkt_types::Duration {
                seconds: timeout as i64,
                nanos: 0,
            }),
            ..Default::default()
        };

        client.preheat_image(request).await?;

        println!(
            "{}{}{}Preheat Succeeded!{}",
            color::Fg(color::Green),
            style::Italic,
            style::Bold,
            style::Reset
        );
        println!(
            "{}{}{}Image:{} {}",
            color::Fg(color::Cyan),
            style::Italic,
            style::Bold,
            style::Reset,
            self.url
        );

        Ok(())
    }

    /// Preheats a file via the scheduler's gRPC PreheatFile RPC.
    async fn preheat_file_grpc(&self) -> Result<()> {
        // Connect to the scheduler gRPC endpoint.
        let channel = Channel::from_shared(self.scheduler.clone())
            .map_err(|_| Error::InvalidURI(self.scheduler.clone()))?
            .connect()
            .await?;
        let mut client = SchedulerGRPCClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);

        let timeout = self.timeout.unwrap_or(1800);
        let request = PreheatFileRequest {
            url: self.url.clone(),
            piece_length: self.piece_length,
            tag: self.tag.clone(),
            application: self.application.clone(),
            timeout: Some(prost_wkt_types::Duration {
                seconds: timeout as i64,
                nanos: 0,
            }),
            ..Default::default()
        };

        client.preheat_file(request).await?;

        println!(
            "{}{}{}Preheat Succeeded!{}",
            color::Fg(color::Green),
            style::Italic,
            style::Bold,
            style::Reset
        );
        println!(
            "{}{}{}File:{} {}",
            color::Fg(color::Cyan),
            style::Italic,
            style::Bold,
            style::Reset,
            self.url
        );

        Ok(())
    }

    /// Preheats an OCI image via the Dragonfly SDK proxy.
    async fn preheat_image_sdk(&self) -> Result<()> {
        let image_ref = self.url.strip_prefix("oci://").ok_or_else(|| {
            Error::Unknown("URL must start with oci:// for image preheat".to_string())
        })?;

        let timeout = Duration::from_secs(self.timeout.unwrap_or(300));

        let proxy = Proxy::builder()
            .scheduler_endpoint(self.scheduler.clone())
            .scheduler_request_timeout(Duration::from_secs(5))
            .health_check_interval(Duration::from_secs(60))
            .max_retries(3)
            .build()
            .await
            .map_err(|err| Error::Unknown(format!("failed to build proxy: {}", err)))?;

        let request = PreheatRequest {
            image: image_ref.to_string(),
            platform: self.platform.clone(),
            username: self.username.clone(),
            password: self.password.clone(),
            piece_length: self.piece_length,
            tag: self.tag.clone(),
            application: self.application.clone(),
            timeout,
            ..Default::default()
        };

        proxy
            .preheat(&request)
            .await
            .map_err(|err| Error::Unknown(format!("preheat failed: {}", err)))?;

        println!(
            "{}{}{}Preheat Succeeded!{}",
            color::Fg(color::Green),
            style::Italic,
            style::Bold,
            style::Reset
        );
        println!(
            "{}{}{}Image:{} {}",
            color::Fg(color::Cyan),
            style::Italic,
            style::Bold,
            style::Reset,
            self.url
        );

        Ok(())
    }

    /// Preheats a file via the Dragonfly SDK proxy.
    async fn preheat_file_sdk(&self) -> Result<()> {
        let timeout = Duration::from_secs(self.timeout.unwrap_or(300));

        let proxy = Proxy::builder()
            .scheduler_endpoint(self.scheduler.clone())
            .scheduler_request_timeout(Duration::from_secs(5))
            .health_check_interval(Duration::from_secs(60))
            .max_retries(3)
            .build()
            .await
            .map_err(|err| Error::Unknown(format!("failed to build proxy: {}", err)))?;

        let request = GetRequest {
            url: self.url.clone(),
            piece_length: self.piece_length,
            tag: self.tag.clone(),
            application: self.application.clone(),
            timeout,
            ..Default::default()
        };

        let response = proxy
            .get(&request)
            .await
            .map_err(|err| Error::Unknown(format!("preheat failed: {}", err)))?;

        if let Some(mut reader) = response.reader {
            tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
        }

        println!(
            "{}{}{}Preheat Succeeded!{}",
            color::Fg(color::Green),
            style::Italic,
            style::Bold,
            style::Reset
        );
        println!(
            "{}{}{}File:{} {}",
            color::Fg(color::Cyan),
            style::Italic,
            style::Bold,
            style::Reset,
            self.url
        );

        Ok(())
    }
}
