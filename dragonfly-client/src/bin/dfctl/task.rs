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

use bytesize::ByteSize;
use chrono::{DateTime, Local};
use clap::{Parser, Subcommand};
use dragonfly_api::common::v2::{Hdfs, ObjectStorage};
use dragonfly_api::dfdaemon::v2::{DeleteTaskRequest, ListLocalTasksRequest};
use dragonfly_api::errordetails::v2::Backend;
use dragonfly_api::scheduler::v2::{
    scheduler_client::SchedulerClient as SchedulerGRPCClient, PreheatFileRequest,
    PreheatImageRequest,
};
use dragonfly_client_backend::{hdfs, object_storage, oci};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use dragonfly_client_util::{
    http::{
        header_vec_to_hashmap, header_vec_to_headermap,
        query_params::default_proxy_rule_filtered_query_params,
    },
    net::preferred_local_ip,
    request::{GetRequest, PreheatRequest, Proxy, Request},
};
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
use url::Url;

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
        (e.g., https://example.com/file.tar.gz). By default, uses scheduler gRPC directly."
    )]
    Preheat(Box<PreheatCommand>),
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
        env = "DFCTL_TASK_LS_LOG_LEVEL",
        help = "Specify the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_t = false,
        env = "DFCTL_TASK_LS_CONSOLE",
        help = "Specify whether to print log"
    )]
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
                    let details = status.details();
                    if let Ok(backend_err) = serde_json::from_slice::<Backend>(details) {
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
                            "{}{}{}Listing Tasks Failed!{}",
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
        env = "DFCTL_TASK_RM_LOG_LEVEL",
        help = "Specify the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_t = false,
        env = "DFCTL_TASK_RM_CONSOLE",
        help = "Specify whether to print log"
    )]
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
                    let details = status.details();
                    if let Ok(backend_err) = serde_json::from_slice::<Backend>(details) {
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
                            "{}{}{}Removing Task Failed!{}",
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
        help = "Specify the URL to preheat, OCI URL for images (e.g., oci://docker.io/library/nginx:latest) or HTTP(S) URL for files (e.g., https://example.com/file.tar.gz)"
    )]
    url: String,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_SCHEDULER_ENDPOINT",
        help = "Specify the Dragonfly scheduler gRPC endpoint (e.g., http://127.0.0.1:8002)"
    )]
    scheduler_endpoint: String,

    #[arg(
        long,
        default_value_t = false,
        env = "DFCTL_TASK_PREHEAT_REQUEST_SDK",
        help = "Specify whether to use request SDK mode for preheat. If not set, uses gRPC mode to call the scheduler directly. \
         If set, uses the request SDK proxy for preheat, refer to https://github.com/dragonflyoss/client/blob/main/dragonfly-client-util/src/request/mod.rs"
    )]
    request_sdk: bool,

    #[arg(
        long,
        default_value_t = true,
        env = "DFCTL_TASK_ENABLE_TASK_ID_BASED_BLOB_DIGEST",
        help = "Specify whether to generate task id based blob digest. It indicates whether to use the blob digest for task ID calculation \
         when downloading from OCI registries. When enabled for OCI blob URLs (e.g., /v2/<name>/blobs/sha256:<digest>), \
         the task ID is derived from the blob digest rather than the full URL. This enables deduplication across \
         registries - the same blob from different registries shares one task ID, eliminating redundant downloads \
         and storage"
    )]
    enable_task_id_based_blob_digest: bool,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_SCOPE",
        default_value = "all_seed_peers",
        help = "Specify the scope for preheating, only used in gRPC mode (non-request-sdk). Possible values: 'single_seed_peer' (preheat from a single seed peer), \
         'all_peers' (preheat from all available peers), 'all_seed_peers' (preheat from all seed peers)."
    )]
    scope: String,

    #[arg(
        long = "ip",
        required = false,
        env = "DFCTL_TASK_PREHEAT_IPS",
        help = "Specify a list of specific peer IPs for preheating, only used in gRPC mode (non-request-sdk). This field has the highest priority: if provided, \
         both 'count' and 'percentage' will be ignored. Applies to 'all_peers' and 'all_seed_peers' scopes. \
         Examples: --ip='192.168.1.1' --ip='192.168.1.2'"
    )]
    ips: Option<Vec<String>>,

    #[arg(
        long,
        required = false,
        env = "DFCTL_TASK_PREHEAT_PERCENTAGE",
        value_parser = clap::value_parser!(u32).range(1..=100),
        help = "Specify the percentage of available peers to preheat, only used in gRPC mode (non-request-sdk). This field has the lowest priority and is only used \
         if both 'ips' and 'count' are not provided. Must be a value between 1 and 100 (inclusive). \
         Applies to 'all_peers' and 'all_seed_peers' scopes"
    )]
    percentage: Option<u32>,

    #[arg(
        long,
        required = false,
        env = "DFCTL_TASK_PREHEAT_COUNT",
        value_parser = clap::value_parser!(u32).range(1..=200),
        help = "Specify the desired number of peers to preheat, only used in gRPC mode (non-request-sdk). This field is used only when 'ips' is not specified and \
         has priority over 'percentage'. Must be a value between 1 and 200 (inclusive). \
         Applies to 'all_peers' and 'all_seed_peers' scopes"
    )]
    count: Option<u32>,

    #[arg(
        long = "concurrent-task-count",
        required = false,
        env = "DFCTL_TASK_PREHEAT_CONCURRENT_TASK_COUNT",
        value_parser = clap::value_parser!(i64).range(1..=100),
        help = "Specify the maximum number of tasks (e.g., image layers) to preheat concurrently, only used in gRPC mode (non-request-sdk). For example, if preheating \
         100 layers with concurrent-task-count set to 10, up to 10 layers are processed simultaneously. Default is 8, maximum is 100"
    )]
    concurrent_task_count: Option<i64>,

    #[arg(
        long = "concurrent-peer-count",
        required = false,
        env = "DFCTL_TASK_PREHEAT_CONCURRENT_PEER_COUNT",
        value_parser = clap::value_parser!(i64).range(1..=1000),
        help = "Specify the maximum number of peers to preheat concurrently for a single task (e.g., an image layer), only used in gRPC mode (non-request-sdk). \
         For example, if preheating a layer with concurrent-peer-count set to 10, up to 10 peers process that layer simultaneously. \
         Default is 500, maximum is 1000"
    )]
    concurrent_peer_count: Option<i64>,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_PLATFORM",
        help = "Specify the platform for image preheat, e.g., linux/amd64 or linux/arm64"
    )]
    platform: Option<String>,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_USERNAME",
        help = "Specify the username for registry authentication"
    )]
    username: Option<String>,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_PASSWORD",
        help = "Specify the password for registry authentication"
    )]
    password: Option<String>,

    #[arg(
        long = "piece-length",
        required = false,
        env = "DFCTL_TASK_PREHEAT_PIECE_LENGTH",
        help = "Specify the piece length for downloading file. If the piece length is not specified, the piece length will be calculated according to the file size. Different piece lengths will be divided into different tasks. The value needs to be set with human readable format and needs to be greater than or equal to 4mib, for example: 4mib, 1gib"
    )]
    piece_length: Option<ByteSize>,

    #[arg(
        long = "tag",
        default_value = "",
        env = "DFCTL_TASK_PREHEAT_TAG",
        help = "Different tags for the same URL will be divided into different tasks"
    )]
    tag: Option<String>,

    #[arg(
        long = "application",
        default_value = "",
        env = "DFCTL_TASK_PREHEAT_APPLICATION",
        help = "Different applications for the same URL will be divided into different tasks"
    )]
    application: Option<String>,

    #[arg(
        long = "filtered-query-param",
        required = false,
        help = "Filter the query parameters of the downloaded URL. If the download URL is the same, it will be scheduled as the same task. Examples: --filtered-query-param='signature' --filtered-query-param='timeout'"
    )]
    filtered_query_params: Option<Vec<String>>,

    #[arg(
        short = 'H',
        long = "header",
        help = "Specify the header for downloading file. Examples: --header='Content-Type: application/json' --header='Accept: application/json'"
    )]
    header: Vec<String>,

    #[arg(
        short = 'p',
        long = "priority",
        default_value_t = 6,
        env = "DFCTL_TASK_PREHEAT_PRIORITY",
        help = "Specify the priority for scheduling task"
    )]
    priority: i32,

    #[arg(
        long,
        default_value_t = false,
        env = "DFCTL_TASK_PREHEAT_INSECURE_SKIP_VERIFY",
        help = "Specify whether to skip verify TLS certification for origin server"
    )]
    insecure_skip_verify: bool,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_STORAGE_REGION",
        help = "Specify the region for the Object Storage Service (e.g., us-east-1)"
    )]
    storage_region: Option<String>,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_STORAGE_ENDPOINT",
        help = "Specify the endpoint URL for the Object Storage Service (e.g., https://s3.amazonaws.com)"
    )]
    storage_endpoint: Option<String>,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_STORAGE_ACCESS_KEY_ID",
        help = "Specify the access key ID for authenticating with the Object Storage Service"
    )]
    storage_access_key_id: Option<String>,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_STORAGE_ACCESS_KEY_SECRET",
        help = "Specify the secret access key for authenticating with the Object Storage Service"
    )]
    storage_access_key_secret: Option<String>,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_STORAGE_SECURITY_TOKEN",
        help = "Specify the security token for the Object Storage Service"
    )]
    storage_security_token: Option<String>,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_STORAGE_INSECURE_SKIP_VERIFY",
        help = "Specify whether to skip verify TLS certification for object storage service"
    )]
    storage_insecure_skip_verify: Option<bool>,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_STORAGE_SESSION_TOKEN",
        help = "Specify the session token for Amazon Simple Storage Service(S3)"
    )]
    storage_session_token: Option<String>,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_STORAGE_CREDENTIAL_PATH",
        help = "Specify the local path to the credential file which is used for OAuth2 authentication for Google Cloud Storage Service(GCS)"
    )]
    storage_credential_path: Option<String>,

    #[arg(
        long,
        default_value = "publicRead",
        env = "DFCTL_TASK_PREHEAT_STORAGE_PREDEFINED_ACL",
        help = "Specify the predefined ACL for Google Cloud Storage Service(GCS)"
    )]
    storage_predefined_acl: Option<String>,

    #[arg(
        long,
        env = "DFCTL_TASK_PREHEAT_HDFS_DELEGATION_TOKEN",
        help = "Specify the delegation token for Hadoop Distributed File System(HDFS)"
    )]
    hdfs_delegation_token: Option<String>,

    #[arg(
        long = "timeout",
        value_parser= humantime::parse_duration,
        default_value = "2h",
        env = "DFCTL_TASK_PREHEAT_TIMEOUT",
        help = "Specify the timeout for downloading a file"
    )]
    timeout: Duration,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        env = "DFCTL_TASK_PREHEAT_LOG_LEVEL",
        help = "Specify the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_t = false,
        env = "DFCTL_TASK_PREHEAT_CONSOLE",
        help = "Specify whether to print log"
    )]
    console: bool,
}

/// Implement the execute for PreheatCommand.
impl PreheatCommand {
    /// Executes the preheat command to preheat an image or file.
    ///
    /// This function preheats content via the Dragonfly scheduler. It supports two modes:
    /// - gRPC mode (default): directly calls the scheduler's preheat RPC.
    /// - Request SDK mode (--request-sdk): uses the request SDK proxy for preheat.
    pub async fn execute(&self) -> Result<()> {
        // Initialize tracing.
        let _guards = init_command_tracing(self.log_level, self.console);

        // Run preheat sub command.
        if let Err(err) = self.run().await {
            match err {
                Error::TonicStatus(status) => {
                    let details = status.details();
                    if let Ok(backend_err) = serde_json::from_slice::<Backend>(details) {
                        println!(
                            "{}{}{}Preheating Task Failed!{}",
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
                            "{}{}{}Preheating Task Failed!{}",
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
                err => {
                    println!(
                        "{}{}{}Preheating Task Failed!{}",
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

    /// Run the preheat logic based on the URL type (image or file) and mode (gRPC or request SDK).
    async fn run(&self) -> Result<()> {
        match (self.url.starts_with(oci::SCHEME), self.request_sdk) {
            (true, true) => self.preheat_image_by_request_sdk().await,
            (true, false) => self.preheat_image().await,
            (false, true) => self.preheat_file_by_request_sdk().await,
            (false, false) => self.preheat_file().await,
        }
    }

    /// Preheats an OCI image via the scheduler's gRPC PreheatImage RPC.
    async fn preheat_image(&self) -> Result<()> {
        let reference: Reference = self
            .url
            .strip_prefix(&format!("{}://", oci::SCHEME))
            .ok_or_else(|| {
                Error::Unknown("URL must start with oci:// for image preheat".to_string())
            })?
            .parse()
            .or_err(ErrorType::ParseError)?;

        let registry = reference.resolve_registry();
        let repository = reference.repository();
        let tag = reference.tag().unwrap_or("latest");
        let manifest_url = format!("https://{}/v2/{}/manifests/{}", registry, repository, tag);

        let channel = Channel::from_shared(self.scheduler_endpoint.clone())
            .or_err(ErrorType::ParseError)?
            .connect()
            .await?;
        let mut client = SchedulerGRPCClient::new(channel);

        let filtered_query_params = self
            .filtered_query_params
            .clone()
            .unwrap_or_else(default_proxy_rule_filtered_query_params);

        let request = PreheatImageRequest {
            url: manifest_url,
            piece_length: self.piece_length.map(|piece_length| piece_length.as_u64()),
            tag: self.tag.clone(),
            application: self.application.clone(),
            filtered_query_params,
            header: header_vec_to_hashmap(self.header.clone())?,
            priority: self.priority,
            username: self.username.clone(),
            password: self.password.clone(),
            platform: self.platform.clone(),
            scope: self.scope.clone(),
            ips: self.ips.clone().unwrap_or_default(),
            percentage: self.percentage,
            count: self.count,
            concurrent_task_count: self.concurrent_task_count,
            concurrent_peer_count: self.concurrent_peer_count,
            timeout: Some(
                prost_wkt_types::Duration::try_from(self.timeout).or_err(ErrorType::ParseError)?,
            ),

            // TODO: Support certificate chain.
            certificate_chain: Vec::new(),
            insecure_skip_verify: self.insecure_skip_verify,
        };

        client.preheat_image(request).await?;
        println!(
            "{}{}Preheat Succeeded!{}",
            color::Fg(color::Green),
            style::Bold,
            style::Reset
        );

        Ok(())
    }

    /// Preheats a file via the scheduler's gRPC PreheatFile RPC.
    async fn preheat_file(&self) -> Result<()> {
        let channel = Channel::from_shared(self.scheduler_endpoint.clone())
            .or_err(ErrorType::ParseError)?
            .connect()
            .await?;
        let mut client = SchedulerGRPCClient::new(channel);

        let filtered_query_params = self
            .filtered_query_params
            .clone()
            .unwrap_or_else(default_proxy_rule_filtered_query_params);

        let url = Url::parse(self.url.as_str()).or_err(ErrorType::ParseError)?;
        let object_storage = if object_storage::Scheme::is_supported(url.scheme()) {
            Some(ObjectStorage {
                access_key_id: self.storage_access_key_id.clone(),
                access_key_secret: self.storage_access_key_secret.clone(),
                security_token: self.storage_security_token.clone(),
                session_token: self.storage_session_token.clone(),
                region: self.storage_region.clone(),
                endpoint: self.storage_endpoint.clone(),
                credential_path: self.storage_credential_path.clone(),
                predefined_acl: self.storage_predefined_acl.clone(),
                insecure_skip_verify: self.storage_insecure_skip_verify,
            })
        } else {
            None
        };

        let hdfs = if url.scheme() == hdfs::SCHEME {
            Some(Hdfs {
                delegation_token: self.hdfs_delegation_token.clone(),
            })
        } else {
            None
        };

        let request = PreheatFileRequest {
            url: self.url.clone(),
            piece_length: self.piece_length.map(|piece_length| piece_length.as_u64()),
            tag: self.tag.clone(),
            application: self.application.clone(),
            filtered_query_params,
            header: header_vec_to_hashmap(self.header.clone())?,
            priority: self.priority,
            scope: self.scope.clone(),
            ips: self.ips.clone().unwrap_or_default(),
            percentage: self.percentage,
            count: self.count,
            concurrent_task_count: self.concurrent_task_count,
            concurrent_peer_count: self.concurrent_peer_count,
            timeout: Some(
                prost_wkt_types::Duration::try_from(self.timeout).or_err(ErrorType::ParseError)?,
            ),

            // TODO: Support certificate chain.
            certificate_chain: Vec::new(),
            insecure_skip_verify: self.insecure_skip_verify,
            object_storage,
            hdfs,
            output_path: None,
        };

        client.preheat_file(request).await?;
        println!(
            "{}{}Preheat Succeeded!{}",
            color::Fg(color::Green),
            style::Bold,
            style::Reset
        );

        Ok(())
    }

    /// Preheats an OCI image via the Dragonfly SDK proxy.
    async fn preheat_image_by_request_sdk(&self) -> Result<()> {
        let proxy = Proxy::builder()
            .scheduler_endpoint(self.scheduler_endpoint.clone())
            .build()
            .await
            .map_err(|err| Error::Unknown(format!("failed to build proxy: {}", err)))?;

        let filtered_query_params = self
            .filtered_query_params
            .clone()
            .unwrap_or_else(default_proxy_rule_filtered_query_params);

        let request = PreheatRequest {
            image: self
                .url
                .strip_prefix(&format!("{}://", oci::SCHEME))
                .ok_or_else(|| {
                    Error::Unknown("URL must start with oci:// for image preheat".to_string())
                })?
                .to_string(),
            username: self.username.clone(),
            password: self.password.clone(),
            platform: self.platform.clone(),
            piece_length: self.piece_length.map(|piece_length| piece_length.as_u64()),
            tag: self.tag.clone(),
            application: self.application.clone(),
            filtered_query_params,

            // TODO: Support content for calculating task ID.
            content_for_calculating_task_id: None,
            enable_task_id_based_blob_digest: self.enable_task_id_based_blob_digest,
            priority: Some(self.priority),
            timeout: self.timeout,

            // TODO: Support certificate chain.
            client_cert: None,
        };

        proxy
            .preheat(&request)
            .await
            .map_err(|err| Error::Unknown(format!("preheat failed: {}", err)))?;

        println!(
            "{}{}Preheat Succeeded!{}",
            color::Fg(color::Green),
            style::Bold,
            style::Reset
        );

        Ok(())
    }

    /// Preheats a file via the Dragonfly SDK proxy.
    async fn preheat_file_by_request_sdk(&self) -> Result<()> {
        let proxy = Proxy::builder()
            .scheduler_endpoint(self.scheduler_endpoint.clone())
            .build()
            .await
            .map_err(|err| Error::Unknown(format!("failed to build proxy: {}", err)))?;

        let filtered_query_params = self
            .filtered_query_params
            .clone()
            .unwrap_or_else(default_proxy_rule_filtered_query_params);

        let request = GetRequest {
            url: self.url.clone(),
            piece_length: self.piece_length.map(|piece_length| piece_length.as_u64()),
            tag: self.tag.clone(),
            application: self.application.clone(),
            filtered_query_params,
            header: header_vec_to_headermap(self.header.clone())?,

            // TODO: Support content for calculating task ID.
            content_for_calculating_task_id: None,
            enable_task_id_based_blob_digest: self.enable_task_id_based_blob_digest,
            priority: Some(self.priority),
            timeout: self.timeout,

            // TODO: Support certificate chain.
            client_cert: None,
        };

        let response = proxy
            .get(&request)
            .await
            .map_err(|err| Error::Unknown(format!("preheat failed: {}", err)))?;

        if let Some(mut reader) = response.reader {
            tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
        }

        println!(
            "{}{}Preheat Succeeded!{}",
            color::Fg(color::Green),
            style::Bold,
            style::Reset
        );

        Ok(())
    }
}
