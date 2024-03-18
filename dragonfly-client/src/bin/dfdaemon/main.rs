/*
 *     Copyright 2023 The Dragonfly Authors
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
use dragonfly_client::announcer::{ManagerAnnouncer, SchedulerAnnouncer};
use dragonfly_client::dynconfig::Dynconfig;
use dragonfly_client::gc::GC;
use dragonfly_client::grpc::{
    dfdaemon_download::DfdaemonDownloadServer, dfdaemon_upload::DfdaemonUploadServer,
    manager::ManagerClient, scheduler::SchedulerClient,
};
use dragonfly_client::metrics::Metrics;
use dragonfly_client::proxy::Proxy;
use dragonfly_client::shutdown;
use dragonfly_client::task::Task;
use dragonfly_client::tracing::init_tracing;
use dragonfly_client_backend::http::HTTP;
use dragonfly_client_config::dfdaemon;
use dragonfly_client_storage::Storage;
use dragonfly_client_util::id_generator::IDGenerator;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{error, info, Level};

#[derive(Debug, Parser)]
#[command(
    name = dfdaemon::NAME,
    author,
    version,
    about = "dfdaemon is a high performance P2P download daemon",
    long_about = "A high performance P2P download daemon in Dragonfly that can download resources of different protocols. \
    When user triggers a file downloading task, dfdaemon will download the pieces of file from other peers. \
    Meanwhile, it will act as an uploader to support other peers to download pieces from it if it owns them."
)]
struct Args {
    #[arg(
        short = 'c',
        long = "config",
        default_value_os_t = dfdaemon::default_dfdaemon_config_path(),
        help = "Specify config file to use")
    ]
    config: PathBuf,

    #[arg(
        long = "lock-path",
        default_value_os_t = dfdaemon::default_dfdaemon_lock_path(),
        help = "Specify the lock file path"
    )]
    lock_path: PathBuf,

    #[arg(
        short = 'l',
        long,
        default_value = "info",
        help = "Specify the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_os_t = dfdaemon::default_dfdaemon_log_dir(),
        help = "Specify the log directory"
    )]
    log_dir: PathBuf,

    #[arg(
        long,
        default_value_t = 24,
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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    // Parse command line arguments.
    let args = Args::parse();

    // Load config.
    let config = dfdaemon::Config::load(&args.config)?;
    let config = Arc::new(config);

    // Initialize tracing.
    let _guards = init_tracing(
        dfdaemon::NAME,
        &args.log_dir,
        args.log_level,
        args.log_max_files,
        config.tracing.addr.to_owned(),
        args.verbose,
    );

    // Initialize storage.
    let storage = Storage::new(config.clone(), config.storage.dir.as_path())
        .await
        .map_err(|err| {
            error!("initialize storage failed: {}", err);
            err
        })?;
    let storage = Arc::new(storage);

    // Initialize id generator.
    let id_generator = IDGenerator::new(
        config.host.ip.unwrap().to_string(),
        config.host.hostname.clone(),
        config.seed_peer.enable,
    );
    let id_generator = Arc::new(id_generator);

    // Initialize http client.
    let http_client = HTTP::new();
    let http_client = Arc::new(http_client);

    // Initialize manager client.
    let manager_client = ManagerClient::new(config.manager.addrs.clone())
        .await
        .map_err(|err| {
            error!("initialize manager client failed: {}", err);
            err
        })?;
    let manager_client = Arc::new(manager_client);

    // Initialize channel for graceful shutdown.
    let shutdown = shutdown::Shutdown::default();
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::unbounded_channel();

    // Initialize dynconfig server.
    let dynconfig = Dynconfig::new(
        config.clone(),
        manager_client.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    )
    .await
    .map_err(|err| {
        error!("initialize dynconfig server failed: {}", err);
        err
    })?;
    let dynconfig = Arc::new(dynconfig);

    // Initialize scheduler client.
    let scheduler_client = SchedulerClient::new(dynconfig.clone())
        .await
        .map_err(|err| {
            error!("initialize scheduler client failed: {}", err);
            err
        })?;
    let scheduler_client = Arc::new(scheduler_client);

    // Initialize task manager.
    let task = Task::new(
        config.clone(),
        id_generator.clone(),
        storage.clone(),
        scheduler_client.clone(),
        http_client.clone(),
    );
    let task = Arc::new(task);

    // Initialize metrics server.
    let metrics = Metrics::new(
        SocketAddr::new(
            config.metrics.server.ip.unwrap(),
            config.metrics.server.port,
        ),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize proxy server.
    let proxy = Proxy::new(
        config.clone(),
        task.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize manager announcer.
    let manager_announcer = ManagerAnnouncer::new(
        config.clone(),
        manager_client.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize scheduler announcer.
    let scheduler_announcer = SchedulerAnnouncer::new(
        config.clone(),
        id_generator.host_id(),
        scheduler_client.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    )
    .await
    .map_err(|err| {
        error!("initialize scheduler announcer failed: {}", err);
        err
    })?;

    // Initialize upload grpc server.
    let mut dfdaemon_upload_grpc = DfdaemonUploadServer::new(
        config.clone(),
        SocketAddr::new(config.upload.server.ip.unwrap(), config.upload.server.port),
        task.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize download grpc server.
    let mut dfdaemon_download_grpc = DfdaemonDownloadServer::new(
        config.download.server.socket_path.clone(),
        task.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize garbage collector.
    let gc = GC::new(
        config.clone(),
        storage.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Log dfdaemon started pid.
    info!("dfdaemon started at pid {}", std::process::id());

    // Wait for servers to exit or shutdown signal.
    tokio::select! {
        _ = tokio::spawn(async move { dynconfig.run().await }) => {
            info!("dynconfig manager exited");
        },

        _ = tokio::spawn(async move { metrics.run().await }) => {
            info!("metrics server exited");
        },

        _ = tokio::spawn(async move { proxy.run().await }) => {
            info!("proxy server exited");
        },

        _ = tokio::spawn(async move { manager_announcer.run().await }) => {
            info!("announcer manager exited");
        },

        _ = tokio::spawn(async move { scheduler_announcer.run().await }) => {
            info!("announcer scheduler exited");
        },

        _ = tokio::spawn(async move { dfdaemon_upload_grpc.run().await }) => {
            info!("dfdaemon upload grpc server exited");
        },

        _ = tokio::spawn(async move { dfdaemon_download_grpc.run().await }) => {
            info!("dfdaemon download grpc unix server exited");
        },

        _ = tokio::spawn(async move { gc.run().await }) => {
            info!("garbage collector exited");
        },

        _ = shutdown::shutdown_signal() => {},
    }

    // Trigger shutdown signal to other servers.
    shutdown.trigger();

    // Drop task to release scheduler_client. when drop the task, it will release the Arc reference
    // of scheduler_client, so scheduler_client can be released normally.
    drop(task);

    // Drop scheduler_client to release dynconfig. when drop the scheduler_client, it will release the
    // Arc reference of dynconfig, so dynconfig can be released normally.
    drop(scheduler_client);

    // Drop shutdown_complete_rx to wait for the other server to exit.
    drop(shutdown_complete_tx);

    // Wait for the other server to exit.
    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}
