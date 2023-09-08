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

use anyhow::Context;
use clap::Parser;
use dragonfly_client::announcer::{ManagerAnnouncer, SchedulerAnnouncer};
use dragonfly_client::config::dfdaemon;
use dragonfly_client::dynconfig::Dynconfig;
use dragonfly_client::grpc::{
    dfdaemon::DfdaemonServer, manager::ManagerClient, scheduler::SchedulerClient,
};
use dragonfly_client::health::Health;
use dragonfly_client::metrics::Metrics;
use dragonfly_client::shutdown;
use dragonfly_client::storage::Storage;
use dragonfly_client::tracing::init_tracing;
use dragonfly_client::utils::id_generator::IDGenerator;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, Level};

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
        short = 'l',
        long,
        default_value = "info",
        help = "Set the logging level [trace, debug, info, warn, error]"
    )]
    log_level: Level,

    #[arg(
        long,
        default_value_os_t = dfdaemon::default_dfdaemon_log_dir(),
        help = "Specify the log directory"
    )]
    log_dir: PathBuf,
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
        config.tracing.addr.to_owned(),
    );

    // Initialize storage.
    let _storage = Storage::new(&config.server.data_dir)?;

    // Initialize id generator.
    let id_generator = IDGenerator::new(
        config.host.ip.unwrap().to_string(),
        config.host.hostname.clone(),
    );

    // Initialize manager client.
    let manager_client = ManagerClient::new(config.manager.addr.as_ref().unwrap())
        .await
        .context("failed to initialize manager client")?;
    let manager_client = Arc::new(manager_client);

    // Initialize channel for graceful shutdown.
    let shutdown = shutdown::Shutdown::default();
    let (shutdown_complete_tx, mut shutdown_complete_rx) = mpsc::unbounded_channel();

    // Initialize dynconfig server.
    let mut dynconfig = Dynconfig::new(
        config.clone(),
        manager_client.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    )
    .await?;

    // Initialize scheduler client.
    let scheduler_client = SchedulerClient::new()
        .await
        .context("failed to initialize scheduler client")?;
    let scheduler_client = Arc::new(scheduler_client);

    // Initialize metrics server.
    let mut metrics = Metrics::new(
        SocketAddr::new(config.metrics.ip.unwrap(), config.metrics.port),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize health server.
    let mut health = Health::new(
        SocketAddr::new(config.health.ip.unwrap(), config.health.port),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize manager announcer.
    let mut manager_announcer = ManagerAnnouncer::new(
        config.clone(),
        manager_client.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize scheduler announcer.
    let mut scheduler_announcer = SchedulerAnnouncer::new(
        config.clone(),
        id_generator.host_id(),
        scheduler_client.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Initialize dfdaemon grpc server.
    let mut dfdaemon_grpc = DfdaemonServer::new(
        SocketAddr::new(config.server.ip.unwrap(), config.server.port),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    );

    // Wait for servers to exit or shutdown signal.
    tokio::select! {
        _ = tokio::spawn(async move { dynconfig.run().await }) => {
            info!("dynconfig manager exited");
        },

        _ = tokio::spawn(async move { metrics.run().await }) => {
            info!("metrics server exited");
        },

        _ = tokio::spawn(async move { health.run().await }) => {
            info!("health server exited");
        },

        _ = tokio::spawn(async move { manager_announcer.run().await }) => {
            info!("announcer manager exited");
        },

        _ = tokio::spawn(async move { scheduler_announcer.run().await }) => {
            info!("announcer scheduler exited");
        },

        _ = tokio::spawn(async move { dfdaemon_grpc.run().await }) => {
            info!("dfdaemon grpc server exited");
        },

        _ = shutdown::shutdown_signal() => {},
    }

    // Trigger shutdown signal to other servers.
    shutdown.trigger();

    // Drop shutdown_complete_rx to wait for the other server to exit.
    drop(shutdown_complete_tx);

    // Wait for the other server to exit.
    let _ = shutdown_complete_rx.recv().await;

    Ok(())
}
