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

use tokio::signal::unix::{signal, SignalKind};
use tokio::sync::broadcast;
use tracing::info;

// Shutdown is a signal to shutdown.
#[derive(Debug)]
pub struct Shutdown {
    // is_shutdown is true if the shutdown signal has been received.
    is_shutdown: bool,

    // notify is used to notify the shutdown signal.
    notify: broadcast::Receiver<()>,
}

// Shutdown implements the shutdown signal.
impl Shutdown {
    // new creates a new Shutdown.
    pub fn new(notify: broadcast::Receiver<()>) -> Shutdown {
        Self {
            is_shutdown: false,
            notify,
        }
    }

    // is_shutdown returns true if the shutdown signal has been received.
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    // recv waits for the shutdown signal.
    pub async fn recv(&mut self) {
        // Return immediately if the shutdown signal has already been received.
        if self.is_shutdown {
            return;
        }

        // Wait for the shutdown signal.
        let _ = self.notify.recv().await;

        // Set the shutdown flag.
        self.is_shutdown = true;
    }
}

// shutdown_signal returns a future that will resolve when a SIGINT, SIGTERM or SIGQUIT signal is
// received by the process.
pub async fn shutdown_signal() {
    let mut sigint = signal(SignalKind::interrupt()).unwrap();
    let mut sigterm = signal(SignalKind::terminate()).unwrap();
    let mut sigquit = signal(SignalKind::quit()).unwrap();

    tokio::select! {
        _ = sigint.recv() => {
            info!("received SIGINT, shutting down");
        },
        _ = sigterm.recv() => {
            info!("received SIGTERM, shutting down");
        }
        _ = sigquit.recv() => {
            info!("received SIGQUIT, shutting down");
        }
    }
}
