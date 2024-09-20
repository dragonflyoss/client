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

/// Shutdown is a signal to shutdown.
#[derive(Debug)]
pub struct Shutdown {
    /// is_shutdown is true if the shutdown signal has been received.
    is_shutdown: bool,

    /// sender is used to send the shutdown signal.
    sender: broadcast::Sender<()>,

    /// receiver is used to receive the shutdown signal.
    receiver: broadcast::Receiver<()>,
}

/// Shutdown implements the shutdown signal.
impl Shutdown {
    /// new creates a new Shutdown.
    pub fn new() -> Shutdown {
        let (sender, receiver) = broadcast::channel(1);
        Self {
            is_shutdown: false,
            sender,
            receiver,
        }
    }

    /// is_shutdown returns true if the shutdown signal has been received.
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// trigger triggers the shutdown signal.
    pub fn trigger(&self) {
        let _ = self.sender.send(());
    }

    /// recv waits for the shutdown signal.
    pub async fn recv(&mut self) {
        // Return immediately if the shutdown signal has already been received.
        if self.is_shutdown {
            return;
        }

        // Wait for the shutdown signal.
        let _ = self.receiver.recv().await;

        // Set the shutdown flag.
        self.is_shutdown = true;
    }
}

// Default implements the Default trait.
impl Default for Shutdown {
    // default returns a new default Shutdown.
    fn default() -> Self {
        Self::new()
    }
}

/// Clone implements the Clone trait.
impl Clone for Shutdown {
    /// clone returns a new Shutdown.
    fn clone(&self) -> Self {
        let sender = self.sender.clone();
        let receiver = self.sender.subscribe();
        Self {
            is_shutdown: self.is_shutdown,
            sender,
            receiver,
        }
    }
}

/// shutdown_signal returns a future that will resolve when a SIGINT, SIGTERM or SIGQUIT signal is
/// received by the process.
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
