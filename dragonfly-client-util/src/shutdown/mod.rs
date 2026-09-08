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

/// A shutdown signal for graceful termination.
#[derive(Debug)]
pub struct Shutdown {
    /// True if the shutdown signal has been received.
    is_shutdown: bool,

    /// Used to send the shutdown signal.
    sender: broadcast::Sender<()>,

    /// Used to receive the shutdown signal.
    receiver: broadcast::Receiver<()>,
}

/// Implements the shutdown signal.
impl Shutdown {
    /// Creates a new shutdown signal.
    pub fn new() -> Shutdown {
        let (sender, receiver) = broadcast::channel(1);
        Self {
            is_shutdown: false,
            sender,
            receiver,
        }
    }

    /// Returns true if the shutdown signal has been received.
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown
    }

    /// Triggers the shutdown signal.
    pub fn trigger(&self) {
        let _ = self.sender.send(());
    }

    /// Waits for the shutdown signal.
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

/// Implements the Default trait.
impl Default for Shutdown {
    /// Returns a new default shutdown signal.
    fn default() -> Self {
        Self::new()
    }
}

/// Implements the Clone trait.
impl Clone for Shutdown {
    /// Returns a cloned shutdown signal.
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

/// Returns a future that will resolve when a SIGINT, SIGTERM or SIGQUIT signal is
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn recv_returns_after_trigger() {
        let mut shutdown = Shutdown::new();
        assert!(!shutdown.is_shutdown());

        let trigger = shutdown.clone();
        tokio::spawn(async move {
            sleep(Duration::from_millis(10)).await;
            trigger.trigger();
        });

        shutdown.recv().await;
        assert!(shutdown.is_shutdown());
    }

    #[tokio::test]
    async fn trigger_wakes_every_clone() {
        let mut shutdown = Shutdown::new();
        let mut clones = vec![shutdown.clone(), shutdown.clone()];
        shutdown.trigger();

        shutdown.recv().await;
        assert!(shutdown.is_shutdown());

        for clone in &mut clones {
            clone.recv().await;
            assert!(clone.is_shutdown());
        }
    }

    #[tokio::test]
    async fn clone_copies_the_shutdown_flag() {
        let mut shutdown = Shutdown::new();
        shutdown.trigger();
        shutdown.recv().await;

        let clone = shutdown.clone();
        assert!(clone.is_shutdown());
    }

    #[tokio::test]
    async fn recv_returns_immediately_once_shutdown() {
        let mut shutdown = Shutdown::new();
        shutdown.trigger();
        shutdown.recv().await;

        let start = std::time::Instant::now();
        shutdown.recv().await;
        assert!(start.elapsed() < Duration::from_millis(5));
    }
}
