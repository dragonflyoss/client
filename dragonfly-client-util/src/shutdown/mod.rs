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
    /// Creates a new Shutdown.
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
    // Returns a new default Shutdown.
    fn default() -> Self {
        Self::new()
    }
}

/// Clone implements the Clone trait.
impl Clone for Shutdown {
    /// Returns a new Shutdown.
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{sleep, Duration};

    #[tokio::test]
    async fn test_shutdown_trigger_and_recv() {
        // Create a new shutdown instance.
        let mut shutdown = Shutdown::new();

        // Trigger the shutdown signal in a separate task.
        let shutdown_clone = shutdown.clone();
        tokio::spawn(async move {
            // Small delay to ensure the receiver is waiting.
            sleep(Duration::from_millis(10)).await;
            shutdown_clone.trigger();
        });

        // Wait for the shutdown signal.
        shutdown.recv().await;

        // Verify that is_shutdown is set to true.
        assert!(shutdown.is_shutdown());
    }

    #[tokio::test]
    async fn test_shutdown_multiple_receivers() {
        // Create a new shutdown instance.
        let mut shutdown1 = Shutdown::new();
        let mut shutdown2 = shutdown1.clone();
        let mut shutdown3 = shutdown1.clone();

        // Trigger the shutdown signal.
        shutdown1.trigger();

        // All receivers should receive the signal.
        shutdown1.recv().await;
        shutdown2.recv().await;
        shutdown3.recv().await;

        // Verify that all instances have is_shutdown set to true.
        assert!(shutdown1.is_shutdown());
        assert!(shutdown2.is_shutdown());
        assert!(shutdown3.is_shutdown());
    }

    #[tokio::test]
    async fn test_shutdown_clone_behavior() {
        // Create a new shutdown instance.
        let mut shutdown1 = Shutdown::new();

        // Set is_shutdown to true.
        shutdown1.trigger();
        shutdown1.recv().await;
        assert!(shutdown1.is_shutdown());

        // Clone the instance.
        let shutdown2 = shutdown1.clone();

        // Verify that the clone has the same is_shutdown value.
        assert_eq!(shutdown1.is_shutdown(), shutdown2.is_shutdown());

        // Create a new instance before triggering.
        let mut shutdown3 = Shutdown::new();
        let mut shutdown4 = shutdown3.clone();

        // Trigger after cloning.
        shutdown3.trigger();

        // Both should receive the signal.
        shutdown3.recv().await;
        shutdown4.recv().await;

        assert!(shutdown3.is_shutdown());
        assert!(shutdown4.is_shutdown());
    }

    #[tokio::test]
    async fn test_shutdown_already_triggered() {
        // Create a new shutdown instance.
        let mut shutdown = Shutdown::new();

        // Trigger and receive.
        shutdown.trigger();
        shutdown.recv().await;
        assert!(shutdown.is_shutdown());

        // Call recv again, should return immediately.
        let start = std::time::Instant::now();
        shutdown.recv().await;
        let elapsed = start.elapsed();

        // Verify that recv returned immediately (less than 5ms).
        assert!(elapsed < Duration::from_millis(5));
    }
}
