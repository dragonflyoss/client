/*
 *     Copyright 2025 The Dragonfly Authors
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

use dragonfly_client_util::request::{PreheatRequest, Proxy, Request};
use std::time::Duration;

/// This example demonstrates how to use the `preheat` method of the Dragonfly request module
/// to pre-cache an OCI image via the Dragonfly P2P network.
///
/// The example preheats the `dragonflyoss/scheduler:v2.4.3` image for the `linux/amd64` platform.
/// All blobs (config and layers) are downloaded through the Dragonfly seed peer proxy and cached
/// in the P2P network, ensuring fast access for subsequent pulls across the cluster.
///
/// Prerequisites:
///   1. A running Dragonfly scheduler service.
///   2. At least one seed peer registered with the scheduler.
///
/// Usage:
///   Set the `DRAGONFLY_SCHEDULER_ENDPOINT` environment variable to the scheduler's gRPC endpoint,
///   then run the example:
///
///   ```shell
///   export DRAGONFLY_SCHEDULER_ENDPOINT="http://127.0.0.1:8002"
///   cargo run -p dragonfly-client-util --example preheat
///   ```
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Read the scheduler endpoint from an environment variable, falling back to a default.
    let scheduler_endpoint = std::env::var("DRAGONFLY_SCHEDULER_ENDPOINT")
        .unwrap_or_else(|_| "http://127.0.0.1:8002".to_string());

    // Build a Proxy client that communicates with the Dragonfly scheduler and seed peers.
    let proxy = Proxy::builder()
        .scheduler_endpoint(scheduler_endpoint)
        .scheduler_request_timeout(Duration::from_secs(5))
        .health_check_interval(Duration::from_secs(60))
        .max_retries(3)
        .build()
        .await
        .map_err(|err| anyhow::anyhow!("failed to build proxy: {}", err))?;

    // Create a preheat request for the dragonflyoss/scheduler:v2.4.3 image.
    // Anonymous authentication is used here since the image is public. For private
    // registries, set `username` and `password` to the appropriate credentials.
    let request = PreheatRequest {
        image: "docker.io/dragonflyoss/scheduler:v2.4.3".to_string(),
        username: None,
        password: None,
        platform: Some("linux/amd64".to_string()),
        piece_length: None,
        tag: None,
        application: None,
        filtered_query_params: Vec::new(),
        content_for_calculating_task_id: None,
        enable_task_id_based_blob_digest: false,
        priority: None,
        timeout: Duration::from_secs(600),
        client_cert: None,
    };

    // Preheat the image. This downloads all blobs (config + layers) through the Dragonfly
    // seed peer proxy, caching them in the P2P network.
    proxy
        .preheat(&request)
        .await
        .map_err(|err| anyhow::anyhow!("preheat failed: {}", err))?;

    println!(
        "Successfully preheated image: {}",
        "docker.io/dragonflyoss/scheduler:v2.4.3"
    );

    Ok(())
}
