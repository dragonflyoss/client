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

use dragonfly_client_util::request::{GetRequest, Proxy, Request};
use std::time::Duration;
use tokio::io::AsyncReadExt;

/// This example demonstrates how to use the `get` method of the Dragonfly request module
/// to download a file via the Dragonfly P2P network.
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
///   cargo run -p dragonfly-client-util --example get
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

    // Create a GET request for the target URL. The request is routed through the Dragonfly
    // seed peer proxy so that the content is cached in the P2P network.
    let request = GetRequest {
        url: "https://example.com/path/to/file".to_string(),
        header: None,
        piece_length: None,
        tag: None,
        application: None,
        filtered_query_params: Vec::new(),
        content_for_calculating_task_id: None,
        enable_task_id_based_blob_digest: false,
        priority: None,
        timeout: Duration::from_secs(300),
        client_cert: None,
    };

    // Send the GET request and receive a streaming response.
    let response = proxy
        .get(request)
        .await
        .map_err(|err| anyhow::anyhow!("get request failed: {}", err))?;

    println!("Response success: {}", response.success);
    println!("Response status:  {:?}", response.status_code);
    println!("Response headers: {:?}", response.header);

    // Read the response body from the streaming reader.
    if let Some(mut reader) = response.reader {
        let mut body = Vec::new();
        reader.read_to_end(&mut body).await?;
        println!("Downloaded {} bytes", body.len());
    }

    Ok(())
}
