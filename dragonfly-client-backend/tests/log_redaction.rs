/*
 *     Copyright 2026 The Dragonfly Authors
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

//! Tests that the backend logs redact secrets. They capture log output with a
//! scoped subscriber, so they run in their own binary: a scoped subscriber
//! misses events whose callsites other tests in the binary hit concurrently.

use dragonfly_client_backend::http::{HTTP, HTTP_SCHEME};
use dragonfly_client_backend::{Backend, GetRequest};
use reqwest::header::HeaderMap;
use std::future::Future;
use std::io::{Read, Seek};
use std::sync::Arc;
use std::time::Duration;
use tracing::instrument::WithSubscriber;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Runs the future with a subscriber that records debug events, and returns
/// its output with the logs.
async fn capture_logs<F: Future>(future: F) -> (F::Output, String) {
    let file = Arc::new(tempfile::tempfile().unwrap());
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(tracing::Level::DEBUG)
        .with_ansi(false)
        .with_writer(file.clone())
        .finish();
    let output = future.with_subscriber(subscriber).await;

    let mut logs = String::new();
    (&*file).rewind().unwrap();
    (&*file).read_to_string(&mut logs).unwrap();
    (output, logs)
}

#[tokio::test]
async fn http_get_logs_redact_presigned_urls_and_credential_headers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/file"))
        .respond_with(ResponseTemplate::new(307).insert_header(
            "location",
            format!("{}/target?X-Amz-Signature=secret", server.uri()).as_str(),
        ))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/target"))
        .respond_with(ResponseTemplate::new(200).set_body_string("content"))
        .mount(&server)
        .await;

    let mut http_header = HeaderMap::new();
    http_header.insert("authorization", "Bearer secret".parse().unwrap());
    let http = HTTP::new(HTTP_SCHEME, None, 0, true, Duration::from_secs(600), true).unwrap();
    let (get, logs) = capture_logs(http.get(GetRequest {
        task_id: "test".to_string(),
        piece_id: "test".to_string(),
        url: format!("{}/file?X-Amz-Signature=secret", server.uri()),
        range: None,
        http_header: Some(http_header),
        timeout: Duration::from_secs(5),
        client_cert: None,
        object_storage: None,
        hdfs: None,
        hugging_face: None,
        model_scope: None,
        open_csg: None,
    }))
    .await;

    assert!(get.unwrap().success);
    assert!(logs.contains("caching temporary redirect"), "{logs}");
    assert!(logs.contains("X-Amz-Signature=[REDACTED]"), "{logs}");
    assert!(!logs.contains("secret"), "{logs}");
}
