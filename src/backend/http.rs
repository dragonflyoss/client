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

use futures::TryStreamExt;
use reqwest::header::HeaderMap;
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio_util::compat::FuturesAsyncReadCompatExt;

// Request is the request for HTTP backend.
pub struct Request {
    // url is the url of the request.
    pub url: String,
    // header is the headers of the request.
    pub header: HeaderMap,
    // timeout is the timeout of the request.
    pub timeout: Option<Duration>,
}

// Response is the response for HTTP backend.
pub struct Response<R: AsyncRead> {
    // header is the headers of the response.
    pub header: HeaderMap,
    // status_code is the status code of the response.
    pub status_code: reqwest::StatusCode,
    // body is the content of the response.
    pub reader: R,
}

// HTTP is the HTTP backend.
pub struct HTTP {}

// HTTP implements the http interface.
impl HTTP {
    // Get gets the content of the request.
    pub async fn get(&self, req: Request) -> super::Result<Response<impl AsyncRead>> {
        let mut request_builder = reqwest::Client::new().get(&req.url).headers(req.header);
        if let Some(timeout) = req.timeout {
            request_builder = request_builder.timeout(timeout);
        }

        let response = request_builder.send().await?;
        let header = response.headers().clone();
        let status_code = response.status();
        let reader = response
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read()
            .compat();

        Ok(Response {
            header,
            status_code,
            reader,
        })
    }
}
