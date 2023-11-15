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

use crate::Result;
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

// HeadResponse is the head response for HTTP backend.
pub struct HeadResponse {
    // header is the headers of the response.
    pub header: HeaderMap,

    // status_code is the status code of the response.
    pub status_code: reqwest::StatusCode,
}

// GetResponse is the get response for HTTP backend.
pub struct GetResponse<R: AsyncRead> {
    // header is the headers of the response.
    pub header: HeaderMap,

    // status_code is the status code of the response.
    pub status_code: reqwest::StatusCode,

    // body is the content of the response.
    pub reader: R,
}

// HTTP is the HTTP backend.
pub struct HTTP {
    // client is the http client.
    client: reqwest::Client,
}

// HTTP implements the http interface.
impl HTTP {
    // new returns a new HTTP.
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }

    // Head gets the header of the request.
    pub async fn head(&self, req: Request) -> Result<HeadResponse> {
        let mut request_builder = self.client.head(&req.url).headers(req.header);
        if let Some(timeout) = req.timeout {
            request_builder = request_builder.timeout(timeout);
        } else {
            request_builder = request_builder.timeout(super::REQUEST_TIMEOUT);
        }

        let response = request_builder.send().await?;
        let header = response.headers().clone();
        let status_code = response.status();

        Ok(HeadResponse {
            header,
            status_code,
        })
    }

    // Get gets the content of the request.
    pub async fn get(&self, req: Request) -> Result<GetResponse<impl AsyncRead>> {
        let mut request_builder = self.client.get(&req.url).headers(req.header);
        if let Some(timeout) = req.timeout {
            request_builder = request_builder.timeout(timeout);
        } else {
            request_builder = request_builder.timeout(super::REQUEST_TIMEOUT);
        }

        let response = request_builder.send().await?;
        let header = response.headers().clone();
        let status_code = response.status();
        let reader = response
            .bytes_stream()
            .map_err(|e| futures::io::Error::new(futures::io::ErrorKind::Other, e))
            .into_async_read()
            .compat();

        Ok(GetResponse {
            header,
            status_code,
            reader,
        })
    }
}

// Default implements the Default trait.
impl Default for HTTP {
    // default returns a new default HTTP.
    fn default() -> Self {
        Self::new()
    }
}
