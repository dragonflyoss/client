/*
 *     Copyright 2024 The Dragonfly Authors
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

use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use reqwest::header::HeaderMap;
use rustls_pki_types::CertificateDer;
use std::{pin::Pin, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{error, info};
use url::Url;

pub mod http;

pub type Body = Box<dyn AsyncRead + Send + Unpin>;

// HeadRequest is the head request for backend.
pub struct HeadRequest {
    // url is the url of the request.
    pub url: String,

    // http_header is the headers of the request.
    pub http_header: Option<HeaderMap>,

    // timeout is the timeout of the request.
    pub timeout: Duration,

    // client_certs is the client certificates for the request.
    pub client_certs: Option<Vec<CertificateDer<'static>>>,
}

// HeadResponse is the head response for backend.
pub struct HeadResponse {
    // http_header is the headers of the response.
    pub http_header: Option<HeaderMap>,

    // http_status_code is the status code of the response.
    pub http_status_code: Option<reqwest::StatusCode>,
}

// GetRequest is the get request for backend.
pub struct GetRequest {
    // url is the url of the request.
    pub url: String,

    // http_header is the headers of the request.
    pub http_header: Option<HeaderMap>,

    // timeout is the timeout of the request.
    pub timeout: Duration,

    // client_certs is the client certificates for the request.
    pub client_certs: Option<Vec<CertificateDer<'static>>>,
}

// GetResponse is the get response for backend.
pub struct GetResponse<R>
where
    R: AsyncRead + Unpin,
{
    // http_header is the headers of the response.
    pub http_header: Option<HeaderMap>,

    // http_status_code is the status code of the response.
    pub http_status_code: Option<reqwest::StatusCode>,

    // body is the content of the response.
    pub reader: R,
}

impl<R> GetResponse<R>
where
    R: AsyncRead + Unpin,
{
    pub async fn text(&mut self) -> Result<String> {
        let mut buffer = String::new();
        Pin::new(&mut self.reader)
            .read_to_string(&mut buffer)
            .await?;
        Ok(buffer)
    }
}

// Backend is the interface of the backend.
#[tonic::async_trait]
pub trait Backend {
    // head gets the header of the request.
    async fn head(&self, request: HeadRequest) -> Result<HeadResponse>;

    // get gets the content of the request.
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>>;
}

// BackendFactory is the factory of the backend.
pub struct BackendFactory;

// BackendFactory implements the factory of the backend.
impl BackendFactory {
    // new_backend creates a new backend factory.
    pub fn new_backend(url: &str) -> Result<Box<dyn Backend + Send>> {
        let url = Url::parse(url).or_err(ErrorType::ParseError)?;
        let scheme = url.scheme();
        info!("backend url scheme: {}", scheme);

        match scheme {
            "http" | "https" => Ok(Box::new(http::HTTP::new())),
            _ => {
                error!("backend unsupported scheme: {}", scheme);
                Err(Error::InvalidParameter)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_http_backend() {
        let backend = BackendFactory::new_backend("http://hello");
        assert!(backend.is_ok());
    }
}
