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

use dragonfly_client_core::{Error, Result};
use dragonfly_client_util::tls::NoVerifier;
use futures::TryStreamExt;
use rustls_pki_types::CertificateDer;
use std::io::{Error as IOError, ErrorKind};
use tokio_util::io::StreamReader;
use tracing::{error, info};

// HTTP is the HTTP backend.
pub struct HTTP;

// HTTP implements the http interface.
impl HTTP {
    // new returns a new HTTP.
    pub fn new() -> HTTP {
        Self
    }

    // client returns a new reqwest client.
    fn client(
        &self,
        client_certs: Option<Vec<CertificateDer<'static>>>,
    ) -> Result<reqwest::Client> {
        let client_config_builder = match client_certs.as_ref() {
            Some(client_certs) => {
                let mut root_cert_store = rustls::RootCertStore::empty();
                root_cert_store.add_parsable_certificates(client_certs.to_owned());

                // TLS client config using the custom CA store for lookups.
                rustls::ClientConfig::builder()
                    .with_root_certificates(root_cert_store)
                    .with_no_client_auth()
            }
            // Default TLS client config with native roots.
            None => rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(NoVerifier::new())
                .with_no_client_auth(),
        };

        let client = reqwest::Client::builder()
            .use_preconfigured_tls(client_config_builder)
            .build()?;
        Ok(client)
    }
}

// Backend implements the Backend trait.
#[tonic::async_trait]
impl super::Backend for HTTP {
    // head gets the header of the request.
    async fn head(&self, request: super::HeadRequest) -> Result<super::HeadResponse> {
        info!(
            "head request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // The header of the request is required.
        let header = request.http_header.ok_or(Error::InvalidParameter)?;

        // The signature in the signed URL generated by the object storage client will include
        // the request method. Therefore, the signed URL of the GET method cannot be requested
        // through the HEAD method. Use GET request to replace of HEAD request
        // to get header and status code.
        let response = self
            .client(request.client_certs)?
            .get(&request.url)
            .headers(header)
            .timeout(request.timeout)
            .send()
            .await
            .map_err(|err| {
                error!(
                    "head request failed {} {}: {}",
                    request.task_id, request.url, err
                );
                err
            })?;

        let header = response.headers().clone();
        let status_code = response.status();
        info!(
            "head response {} {}: {:?} {:?}",
            request.task_id, request.url, status_code, header
        );

        Ok(super::HeadResponse {
            success: status_code.is_success(),
            content_length: response.content_length(),
            http_header: Some(header),
            http_status_code: Some(status_code),
            error_message: Some(status_code.to_string()),
            entries: Vec::new(),
        })
    }

    // get gets the content of the request.
    async fn get(&self, request: super::GetRequest) -> Result<super::GetResponse<super::Body>> {
        info!(
            "get request {} {} {}: {:?}",
            request.task_id, request.piece_id, request.url, request.http_header
        );

        // The header of the request is required.
        let header = request.http_header.ok_or(Error::InvalidParameter)?;
        let response = self
            .client(request.client_certs)?
            .get(&request.url)
            .headers(header)
            .timeout(request.timeout)
            .send()
            .await
            .map_err(|err| {
                error!(
                    "get request failed {} {} {}: {}",
                    request.task_id, request.piece_id, request.url, err
                );
                err
            })?;

        let header = response.headers().clone();
        let status_code = response.status();
        let reader = Box::new(StreamReader::new(
            response
                .bytes_stream()
                .map_err(|err| IOError::new(ErrorKind::Other, err)),
        ));
        info!(
            "get response {} {}: {:?} {:?}",
            request.task_id, request.piece_id, status_code, header
        );

        Ok(super::GetResponse {
            success: status_code.is_success(),
            http_header: Some(header),
            http_status_code: Some(status_code),
            reader,
            error_message: Some(status_code.to_string()),
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

#[cfg(test)]
mod tests {
    use crate::{http, Backend, GetRequest, HeadRequest};
    use httpmock::{Method, MockServer};
    use reqwest::{header::HeaderMap, StatusCode};

    #[tokio::test]
    async fn should_get_head_response() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(Method::GET).path("/head");
            then.status(200)
                .header("content-type", "text/html; charset=UTF-8")
                .body("");
        });

        let http_backend = http::HTTP::new();
        let resp = http_backend
            .head(HeadRequest {
                task_id: "test".to_string(),
                url: server.url("/head"),
                http_header: Some(HeaderMap::new()),
                timeout: std::time::Duration::from_secs(5),
                client_certs: None,
                object_storage: None,
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK))
    }

    #[tokio::test]
    async fn should_return_error_response_when_head_notexists() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(Method::GET).path("/head");
            then.status(200)
                .header("content-type", "text/html; charset=UTF-8")
                .body("");
        });

        let http_backend = http::HTTP::new();
        let resp = http_backend
            .head(HeadRequest {
                task_id: "test".to_string(),
                url: server.url("/head"),
                http_header: None,
                timeout: std::time::Duration::from_secs(5),
                client_certs: None,
                object_storage: None,
            })
            .await;

        assert!(resp.is_err());
    }

    #[tokio::test]
    async fn should_get_response() {
        let server = MockServer::start();
        server.mock(|when, then| {
            when.method(Method::GET).path("/get");
            then.status(200)
                .header("content-type", "text/html; charset=UTF-8")
                .body("OK");
        });

        let http_backend = http::HTTP::new();
        let mut resp = http_backend
            .get(GetRequest {
                task_id: "test".to_string(),
                piece_id: "test".to_string(),
                url: server.url("/get"),
                range: None,
                http_header: Some(HeaderMap::new()),
                timeout: std::time::Duration::from_secs(5),
                client_certs: None,
                object_storage: None,
            })
            .await
            .unwrap();

        assert_eq!(resp.http_status_code, Some(StatusCode::OK));
        assert_eq!(resp.text().await.unwrap(), "OK");
    }
}
