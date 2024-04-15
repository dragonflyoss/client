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
use futures::TryStreamExt;
use rustls_pki_types::CertificateDer;
use tokio::io::AsyncRead;
use tokio_util::compat::FuturesAsyncReadCompatExt;

// HTTP is the HTTP backend.
pub struct HTTP;

// HTTP implements the http interface.
impl HTTP {
    // new returns a new HTTP.
    pub fn new() -> HTTP {
        Self {}
    }

    // client returns a new reqwest client.
    fn client(
        &self,
        client_certs: Option<Vec<CertificateDer<'static>>>,
    ) -> Result<reqwest::Client> {
        match client_certs {
            Some(client_certs) => {
                // TLS client config using the custom CA store for lookups.
                let mut root_cert_store = rustls::RootCertStore::empty();
                root_cert_store.add_parsable_certificates(&client_certs);
                let client_config = rustls::ClientConfig::builder()
                    .with_safe_defaults()
                    .with_root_certificates(root_cert_store)
                    .with_no_client_auth();

                let client = reqwest::Client::builder()
                    .use_preconfigured_tls(client_config)
                    .build()
                    .or_err(ErrorType::HTTPError)?;
                Ok(client)
            }
            None => {
                // Default TLS client config with native roots.
                let client = reqwest::Client::builder()
                    .use_native_tls()
                    .build()
                    .or_err(ErrorType::HTTPError)?;
                Ok(client)
            }
        }
    }
}

// Backend implements the Backend trait.
#[tonic::async_trait]
impl super::Backend for HTTP {
    // head gets the header of the request.
    async fn head(&self, request: super::HeadRequest) -> Result<super::HeadResponse> {
        // The header of the request is required.
        let header = request.http_header.ok_or(Error::InvalidParameter)?;

        // The signature in the signed URL generated by the object storage client will include
        // the request method. Therefore, the signed URL of the GET method cannot be requested
        // through the HEAD method. Use GET request to replace of HEAD request
        // to get header and status code.
        let mut request_builder = self
            .client(request.client_certs)?
            .get(&request.url)
            .headers(header);
        request_builder = request_builder.timeout(request.timeout);

        let response = request_builder.send().await.or_err(ErrorType::HTTPError)?;

        let header = response.headers().clone();
        let status_code = response.status();

        Ok(super::HeadResponse {
            http_header: Some(header),
            http_status_code: Some(status_code),
        })
    }

    // get gets the content of the request.
    async fn get(
        &self,
        request: super::GetRequest,
    ) -> Result<super::GetResponse<Box<dyn AsyncRead + Send + Sync + Unpin>>> {
        // The header of the request is required.
        let header = request.http_header.ok_or(Error::InvalidParameter)?;

        let mut request_builder = self
            .client(request.client_certs)?
            .get(&request.url)
            .headers(header);
        request_builder = request_builder.timeout(request.timeout);

        let response = request_builder.send().await.or_err(ErrorType::HTTPError)?;

        let header = response.headers().clone();
        let status_code = response.status();
        let reader: Box<dyn AsyncRead + Send + Sync + Unpin> = Box::new(
            response
                .bytes_stream()
                .map_err(|err| futures::io::Error::new(futures::io::ErrorKind::Other, err))
                .into_async_read()
                .compat(),
        );

        Ok(super::GetResponse {
            http_header: Some(header),
            http_status_code: Some(status_code),
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
