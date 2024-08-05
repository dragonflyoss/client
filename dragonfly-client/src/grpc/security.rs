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

use dragonfly_api::security::{
    certificate_client::CertificateClient as CertificateGRPCClient, CertificateRequest,
    CertificateResponse,
};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Result,
};
use tonic::transport::Channel;
use tracing::instrument;

// CertificateClient is a wrapper of CertificateGRPCClient.
#[derive(Clone)]
pub struct CertificateClient {
    // client is the grpc client of the certificate.
    pub client: CertificateGRPCClient<Channel>,
}

// CertificateClient implements the grpc client of the certificate.
impl CertificateClient {
    // new creates a new CertificateClient.
    pub async fn new(addr: String) -> Result<Self> {
        let channel = Channel::from_static(Box::leak(addr.into_boxed_str()))
            .connect_timeout(super::CONNECT_TIMEOUT)
            .timeout(super::REQUEST_TIMEOUT)
            .tcp_keepalive(Some(super::TCP_KEEPALIVE))
            .connect()
            .await
            .or_err(ErrorType::ConnectError)?;
        let client = CertificateGRPCClient::new(channel)
            .max_decoding_message_size(usize::MAX)
            .max_encoding_message_size(usize::MAX);
        Ok(Self { client })
    }

    // issue_certificate issues a certificate for the peer.
    #[instrument(skip_all)]
    pub async fn issue_certificate(
        &self,
        request: CertificateRequest,
    ) -> Result<CertificateResponse> {
        let request = Self::make_request(request);
        let response = self.client.clone().issue_certificate(request).await?;
        Ok(response.into_inner())
    }

    // make_request creates a new request with timeout.
    fn make_request<T>(request: T) -> tonic::Request<T> {
        let mut request = tonic::Request::new(request);
        request.set_timeout(super::REQUEST_TIMEOUT);
        request
    }
}
