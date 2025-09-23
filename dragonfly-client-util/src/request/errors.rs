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

use dragonfly_client_core::Error as DFError;
use reqwest;
use std::collections::HashMap;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Base(#[from] DFError),

    #[allow(clippy::enum_variant_names)]
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    #[allow(clippy::enum_variant_names)]
    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),

    #[error(transparent)]
    InvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),

    #[allow(clippy::enum_variant_names)]
    #[error(transparent)]
    BackendError(#[from] BackendError),

    #[allow(clippy::enum_variant_names)]
    #[error(transparent)]
    ProxyError(#[from] ProxyError),

    #[allow(clippy::enum_variant_names)]
    #[error(transparent)]
    DfdaemonError(#[from] DfdaemonError),
}

// BackendError is error detail for Backend.
#[derive(Debug, thiserror::Error)]
#[error("error occurred in the backend server, message: {message:?}, header: {header:?}, status_code: {status_code:?}")]
pub struct BackendError {
    // Backend error message.
    pub message: Option<String>,

    // Backend HTTP response header.
    pub header: HashMap<String, String>,

    // Backend HTTP status code.
    pub status_code: Option<reqwest::StatusCode>,
}

// ProxyError is error detail for Proxy.
#[derive(Debug, thiserror::Error)]
#[error("error occurred in the proxy server, message: {message:?}, header: {header:?}, status_code: {status_code:?}")]
pub struct ProxyError {
    // Proxy error message.
    pub message: Option<String>,

    // Proxy HTTP response header.
    pub header: HashMap<String, String>,

    // Proxy HTTP status code.
    pub status_code: Option<reqwest::StatusCode>,
}

// DfdaemonError is error detail for Dfdaemon.
#[derive(Debug, thiserror::Error)]
#[error("error occurred in the dfdaemon, message: {message:?}")]
pub struct DfdaemonError {
    // Dfdaemon error message.
    pub message: Option<String>,
}
