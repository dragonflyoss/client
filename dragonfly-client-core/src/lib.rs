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

pub mod error;
mod result;

pub type Error = error::DFError;
pub type Result<T> = result::Result<T>;

// // Error is the error for Client.
// #[derive(Debug, thiserror::Error)]
// pub enum Error {
//     // RocksDB is the error for rocksdb.
//     #[error(transparent)]
//     RocksDB(#[from] rocksdb::Error),

//     // JSON is the error for serde_json.
//     #[error(transparent)]
//     JSON(#[from] serde_json::Error),

//     // TomlSerError is the error for toml ser.
//     #[error(transparent)]
//     TomlSerError(#[from] toml::ser::Error),

//     // TomlDeError is the error for toml de.
//     #[error(transparent)]
//     TomlDeError(#[from] toml::de::Error),

//     // TomlError is the error for toml.
//     #[error(transparent)]
//     TomlError(#[from] toml_edit::TomlError),

//     // YAML is the error for serde_yaml.
//     #[error(transparent)]
//     Yaml(#[from] serde_yaml::Error),

//     // IO is the error for IO operation.
//     #[error(transparent)]
//     IO(#[from] std::io::Error),

//     // URLParse is the error for url parse.
//     #[error(transparent)]
//     URLParse(#[from] url::ParseError),

//     // TonicTransport is the error for tonic transport.
//     #[error(transparent)]
//     TonicTransport(#[from] tonic::transport::Error),

//     // TonicStatus is the error for tonic status.
//     #[error(transparent)]
//     TonicStatus(#[from] tonic::Status),

//     // Headers is the error for headers.
//     #[error(transparent)]
//     Headers(#[from] headers::Error),

//     // Reqwest is the error for reqwest.
//     #[error(transparent)]
//     Reqwest(#[from] reqwest::Error),

//     // Rustls is the error for rustls.
//     #[error(transparent)]
//     Rustls(#[from] rustls::Error),

//     // Rcgen is the error for rcgen.
//     #[error(transparent)]
//     Rcgen(#[from] rcgen::Error),

//     // Utf8 is the error for utf8.
//     #[error(transparent)]
//     Utf8(#[from] std::str::Utf8Error),

//     // Hyper is the error for hyper.
//     #[error(transparent)]
//     Hyper(#[from] hyper::Error),

//     /// Hyper is the http request error for hyper.
//     #[error(transparent)]
//     Http(#[from] hyper::http::Error),

//     // HyperUtilClientLegacyError is the error for hyper util client legacy.
//     #[error(transparent)]
//     HyperUtilClientLegacyError(#[from] hyper_util::client::legacy::Error),

//     // Validation is the error for validation.
//     #[error(transparent)]
//     Validation(#[from] validator::ValidationErrors),

//     // MpscSend is the error for send.
//     #[error("mpsc send: {0}")]
//     MpscSend(String),

//     // Acquire is the error for acquire.
//     #[error("acquiring semaphore: {0}")]
//     Acquire(#[from] tokio::sync::AcquireError),

//     // Join is the error for join.
//     #[error(transparent)]
//     Join(#[from] tokio::task::JoinError),

//     #[error("send timeout")]
//     SendTimeout(),

//     // Reqwest is the error for reqwest.
//     #[error(transparent)]
//     HTTP(HTTPError),

//     // InvalidUriParts is the error for invalid uri parts.
//     #[error(transparent)]
//     InvalidUriParts(#[from] http::uri::InvalidUriParts),

//     // InvalidHeaderValue is the error for invalid header value.
//     #[error(transparent)]
//     InvalidHeaderValue(#[from] http::header::InvalidHeaderValue),

//     // ReqwestInvalidHeaderValue is the error for invalid header value in reqwest.
//     #[error(transparent)]
//     ReqwestInvalidHeaderValue(#[from] reqwest::header::InvalidHeaderValue),

//     // ReqwestHeaderToStr is the error for header to str in reqwest.
//     #[error(transparent)]
//     ReqwestHeaderToStr(#[from] reqwest::header::ToStrError),

//     // RangeUnsatisfiableError is the error for range unsatisfiable.
//     #[error(transparent)]
//     RangeUnsatisfiableError(#[from] http_range_header::RangeUnsatisfiableError),

//     // HashRing is the error for hashring.
//     #[error{"hashring {0} is failed"}]
//     HashRing(String),

//     // HostNotFound is the error when the host is not found.
//     #[error{"host {0} not found"}]
//     HostNotFound(String),

//     // TaskNotFound is the error when the task is not found.
//     #[error{"task {0} not found"}]
//     TaskNotFound(String),

//     // PieceNotFound is the error when the piece is not found.
//     #[error{"piece {0} not found"}]
//     PieceNotFound(String),

//     // PieceStateIsFailed is the error when the piece state is failed.
//     #[error{"piece {0} state is failed"}]
//     PieceStateIsFailed(String),

//     // WaitForPieceFinishedTimeout is the error when the wait for piece finished timeout.
//     #[error{"wait for piece {0} finished timeout"}]
//     WaitForPieceFinishedTimeout(String),

//     // AvailableManagerNotFound is the error when the available manager is not found.
//     #[error{"available manager not found"}]
//     AvailableManagerNotFound(),

//     // AvailableSchedulersNotFound is the error when the available schedulers is not found.
//     #[error{"available schedulers not found"}]
//     AvailableSchedulersNotFound(),

//     // DownloadFromRemotePeerFailed is the error when the download from remote peer is failed.
//     #[error(transparent)]
//     DownloadFromRemotePeerFailed(DownloadFromRemotePeerFailed),

//     // ColumnFamilyNotFound is the error when the column family is not found.
//     #[error{"column family {0} not found"}]
//     ColumnFamilyNotFound(String),

//     // InvalidStateTransition is the error when the state transition is invalid.
//     #[error{"can not transit from {0} to {1}"}]
//     InvalidStateTransition(String, String),

//     // InvalidState is the error when the state is invalid.
//     #[error{"invalid state {0}"}]
//     InvalidState(String),

//     // InvalidURI is the error when the uri is invalid.
//     #[error("invalid uri {0}")]
//     InvalidURI(String),

//     // InvalidPeer is the error when the peer is invalid.
//     #[error("invalid peer {0}")]
//     InvalidPeer(String),

//     // SchedulerClientNotFound is the error when the scheduler client is not found.
//     #[error{"scheduler client not found"}]
//     SchedulerClientNotFound(),

//     // UnexpectedResponse is the error when the response is unexpected.
//     #[error{"unexpected response"}]
//     UnexpectedResponse(),

//     // PieceDigestMismatch is the error when the piece digest is mismatch.
//     #[error{"piece digest mismatch"}]
//     PieceDigestMismatch(),

//     // MaxScheduleCountExceeded is the error when the max schedule count is exceeded.
//     #[error("max schedule count {0} exceeded")]
//     MaxScheduleCountExceeded(u32),

//     // InvalidContentLength is the error when the content length is invalid.
//     #[error("invalid content length")]
//     InvalidContentLength(),

//     // InvalidParameter is the error when the parameter is invalid.
//     #[error("invalid parameter")]
//     InvalidParameter(),

//     // Unknown is the error when the error is unknown.
//     #[error("unknown {0}")]
//     Unknown(String),

//     // Unimplemented is the error when the feature is not implemented.
//     #[error{"unimplemented"}]
//     Unimplemented(),
// }

// Result is the result for Client.