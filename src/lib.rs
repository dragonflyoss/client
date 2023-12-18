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

use reqwest::header::HeaderMap;

pub mod announcer;
pub mod backend;
pub mod config;
pub mod dynconfig;
pub mod gc;
pub mod grpc;
pub mod metrics;
pub mod proxy;
pub mod shutdown;
pub mod storage;
pub mod task;
pub mod tracing;
pub mod utils;

// Error is the error for Client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // RocksDB is the error for rocksdb.
    #[error(transparent)]
    RocksDB(#[from] rocksdb::Error),

    // JSON is the error for serde_json.
    #[error(transparent)]
    JSON(#[from] serde_json::Error),

    // IO is the error for IO operation.
    #[error(transparent)]
    IO(#[from] std::io::Error),

    // URLParse is the error for url parse.
    #[error(transparent)]
    URLParse(#[from] url::ParseError),

    // TonicTransport is the error for tonic transport.
    #[error(transparent)]
    TonicTransport(#[from] tonic::transport::Error),

    // TonicStatus is the error for tonic status.
    #[error(transparent)]
    TonicStatus(#[from] tonic::Status),

    // Reqwest is the error for reqwest.
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),

    // MpscSend is the error for send.
    #[error("mpsc send: {0}")]
    MpscSend(String),

    // AcquireError is the error for acquire.
    #[error("acquiring semaphore: {0}")]
    AcquireError(#[from] tokio::sync::AcquireError),

    // JoinError is the error for join.
    #[error(transparent)]
    JoinError(#[from] tokio::task::JoinError),

    #[error("send timeout")]
    SendTimeoutError(),

    // Reqwest is the error for reqwest.
    #[error(transparent)]
    HTTP(HTTPError),

    // HTTPResponseBuilding is the error for http response building.
    #[error(transparent)]
    HttpResponseBuilding(#[from] warp::http::Error),

    // Elapsed is the error for elapsed.
    #[error(transparent)]
    Elapsed(#[from] tokio_stream::Elapsed),

    // HashRing is the error for hashring.
    #[error{"hashring {0} is failed"}]
    HashRing(String),

    // HostNotFound is the error when the host is not found.
    #[error{"host {0} not found"}]
    HostNotFound(String),

    // TaskNotFound is the error when the task is not found.
    #[error{"task {0} not found"}]
    TaskNotFound(String),

    // PieceNotFound is the error when the piece is not found.
    #[error{"piece {0} not found"}]
    PieceNotFound(String),

    // PieceStateIsFailed is the error when the piece state is failed.
    #[error{"piece {0} state is failed"}]
    PieceStateIsFailed(String),

    // WaitForPieceFinishedTimeout is the error when the wait for piece finished timeout.
    #[error{"wait for piece {0} finished timeout"}]
    WaitForPieceFinishedTimeout(String),

    // AvailableSchedulersNotFound is the error when the available schedulers is not found.
    #[error{"available schedulers not found"}]
    AvailableSchedulersNotFound(),

    // DownloadFromRemotePeerFailed is the error when the download from remote peer is failed.
    #[error(transparent)]
    DownloadFromRemotePeerFailed(DownloadFromRemotePeerFailed),

    // ColumnFamilyNotFound is the error when the column family is not found.
    #[error{"column family {0} not found"}]
    ColumnFamilyNotFound(String),

    // InvalidStateTransition is the error when the state transition is invalid.
    #[error{"can not transit from {0} to {1}"}]
    InvalidStateTransition(String, String),

    // InvalidState is the error when the state is invalid.
    #[error{"invalid state {0}"}]
    InvalidState(String),

    // InvalidURI is the error when the uri is invalid.
    #[error("invalid uri {0}")]
    InvalidURI(String),

    // InvalidPeer is the error when the peer is invalid.
    #[error("invalid peer {0}")]
    InvalidPeer(String),

    // SchedulerClientNotFound is the error when the scheduler client is not found.
    #[error{"scheduler client not found"}]
    SchedulerClientNotFound(),

    // UnexpectedResponse is the error when the response is unexpected.
    #[error{"unexpected response"}]
    UnexpectedResponse(),

    // PieceDigestMismatch is the error when the piece digest is mismatch.
    #[error{"piece digest mismatch"}]
    PieceDigestMismatch(),

    // MaxScheduleCountExceeded is the error when the max schedule count is exceeded.
    #[error("max schedule count {0} exceeded")]
    MaxScheduleCountExceeded(u32),

    // InvalidContentLength is the error when the content length is invalid.
    #[error("invalid content length")]
    InvalidContentLength(),

    // InvalidParameter is the error when the parameter is invalid.
    #[error("invalid parameter")]
    InvalidParameter(),

    // Unknown is the error when the error is unknown.
    #[error("unknown {0}")]
    Unknown(String),

    // Unimplemented is the error when the feature is not implemented.
    #[error{"unimplemented"}]
    Unimplemented(),
}

// SendError is the error for send.
impl<T> From<tokio::sync::mpsc::error::SendError<T>> for Error {
    fn from(e: tokio::sync::mpsc::error::SendError<T>) -> Error {
        Error::MpscSend(e.to_string())
    }
}

// SendTimeoutError is the error for send timeout.
impl<T> From<tokio::sync::mpsc::error::SendTimeoutError<T>> for Error {
    fn from(err: tokio::sync::mpsc::error::SendTimeoutError<T>) -> Self {
        match err {
            tokio::sync::mpsc::error::SendTimeoutError::Timeout(_) => Error::SendTimeoutError(),
            tokio::sync::mpsc::error::SendTimeoutError::Closed(_) => Error::SendTimeoutError(),
        }
    }
}

// HttpError is the error for http.
#[derive(Debug, thiserror::Error)]
#[error("http error {status_code}")]
pub struct HTTPError {
    // status_code is the status code of the response.
    pub status_code: reqwest::StatusCode,

    // header is the headers of the response.
    pub header: HeaderMap,
}

// DownloadFromRemotePeerFailed is the error when the download from remote peer is failed.
#[derive(Debug, thiserror::Error)]
#[error("download piece {piece_number} from remote peer {parent_id} failed")]
pub struct DownloadFromRemotePeerFailed {
    // piece_number is the number of the piece.
    pub piece_number: u32,

    // parent_id is the parent id of the piece.
    pub parent_id: String,
}

// Result is the result for Client.
pub type Result<T> = std::result::Result<T, Error>;
