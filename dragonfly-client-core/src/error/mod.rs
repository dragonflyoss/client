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

pub mod errors;
pub mod message;

pub use errors::ErrorType;
pub use errors::ExternalError;

pub use errors::OrErr;
pub use errors::{BackendError, DownloadFromParentFailed};

/// DFError is the error for dragonfly.
#[derive(thiserror::Error, Debug)]
pub enum DFError {
    /// IO is the error for IO operation.
    #[error(transparent)]
    IO(#[from] std::io::Error),

    /// MpscSend is the error for send.
    #[error("mpsc send: {0}")]
    MpscSend(String),

    /// SendTimeout is the error for send timeout.
    #[error("send timeout")]
    SendTimeout,

    /// HashRing is the error for hashring.
    #[error{"hashring {0} is failed"}]
    HashRing(String),

    /// NoSpace is the error when there is no space left on device.
    #[error("no space left on device: {0}")]
    NoSpace(String),

    /// HostNotFound is the error when the host is not found.
    #[error{"host {0} not found"}]
    HostNotFound(String),

    /// TaskNotFound is the error when the task is not found.
    #[error{"task {0} not found"}]
    TaskNotFound(String),

    /// PieceNotFound is the error when the piece is not found.
    #[error{"piece {0} not found"}]
    PieceNotFound(String),

    /// PieceStateIsFailed is the error when the piece state is failed.
    #[error{"piece {0} state is failed"}]
    PieceStateIsFailed(String),

    /// DownloadPieceFinished is the error when the download piece finished timeout.
    #[error{"download piece {0} finished timeout"}]
    DownloadPieceFinishedTimeout(String),

    /// WaitForPieceFinishedTimeout is the error when the wait for piece finished timeout.
    #[error{"wait for piece {0} finished timeout"}]
    WaitForPieceFinishedTimeout(String),

    /// AvailableManagerNotFound is the error when the available manager is not found.
    #[error{"available manager not found"}]
    AvailableManagerNotFound,

    /// AvailableSchedulersNotFound is the error when the available schedulers is not found.
    #[error{"available schedulers not found"}]
    AvailableSchedulersNotFound,

    /// DownloadFromParentFailed is the error when the download from parent is failed.
    #[error(transparent)]
    DownloadFromParentFailed(DownloadFromParentFailed),

    /// ColumnFamilyNotFound is the error when the column family is not found.
    #[error{"column family {0} not found"}]
    ColumnFamilyNotFound(String),

    /// InvalidStateTransition is the error when the state transition is invalid.
    #[error{"can not transit from {0} to {1}"}]
    InvalidStateTransition(String, String),

    /// InvalidState is the error when the state is invalid.
    #[error{"invalid state {0}"}]
    InvalidState(String),

    /// InvalidURI is the error when the uri is invalid.
    #[error("invalid uri {0}")]
    InvalidURI(String),

    /// InvalidPeer is the error when the peer is invalid.
    #[error("invalid peer {0}")]
    InvalidPeer(String),

    /// SchedulerClientNotFound is the error when the scheduler client is not found.
    #[error{"scheduler client not found"}]
    SchedulerClientNotFound,

    /// UnexpectedResponse is the error when the response is unexpected.
    #[error{"unexpected response"}]
    UnexpectedResponse,

    /// DigestMismatch is the error when the digest is mismatch.
    #[error{"digest mismatch expected: {0}, actual: {1}"}]
    DigestMismatch(String, String),

    /// ContentLengthMismatch is the error when the content length is mismatch.
    #[error("content length mismatch expected: {0}, actual: {1}")]
    ContentLengthMismatch(u64, u64),

    /// MaxScheduleCountExceeded is the error when the max schedule count is exceeded.
    #[error("max schedule count {0} exceeded")]
    MaxScheduleCountExceeded(u32),

    /// InvalidContentLength is the error when the content length is invalid.
    #[error("invalid content length")]
    InvalidContentLength,

    /// InvalidPieceLength is the error when the piece length is invalid.
    #[error("invalid piece length")]
    InvalidPieceLength,

    /// InvalidParameter is the error when the parameter is invalid.
    #[error("invalid parameter")]
    InvalidParameter,

    /// Infallible is the error for infallible.
    #[error(transparent)]
    Infallible(#[from] std::convert::Infallible),

    /// Utf8 is the error for utf8.
    #[error(transparent)]
    Utf8(#[from] std::str::Utf8Error),

    /// Unknown is the error when the error is unknown.
    #[error("unknown {0}")]
    Unknown(String),

    /// Unimplemented is the error when the feature is not implemented.
    #[error{"unimplemented"}]
    Unimplemented,

    /// EmptyHTTPRangeError is the error when the range fallback error is empty.
    #[error{"RangeUnsatisfiable: Failed to parse range fallback error, please file an issue"}]
    EmptyHTTPRangeError,

    /// Unauthorized is the error for unauthorized.
    #[error{"unauthorized"}]
    Unauthorized,

    /// ArrayTryFromSliceError is the error for array try from slice.
    #[error(transparent)]
    ArrayTryFromSliceError(#[from] std::array::TryFromSliceError),

    /// VortexProtocolStatus is the error for vortex protocol status.
    #[error("vortex protocol status: code={0:?}, message={1}")]
    VortexProtocolStatus(vortex_protocol::tlv::error::Code, String),

    /// VortexProtocolError is the error for vortex protocol.
    #[error(transparent)]
    VortexProtocolError(#[from] vortex_protocol::error::Error),

    /// TonicStatus is the error for tonic status.
    #[error(transparent)]
    TonicStatus(#[from] tonic::Status),

    /// TonicTransportError is the error for tonic transport.
    #[error(transparent)]
    TonicTransportError(#[from] tonic::transport::Error),

    /// TonicReflectionServerError is the error for tonic reflection server.
    #[error(transparent)]
    TonicReflectionServerError(#[from] tonic_reflection::server::Error),

    /// TonicStreamElapsed is the error for tonic stream elapsed.
    #[error(transparent)]
    TokioStreamElapsed(#[from] tokio_stream::Elapsed),

    // TokioTimeErrorElapsed is the error for tokio time elapsed.
    #[error(transparent)]
    TokioTimeErrorElapsed(#[from] tokio::time::error::Elapsed),

    /// HeadersError is the error for headers.
    #[error(transparent)]
    HeadersError(#[from] headers::Error),

    // InvalidHeaderName is the error for invalid header name.
    #[error(transparent)]
    HTTTHeaderInvalidHeaderName(#[from] http::header::InvalidHeaderName),

    // InvalidHeaderValue is the error for invalid header value.
    #[error(transparent)]
    HTTTHeaderInvalidHeaderValue(#[from] http::header::InvalidHeaderValue),

    /// URLParseError is the error for url parse.
    #[error(transparent)]
    URLParseError(#[from] url::ParseError),

    /// ReqwestError is the error for reqwest.
    #[error(transparent)]
    ReqwestError(#[from] reqwest::Error),

    /// ReqwestMiddlewareError is the error for reqwest middleware.
    #[error(transparent)]
    ReqwestMiddlewareError(#[from] reqwest_middleware::Error),

    /// OpenDALError is the error for opendal.
    #[error(transparent)]
    OpenDALError(#[from] opendal::Error),

    /// HyperError is the error for hyper.
    #[error(transparent)]
    HyperError(#[from] hyper::Error),

    /// BackendError is the error for backend.
    #[error(transparent)]
    BackendError(Box<BackendError>),

    /// HyperUtilClientLegacyError is the error for hyper util client legacy.
    #[error(transparent)]
    HyperUtilClientLegacyError(#[from] hyper_util::client::legacy::Error),

    /// ExternalError is the error for external error.
    #[error(transparent)]
    ExternalError(#[from] ExternalError),

    /// MaxDownloadFilesExceeded is the error for max download files exceeded.
    #[error("max number of files to download exceeded: {0}")]
    MaxDownloadFilesExceeded(usize),

    /// DigestManifestParseFailed is the error for digest manifest parsing failure.
    #[error("digest manifest {filepath} parse failed: {error}")]
    DigestManifestParseFailed { filepath: String, error: String },

    /// MissingDigestEntry is the error when a non-directory entry has no corresponding digest.
    #[error("missing digest for entry: {entry_url}")]
    MissingDigestEntry { entry_url: String },

    /// Unsupported is the error for unsupported.
    #[error("unsupported {0}")]
    Unsupported(String),

    /// TokioJoinError is the error for tokio join.
    #[error(transparent)]
    TokioJoinError(tokio::task::JoinError),

    /// ValidationError is the error for validate.
    #[error("validate failed: {0}")]
    ValidationError(String),
}

/// SendError is the error for send.
impl<T> From<tokio::sync::mpsc::error::SendError<T>> for DFError {
    fn from(e: tokio::sync::mpsc::error::SendError<T>) -> Self {
        Self::MpscSend(e.to_string())
    }
}

/// SendTimeoutError is the error for send timeout.
impl<T> From<tokio::sync::mpsc::error::SendTimeoutError<T>> for DFError {
    fn from(err: tokio::sync::mpsc::error::SendTimeoutError<T>) -> Self {
        match err {
            tokio::sync::mpsc::error::SendTimeoutError::Timeout(_) => Self::SendTimeout,
            tokio::sync::mpsc::error::SendTimeoutError::Closed(_) => Self::SendTimeout,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_convert_externalerror_to_dferror() {
        fn function_return_inner_error() -> Result<(), std::io::Error> {
            let inner_error = std::io::Error::new(std::io::ErrorKind::Other, "inner error");
            Err(inner_error)
        }

        fn do_sth_with_error() -> Result<(), DFError> {
            function_return_inner_error().map_err(|err| {
                ExternalError::new(crate::error::ErrorType::StorageError).with_cause(err.into())
            })?;
            Ok(())
        }

        let err = do_sth_with_error().err().unwrap();
        assert_eq!(format!("{}", err), "StorageError cause: inner error");
    }
}
