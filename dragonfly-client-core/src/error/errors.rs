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

use std::{error::Error as ErrorTrait, fmt};

use super::message::Message;

// ErrorType is the type of the error.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ErrorType {
    StorageError,
    ConfigError,
    SerializeError,
    ValidationError,
    ParseError,
    CertificateError,
    TLSConfigError,
    AsyncRuntimeError,
    StreamError,
    ConnectError,
    PluginError,
}

// ErrorType implements the display for the error type.
impl ErrorType {
    // as_str returns the string of the error type.
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorType::StorageError => "StorageError",
            ErrorType::ConfigError => "ConfigError",
            ErrorType::ValidationError => "ValidationError",
            ErrorType::ParseError => "ParseError",
            ErrorType::CertificateError => "CertificateError",
            ErrorType::SerializeError => "SerializeError",
            ErrorType::TLSConfigError => "TLSConfigError",
            ErrorType::AsyncRuntimeError => "AsyncRuntimeError",
            ErrorType::StreamError => "StreamError",
            ErrorType::ConnectError => "ConnectError",
            ErrorType::PluginError => "PluginError",
        }
    }
}

// ExternalError is the external error.
#[derive(Debug)]
pub struct ExternalError {
    pub etype: ErrorType,
    pub cause: Option<Box<dyn ErrorTrait + Send + Sync>>,
    pub context: Option<Message>,
}

// ExternalError implements the error trait.
impl ExternalError {
    // new returns a new ExternalError.
    pub fn new(etype: ErrorType) -> Self {
        ExternalError {
            etype,
            cause: None,
            context: None,
        }
    }

    // with_context returns a new ExternalError with the context.
    pub fn with_context(mut self, message: impl Into<Message>) -> Self {
        self.context = Some(message.into());
        self
    }

    // with_cause returns a new ExternalError with the cause.
    pub fn with_cause(mut self, cause: Box<dyn ErrorTrait + Send + Sync>) -> Self {
        self.cause = Some(cause);
        self
    }

    // chain_display returns the display of the error with the previous error.
    fn chain_display(
        &self,
        previous: Option<&ExternalError>,
        f: &mut fmt::Formatter<'_>,
    ) -> fmt::Result {
        if previous.map(|p| p.etype != self.etype).unwrap_or(true) {
            write!(f, "{}", self.etype.as_str())?
        }

        if let Some(c) = self.context.as_ref() {
            write!(f, " context: {}", c.as_str())?;
        }

        if let Some(c) = self.cause.as_ref() {
            if let Some(e) = c.downcast_ref::<Box<ExternalError>>() {
                write!(f, " cause: ")?;
                e.chain_display(Some(self), f)
            } else {
                write!(f, " cause: {}", c)
            }
        } else {
            Ok(())
        }
    }
}

// ExternalError implements the display for the error.
impl fmt::Display for ExternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.chain_display(None, f)
    }
}

// ExternalError implements the error trait.
impl ErrorTrait for ExternalError {}

// OrErr is the trait to extend the result with error.
pub trait OrErr<T, E> {
    /// Wrap the E in [Result] with new [ErrorType] and context, the existing E will be the cause.
    ///
    /// This is a shortcut for map_err() + because()
    fn or_err(self, et: ErrorType) -> Result<T, ExternalError>
    where
        E: Into<Box<dyn ErrorTrait + Send + Sync>>;

    fn or_context(self, et: ErrorType, context: &'static str) -> Result<T, ExternalError>
    where
        E: Into<Box<dyn ErrorTrait + Send + Sync>>;
}

// OrErr implements the OrErr for Result.
impl<T, E> OrErr<T, E> for Result<T, E> {
    fn or_err(self, et: ErrorType) -> Result<T, ExternalError>
    where
        E: Into<Box<dyn ErrorTrait + Send + Sync>>,
    {
        self.map_err(|err| ExternalError::new(et).with_cause(err.into()))
    }

    fn or_context(self, et: ErrorType, context: &'static str) -> Result<T, ExternalError>
    where
        E: Into<Box<dyn ErrorTrait + Send + Sync>>,
    {
        self.map_err(|err| {
            ExternalError::new(et)
                .with_cause(err.into())
                .with_context(context)
        })
    }
}

// BackendError is the error for backend.
#[derive(Debug, thiserror::Error)]
#[error("backend error {message}")]
pub struct BackendError {
    // message is the error message.
    pub message: String,

    // status_code is the status code of the response.
    pub status_code: reqwest::StatusCode,

    // header is the headers of the response.
    pub header: reqwest::header::HeaderMap,
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_create_error() {
        let error = ExternalError::new(ErrorType::StorageError).with_context("error message");
        assert_eq!(format!("{}", error), "StorageError context: error message");

        let error = ExternalError::new(ErrorType::StorageError)
            .with_context(format!("error message {}", "with owned string"));
        assert_eq!(
            format!("{}", error),
            "StorageError context: error message with owned string"
        );

        let error = ExternalError::new(ErrorType::StorageError)
            .with_context(format!("error message {}", "with owned string"))
            .with_cause(Box::new(std::io::Error::new(
                std::io::ErrorKind::Other,
                "inner error",
            )));

        assert_eq!(
            format!("{}", error),
            "StorageError context: error message with owned string cause: inner error"
        );
    }

    #[test]
    fn should_extend_result_with_error() {
        let result: Result<(), std::io::Error> = Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "inner error",
        ));

        let error = result.or_err(ErrorType::StorageError).unwrap_err();
        assert_eq!(format!("{}", error), "StorageError cause: inner error");

        let result: Result<(), std::io::Error> = Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "inner error",
        ));

        let error = result
            .or_context(ErrorType::StorageError, "error message")
            .unwrap_err();

        assert_eq!(
            format!("{}", error),
            "StorageError context: error message cause: inner error"
        );
    }
}
