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

/// The type of the error.
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

/// Implements the display for the error type.
impl ErrorType {
    /// Returns the string of the error type.
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

/// The external error.
#[derive(Debug)]
pub struct ExternalError {
    pub etype: ErrorType,
    pub cause: Option<Box<dyn ErrorTrait + Send + Sync>>,
    pub context: Option<Message>,
}

/// Implements the error trait.
impl ExternalError {
    /// Returns a new ExternalError.
    pub fn new(etype: ErrorType) -> Self {
        ExternalError {
            etype,
            cause: None,
            context: None,
        }
    }

    /// Returns a new ExternalError with the context.
    pub fn with_context(mut self, message: impl Into<Message>) -> Self {
        self.context = Some(message.into());
        self
    }

    /// Returns a new ExternalError with the cause.
    pub fn with_cause(mut self, cause: Box<dyn ErrorTrait + Send + Sync>) -> Self {
        self.cause = Some(cause);
        self
    }

    /// Returns the display of the error with the previous error.
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
                write!(f, " cause: {c}")
            }
        } else {
            Ok(())
        }
    }
}

/// Implements the display for the error.
impl fmt::Display for ExternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.chain_display(None, f)
    }
}

/// Implements the error trait.
impl ErrorTrait for ExternalError {}

/// The trait to extend the result with error.
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

/// Implements the OrErr for Result.
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

/// The error for backend.
#[derive(Debug, thiserror::Error)]
#[error("backend error: {message}")]
pub struct BackendError {
    /// The error message.
    pub message: String,

    /// The status code of the response.
    pub status_code: Option<reqwest::StatusCode>,

    /// The headers of the response.
    pub header: Option<reqwest::header::HeaderMap>,
}

/// The error when the download from parent is failed.
#[derive(Debug, thiserror::Error)]
#[error("download piece {piece_number} from parent {parent_id} failed")]
pub struct DownloadFromParentFailed {
    /// The number of the piece.
    pub piece_number: u32,

    /// The parent id of the piece.
    pub parent_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn io_error() -> std::io::Error {
        std::io::Error::other("inner error")
    }

    #[test]
    fn as_str_names_each_error_type() {
        let test_cases = vec![
            (ErrorType::StorageError, "StorageError"),
            (ErrorType::ConfigError, "ConfigError"),
            (ErrorType::SerializeError, "SerializeError"),
            (ErrorType::ValidationError, "ValidationError"),
            (ErrorType::ParseError, "ParseError"),
            (ErrorType::CertificateError, "CertificateError"),
            (ErrorType::TLSConfigError, "TLSConfigError"),
            (ErrorType::AsyncRuntimeError, "AsyncRuntimeError"),
            (ErrorType::StreamError, "StreamError"),
            (ErrorType::ConnectError, "ConnectError"),
            (ErrorType::PluginError, "PluginError"),
        ];

        for (etype, expected) in test_cases {
            assert_eq!(etype.as_str(), expected);
        }
    }

    #[test]
    fn display_chains_type_context_and_cause() {
        let test_cases = vec![
            (
                ExternalError::new(ErrorType::StorageError).with_context("error message"),
                "StorageError context: error message",
            ),
            (
                ExternalError::new(ErrorType::StorageError)
                    .with_context("error message with owned string".to_string()),
                "StorageError context: error message with owned string",
            ),
            (
                ExternalError::new(ErrorType::StorageError)
                    .with_context("error message with owned string".to_string())
                    .with_cause(Box::new(io_error())),
                "StorageError context: error message with owned string cause: inner error",
            ),
            (
                ExternalError::new(ErrorType::StorageError)
                    .with_context("outer")
                    .with_cause(Box::new(Box::new(
                        ExternalError::new(ErrorType::ConfigError).with_context("inner"),
                    ))),
                "StorageError context: outer cause: ConfigError context: inner",
            ),
            (
                ExternalError::new(ErrorType::StorageError)
                    .with_context("outer")
                    .with_cause(Box::new(Box::new(
                        ExternalError::new(ErrorType::StorageError).with_context("inner"),
                    ))),
                "StorageError context: outer cause:  context: inner",
            ),
        ];

        for (error, expected) in test_cases {
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn or_err_and_or_context_wrap_the_cause() {
        let test_cases = vec![
            (
                Err::<(), _>(io_error()).or_err(ErrorType::StorageError),
                "StorageError cause: inner error",
            ),
            (
                Err::<(), _>(io_error()).or_context(ErrorType::StorageError, "error message"),
                "StorageError context: error message cause: inner error",
            ),
        ];

        for (result, expected) in test_cases {
            let error = result.unwrap_err();
            assert_eq!(error.to_string(), expected);
        }
    }
}
