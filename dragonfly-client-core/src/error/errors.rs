use std::{error::Error as ErrorTrait, fmt};

use super::message::Message;

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ErrorType {
    StorageError,
    HTTPError,
    ConfigError,
    SerializeError,
    ValidationError,
    ParseError,
    CertificateError,
    TLSConfigError,
    AsyncRuntimeError,
    StreamError,
    ConnectError,
}

impl ErrorType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorType::StorageError => "StorageError",
            ErrorType::HTTPError => "HTTPError",
            ErrorType::ConfigError => "ConfigError",
            ErrorType::ValidationError => "ValidationError",
            ErrorType::ParseError => "ParseError",
            ErrorType::CertificateError => "CertificateError",
            ErrorType::SerializeError => "SerializeError",
            ErrorType::TLSConfigError => "TLSConfigError",
            ErrorType::AsyncRuntimeError => "AsyncRuntimeError",
            ErrorType::StreamError => "StreamError",
            ErrorType::ConnectError => "ConnectError",
        }
    }
}

#[derive(Debug)]
pub struct ExternalError {
    pub etype: ErrorType,
    pub cause: Option<Box<dyn ErrorTrait + Send + Sync>>,
    pub context: Option<Message>,
}

impl ExternalError {
    pub fn new(etype: ErrorType) -> Self {
        ExternalError {
            etype,
            cause: None,
            context: None,
        }
    }

    pub fn with_context(mut self, message: impl Into<Message>) -> Self {
        self.context = Some(message.into());
        self
    }

    pub fn with_cause(mut self, cause: Box<dyn ErrorTrait + Send + Sync>) -> Self {
        self.cause = Some(cause);
        self
    }

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

impl fmt::Display for ExternalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.chain_display(None, f)
    }
}

impl ErrorTrait for ExternalError {}

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

impl<T, E> OrErr<T, E> for Result<T, E> {
    fn or_err(self, et: ErrorType) -> Result<T, ExternalError>
    where
        E: Into<Box<dyn ErrorTrait + Send + Sync>>,
    {
        self.map_err(|e| ExternalError::new(et).with_cause(e.into()))
    }

    fn or_context(self, et: ErrorType, context: &'static str) -> Result<T, ExternalError>
    where
        E: Into<Box<dyn ErrorTrait + Send + Sync>>,
    {
        self.map_err(|e| {
            ExternalError::new(et)
                .with_cause(e.into())
                .with_context(context)
        })
    }
}

// HttpError is the error for http.
#[derive(Debug, thiserror::Error)]
#[error("http error {status_code}")]
pub struct HTTPError {
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
}
