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

use super::enhanced::{EnhancedError, ErrorBuilder, ErrorCode, ErrorDomain, ErrorSeverity};
use super::DFError;
use crate::Result;

/// Extension trait for Result<T, DFError> to provide enhanced error conversion and context.
pub trait DFErrorExt<T> {
    /// Converts DFError to EnhancedError with specified domain, severity and code.
    fn to_enhanced(self, domain: ErrorDomain, severity: ErrorSeverity, code: ErrorCode) -> std::result::Result<T, EnhancedError>;

    /// Converts DFError to EnhancedError with automatic mapping based on error type.
    fn to_enhanced_auto(self) -> std::result::Result<T, EnhancedError>;

    /// Adds context to the error and converts to EnhancedError.
    fn with_enhanced_context(
        self, 
        domain: ErrorDomain, 
        severity: ErrorSeverity, 
        code: ErrorCode,
        context_key: &str,
        context_value: &str
    ) -> std::result::Result<T, EnhancedError>;
}

impl<T> DFErrorExt<T> for Result<T> {
    fn to_enhanced(self, domain: ErrorDomain, severity: ErrorSeverity, code: ErrorCode) -> std::result::Result<T, EnhancedError> {
        self.map_err(|err| {
            let message = err.to_string();
            ErrorBuilder::new(domain, severity, code, message)
                .cause(Box::new(err))
                .build()
        })
    }

    fn to_enhanced_auto(self) -> std::result::Result<T, EnhancedError> {
        self.map_err(|err| {
            let (domain, severity, code) = match &err {
                // Network errors
                DFError::TonicStatus(_) | DFError::TonicTransportError(_) => {
                    (ErrorDomain::Network, ErrorSeverity::Error, ErrorCode::NETWORK_CONNECTION_FAILED)
                },
                DFError::ReqwestError(_) | DFError::ReqwestMiddlewareError(_) => {
                    (ErrorDomain::Network, ErrorSeverity::Error, ErrorCode::NETWORK_CONNECTION_FAILED)
                },

                // Storage errors
                DFError::IO(_) => {
                    (ErrorDomain::Storage, ErrorSeverity::Error, ErrorCode::STORAGE_IO_ERROR)
                },
                DFError::NoSpace(_) => {
                    (ErrorDomain::Storage, ErrorSeverity::Critical, ErrorCode::STORAGE_DISK_FULL)
                },
                DFError::ColumnFamilyNotFound(_) => {
                    (ErrorDomain::Storage, ErrorSeverity::Error, ErrorCode::STORAGE_CORRUPTION)
                },

                // Security errors  
                DFError::Unauthorized => {
                    (ErrorDomain::Security, ErrorSeverity::Error, ErrorCode::SECURITY_AUTHORIZATION_DENIED)
                },

                // Config errors
                DFError::InvalidParameter | DFError::ValidationError(_) => {
                    (ErrorDomain::Config, ErrorSeverity::Error, ErrorCode::CONFIG_VALIDATION_FAILED)
                },
                DFError::InvalidURI(_) | DFError::URLParseError(_) => {
                    (ErrorDomain::Config, ErrorSeverity::Error, ErrorCode::CONFIG_INVALID_FORMAT)
                },

                // Task errors
                DFError::TaskNotFound(_) => {
                    (ErrorDomain::Task, ErrorSeverity::Warning, ErrorCode::TASK_NOT_FOUND)
                },
                DFError::PieceNotFound(_) => {
                    (ErrorDomain::Task, ErrorSeverity::Warning, ErrorCode::TASK_NOT_FOUND)
                },
                DFError::InvalidState(_) | DFError::InvalidStateTransition(_, _) => {
                    (ErrorDomain::Task, ErrorSeverity::Error, ErrorCode::TASK_INVALID_STATE)
                },
                DFError::DownloadPieceFinished(_) | DFError::WaitForPieceFinishedTimeout(_) => {
                    (ErrorDomain::Task, ErrorSeverity::Warning, ErrorCode::TASK_TIMEOUT)
                },

                // Resource errors
                DFError::MaxScheduleCountExceeded(_) | DFError::MaxDownloadFilesExceeded(_) => {
                    (ErrorDomain::Resource, ErrorSeverity::Warning, ErrorCode::RESOURCE_LIMIT_EXCEEDED)
                },

                // External errors
                DFError::BackendError(_) | DFError::OpenDALError(_) => {
                    (ErrorDomain::External, ErrorSeverity::Error, ErrorCode::EXTERNAL_SERVICE_UNAVAILABLE)
                },

                // Protocol errors
                DFError::UnexpectedResponse | DFError::DigestMismatch(_, _) | DFError::ContentLengthMismatch(_, _) => {
                    (ErrorDomain::Protocol, ErrorSeverity::Error, ErrorCode::PROTOCOL_INVALID_MESSAGE)
                },
                DFError::Unsupported(_) | DFError::Unimplemented => {
                    (ErrorDomain::Protocol, ErrorSeverity::Error, ErrorCode::PROTOCOL_UNSUPPORTED_OPERATION)
                },

                // Default mapping
                _ => (ErrorDomain::External, ErrorSeverity::Error, ErrorCode::new(9999)),
            };

            let message = err.to_string();
            ErrorBuilder::new(domain, severity, code, message)
                .cause(Box::new(err))
                .build()
        })
    }

    fn with_enhanced_context(
        self, 
        domain: ErrorDomain, 
        severity: ErrorSeverity, 
        code: ErrorCode,
        context_key: &str,
        context_value: &str
    ) -> std::result::Result<T, EnhancedError> {
        self.map_err(|err| {
            let message = err.to_string();
            ErrorBuilder::new(domain, severity, code, message)
                .context(context_key, context_value)
                .cause(Box::new(err))
                .build()
        })
    }
}

/// Helper functions for creating common enhanced errors.
pub struct ErrorHelpers;

impl ErrorHelpers {
    /// Creates a network connection error.
    pub fn network_connection_failed(host: &str, port: u16, cause: Option<Box<dyn std::error::Error + Send + Sync>>) -> EnhancedError {
        let mut builder = ErrorBuilder::new(
            ErrorDomain::Network,
            ErrorSeverity::Error,
            ErrorCode::NETWORK_CONNECTION_FAILED,
            format!("Failed to connect to {}:{}", host, port)
        )
        .context("host", host)
        .context("port", port.to_string());

        if let Some(cause) = cause {
            builder = builder.cause(cause);
        }

        builder.build()
    }

    /// Creates a storage I/O error.
    pub fn storage_io_error(operation: &str, path: &str, cause: Option<Box<dyn std::error::Error + Send + Sync>>) -> EnhancedError {
        let mut builder = ErrorBuilder::new(
            ErrorDomain::Storage,
            ErrorSeverity::Error,
            ErrorCode::STORAGE_IO_ERROR,
            format!("I/O error during {}: {}", operation, path)
        )
        .context("operation", operation)
        .context("path", path);

        if let Some(cause) = cause {
            builder = builder.cause(cause);
        }

        builder.build()
    }

    /// Creates a task timeout error.
    pub fn task_timeout(task_id: &str, timeout_seconds: u64) -> EnhancedError {
        ErrorBuilder::new(
            ErrorDomain::Task,
            ErrorSeverity::Warning,
            ErrorCode::TASK_TIMEOUT,
            format!("Task {} timed out after {}s", task_id, timeout_seconds)
        )
        .context("task_id", task_id)
        .context("timeout_seconds", timeout_seconds.to_string())
        .build()
    }

    /// Creates a configuration validation error.
    pub fn config_validation_error(field: &str, value: &str, reason: &str) -> EnhancedError {
        ErrorBuilder::new(
            ErrorDomain::Config,
            ErrorSeverity::Error,
            ErrorCode::CONFIG_VALIDATION_FAILED,
            format!("Invalid configuration for field '{}': {}", field, reason)
        )
        .context("field", field)
        .context("value", value)
        .context("reason", reason)
        .build()
    }

    /// Creates an authorization denied error.
    pub fn authorization_denied(resource: &str, action: &str) -> EnhancedError {
        ErrorBuilder::new(
            ErrorDomain::Security,
            ErrorSeverity::Error,
            ErrorCode::SECURITY_AUTHORIZATION_DENIED,
            format!("Access denied to {} for action '{}'", resource, action)
        )
        .context("resource", resource)
        .context("action", action)
        .build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io;

    #[test]
    fn test_df_error_to_enhanced_auto() {
        let io_error = DFError::IO(io::Error::new(io::ErrorKind::PermissionDenied, "access denied"));
        let result: Result<()> = Err(io_error);
        
        let enhanced_result = result.to_enhanced_auto();
        assert!(enhanced_result.is_err());
        
        let enhanced_error = enhanced_result.unwrap_err();
        assert_eq!(enhanced_error.domain, ErrorDomain::Storage);
        assert_eq!(enhanced_error.code, ErrorCode::STORAGE_IO_ERROR);
    }

    #[test]
    fn test_error_helpers_network() {
        let error = ErrorHelpers::network_connection_failed("example.com", 8080, None);
        assert_eq!(error.domain, ErrorDomain::Network);
        assert_eq!(error.code, ErrorCode::NETWORK_CONNECTION_FAILED);
        assert!(error.context.contains_key("host"));
        assert!(error.context.contains_key("port"));
        assert!(error.is_retryable());
    }

    #[test] 
    fn test_error_helpers_task_timeout() {
        let error = ErrorHelpers::task_timeout("task_123", 300);
        assert_eq!(error.domain, ErrorDomain::Task);
        assert_eq!(error.code, ErrorCode::TASK_TIMEOUT);
        assert_eq!(error.context.get("task_id"), Some(&"task_123".to_string()));
        assert_eq!(error.context.get("timeout_seconds"), Some(&"300".to_string()));
    }
}