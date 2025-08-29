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

use std::collections::HashMap;
use std::fmt;

/// ErrorSeverity defines the severity level of errors for better error handling strategies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ErrorSeverity {
    /// Info level errors that don't affect functionality
    Info,
    /// Warning level errors that may indicate potential issues
    Warning,
    /// Error level that affects functionality but system can continue
    Error,
    /// Critical errors that require immediate attention and may cause system failure
    Critical,
    /// Fatal errors that cause system shutdown
    Fatal,
}

impl ErrorSeverity {
    /// Returns the string representation of the severity level.
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorSeverity::Info => "INFO",
            ErrorSeverity::Warning => "WARNING", 
            ErrorSeverity::Error => "ERROR",
            ErrorSeverity::Critical => "CRITICAL",
            ErrorSeverity::Fatal => "FATAL",
        }
    }
}

/// ErrorDomain categorizes errors by business domain for better organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorDomain {
    /// Network related errors (connections, timeouts, DNS)
    Network,
    /// Storage related errors (disk I/O, database, cache)
    Storage,
    /// Authentication and authorization errors
    Security,
    /// Configuration and validation errors
    Config,
    /// P2P protocol specific errors
    Protocol,
    /// Task and piece management errors
    Task,
    /// Resource management errors (memory, file handles)
    Resource,
    /// External service integration errors
    External,
}

impl ErrorDomain {
    /// Returns the string representation of the error domain.
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorDomain::Network => "NETWORK",
            ErrorDomain::Storage => "STORAGE",
            ErrorDomain::Security => "SECURITY",
            ErrorDomain::Config => "CONFIG",
            ErrorDomain::Protocol => "PROTOCOL",
            ErrorDomain::Task => "TASK",
            ErrorDomain::Resource => "RESOURCE",
            ErrorDomain::External => "EXTERNAL",
        }
    }
}

/// ErrorCode provides a standardized error code system for API integration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ErrorCode(u32);

impl ErrorCode {
    /// Creates a new error code.
    pub const fn new(code: u32) -> Self {
        Self(code)
    }

    /// Returns the numeric value of the error code.
    pub fn value(&self) -> u32 {
        self.0
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DF{:04}", self.0)
    }
}

// Standard error codes organized by domain
impl ErrorCode {
    // Network domain (1000-1999)
    pub const NETWORK_CONNECTION_FAILED: ErrorCode = ErrorCode::new(1001);
    pub const NETWORK_TIMEOUT: ErrorCode = ErrorCode::new(1002);
    pub const NETWORK_DNS_RESOLUTION_FAILED: ErrorCode = ErrorCode::new(1003);
    pub const NETWORK_INVALID_ADDRESS: ErrorCode = ErrorCode::new(1004);
    
    // Storage domain (2000-2999) 
    pub const STORAGE_IO_ERROR: ErrorCode = ErrorCode::new(2001);
    pub const STORAGE_DISK_FULL: ErrorCode = ErrorCode::new(2002);
    pub const STORAGE_PERMISSION_DENIED: ErrorCode = ErrorCode::new(2003);
    pub const STORAGE_CORRUPTION: ErrorCode = ErrorCode::new(2004);
    
    // Security domain (3000-3999)
    pub const SECURITY_AUTHENTICATION_FAILED: ErrorCode = ErrorCode::new(3001);
    pub const SECURITY_AUTHORIZATION_DENIED: ErrorCode = ErrorCode::new(3002);
    pub const SECURITY_CERTIFICATE_INVALID: ErrorCode = ErrorCode::new(3003);
    pub const SECURITY_TLS_HANDSHAKE_FAILED: ErrorCode = ErrorCode::new(3004);
    
    // Config domain (4000-4999)
    pub const CONFIG_INVALID_FORMAT: ErrorCode = ErrorCode::new(4001);
    pub const CONFIG_MISSING_REQUIRED: ErrorCode = ErrorCode::new(4002);
    pub const CONFIG_VALIDATION_FAILED: ErrorCode = ErrorCode::new(4003);
    
    // Protocol domain (5000-5999)
    pub const PROTOCOL_INVALID_MESSAGE: ErrorCode = ErrorCode::new(5001);
    pub const PROTOCOL_VERSION_MISMATCH: ErrorCode = ErrorCode::new(5002);
    pub const PROTOCOL_UNSUPPORTED_OPERATION: ErrorCode = ErrorCode::new(5003);
    
    // Task domain (6000-6999)
    pub const TASK_NOT_FOUND: ErrorCode = ErrorCode::new(6001);
    pub const TASK_ALREADY_EXISTS: ErrorCode = ErrorCode::new(6002);
    pub const TASK_INVALID_STATE: ErrorCode = ErrorCode::new(6003);
    pub const TASK_TIMEOUT: ErrorCode = ErrorCode::new(6004);
    
    // Resource domain (7000-7999)
    pub const RESOURCE_EXHAUSTED: ErrorCode = ErrorCode::new(7001);
    pub const RESOURCE_LIMIT_EXCEEDED: ErrorCode = ErrorCode::new(7002);
    pub const RESOURCE_UNAVAILABLE: ErrorCode = ErrorCode::new(7003);
    
    // External domain (8000-8999)
    pub const EXTERNAL_SERVICE_UNAVAILABLE: ErrorCode = ErrorCode::new(8001);
    pub const EXTERNAL_API_ERROR: ErrorCode = ErrorCode::new(8002);
    pub const EXTERNAL_DEPENDENCY_FAILED: ErrorCode = ErrorCode::new(8003);
}

/// EnhancedError provides structured error information with context and metadata.
#[derive(Debug)]
pub struct EnhancedError {
    /// The error domain this error belongs to
    pub domain: ErrorDomain,
    /// The severity level of this error
    pub severity: ErrorSeverity,
    /// Standardized error code
    pub code: ErrorCode,
    /// Human readable error message
    pub message: String,
    /// Additional context information
    pub context: HashMap<String, String>,
    /// The underlying cause of this error
    pub cause: Option<Box<dyn std::error::Error + Send + Sync>>,
    /// Timestamp when the error occurred
    pub timestamp: std::time::SystemTime,
}

impl EnhancedError {
    /// Creates a new enhanced error.
    pub fn new(domain: ErrorDomain, severity: ErrorSeverity, code: ErrorCode, message: String) -> Self {
        Self {
            domain,
            severity,
            code,
            message,
            context: HashMap::new(),
            cause: None,
            timestamp: std::time::SystemTime::now(),
        }
    }

    /// Adds context information to the error.
    pub fn with_context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.context.insert(key.into(), value.into());
        self
    }

    /// Adds multiple context entries at once.
    pub fn with_context_map(mut self, context: HashMap<String, String>) -> Self {
        self.context.extend(context);
        self
    }

    /// Sets the underlying cause of this error.
    pub fn with_cause(mut self, cause: Box<dyn std::error::Error + Send + Sync>) -> Self {
        self.cause = Some(cause);
        self
    }

    /// Checks if this error can be retried.
    pub fn is_retryable(&self) -> bool {
        match self.domain {
            ErrorDomain::Network => matches!(self.code, ErrorCode::NETWORK_TIMEOUT | ErrorCode::NETWORK_CONNECTION_FAILED),
            ErrorDomain::Storage => matches!(self.code, ErrorCode::STORAGE_IO_ERROR),
            ErrorDomain::External => matches!(self.code, ErrorCode::EXTERNAL_SERVICE_UNAVAILABLE),
            _ => false,
        }
    }

    /// Returns suggested recovery actions for this error.
    pub fn recovery_suggestions(&self) -> Vec<&'static str> {
        match (self.domain, self.code) {
            (ErrorDomain::Network, ErrorCode::NETWORK_CONNECTION_FAILED) => {
                vec!["Check network connectivity", "Verify target address", "Retry after delay"]
            },
            (ErrorDomain::Storage, ErrorCode::STORAGE_DISK_FULL) => {
                vec!["Free up disk space", "Clean up temporary files", "Check disk quota"]
            },
            (ErrorDomain::Security, ErrorCode::SECURITY_CERTIFICATE_INVALID) => {
                vec!["Update certificates", "Check certificate expiry", "Verify certificate chain"]
            },
            _ => vec!["Check logs for details", "Contact system administrator"],
        }
    }
}

impl fmt::Display for EnhancedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}:{}:{}] {}", 
               self.domain.as_str(), 
               self.severity.as_str(),
               self.code,
               self.message)?;
        
        if !self.context.is_empty() {
            write!(f, " | Context: {:?}", self.context)?;
        }
        
        if let Some(cause) = &self.cause {
            write!(f, " | Caused by: {}", cause)?;
        }
        
        Ok(())
    }
}

impl std::error::Error for EnhancedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause.as_ref().map(|c| c.as_ref() as &(dyn std::error::Error + 'static))
    }
}

/// ErrorBuilder provides a fluent interface for building enhanced errors.
pub struct ErrorBuilder {
    error: EnhancedError,
}

impl ErrorBuilder {
    /// Creates a new error builder.
    pub fn new(domain: ErrorDomain, severity: ErrorSeverity, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            error: EnhancedError::new(domain, severity, code, message.into()),
        }
    }

    /// Adds context information.
    pub fn context(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.error = self.error.with_context(key, value);
        self
    }

    /// Sets the underlying cause.
    pub fn cause(mut self, cause: Box<dyn std::error::Error + Send + Sync>) -> Self {
        self.error = self.error.with_cause(cause);
        self
    }

    /// Builds the enhanced error.
    pub fn build(self) -> EnhancedError {
        self.error
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_severity_ordering() {
        assert!(ErrorSeverity::Info < ErrorSeverity::Warning);
        assert!(ErrorSeverity::Warning < ErrorSeverity::Error);
        assert!(ErrorSeverity::Error < ErrorSeverity::Critical);
        assert!(ErrorSeverity::Critical < ErrorSeverity::Fatal);
    }

    #[test]
    fn test_error_code_display() {
        assert_eq!(ErrorCode::NETWORK_CONNECTION_FAILED.to_string(), "DF1001");
        assert_eq!(ErrorCode::STORAGE_IO_ERROR.to_string(), "DF2001");
    }

    #[test]
    fn test_enhanced_error_creation() {
        let error = ErrorBuilder::new(
            ErrorDomain::Network, 
            ErrorSeverity::Error,
            ErrorCode::NETWORK_CONNECTION_FAILED,
            "Connection refused"
        )
        .context("host", "example.com")
        .context("port", "8080")
        .build();

        assert_eq!(error.domain, ErrorDomain::Network);
        assert_eq!(error.severity, ErrorSeverity::Error);
        assert_eq!(error.code, ErrorCode::NETWORK_CONNECTION_FAILED);
        assert!(error.is_retryable());
        assert!(error.context.contains_key("host"));
        assert!(error.context.contains_key("port"));
    }

    #[test]
    fn test_recovery_suggestions() {
        let error = EnhancedError::new(
            ErrorDomain::Storage,
            ErrorSeverity::Critical, 
            ErrorCode::STORAGE_DISK_FULL,
            "Disk space exhausted".to_string()
        );

        let suggestions = error.recovery_suggestions();
        assert!(suggestions.contains(&"Free up disk space"));
        assert!(suggestions.contains(&"Clean up temporary files"));
    }
}