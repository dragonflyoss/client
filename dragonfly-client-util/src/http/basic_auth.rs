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

use base64::prelude::*;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use http::header::{self, HeaderMap};

/// Credentials is the credentials for the basic auth.
pub struct Credentials {
    /// username is the username.
    pub username: String,

    /// password is the password.
    pub password: String,
}

/// Credentials is the basic auth.
impl Credentials {
    /// Creates a new Credentials.
    pub fn new(username: &str, password: &str) -> Credentials {
        Self {
            username: username.to_string(),
            password: password.to_string(),
        }
    }

    /// verify verifies the basic auth with the header.
    pub fn verify(&self, header: &HeaderMap) -> Result<()> {
        let Some(auth_header) = header.get(header::AUTHORIZATION) else {
            return Err(Error::Unauthorized);
        };

        if let Some((typ, payload)) = auth_header
            .to_str()
            .or_err(ErrorType::ParseError)?
            .to_string()
            .split_once(' ')
        {
            if typ.to_lowercase() != "basic" {
                return Err(Error::Unauthorized);
            };

            let decoded = String::from_utf8(
                BASE64_STANDARD
                    .decode(payload)
                    .or_err(ErrorType::ParseError)?,
            )
            .or_err(ErrorType::ParseError)?;

            let Some((username, password)) = decoded.split_once(':') else {
                return Err(Error::Unauthorized);
            };

            if username != self.username || password != self.password {
                return Err(Error::Unauthorized);
            }

            return Ok(());
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::header::HeaderValue;

    #[test]
    fn test_verify_no_auth_header() {
        let credentials = Credentials::new("user", "pass");
        let header = HeaderMap::new();

        let result = credentials.verify(&header);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Unauthorized));
    }

    #[test]
    fn test_verify_invalid_auth_type() {
        let credentials = Credentials::new("user", "pass");
        let mut header = HeaderMap::new();
        header.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer some_token"),
        );

        let result = credentials.verify(&header);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Unauthorized));
    }

    #[test]
    fn test_verify_invalid_base64() {
        let credentials = Credentials::new("user", "pass");
        let mut header = HeaderMap::new();
        header.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic invalid_base64"),
        );

        let result = credentials.verify(&header);

        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.err().unwrap()),
            format!(
                "{:?} cause: Invalid symbol 95, offset 7.",
                ErrorType::ParseError
            ),
        );
    }

    #[test]
    fn test_verify_invalid_format() {
        let credentials = Credentials::new("user", "pass");
        let mut header = HeaderMap::new();
        header.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic dXNlcg=="), // "user" in Base64
        );

        let result = credentials.verify(&header);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Unauthorized));
    }

    #[test]
    fn test_verify_incorrect_credentials() {
        let credentials = Credentials::new("user", "pass");
        let mut header = HeaderMap::new();
        header.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic dXNlcjpwYXNzX2Vycm9y"), // "user:pass_error" in Base64
        );

        let result = credentials.verify(&header);

        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), Error::Unauthorized));
    }

    #[test]
    fn test_verify_correct_credentials() {
        let credentials = Credentials::new("user", "pass");
        let mut header = HeaderMap::new();
        header.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic dXNlcjpwYXNz"), // "user:pass" in Base64
        );

        let result = credentials.verify(&header);

        assert!(result.is_ok());
    }
}
