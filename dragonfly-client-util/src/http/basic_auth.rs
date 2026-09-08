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

/// The credentials for the basic auth.
pub struct Credentials {
    /// The username.
    pub username: String,

    /// The password.
    pub password: String,
}

/// The basic auth.
impl Credentials {
    /// Returns a new Credentials.
    pub fn new(username: &str, password: &str) -> Credentials {
        Self {
            username: username.to_string(),
            password: password.to_string(),
        }
    }

    /// Verifies the basic auth with the header.
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
    #![allow(clippy::type_complexity)]

    use super::*;
    use http::header::{HeaderValue, AUTHORIZATION};

    #[test]
    fn verify_accepts_only_matching_basic_credentials() {
        let test_cases: Vec<(Option<&str>, fn(Result<()>))> = vec![
            (None, |result| {
                assert!(matches!(result, Err(Error::Unauthorized)))
            }),
            (Some("Bearer some_token"), |result| {
                assert!(matches!(result, Err(Error::Unauthorized)))
            }),
            (Some("Basic invalid_base64"), |result| {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "ParseError cause: Invalid symbol 95, offset 7."
                )
            }),
            (Some("Basic //46eA=="), |result| {
                assert_eq!(
                    result.unwrap_err().to_string(),
                    "ParseError cause: invalid utf-8 sequence of 1 bytes from index 0"
                )
            }),
            (Some("Basic dXNlcg=="), |result| {
                assert!(matches!(result, Err(Error::Unauthorized)))
            }),
            (Some("Basic dXNlcjpwYXNzX2Vycm9y"), |result| {
                assert!(matches!(result, Err(Error::Unauthorized)))
            }),
            (Some("Basic dXNlcjpwYXNz"), |result| assert!(result.is_ok())),
            (Some("basic dXNlcjpwYXNz"), |result| assert!(result.is_ok())),
        ];

        let credentials = Credentials::new("user", "pass");
        for (authorization, expect) in test_cases {
            let mut header = HeaderMap::new();
            if let Some(authorization) = authorization {
                header.insert(AUTHORIZATION, HeaderValue::from_str(authorization).unwrap());
            }

            expect(credentials.verify(&header));
        }
    }
}
