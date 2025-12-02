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

use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use lazy_static::lazy_static;
use regex::Regex;
use sha2::Digest as Sha2Digest;
use std::fmt;
use std::io::{self, Read};
use std::path::Path;
use std::str::FromStr;
use tracing::instrument;

/// SEPARATOR is the separator of digest.
pub const SEPARATOR: &str = ":";

lazy_static! {
    /// BLOB_URL_REGEX is the regex for oci blob url, e.g. http(s)://<registry>/v2/<repository>/blobs/<digest>.
    static ref BLOB_URL_REGEX: Regex = Regex::new(r"^(.*)://(.*)/v2/(.*)/blobs/(.*)$").unwrap();
}

/// is_blob_url checks if the url is an oci blob url.
pub fn is_blob_url(url: &str) -> bool {
    BLOB_URL_REGEX.is_match(url)
}

/// Algorithm is an enum of the algorithm that is used to generate digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    /// Crc32 is crc32 algorithm for generate digest.
    Crc32,

    /// Sha256 is sha256 algorithm for generate digest.
    Sha256,

    /// Sha512 is sha512 algorithm for generate digest.
    Sha512,
}

/// Algorithm implements the Display.
impl fmt::Display for Algorithm {
    /// fmt formats the value using the given formatter.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Algorithm::Crc32 => write!(f, "crc32"),
            Algorithm::Sha256 => write!(f, "sha256"),
            Algorithm::Sha512 => write!(f, "sha512"),
        }
    }
}

/// Algorithm implements the FromStr.
impl FromStr for Algorithm {
    type Err = String;

    /// from_str parses an algorithm string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "crc32" => Ok(Algorithm::Crc32),
            "sha256" => Ok(Algorithm::Sha256),
            "sha512" => Ok(Algorithm::Sha512),
            _ => Err(format!("invalid digest algorithm: {}", s)),
        }
    }
}

/// Digest is a struct that is used to generate digest.
pub struct Digest {
    /// algorithm is the algorithm that is used to generate digest.
    algorithm: Algorithm,

    /// encoded is the encoded digest.
    encoded: String,
}

/// Digest implements the Digest.
impl Digest {
    /// new returns a new Digest.
    pub fn new(algorithm: Algorithm, encoded: String) -> Self {
        Self { algorithm, encoded }
    }

    /// extract_from_blob_url extracts the digest from the oci blob url, e.g. http(s)://<registry>/v2/<repository>/blobs/<digest>.
    pub fn extract_from_blob_url(url: &str) -> Option<Self> {
        BLOB_URL_REGEX
            .captures(url)
            .and_then(|caps| caps.get(4))
            .map(|m| m.as_str())?
            .parse()
            .ok()
    }

    /// algorithm returns the algorithm of the digest.
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    // encoded returns the encoded digest.
    pub fn encoded(&self) -> &str {
        &self.encoded
    }
}

/// Digest implements the Display.
impl fmt::Display for Digest {
    /// fmt formats the value using the given formatter.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.algorithm, SEPARATOR, self.encoded)
    }
}

/// Digest implements the FromStr.
impl FromStr for Digest {
    type Err = String;

    /// from_str parses a digest string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.splitn(2, SEPARATOR).collect();
        if parts.len() != 2 {
            return Err(format!("invalid digest: {}", s));
        }

        let algorithm = match parts[0] {
            "crc32" => {
                if parts[1].len() != 10 {
                    return Err(format!(
                        "invalid crc32 digest length: {}, expected 10",
                        parts[1].len()
                    ));
                }

                Algorithm::Crc32
            }
            "sha256" => {
                if parts[1].len() != 64 {
                    return Err(format!(
                        "invalid sha256 digest length: {}, expected 64",
                        parts[1].len()
                    ));
                }

                Algorithm::Sha256
            }
            "sha512" => {
                if parts[1].len() != 128 {
                    return Err(format!(
                        "invalid sha512 digest length: {}, expected 128",
                        parts[1].len()
                    ));
                }

                Algorithm::Sha512
            }
            _ => return Err(format!("invalid digest algorithm: {}", parts[0])),
        };

        Ok(Digest::new(algorithm, parts[1].to_string()))
    }
}

/// calculate_file_digest calculates the digest of a file.
#[instrument(skip_all)]
pub fn calculate_file_digest(algorithm: Algorithm, path: &Path) -> ClientResult<Digest> {
    let f = std::fs::File::open(path)?;
    let mut reader = io::BufReader::new(f);
    match algorithm {
        Algorithm::Crc32 => {
            let mut buffer = [0; 4096];
            let mut hasher = crc32fast::Hasher::new();
            loop {
                match reader.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(n) => hasher.update(&buffer[..n]),
                    Err(ref err) if err.kind() == io::ErrorKind::Interrupted => continue,
                    Err(err) => return Err(err.into()),
                };
            }

            Ok(Digest::new(algorithm, hasher.finalize().to_string()))
        }
        Algorithm::Sha256 => {
            let mut hasher = sha2::Sha256::new();
            io::copy(&mut reader, &mut hasher)?;
            Ok(Digest::new(algorithm, hex::encode(hasher.finalize())))
        }
        Algorithm::Sha512 => {
            let mut hasher = sha2::Sha512::new();
            io::copy(&mut reader, &mut hasher)?;
            Ok(Digest::new(algorithm, hex::encode(hasher.finalize())))
        }
    }
}

/// verify_file_digest verifies the digest of a file against an expected digest.
pub fn verify_file_digest(expected_digest: Digest, file_path: &Path) -> ClientResult<()> {
    let digest = match calculate_file_digest(expected_digest.algorithm(), file_path) {
        Ok(digest) => digest,
        Err(err) => {
            return Err(err);
        }
    };

    if digest.to_string() != expected_digest.to_string() {
        return Err(ClientError::DigestMismatch(
            expected_digest.to_string(),
            digest.to_string(),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_extract_from_blob_url() {
        let url = "http://registry.example.com/v2/library/ubuntu/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e";
        let digest = Digest::extract_from_blob_url(url);
        assert!(digest.is_some());
        let digest = digest.unwrap();
        assert_eq!(digest.algorithm(), Algorithm::Sha256);
        assert_eq!(
            digest.encoded(),
            "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
        );

        let url = "https://registry.example.com/v2/myorg/myrepo/blobs/sha512:94381a28e8c039fedfa78de025158a068226c3ccd041b22c2c8e73fc993584e9b167d9ae32bc8b372c66701c808ab134e0768c8f16b9a3e61eec1ccf8faa9db8";
        let digest = Digest::extract_from_blob_url(url);
        assert!(digest.is_some());
        let digest = digest.unwrap();
        assert_eq!(digest.algorithm(), Algorithm::Sha512);
        assert_eq!(digest.encoded(), "94381a28e8c039fedfa78de025158a068226c3ccd041b22c2c8e73fc993584e9b167d9ae32bc8b372c66701c808ab134e0768c8f16b9a3e61eec1ccf8faa9db8");

        let url = "https://registry.io/v2/org/team/project/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e";
        let digest = Digest::extract_from_blob_url(url);
        assert!(digest.is_some());

        let url = "http://localhost:5000/v2/myrepo/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e";
        let digest = Digest::extract_from_blob_url(url);
        assert!(digest.is_some());
        let digest = digest.unwrap();
        assert_eq!(digest.algorithm(), Algorithm::Sha256);
        assert_eq!(
            digest.encoded(),
            "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
        );

        let invalid_urls = vec![
            "http://registry.example.com/blobs/sha256:abc",
            "http://registry.example.com/v2/repo/manifests/sha256:abc",
            "registry.example.com/v2/repo/blobs/sha256:abc",
            "http://registry.example.com/v2/blobs/sha256:abc",
            "",
            "not-a-url",
            "http://registry.example.com/v2/repo/blobs/invalid-digest",
        ];

        for url in invalid_urls {
            assert!(Digest::extract_from_blob_url(url).is_none());
        }
    }

    #[test]
    fn test_algorithm_display() {
        assert_eq!(Algorithm::Crc32.to_string(), "crc32");
        assert_eq!(Algorithm::Sha256.to_string(), "sha256");
        assert_eq!(Algorithm::Sha512.to_string(), "sha512");
    }

    #[test]
    fn test_algorithm_from_str() {
        assert_eq!("crc32".parse::<Algorithm>(), Ok(Algorithm::Crc32));
        assert_eq!("sha256".parse::<Algorithm>(), Ok(Algorithm::Sha256));
        assert_eq!("sha512".parse::<Algorithm>(), Ok(Algorithm::Sha512));
        assert!("invalid".parse::<Algorithm>().is_err());
    }

    #[test]
    fn test_digest_display() {
        let digest = Digest::new(Algorithm::Sha256, "encoded_hash".to_string());
        assert_eq!(digest.to_string(), "sha256:encoded_hash");
    }

    #[test]
    fn test_calculate_file_digest() {
        let content = b"test content";
        let temp_file = tempfile::NamedTempFile::new().expect("failed to create temp file");
        let path = temp_file.path();
        let mut file = File::create(path).expect("failed to create file");
        file.write_all(content).expect("failed to write to file");

        let expected_sha256 = "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72";
        let digest = calculate_file_digest(Algorithm::Sha256, path)
            .expect("failed to calculate Sha256 hash");
        assert_eq!(digest.encoded(), expected_sha256);

        let expected_sha512 = "0cbf4caef38047bba9a24e621a961484e5d2a92176a859e7eb27df343dd34eb98d538a6c5f4da1ce302ec250b821cc001e46cc97a704988297185a4df7e99602";
        let digest = calculate_file_digest(Algorithm::Sha512, path)
            .expect("failed to calculate Sha512 hash");
        assert_eq!(digest.encoded(), expected_sha512);

        let expected_crc32 = "1475635037";
        let digest =
            calculate_file_digest(Algorithm::Crc32, path).expect("failed to calculate Crc32 hash");
        assert_eq!(digest.encoded(), expected_crc32);
    }

    #[test]
    fn test_verify_file_digest() {
        let content = b"test content";
        let temp_file = tempfile::NamedTempFile::new().expect("failed to create temp file");
        let path = temp_file.path();
        let mut file = File::create(path).expect("failed to create file");
        file.write_all(content).expect("failed to write to file");

        let expected_sha256_digest = Digest::new(
            Algorithm::Sha256,
            "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72".to_string(),
        );
        assert!(verify_file_digest(expected_sha256_digest, path).is_ok());

        let expected_sha512_digest = Digest::new(
            Algorithm::Sha512,
            "0cbf4caef38047bba9a24e621a961484e5d2a92176a859e7eb27df343dd34eb98d538a6c5f4da1ce302ec250b821cc001e46cc97a704988297185a4df7e99602".to_string(),
        );
        assert!(verify_file_digest(expected_sha512_digest, path).is_ok());

        let expected_crc32_digest = Digest::new(Algorithm::Crc32, "1475635037".to_string());
        assert!(verify_file_digest(expected_crc32_digest, path).is_ok());
    }
}
