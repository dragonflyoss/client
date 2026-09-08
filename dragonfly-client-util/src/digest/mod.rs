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
use regex::Regex;
use sha2::Digest as Sha2Digest;
use std::fmt;
use std::io::{self, Read};
use std::path::Path;
use std::str::FromStr;
use std::sync::LazyLock;
use tracing::instrument;

/// The separator character for digest formatting.
pub const SEPARATOR: &str = ":";

/// Regex pattern for OCI blob URLs, e.g. http(s)://<registry>/v2/<repository>/blobs/<digest>.
static BLOB_URL_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(.*)://(.*)/v2/(.*)/blobs/([^?]+)(?:\?.*)?$").unwrap());

/// Checks if the URL is an OCI blob URL.
pub fn is_blob_url(url: &str) -> bool {
    BLOB_URL_REGEX.is_match(url)
}

/// Regex pattern for OCI manifest URLs, e.g. http(s)://<registry>/v2/<repository>/manifests/<reference>.
static MANIFEST_URL_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^(.*)://(.*)/v2/(.*)/manifests/([^?]+)(?:\?.*)?$").unwrap());

/// Checks if the URL is an OCI manifest URL whose reference is a digest with a
/// supported algorithm. A manifest URL referenced by a tag returns false, so it
/// falls back to the url based task id.
pub fn is_manifest_digest_url(url: &str) -> bool {
    Digest::extract_from_manifest_url(url).is_some()
}

/// Algorithm for generating digests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    /// CRC32 algorithm for generating digests.
    Crc32,

    /// SHA-256 algorithm for generating digests.
    Sha256,

    /// SHA-512 algorithm for generating digests.
    Sha512,
}

/// Implements the Display.
impl fmt::Display for Algorithm {
    /// Formats the value using the given formatter.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Algorithm::Crc32 => write!(f, "crc32"),
            Algorithm::Sha256 => write!(f, "sha256"),
            Algorithm::Sha512 => write!(f, "sha512"),
        }
    }
}

/// Implements the FromStr.
impl FromStr for Algorithm {
    type Err = String;

    /// Parses an algorithm string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "crc32" => Ok(Algorithm::Crc32),
            "sha256" => Ok(Algorithm::Sha256),
            "sha512" => Ok(Algorithm::Sha512),
            _ => Err(format!("invalid digest algorithm: {s}")),
        }
    }
}

/// A digest value with its associated algorithm.
pub struct Digest {
    /// The algorithm used to generate the digest.
    algorithm: Algorithm,

    /// The encoded digest value.
    encoded: String,
}

/// Implements the Digest.
impl Digest {
    /// Creates a new digest with the specified algorithm and encoded value.
    pub fn new(algorithm: Algorithm, encoded: String) -> Self {
        Self { algorithm, encoded }
    }

    /// Extracts the digest from an OCI blob URL, e.g. http(s)://<registry>/v2/<repository>/blobs/<digest>.
    pub fn extract_from_blob_url(url: &str) -> Option<Self> {
        BLOB_URL_REGEX
            .captures(url)
            .and_then(|caps| caps.get(4))
            .map(|m| m.as_str())?
            .parse()
            .ok()
    }

    /// Extracts the digest from an OCI manifest URL, e.g. http(s)://<registry>/v2/<repository>/manifests/<digest>.
    pub fn extract_from_manifest_url(url: &str) -> Option<Self> {
        MANIFEST_URL_REGEX
            .captures(url)
            .and_then(|caps| caps.get(4))
            .map(|m| m.as_str())?
            .parse()
            .ok()
    }

    /// Returns the algorithm of the digest.
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// Returns the encoded digest value.
    pub fn encoded(&self) -> &str {
        &self.encoded
    }
}

/// Implements the Display.
impl fmt::Display for Digest {
    /// Formats the value using the given formatter.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.algorithm, SEPARATOR, self.encoded)
    }
}

/// Implements the FromStr.
impl FromStr for Digest {
    type Err = String;

    /// Parses a digest string.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = s.splitn(2, SEPARATOR).collect();
        if parts.len() != 2 {
            return Err(format!("invalid digest: {s}"));
        }

        let algorithm = match parts[0] {
            "crc32" => {
                if parts[1].is_empty() {
                    return Err(format!("invalid crc32 digest: {s}"));
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

/// Calculates the digest of a file.
#[instrument(level = "debug", skip_all)]
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

/// Verifies the digest of a file against an expected digest.
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
    #![allow(clippy::type_complexity)]

    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn is_blob_url_matches_oci_blob_urls() {
        let test_cases = vec![
            (
                "http://registry.example.com/v2/library/ubuntu/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e",
                true,
            ),
            (
                "http://localhost:5000/v2/myrepo/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e?ns=docker.io",
                true,
            ),
            (
                "http://registry.example.com/v2/library/ubuntu/manifests/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e",
                false,
            ),
            ("http://registry.example.com/blobs/sha256:abc", false),
            ("https://example.com/file.txt", false),
        ];

        for (url, expected) in test_cases {
            assert_eq!(is_blob_url(url), expected);
        }
    }

    #[test]
    fn extract_from_blob_url_parses_oci_blob_digests() {
        let test_cases: Vec<(&str, fn(Option<Digest>))> = vec![
            (
                "http://registry.example.com/v2/library/ubuntu/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e",
                |digest| {
                    let digest = digest.unwrap();
                    assert_eq!(digest.algorithm(), Algorithm::Sha256);
                    assert_eq!(
                        digest.encoded(),
                        "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
                    );
                },
            ),
            (
                "https://registry.example.com/v2/myorg/myrepo/blobs/sha512:94381a28e8c039fedfa78de025158a068226c3ccd041b22c2c8e73fc993584e9b167d9ae32bc8b372c66701c808ab134e0768c8f16b9a3e61eec1ccf8faa9db8",
                |digest| {
                    let digest = digest.unwrap();
                    assert_eq!(digest.algorithm(), Algorithm::Sha512);
                    assert_eq!(
                        digest.encoded(),
                        "94381a28e8c039fedfa78de025158a068226c3ccd041b22c2c8e73fc993584e9b167d9ae32bc8b372c66701c808ab134e0768c8f16b9a3e61eec1ccf8faa9db8"
                    );
                },
            ),
            (
                "https://registry.io/v2/org/team/project/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e",
                |digest| {
                    let digest = digest.unwrap();
                    assert_eq!(digest.algorithm(), Algorithm::Sha256);
                    assert_eq!(
                        digest.encoded(),
                        "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
                    );
                },
            ),
            (
                "http://localhost:5000/v2/myrepo/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e",
                |digest| {
                    let digest = digest.unwrap();
                    assert_eq!(digest.algorithm(), Algorithm::Sha256);
                    assert_eq!(
                        digest.encoded(),
                        "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
                    );
                },
            ),
            (
                "https://index.docker.io/v2/library/alpine/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e?ns=docker.io",
                |digest| {
                    let digest = digest.unwrap();
                    assert_eq!(digest.algorithm(), Algorithm::Sha256);
                    assert_eq!(
                        digest.encoded(),
                        "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
                    );
                },
            ),
            (
                "http://localhost:5000/v2/myrepo/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e?id=12345",
                |digest| {
                    let digest = digest.unwrap();
                    assert_eq!(digest.algorithm(), Algorithm::Sha256);
                    assert_eq!(
                        digest.encoded(),
                        "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
                    );
                },
            ),
            ("http://registry.example.com/blobs/sha256:abc", |digest| {
                assert!(digest.is_none())
            }),
            (
                "http://registry.example.com/v2/repo/manifests/sha256:abc",
                |digest| assert!(digest.is_none()),
            ),
            ("registry.example.com/v2/repo/blobs/sha256:abc", |digest| {
                assert!(digest.is_none())
            }),
            ("http://registry.example.com/v2/blobs/sha256:abc", |digest| {
                assert!(digest.is_none())
            }),
            ("", |digest| assert!(digest.is_none())),
            ("not-a-url", |digest| assert!(digest.is_none())),
            (
                "http://registry.example.com/v2/repo/blobs/invalid-digest",
                |digest| assert!(digest.is_none()),
            ),
        ];

        for (url, expect) in test_cases {
            expect(Digest::extract_from_blob_url(url));
        }
    }

    #[test]
    fn is_manifest_digest_url_requires_a_supported_digest() {
        let test_cases = vec![
            (
                "http://registry.example.com/v2/library/ubuntu/manifests/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e",
                true,
            ),
            (
                "http://localhost:5000/v2/myrepo/manifests/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e?ns=docker.io",
                true,
            ),
            (
                "http://registry.example.com/v2/library/ubuntu/manifests/latest",
                false,
            ),
            (
                "http://registry.example.com/v2/library/ubuntu/manifests/md5:8a04994a666b4e4b20a2fd9e5a44f44c",
                false,
            ),
            (
                "http://registry.example.com/v2/library/ubuntu/manifests/sha256:abc",
                false,
            ),
            (
                "http://registry.example.com/v2/library/ubuntu/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e",
                false,
            ),
            ("https://example.com/file.txt", false),
        ];

        for (url, expected) in test_cases {
            assert_eq!(is_manifest_digest_url(url), expected);
        }
    }

    #[test]
    fn extract_from_manifest_url_parses_oci_manifest_digests() {
        let test_cases: Vec<(&str, fn(Option<Digest>))> = vec![
            (
                "http://registry.example.com/v2/library/ubuntu/manifests/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e",
                |digest| {
                    let digest = digest.unwrap();
                    assert_eq!(digest.algorithm(), Algorithm::Sha256);
                    assert_eq!(
                        digest.encoded(),
                        "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
                    );
                },
            ),
            (
                "http://localhost:5000/v2/myrepo/manifests/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e?ns=docker.io",
                |digest| {
                    let digest = digest.unwrap();
                    assert_eq!(digest.algorithm(), Algorithm::Sha256);
                    assert_eq!(
                        digest.encoded(),
                        "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
                    );
                },
            ),
            (
                "http://registry.example.com/v2/library/ubuntu/manifests/latest",
                |digest| assert!(digest.is_none()),
            ),
            (
                "http://registry.example.com/v2/library/ubuntu/manifests/sha256:abc",
                |digest| assert!(digest.is_none()),
            ),
            (
                "http://registry.example.com/v2/library/ubuntu/manifests/md5:8a04994a666b4e4b20a2fd9e5a44f44c",
                |digest| assert!(digest.is_none()),
            ),
            (
                "http://registry.example.com/v2/library/ubuntu/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e",
                |digest| assert!(digest.is_none()),
            ),
            ("https://example.com/file.txt", |digest| {
                assert!(digest.is_none())
            }),
        ];

        for (url, expect) in test_cases {
            expect(Digest::extract_from_manifest_url(url));
        }
    }

    #[test]
    fn algorithm_formats_as_its_name() {
        let test_cases = vec![
            (Algorithm::Crc32, "crc32"),
            (Algorithm::Sha256, "sha256"),
            (Algorithm::Sha512, "sha512"),
        ];

        for (algorithm, expected) in test_cases {
            assert_eq!(algorithm.to_string(), expected);
        }
    }

    #[test]
    fn algorithm_from_str_accepts_known_names() {
        let test_cases = vec![
            ("crc32", Ok(Algorithm::Crc32)),
            ("sha256", Ok(Algorithm::Sha256)),
            ("sha512", Ok(Algorithm::Sha512)),
            (
                "invalid",
                Err("invalid digest algorithm: invalid".to_string()),
            ),
        ];

        for (name, expected) in test_cases {
            assert_eq!(name.parse::<Algorithm>(), expected);
        }
    }

    #[test]
    fn digest_formats_as_algorithm_and_encoded() {
        let test_cases = vec![
            (Algorithm::Crc32, "1475635037", "crc32:1475635037"),
            (Algorithm::Sha256, "encoded_hash", "sha256:encoded_hash"),
            (Algorithm::Sha512, "encoded_hash", "sha512:encoded_hash"),
        ];

        for (algorithm, encoded, expected) in test_cases {
            assert_eq!(
                Digest::new(algorithm, encoded.to_string()).to_string(),
                expected
            );
        }
    }

    #[test]
    fn digest_from_str_validates_algorithm_and_encoded_length() {
        let test_cases: Vec<(&str, fn(Result<Digest, String>))> = vec![
            ("crc32:1475635037", |digest| {
                let digest = digest.unwrap();
                assert_eq!(digest.algorithm(), Algorithm::Crc32);
                assert_eq!(digest.encoded(), "1475635037");
            }),
            ("sha256", |digest| {
                assert_eq!(digest.err().unwrap(), "invalid digest: sha256")
            }),
            ("md5:8a04994a666b4e4b20a2fd9e5a44f44c", |digest| {
                assert_eq!(digest.err().unwrap(), "invalid digest algorithm: md5")
            }),
            ("crc32:", |digest| {
                assert_eq!(digest.err().unwrap(), "invalid crc32 digest: crc32:")
            }),
            ("sha256:abc", |digest| {
                assert_eq!(
                    digest.err().unwrap(),
                    "invalid sha256 digest length: 3, expected 64"
                )
            }),
            ("sha512:abc", |digest| {
                assert_eq!(
                    digest.err().unwrap(),
                    "invalid sha512 digest length: 3, expected 128"
                )
            }),
        ];

        for (raw_digest, expect) in test_cases {
            expect(raw_digest.parse::<Digest>());
        }
    }

    #[test]
    fn calculate_file_digest_hashes_the_file_per_algorithm() {
        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), b"test content").unwrap();

        let test_cases = vec![
            (Algorithm::Crc32, "1475635037"),
            (
                Algorithm::Sha256,
                "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
            ),
            (
                Algorithm::Sha512,
                "0cbf4caef38047bba9a24e621a961484e5d2a92176a859e7eb27df343dd34eb98d538a6c5f4da1ce302ec250b821cc001e46cc97a704988297185a4df7e99602",
            ),
        ];

        for (algorithm, expected) in test_cases {
            let digest = calculate_file_digest(algorithm, temp_file.path()).unwrap();
            assert_eq!(digest.encoded(), expected);
        }
    }

    #[test]
    fn verify_file_digest_accepts_only_the_matching_digest() {
        let temp_file = NamedTempFile::new().unwrap();
        std::fs::write(temp_file.path(), b"test content").unwrap();

        let test_cases: Vec<(Algorithm, &str, fn(ClientResult<()>))> = vec![
            (Algorithm::Crc32, "1475635037", |result| {
                assert!(result.is_ok())
            }),
            (
                Algorithm::Sha256,
                "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72",
                |result| assert!(result.is_ok()),
            ),
            (
                Algorithm::Sha512,
                "0cbf4caef38047bba9a24e621a961484e5d2a92176a859e7eb27df343dd34eb98d538a6c5f4da1ce302ec250b821cc001e46cc97a704988297185a4df7e99602",
                |result| assert!(result.is_ok()),
            ),
            (
                Algorithm::Sha256,
                "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e",
                |result| {
                    assert!(matches!(result, Err(ClientError::DigestMismatch(..))))
                },
            ),
        ];

        for (algorithm, encoded, expect) in test_cases {
            expect(verify_file_digest(
                Digest::new(algorithm, encoded.to_string()),
                temp_file.path(),
            ));
        }
    }
}
