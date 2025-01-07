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

use dragonfly_client_core::Result as ClientResult;
use sha2::Digest as Sha2Digest;
use std::fmt;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;
use tracing::instrument;

/// SEPARATOR is the separator of digest.
pub const SEPARATOR: &str = ":";

/// Algorithm is an enum of the algorithm that is used to generate digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    /// Crc32 is crc32 algorithm for generate digest.
    Crc32,

    /// Blake3 is blake3 algorithm for generate digest.
    Blake3,

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
            Algorithm::Blake3 => write!(f, "blake3"),
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
            "blake3" => Ok(Algorithm::Blake3),
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
            "crc32" => Algorithm::Crc32,
            "blake3" => Algorithm::Blake3,
            "sha256" => Algorithm::Sha256,
            "sha512" => Algorithm::Sha512,
            _ => return Err(format!("invalid digest algorithm: {}", parts[0])),
        };

        Ok(Digest::new(algorithm, parts[1].to_string()))
    }
}

/// calculate_file_hash calculates the hash of a file.
#[instrument(skip_all)]
pub fn calculate_file_hash(algorithm: Algorithm, path: &Path) -> ClientResult<Digest> {
    let f = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(f);
    match algorithm {
        Algorithm::Crc32 => {
            let mut crc: u32 = 0;
            let mut buffer = [0; 4096];
            loop {
                let n = reader.read(&mut buffer)?;
                if n == 0 {
                    break;
                }
                crc = crc32c::crc32c_append(crc, &buffer[..n]);
            }

            Ok(Digest::new(algorithm, crc.to_string()))
        }
        Algorithm::Blake3 => {
            let mut hasher = blake3::Hasher::new();
            std::io::copy(&mut reader, &mut hasher)?;
            Ok(Digest::new(
                algorithm,
                base16ct::lower::encode_string(hasher.finalize().as_bytes()),
            ))
        }
        Algorithm::Sha256 => {
            let mut hasher = sha2::Sha256::new();
            std::io::copy(&mut reader, &mut hasher)?;
            Ok(Digest::new(algorithm, hex::encode(hasher.finalize())))
        }
        Algorithm::Sha512 => {
            let mut hasher = sha2::Sha512::new();
            std::io::copy(&mut reader, &mut hasher)?;
            Ok(Digest::new(algorithm, hex::encode(hasher.finalize())))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn test_algorithm_display() {
        assert_eq!(Algorithm::Crc32.to_string(), "crc32");
        assert_eq!(Algorithm::Blake3.to_string(), "blake3");
        assert_eq!(Algorithm::Sha256.to_string(), "sha256");
        assert_eq!(Algorithm::Sha512.to_string(), "sha512");
    }

    #[test]
    fn test_algorithm_from_str() {
        assert_eq!("crc32".parse::<Algorithm>(), Ok(Algorithm::Crc32));
        assert_eq!("blake3".parse::<Algorithm>(), Ok(Algorithm::Blake3));
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
    fn test_calculate_file_hash() {
        let content = b"test content";
        let temp_file = tempfile::NamedTempFile::new().expect("failed to create temp file");
        let path = temp_file.path();
        let mut file = File::create(path).expect("failed to create file");
        file.write_all(content).expect("failed to write to file");

        let expected_blake3 = "ead3df8af4aece7792496936f83b6b6d191a7f256585ce6b6028db161278017e";
        let digest =
            calculate_file_hash(Algorithm::Blake3, path).expect("failed to calculate Blake3 hash");
        assert_eq!(digest.encoded(), expected_blake3);

        let expected_sha256 = "6ae8a75555209fd6c44157c0aed8016e763ff435a19cf186f76863140143ff72";
        let digest =
            calculate_file_hash(Algorithm::Sha256, path).expect("failed to calculate Sha256 hash");
        assert_eq!(digest.encoded(), expected_sha256);

        let expected_sha512 = "0cbf4caef38047bba9a24e621a961484e5d2a92176a859e7eb27df343dd34eb98d538a6c5f4da1ce302ec250b821cc001e46cc97a704988297185a4df7e99602";
        let digest =
            calculate_file_hash(Algorithm::Sha512, path).expect("failed to calculate Sha512 hash");
        assert_eq!(digest.encoded(), expected_sha512);

        let expected_crc32 = "422618885";
        let digest =
            calculate_file_hash(Algorithm::Crc32, path).expect("failed to calculate Sha512 hash");
        assert_eq!(digest.encoded(), expected_crc32);
    }
}
