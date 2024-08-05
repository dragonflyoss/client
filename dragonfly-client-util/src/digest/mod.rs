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
use sha2::Digest as Sha2Digest;
use std::fmt;
use std::path::Path;
use std::str::FromStr;

// SEPARATOR is the separator of digest.
pub const SEPARATOR: &str = ":";

// Algorithm is an enum of the algorithm that is used to generate digest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Algorithm {
    // Crc32 is crc32 algorithm for generate digest.
    Crc32,

    // Blake3 is blake3 algorithm for generate digest.
    Blake3,

    // Sha256 is sha256 algorithm for generate digest.
    Sha256,

    // Sha512 is sha512 algorithm for generate digest.
    Sha512,
}

// Algorithm implements the Display.
impl fmt::Display for Algorithm {
    // fmt formats the value using the given formatter.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Algorithm::Crc32 => write!(f, "crc32"),
            Algorithm::Blake3 => write!(f, "blake3"),
            Algorithm::Sha256 => write!(f, "sha256"),
            Algorithm::Sha512 => write!(f, "sha512"),
        }
    }
}

// Algorithm implements the FromStr.
impl FromStr for Algorithm {
    type Err = String;

    // from_str parses an algorithm string.
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

// Digest is a struct that is used to generate digest.
pub struct Digest {
    // algorithm is the algorithm that is used to generate digest.
    algorithm: Algorithm,

    // encoded is the encoded digest.
    encoded: String,
}

// Digest implements the Digest.
impl Digest {
    // new returns a new Digest.
    pub fn new(algorithm: Algorithm, encoded: String) -> Self {
        Self { algorithm, encoded }
    }

    // algorithm returns the algorithm of the digest.
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    // encoded returns the encoded digest.
    pub fn encoded(&self) -> &str {
        &self.encoded
    }
}

// Digest implements the Display.
impl fmt::Display for Digest {
    // fmt formats the value using the given formatter.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.algorithm, SEPARATOR, self.encoded)
    }
}

// Digest implements the FromStr.
impl FromStr for Digest {
    type Err = String;

    // from_str parses a digest string.
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

// calculate_file_hash calculates the hash of a file.
pub fn calculate_file_hash(algorithm: Algorithm, path: &Path) -> ClientResult<Digest> {
    let f = std::fs::File::open(path)?;
    let mut reader = std::io::BufReader::new(f);
    match algorithm {
        Algorithm::Crc32 => Err(ClientError::Unimplemented),
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
