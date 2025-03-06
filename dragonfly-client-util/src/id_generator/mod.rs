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

use dragonfly_api::common::v2::TaskType;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Result,
};
use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::PathBuf;
use tracing::instrument;
use url::Url;
use uuid::Uuid;

/// SEED_PEER_SUFFIX is the suffix of the seed peer.
const SEED_PEER_SUFFIX: &str = "seed";

/// PERSISTENT_CACHE_TASK_SUFFIX is the suffix of the persistent cache task.
const PERSISTENT_CACHE_TASK_SUFFIX: &str = "persistent-cache-task";

/// IDGenerator is used to generate the id for the resources.
#[derive(Debug)]
pub struct IDGenerator {
    /// ip is the ip of the host.
    ip: String,

    /// hostname is the hostname of the host.
    hostname: String,

    /// is_seed_peer indicates whether the host is a seed peer.
    is_seed_peer: bool,
}

/// IDGenerator implements the IDGenerator.
impl IDGenerator {
    /// new creates a new IDGenerator.
    #[instrument(skip_all)]
    pub fn new(ip: String, hostname: String, is_seed_peer: bool) -> Self {
        IDGenerator {
            ip,
            hostname,
            is_seed_peer,
        }
    }

    /// host_id generates the host id.
    #[inline]
    #[instrument(skip_all)]
    pub fn host_id(&self) -> String {
        if self.is_seed_peer {
            return format!("{}-{}-{}", self.ip, self.hostname, "seed");
        }

        format!("{}-{}", self.ip, self.hostname)
    }

    /// task_id generates the task id.
    #[inline]
    #[instrument(skip_all)]
    pub fn task_id(
        &self,
        url: &str,
        piece_length: Option<u64>,
        tag: Option<&str>,
        application: Option<&str>,
        filtered_query_params: Vec<String>,
    ) -> Result<String> {
        // Filter the query parameters.
        let url = Url::parse(url).or_err(ErrorType::ParseError)?;
        let query = url
            .query_pairs()
            .filter(|(k, _)| !filtered_query_params.contains(&k.to_string()));

        let mut artifact_url = url.clone();
        if query.clone().count() == 0 {
            artifact_url.set_query(None);
        } else {
            artifact_url.query_pairs_mut().clear().extend_pairs(query);
        }

        let artifact_url_str = artifact_url.to_string();
        let final_url = if artifact_url_str.ends_with('/') && artifact_url.path() == "/" {
            artifact_url_str.trim_end_matches('/').to_string()
        } else {
            artifact_url_str
        };

        // Initialize the hasher.
        let mut hasher = Sha256::new();

        // Add the url to generate the task id.
        hasher.update(final_url);

        // Add the tag to generate the task id.
        if let Some(tag) = tag {
            hasher.update(tag);
        }

        // Add the application to generate the task id.
        if let Some(application) = application {
            hasher.update(application);
        }

        // Add the piece length to generate the task id.
        if let Some(piece_length) = piece_length {
            hasher.update(piece_length.to_string());
        }

        // Generate the task id.
        Ok(hex::encode(hasher.finalize()))
    }

    /// persistent_cache_task_id generates the persistent cache task id.
    #[inline]
    #[instrument(skip_all)]
    pub fn persistent_cache_task_id(
        &self,
        path: &PathBuf,
        piece_length: Option<u64>,
        tag: Option<&str>,
        application: Option<&str>,
    ) -> Result<String> {
        // Calculate the hash of the file.
        let f = std::fs::File::open(path)?;
        let mut buffer = [0; 4096];
        let mut reader = std::io::BufReader::with_capacity(buffer.len(), f);
        let mut hasher = crc32fast::Hasher::new();
        loop {
            let n = reader.read(&mut buffer)?;
            if n == 0 {
                break;
            }

            hasher.update(&buffer[..n]);
        }

        // Add the tag to generate the persistent cache task id.
        if let Some(tag) = tag {
            hasher.update(tag.as_bytes());
        }

        // Add the application to generate the persistent cache task id.
        if let Some(application) = application {
            hasher.update(application.as_bytes());
        }

        // Add the piece length to generate the persistent cache task id.
        if let Some(piece_length) = piece_length {
            hasher.update(piece_length.to_string().as_bytes());
        }

        // Generate the task id by crc32.
        Ok(hasher.finalize().to_string())
    }

    /// peer_id generates the peer id.
    #[inline]
    #[instrument(skip_all)]
    pub fn peer_id(&self) -> String {
        if self.is_seed_peer {
            return format!(
                "{}-{}-{}-{}",
                self.ip,
                self.hostname,
                Uuid::new_v4(),
                SEED_PEER_SUFFIX,
            );
        }

        format!("{}-{}-{}", self.ip, self.hostname, Uuid::new_v4())
    }

    /// task_type generates the task type by the task id.
    #[instrument(skip_all)]
    pub fn task_type(&self, id: &str) -> TaskType {
        if id.ends_with(PERSISTENT_CACHE_TASK_SUFFIX) {
            return TaskType::PersistentCache;
        }

        TaskType::Standard
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn should_generate_host_id() {
        let test_cases = vec![
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                "127.0.0.1-localhost",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), true),
                "127.0.0.1-localhost-seed",
            ),
        ];

        for (generator, expected) in test_cases {
            assert_eq!(generator.host_id(), expected);
        }
    }

    #[test]
    fn should_generate_task_id() {
        let test_cases = vec![
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                "https://example.com",
                Some(1024_u64),
                Some("foo"),
                Some("bar"),
                vec![],
                "99a47b38e9d3321aebebd715bea0483c1400cef2f767f84d97458f9dcedff221",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                "https://example.com",
                None,
                Some("foo"),
                Some("bar"),
                vec![],
                "160fa7f001d9d2e893130894fbb60a5fb006e1d61bff82955f2946582bc9de1d",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                "https://example.com",
                None,
                Some("foo"),
                None,
                vec![],
                "2773851c628744fb7933003195db436ce397c1722920696c4274ff804d86920b",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                "https://example.com",
                None,
                None,
                Some("bar"),
                vec![],
                "63dee2822037636b0109876b58e95692233840753a882afa69b9b5ee82a6c57d",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                "https://example.com",
                Some(1024_u64),
                None,
                None,
                vec![],
                "40c21de3ad2f1470ca1a19a2ad2577803a1829851f6cf862ffa2d4577ae51d38",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                "https://example.com?foo=foo&bar=bar",
                None,
                None,
                None,
                vec!["foo".to_string(), "bar".to_string()],
                "100680ad546ce6a577f42f52df33b4cfdca756859e664b8d7de329b150d09ce9",
            ),
        ];

        for (generator, url, piece_length, tag, application, filtered_query_params, expected_id) in
            test_cases
        {
            let task_id = generator
                .task_id(url, piece_length, tag, application, filtered_query_params)
                .unwrap();
            assert_eq!(task_id, expected_id);
        }
    }

    #[test]
    fn should_generate_persistent_cache_task_id() {
        let test_cases = vec![
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                "This is a test file",
                Some(1024_u64),
                Some("tag1"),
                Some("app1"),
                "223755482",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                "This is a test file",
                None,
                None,
                Some("app1"),
                "1152081721",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                "This is a test file",
                None,
                Some("tag1"),
                None,
                "990623045",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                "This is a test file",
                Some(1024_u64),
                None,
                None,
                "1293485139",
            ),
        ];

        for (generator, file_content, piece_length, tag, application, expected_id) in test_cases {
            let dir = tempdir().unwrap();
            let file_path = dir.path().join("testfile");
            let mut f = File::create(&file_path).unwrap();
            f.write_all(file_content.as_bytes()).unwrap();

            let task_id = generator
                .persistent_cache_task_id(&file_path, piece_length, tag, application)
                .unwrap();
            assert_eq!(task_id, expected_id);
        }
    }

    #[test]
    fn should_generate_peer_id() {
        let test_cases = vec![
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                false,
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), true),
                true,
            ),
        ];

        for (generator, is_seed_peer) in test_cases {
            let peer_id = generator.peer_id();
            assert!(peer_id.starts_with("127.0.0.1-localhost-"));
            if is_seed_peer {
                assert!(peer_id.ends_with("-seed"));
            }
        }
    }

    #[test]
    fn should_generate_task_type() {
        let test_cases = vec![
            ("some-task-id", TaskType::Standard),
            (
                "some-task-id-persistent-cache-task",
                TaskType::PersistentCache,
            ),
        ];

        let generator = IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false);
        for (id, expected_type) in test_cases {
            assert_eq!(generator.task_type(id), expected_type);
        }
    }
}
