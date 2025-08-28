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
use std::io::{self, Read};
use std::path::PathBuf;
use url::Url;
use uuid::Uuid;

/// SEED_PEER_SUFFIX is the suffix of the seed peer.
const SEED_PEER_SUFFIX: &str = "seed";

/// PERSISTENT_CACHE_TASK_SUFFIX is the suffix of the persistent cache task.
const PERSISTENT_CACHE_TASK_SUFFIX: &str = "persistent-cache-task";

/// TaskIDParameter is the parameter of the task id.
pub enum TaskIDParameter {
    /// Content uses the content to generate the task id.
    Content(String),
    /// URLBased uses the url, piece_length, tag, application and filtered_query_params to generate
    /// the task id.
    URLBased {
        url: String,
        piece_length: Option<u64>,
        tag: Option<String>,
        application: Option<String>,
        filtered_query_params: Vec<String>,
    },
}

/// PersistentCacheTaskIDParameter is the parameter of the persistent cache task id.
pub enum PersistentCacheTaskIDParameter {
    /// Content uses the content to generate the persistent cache task id.
    Content(String),
    /// FileContentBased uses the file path, piece_length, tag and application to generate the persistent cache task id.
    FileContentBased {
        path: PathBuf,
        piece_length: Option<u64>,
        tag: Option<String>,
        application: Option<String>,
    },
}

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
    pub fn new(ip: String, hostname: String, is_seed_peer: bool) -> Self {
        IDGenerator {
            ip,
            hostname,
            is_seed_peer,
        }
    }

    /// host_id generates the host id.
    #[inline]
    pub fn host_id(&self) -> String {
        if self.is_seed_peer {
            return format!("{}-{}-{}", self.ip, self.hostname, "seed");
        }

        format!("{}-{}", self.ip, self.hostname)
    }

    /// task_id generates the task id.
    #[inline]
    pub fn task_id(&self, parameter: TaskIDParameter) -> Result<String> {
        match parameter {
            TaskIDParameter::Content(content) => {
                Ok(hex::encode(Sha256::digest(content.as_bytes())))
            }
            TaskIDParameter::URLBased {
                url,
                piece_length,
                tag,
                application,
                filtered_query_params,
            } => {
                // Filter the query parameters.
                let url = Url::parse(url.as_str()).or_err(ErrorType::ParseError)?;
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

                hasher.update(TaskType::Standard.as_str_name().as_bytes());

                // Generate the task id.
                Ok(hex::encode(hasher.finalize()))
            }
        }
    }

    /// persistent_cache_task_id generates the persistent cache task id.
    #[inline]
    pub fn persistent_cache_task_id(
        &self,
        parameter: PersistentCacheTaskIDParameter,
    ) -> Result<String> {
        let mut hasher = crc32fast::Hasher::new();

        match parameter {
            PersistentCacheTaskIDParameter::Content(content) => {
                hasher.update(content.as_bytes());
                // TODO: `rocksdb::set_prefix_extractor` is set to 64 bytes, but CRC32 <= 10 bytes
                // Repeat to fill 64 bytes, this is temporary
                Ok(hasher.finalize().to_string().repeat(8))
            }
            PersistentCacheTaskIDParameter::FileContentBased {
                path,
                piece_length,
                tag,
                application,
            } => {
                // Calculate the hash of the file.
                let f = std::fs::File::open(path)?;
                let mut buffer = [0; 4096];
                let mut reader = io::BufReader::with_capacity(buffer.len(), f);
                loop {
                    match reader.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(n) => hasher.update(&buffer[..n]),
                        Err(ref err) if err.kind() == io::ErrorKind::Interrupted => continue,
                        Err(err) => return Err(err.into()),
                    };
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

                hasher.update(TaskType::PersistentCache.as_str_name().as_bytes());

                // Generate the task id by crc32.
                // TODO
                Ok(hasher.finalize().to_string().repeat(8))
            }
        }
    }

    /// peer_id generates the peer id.
    #[inline]
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
    pub fn task_type(&self, id: &str) -> TaskType {
        // TODO: useless?
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
                TaskIDParameter::URLBased {
                    url: "https://example.com".to_string(),
                    piece_length: Some(1024_u64),
                    tag: Some("foo".to_string()),
                    application: Some("bar".to_string()),
                    filtered_query_params: vec![],
                },
                "27554d06dfc788c2c2c60e01960152ffbd4b145fc103fcb80b432b4dc238a6fe",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                TaskIDParameter::URLBased {
                    url: "https://example.com".to_string(),
                    piece_length: None,
                    tag: Some("foo".to_string()),
                    application: Some("bar".to_string()),
                    filtered_query_params: vec![],
                },
                "06408fbf247ddaca478f8cb9565fe5591c28efd0994b8fea80a6a87d3203c5ca",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                TaskIDParameter::URLBased {
                    url: "https://example.com".to_string(),
                    piece_length: None,
                    tag: Some("foo".to_string()),
                    application: None,
                    filtered_query_params: vec![],
                },
                "3c3f230ef9f191dd2821510346a7bc138e4894bee9aee184ba250a3040701d2a",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                TaskIDParameter::URLBased {
                    url: "https://example.com".to_string(),
                    piece_length: None,
                    tag: None,
                    application: Some("bar".to_string()),
                    filtered_query_params: vec![],
                },
                "c9f9261b7305c24371244f9f149f5d4589ed601348fdf22d7f6f4b10658fdba2",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                TaskIDParameter::URLBased {
                    url: "https://example.com".to_string(),
                    piece_length: Some(1024_u64),
                    tag: None,
                    application: None,
                    filtered_query_params: vec![],
                },
                "9f7c9aafbc6f30f8f41a96ca77eeae80c5b60964b3034b0ee43ccf7b2f9e52b8",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                TaskIDParameter::URLBased {
                    url: "https://example.com?foo=foo&bar=bar".to_string(),
                    piece_length: None,
                    tag: None,
                    application: None,
                    filtered_query_params: vec!["foo".to_string(), "bar".to_string()],
                },
                "457b4328cde278e422c9e243f7bfd1e97f511fec43a80f535cf6b0ef6b086776",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                TaskIDParameter::Content("This is a test file".to_string()),
                "e2d0fe1585a63ec6009c8016ff8dda8b17719a637405a4e23c0ff81339148249",
            ),
        ];

        for (generator, parameter, expected_id) in test_cases {
            let task_id = generator.task_id(parameter).unwrap();
            assert_eq!(task_id, expected_id);
        }
    }

    #[test]
    fn should_generate_persistent_cache_task_id() {
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("testfile");
        let mut f = File::create(&file_path).unwrap();
        f.write_all("This is a test file".as_bytes()).unwrap();

        let test_cases = vec![
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                PersistentCacheTaskIDParameter::FileContentBased {
                    path: file_path.clone(),
                    piece_length: Some(1024_u64),
                    tag: Some("tag1".to_string()),
                    application: Some("app1".to_string()),
                },
                "3490958009",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                PersistentCacheTaskIDParameter::FileContentBased {
                    path: file_path.clone(),
                    piece_length: None,
                    tag: None,
                    application: Some("app1".to_string()),
                },
                "735741469",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                PersistentCacheTaskIDParameter::FileContentBased {
                    path: file_path.clone(),
                    piece_length: None,
                    tag: Some("tag1".to_string()),
                    application: None,
                },
                "3954905097",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                PersistentCacheTaskIDParameter::FileContentBased {
                    path: file_path.clone(),
                    piece_length: Some(1024_u64),
                    tag: None,
                    application: None,
                },
                "4162557545",
            ),
            (
                IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false),
                PersistentCacheTaskIDParameter::Content("This is a test file".to_string()),
                "107352521",
            ),
        ];

        for (generator, parameter, expected_id) in test_cases {
            let task_id = generator.persistent_cache_task_id(parameter).unwrap();
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
