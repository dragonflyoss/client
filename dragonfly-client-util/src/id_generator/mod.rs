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

use crate::digest;
use crate::url::filter_query_params;
use dragonfly_api::common::v2::{Download, TaskType};
use dragonfly_client_core::{Error, Result};
use sha2::{Digest, Sha256};
use std::io::{self, Read};
use std::path::PathBuf;
use uuid::Uuid;

/// The suffix of the seed peer.
const SEED_PEER_SUFFIX: &str = "seed";

/// The parameter of the task id.
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
        // Revision is used to generate the task id for the artifact with the same url but
        // different revisions, such as git repository.
        revision: Option<String>,
    },
    /// BlobDigestBased will extract the digest in the oci blob url and use the digest's encoded as
    /// the task id.
    BlobDigestBased(String),
    /// ManifestDigestBased will extract the digest in the oci manifest url and use the digest's
    /// encoded as the task id.
    ManifestDigestBased(String),
}

/// The parameter of the persistent task id.
pub enum PersistentTaskIDParameter {
    /// FileContentBased uses the object storage url, region, endpoint, piece_length, tag and application
    /// to generate the persistent task id.
    FileContentBased {
        url: String,
        region: String,
        endpoint: String,
    },
}

/// The parameter of the persistent cache task id.
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

/// Returns the repository revision from the download options of the hub backends
/// (Hugging Face, ModelScope or OpenCSG), used as the task id revision.
pub fn repository_revision(download: &Download) -> Option<String> {
    if let Some(hugging_face) = &download.hugging_face {
        return Some(hugging_face.revision.clone());
    }

    if let Some(model_scope) = &download.model_scope {
        return Some(model_scope.revision.clone());
    }

    if let Some(open_csg) = &download.open_csg {
        return Some(open_csg.revision.clone());
    }

    None
}

/// Used to generate the id for the resources.
#[derive(Debug)]
pub struct IDGenerator {
    /// The ip of the host.
    ip: String,

    /// The hostname of the host.
    hostname: String,

    /// Indicates whether the host is a seed peer.
    is_seed_peer: bool,
}

/// Implements the IDGenerator.
impl IDGenerator {
    /// Creates a new IDGenerator.
    pub fn new(ip: String, hostname: String, is_seed_peer: bool) -> Self {
        IDGenerator {
            ip,
            hostname,
            is_seed_peer,
        }
    }

    /// Generates the host id.
    #[inline]
    pub fn host_id(&self) -> String {
        if self.is_seed_peer {
            return format!("{}-{}-{}", self.ip, self.hostname, "seed");
        }

        format!("{}-{}", self.ip, self.hostname)
    }

    /// Generates the task id.
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
                revision,
            } => {
                // Canonicalize the url, identical to the scheduler's task id generation.
                let final_url = filter_query_params(&url, &filtered_query_params)?;

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

                // Add the revision to generate the task id for the artifact with the same url but
                // different revisions, such as git repository.
                if let Some(revision) = revision {
                    hasher.update(revision);
                }

                // Add the piece length to generate the task id.
                if let Some(piece_length) = piece_length {
                    hasher.update(piece_length.to_string());
                }

                hasher.update(TaskType::Standard.as_str_name().as_bytes());

                // Generate the task id.
                Ok(hex::encode(hasher.finalize()))
            }
            TaskIDParameter::BlobDigestBased(url) => {
                Ok(digest::Digest::extract_from_blob_url(&url)
                    .ok_or_else(|| Error::InvalidURI(url))?
                    .encoded()
                    .to_string())
            }
            TaskIDParameter::ManifestDigestBased(url) => {
                Ok(digest::Digest::extract_from_manifest_url(&url)
                    .ok_or_else(|| Error::InvalidURI(url))?
                    .encoded()
                    .to_string())
            }
        }
    }

    /// Generates the persistent task id.
    #[inline]
    pub fn persistent_task_id(&self, parameter: PersistentTaskIDParameter) -> Result<String> {
        match parameter {
            PersistentTaskIDParameter::FileContentBased {
                url,
                region,
                endpoint,
            } => {
                // Calculate the hash of the file.
                let mut hasher = Sha256::new();
                hasher.update(url.as_bytes());
                hasher.update(region.as_bytes());
                hasher.update(endpoint.as_bytes());
                hasher.update(TaskType::Persistent.as_str_name().as_bytes());

                // Generate the persistent task id by sha256.
                Ok(hex::encode(hasher.finalize()))
            }
        }
    }

    /// Generates the persistent cache task id.
    #[inline]
    pub fn persistent_cache_task_id(
        &self,
        parameter: PersistentCacheTaskIDParameter,
    ) -> Result<String> {
        match parameter {
            PersistentCacheTaskIDParameter::Content(content) => {
                Ok(hex::encode(Sha256::digest(content.as_bytes())))
            }
            PersistentCacheTaskIDParameter::FileContentBased {
                path,
                piece_length,
                tag,
                application,
            } => {
                // Calculate the hash of the file.
                let mut hasher = Sha256::new();

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

                // Generate the task id by sha256.
                Ok(hex::encode(hasher.finalize()))
            }
        }
    }

    /// Generates the peer id.
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
}

#[cfg(test)]
mod tests {
    #![allow(clippy::type_complexity)]

    use super::*;
    use dragonfly_api::common::v2::{HuggingFace, ModelScope, OpenCsg};
    use tempfile::tempdir;

    #[test]
    fn repository_revision_reads_the_hub_backend_revision() {
        let test_cases = vec![
            (
                Download {
                    hugging_face: Some(HuggingFace {
                        revision: "main".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Some("main"),
            ),
            (
                Download {
                    model_scope: Some(ModelScope {
                        revision: "master".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Some("master"),
            ),
            (
                Download {
                    open_csg: Some(OpenCsg {
                        revision: "main".to_string(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                Some("main"),
            ),
            (Download::default(), None),
        ];

        for (download, expected) in test_cases {
            assert_eq!(repository_revision(&download).as_deref(), expected);
        }
    }

    #[test]
    fn host_id_appends_seed_suffix_for_seed_peers() {
        let test_cases = vec![
            (false, "127.0.0.1-localhost"),
            (true, "127.0.0.1-localhost-seed"),
        ];

        for (is_seed_peer, expected) in test_cases {
            let generator = IDGenerator::new(
                "127.0.0.1".to_string(),
                "localhost".to_string(),
                is_seed_peer,
            );
            assert_eq!(generator.host_id(), expected);
        }
    }

    #[test]
    fn task_id_hashes_the_canonical_url_with_options() {
        let test_cases = vec![
            (
                "https://example.com",
                Some(1024),
                Some("foo"),
                Some("bar"),
                vec![],
                Some("v1.0"),
                "5844f27a257287e9b734256bb25603d8005422ced8c0377f15063ec11963b25f",
            ),
            (
                "https://example.com",
                None,
                Some("foo"),
                Some("bar"),
                vec![],
                None,
                "06408fbf247ddaca478f8cb9565fe5591c28efd0994b8fea80a6a87d3203c5ca",
            ),
            (
                "https://example.com",
                None,
                Some("foo"),
                None,
                vec![],
                None,
                "3c3f230ef9f191dd2821510346a7bc138e4894bee9aee184ba250a3040701d2a",
            ),
            (
                "https://example.com",
                None,
                None,
                Some("bar"),
                vec![],
                None,
                "c9f9261b7305c24371244f9f149f5d4589ed601348fdf22d7f6f4b10658fdba2",
            ),
            (
                "https://example.com",
                Some(1024),
                None,
                None,
                vec![],
                None,
                "9f7c9aafbc6f30f8f41a96ca77eeae80c5b60964b3034b0ee43ccf7b2f9e52b8",
            ),
            (
                "https://example.com?foo=foo&bar=bar",
                None,
                None,
                None,
                vec!["foo", "bar"],
                None,
                "457b4328cde278e422c9e243f7bfd1e97f511fec43a80f535cf6b0ef6b086776",
            ),
            (
                "https://example.com/file.txt?z=9&b=2&a=1",
                None,
                Some("foo"),
                Some("bar"),
                vec!["z"],
                None,
                "8b3f6e9b9b8fe20903bced565cfd1d0aaef354a4c17573f0c2c1979210443f9d",
            ),
            (
                "https://example.com/file.txt?b=2&a=1&b=1",
                None,
                None,
                None,
                vec!["c"],
                None,
                "7c8801d0596be5e8f9449d5c4af23866c72fe5205119c0e5912981f3b16a37aa",
            ),
            (
                "https://example.com/file.txt?k=a b&m=x*y&n=c~d",
                Some(1024),
                None,
                None,
                vec!["none"],
                None,
                "6196a6846023f6d3c1e4d30f6c86f3d4186e4c664a33e5692b0e04e49b26a9af",
            ),
            (
                "https://example.com/file.txt?a=1&b=2",
                None,
                Some("foo"),
                None,
                vec!["a", "b"],
                None,
                "c8f4b41117329d54af920010394f6f607bac707e933ab2f18d372e3dd4c7fcb3",
            ),
            (
                "https://example.com/file.txt?b=2&a=1",
                None,
                None,
                None,
                vec![],
                None,
                "980ee327518ccc5a7c30703e1a2232e8ba9047b39431f940636c85b6146f8b9a",
            ),
            (
                "https://example.com",
                None,
                None,
                None,
                vec![],
                Some("v1.0"),
                "b171331534b80e0bf91da38ebbfcdbf4d177898f4b9beac44f14733e3f004d4e",
            ),
        ];

        let generator = IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false);
        for (url, piece_length, tag, application, filtered_query_params, revision, expected) in
            test_cases
        {
            let parameter = TaskIDParameter::URLBased {
                url: url.to_string(),
                piece_length,
                tag: tag.map(|tag| tag.to_string()),
                application: application.map(|application| application.to_string()),
                filtered_query_params: filtered_query_params
                    .iter()
                    .map(|param| param.to_string())
                    .collect(),
                revision: revision.map(|revision| revision.to_string()),
            };
            let task_id = generator.task_id(parameter).unwrap();
            assert_eq!(task_id, expected);
        }
    }

    #[test]
    fn task_id_hashes_content() {
        let task_id = IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false)
            .task_id(TaskIDParameter::Content("This is a test file".to_string()))
            .unwrap();
        assert_eq!(
            task_id,
            "e2d0fe1585a63ec6009c8016ff8dda8b17719a637405a4e23c0ff81339148249"
        );
    }

    #[test]
    fn task_id_extracts_oci_digests_or_rejects_invalid_urls() {
        let test_cases: Vec<(TaskIDParameter, fn(Result<String>))> = vec![
            (
                TaskIDParameter::BlobDigestBased(
                    "http://registry.example.com/v2/library/ubuntu/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
                        .to_string(),
                ),
                |task_id| {
                    assert_eq!(
                        task_id.unwrap(),
                        "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
                    )
                },
            ),
            (
                TaskIDParameter::BlobDigestBased(
                    "https://registry.example.com/v2/myorg/myrepo/blobs/sha512:94381a28e8c039fedfa78de025158a068226c3ccd041b22c2c8e73fc993584e9b167d9ae32bc8b372c66701c808ab134e0768c8f16b9a3e61eec1ccf8faa9db8"
                        .to_string(),
                ),
                |task_id| {
                    assert_eq!(
                        task_id.unwrap(),
                        "94381a28e8c039fedfa78de025158a068226c3ccd041b22c2c8e73fc993584e9b167d9ae32bc8b372c66701c808ab134e0768c8f16b9a3e61eec1ccf8faa9db8"
                    )
                },
            ),
            (
                TaskIDParameter::BlobDigestBased(
                    "http://localhost:5000/v2/myrepo/blobs/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e?ns=docker.io"
                        .to_string(),
                ),
                |task_id| {
                    assert_eq!(
                        task_id.unwrap(),
                        "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
                    )
                },
            ),
            (
                TaskIDParameter::ManifestDigestBased(
                    "http://registry.example.com/v2/library/ubuntu/manifests/sha256:b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
                        .to_string(),
                ),
                |task_id| {
                    assert_eq!(
                        task_id.unwrap(),
                        "b2c366cce7e68013d5441c6326d5a3e1b12aeb5ed58564d0fd3fa089bc29cb6e"
                    )
                },
            ),
            (
                TaskIDParameter::ManifestDigestBased(
                    "http://registry.example.com/v2/library/ubuntu/manifests/latest".to_string(),
                ),
                |task_id| assert!(matches!(task_id, Err(Error::InvalidURI(_)))),
            ),
            (
                TaskIDParameter::BlobDigestBased("https://example.com/file.txt".to_string()),
                |task_id| assert!(matches!(task_id, Err(Error::InvalidURI(_)))),
            ),
            (
                TaskIDParameter::BlobDigestBased(
                    "http://registry.example.com/v2/library/ubuntu/blobs/sha256:abc".to_string(),
                ),
                |task_id| assert!(matches!(task_id, Err(Error::InvalidURI(_)))),
            ),
            (
                TaskIDParameter::BlobDigestBased(
                    "http://registry.example.com/v2/library/ubuntu/blobs/md5:8a04994a666b4e4b20a2fd9e5a44f44c"
                        .to_string(),
                ),
                |task_id| assert!(matches!(task_id, Err(Error::InvalidURI(_)))),
            ),
        ];

        let generator = IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false);
        for (parameter, expect) in test_cases {
            expect(generator.task_id(parameter));
        }
    }

    #[test]
    fn persistent_task_id_hashes_url_region_and_endpoint() {
        let task_id = IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false)
            .persistent_task_id(PersistentTaskIDParameter::FileContentBased {
                url: "my-object-key".to_string(),
                region: "us-west-1".to_string(),
                endpoint: "https://s3.us-west-1.amazonaws.com".to_string(),
            })
            .unwrap();
        assert_eq!(
            task_id,
            "b51f4f44921bb585277a5cbac13e7f6e2858238e98546f3ee6bfeb56369979c0"
        );
    }

    #[test]
    fn persistent_cache_task_id_hashes_file_content_with_options() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("testfile");
        std::fs::write(&path, b"This is a test file").unwrap();

        let test_cases = vec![
            (
                Some(1024),
                Some("tag1"),
                Some("app1"),
                "7160a071a9acea5ac341e770c14d0211c38a4b15b3bbe2c5f848a706fd47419e",
            ),
            (
                None,
                None,
                Some("app1"),
                "0d0f8536f51227fda07141308f5ae8149b561b51b61c6517125f25dfa27acf5b",
            ),
            (
                None,
                Some("tag1"),
                None,
                "a98b76813681e30cf83733fe055792b86393bba6f18e3d89fd8c18253922d992",
            ),
            (
                Some(1024),
                None,
                None,
                "e894374a39e39cfa78c409cac02f2cdbb5605a24f5ff55c7bc2b624877556c03",
            ),
        ];

        let generator = IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false);
        for (piece_length, tag, application, expected) in test_cases {
            let parameter = PersistentCacheTaskIDParameter::FileContentBased {
                path: path.clone(),
                piece_length,
                tag: tag.map(|tag| tag.to_string()),
                application: application.map(|application| application.to_string()),
            };
            let task_id = generator.persistent_cache_task_id(parameter).unwrap();
            assert_eq!(task_id, expected);
        }
    }

    #[test]
    fn persistent_cache_task_id_hashes_content() {
        let task_id = IDGenerator::new("127.0.0.1".to_string(), "localhost".to_string(), false)
            .persistent_cache_task_id(PersistentCacheTaskIDParameter::Content(
                "This is a test file".to_string(),
            ))
            .unwrap();
        assert_eq!(
            task_id,
            "e2d0fe1585a63ec6009c8016ff8dda8b17719a637405a4e23c0ff81339148249"
        );
    }

    #[test]
    fn peer_id_appends_a_uuid_and_seed_suffix_for_seed_peers() {
        let test_cases = vec![(false, ""), (true, "-seed")];

        for (is_seed_peer, expected_suffix) in test_cases {
            let generator = IDGenerator::new(
                "127.0.0.1".to_string(),
                "localhost".to_string(),
                is_seed_peer,
            );
            let peer_id = generator.peer_id();
            let uuid = peer_id
                .strip_prefix("127.0.0.1-localhost-")
                .and_then(|uuid| uuid.strip_suffix(expected_suffix));
            assert!(uuid.is_some_and(|uuid| Uuid::parse_str(uuid).is_ok()));
        }
    }
}
