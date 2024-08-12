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
use std::path::PathBuf;
use url::Url;
use uuid::Uuid;

// SEED_PEER_KEY is the key of the seed peer.
const SEED_PEER_KEY: &str = "seed";

// CACHE_KEY is the key of the cache.
const CACHE_KEY: &str = "cache";

// PERSISTENT_CACHE_KEY is the key of the persistent cache.
const PERSISTENT_CACHE_KEY: &str = "persistent";

// IDGenerator is used to generate the id for the resources.
#[derive(Debug)]
pub struct IDGenerator {
    // ip is the ip of the host.
    ip: String,

    // hostname is the hostname of the host.
    hostname: String,

    // is_seed_peer indicates whether the host is a seed peer.
    is_seed_peer: bool,
}

// IDGenerator implements the IDGenerator.
impl IDGenerator {
    // new creates a new IDGenerator.
    pub fn new(ip: String, hostname: String, is_seed_peer: bool) -> Self {
        IDGenerator {
            ip,
            hostname,
            is_seed_peer,
        }
    }

    // host_id generates the host id.
    pub fn host_id(&self) -> String {
        if self.is_seed_peer {
            return format!("{}-{}-{}", self.ip, self.hostname, "seed");
        }

        format!("{}-{}", self.ip, self.hostname)
    }

    // task_id generates the task id.
    pub fn task_id(
        &self,
        url: &str,
        digest: Option<&str>,
        tag: Option<&str>,
        application: Option<&str>,
        filtered_query_params: Vec<String>,
    ) -> Result<String> {
        // Filter the query parameters.
        let url = Url::parse(url).or_err(ErrorType::ParseError)?;
        let query = url
            .query_pairs()
            .filter(|(k, _)| filtered_query_params.contains(&k.to_string()));
        let mut artifact_url = url.clone();
        artifact_url.query_pairs_mut().clear().extend_pairs(query);

        // Initialize the hasher.
        let mut hasher = Sha256::new();

        // Add the url to generate the task id.
        hasher.update(artifact_url.to_string());

        // Add the digest to generate the task id.
        if let Some(digest) = digest {
            hasher.update(digest);
        }

        // Add the tag to generate the task id.
        if let Some(tag) = tag {
            hasher.update(tag);
        }

        // Add the application to generate the task id.
        if let Some(application) = application {
            hasher.update(application);
        }

        // Generate the task id.
        Ok(hex::encode(hasher.finalize()))
    }

    // cache_task_id generates the cache task id.
    pub fn cache_task_id(
        &self,
        path: &PathBuf,
        tag: Option<&str>,
        application: Option<&str>,
    ) -> Result<String> {
        // Initialize the hasher.
        let mut hasher = blake3::Hasher::new();

        // Calculate the hash of the file.
        let mut f = std::fs::File::open(path)?;
        std::io::copy(&mut f, &mut hasher)?;

        // Add the tag to generate the cache task id.
        if let Some(tag) = tag {
            hasher.update(tag.as_bytes());
        }

        // Add the application to generate the cache task id.
        if let Some(application) = application {
            hasher.update(application.as_bytes());
        }

        // Generate the cache task id.
        Ok(hasher.finalize().to_hex().to_string())
    }

    // peer_id generates the peer id.
    pub fn peer_id(&self) -> String {
        if self.is_seed_peer {
            return format!(
                "{}-{}-{}-{}",
                self.ip,
                self.hostname,
                Uuid::new_v4(),
                SEED_PEER_KEY,
            );
        }

        format!("{}-{}-{}", self.ip, self.hostname, Uuid::new_v4())
    }

    // cache_peer_id generates the cache peer id.
    pub fn cache_peer_id(&self, persistent: bool) -> String {
        if persistent {
            return format!(
                "{}-{}-{}-{}-{}",
                self.ip,
                self.hostname,
                Uuid::new_v4(),
                CACHE_KEY,
                PERSISTENT_CACHE_KEY,
            );
        }

        format!(
            "{}-{}-{}-{}",
            self.ip,
            self.hostname,
            Uuid::new_v4(),
            CACHE_KEY,
        )
    }

    // task_type generates the task type by the task id.
    pub fn task_type(&self, id: &str) -> TaskType {
        if id.contains(CACHE_KEY) {
            return TaskType::Dfcache;
        }

        TaskType::Dfdaemon
    }
}
