/*
 *     Copyright 2026 The Dragonfly Authors
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

use super::{
    Data, SchedulerClusterConfigBlockList, SchedulerClusterConfigDownloadBlockList,
    SchedulerClusterConfigUploadBlockList,
};
use dragonfly_client_config::dfdaemon::Config;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Parameters for checking download block list.
#[derive(Debug, Clone)]
pub struct DownloadBlockListCheckParams {
    /// The URL of the task.
    pub url: Option<String>,

    /// The application name of the task.
    pub application: Option<String>,

    /// The tag of the task.
    pub tag: Option<String>,

    /// The priority of the task.
    pub priority: Option<i32>,
}

/// Parameters for checking upload block list.
#[derive(Debug, Clone)]
pub struct UploadBlockListCheckParams {
    /// The URL of the task.
    pub url: Option<String>,

    /// The application name of the task.
    pub application: Option<String>,

    /// The tag of the task.
    pub tag: Option<String>,
}

/// Block list provides methods to check if tasks are blocked based on dynamic configuration.
pub struct BlockList {
    /// Configuration of the dfdaemon.
    config: Arc<Config>,

    /// Dynamic configuration data shared with the dynconfig.
    data: Arc<RwLock<Data>>,
}

/// The block list struct provides methods to check if certain tasks are blocked based on the
/// dynamic configuration.
impl BlockList {
    /// Creates a new `BlockList` instance with the given static configuration and shared dynamic
    /// configuration data.
    pub fn new(config: Arc<Config>, data: Arc<RwLock<Data>>) -> Self {
        Self { config, data }
    }

    /// Acquires a read lock on the dynamic configuration data and applies the given transformation
    /// function to the block list. This avoids heavy clones of the entire configuration structure.
    /// Returns `None` if the block list configuration is not present or if the transformation
    /// function returns `None`.
    async fn with_block_list<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&SchedulerClusterConfigBlockList) -> Option<R>,
    {
        let data = self.data.read().await;
        let config = if self.config.seed_peer.enable {
            data.seed_client_config
                .as_ref()
                .and_then(|config| config.block_list.as_ref())
        } else {
            data.client_config
                .as_ref()
                .and_then(|config| config.block_list.as_ref())
        };

        config.and_then(f)
    }

    /// Checks whether a regular task download is blocked based on the current block list configuration.
    /// Returns `true` if the download matches any blocked URL, application, tag, or priority.
    /// Returns `false` if no block list is configured or no match is found.
    pub async fn is_task_download_blocked(&self, params: &DownloadBlockListCheckParams) -> bool {
        self.with_block_list(|block_list| {
            let block_list = block_list.task.as_ref()?.download.as_ref()?;
            Some(Self::is_download_blocked(block_list, params))
        })
        .await
        .unwrap_or(false)
    }

    /// Checks whether a persistent task download is blocked based on the current block list configuration.
    /// Returns `true` if the download matches any blocked URL, application, tag, or priority.
    /// Returns `false` if no block list is configured or no match is found.
    pub async fn is_persistent_task_download_blocked(
        &self,
        params: &DownloadBlockListCheckParams,
    ) -> bool {
        self.with_block_list(|block_list| {
            let block_list = block_list.persistent_task.as_ref()?.download.as_ref()?;
            Some(Self::is_download_blocked(block_list, params))
        })
        .await
        .unwrap_or(false)
    }

    /// Checks whether a persistent task upload is blocked based on the current block list configuration.
    /// Returns `true` if the upload matches any blocked URL, application, or tag.
    /// Returns `false` if no block list is configured or no match is found.
    pub async fn is_persistent_task_upload_blocked(
        &self,
        params: &UploadBlockListCheckParams,
    ) -> bool {
        self.with_block_list(|block_list| {
            let block_list = block_list.persistent_task.as_ref()?.upload.as_ref()?;
            Some(Self::is_upload_blocked(block_list, params))
        })
        .await
        .unwrap_or(false)
    }

    /// Checks whether a persistent cache task download is blocked based on the current block list configuration.
    /// Returns `true` if the download matches any blocked URL, application, tag, or priority.
    /// Returns `false` if no block list is configured or no match is found.
    pub async fn is_persistent_cache_task_download_blocked(
        &self,
        params: &DownloadBlockListCheckParams,
    ) -> bool {
        self.with_block_list(|block_list| {
            let block_list = block_list
                .persistent_cache_task
                .as_ref()?
                .download
                .as_ref()?;
            Some(Self::is_download_blocked(block_list, params))
        })
        .await
        .unwrap_or(false)
    }

    /// Checks whether a persistent cache task upload is blocked based on the current block list configuration.
    /// Returns `true` if the upload matches any blocked URL, application, or tag.
    /// Returns `false` if no block list is configured or no match is found.
    pub async fn is_persistent_cache_task_upload_blocked(
        &self,
        params: &UploadBlockListCheckParams,
    ) -> bool {
        self.with_block_list(|block_list| {
            let block_list = block_list.persistent_cache_task.as_ref()?.upload.as_ref()?;
            Some(Self::is_upload_blocked(block_list, params))
        })
        .await
        .unwrap_or(false)
    }

    /// Determines whether a download should be blocked by checking the provided parameters against
    /// the given download block list. Matches are checked against blocked URLs (via regex),
    /// applications, tags, and priorities. Returns `true` if any field matches a blocked entry.
    fn is_download_blocked(
        block_list: &SchedulerClusterConfigDownloadBlockList,
        params: &DownloadBlockListCheckParams,
    ) -> bool {
        if let Some(url) = &params.url {
            if block_list
                .urls
                .iter()
                .any(|blocked_url| blocked_url.is_match(url))
            {
                return true;
            }
        }

        if let (Some(application), Some(blocked_applications)) =
            (&params.application, &block_list.applications)
        {
            if blocked_applications.contains(application) {
                return true;
            }
        }

        if let (Some(tag), Some(blocked_tags)) = (&params.tag, &block_list.tags) {
            if blocked_tags.contains(tag) {
                return true;
            }
        }

        if let (Some(priority), Some(blocked_priorities)) =
            (&params.priority, &block_list.priorities)
        {
            if blocked_priorities.contains(priority) {
                return true;
            }
        }

        false
    }

    /// Determines whether an upload should be blocked by checking the provided parameters against
    /// the given upload block list. Matches are checked against blocked URLs (via regex),
    /// applications, and tags. Returns `true` if any field matches a blocked entry.
    fn is_upload_blocked(
        block_list: &SchedulerClusterConfigUploadBlockList,
        params: &UploadBlockListCheckParams,
    ) -> bool {
        if let Some(url) = &params.url {
            if block_list
                .urls
                .iter()
                .any(|blocked_url| blocked_url.is_match(url))
            {
                return true;
            }
        }

        if let (Some(application), Some(blocked_applications)) =
            (&params.application, &block_list.applications)
        {
            if blocked_applications.contains(application) {
                return true;
            }
        }

        if let (Some(tag), Some(blocked_tags)) = (&params.tag, &block_list.tags) {
            if blocked_tags.contains(tag) {
                return true;
            }
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dynconfig::{
        SchedulerClusterClientConfig, SchedulerClusterConfigTaskBlockList,
        SchedulerClusterSeedClientConfig,
    };
    use dragonfly_client_config::dfdaemon::SeedPeer;
    use regex::Regex;

    fn strings(values: &[&str]) -> Option<Vec<String>> {
        (!values.is_empty()).then(|| values.iter().map(|value| value.to_string()).collect())
    }

    fn regexes(patterns: &[&str]) -> Vec<Regex> {
        patterns
            .iter()
            .map(|pattern| Regex::new(pattern).unwrap())
            .collect()
    }

    fn download_block_list(
        urls: &[&str],
        applications: &[&str],
        tags: &[&str],
        priorities: &[i32],
    ) -> SchedulerClusterConfigDownloadBlockList {
        SchedulerClusterConfigDownloadBlockList {
            applications: strings(applications),
            urls: regexes(urls),
            tags: strings(tags),
            priorities: (!priorities.is_empty()).then(|| priorities.to_vec()),
        }
    }

    fn upload_block_list(
        urls: &[&str],
        applications: &[&str],
        tags: &[&str],
    ) -> SchedulerClusterConfigUploadBlockList {
        SchedulerClusterConfigUploadBlockList {
            applications: strings(applications),
            urls: regexes(urls),
            tags: strings(tags),
        }
    }

    fn download_params(
        url: Option<&str>,
        application: Option<&str>,
        tag: Option<&str>,
        priority: Option<i32>,
    ) -> DownloadBlockListCheckParams {
        DownloadBlockListCheckParams {
            url: url.map(str::to_string),
            application: application.map(str::to_string),
            tag: tag.map(str::to_string),
            priority,
        }
    }

    fn upload_params(
        url: Option<&str>,
        application: Option<&str>,
        tag: Option<&str>,
    ) -> UploadBlockListCheckParams {
        UploadBlockListCheckParams {
            url: url.map(str::to_string),
            application: application.map(str::to_string),
            tag: tag.map(str::to_string),
        }
    }

    fn task_download_block_list(applications: &[&str]) -> SchedulerClusterConfigBlockList {
        SchedulerClusterConfigBlockList {
            task: Some(SchedulerClusterConfigTaskBlockList {
                download: Some(download_block_list(&[], applications, &[], &[])),
            }),
            ..Default::default()
        }
    }

    #[test]
    fn is_download_blocked_matches_url_application_tag_or_priority() {
        let test_cases = vec![
            (
                download_block_list(&[], &["blocked-app"], &[], &[]),
                download_params(None, Some("blocked-app"), None, None),
                true,
            ),
            (
                download_block_list(&[], &["blocked-app"], &[], &[]),
                download_params(None, Some("allowed-app"), None, None),
                false,
            ),
            (
                download_block_list(&[], &["blocked-app"], &[], &[]),
                download_params(None, None, None, None),
                false,
            ),
            (
                download_block_list(
                    &[r".*\.blocked\.com.*", r"^https://forbidden\."],
                    &[],
                    &[],
                    &[],
                ),
                download_params(Some("https://example.blocked.com/file"), None, None, None),
                true,
            ),
            (
                download_block_list(
                    &[r".*\.blocked\.com.*", r"^https://forbidden\."],
                    &[],
                    &[],
                    &[],
                ),
                download_params(Some("https://forbidden.example.com/file"), None, None, None),
                true,
            ),
            (
                download_block_list(
                    &[r".*\.blocked\.com.*", r"^https://forbidden\."],
                    &[],
                    &[],
                    &[],
                ),
                download_params(Some("https://allowed.com/file"), None, None, None),
                false,
            ),
            (
                download_block_list(
                    &[r".*\.blocked\.com.*", r"^https://forbidden\."],
                    &[],
                    &[],
                    &[],
                ),
                download_params(None, None, None, None),
                false,
            ),
            (
                download_block_list(&[], &[], &["blocked-tag"], &[]),
                download_params(None, None, Some("blocked-tag"), None),
                true,
            ),
            (
                download_block_list(&[], &[], &["blocked-tag"], &[]),
                download_params(None, None, Some("allowed-tag"), None),
                false,
            ),
            (
                download_block_list(&[], &[], &[], &[0]),
                download_params(None, None, None, Some(0)),
                true,
            ),
            (
                download_block_list(&[], &[], &[], &[0]),
                download_params(None, None, None, Some(5)),
                false,
            ),
            (
                download_block_list(&[], &[], &[], &[]),
                download_params(
                    Some("https://any.url.com"),
                    Some("any-app"),
                    Some("any-tag"),
                    Some(1),
                ),
                false,
            ),
            (
                download_block_list(
                    &[r"^https://forbidden\."],
                    &["blocked-app"],
                    &["blocked-tag"],
                    &[0],
                ),
                download_params(
                    Some("https://allowed.com/file"),
                    Some("allowed-app"),
                    Some("allowed-tag"),
                    Some(0),
                ),
                true,
            ),
        ];

        for (block_list, params, expected) in test_cases {
            assert_eq!(
                BlockList::is_download_blocked(&block_list, &params),
                expected
            );
        }
    }

    #[test]
    fn is_upload_blocked_matches_url_application_or_tag() {
        let test_cases = vec![
            (
                upload_block_list(&[], &["blocked-app"], &[]),
                upload_params(None, Some("blocked-app"), None),
                true,
            ),
            (
                upload_block_list(&[], &["blocked-app"], &[]),
                upload_params(None, Some("allowed-app"), None),
                false,
            ),
            (
                upload_block_list(&[r".*blocked.*"], &[], &[]),
                upload_params(Some("https://blocked.com/upload"), None, None),
                true,
            ),
            (
                upload_block_list(&[r".*blocked.*"], &[], &[]),
                upload_params(Some("https://allowed.com/upload"), None, None),
                false,
            ),
            (
                upload_block_list(&[], &[], &["blocked-tag"]),
                upload_params(None, None, Some("blocked-tag")),
                true,
            ),
            (
                upload_block_list(&[], &[], &["blocked-tag"]),
                upload_params(None, None, Some("allowed-tag")),
                false,
            ),
            (
                upload_block_list(&[], &[], &[]),
                upload_params(
                    Some("https://any.url.com"),
                    Some("any-app"),
                    Some("any-tag"),
                ),
                false,
            ),
            (
                upload_block_list(&[r".*blocked.*"], &["blocked-app"], &["blocked-tag"]),
                upload_params(
                    Some("https://allowed.com/upload"),
                    Some("allowed-app"),
                    Some("blocked-tag"),
                ),
                true,
            ),
        ];

        for (block_list, params, expected) in test_cases {
            assert_eq!(BlockList::is_upload_blocked(&block_list, &params), expected);
        }
    }

    #[tokio::test]
    async fn is_task_download_blocked_reads_client_or_seed_client_config() {
        let test_cases = vec![
            (
                false,
                Some(SchedulerClusterClientConfig {
                    block_list: Some(task_download_block_list(&["blocked-app"])),
                }),
                None,
                true,
            ),
            (
                true,
                Some(SchedulerClusterClientConfig {
                    block_list: Some(task_download_block_list(&["blocked-app"])),
                }),
                None,
                false,
            ),
            (
                true,
                None,
                Some(SchedulerClusterSeedClientConfig {
                    block_list: Some(task_download_block_list(&["blocked-app"])),
                }),
                true,
            ),
            (
                false,
                None,
                Some(SchedulerClusterSeedClientConfig {
                    block_list: Some(task_download_block_list(&["blocked-app"])),
                }),
                false,
            ),
            (
                false,
                Some(SchedulerClusterClientConfig { block_list: None }),
                None,
                false,
            ),
            (false, None, None, false),
        ];

        for (seed_peer_enabled, client_config, seed_client_config, expected) in test_cases {
            let config = Config {
                seed_peer: SeedPeer {
                    enable: seed_peer_enabled,
                    ..Default::default()
                },
                ..Default::default()
            };
            let data = Data {
                client_config: client_config.clone(),
                seed_client_config: seed_client_config.clone(),
                ..Default::default()
            };
            let block_list = BlockList::new(Arc::new(config), Arc::new(RwLock::new(data)));
            let params = download_params(None, Some("blocked-app"), None, None);
            assert_eq!(block_list.is_task_download_blocked(&params).await, expected);
        }
    }
}
