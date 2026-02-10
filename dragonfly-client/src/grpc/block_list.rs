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

use crate::dynconfig::{
    Dynconfig, SchedulerClusterConfigBlockList, SchedulerClusterConfigDownloadBlockList,
    SchedulerClusterConfigUploadBlockList,
};
use dragonfly_client_config::dfdaemon::Config;
use std::sync::Arc;

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

    /// Dynamic configuration of the dfdaemon.
    dynconfig: Arc<Dynconfig>,
}

/// The block list struct provides methods to check if certain tasks are blocked based on the
/// dynamic configuration.
impl BlockList {
    /// Creates a new `BlockList` instance with the given static configuration and dynamic configuration.
    pub fn new(config: Arc<Config>, dynconfig: Arc<Dynconfig>) -> Self {
        Self { config, dynconfig }
    }

    /// Acquires a read lock on the dynamic configuration data and applies the given transformation
    /// function to the block list. This avoids heavy clones of the entire configuration structure.
    /// Returns `None` if the block list configuration is not present or if the transformation
    /// function returns `None`.
    async fn with_block_list<F, R>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&SchedulerClusterConfigBlockList) -> Option<R>,
    {
        let data = self.dynconfig.data.read().await;
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
    use regex::Regex;

    #[test]
    fn test_is_download_blocked_by_application() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: Some(vec!["blocked-app".to_string()]),
            urls: Vec::new(),
            tags: None,
            priorities: None,
        };

        // Should be blocked when application matches.
        let params = DownloadBlockListCheckParams {
            application: Some("blocked-app".to_string()),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(BlockList::is_download_blocked(&block_list, &params));

        // Should not be blocked when application doesn't match.
        let params = DownloadBlockListCheckParams {
            application: Some("allowed-app".to_string()),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(!BlockList::is_download_blocked(&block_list, &params));

        // Should not be blocked when application is None.
        let params = DownloadBlockListCheckParams {
            application: None,
            url: None,
            tag: None,
            priority: None,
        };
        assert!(!BlockList::is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_download_blocked_by_url_regex() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: None,
            urls: vec![
                Regex::new(r".*\.blocked\.com.*").unwrap(),
                Regex::new(r"^https://forbidden\.").unwrap(),
            ],
            tags: None,
            priorities: None,
        };

        // Should be blocked when URL matches regex.
        let params = DownloadBlockListCheckParams {
            application: None,
            url: Some("https://example.blocked.com/file".to_string()),
            tag: None,
            priority: None,
        };
        assert!(BlockList::is_download_blocked(&block_list, &params));

        // Should be blocked when URL matches another regex.
        let params = DownloadBlockListCheckParams {
            application: None,
            url: Some("https://forbidden.example.com/file".to_string()),
            tag: None,
            priority: None,
        };
        assert!(BlockList::is_download_blocked(&block_list, &params));

        // Should not be blocked when URL doesn't match.
        let params = DownloadBlockListCheckParams {
            application: None,
            url: Some("https://allowed.com/file".to_string()),
            tag: None,
            priority: None,
        };
        assert!(!BlockList::is_download_blocked(&block_list, &params));

        // Should not be blocked when URL is None.
        let params = DownloadBlockListCheckParams {
            application: None,
            url: None,
            tag: None,
            priority: None,
        };
        assert!(!BlockList::is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_download_blocked_by_tag() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: None,
            urls: Vec::new(),
            tags: Some(vec!["blocked-tag".to_string()]),
            priorities: None,
        };

        // Should be blocked when tag matches.
        let params = DownloadBlockListCheckParams {
            application: None,
            url: None,
            tag: Some("blocked-tag".to_string()),
            priority: None,
        };
        assert!(BlockList::is_download_blocked(&block_list, &params));

        // Should not be blocked when tag doesn't match.
        let params = DownloadBlockListCheckParams {
            application: None,
            url: None,
            tag: Some("allowed-tag".to_string()),
            priority: None,
        };
        assert!(!BlockList::is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_download_blocked_by_priority() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: None,
            urls: Vec::new(),
            tags: None,
            priorities: Some(vec![0]),
        };

        // Should be blocked when priority matches.
        let params = DownloadBlockListCheckParams {
            application: None,
            url: None,
            tag: None,
            priority: Some(0),
        };
        assert!(BlockList::is_download_blocked(&block_list, &params));

        // Should not be blocked when priority doesn't match.
        let params = DownloadBlockListCheckParams {
            application: None,
            url: None,
            tag: None,
            priority: Some(5),
        };
        assert!(!BlockList::is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_download_blocked_empty_block_list() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: None,
            urls: Vec::new(),
            tags: None,
            priorities: None,
        };

        let params = DownloadBlockListCheckParams {
            application: Some("any-app".to_string()),
            url: Some("https://any.url.com".to_string()),
            tag: Some("any-tag".to_string()),
            priority: Some(1),
        };
        assert!(!BlockList::is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_by_application() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: Some(vec!["blocked-app".to_string()]),
            urls: Vec::new(),
            tags: None,
        };

        // Should be blocked when application matches.
        let params = UploadBlockListCheckParams {
            application: Some("blocked-app".to_string()),
            url: None,
            tag: None,
        };
        assert!(BlockList::is_upload_blocked(&block_list, &params));

        // Should not be blocked when application doesn't match.
        let params = UploadBlockListCheckParams {
            application: Some("allowed-app".to_string()),
            url: None,
            tag: None,
        };
        assert!(!BlockList::is_upload_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_by_url_regex() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: None,
            urls: vec![Regex::new(r".*blocked.*").unwrap()],
            tags: None,
        };

        // Should be blocked when URL matches regex.
        let params = UploadBlockListCheckParams {
            application: None,
            url: Some("https://blocked.com/upload".to_string()),
            tag: None,
        };
        assert!(BlockList::is_upload_blocked(&block_list, &params));

        // Should not be blocked when URL doesn't match.
        let params = UploadBlockListCheckParams {
            application: None,
            url: Some("https://allowed.com/upload".to_string()),
            tag: None,
        };
        assert!(!BlockList::is_upload_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_by_tag() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: None,
            urls: Vec::new(),
            tags: Some(vec!["blocked-tag".to_string()]),
        };

        // Should be blocked when tag matches.
        let params = UploadBlockListCheckParams {
            application: None,
            url: None,
            tag: Some("blocked-tag".to_string()),
        };
        assert!(BlockList::is_upload_blocked(&block_list, &params));

        // Should not be blocked when tag doesn't match.
        let params = UploadBlockListCheckParams {
            application: None,
            url: None,
            tag: Some("allowed-tag".to_string()),
        };
        assert!(!BlockList::is_upload_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_empty_block_list() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: None,
            urls: Vec::new(),
            tags: None,
        };

        let params = UploadBlockListCheckParams {
            application: Some("any-app".to_string()),
            url: Some("https://any.url.com".to_string()),
            tag: Some("any-tag".to_string()),
        };
        assert!(!BlockList::is_upload_blocked(&block_list, &params));
    }
}
