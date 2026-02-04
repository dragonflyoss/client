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
use regex::Regex;
use std::sync::Arc;
use tracing::error;

/// Parameters for checking download block list.
#[derive(Debug)]
pub struct DownloadBlockListParams<'a> {
    /// The application name of the task.
    pub application: Option<&'a str>,
    /// The URL of the task.
    pub url: Option<&'a str>,
    /// The tag of the task.
    pub tag: Option<&'a str>,
    /// The priority of the task.
    pub priority: Option<i32>,
}

/// Parameters for checking upload block list.
#[derive(Debug)]
pub struct UploadBlockListParams<'a> {
    /// The application name of the task.
    pub application: Option<&'a str>,
    /// The URL of the task.
    pub url: Option<&'a str>,
    /// The tag of the task.
    pub tag: Option<&'a str>,
}

/// Blocklist provides methods to check if tasks are blocked based on dynamic configuration.
pub struct Blocklist {
    /// Configuration of the dfdaemon.
    config: Arc<Config>,
    /// Dynamic configuration of the dfdaemon.
    dynconfig: Arc<Dynconfig>,
}

impl Blocklist {
    /// Creates a new Blocklist instance.
    pub fn new(config: Arc<Config>, dynconfig: Arc<Dynconfig>) -> Self {
        Self { config, dynconfig }
    }

    /// Gets the block list configuration from dynconfig.
    /// Returns the appropriate block list based on whether the client is a seed peer or not.
    async fn get_block_list(&self) -> Option<SchedulerClusterConfigBlockList> {
        let data = self.dynconfig.data.read().await;

        // Get the appropriate config based on whether this is a seed peer.
        if self.config.seed_peer.enable {
            // For seed peers, use seed_client_config.
            data.seed_client_config
                .as_ref()
                .and_then(|c| c.block_list.clone())
        } else {
            // For regular clients, use client_config.
            data.client_config
                .as_ref()
                .and_then(|c| c.block_list.clone())
        }
    }

    /// Checks if a download task should be blocked.
    /// Returns true if the task is blocked by the block list.
    pub async fn is_task_download_blocked(&self, params: &DownloadBlockListParams<'_>) -> bool {
        self.get_block_list()
            .await
            .and_then(|b| b.task)
            .and_then(|t| t.download)
            .map(|d| is_download_blocked(&d, params))
            .unwrap_or(false)
    }

    /// Checks if a persistent task download should be blocked.
    /// Returns true if the task is blocked by the block list.
    pub async fn is_persistent_task_download_blocked(
        &self,
        params: &DownloadBlockListParams<'_>,
    ) -> bool {
        self.get_block_list()
            .await
            .and_then(|b| b.persistent_task)
            .and_then(|t| t.download)
            .map(|d| is_download_blocked(&d, params))
            .unwrap_or(false)
    }

    /// Checks if a persistent task upload should be blocked.
    /// Returns true if the task is blocked by the block list.
    pub async fn is_persistent_task_upload_blocked(
        &self,
        params: &UploadBlockListParams<'_>,
    ) -> bool {
        self.get_block_list()
            .await
            .and_then(|b| b.persistent_task)
            .and_then(|t| t.upload)
            .map(|u| is_upload_blocked(&u, params))
            .unwrap_or(false)
    }

    /// Checks if a persistent cache task download should be blocked.
    /// Returns true if the task is blocked by the block list.
    pub async fn is_persistent_cache_task_download_blocked(
        &self,
        params: &DownloadBlockListParams<'_>,
    ) -> bool {
        self.get_block_list()
            .await
            .and_then(|b| b.persistent_cache_task)
            .and_then(|t| t.download)
            .map(|d| is_download_blocked(&d, params))
            .unwrap_or(false)
    }

    /// Checks if a persistent cache task upload should be blocked.
    /// Returns true if the task is blocked by the block list.
    pub async fn is_persistent_cache_task_upload_blocked(
        &self,
        params: &UploadBlockListParams<'_>,
    ) -> bool {
        self.get_block_list()
            .await
            .and_then(|b| b.persistent_cache_task)
            .and_then(|t| t.upload)
            .map(|u| is_upload_blocked(&u, params))
            .unwrap_or(false)
    }
}

/// Checks if a download task is blocked by the block list.
/// Returns true if the task should be blocked.
fn is_download_blocked(
    block_list: &SchedulerClusterConfigDownloadBlockList,
    params: &DownloadBlockListParams,
) -> bool {
    // Check application block list (exact match).
    if params
        .application
        .filter(|a| !a.is_empty())
        .and_then(|app| {
            block_list
                .applications
                .as_ref()
                .map(|apps| apps.iter().any(|a| a == app))
        })
        .unwrap_or(false)
    {
        return true;
    }

    // Check URL block list (regex match).
    if params
        .url
        .filter(|u| !u.is_empty())
        .and_then(|url| {
            block_list.urls.as_ref().map(|patterns| {
                patterns.iter().any(|pattern| match Regex::new(pattern) {
                    Ok(re) => re.is_match(url),
                    Err(err) => {
                        error!("invalid regex pattern '{}': {}", pattern, err);
                        false
                    }
                })
            })
        })
        .unwrap_or(false)
    {
        return true;
    }

    // Check tag block list (exact match).
    if params
        .tag
        .filter(|t| !t.is_empty())
        .and_then(|tag| {
            block_list
                .tags
                .as_ref()
                .map(|tags| tags.iter().any(|t| t == tag))
        })
        .unwrap_or(false)
    {
        return true;
    }

    // Check priority block list (exact match).
    if params
        .priority
        .and_then(|priority| {
            block_list.priorities.as_ref().map(|priorities| {
                let priority_str = priority.to_string();
                priorities.iter().any(|p| p == &priority_str)
            })
        })
        .unwrap_or(false)
    {
        return true;
    }

    false
}

/// Checks if an upload task is blocked by the block list.
/// Returns true if the task should be blocked.
fn is_upload_blocked(
    block_list: &SchedulerClusterConfigUploadBlockList,
    params: &UploadBlockListParams,
) -> bool {
    // Check application block list (exact match).
    if params
        .application
        .filter(|a| !a.is_empty())
        .and_then(|app| {
            block_list
                .applications
                .as_ref()
                .map(|apps| apps.iter().any(|a| a == app))
        })
        .unwrap_or(false)
    {
        return true;
    }

    // Check URL block list (regex match).
    if params
        .url
        .filter(|u| !u.is_empty())
        .and_then(|url| {
            block_list.urls.as_ref().map(|patterns| {
                patterns.iter().any(|pattern| match Regex::new(pattern) {
                    Ok(re) => re.is_match(url),
                    Err(err) => {
                        error!("invalid regex pattern '{}': {}", pattern, err);
                        false
                    }
                })
            })
        })
        .unwrap_or(false)
    {
        return true;
    }

    // Check tag block list (exact match).
    if params
        .tag
        .filter(|t| !t.is_empty())
        .and_then(|tag| {
            block_list
                .tags
                .as_ref()
                .map(|tags| tags.iter().any(|t| t == tag))
        })
        .unwrap_or(false)
    {
        return true;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_download_blocked_by_application() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: Some(vec!["blocked-app".to_string()]),
            urls: None,
            tags: None,
            priorities: None,
        };

        // Should be blocked when application matches.
        let params = DownloadBlockListParams {
            application: Some("blocked-app"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Should not be blocked when application doesn't match.
        let params = DownloadBlockListParams {
            application: Some("allowed-app"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));

        // Should not be blocked when application is None.
        let params = DownloadBlockListParams {
            application: None,
            url: None,
            tag: None,
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_download_blocked_by_url_regex() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: None,
            urls: Some(vec![
                r".*\.blocked\.com.*".to_string(),
                r"^https://forbidden\.".to_string(),
            ]),
            tags: None,
            priorities: None,
        };

        // Should be blocked when URL matches regex.
        let params = DownloadBlockListParams {
            application: None,
            url: Some("https://example.blocked.com/file"),
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Should be blocked when URL matches another regex.
        let params = DownloadBlockListParams {
            application: None,
            url: Some("https://forbidden.example.com/file"),
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Should not be blocked when URL doesn't match.
        let params = DownloadBlockListParams {
            application: None,
            url: Some("https://allowed.com/file"),
            tag: None,
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));

        // Should not be blocked when URL is None.
        let params = DownloadBlockListParams {
            application: None,
            url: None,
            tag: None,
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_download_blocked_by_tag() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: None,
            urls: None,
            tags: Some(vec!["blocked-tag".to_string()]),
            priorities: None,
        };

        // Should be blocked when tag matches.
        let params = DownloadBlockListParams {
            application: None,
            url: None,
            tag: Some("blocked-tag"),
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Should not be blocked when tag doesn't match.
        let params = DownloadBlockListParams {
            application: None,
            url: None,
            tag: Some("allowed-tag"),
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_download_blocked_by_priority() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: None,
            urls: None,
            tags: None,
            priorities: Some(vec!["0".to_string()]),
        };

        // Should be blocked when priority matches.
        let params = DownloadBlockListParams {
            application: None,
            url: None,
            tag: None,
            priority: Some(0),
        };
        assert!(is_download_blocked(&block_list, &params));

        // Should not be blocked when priority doesn't match.
        let params = DownloadBlockListParams {
            application: None,
            url: None,
            tag: None,
            priority: Some(5),
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_download_blocked_empty_block_list() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: None,
            urls: None,
            tags: None,
            priorities: None,
        };

        let params = DownloadBlockListParams {
            application: Some("any-app"),
            url: Some("https://any.url.com"),
            tag: Some("any-tag"),
            priority: Some(1),
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_by_application() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: Some(vec!["blocked-app".to_string()]),
            urls: None,
            tags: None,
        };

        // Should be blocked when application matches.
        let params = UploadBlockListParams {
            application: Some("blocked-app"),
            url: None,
            tag: None,
        };
        assert!(is_upload_blocked(&block_list, &params));

        // Should not be blocked when application doesn't match.
        let params = UploadBlockListParams {
            application: Some("allowed-app"),
            url: None,
            tag: None,
        };
        assert!(!is_upload_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_by_url_regex() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: None,
            urls: Some(vec![r".*blocked.*".to_string()]),
            tags: None,
        };

        // Should be blocked when URL matches regex.
        let params = UploadBlockListParams {
            application: None,
            url: Some("https://blocked.com/upload"),
            tag: None,
        };
        assert!(is_upload_blocked(&block_list, &params));

        // Should not be blocked when URL doesn't match.
        let params = UploadBlockListParams {
            application: None,
            url: Some("https://allowed.com/upload"),
            tag: None,
        };
        assert!(!is_upload_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_by_tag() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: None,
            urls: None,
            tags: Some(vec!["blocked-tag".to_string()]),
        };

        // Should be blocked when tag matches.
        let params = UploadBlockListParams {
            application: None,
            url: None,
            tag: Some("blocked-tag"),
        };
        assert!(is_upload_blocked(&block_list, &params));

        // Should not be blocked when tag doesn't match.
        let params = UploadBlockListParams {
            application: None,
            url: None,
            tag: Some("allowed-tag"),
        };
        assert!(!is_upload_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_empty_block_list() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: None,
            urls: None,
            tags: None,
        };

        let params = UploadBlockListParams {
            application: Some("any-app"),
            url: Some("https://any.url.com"),
            tag: Some("any-tag"),
        };
        assert!(!is_upload_blocked(&block_list, &params));
    }
}
