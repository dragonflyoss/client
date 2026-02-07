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

use crate::dynconfig::Dynconfig;
use dragonfly_client_config::dfdaemon::Config;
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::error;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterClientConfig {
    /// The block list of the scheduler cluster client.
    pub block_list: Option<SchedulerClusterConfigBlockList>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterSeedClientConfig {
    /// The block list of the scheduler cluster seed client.
    pub block_list: Option<SchedulerClusterConfigBlockList>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigBlockList {
    /// The block list of the type task.
    pub task: Option<SchedulerClusterConfigTaskBlockList>,

    /// The block list of the type persistent task.
    pub persistent_task: Option<SchedulerClusterConfigPersistentTaskBlockList>,

    /// The block list of the type persistent cache task.
    pub persistent_cache_task: Option<SchedulerClusterConfigPersistentCacheTaskBlockList>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigTaskBlockList {
    /// The block list of the download grpc.
    pub download: Option<SchedulerClusterConfigDownloadBlockList>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigPersistentTaskBlockList {
    /// The block list of the download grpc.
    pub download: Option<SchedulerClusterConfigDownloadBlockList>,

    /// The block list of the upload grpc.
    pub upload: Option<SchedulerClusterConfigUploadBlockList>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigPersistentCacheTaskBlockList {
    /// The block list of the download grpc.
    pub download: Option<SchedulerClusterConfigDownloadBlockList>,

    /// The block list of the upload grpc.
    pub upload: Option<SchedulerClusterConfigUploadBlockList>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigDownloadBlockList {
    #[serde(default)]
    /// The regex patterns of the application name.
    pub applications: Option<Vec<String>>,

    /// The regex patterns of the url.
    #[serde(default)]
    pub urls: Option<Vec<String>>,

    /// The regex patterns of the tag.
    #[serde(default)]
    pub tags: Option<Vec<String>>,

    /// The regex patterns of the priority.
    #[serde(default)]
    pub priorities: Option<Vec<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct SchedulerClusterConfigUploadBlockList {
    /// The regex patterns of the application name.
    #[serde(default)]
    pub applications: Option<Vec<String>>,

    /// The regex patterns of the url.
    #[serde(default)]
    pub urls: Option<Vec<String>>,

    /// The regex patterns of the tag.
    #[serde(default)]
    pub tags: Option<Vec<String>>,
}

/// Gets the block list configuration from dynconfig.
/// Returns the appropriate block list based on whether the client is a seed peer or not.
pub async fn get_block_list(
    config: &Arc<Config>,
    dynconfig: &Arc<Dynconfig>,
) -> Option<SchedulerClusterConfigBlockList> {
    let data = dynconfig.data.read().await;

    // Get the first scheduler from the response.
    let scheduler = data.schedulers.schedulers.first()?;

    // Get the scheduler cluster.
    let scheduler_cluster = scheduler.scheduler_cluster.as_ref()?;

    // Deserialize the appropriate config based on whether this is a seed peer.
    if config.seed_peer.enable {
        // For seed peers, use seed_client_config.
        let seed_client_config_bytes = &scheduler_cluster.seed_client_config;
        if seed_client_config_bytes.is_empty() {
            return None;
        }

        match serde_json::from_slice::<SchedulerClusterSeedClientConfig>(seed_client_config_bytes) {
            Ok(seed_client_config) => seed_client_config.block_list,
            Err(err) => {
                error!("failed to deserialize seed client config: {}", err);
                None
            }
        }
    } else {
        // For regular clients, use client_config.
        let client_config_bytes = &scheduler_cluster.client_config;
        if client_config_bytes.is_empty() {
            return None;
        }

        match serde_json::from_slice::<SchedulerClusterClientConfig>(client_config_bytes) {
            Ok(client_config) => client_config.block_list,
            Err(err) => {
                error!("failed to deserialize client config: {}", err);
                None
            }
        }
    }
}

/// Checks if a value matches any of the given regex patterns.
fn matches_any_pattern(value: &str, patterns: Option<&[String]>) -> bool {
    let Some(patterns) = patterns else {
        return false;
    };

    for pattern in patterns {
        match Regex::new(pattern) {
            Ok(re) => {
                if re.is_match(value) {
                    return true;
                }
            }
            Err(err) => {
                error!("invalid regex pattern '{}': {}", pattern, err);
            }
        }
    }

    false
}

/// Parameters for checking download block list.
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
pub struct UploadBlockListParams<'a> {
    /// The application name of the task.
    pub application: Option<&'a str>,
    /// The URL of the task.
    pub url: Option<&'a str>,
    /// The tag of the task.
    pub tag: Option<&'a str>,
}

/// Checks if a download task is blocked by the block list.
/// Returns true if the task should be blocked.
pub fn is_download_blocked(
    block_list: &SchedulerClusterConfigDownloadBlockList,
    params: &DownloadBlockListParams,
) -> bool {
    // Check application block list.
    if let Some(application) = params.application {
        if !application.is_empty()
            && matches_any_pattern(application, block_list.applications.as_deref())
        {
            return true;
        }
    }

    // Check URL block list.
    if let Some(url) = params.url {
        if !url.is_empty() && matches_any_pattern(url, block_list.urls.as_deref()) {
            return true;
        }
    }

    // Check tag block list.
    if let Some(tag) = params.tag {
        if !tag.is_empty() && matches_any_pattern(tag, block_list.tags.as_deref()) {
            return true;
        }
    }

    // Check priority block list.
    if let Some(priority) = params.priority {
        let priority_str = priority.to_string();
        if matches_any_pattern(&priority_str, block_list.priorities.as_deref()) {
            return true;
        }
    }

    false
}

/// Checks if an upload task is blocked by the block list.
/// Returns true if the task should be blocked.
pub fn is_upload_blocked(
    block_list: &SchedulerClusterConfigUploadBlockList,
    params: &UploadBlockListParams,
) -> bool {
    // Check application block list.
    if let Some(application) = params.application {
        if !application.is_empty()
            && matches_any_pattern(application, block_list.applications.as_deref())
        {
            return true;
        }
    }

    // Check URL block list.
    if let Some(url) = params.url {
        if !url.is_empty() && matches_any_pattern(url, block_list.urls.as_deref()) {
            return true;
        }
    }

    // Check tag block list.
    if let Some(tag) = params.tag {
        if !tag.is_empty() && matches_any_pattern(tag, block_list.tags.as_deref()) {
            return true;
        }
    }

    false
}

/// Checks if a download task should be blocked.
/// Returns true if the task is blocked by the block list.
pub async fn is_task_download_blocked(
    config: &Arc<Config>,
    dynconfig: &Arc<Dynconfig>,
    params: &DownloadBlockListParams<'_>,
) -> bool {
    let Some(block_list) = get_block_list(config, dynconfig).await else {
        return false;
    };

    let Some(task_block_list) = block_list.task else {
        return false;
    };

    let Some(download_block_list) = task_block_list.download else {
        return false;
    };

    is_download_blocked(&download_block_list, params)
}

/// Checks if a persistent task download should be blocked.
/// Returns true if the task is blocked by the block list.
pub async fn is_persistent_task_download_blocked(
    config: &Arc<Config>,
    dynconfig: &Arc<Dynconfig>,
    params: &DownloadBlockListParams<'_>,
) -> bool {
    let Some(block_list) = get_block_list(config, dynconfig).await else {
        return false;
    };

    let Some(persistent_task_block_list) = block_list.persistent_task else {
        return false;
    };

    let Some(download_block_list) = persistent_task_block_list.download else {
        return false;
    };

    is_download_blocked(&download_block_list, params)
}

/// Checks if a persistent task upload should be blocked.
/// Returns true if the task is blocked by the block list.
pub async fn is_persistent_task_upload_blocked(
    config: &Arc<Config>,
    dynconfig: &Arc<Dynconfig>,
    params: &UploadBlockListParams<'_>,
) -> bool {
    let Some(block_list) = get_block_list(config, dynconfig).await else {
        return false;
    };

    let Some(persistent_task_block_list) = block_list.persistent_task else {
        return false;
    };

    let Some(upload_block_list) = persistent_task_block_list.upload else {
        return false;
    };

    is_upload_blocked(&upload_block_list, params)
}

/// Checks if a persistent cache task download should be blocked.
/// Returns true if the task is blocked by the block list.
pub async fn is_persistent_cache_task_download_blocked(
    config: &Arc<Config>,
    dynconfig: &Arc<Dynconfig>,
    params: &DownloadBlockListParams<'_>,
) -> bool {
    let Some(block_list) = get_block_list(config, dynconfig).await else {
        return false;
    };

    let Some(persistent_cache_task_block_list) = block_list.persistent_cache_task else {
        return false;
    };

    let Some(download_block_list) = persistent_cache_task_block_list.download else {
        return false;
    };

    is_download_blocked(&download_block_list, params)
}

/// Checks if a persistent cache task upload should be blocked.
/// Returns true if the task is blocked by the block list.
pub async fn is_persistent_cache_task_upload_blocked(
    config: &Arc<Config>,
    dynconfig: &Arc<Dynconfig>,
    params: &UploadBlockListParams<'_>,
) -> bool {
    let Some(block_list) = get_block_list(config, dynconfig).await else {
        return false;
    };

    let Some(persistent_cache_task_block_list) = block_list.persistent_cache_task else {
        return false;
    };

    let Some(upload_block_list) = persistent_cache_task_block_list.upload else {
        return false;
    };

    is_upload_blocked(&upload_block_list, params)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scheduler_cluster_client_config_default() {
        let config = SchedulerClusterClientConfig::default();
        assert!(config.block_list.is_none());
    }

    #[test]
    fn test_scheduler_cluster_seed_client_config_default() {
        let config = SchedulerClusterSeedClientConfig::default();
        assert!(config.block_list.is_none());
    }

    #[test]
    fn test_scheduler_cluster_config_block_list_default() {
        let block_list = SchedulerClusterConfigBlockList::default();
        assert!(block_list.task.is_none());
        assert!(block_list.persistent_task.is_none());
        assert!(block_list.persistent_cache_task.is_none());
    }

    #[test]
    fn test_scheduler_cluster_config_download_block_list_default() {
        let download_block_list = SchedulerClusterConfigDownloadBlockList::default();
        assert!(download_block_list.applications.is_none());
        assert!(download_block_list.urls.is_none());
        assert!(download_block_list.tags.is_none());
        assert!(download_block_list.priorities.is_none());
    }

    #[test]
    fn test_scheduler_cluster_config_upload_block_list_default() {
        let upload_block_list = SchedulerClusterConfigUploadBlockList::default();
        assert!(upload_block_list.applications.is_none());
        assert!(upload_block_list.urls.is_none());
        assert!(upload_block_list.tags.is_none());
    }

    #[test]
    fn test_deserialize_client_config_with_block_list() {
        let json = r#"
        {
            "block_list": {
                "task": {
                    "download": {
                        "applications": ["^test-app.*$"],
                        "urls": ["^https://example.com/.*$"],
                        "tags": ["^test-tag$"],
                        "priorities": ["^1$", "^2$"]
                    }
                }
            }
        }
        "#;

        let config: SchedulerClusterClientConfig = serde_json::from_str(json).unwrap();
        assert!(config.block_list.is_some());
        let block_list = config.block_list.unwrap();
        assert!(block_list.task.is_some());
        let task = block_list.task.unwrap();
        assert!(task.download.is_some());
        let download = task.download.unwrap();
        assert_eq!(download.applications.unwrap(), vec!["^test-app.*$"]);
        assert_eq!(download.urls.unwrap(), vec!["^https://example.com/.*$"]);
        assert_eq!(download.tags.unwrap(), vec!["^test-tag$"]);
        assert_eq!(download.priorities.unwrap(), vec!["^1$", "^2$"]);
    }

    #[test]
    fn test_deserialize_seed_client_config_with_block_list() {
        let json = r#"
        {
            "block_list": {
                "persistent_task": {
                    "download": {
                        "applications": ["^seed-app.*$"]
                    },
                    "upload": {
                        "urls": ["^https://blocked.com/.*$"]
                    }
                }
            }
        }
        "#;

        let config: SchedulerClusterSeedClientConfig = serde_json::from_str(json).unwrap();
        assert!(config.block_list.is_some());
        let block_list = config.block_list.unwrap();
        assert!(block_list.persistent_task.is_some());
        let persistent_task = block_list.persistent_task.unwrap();
        assert!(persistent_task.download.is_some());
        assert!(persistent_task.upload.is_some());
    }

    #[test]
    fn test_deserialize_empty_config() {
        let json = r#"{}"#;
        let config: SchedulerClusterClientConfig = serde_json::from_str(json).unwrap();
        assert!(config.block_list.is_none());
    }

    #[test]
    fn test_is_download_blocked_by_application() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: Some(vec!["^test-app.*$".to_string()]),
            urls: None,
            tags: None,
            priorities: None,
        };

        // Should be blocked.
        let params = DownloadBlockListParams {
            application: Some("test-app-v1"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Should not be blocked.
        let params = DownloadBlockListParams {
            application: Some("other-app"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_download_blocked_by_url() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: None,
            urls: Some(vec!["^https://blocked.example.com/.*$".to_string()]),
            tags: None,
            priorities: None,
        };

        // Should be blocked.
        let params = DownloadBlockListParams {
            application: None,
            url: Some("https://blocked.example.com/file.txt"),
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Should not be blocked.
        let params = DownloadBlockListParams {
            application: None,
            url: Some("https://allowed.example.com/file.txt"),
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
            tags: Some(vec!["^blocked-tag$".to_string()]),
            priorities: None,
        };

        // Should be blocked.
        let params = DownloadBlockListParams {
            application: None,
            url: None,
            tag: Some("blocked-tag"),
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Should not be blocked.
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
            priorities: Some(vec!["^1$".to_string(), "^2$".to_string()]),
        };

        // Should be blocked.
        let params = DownloadBlockListParams {
            application: None,
            url: None,
            tag: None,
            priority: Some(1),
        };
        assert!(is_download_blocked(&block_list, &params));

        let params = DownloadBlockListParams {
            application: None,
            url: None,
            tag: None,
            priority: Some(2),
        };
        assert!(is_download_blocked(&block_list, &params));

        // Should not be blocked.
        let params = DownloadBlockListParams {
            application: None,
            url: None,
            tag: None,
            priority: Some(3),
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_download_blocked_empty_block_list() {
        let block_list = SchedulerClusterConfigDownloadBlockList::default();

        let params = DownloadBlockListParams {
            application: Some("any-app"),
            url: Some("https://any.url/file"),
            tag: Some("any-tag"),
            priority: Some(1),
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_download_blocked_empty_values() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: Some(vec!["^test$".to_string()]),
            urls: Some(vec!["^https://test.com$".to_string()]),
            tags: Some(vec!["^test$".to_string()]),
            priorities: None,
        };

        // Empty values should not be blocked.
        let params = DownloadBlockListParams {
            application: Some(""),
            url: Some(""),
            tag: Some(""),
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_download_blocked_none_values() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: Some(vec!["^test$".to_string()]),
            urls: Some(vec!["^https://test.com$".to_string()]),
            tags: Some(vec!["^test$".to_string()]),
            priorities: Some(vec!["^1$".to_string()]),
        };

        // None values should not be blocked.
        let params = DownloadBlockListParams {
            application: None,
            url: None,
            tag: None,
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_by_application() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: Some(vec!["^blocked-app.*$".to_string()]),
            urls: None,
            tags: None,
        };

        // Should be blocked.
        let params = UploadBlockListParams {
            application: Some("blocked-app-v2"),
            url: None,
            tag: None,
        };
        assert!(is_upload_blocked(&block_list, &params));

        // Should not be blocked.
        let params = UploadBlockListParams {
            application: Some("allowed-app"),
            url: None,
            tag: None,
        };
        assert!(!is_upload_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_by_url() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: None,
            urls: Some(vec!["^https://blocked.upload.com/.*$".to_string()]),
            tags: None,
        };

        // Should be blocked.
        let params = UploadBlockListParams {
            application: None,
            url: Some("https://blocked.upload.com/data"),
            tag: None,
        };
        assert!(is_upload_blocked(&block_list, &params));

        // Should not be blocked.
        let params = UploadBlockListParams {
            application: None,
            url: Some("https://allowed.upload.com/data"),
            tag: None,
        };
        assert!(!is_upload_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_by_tag() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: None,
            urls: None,
            tags: Some(vec!["^upload-blocked$".to_string()]),
        };

        // Should be blocked.
        let params = UploadBlockListParams {
            application: None,
            url: None,
            tag: Some("upload-blocked"),
        };
        assert!(is_upload_blocked(&block_list, &params));

        // Should not be blocked.
        let params = UploadBlockListParams {
            application: None,
            url: None,
            tag: Some("upload-allowed"),
        };
        assert!(!is_upload_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_empty_block_list() {
        let block_list = SchedulerClusterConfigUploadBlockList::default();

        let params = UploadBlockListParams {
            application: Some("any-app"),
            url: Some("https://any.url/file"),
            tag: Some("any-tag"),
        };
        assert!(!is_upload_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_empty_values() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: Some(vec!["^test$".to_string()]),
            urls: Some(vec!["^https://test.com$".to_string()]),
            tags: Some(vec!["^test$".to_string()]),
        };

        // Empty values should not be blocked.
        let params = UploadBlockListParams {
            application: Some(""),
            url: Some(""),
            tag: Some(""),
        };
        assert!(!is_upload_blocked(&block_list, &params));
    }

    #[test]
    fn test_is_upload_blocked_none_values() {
        let block_list = SchedulerClusterConfigUploadBlockList {
            applications: Some(vec!["^test$".to_string()]),
            urls: Some(vec!["^https://test.com$".to_string()]),
            tags: Some(vec!["^test$".to_string()]),
        };

        // None values should not be blocked.
        let params = UploadBlockListParams {
            application: None,
            url: None,
            tag: None,
        };
        assert!(!is_upload_blocked(&block_list, &params));
    }

    #[test]
    fn test_multiple_patterns_any_match() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: Some(vec![
                "^app-one$".to_string(),
                "^app-two$".to_string(),
                "^app-three$".to_string(),
            ]),
            urls: None,
            tags: None,
            priorities: None,
        };

        // First pattern matches.
        let params = DownloadBlockListParams {
            application: Some("app-one"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Second pattern matches.
        let params = DownloadBlockListParams {
            application: Some("app-two"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Third pattern matches.
        let params = DownloadBlockListParams {
            application: Some("app-three"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // No pattern matches.
        let params = DownloadBlockListParams {
            application: Some("app-four"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_regex_patterns() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: Some(vec![
                r"^prefix-.*-suffix$".to_string(),
                r"^\d+$".to_string(),
            ]),
            urls: None,
            tags: None,
            priorities: None,
        };

        // Matches prefix-*-suffix pattern.
        let params = DownloadBlockListParams {
            application: Some("prefix-middle-suffix"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Matches numeric pattern.
        let params = DownloadBlockListParams {
            application: Some("12345"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Does not match any pattern.
        let params = DownloadBlockListParams {
            application: Some("no-match"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_invalid_regex_pattern_does_not_block() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: Some(vec![
                r"[invalid".to_string(), // Invalid regex.
                r"^valid$".to_string(),  // Valid regex.
            ]),
            urls: None,
            tags: None,
            priorities: None,
        };

        // Invalid pattern should be skipped, but valid pattern should still work.
        let params = DownloadBlockListParams {
            application: Some("valid"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Neither pattern matches.
        let params = DownloadBlockListParams {
            application: Some("other"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_combined_blocking_first_match_wins() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: Some(vec!["^blocked-app$".to_string()]),
            urls: Some(vec!["^https://blocked.com/.*$".to_string()]),
            tags: Some(vec!["^blocked-tag$".to_string()]),
            priorities: Some(vec!["^0$".to_string()]),
        };

        // Blocked by application only.
        let params = DownloadBlockListParams {
            application: Some("blocked-app"),
            url: Some("https://allowed.com/file"),
            tag: Some("allowed-tag"),
            priority: Some(5),
        };
        assert!(is_download_blocked(&block_list, &params));

        // Blocked by URL only.
        let params = DownloadBlockListParams {
            application: Some("allowed-app"),
            url: Some("https://blocked.com/file"),
            tag: Some("allowed-tag"),
            priority: Some(5),
        };
        assert!(is_download_blocked(&block_list, &params));

        // Blocked by tag only.
        let params = DownloadBlockListParams {
            application: Some("allowed-app"),
            url: Some("https://allowed.com/file"),
            tag: Some("blocked-tag"),
            priority: Some(5),
        };
        assert!(is_download_blocked(&block_list, &params));

        // Blocked by priority only.
        let params = DownloadBlockListParams {
            application: Some("allowed-app"),
            url: Some("https://allowed.com/file"),
            tag: Some("allowed-tag"),
            priority: Some(0),
        };
        assert!(is_download_blocked(&block_list, &params));

        // Not blocked.
        let params = DownloadBlockListParams {
            application: Some("allowed-app"),
            url: Some("https://allowed.com/file"),
            tag: Some("allowed-tag"),
            priority: Some(5),
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_serialize_deserialize_roundtrip() {
        let original = SchedulerClusterClientConfig {
            block_list: Some(SchedulerClusterConfigBlockList {
                task: Some(SchedulerClusterConfigTaskBlockList {
                    download: Some(SchedulerClusterConfigDownloadBlockList {
                        applications: Some(vec!["^app$".to_string()]),
                        urls: Some(vec!["^url$".to_string()]),
                        tags: Some(vec!["^tag$".to_string()]),
                        priorities: Some(vec!["^1$".to_string()]),
                    }),
                }),
                persistent_task: None,
                persistent_cache_task: None,
            }),
        };

        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: SchedulerClusterClientConfig = serde_json::from_str(&serialized).unwrap();

        let block_list = deserialized.block_list.unwrap();
        let task = block_list.task.unwrap();
        let download = task.download.unwrap();
        assert_eq!(download.applications.unwrap(), vec!["^app$"]);
        assert_eq!(download.urls.unwrap(), vec!["^url$"]);
        assert_eq!(download.tags.unwrap(), vec!["^tag$"]);
        assert_eq!(download.priorities.unwrap(), vec!["^1$"]);
    }

    #[test]
    fn test_persistent_task_block_list_structure() {
        let json = r#"
        {
            "block_list": {
                "persistent_task": {
                    "download": {
                        "applications": ["^download-app$"],
                        "urls": ["^https://download.com$"],
                        "tags": ["^download-tag$"],
                        "priorities": ["^1$"]
                    },
                    "upload": {
                        "applications": ["^upload-app$"],
                        "urls": ["^https://upload.com$"],
                        "tags": ["^upload-tag$"]
                    }
                }
            }
        }
        "#;

        let config: SchedulerClusterClientConfig = serde_json::from_str(json).unwrap();
        let block_list = config.block_list.unwrap();
        let persistent_task = block_list.persistent_task.unwrap();

        // Verify download block list.
        let download = persistent_task.download.unwrap();
        assert_eq!(download.applications.unwrap(), vec!["^download-app$"]);
        assert_eq!(download.urls.unwrap(), vec!["^https://download.com$"]);
        assert_eq!(download.tags.unwrap(), vec!["^download-tag$"]);
        assert_eq!(download.priorities.unwrap(), vec!["^1$"]);

        // Verify upload block list.
        let upload = persistent_task.upload.unwrap();
        assert_eq!(upload.applications.unwrap(), vec!["^upload-app$"]);
        assert_eq!(upload.urls.unwrap(), vec!["^https://upload.com$"]);
        assert_eq!(upload.tags.unwrap(), vec!["^upload-tag$"]);
    }

    #[test]
    fn test_persistent_cache_task_block_list_structure() {
        let json = r#"
        {
            "block_list": {
                "persistent_cache_task": {
                    "download": {
                        "applications": ["^cache-download-app$"]
                    },
                    "upload": {
                        "applications": ["^cache-upload-app$"]
                    }
                }
            }
        }
        "#;

        let config: SchedulerClusterClientConfig = serde_json::from_str(json).unwrap();
        let block_list = config.block_list.unwrap();
        let persistent_cache_task = block_list.persistent_cache_task.unwrap();

        // Verify download block list.
        let download = persistent_cache_task.download.unwrap();
        assert_eq!(download.applications.unwrap(), vec!["^cache-download-app$"]);

        // Verify upload block list.
        let upload = persistent_cache_task.upload.unwrap();
        assert_eq!(upload.applications.unwrap(), vec!["^cache-upload-app$"]);
    }

    #[test]
    fn test_case_sensitive_regex_matching() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: Some(vec!["^TestApp$".to_string()]),
            urls: None,
            tags: None,
            priorities: None,
        };

        // Exact case match.
        let params = DownloadBlockListParams {
            application: Some("TestApp"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Different case should not match (regex is case-sensitive by default).
        let params = DownloadBlockListParams {
            application: Some("testapp"),
            url: None,
            tag: None,
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));
    }

    #[test]
    fn test_partial_url_matching() {
        let block_list = SchedulerClusterConfigDownloadBlockList {
            applications: None,
            urls: Some(vec!["blocked-domain".to_string()]),
            tags: None,
            priorities: None,
        };

        // Partial match should work.
        let params = DownloadBlockListParams {
            application: None,
            url: Some("https://blocked-domain.com/path"),
            tag: None,
            priority: None,
        };
        assert!(is_download_blocked(&block_list, &params));

        // Should not match.
        let params = DownloadBlockListParams {
            application: None,
            url: Some("https://allowed-domain.com/path"),
            tag: None,
            priority: None,
        };
        assert!(!is_download_blocked(&block_list, &params));
    }
}
