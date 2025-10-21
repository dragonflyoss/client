/*
 *     Copyright 2025 The Dragonfly Authors
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

use std::collections::HashSet;

/// default_s3_filtered_query_params is the default filtered query params with s3 protocol to generate the task id.
#[inline]
fn s3_filtered_query_params() -> Vec<String> {
    vec![
        "X-Amz-Algorithm".to_string(),
        "X-Amz-Credential".to_string(),
        "X-Amz-Date".to_string(),
        "X-Amz-Expires".to_string(),
        "X-Amz-SignedHeaders".to_string(),
        "X-Amz-Signature".to_string(),
        "X-Amz-Security-Token".to_string(),
        "X-Amz-User-Agent".to_string(),
    ]
}

/// gcs_filtered_query_params is the filtered query params with gcs protocol to generate the task id.
#[inline]
fn gcs_filtered_query_params() -> Vec<String> {
    vec![
        "X-Goog-Algorithm".to_string(),
        "X-Goog-Credential".to_string(),
        "X-Goog-Date".to_string(),
        "X-Goog-Expires".to_string(),
        "X-Goog-SignedHeaders".to_string(),
        "X-Goog-Signature".to_string(),
    ]
}

/// oss_filtered_query_params is the filtered query params with oss protocol to generate the task id.
#[inline]
fn oss_filtered_query_params() -> Vec<String> {
    vec![
        "OSSAccessKeyId".to_string(),
        "Expires".to_string(),
        "Signature".to_string(),
        "SecurityToken".to_string(),
    ]
}

/// obs_filtered_query_params is the filtered query params with obs protocol to generate the task id.
#[inline]
fn obs_filtered_query_params() -> Vec<String> {
    vec![
        "AccessKeyId".to_string(),
        "Signature".to_string(),
        "Expires".to_string(),
        "X-Obs-Date".to_string(),
        "X-Obs-Security-Token".to_string(),
    ]
}

/// cos_filtered_query_params is the filtered query params with cos protocol to generate the task id.
#[inline]
fn cos_filtered_query_params() -> Vec<String> {
    vec![
        "q-sign-algorithm".to_string(),
        "q-ak".to_string(),
        "q-sign-time".to_string(),
        "q-key-time".to_string(),
        "q-header-list".to_string(),
        "q-url-param-list".to_string(),
        "q-signature".to_string(),
        "x-cos-security-token".to_string(),
    ]
}

/// containerd_filtered_query_params is the filtered query params with containerd to generate the task id.
#[inline]
fn containerd_filtered_query_params() -> Vec<String> {
    vec!["ns".to_string()]
}

/// default_proxy_rule_filtered_query_params is the default filtered query params to generate the task id.
#[inline]
pub fn default_proxy_rule_filtered_query_params() -> Vec<String> {
    let mut visited = HashSet::new();
    for query_param in s3_filtered_query_params() {
        visited.insert(query_param);
    }

    for query_param in gcs_filtered_query_params() {
        visited.insert(query_param);
    }

    for query_param in oss_filtered_query_params() {
        visited.insert(query_param);
    }

    for query_param in obs_filtered_query_params() {
        visited.insert(query_param);
    }

    for query_param in cos_filtered_query_params() {
        visited.insert(query_param);
    }

    for query_param in containerd_filtered_query_params() {
        visited.insert(query_param);
    }

    visited.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn default_proxy_rule_filtered_query_params_contains_all_params() {
        let mut expected = HashSet::new();
        expected.extend(s3_filtered_query_params());
        expected.extend(gcs_filtered_query_params());
        expected.extend(oss_filtered_query_params());
        expected.extend(obs_filtered_query_params());
        expected.extend(cos_filtered_query_params());
        expected.extend(containerd_filtered_query_params());

        let actual = default_proxy_rule_filtered_query_params();
        let actual_set: HashSet<_> = actual.into_iter().collect();

        assert_eq!(actual_set, expected);
    }

    #[test]
    fn default_proxy_rule_removes_duplicates() {
        let params: Vec<String> = default_proxy_rule_filtered_query_params();
        let param_count = params.len();

        let unique_params: HashSet<_> = params.into_iter().collect();
        assert_eq!(unique_params.len(), param_count);
    }

    #[test]
    fn default_proxy_rule_filtered_query_params_contains_key_properties() {
        let params = default_proxy_rule_filtered_query_params();
        let param_set: HashSet<_> = params.into_iter().collect();

        assert!(param_set.contains("X-Amz-Signature"));
        assert!(param_set.contains("X-Goog-Signature"));
        assert!(param_set.contains("OSSAccessKeyId"));
        assert!(param_set.contains("X-Obs-Security-Token"));
        assert!(param_set.contains("q-sign-algorithm"));
        assert!(param_set.contains("ns"));
    }
}
