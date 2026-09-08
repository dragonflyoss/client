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
use std::sync::LazyLock;

/// The default filtered query params with s3 protocol to generate the task id.
const S3_FILTERED_QUERY_PARAMS: &[&str] = &[
    "X-Amz-Algorithm",
    "X-Amz-Credential",
    "X-Amz-Date",
    "X-Amz-Expires",
    "X-Amz-SignedHeaders",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
    "X-Amz-User-Agent",
];

/// The filtered query params with gcs protocol to generate the task id.
const GCS_FILTERED_QUERY_PARAMS: &[&str] = &[
    "X-Goog-Algorithm",
    "X-Goog-Credential",
    "X-Goog-Date",
    "X-Goog-Expires",
    "X-Goog-SignedHeaders",
    "X-Goog-Signature",
];

/// The filtered query params with oss protocol to generate the task id.
const OSS_FILTERED_QUERY_PARAMS: &[&str] =
    &["OSSAccessKeyId", "Expires", "Signature", "SecurityToken"];

/// The filtered query params with obs protocol to generate the task id.
const OBS_FILTERED_QUERY_PARAMS: &[&str] = &[
    "AccessKeyId",
    "Signature",
    "Expires",
    "X-Obs-Date",
    "X-Obs-Security-Token",
];

/// The filtered query params with cos protocol to generate the task id.
const COS_FILTERED_QUERY_PARAMS: &[&str] = &[
    "q-sign-algorithm",
    "q-ak",
    "q-sign-time",
    "q-key-time",
    "q-header-list",
    "q-url-param-list",
    "q-signature",
    "x-cos-security-token",
];

/// The filtered query params with containerd to generate the task id.
const CONTAINERD_FILTERED_QUERY_PARAMS: &[&str] = &["ns"];

/// The default filtered query params to generate the task id, deduplicated
/// across all protocols and built once.
static DEFAULT_PROXY_RULE_FILTERED_QUERY_PARAMS: LazyLock<Vec<String>> = LazyLock::new(|| {
    let mut visited = HashSet::new();
    let mut params = Vec::new();
    for query_param in [
        S3_FILTERED_QUERY_PARAMS,
        GCS_FILTERED_QUERY_PARAMS,
        OSS_FILTERED_QUERY_PARAMS,
        OBS_FILTERED_QUERY_PARAMS,
        COS_FILTERED_QUERY_PARAMS,
        CONTAINERD_FILTERED_QUERY_PARAMS,
    ]
    .concat()
    {
        if visited.insert(query_param) {
            params.push(query_param.to_string());
        }
    }

    params
});

/// The default filtered query params to generate the task id.
#[inline]
pub fn default_proxy_rule_filtered_query_params() -> Vec<String> {
    DEFAULT_PROXY_RULE_FILTERED_QUERY_PARAMS.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_proxy_rule_filtered_query_params_unions_protocols_once() {
        let params = default_proxy_rule_filtered_query_params();
        let unique_params: HashSet<&str> = params.iter().map(|param| param.as_str()).collect();
        let expected: HashSet<&str> = [
            S3_FILTERED_QUERY_PARAMS,
            GCS_FILTERED_QUERY_PARAMS,
            OSS_FILTERED_QUERY_PARAMS,
            OBS_FILTERED_QUERY_PARAMS,
            COS_FILTERED_QUERY_PARAMS,
            CONTAINERD_FILTERED_QUERY_PARAMS,
        ]
        .concat()
        .into_iter()
        .collect();

        assert_eq!(unique_params.len(), params.len());
        assert_eq!(unique_params, expected);
    }
}
