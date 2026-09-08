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

use bytesize::ByteSize;
use dragonfly_api::common::v2::{Priority, SchedulingPolicy};
use reqwest::header::HeaderMap;
use std::{fmt, str::FromStr};
use tracing::error;

/// The header key of tag in http request.
pub const DRAGONFLY_TAG_HEADER: &str = "X-Dragonfly-Tag";

/// The header key of application in http request.
pub const DRAGONFLY_APPLICATION_HEADER: &str = "X-Dragonfly-Application";

/// The header key of priority in http request,
/// refer to https://github.com/dragonflyoss/api/blob/main/proto/common.proto#L67.
pub const DRAGONFLY_PRIORITY_HEADER: &str = "X-Dragonfly-Priority";

/// The header key of custom address of container registry.
pub const DRAGONFLY_REGISTRY_HEADER: &str = "X-Dragonfly-Registry";

/// The header key of filters in http request,
/// it is the filtered query params to generate the task id.
/// When filter is "X-Dragonfly-Filtered-Query-Params: Signature,Expires,ns" for example:
/// http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io and http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io
/// will generate the same task id.
/// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
pub const DRAGONFLY_FILTERED_QUERY_PARAMS_HEADER: &str = "X-Dragonfly-Filtered-Query-Params";

/// The header key of use p2p in http request.
/// If the value is "true", the request will use P2P technology to distribute
/// the content. If the value is "false", but url matches the regular expression in proxy config.
/// The request will also use P2P technology to distribute the content.
pub const DRAGONFLY_USE_P2P_HEADER: &str = "X-Dragonfly-Use-P2P";

/// The header key of prefetch in http request.
/// X-Dragonfly-Prefetch priority is higher than prefetch in config.
/// If the value is "true", the range request will prefetch the entire file.
/// If the value is "false", the range request will fetch the range content.
pub const DRAGONFLY_PREFETCH_HEADER: &str = "X-Dragonfly-Prefetch";

/// The header key of absolute output path in http request.
///
/// If `X-Dragonfly-Output-Path` is set, the downloaded file will be saved to the specified path.
/// Dfdaemon will try to create hard link to the output path before starting the download. If hard link creation fails,
/// it will copy the file to the output path after the download is completed.
/// For more details refer to https://github.com/dragonflyoss/design/blob/main/systems-analysis/file-download-workflow-with-hard-link/README.md.
pub const DRAGONFLY_OUTPUT_PATH_HEADER: &str = "X-Dragonfly-Output-Path";

/// The header key of force hard link in http request.
///
/// `X-Dragonfly-Force-Hard-Link` is the flag to indicate whether the download file must be hard linked to the output path.
/// For more details refer to https://github.com/dragonflyoss/design/blob/main/systems-analysis/file-download-workflow-with-hard-link/README.md.
pub const DRAGONFLY_FORCE_HARD_LINK_HEADER: &str = "X-Dragonfly-Force-Hard-Link";

/// The header key of piece length in http request.
/// If the value is set, the piece length will be used to download the file.
/// Different piece length will generate different task id. The value needs to
/// be set with human readable format and needs to be greater than or equal
/// to 4mib, for example: 4mib, 1gib
pub const DRAGONFLY_PIECE_LENGTH_HEADER: &str = "X-Dragonfly-Piece-Length";

/// The header key of content for calculating task id.
/// If DRAGONFLY_CONTENT_FOR_CALCULATING_TASK_ID_HEADER is set, use its value to calculate the task ID.
/// Otherwise, calculate the task ID based on `url`, `piece_length`, `tag`, `application`, and `filtered_query_params`.
pub const DRAGONFLY_CONTENT_FOR_CALCULATING_TASK_ID_HEADER: &str =
    "X-Dragonfly-Content-For-Calculating-Task-ID";

/// The header key to indicate whether to use the blob's content
/// digest (e.g., SHA-256 hash) for task ID calculation, when downloading from OCI registries. When enabled
/// for OCI blob URLs (e.g., /v2/<name>/blobs/sha256:<digest>), the task ID is derived from the blob digest
/// rather than the full URL. This enables deduplication across registries - the same blob from different
/// registries shares one task ID, eliminating redundant downloads and storage.
pub const DRAGONFLY_ENABLE_TASK_ID_BASED_BLOB_DIGEST: &str =
    "X-Dragonfly-Enable-Task-ID-Based-Blob-Digest";

/// The header key of scheduling policy. It represents how the download interacts
/// with the scheduler. The value is case-insensitive, e.g. "auto" or "always".
/// "auto" downloads small files from the source directly, skipping the scheduler.
/// "always" downloads through the scheduler even if the content length is smaller
/// than the minimum piece length, so that the peer announces the task to the
/// scheduler and other peers can discover it as a parent.
pub const DRAGONFLY_SCHEDULING_POLICY_HEADER: &str = "X-Dragonfly-Scheduling-Policy";

/// The response header key to indicate whether the task download finished.
/// When the task download is finished, the response will include this header with the value `"true"`,
/// indicating that the download hit the local cache.
pub const DRAGONFLY_TASK_DOWNLOAD_FINISHED_HEADER: &str = "X-Dragonfly-Task-Download-Finished";

/// The response header key of task id. Client will calculate the task ID
/// based on `url`, `piece_length`, `tag`, `application`, and `filtered_query_params`.
pub const DRAGONFLY_TASK_ID_HEADER: &str = "X-Dragonfly-Task-ID";

/// The response header key of server IP.
/// It is used to indicate the IP address of the server that handled the request.
pub const DRAGONFLY_SERVER_IP_HEADER: &str = "X-Dragonfly-Server-IP";

/// The response header key of error type.
/// It is used to indicate the type of error that occurred during the request.
/// The value of this header can be one of the following:
/// - "backend": Indicates an upstream error occurred during the request.
/// - "proxy": Indicates a proxy error occurred during the request.
/// - "dfdaemon": Indicates a dfdaemon error occurred during the request.
pub const DRAGONFLY_ERROR_TYPE_HEADER: &str = "X-Dragonfly-Error-Type";

/// Represents the type of error that occurred during the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorType {
    /// Indicates an upstream error occurred during the request.
    Backend,

    /// Indicates a proxy error occurred during the request.
    Proxy,

    /// Indicates a dfdaemon error occurred during the request.
    Dfdaemon,
}

/// Error type implements as_str.
impl ErrorType {
    pub fn as_str(&self) -> &'static str {
        match self {
            ErrorType::Backend => "backend",
            ErrorType::Proxy => "proxy",
            ErrorType::Dfdaemon => "dfdaemon",
        }
    }
}

/// Error type implements fmt::Display.
impl fmt::Display for ErrorType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Implements std::str::FromStr.
impl FromStr for ErrorType {
    type Err = String;

    /// Parses a string into an ErrorType. The string must be one of "backend", "proxy", or
    /// "dfdaemon".
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "backend" => Ok(ErrorType::Backend),
            "proxy" => Ok(ErrorType::Proxy),
            "dfdaemon" => Ok(ErrorType::Dfdaemon),
            _ => Err(format!("invalid error type: {s}")),
        }
    }
}

/// Get X-Dragonfly-Tag header value to determine the tag of the task.
pub fn get_tag(header: &HeaderMap) -> Option<String> {
    header
        .get(DRAGONFLY_TAG_HEADER)
        .and_then(|tag| tag.to_str().ok())
        .map(|tag| tag.to_string())
}

/// Get X-Dragonfly-Application header value to determine the application of the task.
pub fn get_application(header: &HeaderMap) -> Option<String> {
    header
        .get(DRAGONFLY_APPLICATION_HEADER)
        .and_then(|application| application.to_str().ok())
        .map(|application| application.to_string())
}

/// Get X-Dragonfly-Priority header value to determine the priority of the task.
pub fn get_priority(header: &HeaderMap) -> i32 {
    let default_priority = Priority::Level6 as i32;
    match header.get(DRAGONFLY_PRIORITY_HEADER) {
        Some(priority) => match priority.to_str() {
            Ok(priority) => match priority.parse::<i32>() {
                Ok(priority) => priority,
                Err(err) => {
                    error!("parse priority from header failed: {}", err);
                    default_priority
                }
            },
            Err(err) => {
                error!("get priority from header failed: {}", err);
                default_priority
            }
        },
        None => default_priority,
    }
}

/// Get X-Dragonfly-Registry header value to determine the custom address of container registry for
/// downloading.
pub fn get_registry(header: &HeaderMap) -> Option<String> {
    header
        .get(DRAGONFLY_REGISTRY_HEADER)
        .and_then(|registry| registry.to_str().ok())
        .map(|registry| registry.to_string())
}

/// Get X-Dragonfly-Filtered-Query-Params header value to determine the filtered query params for
/// generating task ID.
pub fn get_filtered_query_params(
    header: &HeaderMap,
    default_filtered_query_params: &[String],
) -> Vec<String> {
    match header.get(DRAGONFLY_FILTERED_QUERY_PARAMS_HEADER) {
        Some(filters) => match filters.to_str() {
            Ok(filters) => filters.split(',').map(|s| s.trim().to_string()).collect(),
            Err(err) => {
                error!("get filters from header failed: {}", err);
                default_filtered_query_params.to_vec()
            }
        },
        None => default_filtered_query_params.to_vec(),
    }
}

/// Get X-Dragonfly-Use-P2P header value to determine whether to use P2P technology to distribute
/// the content.
pub fn get_use_p2p(header: &HeaderMap) -> bool {
    match header.get(DRAGONFLY_USE_P2P_HEADER) {
        Some(value) => match value.to_str() {
            Ok(value) => value.eq_ignore_ascii_case("true"),
            Err(err) => {
                error!("get use p2p from header failed: {}", err);
                false
            }
        },
        None => false,
    }
}

/// Get X-Dragonfly-Prefetch header value to determine whether to prefetch the entire file for
/// range request.
pub fn get_prefetch(header: &HeaderMap) -> Option<bool> {
    match header.get(DRAGONFLY_PREFETCH_HEADER) {
        Some(value) => match value.to_str() {
            Ok(value) => Some(value.eq_ignore_ascii_case("true")),
            Err(err) => {
                error!("get use p2p from header failed: {}", err);
                None
            }
        },
        None => None,
    }
}

/// Get X-Dragonfly-Output-Path header value to determine the absolute output path for the
/// downloaded file.
pub fn get_output_path(header: &HeaderMap) -> Option<String> {
    header
        .get(DRAGONFLY_OUTPUT_PATH_HEADER)
        .and_then(|output_path| output_path.to_str().ok())
        .map(|output_path| output_path.to_string())
}

/// Get X-Dragonfly-Force-Hard-Link header value to determine whether the download file must be
/// hard linked to the output path.
pub fn get_force_hard_link(header: &HeaderMap) -> bool {
    match header.get(DRAGONFLY_FORCE_HARD_LINK_HEADER) {
        Some(value) => match value.to_str() {
            Ok(value) => value.eq_ignore_ascii_case("true"),
            Err(err) => {
                error!("get force hard link from header failed: {}", err);
                false
            }
        },
        None => false,
    }
}

/// Get X-Dragonfly-Piece-Length header value to determine the piece length for downloading the
/// file.
pub fn get_piece_length(header: &HeaderMap) -> Option<ByteSize> {
    match header.get(DRAGONFLY_PIECE_LENGTH_HEADER) {
        Some(piece_length) => match piece_length.to_str() {
            Ok(piece_length) => match piece_length.parse::<ByteSize>() {
                Ok(piece_length) => Some(piece_length),
                Err(err) => {
                    error!("parse piece length from header failed: {}", err);
                    None
                }
            },
            Err(err) => {
                error!("get piece length from header failed: {}", err);
                None
            }
        },
        None => None,
    }
}

/// Get X-Dragonfly-Content-For-Calculating-Task-ID header value to determine the content for
/// calculating task ID.
pub fn get_content_for_calculating_task_id(header: &HeaderMap) -> Option<String> {
    header
        .get(DRAGONFLY_CONTENT_FOR_CALCULATING_TASK_ID_HEADER)
        .and_then(|content| content.to_str().ok())
        .map(|content| content.to_string())
}

/// Get X-Dragonfly-Enable-Task-ID-Based-Blob-Digest header value to determine whether to use the
/// blob's content digest for task ID calculation.
pub fn get_enable_task_id_based_blob_digest(header: &HeaderMap, default: bool) -> bool {
    match header.get(DRAGONFLY_ENABLE_TASK_ID_BASED_BLOB_DIGEST) {
        Some(value) => match value.to_str() {
            Ok(value) => value.eq_ignore_ascii_case("true"),
            Err(err) => {
                error!(
                    "get enable task id based blob digest from header failed: {}",
                    err
                );
                default
            }
        },
        None => default,
    }
}

/// Get X-Dragonfly-Scheduling-Policy header value to determine how the download
/// interacts with the scheduler.
pub fn get_scheduling_policy(header: &HeaderMap, default: SchedulingPolicy) -> SchedulingPolicy {
    match header.get(DRAGONFLY_SCHEDULING_POLICY_HEADER) {
        Some(value) => match value.to_str() {
            Ok(value) if value.eq_ignore_ascii_case("auto") => SchedulingPolicy::Auto,
            Ok(value) if value.eq_ignore_ascii_case("always") => SchedulingPolicy::Always,
            Ok(value) => {
                error!("invalid scheduling policy from header: {}", value);
                default
            }
            Err(err) => {
                error!("get scheduling policy from header failed: {}", err);
                default
            }
        },
        None => default,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::type_complexity)]

    use super::*;
    use reqwest::header::HeaderValue;

    #[test]
    fn error_type_parses_and_formats_its_name() {
        let test_cases = vec![
            ("backend", Ok(ErrorType::Backend), Some("backend")),
            ("proxy", Ok(ErrorType::Proxy), Some("proxy")),
            ("dfdaemon", Ok(ErrorType::Dfdaemon), Some("dfdaemon")),
            (
                "Backend",
                Err("invalid error type: Backend".to_string()),
                None,
            ),
        ];

        for (name, expected, expected_display) in test_cases {
            let error_type = name.parse::<ErrorType>();
            assert_eq!(error_type, expected);
            assert_eq!(
                error_type.ok().map(|error_type| error_type.to_string()),
                expected_display.map(str::to_string)
            );
        }
    }

    #[test]
    fn string_getters_return_the_header_value() {
        let test_cases: Vec<(&'static str, fn(&HeaderMap) -> Option<String>)> = vec![
            (DRAGONFLY_TAG_HEADER, get_tag),
            (DRAGONFLY_APPLICATION_HEADER, get_application),
            (DRAGONFLY_REGISTRY_HEADER, get_registry),
            (DRAGONFLY_OUTPUT_PATH_HEADER, get_output_path),
            (
                DRAGONFLY_CONTENT_FOR_CALCULATING_TASK_ID_HEADER,
                get_content_for_calculating_task_id,
            ),
        ];

        for (name, getter) in test_cases {
            let mut headers = HeaderMap::new();
            headers.insert(name, HeaderValue::from_str("value").unwrap());
            assert_eq!(getter(&headers), Some("value".to_string()));

            assert_eq!(getter(&HeaderMap::new()), None);

            headers.insert(name, HeaderValue::from_str("é").unwrap());
            assert_eq!(getter(&headers), None);
        }
    }

    #[test]
    fn get_priority_parses_the_header_or_falls_back_to_level6() {
        let test_cases = vec![
            (Some("5"), 5),
            (Some("invalid"), Priority::Level6 as i32),
            (Some("é"), Priority::Level6 as i32),
            (None, Priority::Level6 as i32),
        ];

        for (value, expected) in test_cases {
            let mut headers = HeaderMap::new();
            if let Some(value) = value {
                headers.insert(
                    DRAGONFLY_PRIORITY_HEADER,
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            assert_eq!(get_priority(&headers), expected);
        }
    }

    #[test]
    fn get_filtered_query_params_splits_the_header_or_uses_defaults() {
        let default_filtered_query_params = vec!["default".to_string()];

        let test_cases = vec![
            (Some("param1,param2"), vec!["param1", "param2"]),
            (
                Some("param1, param2 ,param3"),
                vec!["param1", "param2", "param3"],
            ),
            (Some("é"), vec!["default"]),
            (None, vec!["default"]),
        ];

        for (value, expected) in test_cases {
            let expected: Vec<String> = expected.iter().map(|param| param.to_string()).collect();
            let mut headers = HeaderMap::new();
            if let Some(value) = value {
                headers.insert(
                    DRAGONFLY_FILTERED_QUERY_PARAMS_HEADER,
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            assert_eq!(
                get_filtered_query_params(&headers, &default_filtered_query_params),
                expected
            );
        }
    }

    #[test]
    fn bool_getters_are_true_only_for_a_true_header() {
        let test_cases: Vec<(&'static str, fn(&HeaderMap) -> bool)> = vec![
            (DRAGONFLY_USE_P2P_HEADER, get_use_p2p),
            (DRAGONFLY_FORCE_HARD_LINK_HEADER, get_force_hard_link),
        ];

        for (name, getter) in test_cases {
            let mut headers = HeaderMap::new();
            headers.insert(name, HeaderValue::from_str("true").unwrap());
            assert!(getter(&headers));

            headers.insert(name, HeaderValue::from_str("TRUE").unwrap());
            assert!(getter(&headers));

            headers.insert(name, HeaderValue::from_str("false").unwrap());
            assert!(!getter(&headers));

            headers.insert(name, HeaderValue::from_str("é").unwrap());
            assert!(!getter(&headers));

            assert!(!getter(&HeaderMap::new()));
        }
    }

    #[test]
    fn get_prefetch_returns_the_flag_or_none() {
        let test_cases = vec![
            (Some("true"), Some(true)),
            (Some("false"), Some(false)),
            (Some("é"), None),
            (None, None),
        ];

        for (value, expected) in test_cases {
            let mut headers = HeaderMap::new();
            if let Some(value) = value {
                headers.insert(
                    DRAGONFLY_PREFETCH_HEADER,
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            assert_eq!(get_prefetch(&headers), expected);
        }
    }

    #[test]
    fn get_piece_length_parses_human_readable_sizes() {
        let test_cases = vec![
            (Some("4mib"), Some(ByteSize::mib(4))),
            (Some("0"), Some(ByteSize::b(0))),
            (Some("invalid"), None),
            (Some("é"), None),
            (None, None),
        ];

        for (value, expected) in test_cases {
            let mut headers = HeaderMap::new();
            if let Some(value) = value {
                headers.insert(
                    DRAGONFLY_PIECE_LENGTH_HEADER,
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            assert_eq!(get_piece_length(&headers), expected);
        }
    }

    #[test]
    fn get_enable_task_id_based_blob_digest_falls_back_to_default() {
        let test_cases = vec![
            (Some("true"), false, true),
            (Some("false"), true, false),
            (Some("é"), true, true),
            (None, true, true),
            (None, false, false),
        ];

        for (value, default, expected) in test_cases {
            let mut headers = HeaderMap::new();
            if let Some(value) = value {
                headers.insert(
                    DRAGONFLY_ENABLE_TASK_ID_BASED_BLOB_DIGEST,
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            assert_eq!(
                get_enable_task_id_based_blob_digest(&headers, default),
                expected
            );
        }
    }

    #[test]
    fn get_scheduling_policy_parses_case_insensitively_or_uses_default() {
        let test_cases = vec![
            (
                Some("always"),
                SchedulingPolicy::Auto,
                SchedulingPolicy::Always,
            ),
            (
                Some("AUTO"),
                SchedulingPolicy::Always,
                SchedulingPolicy::Auto,
            ),
            (
                Some("invalid"),
                SchedulingPolicy::Always,
                SchedulingPolicy::Always,
            ),
            (Some("é"), SchedulingPolicy::Auto, SchedulingPolicy::Auto),
            (None, SchedulingPolicy::Always, SchedulingPolicy::Always),
            (None, SchedulingPolicy::Auto, SchedulingPolicy::Auto),
        ];

        for (value, default, expected) in test_cases {
            let mut headers = HeaderMap::new();
            if let Some(value) = value {
                headers.insert(
                    DRAGONFLY_SCHEDULING_POLICY_HEADER,
                    HeaderValue::from_str(value).unwrap(),
                );
            }
            assert_eq!(get_scheduling_policy(&headers, default), expected);
        }
    }
}
