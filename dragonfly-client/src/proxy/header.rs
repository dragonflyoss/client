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
use dragonfly_api::common::v2::Priority;
use reqwest::header::HeaderMap;
use tracing::error;

/// DRAGONFLY_TAG_HEADER is the header key of tag in http request.
pub const DRAGONFLY_TAG_HEADER: &str = "X-Dragonfly-Tag";

/// DRAGONFLY_APPLICATION_HEADER is the header key of application in http request.
pub const DRAGONFLY_APPLICATION_HEADER: &str = "X-Dragonfly-Application";

/// DRAGONFLY_PRIORITY_HEADER is the header key of priority in http request,
/// refer to https://github.com/dragonflyoss/api/blob/main/proto/common.proto#L67.
pub const DRAGONFLY_PRIORITY_HEADER: &str = "X-Dragonfly-Priority";

/// DRAGONFLY_REGISTRY_HEADER is the header key of custom address of container registry.
pub const DRAGONFLY_REGISTRY_HEADER: &str = "X-Dragonfly-Registry";

/// DRAGONFLY_FILTERS_HEADER is the header key of filters in http request,
/// it is the filtered query params to generate the task id.
/// When filter is "X-Dragonfly-Filtered-Query-Params: Signature,Expires,ns" for example:
/// http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io and http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io
/// will generate the same task id.
/// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
pub const DRAGONFLY_FILTERED_QUERY_PARAMS_HEADER: &str = "X-Dragonfly-Filtered-Query-Params";

/// DRAGONFLY_USE_P2P_HEADER is the header key of use p2p in http request.
/// If the value is "true", the request will use P2P technology to distribute
/// the content. If the value is "false", but url matches the regular expression in proxy config.
/// The request will also use P2P technology to distribute the content.
pub const DRAGONFLY_USE_P2P_HEADER: &str = "X-Dragonfly-Use-P2P";

/// DRAGONFLY_PREFETCH_HEADER is the header key of prefetch in http request.
/// X-Dragonfly-Prefetch priority is higher than prefetch in config.
/// If the value is "true", the range request will prefetch the entire file.
/// If the value is "false", the range request will fetch the range content.
pub const DRAGONFLY_PREFETCH_HEADER: &str = "X-Dragonfly-Prefetch";

/// DRAGONFLY_OUTPUT_PATH_HEADER is the header key of absolute output path in http request.
///
/// If `X-Dragonfly-Output-Path` is set, the downloaded file will be saved to the specified path.
/// Dfdaemon will try to create hard link to the output path before starting the download. If hard link creation fails,
/// it will copy the file to the output path after the download is completed.
/// For more details refer to https://github.com/dragonflyoss/design/blob/main/systems-analysis/file-download-workflow-with-hard-link/README.md.
pub const DRAGONFLY_OUTPUT_PATH_HEADER: &str = "X-Dragonfly-Output-Path";

/// DRAGONFLY_FORCE_HARD_LINK_HEADER is the header key of force hard link in http request.
///
/// `X-Dragonfly-Force-Hard-Link` is the flag to indicate whether the download file must be hard linked to the output path.
/// For more details refer to https://github.com/dragonflyoss/design/blob/main/systems-analysis/file-download-workflow-with-hard-link/README.md.
pub const DRAGONFLY_FORCE_HARD_LINK_HEADER: &str = "X-Dragonfly-Force-Hard-Link";

/// DRAGONFLY_PIECE_LENGTH is the header key of piece length in http request.
/// If the value is set, the piece length will be used to download the file.
/// Different piece length will generate different task id. The value needs to
/// be set with human readable format and needs to be greater than or equal
/// to 4mib, for example: 4mib, 1gib
pub const DRAGONFLY_PIECE_LENGTH: &str = "X-Dragonfly-Piece-Length";

/// DRAGONFLY_CONTENT_FOR_CALCULATING_TASK_ID is the header key of content for calculating task id.
/// If DRAGONFLY_CONTENT_FOR_CALCULATING_TASK_ID is set, use its value to calculate the task ID.
/// Otherwise, calculate the task ID based on `url`, `piece_length`, `tag`, `application`, and `filtered_query_params`.
pub const DRAGONFLY_CONTENT_FOR_CALCULATING_TASK_ID: &str =
    "X-Dragonfly-Content-For-Calculating-Task-ID";

/// get_tag gets the tag from http header.
pub fn get_tag(header: &HeaderMap) -> Option<String> {
    header
        .get(DRAGONFLY_TAG_HEADER)
        .and_then(|tag| tag.to_str().ok())
        .map(|tag| tag.to_string())
}

/// get_application gets the application from http header.
pub fn get_application(header: &HeaderMap) -> Option<String> {
    header
        .get(DRAGONFLY_APPLICATION_HEADER)
        .and_then(|application| application.to_str().ok())
        .map(|application| application.to_string())
}

/// get_priority gets the priority from http header.
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

/// get_registry gets the custom address of container registry from http header.
pub fn get_registry(header: &HeaderMap) -> Option<String> {
    header
        .get(DRAGONFLY_REGISTRY_HEADER)
        .and_then(|registry| registry.to_str().ok())
        .map(|registry| registry.to_string())
}

/// get_filters gets the filters from http header.
pub fn get_filtered_query_params(
    header: &HeaderMap,
    default_filtered_query_params: Vec<String>,
) -> Vec<String> {
    match header.get(DRAGONFLY_FILTERED_QUERY_PARAMS_HEADER) {
        Some(filters) => match filters.to_str() {
            Ok(filters) => filters.split(',').map(|s| s.trim().to_string()).collect(),
            Err(err) => {
                error!("get filters from header failed: {}", err);
                default_filtered_query_params
            }
        },
        None => default_filtered_query_params,
    }
}

/// get_use_p2p gets the use p2p from http header.
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

/// get_prefetch gets the prefetch from http header.
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

/// get_output_path gets the output path from http header.
pub fn get_output_path(header: &HeaderMap) -> Option<String> {
    header
        .get(DRAGONFLY_OUTPUT_PATH_HEADER)
        .and_then(|output_path| output_path.to_str().ok())
        .map(|output_path| output_path.to_string())
}

/// get_force_hard_link gets the force hard link from http header.
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

/// get_piece_length gets the piece length from http header.
pub fn get_piece_length(header: &HeaderMap) -> Option<ByteSize> {
    match header.get(DRAGONFLY_PIECE_LENGTH) {
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

/// get_content_for_calculating_task_id gets the content for calculating task id from http header.
pub fn get_content_for_calculating_task_id(header: &HeaderMap) -> Option<String> {
    header
        .get(DRAGONFLY_CONTENT_FOR_CALCULATING_TASK_ID)
        .and_then(|content| content.to_str().ok())
        .map(|content| content.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn test_get_tag() {
        let mut headers = HeaderMap::new();
        headers.insert(DRAGONFLY_TAG_HEADER, HeaderValue::from_static("test-tag"));
        assert_eq!(get_tag(&headers), Some("test-tag".to_string()));

        let empty_headers = HeaderMap::new();
        assert_eq!(get_tag(&empty_headers), None);
    }

    #[test]
    fn test_get_application() {
        let mut headers = HeaderMap::new();
        headers.insert(
            DRAGONFLY_APPLICATION_HEADER,
            HeaderValue::from_static("test-app"),
        );
        assert_eq!(get_application(&headers), Some("test-app".to_string()));

        let empty_headers = HeaderMap::new();
        assert_eq!(get_application(&empty_headers), None);
    }

    #[test]
    fn test_get_priority() {
        let mut headers = HeaderMap::new();
        headers.insert(DRAGONFLY_PRIORITY_HEADER, HeaderValue::from_static("5"));
        assert_eq!(get_priority(&headers), 5);

        let empty_headers = HeaderMap::new();
        assert_eq!(get_priority(&empty_headers), Priority::Level6 as i32);

        headers.insert(
            DRAGONFLY_PRIORITY_HEADER,
            HeaderValue::from_static("invalid"),
        );
        assert_eq!(get_priority(&headers), Priority::Level6 as i32);
    }

    #[test]
    fn test_get_registry() {
        let mut headers = HeaderMap::new();
        headers.insert(
            DRAGONFLY_REGISTRY_HEADER,
            HeaderValue::from_static("test-registry"),
        );
        assert_eq!(get_registry(&headers), Some("test-registry".to_string()));

        let empty_headers = HeaderMap::new();
        assert_eq!(get_registry(&empty_headers), None);
    }

    #[test]
    fn test_get_filtered_query_params() {
        let mut headers = HeaderMap::new();
        headers.insert(
            DRAGONFLY_FILTERED_QUERY_PARAMS_HEADER,
            HeaderValue::from_static("param1,param2"),
        );
        assert_eq!(
            get_filtered_query_params(&headers, vec!["default".to_string()]),
            vec!["param1".to_string(), "param2".to_string()]
        );

        let empty_headers = HeaderMap::new();
        assert_eq!(
            get_filtered_query_params(&empty_headers, vec!["default".to_string()]),
            vec!["default".to_string()]
        );
    }

    #[test]
    fn test_get_use_p2p() {
        let mut headers = HeaderMap::new();
        headers.insert(DRAGONFLY_USE_P2P_HEADER, HeaderValue::from_static("true"));
        assert!(get_use_p2p(&headers));

        headers.insert(DRAGONFLY_USE_P2P_HEADER, HeaderValue::from_static("false"));
        assert!(!get_use_p2p(&headers));

        let empty_headers = HeaderMap::new();
        assert!(!get_use_p2p(&empty_headers));
    }

    #[test]
    fn test_get_prefetch() {
        let mut headers = HeaderMap::new();
        headers.insert(DRAGONFLY_PREFETCH_HEADER, HeaderValue::from_static("true"));
        assert_eq!(get_prefetch(&headers), Some(true));

        headers.insert(DRAGONFLY_PREFETCH_HEADER, HeaderValue::from_static("false"));
        assert_eq!(get_prefetch(&headers), Some(false));

        let empty_headers = HeaderMap::new();
        assert_eq!(get_prefetch(&empty_headers), None);
    }

    #[test]
    fn test_get_output_path() {
        let mut headers = HeaderMap::new();
        headers.insert(
            DRAGONFLY_OUTPUT_PATH_HEADER,
            HeaderValue::from_static("/path/to/output"),
        );
        assert_eq!(
            get_output_path(&headers),
            Some("/path/to/output".to_string())
        );

        let empty_headers = HeaderMap::new();
        assert_eq!(get_output_path(&empty_headers), None);
    }

    #[test]
    fn test_get_force_hard_link() {
        let mut headers = HeaderMap::new();
        headers.insert(
            DRAGONFLY_FORCE_HARD_LINK_HEADER,
            HeaderValue::from_static("true"),
        );
        assert!(get_force_hard_link(&headers));

        headers.insert(
            DRAGONFLY_FORCE_HARD_LINK_HEADER,
            HeaderValue::from_static("false"),
        );
        assert!(!get_force_hard_link(&headers));

        let empty_headers = HeaderMap::new();
        assert!(!get_force_hard_link(&empty_headers));
    }

    #[test]
    fn test_get_piece_length() {
        let mut headers = HeaderMap::new();
        headers.insert(DRAGONFLY_PIECE_LENGTH, HeaderValue::from_static("4mib"));
        assert_eq!(get_piece_length(&headers), Some(ByteSize::mib(4)));

        let empty_headers = HeaderMap::new();
        assert_eq!(get_piece_length(&empty_headers), None);

        headers.insert(DRAGONFLY_PIECE_LENGTH, HeaderValue::from_static("invalid"));
        assert_eq!(get_piece_length(&headers), None);

        headers.insert(DRAGONFLY_PIECE_LENGTH, HeaderValue::from_static("0"));
        assert_eq!(get_piece_length(&headers), Some(ByteSize::b(0)));
    }

    #[test]
    fn test_get_content_for_calculating_task_id() {
        let mut headers = HeaderMap::new();
        headers.insert(
            DRAGONFLY_CONTENT_FOR_CALCULATING_TASK_ID,
            HeaderValue::from_static("test-content"),
        );
        assert_eq!(
            get_content_for_calculating_task_id(&headers),
            Some("test-content".to_string())
        );

        let empty_headers = HeaderMap::new();
        assert_eq!(get_registry(&empty_headers), None);
    }
}
