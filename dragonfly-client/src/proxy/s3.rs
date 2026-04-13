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

use dragonfly_client_config::dfdaemon::ProxyS3;
use hyper::{Method, Uri};
use url::Url;

/// Route is how the proxy should handle a classified S3 request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// ViaDfdaemon proxies the request through the Dragonfly download path.
    ViaDfdaemon,

    /// Passthrough proxies the request directly to the origin.
    Passthrough,
}

impl Route {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ViaDfdaemon => "dfdaemon",
            Self::Passthrough => "passthrough",
        }
    }
}

/// Operation is the S3 API operation inferred from the request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// GetObject is a GET request for an object body.
    GetObject,

    /// HeadObject is a HEAD request for object metadata.
    HeadObject,

    /// ListObjectsV2 is a GET request for listing bucket contents.
    ListObjectsV2,

    /// Other is any other S3 API request that should remain passthrough.
    Other,
}

impl Operation {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::GetObject => "get_object",
            Self::HeadObject => "head_object",
            Self::ListObjectsV2 => "list_objects_v2",
            Self::Other => "other",
        }
    }
}

/// ClassifiedRequest is the S3 classification result for a proxied request.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassifiedRequest {
    /// Operation is the inferred S3 API operation.
    pub operation: Operation,

    /// Route is how the proxy should handle the request.
    pub route: Route,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndpointStyle {
    Path,
    VirtualHosted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EndpointMatch {
    style: EndpointStyle,
}

/// classify_request classifies a request targeting an S3-compatible endpoint.
pub fn classify_request(config: &ProxyS3, method: &Method, uri: &Uri) -> Option<ClassifiedRequest> {
    if !config.enable {
        return None;
    }

    let url = Url::parse(&uri.to_string()).ok()?;
    let host = url.host_str()?;
    let endpoint = match_endpoint(host, config)?;
    let object_key = extract_object_key(&url, endpoint.style);

    if is_list_objects_v2(method, &url) {
        return Some(ClassifiedRequest {
            operation: Operation::ListObjectsV2,
            route: Route::Passthrough,
        });
    }

    if *method == Method::HEAD && object_key.is_some() && only_head_object_query_params(&url) {
        return Some(ClassifiedRequest {
            operation: Operation::HeadObject,
            route: Route::Passthrough,
        });
    }

    if *method == Method::GET && object_key.is_some() && only_get_object_query_params(&url) {
        return Some(ClassifiedRequest {
            operation: Operation::GetObject,
            route: Route::ViaDfdaemon,
        });
    }

    Some(ClassifiedRequest {
        operation: Operation::Other,
        route: Route::Passthrough,
    })
}

fn match_endpoint(host: &str, config: &ProxyS3) -> Option<EndpointMatch> {
    let normalized_host = host.to_ascii_lowercase();

    if config.detect_aws_endpoints {
        if let Some(endpoint) = match_aws_endpoint(&normalized_host) {
            return Some(endpoint);
        }
    }

    if config
        .hosts
        .iter()
        .any(|configured_host| configured_host.eq_ignore_ascii_case(normalized_host.as_str()))
    {
        return Some(EndpointMatch {
            style: EndpointStyle::Path,
        });
    }

    for suffix in &config.host_suffixes {
        let normalized_suffix = suffix.trim_start_matches('.').to_ascii_lowercase();
        if normalized_suffix.is_empty() {
            continue;
        }

        if normalized_host == normalized_suffix {
            return Some(EndpointMatch {
                style: EndpointStyle::Path,
            });
        }

        let virtual_suffix = format!(".{}", normalized_suffix);
        if normalized_host.ends_with(&virtual_suffix) {
            return Some(EndpointMatch {
                style: EndpointStyle::VirtualHosted,
            });
        }
    }

    None
}

fn match_aws_endpoint(host: &str) -> Option<EndpointMatch> {
    let is_aws_host = host.ends_with(".amazonaws.com") || host.ends_with(".amazonaws.com.cn");
    if !is_aws_host {
        return None;
    }

    if host.starts_with("s3.") || host.starts_with("s3-") {
        return Some(EndpointMatch {
            style: EndpointStyle::Path,
        });
    }

    if host.contains(".s3.") || host.contains(".s3-") {
        return Some(EndpointMatch {
            style: EndpointStyle::VirtualHosted,
        });
    }

    None
}

fn extract_object_key(url: &Url, style: EndpointStyle) -> Option<String> {
    let path = url.path().trim_start_matches('/');
    if path.is_empty() {
        return None;
    }

    match style {
        EndpointStyle::Path => {
            let (_bucket, key) = path.split_once('/')?;
            if key.is_empty() {
                None
            } else {
                Some(key.to_string())
            }
        }
        EndpointStyle::VirtualHosted => Some(path.to_string()),
    }
}

fn is_list_objects_v2(method: &Method, url: &Url) -> bool {
    if *method != Method::GET {
        return false;
    }

    let mut has_list_type = false;
    for (key, value) in url.query_pairs() {
        let key = key.to_ascii_lowercase();
        if key == "x-id" && value.eq_ignore_ascii_case("ListObjectsV2") {
            return true;
        }

        if key == "list-type" && value == "2" {
            has_list_type = true;
        }
    }

    has_list_type && only_list_objects_v2_query_params(url)
}

fn only_head_object_query_params(url: &Url) -> bool {
    only_allowed_query_params(url, |key| matches!(key, "versionid" | "x-id"))
}

fn only_get_object_query_params(url: &Url) -> bool {
    only_allowed_query_params(url, |key| {
        matches!(
            key,
            "versionid"
                | "x-id"
                | "response-cache-control"
                | "response-content-disposition"
                | "response-content-encoding"
                | "response-content-language"
                | "response-content-type"
                | "response-expires"
        )
    })
}

fn only_list_objects_v2_query_params(url: &Url) -> bool {
    only_allowed_query_params(url, |key| {
        matches!(
            key,
            "list-type"
                | "prefix"
                | "delimiter"
                | "continuation-token"
                | "encoding-type"
                | "fetch-owner"
                | "max-keys"
                | "start-after"
                | "x-id"
        )
    })
}

fn only_allowed_query_params(url: &Url, is_allowed: impl Fn(&str) -> bool) -> bool {
    url.query_pairs().all(|(key, _)| {
        let normalized_key = key.to_ascii_lowercase();
        normalized_key.starts_with("x-amz-") || is_allowed(normalized_key.as_str())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn enabled_config() -> ProxyS3 {
        ProxyS3 {
            enable: true,
            ..Default::default()
        }
    }

    #[test]
    fn should_not_match_when_s3_proxy_is_disabled() {
        let request = classify_request(
            &ProxyS3::default(),
            &Method::GET,
            &"https://bucket.s3.us-west-2.amazonaws.com/model.bin"
                .parse()
                .unwrap(),
        );

        assert_eq!(request, None);
    }

    #[test]
    fn should_classify_virtual_hosted_get_object() {
        let request = classify_request(
            &enabled_config(),
            &Method::GET,
            &"https://bucket.s3.us-west-2.amazonaws.com/models/model.bin?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature=test"
                .parse()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(request.operation, Operation::GetObject);
        assert_eq!(request.route, Route::ViaDfdaemon);
    }

    #[test]
    fn should_classify_path_style_list_objects_v2() {
        let request = classify_request(
            &enabled_config(),
            &Method::GET,
            &"https://s3.us-west-2.amazonaws.com/test-bucket?list-type=2&prefix=models/"
                .parse()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(request.operation, Operation::ListObjectsV2);
        assert_eq!(request.route, Route::Passthrough);
    }

    #[test]
    fn should_classify_head_object_as_passthrough() {
        let request = classify_request(
            &enabled_config(),
            &Method::HEAD,
            &"https://bucket.s3.amazonaws.com/models/model.bin?x-id=HeadObject"
                .parse()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(request.operation, Operation::HeadObject);
        assert_eq!(request.route, Route::Passthrough);
    }

    #[test]
    fn should_passthrough_other_object_subresources() {
        let request = classify_request(
            &enabled_config(),
            &Method::GET,
            &"https://bucket.s3.amazonaws.com/models/model.bin?tagging"
                .parse()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(request.operation, Operation::Other);
        assert_eq!(request.route, Route::Passthrough);
    }

    #[test]
    fn should_support_custom_exact_host_as_path_style() {
        let request = classify_request(
            &ProxyS3 {
                enable: true,
                detect_aws_endpoints: false,
                hosts: vec!["minio.example.com".to_string()],
                host_suffixes: Vec::new(),
            },
            &Method::GET,
            &"https://minio.example.com/test-bucket/models/model.bin"
                .parse()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(request.operation, Operation::GetObject);
        assert_eq!(request.route, Route::ViaDfdaemon);
    }

    #[test]
    fn should_support_custom_suffix_as_virtual_hosted() {
        let request = classify_request(
            &ProxyS3 {
                enable: true,
                detect_aws_endpoints: false,
                hosts: Vec::new(),
                host_suffixes: vec!["storage.example.com".to_string()],
            },
            &Method::GET,
            &"https://bucket.storage.example.com/models/model.bin"
                .parse()
                .unwrap(),
        )
        .unwrap();

        assert_eq!(request.operation, Operation::GetObject);
        assert_eq!(request.route, Route::ViaDfdaemon);
    }

    #[test]
    fn should_passthrough_bucket_root_requests() {
        let request = classify_request(
            &enabled_config(),
            &Method::GET,
            &"https://bucket.s3.amazonaws.com/".parse().unwrap(),
        )
        .unwrap();

        assert_eq!(request.operation, Operation::Other);
        assert_eq!(request.route, Route::Passthrough);
    }
}
