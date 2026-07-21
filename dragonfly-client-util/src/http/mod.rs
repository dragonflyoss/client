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

use dragonfly_api::common::v2::Range;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, RANGE};
use std::collections::HashMap;

pub mod basic_auth;
pub mod query_params;

/// Converts a headermap to a hashmap.
pub fn headermap_to_hashmap(header: &HeaderMap<HeaderValue>) -> HashMap<String, String> {
    let mut hashmap: HashMap<String, String> = HashMap::with_capacity(header.len());
    for (k, v) in header {
        if let Ok(v) = v.to_str() {
            hashmap.insert(k.to_string(), v.to_string());
        }
    }

    hashmap
}

/// Converts a hashmap to a headermap.
pub fn hashmap_to_headermap(header: &HashMap<String, String>) -> Result<HeaderMap<HeaderValue>> {
    let mut headermap = HeaderMap::with_capacity(header.len());
    for (k, v) in header {
        let name = HeaderName::from_bytes(k.as_bytes()).or_err(ErrorType::ParseError)?;
        let value = HeaderValue::from_bytes(v.as_bytes()).or_err(ErrorType::ParseError)?;
        headermap.insert(name, value);
    }

    Ok(headermap)
}

/// Converts a vector of header string to a hashmap.
pub fn header_vec_to_hashmap(raw_header: Vec<String>) -> Result<HashMap<String, String>> {
    let mut header = HashMap::with_capacity(raw_header.len());
    for h in raw_header {
        if let Some((k, v)) = h.split_once(':') {
            header.insert(k.trim().to_string(), v.trim().to_string());
        }
    }

    Ok(header)
}

/// Converts a vector of header string to a reqwest headermap.
pub fn header_vec_to_headermap(raw_header: Vec<String>) -> Result<HeaderMap> {
    hashmap_to_headermap(&header_vec_to_hashmap(raw_header)?)
}

/// Returns the Range header when it is covered by an AWS Signature Version 4
/// Authorization header or presigned URL.
///
/// A signature-bound Range must be forwarded unchanged. Replacing it with a
/// piece range invalidates the request signature.
pub fn signature_bound_range<'a>(header: &'a HeaderMap, url: &str) -> Option<&'a str> {
    let range = header.get(RANGE)?.to_str().ok()?;
    let authorization = header
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());

    has_signature_bound_range(authorization, url).then_some(range)
}

/// Returns the Range header from a string map when it is covered by an AWS
/// Signature Version 4 Authorization header or presigned URL.
pub fn signature_bound_range_from_hashmap<'a>(
    header: &'a HashMap<String, String>,
    url: &str,
) -> Option<&'a str> {
    let range = find_header(header, RANGE.as_str())?;
    let authorization = find_header(header, AUTHORIZATION.as_str());

    has_signature_bound_range(authorization, url).then_some(range)
}

fn find_header<'a>(header: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    header
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn has_signature_bound_range(authorization: Option<&str>, url: &str) -> bool {
    authorization.is_some_and(is_range_signed_in_authorization)
        || is_range_signed_in_presigned_url(url)
}

fn is_range_signed_in_authorization(authorization: &str) -> bool {
    let Some((algorithm, parameters)) = authorization.split_once(' ') else {
        return false;
    };
    if !is_aws_v4_algorithm(algorithm) {
        return false;
    }

    let mut has_credential = false;
    let mut has_signature = false;
    let mut range_is_signed = false;
    for parameter in parameters.split(',') {
        let Some((name, value)) = parameter.trim().split_once('=') else {
            continue;
        };

        if name.eq_ignore_ascii_case("Credential") {
            has_credential = !value.is_empty();
        } else if name.eq_ignore_ascii_case("Signature") {
            has_signature = !value.is_empty();
        } else if name.eq_ignore_ascii_case("SignedHeaders") {
            range_is_signed = signed_headers_contain_range(value);
        }
    }

    has_credential && has_signature && range_is_signed
}

fn is_range_signed_in_presigned_url(url: &str) -> bool {
    let Ok(url) = url::Url::parse(url) else {
        return false;
    };

    let mut has_algorithm = false;
    let mut has_signature = false;
    let mut range_is_signed = false;
    for (name, value) in url.query_pairs() {
        if name.eq_ignore_ascii_case("X-Amz-Algorithm") {
            has_algorithm = is_aws_v4_algorithm(&value);
        } else if name.eq_ignore_ascii_case("X-Amz-Signature") {
            has_signature = !value.is_empty();
        } else if name.eq_ignore_ascii_case("X-Amz-SignedHeaders") {
            range_is_signed = signed_headers_contain_range(&value);
        }
    }

    has_algorithm && has_signature && range_is_signed
}

fn is_aws_v4_algorithm(algorithm: &str) -> bool {
    matches!(algorithm, "AWS4-HMAC-SHA256" | "AWS4-ECDSA-P256-SHA256")
}

fn signed_headers_contain_range(signed_headers: &str) -> bool {
    signed_headers
        .split(';')
        .any(|header| header.trim().eq_ignore_ascii_case(RANGE.as_str()))
}

/// Gets the range from http header.
pub fn get_range(header: &HeaderMap, content_length: u64) -> Result<Option<Range>> {
    match header.get(reqwest::header::RANGE) {
        Some(range) => {
            let range = range.to_str().or_err(ErrorType::ParseError)?;
            Ok(Some(parse_range_header(range, content_length)?))
        }
        None => Ok(None),
    }
}

/// Parses a Range header string as per RFC 7233,
/// supported Range Header: "Range": "bytes=100-200", "Range": "bytes=-50",
/// "Range": "bytes=150-", "Range": "bytes=0-0,-1".
pub fn parse_range_header(range_header_value: &str, content_length: u64) -> Result<Range> {
    let parsed_ranges =
        http_range_header::parse_range_header(range_header_value).or_err(ErrorType::ParseError)?;
    let valid_ranges = parsed_ranges
        .validate(content_length)
        .or_err(ErrorType::ParseError)?;

    // Not support multiple ranges.
    let valid_range = valid_ranges
        .first()
        .ok_or_else(|| Error::EmptyHTTPRangeError)?;

    let start = valid_range.start().to_owned();
    let length = valid_range.end() - start + 1;
    Ok(Range { start, length })
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::{HeaderMap, HeaderValue};

    #[test]
    fn test_headermap_to_hashmap() {
        let mut header = HeaderMap::new();
        header.insert("Content-Type", HeaderValue::from_static("application/json"));
        header.insert("Authorization", HeaderValue::from_static("Bearer token"));

        let hashmap = headermap_to_hashmap(&header);
        assert_eq!(hashmap.get("content-type").unwrap(), "application/json");
        assert_eq!(hashmap.get("authorization").unwrap(), "Bearer token");
        assert_eq!(hashmap.get("foo"), None);
    }

    #[test]
    fn test_hashmap_to_headermap() {
        let mut hashmap = HashMap::new();
        hashmap.insert("Content-Type".to_string(), "application/json".to_string());
        hashmap.insert("Authorization".to_string(), "Bearer token".to_string());

        let header = hashmap_to_headermap(&hashmap).unwrap();
        assert_eq!(header.get("Content-Type").unwrap(), "application/json");
        assert_eq!(header.get("Authorization").unwrap(), "Bearer token");
    }

    #[test]
    fn test_header_vec_to_hashmap() {
        let raw_header = vec![
            "Content-Type: application/json".to_string(),
            "Authorization: Bearer token".to_string(),
        ];

        let hashmap = header_vec_to_hashmap(raw_header).unwrap();
        assert_eq!(hashmap.get("Content-Type").unwrap(), "application/json");
        assert_eq!(hashmap.get("Authorization").unwrap(), "Bearer token");
    }

    #[test]
    fn test_header_vec_to_headermap() {
        let raw_header = vec![
            "Content-Type: application/json".to_string(),
            "Authorization: Bearer token".to_string(),
        ];

        let header = header_vec_to_headermap(raw_header).unwrap();
        assert_eq!(header.get("Content-Type").unwrap(), "application/json");
        assert_eq!(header.get("Authorization").unwrap(), "Bearer token");
    }

    #[test]
    fn test_get_range() {
        let mut header = HeaderMap::new();
        header.insert(
            reqwest::header::RANGE,
            HeaderValue::from_static("bytes=0-100"),
        );

        let range = get_range(&header, 200).unwrap().unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.length, 101);
    }

    #[test]
    fn test_parse_range_header() {
        let range = parse_range_header("bytes=0-100", 200).unwrap();
        assert_eq!(range.start, 0);
        assert_eq!(range.length, 101);
    }

    #[test]
    fn test_signature_bound_range_with_authorization_header() {
        let mut header = HeaderMap::new();
        header.insert(RANGE, HeaderValue::from_static("bytes=100-199"));
        header.insert(
            AUTHORIZATION,
            HeaderValue::from_static(
                "AWS4-HMAC-SHA256 Credential=key/scope, SignedHeaders=host;range;x-amz-date, Signature=abc",
            ),
        );

        assert_eq!(
            signature_bound_range(&header, "https://example.com/object"),
            Some("bytes=100-199")
        );
    }

    #[test]
    fn test_signature_bound_range_with_sigv4a_authorization_header() {
        let mut header = HeaderMap::new();
        header.insert(RANGE, HeaderValue::from_static("bytes=100-199"));
        header.insert(
            AUTHORIZATION,
            HeaderValue::from_static(
                "AWS4-ECDSA-P256-SHA256 Credential=key/scope, SignedHeaders=host;range;x-amz-date, Signature=abc",
            ),
        );

        assert_eq!(
            signature_bound_range(&header, "https://example.com/object"),
            Some("bytes=100-199")
        );
    }

    #[test]
    fn test_signature_bound_range_with_presigned_url() {
        let mut header = HeaderMap::new();
        header.insert(RANGE, HeaderValue::from_static("bytes=100-199"));
        let url = "https://example.com/object?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-SignedHeaders=host%3Brange&X-Amz-Signature=abc";

        assert_eq!(signature_bound_range(&header, url), Some("bytes=100-199"));
    }

    #[test]
    fn test_signature_bound_range_rejects_incomplete_signatures() {
        let mut header = HeaderMap::new();
        header.insert(RANGE, HeaderValue::from_static("bytes=100-199"));
        header.insert(
            AUTHORIZATION,
            HeaderValue::from_static("Bearer SignedHeaders=host;range"),
        );

        assert_eq!(
            signature_bound_range(
                &header,
                "https://example.com/object?X-Amz-SignedHeaders=host%3Brange"
            ),
            None
        );
    }

    #[test]
    fn test_signature_bound_range_requires_range_in_signed_headers() {
        let mut header = HeaderMap::new();
        header.insert(RANGE, HeaderValue::from_static("bytes=100-199"));
        header.insert(
            AUTHORIZATION,
            HeaderValue::from_static(
                "AWS4-HMAC-SHA256 Credential=key/scope, SignedHeaders=host;x-amz-date, Signature=abc",
            ),
        );

        assert_eq!(
            signature_bound_range(&header, "https://example.com/object"),
            None
        );
    }

    #[test]
    fn test_signature_bound_range_from_hashmap_is_case_insensitive() {
        let header = HashMap::from([
            ("Range".to_string(), "bytes=100-199".to_string()),
            (
                "Authorization".to_string(),
                "AWS4-HMAC-SHA256 Credential=key/scope, SignedHeaders=host;range, Signature=abc"
                    .to_string(),
            ),
        ]);

        assert_eq!(
            signature_bound_range_from_hashmap(&header, "https://example.com/object"),
            Some("bytes=100-199")
        );
    }
}
