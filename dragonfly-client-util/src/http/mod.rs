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
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_RANGE, RANGE};
use std::collections::{HashMap, HashSet};

pub mod basic_auth;
pub mod query_params;

/// A parsed HTTP Content-Range header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContentRange {
    /// The returned byte range.
    pub range: Range,

    /// The complete length of the selected representation.
    pub complete_length: u64,
}

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

/// Headers that select which bytes the origin returns for a ranged GET, and so
/// have to separate one cached range from another.
///
/// This is an allow list rather than a deny list. Keying on every signed header
/// would fold per-request values into the cache key: the AWS SDK for Java and the
/// Hadoop S3A connector sign `amz-sdk-invocation-id`, which is a fresh UUID for
/// every call, so such a key would never be reused and every request would store
/// its own copy of the same bytes.
const REPRESENTATION_HEADER_NAMES: &[&str] = &[
    "accept-encoding",
    "if-match",
    "if-modified-since",
    "if-none-match",
    "if-range",
    "if-unmodified-since",
    "x-amz-checksum-mode",
    "x-amz-server-side-encryption-customer-algorithm",
    "x-amz-server-side-encryption-customer-key",
    "x-amz-server-side-encryption-customer-key-md5",
];

/// Returns a stable cache-key component for a signature-bound range request.
///
/// Rotating authentication material is intentionally excluded so independently
/// authorized callers can share the same cached bytes. Only the range itself and
/// the headers that change the selected representation take part in the key.
pub fn signature_bound_range_cache_key_from_hashmap(
    header: &HashMap<String, String>,
    url: &str,
) -> Option<String> {
    let range = signature_bound_range_from_hashmap(header, url)?;
    let selected_names: HashSet<&str> = REPRESENTATION_HEADER_NAMES.iter().copied().collect();

    let mut selected_headers: Vec<(String, &str)> = header
        .iter()
        .filter_map(|(name, value)| {
            let name = name.to_ascii_lowercase();
            selected_names
                .contains(name.as_str())
                .then_some((name, value.trim()))
        })
        .collect();
    selected_headers.sort_unstable();

    let mut key = range.to_string();
    for (name, value) in selected_headers {
        key.push('\0');
        key.push_str(&name);
        key.push(':');
        key.push_str(value);
    }
    Some(key)
}

/// Returns whether a Range header value asks for exactly one byte range.
///
/// RFC 7233 allows a comma-separated set of ranges. A signature-bound Range
/// cannot be narrowed to the first entry without invalidating the signature, and
/// dfdaemon has no way to return a `multipart/byteranges` body, so the multi-range
/// form has to be rejected instead of partially honoured.
pub fn is_single_byte_range(range_header_value: &str) -> bool {
    match range_header_value.split_once('=') {
        Some((unit, spec)) => unit.trim().eq_ignore_ascii_case("bytes") && !spec.contains(','),
        None => false,
    }
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

/// Parses a Content-Range header in the form `bytes START-END/TOTAL`.
pub fn parse_content_range_header(content_range: &str) -> Result<ContentRange> {
    let (unit, value) = content_range
        .trim()
        .split_once(' ')
        .ok_or(Error::InvalidParameter)?;
    if !unit.eq_ignore_ascii_case("bytes") {
        return Err(Error::InvalidParameter);
    }

    let (range, complete_length) = value.split_once('/').ok_or(Error::InvalidParameter)?;
    let complete_length = complete_length
        .parse::<u64>()
        .or_err(ErrorType::ParseError)?;
    let (start, end) = range.split_once('-').ok_or(Error::InvalidParameter)?;
    let start = start.parse::<u64>().or_err(ErrorType::ParseError)?;
    let end = end.parse::<u64>().or_err(ErrorType::ParseError)?;

    if start > end || end >= complete_length {
        return Err(Error::InvalidParameter);
    }

    let length = end
        .checked_sub(start)
        .and_then(|length| length.checked_add(1))
        .ok_or(Error::InvalidParameter)?;

    Ok(ContentRange {
        range: Range { start, length },
        complete_length,
    })
}

/// Gets and parses Content-Range from a header map.
pub fn get_content_range(header: &HeaderMap) -> Result<Option<ContentRange>> {
    header
        .get(CONTENT_RANGE)
        .map(|value| {
            let value = value.to_str().or_err(ErrorType::ParseError)?;
            parse_content_range_header(value)
        })
        .transpose()
}

/// Gets and parses Content-Range from a string header map.
pub fn get_content_range_from_hashmap(
    header: &HashMap<String, String>,
) -> Result<Option<ContentRange>> {
    find_header(header, CONTENT_RANGE.as_str())
        .map(parse_content_range_header)
        .transpose()
}

/// Parses a Range header string as per RFC 7233,
/// supported Range Header: "Range": "bytes=100-200", "Range": "bytes=-50",
/// "Range": "bytes=150-".
pub fn parse_range_header(range_header_value: &str, content_length: u64) -> Result<Range> {
    let parsed_ranges =
        http_range_header::parse_range_header(range_header_value).or_err(ErrorType::ParseError)?;
    let valid_ranges = parsed_ranges
        .validate(content_length)
        .or_err(ErrorType::ParseError)?;

    if valid_ranges.len() != 1 {
        return Err(Error::InvalidParameter);
    }

    let valid_range = valid_ranges.first().ok_or(Error::EmptyHTTPRangeError)?;

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

        assert!(parse_range_header("bytes=0-0,-1", 200).is_err());
    }

    #[test]
    fn test_parse_content_range_header() {
        assert_eq!(
            parse_content_range_header("bytes 100-199/1000").unwrap(),
            ContentRange {
                range: Range {
                    start: 100,
                    length: 100,
                },
                complete_length: 1000,
            }
        );
        assert_eq!(
            parse_content_range_header("BYTES 0-0/1").unwrap(),
            ContentRange {
                range: Range {
                    start: 0,
                    length: 1,
                },
                complete_length: 1,
            }
        );

        for invalid in [
            "bytes */1000",
            "bytes 100-99/1000",
            "bytes 100-100/100",
            "items 0-0/1",
            "bytes 0-0/*",
            "bytes 0/1",
        ] {
            assert!(
                parse_content_range_header(invalid).is_err(),
                "{invalid} should be rejected"
            );
        }
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
    fn test_signature_bound_range_cache_key() {
        let mut header = HashMap::from([
            ("range".to_string(), "bytes=100-199".to_string()),
            (
                "authorization".to_string(),
                "AWS4-HMAC-SHA256 Credential=first/scope, SignedHeaders=host;range;if-match;x-amz-date;x-amz-content-sha256, Signature=first".to_string(),
            ),
            ("if-match".to_string(), "\"version-1\"".to_string()),
            ("x-amz-date".to_string(), "20260724T000000Z".to_string()),
            (
                "x-amz-content-sha256".to_string(),
                "UNSIGNED-PAYLOAD".to_string(),
            ),
        ]);
        let first =
            signature_bound_range_cache_key_from_hashmap(&header, "https://example.com/object")
                .unwrap();

        header.insert(
            "authorization".to_string(),
            "AWS4-HMAC-SHA256 Credential=second/scope, SignedHeaders=host;range;if-match;x-amz-date;x-amz-content-sha256, Signature=second".to_string(),
        );
        header.insert("x-amz-date".to_string(), "20260724T010000Z".to_string());
        assert_eq!(
            first,
            signature_bound_range_cache_key_from_hashmap(&header, "https://example.com/object")
                .unwrap()
        );

        header.insert("if-match".to_string(), "\"version-2\"".to_string());
        assert_ne!(
            first,
            signature_bound_range_cache_key_from_hashmap(&header, "https://example.com/object")
                .unwrap()
        );
    }

    #[test]
    fn test_signature_bound_range_cache_key_includes_signed_sse_header() {
        let mut header = HashMap::from([
            ("range".to_string(), "bytes=100-199".to_string()),
            (
                "authorization".to_string(),
                "AWS4-HMAC-SHA256 Credential=key/scope, SignedHeaders=host;range;x-amz-server-side-encryption-customer-key, Signature=abc".to_string(),
            ),
            (
                "x-amz-server-side-encryption-customer-key".to_string(),
                "first-key".to_string(),
            ),
        ]);
        let first =
            signature_bound_range_cache_key_from_hashmap(&header, "https://example.com/object")
                .unwrap();
        header.insert(
            "x-amz-server-side-encryption-customer-key".to_string(),
            "second-key".to_string(),
        );
        assert_ne!(
            first,
            signature_bound_range_cache_key_from_hashmap(&header, "https://example.com/object")
                .unwrap()
        );
    }

    #[test]
    fn test_signature_bound_range_cache_key_ignores_per_request_sdk_headers() {
        // The AWS SDK for Java and the Hadoop S3A connector sign these, and
        // amz-sdk-invocation-id is a fresh UUID for every call.
        let mut header = HashMap::from([
            ("range".to_string(), "bytes=100-199".to_string()),
            (
                "authorization".to_string(),
                "AWS4-HMAC-SHA256 Credential=key/scope, SignedHeaders=amz-sdk-invocation-id;amz-sdk-request;host;range;x-amz-content-sha256;x-amz-date, Signature=abc".to_string(),
            ),
            (
                "amz-sdk-invocation-id".to_string(),
                "3d6a2f80-1f2e-4f0b-9d0f-2a0f6b1d0c11".to_string(),
            ),
            (
                "amz-sdk-request".to_string(),
                "attempt=1; max=3".to_string(),
            ),
            ("user-agent".to_string(), "aws-sdk-java/2.25.0".to_string()),
        ]);
        let first =
            signature_bound_range_cache_key_from_hashmap(&header, "https://example.com/object")
                .unwrap();

        header.insert(
            "amz-sdk-invocation-id".to_string(),
            "8b1c4e55-6d2a-4f77-9e33-5c0b7a2d9f42".to_string(),
        );
        header.insert(
            "amz-sdk-request".to_string(),
            "attempt=2; max=3".to_string(),
        );
        assert_eq!(
            first,
            signature_bound_range_cache_key_from_hashmap(&header, "https://example.com/object")
                .unwrap()
        );
    }

    #[test]
    fn test_is_single_byte_range() {
        assert!(is_single_byte_range("bytes=0-99"));
        assert!(is_single_byte_range("bytes=-50"));
        assert!(is_single_byte_range("bytes=150-"));
        assert!(is_single_byte_range("BYTES = 0-99"));

        assert!(!is_single_byte_range("bytes=0-99,200-299"));
        assert!(!is_single_byte_range("bytes=0-0,-1"));
        assert!(!is_single_byte_range("items=0-99"));
        assert!(!is_single_byte_range("0-99"));
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
