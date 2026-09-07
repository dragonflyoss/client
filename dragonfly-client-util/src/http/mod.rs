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
    error::{BackendError, ErrorType, OrErr},
    Error, Result,
};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, CONTENT_RANGE};
use reqwest::StatusCode;
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

/// Validates that a ranged response satisfies the requested range, since the server may
/// ignore the Range header or transfer a range different from the requested one, which is
/// described by the Content-Range header, refer to RFC 9110 Section 15.3.7.
pub fn validate_ranged_response(
    range: Option<Range>,
    status_code: StatusCode,
    response_header: &HeaderMap,
) -> Result<()> {
    let Some(range) = range else {
        return Ok(());
    };

    if !status_code.is_success() {
        return Ok(());
    }

    let err = |message: String| {
        Error::BackendError(Box::new(BackendError {
            message,
            status_code: Some(status_code),
            header: Some(response_header.clone()),
        }))
    };

    let expected_end = range.start + range.length - 1;
    if status_code != StatusCode::PARTIAL_CONTENT {
        if range.start == 0 {
            return Ok(());
        }

        return Err(err(format!(
            "expected 206 Partial Content for range bytes={}-{}, got {}",
            range.start, expected_end, status_code
        )));
    }

    let content_range = response_header
        .get(CONTENT_RANGE)
        .and_then(|content_range| content_range.to_str().ok())
        .ok_or_else(|| {
            err(format!(
                "missing Content-Range for range bytes={}-{}",
                range.start, expected_end
            ))
        })?;

    // The Content-Range is formatted as "bytes <start>-<end>/<total>".
    let (actual_start, actual_end) = content_range
        .strip_prefix("bytes ")
        .and_then(|content_range| content_range.split_once('/'))
        .and_then(|(bytes_range, _)| bytes_range.split_once('-'))
        .and_then(|(start, end)| Some((start.parse::<u64>().ok()?, end.parse::<u64>().ok()?)))
        .ok_or_else(|| err(format!("invalid Content-Range {content_range}")))?;

    if actual_start != range.start || actual_end != expected_end {
        return Err(err(format!(
            "Content-Range {} mismatches requested range bytes={}-{}",
            content_range, range.start, expected_end
        )));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::header::RANGE;

    type ExpectHeaderMap = fn(Result<HeaderMap>);
    type ExpectRange = fn(Result<Option<Range>>);
    type ExpectParsedRange = fn(Result<Range>);
    type ExpectValidated = fn(Result<()>);

    fn header(name: HeaderName, value: Option<&str>) -> HeaderMap {
        let mut header = HeaderMap::new();
        if let Some(value) = value {
            header.insert(name, HeaderValue::from_str(value).unwrap());
        }
        header
    }

    fn range(start: u64, length: u64) -> Option<Range> {
        Some(Range { start, length })
    }

    fn expect_ok(result: Result<()>) {
        assert!(result.is_ok());
    }

    fn expect_backend_error(result: Result<()>) {
        assert!(matches!(result, Err(Error::BackendError(_))));
    }

    #[test]
    fn headermap_to_hashmap_keeps_visible_ascii_values() {
        let mut header = HeaderMap::new();
        header.insert("Content-Type", HeaderValue::from_static("application/json"));
        header.insert("Authorization", HeaderValue::from_static("Bearer token"));
        header.insert("X-Binary", HeaderValue::from_bytes(b"\xff").unwrap());

        let hashmap = headermap_to_hashmap(&header);
        assert_eq!(hashmap.len(), 2);
        assert_eq!(hashmap.get("content-type").unwrap(), "application/json");
        assert_eq!(hashmap.get("authorization").unwrap(), "Bearer token");
    }

    #[test]
    fn hashmap_to_headermap_rejects_invalid_names_and_values() {
        let test_cases: Vec<(Vec<(&str, &str)>, ExpectHeaderMap)> = vec![
            (
                vec![
                    ("Content-Type", "application/json"),
                    ("Authorization", "Bearer token"),
                ],
                |header| {
                    let header = header.unwrap();
                    assert_eq!(header.get("Content-Type").unwrap(), "application/json");
                    assert_eq!(header.get("Authorization").unwrap(), "Bearer token");
                },
            ),
            (vec![("Content Type", "application/json")], |header| {
                assert!(matches!(header, Err(Error::ExternalError(_))))
            }),
            (vec![("Content-Type", "application/\njson")], |header| {
                assert!(matches!(header, Err(Error::ExternalError(_))))
            }),
        ];

        for (header_pairs, expect) in test_cases {
            let hashmap: HashMap<String, String> = header_pairs
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect();
            expect(hashmap_to_headermap(&hashmap));
        }
    }

    #[test]
    fn header_vec_to_hashmap_splits_on_the_first_colon_and_trims() {
        let test_cases = vec![
            (
                vec![
                    "Content-Type: application/json",
                    "Authorization: Bearer token",
                ],
                vec![
                    ("Content-Type", "application/json"),
                    ("Authorization", "Bearer token"),
                ],
            ),
            (
                vec!["Host:localhost:8080"],
                vec![("Host", "localhost:8080")],
            ),
            (
                vec!["  Accept :  text/plain  "],
                vec![("Accept", "text/plain")],
            ),
            (vec!["invalid header", ""], vec![]),
        ];

        for (raw_header, expected) in test_cases {
            let hashmap =
                header_vec_to_hashmap(raw_header.iter().map(|header| header.to_string()).collect())
                    .unwrap();
            let expected: HashMap<String, String> = expected
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect();
            assert_eq!(hashmap, expected);
        }
    }

    #[test]
    fn header_vec_to_headermap_parses_raw_headers() {
        let raw_header = vec![
            "Content-Type: application/json".to_string(),
            "Authorization: Bearer token".to_string(),
        ];

        let header = header_vec_to_headermap(raw_header).unwrap();
        assert_eq!(header.get("Content-Type").unwrap(), "application/json");
        assert_eq!(header.get("Authorization").unwrap(), "Bearer token");
    }

    #[test]
    fn get_range_parses_the_range_header_when_present() {
        let test_cases: Vec<(Option<&str>, ExpectRange)> = vec![
            (None, |result| assert_eq!(result.unwrap(), None)),
            (Some("bytes=0-100"), |result| {
                assert_eq!(result.unwrap(), range(0, 101))
            }),
            (Some("invalid"), |result| {
                assert!(matches!(result, Err(Error::ExternalError(_))))
            }),
        ];

        for (range_header, expect) in test_cases {
            expect(get_range(&header(RANGE, range_header), 200));
        }
    }

    #[test]
    fn parse_range_header_returns_the_first_satisfiable_range() {
        let test_cases: Vec<(&str, ExpectParsedRange)> = vec![
            ("bytes=0-100", |result| {
                assert_eq!(
                    result.unwrap(),
                    Range {
                        start: 0,
                        length: 101
                    }
                )
            }),
            ("bytes=-50", |result| {
                assert_eq!(
                    result.unwrap(),
                    Range {
                        start: 150,
                        length: 50
                    }
                )
            }),
            ("bytes=150-", |result| {
                assert_eq!(
                    result.unwrap(),
                    Range {
                        start: 150,
                        length: 50
                    }
                )
            }),
            ("bytes=0-0,-1", |result| {
                assert_eq!(
                    result.unwrap(),
                    Range {
                        start: 0,
                        length: 1
                    }
                )
            }),
            ("bytes=100-300", |result| {
                assert_eq!(
                    result.unwrap(),
                    Range {
                        start: 100,
                        length: 100
                    }
                )
            }),
            ("bytes=200-", |result| {
                assert!(matches!(result, Err(Error::ExternalError(_))))
            }),
            ("invalid", |result| {
                assert!(matches!(result, Err(Error::ExternalError(_))))
            }),
        ];

        for (range_header, expect) in test_cases {
            expect(parse_range_header(range_header, 200));
        }
    }

    #[test]
    fn validate_ranged_response_requires_a_matching_content_range() {
        let test_cases: Vec<(Option<Range>, StatusCode, Option<&str>, ExpectValidated)> = vec![
            (None, StatusCode::OK, None, expect_ok),
            (range(10, 20), StatusCode::NOT_FOUND, None, expect_ok),
            (range(0, 20), StatusCode::OK, None, expect_ok),
            (range(10, 20), StatusCode::OK, None, expect_backend_error),
            (
                range(10, 20),
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 10-29/100"),
                expect_ok,
            ),
            (
                range(10, 20),
                StatusCode::PARTIAL_CONTENT,
                None,
                expect_backend_error,
            ),
            (
                range(10, 20),
                StatusCode::PARTIAL_CONTENT,
                Some("bytes */100"),
                expect_backend_error,
            ),
            (
                range(10, 20),
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 10-/100"),
                expect_backend_error,
            ),
            (
                range(10, 20),
                StatusCode::PARTIAL_CONTENT,
                Some("10-29/100"),
                expect_backend_error,
            ),
            (
                range(10, 20),
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 10-29"),
                expect_backend_error,
            ),
            (
                range(10, 20),
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 0-29/100"),
                expect_backend_error,
            ),
            (
                range(10, 20),
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 10-30/100"),
                expect_backend_error,
            ),
            (
                range(10, 20),
                StatusCode::PARTIAL_CONTENT,
                Some("bytes 0-99/100"),
                expect_backend_error,
            ),
        ];

        for (range, status_code, content_range, expect) in test_cases {
            expect(validate_ranged_response(
                range,
                status_code,
                &header(CONTENT_RANGE, content_range),
            ));
        }
    }
}
