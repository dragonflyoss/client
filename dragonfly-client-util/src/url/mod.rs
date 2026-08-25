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

use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Result,
};
use percent_encoding::percent_encode_byte;
use url::{form_urlencoded, Url};

/// Escapes the string so it can be safely placed inside a url query, identical
/// to the scheduler's query escaping (Go's url.QueryEscape). Mirrors the byte
/// serializer of form_urlencoded with the unreserved characters of Go.
fn query_escape(s: &str) -> String {
    let mut escaped = String::with_capacity(s.len());
    for &byte in s.as_bytes() {
        match byte {
            b'-' | b'.' | b'0'..=b'9' | b'A'..=b'Z' | b'_' | b'a'..=b'z' | b'~' => {
                escaped.push(byte as char)
            }
            b' ' => escaped.push('+'),
            _ => escaped.push_str(percent_encode_byte(byte)),
        }
    }

    escaped
}

/// Filters and sorts the query parameters by key to canonicalize the url,
/// identical to the scheduler. The values of the same key keep the original order.
pub fn filter_query_params(url: &str, filtered_query_params: &[String]) -> Result<String> {
    if filtered_query_params.is_empty() {
        return Ok(url.to_string());
    }

    let mut url = Url::parse(url).or_err(ErrorType::ParseError)?;
    let mut query_pairs: Vec<(String, String)> = url
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|segment| !segment.contains(';'))
        .flat_map(|segment| form_urlencoded::parse(segment.as_bytes()))
        .filter(|(k, _)| {
            !filtered_query_params
                .iter()
                .any(|param| param.as_str() == k.as_ref())
        })
        .map(|(k, v)| (k.into_owned(), v.into_owned()))
        .collect();
    query_pairs.sort_by(|(a, _), (b, _)| a.cmp(b));

    if query_pairs.is_empty() {
        url.set_query(None);
    } else {
        let query = query_pairs
            .iter()
            .map(|(k, v)| format!("{}={}", query_escape(k), query_escape(v)))
            .collect::<Vec<_>>()
            .join("&");
        url.set_query(Some(&query));
    }

    let filtered = url.to_string();
    if url.path() == "/" && filtered.ends_with('/') {
        return Ok(filtered.trim_end_matches('/').to_string());
    }

    Ok(filtered)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_escape_query() {
        assert_eq!(query_escape("a b"), "a+b");
        assert_eq!(query_escape("x*y"), "x%2Ay");
        assert_eq!(query_escape("c~d"), "c~d");
        assert_eq!(query_escape("1+1"), "1%2B1");
        assert_eq!(query_escape("中"), "%E4%B8%AD");
    }

    #[test]
    fn should_filter_query_params() {
        let test_cases = vec![
            (
                "https://example.com/file.txt?z=9&b=2&a=1",
                vec!["z".to_string()],
                "https://example.com/file.txt?a=1&b=2",
            ),
            (
                "https://example.com/file.txt?b=2&a=1&b=1",
                vec!["c".to_string()],
                "https://example.com/file.txt?a=1&b=2&b=1",
            ),
            (
                "https://example.com?foo=foo",
                vec!["foo".to_string()],
                "https://example.com",
            ),
            (
                "https://example.com/file.txt?k=a b&m=x*y&n=c~d",
                vec!["none".to_string()],
                "https://example.com/file.txt?k=a+b&m=x%2Ay&n=c~d",
            ),
            (
                "http://www.xx.yy/path?u=f&x=y&m=z&x=s#size",
                vec!["x".to_string(), "m".to_string()],
                "http://www.xx.yy/path?u=f#size",
            ),
            (
                "http://www.xx.yy/path?u=f&x=y&m=z&x=s#size",
                vec![],
                "http://www.xx.yy/path?u=f&x=y&m=z&x=s#size",
            ),
            (
                "https://example.com/file.txt?a=1;x&b=2",
                vec!["none".to_string()],
                "https://example.com/file.txt?b=2",
            ),
        ];

        for (url, filtered_query_params, expected) in test_cases {
            assert_eq!(
                filter_query_params(url, &filtered_query_params).unwrap(),
                expected
            );
        }

        assert!(filter_query_params(":error_url", &["x".to_string()]).is_err());
    }
}
