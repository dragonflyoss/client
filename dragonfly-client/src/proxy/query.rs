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

/// Extracts the `ns` query parameter from the request URI.
/// Containerd appends `?ns=<registry>` when routing requests through a mirror
/// (e.g., `GET /v2/library/nginx/manifests/latest?ns=docker.io`).
pub fn get_ns_from_query(uri: &http::Uri) -> Option<String> {
    let query = uri.query()?;
    let (_, value) = url::form_urlencoded::parse(query.as_bytes()).find(|(key, _)| key == "ns")?;
    if value.contains("://") {
        Some(value.into_owned())
    } else {
        Some(format!("https://{value}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_ns_from_query_with_ns_param() {
        let uri: http::Uri = "/v2/library/nginx/manifests/latest?ns=docker.io"
            .parse()
            .unwrap();
        assert_eq!(
            get_ns_from_query(&uri),
            Some("https://docker.io".to_string())
        );
    }

    #[test]
    fn test_get_ns_from_query_with_scheme() {
        let uri: http::Uri = "/v2/library/nginx/manifests/latest?ns=https://registry.example.com"
            .parse()
            .unwrap();
        assert_eq!(
            get_ns_from_query(&uri),
            Some("https://registry.example.com".to_string())
        );
    }

    #[test]
    fn test_get_ns_from_query_no_ns_param() {
        let uri: http::Uri = "/v2/library/nginx/manifests/latest?foo=bar"
            .parse()
            .unwrap();
        assert_eq!(get_ns_from_query(&uri), None);
    }

    #[test]
    fn test_get_ns_from_query_no_query() {
        let uri: http::Uri = "/v2/library/nginx/manifests/latest".parse().unwrap();
        assert_eq!(get_ns_from_query(&uri), None);
    }

    #[test]
    fn test_get_ns_from_query_multiple_params() {
        let uri: http::Uri = "/v2/library/nginx/manifests/latest?foo=bar&ns=ghcr.io&baz=qux"
            .parse()
            .unwrap();
        assert_eq!(get_ns_from_query(&uri), Some("https://ghcr.io".to_string()));
    }
}
