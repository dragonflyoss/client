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
    fn get_ns_from_query_extracts_the_registry_with_a_scheme() {
        let test_cases = vec![
            (
                "/v2/library/nginx/manifests/latest?ns=docker.io",
                Some("https://docker.io"),
            ),
            (
                "/v2/library/nginx/manifests/latest?ns=https://registry.example.com",
                Some("https://registry.example.com"),
            ),
            (
                "/v2/library/nginx/manifests/latest?ns=registry.example.com%3A5000",
                Some("https://registry.example.com:5000"),
            ),
            (
                "/v2/library/nginx/manifests/latest?foo=bar&ns=ghcr.io&baz=qux",
                Some("https://ghcr.io"),
            ),
            ("/v2/library/nginx/manifests/latest?foo=bar", None),
            ("/v2/library/nginx/manifests/latest", None),
        ];

        for (uri, expected) in test_cases {
            let uri: http::Uri = uri.parse().unwrap();
            assert_eq!(get_ns_from_query(&uri), expected.map(str::to_string));
        }
    }
}
