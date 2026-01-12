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

/// join_host_port formats a host and port into `host:port`, adding brackets for IPv6 literals.
///
/// Examples:
/// - ("127.0.0.1", 80) -> "127.0.0.1:80"
/// - ("::1", 80) -> "[::1]:80"
pub fn join_host_port(host: &str, port: u16) -> String {
    if host.is_empty() {
        return format!(":{}", port);
    }

    // If already bracketed, don't double-bracket.
    if host.starts_with('[') && host.ends_with(']') {
        return format!("{}:{}", host, port);
    }

    if host.contains(':') {
        format!("[{}]:{}", host, port)
    } else {
        format!("{}:{}", host, port)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_host_port_ipv4() {
        assert_eq!(join_host_port("127.0.0.1", 80), "127.0.0.1:80");
        assert_eq!(join_host_port("192.168.1.1", 8080), "192.168.1.1:8080");
    }

    #[test]
    fn test_join_host_port_ipv6() {
        assert_eq!(join_host_port("::1", 80), "[::1]:80");
        assert_eq!(join_host_port("2001:db8::1", 8080), "[2001:db8::1]:8080");
    }

    #[test]
    fn test_join_host_port_already_bracketed() {
        assert_eq!(join_host_port("[::1]", 80), "[::1]:80");
    }

    #[test]
    fn test_join_host_port_empty() {
        assert_eq!(join_host_port("", 80), ":80");
    }

    #[test]
    fn test_join_host_port_hostname() {
        assert_eq!(join_host_port("example.com", 80), "example.com:80");
    }
}
