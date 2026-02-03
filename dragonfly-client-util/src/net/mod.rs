/*
 *     Copyright 2025 The Dragonfly Authors
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

use local_ip_address::{local_ip, local_ipv6};
use std::net::IpAddr;

#[cfg(target_os = "linux")]
use std::{io, mem, os::unix::io::RawFd};

/// Formats an IP address and port into a socket address string.
///
/// IPv4 addresses are formatted as `ip:port` (e.g., "192.168.1.1:8080")
/// IPv6 addresses are formatted as `[ip]:port` (e.g., "[::1]:8080")
pub fn format_socket_addr(ip: IpAddr, port: u16) -> String {
    match ip {
        IpAddr::V4(v4) => format!("{}:{}", v4, port),
        IpAddr::V6(v6) => format!("[{}]:{}", v6, port),
    }
}

/// Formats a complete URL with scheme, IP address, and port.
pub fn format_url(scheme: &str, ip: IpAddr, port: u16) -> String {
    format!("{}://{}", scheme, format_socket_addr(ip, port))
}

/// Get the local IP address of the machine.
///
/// Attempts to retrieve the local IPv4 address first. If unavailable or if the
/// operation fails, falls back to attempting IPv6 address retrieval.
pub fn preferred_local_ip() -> Option<IpAddr> {
    local_ip().ok().or_else(|| local_ipv6().ok())
}

/// set_tcp_fastopen_connect enables TCP Fast Open for client connections on the given socket file
/// descriptor.
#[cfg(target_os = "linux")]
pub fn set_tcp_fastopen_connect(fd: RawFd) -> io::Result<()> {
    let enable: libc::c_int = 1;

    unsafe {
        let ret = libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_FASTOPEN_CONNECT,
            &enable as *const _ as *const libc::c_void,
            mem::size_of_val(&enable) as libc::socklen_t,
        );

        if ret != 0 {
            let err = std::io::Error::last_os_error();
            return Err(err);
        }
    }

    Ok(())
}

/// set_tcp_fastopen enables TCP Fast Open for server connections on the given socket file
/// descriptor.
#[cfg(target_os = "linux")]
pub fn set_tcp_fastopen(fd: RawFd) -> io::Result<()> {
    let queue: libc::c_int = 1024;

    unsafe {
        let ret = libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_FASTOPEN,
            &queue as *const _ as *const libc::c_void,
            mem::size_of_val(&queue) as libc::socklen_t,
        );

        if ret != 0 {
            let err = std::io::Error::last_os_error();
            return Err(err);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    #[test]
    fn test_format_socket_addr_ipv4() {
        assert_eq!(
            format_socket_addr(IpAddr::from_str("127.0.0.1").unwrap(), 80),
            "127.0.0.1:80"
        );

        assert_eq!(
            format_socket_addr(IpAddr::from_str("192.168.1.1").unwrap(), 8080),
            "192.168.1.1:8080"
        );
    }

    #[test]
    fn test_format_socket_addr_ipv6() {
        assert_eq!(
            format_socket_addr(IpAddr::from_str("::1").unwrap(), 80),
            "[::1]:80"
        );

        assert_eq!(
            format_socket_addr(IpAddr::from_str("2001:db8::1").unwrap(), 8080),
            "[2001:db8::1]:8080"
        );
    }

    #[test]
    fn test_format_url_ipv4() {
        assert_eq!(
            format_url("http", IpAddr::from_str("127.0.0.1").unwrap(), 80),
            "http://127.0.0.1:80"
        );

        assert_eq!(
            format_url("https", IpAddr::from_str("192.168.1.1").unwrap(), 443),
            "https://192.168.1.1:443"
        );
    }

    #[test]
    fn test_format_url_ipv6() {
        assert_eq!(
            format_url("http", IpAddr::from_str("::1").unwrap(), 80),
            "http://[::1]:80"
        );
        assert_eq!(
            format_url("https", IpAddr::from_str("2001:db8::1").unwrap(), 443),
            "https://[2001:db8::1]:443"
        );
    }

    #[test]
    fn test_preferred_local_ip() {
        let ip = preferred_local_ip();
        assert!(ip.is_some());
    }
}
