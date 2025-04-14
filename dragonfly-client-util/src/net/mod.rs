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

use bytesize::{ByteSize, MB};
use pnet::datalink::{self, NetworkInterface};
use std::cmp::min;
use std::net::IpAddr;

#[cfg(not(target_os = "linux"))]
use tracing::warn;

/// get_interface_by_ip returns the name of the network interface that has the specified IP
/// address.
pub fn get_interface_by_ip(ip: IpAddr) -> Option<NetworkInterface> {
    for interface in datalink::interfaces() {
        for ip_network in interface.ips.iter() {
            if ip_network.ip() == ip {
                return Some(interface);
            }
        }
    }

    None
}

/// get_interface_speed_by_ip returns the speed of the network interface that has the specified IP
/// address in Mbps.
pub fn get_interface_speed(interface_name: &str) -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let speed_path = format!("/sys/class/net/{}/speed", interface_name);
        std::fs::read_to_string(&speed_path)
            .ok()
            .and_then(|speed_str| speed_str.trim().parse::<u64>().ok())
    }

    #[cfg(not(target_os = "linux"))]
    {
        warn!(
            "can not get interface {} speed on non-linux platform",
            interface_name
        );
        None
    }
}

/// Interface represents a network interface with its information.
#[derive(Debug, Clone, Default)]
pub struct Interface {
    /// name is the name of the network interface.
    pub name: String,

    // bandwidth is the bandwidth of the network interface in Mbps.
    pub bandwidth: u64,
}

/// get_interface_info returns the network interface information for the specified IP address.
pub fn get_interface_info(ip: IpAddr, rate_limit: ByteSize) -> Option<Interface> {
    let rate_limit = rate_limit.as_u64() / MB * 8; // convert to Mbps

    let interface = get_interface_by_ip(ip)?;
    match get_interface_speed(&interface.name) {
        Some(speed) => Some(Interface {
            name: interface.name,
            bandwidth: min(speed, rate_limit),
        }),
        None => Some(Interface {
            name: interface.name,
            bandwidth: rate_limit,
        }),
    }
}
