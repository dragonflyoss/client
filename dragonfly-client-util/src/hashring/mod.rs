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

use hashring::HashRing;
use std::fmt;
use std::hash::Hash;
use std::net::SocketAddr;

/// A virtual node (vnode) on the consistent hash ring.
/// Each physical node (SocketAddr) is represented by multiple vnodes to better
/// balance key distribution across the ring.
#[derive(Debug, Copy, Clone, Hash, PartialEq)]
pub struct VNode {
    /// The replica index of this vnode for its physical node (0..replica_count-1).
    id: usize,

    /// The physical node address this vnode represents.
    addr: SocketAddr,
}

/// VNode implements virtual node for consistent hashing.
impl VNode {
    /// Creates a new virtual node with the given replica id and physical address.
    fn new(id: usize, addr: SocketAddr) -> Self {
        VNode { id, addr }
    }
}

/// VNode implements Display trait to format.
impl fmt::Display for VNode {
    /// Formats the virtual node as "address|id" as the key for the hash ring.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}|{}", self.addr, self.id)
    }
}

/// VNode implements methods for hash ring operations.
impl VNode {
    /// Returns a reference to the physical node address associated with this vnode.
    pub fn addr(&self) -> &SocketAddr {
        &self.addr
    }
}

/// A consistent hash ring that uses virtual nodes (vnodes) to improve key distribution.
/// When a physical node is added, replica_count vnodes are inserted into the ring.
pub struct VNodeHashRing {
    /// Number of vnodes to create per physical node.
    replica_count: usize,

    /// The underlying hash ring that stores vnodes.
    ring: HashRing<VNode>,
}

/// VNodeHashRing implements methods for managing the hash ring.
impl VNodeHashRing {
    /// Creates a new vnode-based hash ring.
    pub fn new(replica_count: usize) -> Self {
        VNodeHashRing {
            replica_count,
            ring: HashRing::new(),
        }
    }

    /// Add `node` to the hash ring, it will add `virtual_nodes_count` virtual nodes
    /// to the ring for the given `node`.
    pub fn add(&mut self, addr: SocketAddr) {
        for id in 0..self.replica_count {
            let vnode = VNode::new(id, addr);
            self.ring.add(vnode);
        }
    }

    /// Get the node responsible for `key`. Returns an `Option` that will contain the `node`
    /// if the hash ring is not empty or `None` if it was empty.
    pub fn get<U: Hash>(&self, key: &U) -> Option<&VNode> {
        self.ring.get(key)
    }

    /// Get the node responsible for `key` along with the next `replica` nodes after.
    /// Returns None when the ring is empty. If `replicas` is larger than the length
    /// of the ring, this function will shrink to just contain the entire ring.
    pub fn get_with_replicas<U: Hash>(&self, key: &U, replicas: usize) -> Option<Vec<VNode>> {
        self.ring.get_with_replicas(key, replicas)
    }

    /// Get the number of nodes in the hash ring.
    pub fn len(&self) -> usize {
        self.ring.len()
    }

    /// Returns true if the ring has no elements.
    pub fn is_empty(&self) -> bool {
        self.ring.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_vnode_new() {
        let vnode = VNode::new(
            1,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        );
        assert_eq!(vnode.id, 1);
        assert_eq!(
            vnode.addr,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080)
        );
    }

    #[test]
    fn test_vnode_to_string() {
        let vnode = VNode::new(
            1,
            SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8080),
        );
        assert_eq!(vnode.to_string(), "127.0.0.1:8080|1");
    }

    #[test]
    fn test_hashring_new() {
        let ring = VNodeHashRing::new(3);
        assert_eq!(ring.replica_count, 3);
        assert!(ring.is_empty());
        assert_eq!(ring.len(), 0);
    }

    #[test]
    fn test_add_and_len() {
        let mut ring = VNodeHashRing::new(2);
        ring.add(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8080,
        ));
        assert_eq!(ring.len(), 2); // 1 node * 2 virtual nodes
        ring.add(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8081,
        ));
        assert_eq!(ring.len(), 4); // 2 nodes * 2 virtual nodes
        assert!(!ring.is_empty());
    }

    #[test]
    fn test_get_empty_ring() {
        let ring = VNodeHashRing::new(2);
        let key = "test_key";
        assert!(ring.get(&key).is_none());
    }

    #[test]
    fn test_get_with_nodes() {
        let mut ring = VNodeHashRing::new(2);
        ring.add(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8080,
        ));
        ring.add(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8081,
        ));

        let key = "test_key";
        let node = ring.get(&key);
        assert!(node.is_some());
        let node = node.unwrap();
        assert!(node.addr.port() == 8080 || node.addr.port() == 8081);
        assert!(node.id == 0 || node.id == 1);
    }

    #[test]
    fn test_get_with_replicas_empty() {
        let ring = VNodeHashRing::new(2);
        let key = "test_key";
        assert!(ring.get_with_replicas(&key, 2).is_none());
    }

    #[test]
    fn test_get_with_replicas() {
        let mut ring = VNodeHashRing::new(2);
        ring.add(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8080,
        ));
        ring.add(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8081,
        ));

        let key = "test_key";
        let replicas = ring.get_with_replicas(&key, 3).unwrap();
        assert_eq!(replicas.len(), 4);
        assert!(replicas.iter().all(|vnode| {
            (vnode.addr.port() == 8080 || vnode.addr.port() == 8081)
                && (vnode.id == 0 || vnode.id == 1)
        }));
    }

    #[test]
    fn test_get_with_replicas_exact_size() {
        let mut ring = VNodeHashRing::new(2);
        ring.add(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8080,
        ));
        ring.add(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8081,
        ));

        let key = "test_key";
        let replicas = ring.get_with_replicas(&key, 4).unwrap();
        assert_eq!(replicas.len(), 5);
    }

    #[test]
    fn test_get_with_replicas_smaller_size() {
        let mut ring = VNodeHashRing::new(2);
        ring.add(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8080,
        ));
        ring.add(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            8081,
        ));

        let key = "test_key";
        let replicas = ring.get_with_replicas(&key, 2).unwrap();
        assert_eq!(replicas.len(), 3);
        assert!(replicas.iter().all(|vnode| {
            (vnode.addr.port() == 8080 || vnode.addr.port() == 8081)
                && (vnode.id == 0 || vnode.id == 1)
        }));
    }
}
