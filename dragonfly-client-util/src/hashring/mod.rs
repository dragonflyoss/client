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

/// A virtual node (vnode) on the consistent hash ring.
/// Each physical node (String) is represented by multiple vnodes to better
/// balance key distribution across the ring.
#[derive(Debug, Clone, Hash, PartialEq)]
pub struct VNode {
    /// The replica index of this vnode for its physical node (0..replica_count-1).
    id: usize,

    /// The physical node name this vnode represents.
    name: String,
}

/// Implements virtual node for consistent hashing.
impl VNode {
    /// Creates a new virtual node with the given replica id and physical name.
    fn new(id: usize, name: String) -> Self {
        VNode { id, name }
    }
}

/// Implements Display trait to format.
impl fmt::Display for VNode {
    /// Formats the virtual node as "name|id" as the key for the hash ring.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}|{}", self.name, self.id)
    }
}

/// Implements methods for hash ring operations.
impl VNode {
    /// Returns a reference to the physical node name associated with this vnode.
    pub fn name(&self) -> &str {
        &self.name
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

/// Implements methods for managing the hash ring.
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
    pub fn add(&mut self, name: String) {
        for id in 0..self.replica_count {
            let vnode = VNode::new(id, name.clone());
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
    #![allow(clippy::type_complexity)]

    use super::*;
    use uuid::Uuid;

    #[test]
    fn vnode_formats_as_name_and_id() {
        let vnode = VNode::new(1, "default-pod-1".to_string());
        assert_eq!(vnode.id, 1);
        assert_eq!(vnode.name(), "default-pod-1");
        assert_eq!(vnode.to_string(), "default-pod-1|1");
    }

    #[test]
    fn add_inserts_replica_count_vnodes_per_node() {
        let test_cases = vec![
            (3, vec![], 0),
            (2, vec!["default-pod-1"], 2),
            (2, vec!["default-pod-1", "default-pod-2"], 4),
            (0, vec!["default-pod-1"], 0),
        ];

        for (replica_count, names, expected_len) in test_cases {
            let mut ring = VNodeHashRing::new(replica_count);
            for name in names {
                ring.add(name.to_string());
            }

            assert_eq!(ring.replica_count, replica_count);
            assert_eq!(ring.len(), expected_len);
            assert_eq!(ring.is_empty(), expected_len == 0);
        }
    }

    #[test]
    fn get_returns_a_vnode_of_an_added_node() {
        let test_cases: Vec<(Vec<&str>, fn(Option<&VNode>))> = vec![
            (vec![], |vnode| assert!(vnode.is_none())),
            (vec!["default-pod-1", "default-pod-2"], |vnode| {
                let vnode = vnode.unwrap();
                assert!(["default-pod-1", "default-pod-2"].contains(&vnode.name()) && vnode.id < 2);
            }),
        ];

        for (names, expect) in test_cases {
            let mut ring = VNodeHashRing::new(2);
            for name in names {
                ring.add(name.to_string());
            }

            expect(ring.get(&"test_key"));
        }
    }

    #[test]
    fn get_with_replicas_spans_the_nodes() {
        let test_cases: Vec<(Vec<&str>, usize, fn(Option<Vec<VNode>>))> = vec![
            (vec![], 2, |vnodes| assert!(vnodes.is_none())),
            (vec!["default-pod-1", "default-pod-2"], 2, |vnodes| {
                let vnodes = vnodes.unwrap();
                assert_eq!(vnodes.len(), 3);
                assert!(vnodes.iter().all(|vnode| ["default-pod-1", "default-pod-2"]
                    .contains(&vnode.name())
                    && vnode.id < 2));
            }),
            (vec!["default-pod-1", "default-pod-2"], 3, |vnodes| {
                let vnodes = vnodes.unwrap();
                assert_eq!(vnodes.len(), 4);
                assert!(vnodes.iter().all(|vnode| ["default-pod-1", "default-pod-2"]
                    .contains(&vnode.name())
                    && vnode.id < 2));
            }),
            (vec!["default-pod-1", "default-pod-2"], 4, |vnodes| {
                let vnodes = vnodes.unwrap();
                assert_eq!(vnodes.len(), 5);
                assert!(vnodes.iter().all(|vnode| ["default-pod-1", "default-pod-2"]
                    .contains(&vnode.name())
                    && vnode.id < 2));
            }),
        ];

        for (names, replicas, expect) in test_cases {
            let mut ring = VNodeHashRing::new(2);
            for name in names {
                ring.add(name.to_string());
            }

            expect(ring.get_with_replicas(&"test_key", replicas));
        }
    }

    #[test]
    fn add_order_does_not_affect_get_result() {
        let mut ring_a = VNodeHashRing::new(150);
        for name in ["default-pod-1", "default-pod-2", "default-pod-3"] {
            ring_a.add(name.to_string());
        }

        let mut ring_b = VNodeHashRing::new(150);
        for name in ["default-pod-3", "default-pod-1", "default-pod-2"] {
            ring_b.add(name.to_string());
        }

        for _ in 0..200 {
            let key = Uuid::new_v4().to_string();
            let vnode_a = ring_a.get(&key).unwrap();
            let vnode_b = ring_b.get(&key).unwrap();
            assert_eq!(vnode_a.to_string(), vnode_b.to_string());
        }
    }
}
