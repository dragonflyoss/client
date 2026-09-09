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

//! RDMA piece transport over libfabric.
//!
//! Both AWS EFA (SRD, no RC queue pairs) and conventional RDMA (RoCE/InfiniBand through
//! `verbs;ofi_rxm`) are driven through libfabric as the single transport stack. EFA-specific
//! direct-verbs APIs exist, but standard RC-based ibverbs libraries cannot drive EFA. The design is:
//!
//! - control plane (piece request, capability negotiation, metadata, errors) over a TCP
//!   rendezvous connection ([`rendezvous`]);
//! - data plane (bulk piece bytes) over two-sided tagged messaging on a shared FI_EP_RDM
//!   endpoint ([`fabric`], feature `rdma`), which never exposes remote-access memory keys;
//! - live, fail-closed capability discovery on the already-advertised TCP piece endpoint,
//!   with mandatory per-piece TCP fallback ([`rendezvous`]).
//!
//! The [`fabric`] module (and the client/server built on it) requires libfabric at build
//! time and is gated behind the `rdma` cargo feature. That feature is Linux-only; [`rendezvous`]
//! is plain TCP and builds everywhere so the capability handshake can be tested on any host.

#[cfg(all(feature = "rdma", not(target_os = "linux")))]
compile_error!(
    "the rdma feature is only supported on linux: it needs libfabric and the registered-memory \
     write path in content_linux.rs"
);

pub mod rendezvous;

#[cfg(feature = "rdma")]
pub mod fabric;
