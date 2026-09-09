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

//! Wire protocol for the RDMA rendezvous channel.
//!
//! Control messages (piece request, capability exchange, metadata, readiness, errors) ride
//! a plain TCP connection to the parent's RDMA rendezvous port; only bulk piece bytes move
//! over libfabric. Keeping control on TCP gives reliable framing for messages that must not
//! be lost, sidesteps EFA's limited unexpected-message buffering, and makes falling back to
//! the TCP piece transport trivial.
//!
//! Framing: magic (u32) | version (u8) | frame type (u8) | payload length (u32) | payload.
//! All integers are big-endian. Payload fields are fixed-width integers and
//! length-prefixed byte strings with hard caps, so a malicious peer cannot force large
//! allocations.

use dragonfly_client_core::{Error, Result};
use std::sync::{Arc, RwLock};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// MAGIC identifies a Dragonfly RDMA rendezvous frame ("DFRD").
pub const MAGIC: u32 = 0x4446_5244;

/// VERSION is the rendezvous wire-contract version. Peers with different versions must
/// fall back to TCP.
pub const VERSION: u8 = 2;

/// MAX_TASK_ID_LENGTH caps the task id field.
const MAX_TASK_ID_LENGTH: usize = 4096;

/// MAX_ENDPOINT_LENGTH caps provider-opaque endpoint addresses.
const MAX_ENDPOINT_LENGTH: usize = 512;

/// MAX_STRING_LENGTH caps provider names, fabric tags, digests, and error messages.
const MAX_STRING_LENGTH: usize = 4096;

/// MAX_PAYLOAD_LENGTH caps a whole frame payload.
const MAX_PAYLOAD_LENGTH: usize = 64 * 1024;

/// ERROR_CODE_INCOMPATIBLE means the peers cannot form a fabric pair (provider, fabric
/// tag, or contract-version mismatch). The client should cache this and stop attempting
/// RDMA to this parent for a while.
pub const ERROR_CODE_INCOMPATIBLE: u32 = 1;

/// ERROR_CODE_NOT_FOUND means the requested piece is not available on the parent.
pub const ERROR_CODE_NOT_FOUND: u32 = 2;

/// ERROR_CODE_INTERNAL means the parent failed to serve the piece.
pub const ERROR_CODE_INTERNAL: u32 = 3;

/// ERROR_CODE_TOO_LARGE means the piece exceeds the parent's transfer limits.
pub const ERROR_CODE_TOO_LARGE: u32 = 4;

/// ERROR_CODE_BUSY means the parent is already serving as many RDMA transfers as it admits. The
/// client should fall back to TCP for this piece, and unlike the other codes this says nothing
/// about whether the parent can serve RDMA at all.
pub const ERROR_CODE_BUSY: u32 = 5;

/// PieceKind selects which piece namespace a request addresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PieceKind {
    /// Piece is a regular task piece.
    Piece,

    /// PersistentPiece is a persistent task piece.
    PersistentPiece,

    /// PersistentCachePiece is a persistent cache task piece.
    PersistentCachePiece,
}

impl TryFrom<u8> for PieceKind {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Piece),
            1 => Ok(Self::PersistentPiece),
            2 => Ok(Self::PersistentCachePiece),
            _ => Err(Error::Unknown(format!("invalid piece kind: {value}"))),
        }
    }
}

impl From<PieceKind> for u8 {
    fn from(value: PieceKind) -> Self {
        match value {
            PieceKind::Piece => 0,
            PieceKind::PersistentPiece => 1,
            PieceKind::PersistentCachePiece => 2,
        }
    }
}

/// WireCapability describes one side of a prospective fabric pair.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireCapability {
    /// provider is the concrete libfabric provider name (e.g. "efa", "verbs;ofi_rxm").
    pub provider: String,

    /// fabric_tag is the operator-supplied reachability-domain label.
    pub fabric_tag: String,
}

/// RdmaAdvertisement is returned on the already-advertised TCP piece port so downloaders learn
/// the actual RDMA rendezvous port and only attempt compatible, initialized parents.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RdmaAdvertisement {
    /// capability is the concrete provider and reachability domain currently serving requests.
    pub capability: WireCapability,

    /// port is the parent's RDMA rendezvous listener port.
    pub port: u16,
}

/// CapabilityRegistry publishes RDMA readiness to the TCP piece server. A value is present only
/// after the RDMA fabric and rendezvous listener have both initialized successfully.
#[derive(Clone, Default)]
pub struct CapabilityRegistry {
    inner: Arc<RwLock<Option<RdmaAdvertisement>>>,
}

impl CapabilityRegistry {
    /// publish replaces the current ready advertisement.
    pub fn publish(&self, advertisement: RdmaAdvertisement) {
        *self.inner.write().unwrap() = Some(advertisement);
    }

    /// clear removes the advertisement when the listener exits.
    pub fn clear(&self) {
        *self.inner.write().unwrap() = None;
    }

    /// get returns the current ready advertisement.
    pub fn get(&self) -> Option<RdmaAdvertisement> {
        self.inner.read().unwrap().clone()
    }
}

impl WireCapability {
    /// compatible fails closed: peers form a fabric pair only with identical providers and
    /// identical, non-empty fabric tags. An EFA endpoint is never compatible with a verbs
    /// endpoint even though both speak libfabric.
    pub fn compatible(&self, remote: &WireCapability) -> std::result::Result<(), String> {
        if self.provider.is_empty() || remote.provider.is_empty() {
            return Err("missing provider".to_string());
        }
        if self.provider != remote.provider {
            return Err(format!(
                "provider mismatch: local {}, remote {}",
                self.provider, remote.provider
            ));
        }
        if self.fabric_tag.is_empty() || remote.fabric_tag.is_empty() {
            return Err("missing fabric tag".to_string());
        }
        if self.fabric_tag != remote.fabric_tag {
            return Err(format!(
                "fabric tag mismatch: local {}, remote {}",
                self.fabric_tag, remote.fabric_tag
            ));
        }
        Ok(())
    }
}

/// PieceRequest asks a parent for one piece over the fabric.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PieceRequest {
    /// kind selects the piece namespace.
    pub kind: PieceKind,

    /// task_id is the task the piece belongs to.
    pub task_id: String,

    /// piece_number is the piece index within the task.
    pub piece_number: u32,

    /// capability describes the downloader's fabric endpoint.
    pub capability: WireCapability,

    /// client_endpoint is the downloader's provider-opaque endpoint address.
    pub client_endpoint: Vec<u8>,

    /// tag is the base tag for the transfer; chunk i uses tag + i.
    pub tag: u64,

    /// chunk_size is the largest single fabric message the downloader accepts.
    pub chunk_size: u64,

    /// max_inflight_chunks is the maximum number of receives the downloader posts at once.
    pub max_inflight_chunks: u32,
}

/// PieceReady tells the downloader the parent is ready to send the piece.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PieceReady {
    /// offset is the piece offset within the task.
    pub offset: u64,

    /// length is the piece length in bytes.
    pub length: u64,

    /// digest is the piece digest for end-to-end verification.
    pub digest: String,

    /// server_endpoint is the parent's provider-opaque endpoint address.
    pub server_endpoint: Vec<u8>,

    /// chunk_size is the negotiated fabric message size (min of both sides).
    pub chunk_size: u64,

    /// max_inflight_chunks is the negotiated operation window (min of both sides).
    pub max_inflight_chunks: u32,
}

/// RendezvousError reports a failure over the rendezvous channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RendezvousError {
    /// code is one of the ERROR_CODE_* constants.
    pub code: u32,

    /// message is a human-readable description.
    pub message: String,
}

/// Frame is one rendezvous message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// Discover asks the normal TCP piece server for its current RDMA advertisement.
    Discover,

    /// Capability returns the concrete provider and rendezvous port of a ready RDMA server.
    Capability(RdmaAdvertisement),

    /// Request initiates a piece transfer (client to parent).
    Request(PieceRequest),

    /// Ready reports piece metadata and the parent endpoint (parent to client).
    Ready(PieceReady),

    /// RecvPosted grants permission to send one contiguous chunk window. The parent must not
    /// send the window before this arrives (EFA has limited unexpected-message buffering).
    RecvPosted {
        /// start_chunk is the zero-based first chunk in the posted window.
        start_chunk: u64,

        /// chunk_count is the number of contiguous posted receives.
        chunk_count: u32,
    },

    /// Done signals all fabric sends completed (parent to client).
    Done,

    /// Error aborts the transfer.
    Error(RendezvousError),
}

impl Frame {
    /// frame_type returns the wire discriminant.
    fn frame_type(&self) -> u8 {
        match self {
            Frame::Discover => 6,
            Frame::Capability(_) => 7,
            Frame::Request(_) => 1,
            Frame::Ready(_) => 2,
            Frame::RecvPosted { .. } => 3,
            Frame::Done => 4,
            Frame::Error(_) => 5,
        }
    }
}

/// put_bytes appends a length-prefixed byte string.
fn put_bytes(payload: &mut Vec<u8>, bytes: &[u8]) {
    payload.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    payload.extend_from_slice(bytes);
}

/// Reader decodes payload fields with bounds checks.
struct Reader<'a> {
    /// buf is the remaining payload.
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    /// take splits off `n` bytes from the payload.
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.buf.len() < n {
            return Err(Error::Unknown("rendezvous payload truncated".to_string()));
        }
        let (head, tail) = self.buf.split_at(n);
        self.buf = tail;
        Ok(head)
    }

    /// u8 reads one byte.
    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    /// u32 reads a big-endian u32.
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    /// u64 reads a big-endian u64.
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    /// bytes reads a length-prefixed byte string capped at `max`.
    fn bytes(&mut self, max: usize) -> Result<Vec<u8>> {
        let len = self.u32()? as usize;
        if len > max {
            return Err(Error::Unknown(format!(
                "rendezvous field of {len} bytes exceeds the {max} byte cap"
            )));
        }
        Ok(self.take(len)?.to_vec())
    }

    /// string reads a length-prefixed UTF-8 string capped at `max`.
    fn string(&mut self, max: usize) -> Result<String> {
        String::from_utf8(self.bytes(max)?)
            .map_err(|_| Error::Unknown("rendezvous string is not utf-8".to_string()))
    }
}

/// write_frame encodes and sends one frame.
pub async fn write_frame<W: AsyncWrite + Unpin>(writer: &mut W, frame: &Frame) -> Result<()> {
    let mut payload = Vec::new();
    match frame {
        Frame::Discover => {}
        Frame::Capability(advertisement) => {
            put_bytes(&mut payload, advertisement.capability.provider.as_bytes());
            put_bytes(&mut payload, advertisement.capability.fabric_tag.as_bytes());
            payload.extend_from_slice(&advertisement.port.to_be_bytes());
        }
        Frame::Request(request) => {
            payload.push(request.kind.into());
            put_bytes(&mut payload, request.task_id.as_bytes());
            payload.extend_from_slice(&request.piece_number.to_be_bytes());
            put_bytes(&mut payload, request.capability.provider.as_bytes());
            put_bytes(&mut payload, request.capability.fabric_tag.as_bytes());
            put_bytes(&mut payload, &request.client_endpoint);
            payload.extend_from_slice(&request.tag.to_be_bytes());
            payload.extend_from_slice(&request.chunk_size.to_be_bytes());
            payload.extend_from_slice(&request.max_inflight_chunks.to_be_bytes());
        }
        Frame::Ready(ready) => {
            payload.extend_from_slice(&ready.offset.to_be_bytes());
            payload.extend_from_slice(&ready.length.to_be_bytes());
            put_bytes(&mut payload, ready.digest.as_bytes());
            put_bytes(&mut payload, &ready.server_endpoint);
            payload.extend_from_slice(&ready.chunk_size.to_be_bytes());
            payload.extend_from_slice(&ready.max_inflight_chunks.to_be_bytes());
        }
        Frame::RecvPosted {
            start_chunk,
            chunk_count,
        } => {
            payload.extend_from_slice(&start_chunk.to_be_bytes());
            payload.extend_from_slice(&chunk_count.to_be_bytes());
        }
        Frame::Done => {}
        Frame::Error(error) => {
            payload.extend_from_slice(&error.code.to_be_bytes());
            put_bytes(&mut payload, error.message.as_bytes());
        }
    }

    let mut buf = Vec::with_capacity(10 + payload.len());
    buf.extend_from_slice(&MAGIC.to_be_bytes());
    buf.push(VERSION);
    buf.push(frame.frame_type());
    buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    buf.extend_from_slice(&payload);

    writer.write_all(&buf).await?;
    writer.flush().await?;
    Ok(())
}

/// read_frame reads and decodes one frame. A version mismatch is reported as an
/// incompatibility so callers fall back to TCP.
pub async fn read_frame<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Frame> {
    let mut header = [0u8; 10];
    reader.read_exact(&mut header).await?;

    let magic = u32::from_be_bytes(header[0..4].try_into().unwrap());
    if magic != MAGIC {
        return Err(Error::Unknown("invalid rendezvous magic".to_string()));
    }

    let version = header[4];
    if version != VERSION {
        return Err(Error::Unknown(format!(
            "rendezvous version mismatch: local {VERSION}, remote {version}"
        )));
    }

    let frame_type = header[5];
    let payload_length = u32::from_be_bytes(header[6..10].try_into().unwrap()) as usize;
    if payload_length > MAX_PAYLOAD_LENGTH {
        return Err(Error::Unknown(format!(
            "rendezvous payload of {payload_length} bytes exceeds the cap"
        )));
    }

    let mut payload = vec![0u8; payload_length];
    reader.read_exact(&mut payload).await?;
    let mut reader = Reader { buf: &payload };

    let frame = match frame_type {
        1 => Frame::Request(PieceRequest {
            kind: reader.u8()?.try_into()?,
            task_id: reader.string(MAX_TASK_ID_LENGTH)?,
            piece_number: reader.u32()?,
            capability: WireCapability {
                provider: reader.string(MAX_STRING_LENGTH)?,
                fabric_tag: reader.string(MAX_STRING_LENGTH)?,
            },
            client_endpoint: reader.bytes(MAX_ENDPOINT_LENGTH)?,
            tag: reader.u64()?,
            chunk_size: reader.u64()?,
            max_inflight_chunks: reader.u32()?,
        }),
        2 => Frame::Ready(PieceReady {
            offset: reader.u64()?,
            length: reader.u64()?,
            digest: reader.string(MAX_STRING_LENGTH)?,
            server_endpoint: reader.bytes(MAX_ENDPOINT_LENGTH)?,
            chunk_size: reader.u64()?,
            max_inflight_chunks: reader.u32()?,
        }),
        3 => Frame::RecvPosted {
            start_chunk: reader.u64()?,
            chunk_count: reader.u32()?,
        },
        4 => Frame::Done,
        5 => Frame::Error(RendezvousError {
            code: reader.u32()?,
            message: reader.string(MAX_STRING_LENGTH)?,
        }),
        6 => Frame::Discover,
        7 => Frame::Capability(RdmaAdvertisement {
            capability: WireCapability {
                provider: reader.string(MAX_STRING_LENGTH)?,
                fabric_tag: reader.string(MAX_STRING_LENGTH)?,
            },
            port: u16::from_be_bytes(reader.take(2)?.try_into().unwrap()),
        }),
        _ => {
            return Err(Error::Unknown(format!(
                "unknown rendezvous frame type: {frame_type}"
            )));
        }
    };
    if !reader.buf.is_empty() {
        return Err(Error::Unknown(format!(
            "rendezvous frame has {} trailing payload bytes",
            reader.buf.len()
        )));
    }
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// roundtrip encodes and decodes a frame through an in-memory duplex pipe.
    async fn roundtrip(frame: Frame) -> Frame {
        let (mut client, mut server) = tokio::io::duplex(64 * 1024 + 1024);
        write_frame(&mut client, &frame).await.unwrap();
        read_frame(&mut server).await.unwrap()
    }

    #[tokio::test]
    async fn roundtrips_all_frames() {
        assert_eq!(roundtrip(Frame::Discover).await, Frame::Discover);

        let advertisement = Frame::Capability(RdmaAdvertisement {
            capability: WireCapability {
                provider: "efa".to_string(),
                fabric_tag: "vpc-1/use1-az1".to_string(),
            },
            port: 4007,
        });
        assert_eq!(roundtrip(advertisement.clone()).await, advertisement);

        let request = Frame::Request(PieceRequest {
            kind: PieceKind::PersistentCachePiece,
            task_id: "task-123".to_string(),
            piece_number: 42,
            capability: WireCapability {
                provider: "efa".to_string(),
                fabric_tag: "vpc-1/use1-az1".to_string(),
            },
            client_endpoint: vec![1, 2, 3, 4],
            tag: 0xdead_beef_dead_beef,
            chunk_size: 4 * 1024 * 1024,
            max_inflight_chunks: 16,
        });
        assert_eq!(roundtrip(request.clone()).await, request);

        let ready = Frame::Ready(PieceReady {
            offset: 128,
            length: 4096,
            digest: "crc32:12345678".to_string(),
            server_endpoint: vec![9, 8, 7],
            chunk_size: 1024 * 1024,
            max_inflight_chunks: 8,
        });
        assert_eq!(roundtrip(ready.clone()).await, ready);

        let recv_posted = Frame::RecvPosted {
            start_chunk: 32,
            chunk_count: 8,
        };
        assert_eq!(roundtrip(recv_posted.clone()).await, recv_posted);
        assert_eq!(roundtrip(Frame::Done).await, Frame::Done);

        let error = Frame::Error(RendezvousError {
            code: ERROR_CODE_INCOMPATIBLE,
            message: "provider mismatch".to_string(),
        });
        assert_eq!(roundtrip(error.clone()).await, error);
    }

    #[tokio::test]
    async fn rejects_bad_magic_and_version() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        client.write_all(&[0xff; 10]).await.unwrap();
        assert!(read_frame(&mut server).await.is_err());

        let (mut client, mut server) = tokio::io::duplex(1024);
        let mut header = Vec::new();
        header.extend_from_slice(&MAGIC.to_be_bytes());
        header.push(VERSION + 1);
        header.push(3);
        header.extend_from_slice(&0u32.to_be_bytes());
        client.write_all(&header).await.unwrap();
        let err = read_frame(&mut server).await.unwrap_err();
        assert!(err.to_string().contains("version mismatch"));
    }

    #[tokio::test]
    async fn rejects_oversized_fields() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let mut header = Vec::new();
        header.extend_from_slice(&MAGIC.to_be_bytes());
        header.push(VERSION);
        header.push(1);
        header.extend_from_slice(&(MAX_PAYLOAD_LENGTH as u32 + 1).to_be_bytes());
        client.write_all(&header).await.unwrap();
        assert!(read_frame(&mut server).await.is_err());
    }

    #[tokio::test]
    async fn rejects_trailing_payload_bytes() {
        let (mut client, mut server) = tokio::io::duplex(1024);
        let mut frame = Vec::new();
        frame.extend_from_slice(&MAGIC.to_be_bytes());
        frame.push(VERSION);
        frame.push(Frame::Done.frame_type());
        frame.extend_from_slice(&1u32.to_be_bytes());
        frame.push(0xff);
        client.write_all(&frame).await.unwrap();

        let err = read_frame(&mut server).await.unwrap_err();
        assert!(err.to_string().contains("trailing payload"));
    }

    #[test]
    fn capability_compatibility_fails_closed() {
        let efa = WireCapability {
            provider: "efa".to_string(),
            fabric_tag: "vpc-1/use1-az1".to_string(),
        };
        assert!(efa.compatible(&efa.clone()).is_ok());

        let verbs = WireCapability {
            provider: "verbs;ofi_rxm".to_string(),
            ..efa.clone()
        };
        assert!(efa.compatible(&verbs).is_err());

        let other_az = WireCapability {
            fabric_tag: "vpc-1/use1-az2".to_string(),
            ..efa.clone()
        };
        assert!(efa.compatible(&other_az).is_err());

        let untagged = WireCapability {
            fabric_tag: String::new(),
            ..efa.clone()
        };
        assert!(efa.compatible(&untagged).is_err());
        assert!(untagged.compatible(&untagged.clone()).is_err());
    }
}
