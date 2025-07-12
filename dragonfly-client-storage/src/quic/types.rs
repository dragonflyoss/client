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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use dragonfly_api::common::v2::Piece;
use dragonfly_api::dfdaemon::v2::{
    DownloadPieceRequest, DownloadPieceResponse, DownloadTaskRequest, DownloadTaskResponse,
    SyncPiecesRequest, SyncPiecesResponse, DownloadPersistentCachePieceRequest, DownloadPersistentCachePieceResponse,
};
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// QUIC message types
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuicMessageType {
    /// Download piece request
    DownloadPiece,
    /// Download piece response
    DownloadPieceResponse,
    /// Download task request
    DownloadTask,
    /// Download task response
    DownloadTaskResponse,
    /// Sync pieces request
    SyncPieces,
    /// Sync pieces response
    SyncPiecesResponse,
    /// Download persistent cache piece request
    DownloadPersistentCachePiece,
    /// Download persistent cache piece response
    DownloadPersistentCachePieceResponse,
    /// Health check
    HealthCheck,
    /// Health check response
    HealthCheckResponse,
}

/// QUIC message header
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicMessageHeader {
    /// Message type
    pub message_type: QuicMessageType,
    /// Message ID for correlation
    pub message_id: u64,
    /// Message size in bytes
    pub message_size: u32,
    /// Timestamp
    pub timestamp: u64,
}

/// QUIC message wrapper
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuicMessage {
    /// Message header
    pub header: QuicMessageHeader,
    /// Message payload
    pub payload: QuicMessagePayload,
}

/// QUIC message payload
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QuicMessagePayload {
    /// Download piece request
    DownloadPieceRequest(DownloadPieceRequest),
    /// Download piece response
    DownloadPieceResponse(DownloadPieceResponse),
    /// Download task request
    DownloadTaskRequest(DownloadTaskRequest),
    /// Download task response
    DownloadTaskResponse(DownloadTaskResponse),
    /// Sync pieces request
    SyncPiecesRequest(SyncPiecesRequest),
    /// Sync pieces response
    SyncPiecesResponse(SyncPiecesResponse),
    /// Download persistent cache piece request
    DownloadPersistentCachePieceRequest(DownloadPersistentCachePieceRequest),
    /// Download persistent cache piece response
    DownloadPersistentCachePieceResponse(DownloadPersistentCachePieceResponse),
    /// Health check
    HealthCheck,
    /// Health check response
    HealthCheckResponse { status: String },
}

impl QuicMessage {
    /// Create a new QUIC message
    pub fn new(message_type: QuicMessageType, payload: QuicMessagePayload) -> Self {
        let message_id = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        Self {
            header: QuicMessageHeader {
                message_type,
                message_id,
                message_size: 0, // Will be set during serialization
                timestamp,
            },
            payload,
        }
    }

    /// Serialize message to bytes
    pub fn serialize(&self) -> ClientResult<Bytes> {
        let mut buf = BytesMut::new();
        
        // Serialize header
        let header_bytes = bincode::serialize(&self.header)
            .map_err(|e| ClientError::InvalidParameter)?;
        buf.put_u32_le(header_bytes.len() as u32);
        buf.extend_from_slice(&header_bytes);
        
        // Serialize payload
        let payload_bytes = bincode::serialize(&self.payload)
            .map_err(|e| ClientError::InvalidParameter)?;
        buf.put_u32_le(payload_bytes.len() as u32);
        buf.extend_from_slice(&payload_bytes);
        
        Ok(buf.freeze())
    }

    /// Deserialize message from bytes
    pub fn deserialize(data: &[u8]) -> ClientResult<Self> {
        let mut buf = std::io::Cursor::new(data);
        
        // Deserialize header
        let header_size = buf.get_u32_le() as usize;
        let header_data = &data[4..4 + header_size];
        let header: QuicMessageHeader = bincode::deserialize(header_data)
            .map_err(|e| ClientError::InvalidParameter)?;
        
        // Deserialize payload
        let payload_size = buf.get_u32_le() as usize;
        let payload_data = &data[4 + header_size..4 + header_size + payload_size];
        let payload: QuicMessagePayload = bincode::deserialize(payload_data)
            .map_err(|e| ClientError::InvalidParameter)?;
        
        Ok(Self { header, payload })
    }
}

/// QUIC connection configuration
#[derive(Debug, Clone)]
pub struct QuicConfig {
    /// Server address
    pub addr: String,
    /// Connection timeout
    pub timeout: Duration,
    /// Max concurrent streams
    pub max_concurrent_streams: u32,
    /// Keep alive interval
    pub keep_alive_interval: Duration,
}

impl Default for QuicConfig {
    fn default() -> Self {
        Self {
            addr: "127.0.0.1:8080".to_string(),
            timeout: Duration::from_secs(30),
            max_concurrent_streams: 100,
            keep_alive_interval: Duration::from_secs(60),
        }
    }
}

/// QUIC server configuration
#[derive(Debug, Clone)]
pub struct QuicServerConfig {
    /// Listen address
    pub listen_addr: String,
    /// Certificate path
    pub cert_path: Option<String>,
    /// Key path
    pub key_path: Option<String>,
    /// Max concurrent connections
    pub max_concurrent_connections: u32,
    /// Request timeout
    pub request_timeout: Duration,
}

impl Default for QuicServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "0.0.0.0:8080".to_string(),
            cert_path: None,
            key_path: None,
            max_concurrent_connections: 1000,
            request_timeout: Duration::from_secs(30),
        }
    }
} 