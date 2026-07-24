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

pub mod quic;
pub mod tcp;

use bytes::Bytes;
use futures::stream::BoxStream;
use std::time::Duration;

/// The stream of bytes chunks of the piece content, consumed by the storage
/// to write the piece without copying the chunks.
pub type PieceContentStream = BoxStream<'static, std::io::Result<Bytes>>;

/// The default size of the send buffer for network connections.
const DEFAULT_SEND_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// The default size of the receive buffer for network connections.
const DEFAULT_RECV_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// The default timeout for establishing network connections.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(2);

/// The default interval for sending keepalive messages.
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(3);

/// The default time before starting keepalive messages.
const DEFAULT_KEEPALIVE_TIME: Duration = Duration::from_secs(5);

/// The default number of keepalive retries.
const DEFAULT_KEEPALIVE_RETRIES: u32 = 3;

/// The default maximum idle timeout for connections.
const DEFAULT_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
