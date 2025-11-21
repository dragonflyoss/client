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

use std::time::Duration;

/// DEFAULT_SEND_BUFFER_SIZE is the default size of the send buffer for network connections.
const DEFAULT_SEND_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// DEFAULT_RECV_BUFFER_SIZE is the default size of the receive buffer for network connections.
const DEFAULT_RECV_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// DEFAULT_KEEPALIVE_INTERVAL is the default interval for sending keepalive messages.
const DEFAULT_KEEPALIVE_INTERVAL: Duration = Duration::from_secs(3);

/// DEFAULT_KEEPALIVE_TIME is the default time before starting keepalive messages.
const DEFAULT_KEEPALIVE_TIME: Duration = Duration::from_secs(5);

/// DEFAULT_KEEPALIVE_RETRIES is the default number of keepalive retries.
const DEFAULT_KEEPALIVE_RETRIES: u32 = 3;

/// DEFAULT_MAX_IDLE_TIMEOUT is the default maximum idle timeout for connections.
const DEFAULT_MAX_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
