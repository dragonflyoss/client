/*
 *     Copyright 2023 The Dragonfly Authors
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

use std::time::Duration;

pub mod dfdaemon_download;
pub mod dfdaemon_upload;
pub mod health;
pub mod manager;
pub mod scheduler;
pub mod security;

// CONNECT_TIMEOUT is the timeout for GRPC connection.
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(4);

// REQUEST_TIMEOUT is the timeout for GRPC requests.
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
