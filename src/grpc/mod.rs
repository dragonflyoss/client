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

pub mod health;
pub mod manager;
pub mod scheduler;
pub mod security;

// REQUEST_TIMEOUT is the timeout for GRPC requests.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

// Error is the error for GRPC.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    // TonicTransport is the error for tonic transport.
    #[error(transparent)]
    TonicTransport(#[from] tonic::transport::Error),

    // TonicStatus is the error for tonic status.
    #[error(transparent)]
    TonicStatus(#[from] tonic::Status),
}

// Result is the result for GRPC.
pub type Result<T> = std::result::Result<T, Error>;
