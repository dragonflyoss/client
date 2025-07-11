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

use dragonfly_api::dfdaemon::v2::{
    DownloadPieceRequest, 
    DownloadPieceResponse,
    DownloadPersistentCachePieceRequest,
    DownloadPersistentCachePieceResponse,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Result as ClientResult;
use std::sync::Arc;
use std::time::Duration;

pub mod tcp;

#[tonic::async_trait]
pub trait Client: Send + Sync {
    
    async fn new(
        config: Arc<Config>,
        addr: String,
        is_download_piece: bool,
    ) -> ClientResult<Self> where Self: Sized;
    
    async fn download_piece(
        &self,
        request: DownloadPieceRequest,
        timeout: Duration,
    ) -> ClientResult<DownloadPieceResponse>;

    async fn download_persistent_cache_piece(
        &self,
        request: DownloadPersistentCachePieceRequest,
        timeout: Duration,
    ) -> ClientResult<DownloadPersistentCachePieceResponse>;
}