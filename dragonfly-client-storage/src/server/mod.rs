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


use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Result as ClientResult;
use dragonfly_client_util::id_generator::IDGenerator;
use std::sync::Arc;
use std::net::SocketAddr;
use crate::shutdown;
use tokio::sync::mpsc;
use tokio::sync::Barrier;
use crate::Storage;

pub mod tcp;

#[tonic::async_trait]
pub trait Server: Send + Sync {
    
    fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        addr: SocketAddr,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self;
    
    async fn run(&mut self, tcp_server_started_barrier: Arc<Barrier>) -> ClientResult<()>;
}

#[tonic::async_trait]
pub trait ServerHandler: Send + Sync {
    
    async fn handle_download_piece_request(
        &self,
        request_data: &[u8],
    ) -> Result<Vec<u8>, String>;

    async fn handle_download_persistent_cache_piece_request(
        &self,
        request_data: &[u8],
    ) -> Result<Vec<u8>, String>;
}