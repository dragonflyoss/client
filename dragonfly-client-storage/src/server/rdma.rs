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
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
#[cfg(target_os = "linux")]
use ibverbs::{devices, ibv_qp_type, ibv_wc, CompletionQueue, QueuePairEndpoint};
use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tracing::error;

const MAX_CQ_SIZE: usize = 16 * 1024;
const MIN_CQ_ENTRIES: i32 = 256;
const DEFAULT_GID_INDEX: u32 = 1;

#[cfg(target_os = "linux")]
pub struct RDMAServer {
    ctx: Arc<ibverbs::Context>,

    addr: SocketAddr,

    available_cp_ids: Mutex<VecDeque<u64>>,
}

#[cfg(target_os = "linux")]
impl RDMAServer {
    pub fn new(addr: SocketAddr, device: Option<String>) -> Result<Self> {
        let devices = devices()?;
        let ctx = match device {
            Some(device_name) => devices
                .iter()
                .find(|d| d.name().unwrap().to_bytes() == device_name.as_bytes())
                .map(|d| d.open())
                .transpose()?
                .ok_or_else(|| Error::InvalidParameter)?,
            None => devices
                .iter()
                .next()
                .ok_or_else(|| Error::InvalidParameter)?
                .open()?,
        };

        let mut initial_ids = VecDeque::with_capacity(MAX_CQ_SIZE);
        for i in 0..MAX_CQ_SIZE as u64 {
            initial_ids.push_back(i);
        }

        Ok(Self {
            ctx: Arc::new(ctx),
            addr,
            available_cp_ids: Mutex::new(initial_ids),
        })
    }

    async fn acquire_cp_id(&self) -> Result<u64> {
        let mut available_cp_ids = self.available_cp_ids.lock().await;
        available_cp_ids.pop_front().ok_or_else(|| {
            error!("connection pool exhausted");
            Error::ResourceExhausted("connection pool exhausted".to_string())
        })
    }

    async fn release_cp_id(&self, cp_id: u64) {
        let mut available_cp_ids = self.available_cp_ids.lock().await;
        available_cp_ids.push_back(cp_id);
    }

    async fn handle_connection(
        &self,
        ctx: Arc<ibverbs::Context>,
        mut stream: TcpStream,
    ) -> Result<()> {
        let cp_id = self.acquire_cp_id().await?;
        let cq = ctx.create_cq(MIN_CQ_ENTRIES, cp_id as isize)?;
        let pd = ctx.alloc_pd()?;
        let qp_builder = pd
            .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)
            .set_gid_index(DEFAULT_GID_INDEX)
            .build()?;

        let local_endpoint = qp_builder.endpoint();
        self.send_local_endpoint(&mut stream, &local_endpoint)
            .await?;

        let remote_endpoint = self.recv_remote_endpoint(&mut stream).await?;
        let mut qp = qp_builder.handshake(remote_endpoint)?;

        Ok(())
    }

    async fn recv_remote_endpoint(&self, stream: &mut TcpStream) -> Result<QueuePairEndpoint> {
        let mut len_bytes = [0u8; 4];
        stream.read_exact(&mut len_bytes).await?;
        let len = u32::from_be_bytes(len_bytes);
        let mut remote_endpoint_bytes = vec![0u8; len as usize];
        stream.read_exact(&mut remote_endpoint_bytes).await?;

        let remote_endpoint =
            bincode::deserialize(&remote_endpoint_bytes).or_err(ErrorType::ParseError)?;
        Ok(remote_endpoint)
    }

    async fn send_local_endpoint(
        &self,
        stream: &mut TcpStream,
        local_endpoint: &QueuePairEndpoint,
    ) -> Result<()> {
        let local_endpoint_bytes =
            bincode::serialize(&local_endpoint).or_err(ErrorType::SerializeError)?;
        let len = local_endpoint_bytes.len() as u32;

        stream.write_all(&len.to_be_bytes()).await?;
        stream.write_all(&local_endpoint_bytes).await?;
        Ok(())
    }
}

#[cfg(target_os = "linux")]
#[tonic::async_trait]
impl super::Server for RDMAServer {
    async fn run(self: Arc<Self>) -> Result<()> {
        let listener = TcpListener::bind(&self.addr).await?;
        loop {
            let (stream, _) = listener.accept().await?;
            let ctx = self.ctx.clone();
            let self_clone = self.clone();
            tokio::spawn(async move {
                if let Err(err) = self_clone.handle_connection(ctx, stream).await {
                    error!("failed to handle connection: {}", err);
                }
            });
        }
    }
}
