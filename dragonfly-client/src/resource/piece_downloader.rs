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

use crate::grpc::dfdaemon_upload::DfdaemonUploadClient;
use dragonfly_api::dfdaemon::v2::DownloadPieceRequest;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error, Result};
use std::sync::Arc;
use tracing::{error, instrument};

/// Downloader is the interface for downloading pieces, which is implemented by different
/// protocols. The downloader is used to download pieces from the other peers.
#[tonic::async_trait]
pub trait Downloader {
    /// download_piece downloads a piece from the other peer by different protocols.
    async fn download_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(Vec<u8>, u64, String)>;
}

/// DownloaderFactory is the factory for creating different downloaders by different protocols.
pub struct DownloaderFactory {
    /// downloader is the downloader for downloading pieces, which is implemented by different
    /// protocols.
    downloader: Arc<dyn Downloader + Send + Sync>,
}

/// DownloadFactory implements the DownloadFactory trait.
impl DownloaderFactory {
    /// new returns a new DownloadFactory.
    #[instrument(skip_all)]
    pub fn new(protocol: &str, config: Arc<Config>) -> Result<Self> {
        let downloader = match protocol {
            "grpc" => Arc::new(GRPCDownloader::new(config.clone())),
            _ => {
                error!("downloader unsupported protocol: {}", protocol);
                return Err(Error::InvalidParameter);
            }
        };

        Ok(Self { downloader })
    }

    /// build returns the downloader.
    #[instrument(skip_all)]
    pub fn build(&self) -> Arc<dyn Downloader + Send + Sync> {
        self.downloader.clone()
    }
}

/// GRPCDownloader is the downloader for downloading pieces by the gRPC protocol.
pub struct GRPCDownloader {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,
}

/// GRPCDownloader implements the downloader with the gRPC protocol.
impl GRPCDownloader {
    /// new returns a new GRPCDownloader.
    #[instrument(skip_all)]
    pub fn new(config: Arc<Config>) -> Self {
        Self { config }
    }
}

/// GRPCDownloader implements the Downloader trait.
#[tonic::async_trait]
impl Downloader for GRPCDownloader {
    /// download_piece downloads a piece from the other peer by the gRPC protocol.
    #[instrument(skip_all)]
    async fn download_piece(
        &self,
        addr: &str,
        number: u32,
        host_id: &str,
        task_id: &str,
    ) -> Result<(Vec<u8>, u64, String)> {
        let dfdaemon_upload_client =
            DfdaemonUploadClient::new(self.config.clone(), format!("http://{}", addr)).await?;

        let response = dfdaemon_upload_client
            .download_piece(
                DownloadPieceRequest {
                    host_id: host_id.to_string(),
                    task_id: task_id.to_string(),
                    piece_number: number,
                },
                self.config.download.piece_timeout,
            )
            .await?;

        let Some(piece) = response.piece else {
            return Err(Error::InvalidParameter);
        };

        let Some(content) = piece.content else {
            return Err(Error::InvalidParameter);
        };

        Ok((content, piece.offset, piece.digest))
    }
}
