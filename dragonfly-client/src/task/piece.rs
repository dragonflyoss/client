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

use crate::grpc::dfdaemon_upload::DfdaemonUploadClient;
use chrono::Utc;
use dragonfly_api::common::v2::{Peer, Range};
use dragonfly_api::dfdaemon::v2::DownloadPieceRequest;
use dragonfly_client_backend::{BackendFactory, GetRequest};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{error::HTTPError, Error, Result};
use dragonfly_client_storage::{metadata, Storage};
use leaky_bucket::RateLimiter;
use reqwest::header::{self, HeaderMap};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{error, info};

// CollectPiece represents a piece to collect.
pub struct CollectPiece {
    // number is the piece number.
    pub number: u32,

    // parent is the parent peer.
    pub parent: Peer,
}

// Piece represents a piece manager.
pub struct Piece {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

    // manager_client is the grpc client of the manager.
    storage: Arc<Storage>,

    // download_rate_limiter is the rate limiter of the download speed in bps(bytes per second).
    download_rate_limiter: Arc<RateLimiter>,

    // upload_rate_limiter is the rate limiter of the upload speed in bps(bytes per second).
    upload_rate_limiter: Arc<RateLimiter>,
}

// Piece implements the piece manager.
impl Piece {
    // new returns a new Piece.
    pub fn new(config: Arc<Config>, storage: Arc<Storage>) -> Self {
        Self {
            config: config.clone(),
            storage,
            download_rate_limiter: Arc::new(
                RateLimiter::builder()
                    .initial(config.download.rate_limit as usize)
                    .refill(config.download.rate_limit as usize)
                    .interval(Duration::from_secs(1))
                    .fair(false)
                    .build(),
            ),
            upload_rate_limiter: Arc::new(
                RateLimiter::builder()
                    .initial(config.upload.rate_limit as usize)
                    .refill(config.upload.rate_limit as usize)
                    .interval(Duration::from_secs(1))
                    .build(),
            ),
        }
    }

    // get gets a piece from the local storage.
    pub fn get(&self, task_id: &str, number: u32) -> Result<Option<metadata::Piece>> {
        self.storage.get_piece(task_id, number)
    }

    // get_all gets all pieces from the local storage.
    pub fn get_all(&self, task_id: &str) -> Result<Vec<metadata::Piece>> {
        self.storage.get_pieces(task_id)
    }

    // calculate_interested calculates the interested pieces by content_length and range.
    pub fn calculate_interested(
        &self,
        piece_length: u64,
        content_length: u64,
        range: Option<Range>,
    ) -> Result<Vec<metadata::Piece>> {
        // If content_length is 0, return empty piece.
        if content_length == 0 {
            return Ok(Vec::new());
        }

        // If range is not None, calculate the pieces by range.
        if let Some(range) = range {
            if range.length == 0 {
                return Err(Error::InvalidParameter);
            }

            let mut number = 0;
            let mut offset = 0;
            let mut pieces: Vec<metadata::Piece> = Vec::new();
            loop {
                // If offset is greater than content_length, break the loop.
                if offset >= content_length {
                    let mut piece = pieces.pop().ok_or(Error::InvalidParameter)?;
                    piece.length = piece_length + content_length - offset;
                    pieces.push(piece);
                    break;
                }

                // If offset is greater than range.start + range.length, break the loop.
                if offset >= range.start + range.length {
                    break;
                }

                if offset + piece_length > range.start {
                    pieces.push(metadata::Piece {
                        number: number as u32,
                        offset,
                        length: piece_length,
                        digest: "".to_string(),
                        parent_id: None,
                        uploading_count: 0,
                        uploaded_count: 0,
                        updated_at: Utc::now().naive_utc(),
                        created_at: Utc::now().naive_utc(),
                        finished_at: None,
                    });
                }

                offset = (number + 1) * piece_length;
                number += 1;
            }

            info!(
                "calculate interested pieces by range: {:?}, {:?}",
                range, pieces
            );
            return Ok(pieces);
        }

        // Calculate the pieces by content_length without range.
        let mut number = 0;
        let mut offset = 0;
        let mut pieces: Vec<metadata::Piece> = Vec::new();
        loop {
            // If offset is greater than content_length, break the loop.
            if offset >= content_length {
                let mut piece = pieces.pop().ok_or(Error::InvalidParameter)?;
                piece.length = piece_length + content_length - offset;
                pieces.push(piece);
                break;
            }

            pieces.push(metadata::Piece {
                number: number as u32,
                offset,
                length: piece_length,
                digest: "".to_string(),
                parent_id: None,
                uploading_count: 0,
                uploaded_count: 0,
                updated_at: Utc::now().naive_utc(),
                created_at: Utc::now().naive_utc(),
                finished_at: None,
            });

            offset = (number + 1) * piece_length;
            number += 1;
        }

        info!(
            "calculate interested pieces by content length: {:?}",
            pieces
        );
        Ok(pieces)
    }

    // remove_finished_from_interested removes the finished pieces from interested pieces.
    pub fn remove_finished_from_interested(
        &self,
        finished_pieces: Vec<metadata::Piece>,
        interested_pieces: Vec<metadata::Piece>,
    ) -> Vec<metadata::Piece> {
        interested_pieces
            .iter()
            .filter(|piece| {
                !finished_pieces
                    .iter()
                    .any(|finished_piece| finished_piece.number == piece.number)
            })
            .cloned()
            .collect::<Vec<metadata::Piece>>()
    }

    // upload_from_local_peer_into_async_read uploads a single piece from a local peer.
    pub async fn upload_from_local_peer_into_async_read(
        &self,
        task_id: &str,
        number: u32,
        length: u64,
        range: Option<Range>,
        disable_rate_limit: bool,
    ) -> Result<impl AsyncRead> {
        // Acquire the upload rate limiter.
        if !disable_rate_limit {
            self.upload_rate_limiter.acquire(length as usize).await;
        }

        // Upload the piece content.
        self.storage.upload_piece(task_id, number, range).await
    }

    // download_from_local_peer_into_async_read downloads a single piece from a local peer.
    pub async fn download_from_local_peer_into_async_read(
        &self,
        task_id: &str,
        number: u32,
        length: u64,
        range: Option<Range>,
        disable_rate_limit: bool,
    ) -> Result<impl AsyncRead> {
        // Acquire the download rate limiter.
        if !disable_rate_limit {
            self.download_rate_limiter.acquire(length as usize).await;
        }

        // Upload the piece content.
        self.storage.upload_piece(task_id, number, range).await
    }

    // download_from_remote_peer downloads a single piece from a remote peer.
    pub async fn download_from_remote_peer(
        &self,
        task_id: &str,
        number: u32,
        length: u64,
        parent: Peer,
    ) -> Result<metadata::Piece> {
        // Acquire the download rate limiter.
        self.download_rate_limiter.acquire(length as usize).await;

        // Record the start of downloading piece.
        self.storage.download_piece_started(task_id, number).await?;

        // Create a dfdaemon client.
        let host = parent
            .host
            .clone()
            .ok_or(Error::InvalidPeer(parent.id.clone()))?;
        let dfdaemon_upload_client =
            DfdaemonUploadClient::new(format!("http://{}:{}", host.ip, host.port))
                .await
                .map_err(|err| {
                    error!(
                        "create dfdaemon upload client from {}:{} failed: {}",
                        host.ip, host.port, err
                    );
                    err
                })?;

        // Send the interested pieces request.
        let response = dfdaemon_upload_client
            .download_piece(
                DownloadPieceRequest {
                    task_id: task_id.to_string(),
                    piece_number: number,
                },
                self.config.download.piece_timeout,
            )
            .await
            .map_err(|err| {
                error!("download piece failed: {}", err);
                if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                    error!("set piece metadata failed: {}", err)
                };
                err
            })?;

        let piece = response.piece.ok_or_else(|| {
            error!("piece is empty");
            if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                error!("set piece metadata failed: {}", err)
            };
            Error::InvalidParameter
        })?;

        // Get the piece content.
        let content = piece.content.ok_or_else(|| {
            error!("piece content is empty");
            if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                error!("set piece metadata failed: {}", err)
            };
            Error::InvalidParameter
        })?;

        // Record the finish of downloading piece.
        self.storage
            .download_piece_from_remote_peer_finished(
                task_id,
                number,
                piece.offset,
                piece.digest.as_str(),
                parent.id.as_str(),
                &mut content.as_slice(),
            )
            .await
            .map_err(|err| {
                // Record the failure of downloading piece,
                // If storage fails to record piece.
                error!("download piece finished: {}", err);
                if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                    error!("set piece metadata failed: {}", err)
                };
                err
            })?;

        self.storage
            .get_piece(task_id, number)?
            .ok_or(Error::PieceNotFound(number.to_string()))
    }

    // download_from_source downloads a single piece from the source.
    #[allow(clippy::too_many_arguments)]
    pub async fn download_from_source(
        &self,
        task_id: &str,
        number: u32,
        url: &str,
        offset: u64,
        length: u64,
        request_header: HeaderMap,
    ) -> Result<metadata::Piece> {
        // Acquire the download rate limiter.
        self.download_rate_limiter.acquire(length as usize).await;

        // Record the start of downloading piece.
        self.storage.download_piece_started(task_id, number).await?;

        // Add range header to the request by offset and length.
        let mut request_header = request_header.clone();
        request_header.insert(
            header::RANGE,
            format!("bytes={}-{}", offset, offset + length - 1)
                .parse()
                .unwrap(),
        );

        // Download the piece from the source.
        let backend = BackendFactory::new_backend(url)?;
        let mut response = backend
            .get(GetRequest {
                url: url.to_string(),
                http_header: Some(request_header.to_owned()),
                timeout: self.config.download.piece_timeout,
                client_certs: None,
            })
            .await
            .map_err(|err| {
                // Record the failure of downloading piece,
                // if the request is failed.
                error!("http error: {}", err);
                if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                    error!("set piece metadata failed: {}", err)
                };
                err
            })?;

        // HTTP status code is not OK, handle the error.
        if let Some(http_status_code) = response.http_status_code {
            let http_header = response.http_header.ok_or(Error::InvalidParameter)?;
            if !http_status_code.is_success() {
                // Record the failure of downloading piece,
                // if the status code is not OK.
                self.storage.download_piece_failed(task_id, number)?;

                let mut buffer = String::new();
                response.reader.read_to_string(&mut buffer).await?;
                error!("http error {}: {}", http_status_code, buffer.as_str());
                return Err(Error::HTTP(HTTPError {
                    status_code: http_status_code,
                    header: http_header,
                }));
            }

            // Record the finish of downloading piece.
            self.storage
                .download_piece_from_source_finished(
                    task_id,
                    number,
                    offset,
                    length,
                    &mut response.reader,
                )
                .await
                .map_err(|err| {
                    // Record the failure of downloading piece,
                    // If storage fails to record piece.
                    error!("download piece finished: {}", err);
                    if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                        error!("set piece metadata failed: {}", err)
                    };
                    err
                })?;

            return self
                .storage
                .get_piece(task_id, number)?
                .ok_or(Error::PieceNotFound(number.to_string()));
        }

        error!("backend returns invalid response");
        Err(Error::InvalidParameter)
    }
}
