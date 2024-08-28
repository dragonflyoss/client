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
use crate::metrics::{
    collect_backend_request_failure_metrics, collect_backend_request_finished_metrics,
    collect_backend_request_started_metrics, collect_download_piece_traffic_metrics,
    collect_upload_piece_traffic_metrics,
};
use chrono::Utc;
use dragonfly_api::common::v2::{ObjectStorage, Range, TrafficType};
use dragonfly_api::dfdaemon::v2::DownloadPieceRequest;
use dragonfly_client_backend::{BackendFactory, GetRequest};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{error::BackendError, Error, Result};
use dragonfly_client_storage::{metadata, Storage};
use dragonfly_client_util::id_generator::IDGenerator;
use leaky_bucket::RateLimiter;
use reqwest::header::{self, HeaderMap};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{error, info, instrument, Span};

use super::*;

// MAX_PIECE_COUNT is the maximum piece count. If the piece count is upper
// than MAX_PIECE_COUNT, the piece length will be optimized by the file length.
// When piece length becames the MAX_PIECE_LENGTH, the piece piece count
// probably will be upper than MAX_PIECE_COUNT.
const MAX_PIECE_COUNT: u64 = 500;

// MIN_PIECE_LENGTH is the minimum piece length.
const MIN_PIECE_LENGTH: u64 = 4 * 1024 * 1024;

// MAX_PIECE_LENGTH is the maximum piece length.
const MAX_PIECE_LENGTH: u64 = 16 * 1024 * 1024;

// PieceLengthStrategy sets the optimization strategy of piece length.
pub enum PieceLengthStrategy {
    // OptimizeByFileLength optimizes the piece length by the file length.
    OptimizeByFileLength,
}

// Piece represents a piece manager.
pub struct Piece {
    // config is the configuration of the dfdaemon.
    config: Arc<Config>,

    // id_generator is the id generator.
    id_generator: Arc<IDGenerator>,

    // manager_client is the grpc client of the manager.
    storage: Arc<Storage>,

    // backend_factory is the backend factory.
    backend_factory: Arc<BackendFactory>,

    // download_rate_limiter is the rate limiter of the download speed in bps(bytes per second).
    download_rate_limiter: Arc<RateLimiter>,

    // upload_rate_limiter is the rate limiter of the upload speed in bps(bytes per second).
    upload_rate_limiter: Arc<RateLimiter>,
}

// Piece implements the piece manager.
impl Piece {
    // new returns a new Piece.
    #[instrument(skip_all)]
    pub fn new(
        config: Arc<Config>,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        backend_factory: Arc<BackendFactory>,
    ) -> Self {
        Self {
            config: config.clone(),
            id_generator,
            storage,
            backend_factory,
            download_rate_limiter: Arc::new(
                RateLimiter::builder()
                    .initial(config.download.rate_limit.as_u64() as usize)
                    .refill(config.download.rate_limit.as_u64() as usize)
                    .interval(Duration::from_secs(1))
                    .fair(false)
                    .build(),
            ),
            upload_rate_limiter: Arc::new(
                RateLimiter::builder()
                    .initial(config.upload.rate_limit.as_u64() as usize)
                    .refill(config.upload.rate_limit.as_u64() as usize)
                    .interval(Duration::from_secs(1))
                    .build(),
            ),
        }
    }

    // get gets a piece from the local storage.
    #[instrument(skip_all)]
    pub fn get(&self, task_id: &str, number: u32) -> Result<Option<metadata::Piece>> {
        self.storage.get_piece(task_id, number)
    }

    // calculate_interested calculates the interested pieces by content_length and range.
    #[instrument(skip_all)]
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
                    let mut piece = pieces.pop().ok_or_else(|| {
                        error!("piece not found");
                        Error::InvalidParameter
                    })?;
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
                "calculate interested pieces by range: {:?}, piece length: {:?}. pieces: {:?}",
                range,
                piece_length,
                pieces
                    .iter()
                    .map(|piece| piece.number)
                    .collect::<Vec<u32>>()
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
                let mut piece = pieces.pop().ok_or_else(|| {
                    error!("piece not found");
                    Error::InvalidParameter
                })?;
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
            "calculate interested pieces by content length, piece length: {:?}, pieces: {:?}",
            piece_length,
            pieces
                .iter()
                .map(|piece| piece.number)
                .collect::<Vec<u32>>()
        );
        Ok(pieces)
    }

    // remove_finished_from_interested removes the finished pieces from interested pieces.
    #[instrument(skip_all)]
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

    // merge_finished_pieces merges the finished pieces and has finished pieces.
    #[instrument(skip_all)]
    pub fn merge_finished_pieces(
        &self,
        finished_pieces: Vec<metadata::Piece>,
        old_finished_pieces: Vec<metadata::Piece>,
    ) -> Vec<metadata::Piece> {
        let mut pieces: HashMap<u32, metadata::Piece> = HashMap::new();
        for finished_piece in finished_pieces.into_iter() {
            pieces.insert(finished_piece.number, finished_piece);
        }

        for old_finished_piece in old_finished_pieces.into_iter() {
            pieces
                .entry(old_finished_piece.number)
                .or_insert(old_finished_piece);
        }

        pieces.into_values().collect()
    }

    // calculate_piece_size calculates the piece size by content_length.
    pub fn calculate_piece_length(
        &self,
        strategy: PieceLengthStrategy,
        content_length: u64,
    ) -> u64 {
        match strategy {
            PieceLengthStrategy::OptimizeByFileLength => {
                let piece_length = (content_length as f64 / MAX_PIECE_COUNT as f64) as u64;
                let actual_piece_length = piece_length.next_power_of_two();

                match (
                    actual_piece_length > MIN_PIECE_LENGTH,
                    actual_piece_length < MAX_PIECE_LENGTH,
                ) {
                    (true, true) => actual_piece_length,
                    (_, false) => MAX_PIECE_LENGTH,
                    (false, _) => MIN_PIECE_LENGTH,
                }
            }
        }
    }

    // upload_from_local_peer_into_async_read uploads a single piece from a local peer.
    #[instrument(skip_all, fields(piece_id))]
    pub async fn upload_from_local_peer_into_async_read(
        &self,
        task_id: &str,
        number: u32,
        length: u64,
        range: Option<Range>,
        disable_rate_limit: bool,
    ) -> Result<impl AsyncRead> {
        // Span record the piece_id.
        Span::current().record("piece_id", self.storage.piece_id(task_id, number));

        // Acquire the upload rate limiter.
        if !disable_rate_limit {
            self.upload_rate_limiter.acquire(length as usize).await;
        }

        // Upload the piece content.
        self.storage
            .upload_piece(task_id, number, range)
            .await
            .map(|reader| {
                collect_upload_piece_traffic_metrics(
                    self.id_generator.task_type(task_id) as i32,
                    length,
                );
                reader
            })
    }

    // download_from_local_peer_into_async_read downloads a single piece from a local peer.
    #[instrument(skip_all, fields(piece_id))]
    pub async fn download_from_local_peer_into_async_read(
        &self,
        task_id: &str,
        number: u32,
        length: u64,
        range: Option<Range>,
        disable_rate_limit: bool,
    ) -> Result<impl AsyncRead> {
        // Span record the piece_id.
        Span::current().record("piece_id", self.storage.piece_id(task_id, number));

        // Acquire the download rate limiter.
        if !disable_rate_limit {
            self.download_rate_limiter.acquire(length as usize).await;
        }

        // Upload the piece content.
        self.storage.upload_piece(task_id, number, range).await
    }

    // download_from_local_peer downloads a single piece from a local peer. Fake the download piece
    // from the local peer, just collect the metrics.
    #[instrument(skip_all)]
    pub fn download_from_local_peer(&self, task_id: &str, length: u64) {
        collect_download_piece_traffic_metrics(
            &TrafficType::LocalPeer,
            self.id_generator.task_type(task_id) as i32,
            length,
        );
    }

    // download_from_remote_peer downloads a single piece from a remote peer.
    #[instrument(skip_all, fields(piece_id))]
    pub async fn download_from_remote_peer(
        &self,
        host_id: &str,
        task_id: &str,
        number: u32,
        length: u64,
        parent: piece_collector::CollectedParent,
    ) -> Result<metadata::Piece> {
        // Span record the piece_id.
        Span::current().record("piece_id", self.storage.piece_id(task_id, number));

        // Acquire the download rate limiter.
        self.download_rate_limiter.acquire(length as usize).await;

        // Record the start of downloading piece.
        let piece = self.storage.download_piece_started(task_id, number).await?;

        // If the piece is downloaded by the other thread,
        // return the piece directly.
        if piece.is_finished() {
            return Ok(piece);
        }

        // Create a dfdaemon client.
        let host = parent.host.clone().ok_or_else(|| {
            error!("peer host is empty");
            if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                error!("set piece metadata failed: {}", err)
            };

            Error::InvalidPeer(parent.id.clone())
        })?;
        let dfdaemon_upload_client =
            DfdaemonUploadClient::new(format!("http://{}:{}", host.ip, host.port))
                .await
                .map_err(|err| {
                    error!(
                        "create dfdaemon upload client from {}:{} failed: {}",
                        host.ip, host.port, err
                    );
                    if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                        error!("set piece metadata failed: {}", err)
                    };

                    err
                })?;

        // Send the interested pieces request.
        let response = dfdaemon_upload_client
            .download_piece(
                DownloadPieceRequest {
                    host_id: host_id.to_string(),
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
            .ok_or_else(|| {
                error!("piece not found");
                Error::PieceNotFound(number.to_string())
            })
            .map(|piece| {
                collect_download_piece_traffic_metrics(
                    &TrafficType::RemotePeer,
                    self.id_generator.task_type(task_id) as i32,
                    length,
                );
                piece
            })
    }

    // download_from_source downloads a single piece from the source.
    #[allow(clippy::too_many_arguments)]
    #[instrument(skip_all, fields(piece_id))]
    pub async fn download_from_source(
        &self,
        task_id: &str,
        number: u32,
        url: &str,
        offset: u64,
        length: u64,
        request_header: HeaderMap,
        object_storage: Option<ObjectStorage>,
    ) -> Result<metadata::Piece> {
        // Span record the piece_id.
        Span::current().record("piece_id", self.storage.piece_id(task_id, number));

        // Acquire the download rate limiter.
        self.download_rate_limiter.acquire(length as usize).await;

        // Record the start of downloading piece.
        let piece = self.storage.download_piece_started(task_id, number).await?;

        // If the piece is downloaded by the other thread,
        // return the piece directly.
        if piece.is_finished() {
            return Ok(piece);
        }

        // Add range header to the request by offset and length.
        let mut request_header = request_header.clone();
        request_header.insert(
            header::RANGE,
            format!("bytes={}-{}", offset, offset + length - 1)
                .parse()
                .unwrap(),
        );

        // Download the piece from the source.
        let backend = self.backend_factory.build(url).map_err(|err| {
            error!("build backend failed: {}", err);
            if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                error!("set piece metadata failed: {}", err)
            };

            err
        })?;

        // Record the start time.
        let start_time = Instant::now();

        // Collect the backend request started metrics.
        collect_backend_request_started_metrics(
            backend.scheme().as_str(),
            http::Method::GET.as_str(),
        );
        let mut response = backend
            .get(GetRequest {
                task_id: task_id.to_string(),
                piece_id: self.storage.piece_id(task_id, number),
                url: url.to_string(),
                range: Some(Range {
                    start: offset,
                    length,
                }),
                http_header: Some(request_header),
                timeout: self.config.download.piece_timeout,
                client_certs: None,
                object_storage,
            })
            .await
            .map_err(|err| {
                // Collect the backend request failure metrics.
                collect_backend_request_failure_metrics(
                    backend.scheme().as_str(),
                    http::Method::GET.as_str(),
                );

                // if the request is failed.
                error!("backend get failed: {}", err);
                if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                    error!("set piece metadata failed: {}", err)
                };

                err
            })?;

        if !response.success {
            // Collect the backend request failure metrics.
            collect_backend_request_failure_metrics(
                backend.scheme().as_str(),
                http::Method::GET.as_str(),
            );

            // if the status code is not OK.
            let mut buffer = String::new();
            response
                .reader
                .read_to_string(&mut buffer)
                .await
                .unwrap_or_default();

            let error_message = response.error_message.unwrap_or_default();
            error!("backend get failed: {} {}", error_message, buffer.as_str());

            self.storage.download_piece_failed(task_id, number)?;
            return Err(Error::BackendError(BackendError {
                message: error_message,
                status_code: Some(response.http_status_code.unwrap_or_default()),
                header: Some(response.http_header.unwrap_or_default()),
            }));
        }

        // Collect the backend request finished metrics.
        collect_backend_request_finished_metrics(
            backend.scheme().as_str(),
            http::Method::GET.as_str(),
            start_time.elapsed(),
        );

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

        self.storage
            .get_piece(task_id, number)?
            .ok_or_else(|| {
                error!("piece not found");
                Error::PieceNotFound(number.to_string())
            })
            .map(|piece| {
                collect_download_piece_traffic_metrics(
                    &TrafficType::BackToSource,
                    self.id_generator.task_type(task_id) as i32,
                    length,
                );
                piece
            })
    }
}
