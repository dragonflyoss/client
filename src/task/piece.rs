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

use crate::backend::http::{Request, HTTP};
use crate::config::dfdaemon::Config;
use crate::grpc::{dfdaemon::DfdaemonClient, scheduler::SchedulerClient};
use crate::storage::{metadata, Storage};
use crate::utils::digest::{Algorithm, Digest as UtilsDigest};
use crate::{Error, HTTPError, Result};
use chrono::Utc;
use dragonfly_api::common::v2::{Peer, Range};
use dragonfly_api::dfdaemon::v2::DownloadPieceRequest;
use reqwest::header::{self, HeaderMap};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use tokio::{
    fs,
    io::{self, AsyncRead, AsyncReadExt, AsyncSeekExt, SeekFrom},
};
use tokio_util::io::InspectReader;
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

    // scheduler_client is the grpc client of the scheduler.
    pub scheduler_client: Arc<SchedulerClient>,

    // http_client is the http client.
    http_client: Arc<HTTP>,
}

// Piece implements the piece manager.
impl Piece {
    // new returns a new Piece.
    pub fn new(
        config: Arc<Config>,
        storage: Arc<Storage>,
        scheduler_client: Arc<SchedulerClient>,
        http_client: Arc<HTTP>,
    ) -> Self {
        Self {
            config,
            storage,
            scheduler_client,
            http_client,
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

    // write_into_file_and_verify writes the piece into the file and verifies the digest of the piece.
    pub async fn write_into_file_and_verify<R: AsyncRead + Unpin + ?Sized>(
        &self,
        reader: &mut R,
        f: &mut fs::File,
        offset: u64,
        expected_digest: &str,
    ) -> Result<()> {
        // Sha256 is used to calculate the hash of the piece.
        let mut hasher = Sha256::new();

        // InspectReader is used to calculate the hash of the piece.
        let mut tee = InspectReader::new(reader, |bytes| hasher.update(bytes));

        // Seek the file to the offset.
        f.seek(SeekFrom::Start(offset)).await?;

        // Copy the piece to the file.
        io::copy(&mut tee, f).await?;

        // Calculate the hash of the piece.
        let hash = hasher.finalize();

        // Calculate the digest of the piece.
        let digest = UtilsDigest::new(Algorithm::Sha256, base16ct::lower::encode_string(&hash));

        // Check the digest of the piece.
        if expected_digest != digest.to_string() {
            error!(
                "piece digest mismatch: expected {}, got {}",
                expected_digest,
                digest.to_string()
            );
            return Err(Error::PieceDigestMismatch());
        }

        Ok(())
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
            info!("content length is 0");
            return Ok(Vec::new());
        }

        // If range is not None, calculate the pieces by range.
        if let Some(range) = range {
            if range.length == 0 {
                info!("range length is 0");
                return Err(Error::InvalidParameter());
            }

            let mut number = 0;
            let mut offset = 0;
            let mut pieces: Vec<metadata::Piece> = Vec::new();
            loop {
                // If offset is greater than content_length, break the loop.
                if offset >= content_length {
                    let mut piece = pieces.pop().ok_or(Error::InvalidParameter())?;
                    piece.length = piece_length + content_length - offset;
                    pieces.push(piece);
                    break;
                }

                // If offset is greater than range.start + range.length, break the loop.
                if offset < range.start + range.length {
                    break;
                }

                if offset > range.start {
                    pieces.push(metadata::Piece {
                        number: number as u32,
                        offset,
                        length: piece_length,
                        digest: "".to_string(),
                        parent_id: None,
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
                let mut piece = pieces.pop().ok_or(Error::InvalidParameter())?;
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

    // download_from_local_peer_into_async_read downloads a single piece from a local peer.
    pub async fn download_from_local_peer_into_async_read(
        &self,
        task_id: &str,
        number: u32,
    ) -> Result<impl AsyncRead> {
        // Upload the piece content.
        self.storage.upload_piece(task_id, number).await
    }

    // download_from_remote_peer downloads a single piece from a remote peer.
    pub async fn download_from_remote_peer(
        &self,
        task_id: &str,
        number: u32,
        parent: Peer,
    ) -> Result<metadata::Piece> {
        // Record the start of downloading piece.
        self.storage.download_piece_started(task_id, number).await?;

        // Create a dfdaemon client.
        let host = parent
            .host
            .clone()
            .ok_or(Error::InvalidPeer(parent.id.clone()))?;
        let dfdaemon_client =
            DfdaemonClient::new(format!("http://{}:{}", host.ip, host.port)).await?;

        // Send the interested pieces request.
        let response = dfdaemon_client
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
            Error::InvalidParameter()
        })?;

        // Get the piece content.
        let content = piece.content.ok_or_else(|| {
            error!("piece content is empty");
            if let Some(err) = self.storage.download_piece_failed(task_id, number).err() {
                error!("set piece metadata failed: {}", err)
            };
            Error::InvalidParameter()
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

        let metadata = self
            .storage
            .get_piece(task_id, number)?
            .ok_or(Error::PieceNotFound(number.to_string()))?;

        // Return reader of the piece.
        Ok(metadata)
    }

    // download_from_remote_peer_into_async_read downloads a single piece from a remote peer.
    pub async fn download_from_remote_peer_into_async_read(
        &self,
        task_id: &str,
        number: u32,
        parent: Peer,
    ) -> Result<impl AsyncRead> {
        // Download the piece from the remote peer.
        self.download_from_remote_peer(task_id, number, parent)
            .await?;

        // Return reader of the piece.
        self.storage.upload_piece(task_id, number).await
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
        header: HeaderMap,
    ) -> Result<metadata::Piece> {
        // Record the start of downloading piece.
        self.storage.download_piece_started(task_id, number).await?;

        // Add range header to the request by offset and length.
        let mut header = header.clone();
        header.insert(
            header::RANGE,
            format!("bytes={}-{}", offset, offset + length - 1)
                .parse()
                .unwrap(),
        );

        // Download the piece from the source.
        let mut response = self
            .http_client
            .get(Request {
                url: url.to_string(),
                header: header.to_owned(),
                timeout: self.config.download.piece_timeout,
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
        if !response.status_code.is_success() {
            // Record the failure of downloading piece,
            // if the status code is not OK.
            self.storage.download_piece_failed(task_id, number)?;

            let mut buffer = String::new();
            response.reader.read_to_string(&mut buffer).await?;
            error!("http error {}: {}", response.status_code, buffer.as_str());
            return Err(Error::HTTP(HTTPError {
                status_code: response.status_code,
                header: response.header,
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

        let metadata = self
            .storage
            .get_piece(task_id, number)?
            .ok_or(Error::PieceNotFound(number.to_string()))?;

        Ok(metadata)
    }

    // download_from_source_into_async_read downloads a single piece from the source.
    #[allow(clippy::too_many_arguments)]
    pub async fn download_from_source_into_async_read(
        &self,
        task_id: &str,
        number: u32,
        url: &str,
        offset: u64,
        length: u64,
        header: HeaderMap,
    ) -> Result<impl AsyncRead> {
        // Download the piece from the source.
        self.download_from_source(task_id, number, url, offset, length, header)
            .await?;

        // Return reader of the piece.
        self.storage.upload_piece(task_id, number).await
    }
}
