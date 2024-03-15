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

use dragonfly_api::common::v2::Range;
use dragonfly_client_core::Result;
use sha2::{Digest, Sha256};
use std::cmp::{max, min};
use std::path::{Path, PathBuf};
use tokio::fs::{self, File, OpenOptions};
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncSeekExt, BufReader, SeekFrom};
use tokio_util::io::InspectReader;
use tracing::info;

// DEFAULT_DIR_NAME is the default directory name to store content.
const DEFAULT_DIR_NAME: &str = "content";

// DEFAULT_BUFFER_SIZE is the buffer size to read and write, default is 2MB.
const DEFAULT_BUFFER_SIZE: usize = 2 * 1024 * 1024;

// Content is the content of a piece.
pub struct Content {
    // dir is the directory to store content.
    dir: PathBuf,
}

// WritePieceResponse is the response of writing a piece.
pub struct WritePieceResponse {
    // length is the length of the piece.
    pub length: u64,

    // hash is the hash of the piece.
    pub hash: String,
}

// Content implements the content storage.
impl Content {
    // new returns a new content.
    pub async fn new(dir: &Path) -> Result<Content> {
        let dir = dir.join(DEFAULT_DIR_NAME);
        fs::create_dir_all(&dir).await?;
        info!("content initialized directory: {:?}", dir);

        Ok(Content { dir })
    }

    // hard_link_or_copy_task hard links or copies the task content to the destination.
    pub async fn hard_link_or_copy_task(
        &self,
        task: super::metadata::Task,
        to: &Path,
        range: Option<Range>,
    ) -> Result<()> {
        // Copy the task content to the destination by range
        // if the range is specified.
        if let Some(range) = range {
            // If the range length is 0, no need to copy. Need to open the file to
            // ensure the file exists.
            if range.length == 0 {
                info!("range length is 0, no need to copy");
                File::create(to).await?;
                return Ok(());
            }

            self.copy_task_by_range(task.id.as_str(), to, range).await?;
            info!("copy range of task success");
            return Ok(());
        }

        // If the hard link fails,
        // copy the task content to the destination.
        if let Err(err) = self.hard_link_task(task.id.as_str(), to).await {
            info!("hard link task failed: {}", err);

            // If the task is empty, no need to copy. Need to open the file to
            // ensure the file exists.
            if task.is_empty() {
                info!("task is empty, no need to copy");
                File::create(to).await?;
                return Ok(());
            }

            fs::remove_file(to).await?;
            self.copy_task(task.id.as_str(), to).await?;
            info!("copy task success");
            return Ok(());
        }

        info!("hard link task success");
        Ok(())
    }

    // hard_link_task hard links the task content.
    pub async fn hard_link_task(&self, task_id: &str, link: &Path) -> Result<()> {
        fs::hard_link(self.dir.join(task_id), link).await?;
        Ok(())
    }

    // copy_task copies the task content to the destination.
    pub async fn copy_task(&self, task_id: &str, to: &Path) -> Result<()> {
        fs::copy(self.dir.join(task_id), to).await?;
        Ok(())
    }

    // copy_task_by_range copies the task content to the destination by range.
    pub async fn copy_task_by_range(&self, task_id: &str, to: &Path, range: Range) -> Result<()> {
        let mut from_f = File::open(self.dir.join(task_id)).await?;
        from_f.seek(SeekFrom::Start(range.start)).await?;
        let range_reader = from_f.take(range.length);

        // Use a buffer to read the range.
        let mut range_reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, range_reader);

        let mut to_f = OpenOptions::new()
            .create(true)
            .write(true)
            .open(to.as_os_str())
            .await?;

        io::copy(&mut range_reader, &mut to_f).await?;
        Ok(())
    }

    // read_task reads the task content by range.
    pub async fn read_task_by_range(&self, task_id: &str, range: Range) -> Result<impl AsyncRead> {
        let mut from_f = File::open(self.dir.join(task_id)).await?;
        from_f.seek(SeekFrom::Start(range.start)).await?;
        let range_reader = from_f.take(range.length);
        Ok(range_reader)
    }

    // delete_task deletes the task content.
    pub async fn delete_task(&self, task_id: &str) -> Result<()> {
        fs::remove_file(self.dir.join(task_id)).await?;
        Ok(())
    }

    // read_piece reads the piece from the content.
    pub async fn read_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
        range: Option<Range>,
    ) -> Result<impl AsyncRead> {
        if let Some(range) = range {
            let target_offset = max(offset, range.start);
            let target_length =
                min(offset + length - 1, range.start + range.length - 1) - target_offset + 1;

            let mut f = File::open(self.dir.join(task_id)).await?;
            f.seek(SeekFrom::Start(target_offset)).await?;
            return Ok(f.take(target_length));
        }

        let mut f = File::open(self.dir.join(task_id)).await?;
        f.seek(SeekFrom::Start(offset)).await?;
        Ok(f.take(length))
    }

    // write_piece writes the piece to the content.
    pub async fn write_piece<R: AsyncRead + Unpin + ?Sized>(
        &self,
        task_id: &str,
        offset: u64,
        reader: &mut R,
    ) -> Result<WritePieceResponse> {
        // Use a buffer to read the piece.
        let reader = BufReader::with_capacity(DEFAULT_BUFFER_SIZE, reader);

        // Sha256 is used to calculate the hash of the piece.
        let mut hasher = Sha256::new();

        // InspectReader is used to calculate the hash of the piece.
        let mut tee = InspectReader::new(reader, |bytes| hasher.update(bytes));

        // Open the file and seek to the offset.
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .open(self.dir.join(task_id))
            .await?;
        f.seek(SeekFrom::Start(offset)).await?;

        // Copy the piece to the file.
        let length = io::copy(&mut tee, &mut f).await?;

        // Calculate the hash of the piece.
        let hash = hasher.finalize();

        Ok(WritePieceResponse {
            length,
            hash: base16ct::lower::encode_string(&hash),
        })
    }
}
