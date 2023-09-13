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

use crate::config;
use crate::Result;
use std::fs;
use std::path::{Path, PathBuf};
use tokio::fs::File;
use tokio::io::{self, AsyncRead, AsyncReadExt, AsyncSeekExt, SeekFrom};
use tracing::info;

// DEFAULT_DIR_NAME is the default directory name to store content.
const DEFAULT_DIR_NAME: &str = "content";

// Content is the content of a piece.
pub struct Content {
    // dir is the directory to store content.
    dir: PathBuf,
}

// Content implements the content storage.
impl Content {
    // new returns a new content.
    pub fn new(data_dir: &Path) -> Result<Content> {
        let dir = data_dir.join(config::NAME).join(DEFAULT_DIR_NAME);
        fs::create_dir_all(&dir)?;
        info!("create content directory: {:?}", dir);

        Ok(Content { dir })
    }

    // read_piece reads the piece from the content.
    pub async fn read_piece(
        &self,
        task_id: &str,
        offset: u64,
        length: u64,
    ) -> Result<impl AsyncRead> {
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
    ) -> Result<u64> {
        let mut f = File::open(self.dir.join(task_id)).await?;
        f.seek(SeekFrom::Start(offset)).await?;
        Ok(io::copy(reader, &mut f).await?)
    }
}
