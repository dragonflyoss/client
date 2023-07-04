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
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};
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
    pub fn new(data_dir: &Path) -> super::Result<Content> {
        let dir = data_dir.join(config::NAME).join(DEFAULT_DIR_NAME);
        fs::create_dir_all(&dir)?;
        info!("create content directory: {:?}", dir);

        Ok(Content { dir })
    }

    // read_piece reads the piece from the content.
    pub fn read_piece(&self, task_id: &str, offset: u64, length: u64) -> super::Result<Vec<u8>> {
        let mut f = fs::File::open(self.dir.join(task_id))?;
        f.seek(io::SeekFrom::Start(offset))?;

        let mut buf = vec![0; length as usize];
        f.read_exact(&mut buf)?;
        Ok(buf)
    }

    // write_piece writes the piece to the content.
    pub fn write_piece(&self, task_id: &str, offset: u64, data: &[u8]) -> super::Result<()> {
        let mut f = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(self.dir.join(task_id))?;
        f.seek(io::SeekFrom::Start(offset))?;
        f.write_all(data)?;
        f.flush()?;
        Ok(())
    }
}
