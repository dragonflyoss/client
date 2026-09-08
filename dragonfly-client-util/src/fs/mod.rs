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

use dragonfly_client_core::Result;
use tokio::fs;

pub mod fd;

#[cfg(target_os = "linux")]
use tracing::warn;

/// Fallocate allocates the space for the file and fills it with zero, only on Linux.
#[allow(unused_variables)]
pub async fn fallocate(f: &fs::File, length: u64) -> Result<()> {
    // No allocation needed for zero length. Avoids potential fallocate errors.
    if length == 0 {
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        use dragonfly_client_core::Error;
        use rustix::fs::{fallocate, FallocateFlags};
        use std::os::unix::io::AsFd;
        use tokio::io;

        // Set length (potential truncation).
        f.set_len(length).await?;
        let f = f.try_clone().await?;
        tokio::task::spawn_blocking(move || {
            let fd = f.as_fd();
            let offset = 0;
            let flags = FallocateFlags::KEEP_SIZE;

            loop {
                match fallocate(fd, flags, offset, length) {
                    Ok(_) => return Ok(()),
                    Err(rustix::io::Errno::INTR) => continue,
                    Err(err)
                        if err == rustix::io::Errno::NOTSUP
                            || err == rustix::io::Errno::OPNOTSUPP =>
                    {
                        warn!("fallocate not supported, skipping preallocation");
                        return Ok(());
                    }
                    Err(err) => {
                        return Err(Error::IO(io::Error::from_raw_os_error(err.raw_os_error())))
                    }
                }
            }
        })
        .await
        .map_err(io::Error::other)??;
    }

    Ok(())
}

/// Fadvise dontneed advises the kernel to drop the cached pages of the whole
/// file, only on Linux.
#[allow(unused_variables)]
pub async fn fadvise_dontneed(f: &fs::File) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use dragonfly_client_core::Error;
        use rustix::fs::{fadvise, Advice};
        use std::os::unix::io::AsFd;
        use tokio::io;

        let f = f.try_clone().await?;
        tokio::task::spawn_blocking(move || {
            fadvise(f.as_fd(), 0, None, Advice::DontNeed)
                .map_err(|err| Error::IO(io::Error::from_raw_os_error(err.raw_os_error())))
        })
        .await
        .map_err(io::Error::other)??;
    }

    Ok(())
}

/// Fadvise sequential advises the kernel that the file will be read
/// sequentially, doubling the readahead window, only on Linux.
#[allow(unused_variables)]
pub fn fadvise_sequential(f: &std::fs::File) -> Result<()> {
    #[cfg(target_os = "linux")]
    {
        use dragonfly_client_core::Error;
        use rustix::fs::{fadvise, Advice};
        use std::os::unix::io::AsFd;

        fadvise(f.as_fd(), 0, None, Advice::Sequential)
            .map_err(|err| Error::IO(std::io::Error::from_raw_os_error(err.raw_os_error())))?;
    }

    Ok(())
}

/// Fadvise willneed initiates nonblocking readahead of the file range,
/// bypassing the sequential detection of the kernel, only on Linux.
#[allow(unused_variables)]
pub async fn fadvise_willneed(f: &std::fs::File, offset: u64, length: u64) -> Result<()> {
    // No readahead needed for zero length. Avoids reading ahead from the
    // offset to the end of file, which is the syscall behavior for zero.
    if length == 0 {
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        use dragonfly_client_core::Error;
        use rustix::fs::{fadvise, Advice};
        use std::num::NonZeroU64;
        use std::os::unix::io::AsFd;
        use tokio::io;

        let f = f.try_clone()?;
        tokio::task::spawn_blocking(move || {
            fadvise(f.as_fd(), offset, NonZeroU64::new(length), Advice::WillNeed)
                .map_err(|err| Error::IO(io::Error::from_raw_os_error(err.raw_os_error())))
        })
        .await
        .map_err(io::Error::other)??;
    }

    Ok(())
}

/// Sync file range initiates asynchronous writeback of the file range without
/// waiting for it to complete, only on Linux.
#[allow(unused_variables)]
pub async fn sync_file_range(f: &std::fs::File, offset: u64, length: u64) -> Result<()> {
    // No writeback needed for zero length. Avoids flushing from the offset
    // to the end of file, which is the syscall behavior for zero.
    if length == 0 {
        return Ok(());
    }

    #[cfg(target_os = "linux")]
    {
        use dragonfly_client_core::Error;
        use std::os::unix::io::AsRawFd;
        use tokio::io;

        let f = f.try_clone()?;
        tokio::task::spawn_blocking(move || {
            match unsafe {
                libc::sync_file_range(
                    f.as_raw_fd(),
                    offset as libc::off64_t,
                    length as libc::off64_t,
                    libc::SYNC_FILE_RANGE_WRITE,
                )
            } {
                0 => Ok(()),
                _ => Err(Error::IO(io::Error::last_os_error())),
            }
        })
        .await
        .map_err(io::Error::other)??;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::{Path, PathBuf};
    use tempfile::tempdir;

    const CONTENT: &[u8] = b"hello, world!";

    fn file(dir: &Path) -> PathBuf {
        let path = dir.join("task");
        std::fs::write(&path, CONTENT).unwrap();
        path
    }

    #[tokio::test]
    async fn fallocate_keeps_the_content_readable() {
        let test_cases = vec![0, 13, 4096];

        for length in test_cases {
            let temp_dir = tempdir().unwrap();
            let path = file(temp_dir.path());
            let f = fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .await
                .unwrap();
            fallocate(&f, length).await.unwrap();
            assert_eq!(&std::fs::read(&path).unwrap()[..CONTENT.len()], CONTENT);
        }
    }

    #[tokio::test]
    async fn fadvise_dontneed_keeps_the_content_readable() {
        let temp_dir = tempdir().unwrap();
        let path = file(temp_dir.path());

        let f = fs::File::open(&path).await.unwrap();
        fadvise_dontneed(&f).await.unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), CONTENT);
    }

    #[test]
    fn fadvise_sequential_keeps_the_content_readable() {
        let temp_dir = tempdir().unwrap();
        let path = file(temp_dir.path());

        let f = std::fs::File::open(&path).unwrap();
        fadvise_sequential(&f).unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), CONTENT);
    }

    #[tokio::test]
    async fn fadvise_willneed_accepts_any_range() {
        let temp_dir = tempdir().unwrap();
        let path = file(temp_dir.path());
        let f = std::fs::File::open(&path).unwrap();

        let test_cases = vec![(0, 13), (7, 5), (0, 0), (13, 100)];

        for (offset, length) in test_cases {
            fadvise_willneed(&f, offset, length).await.unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), CONTENT);
        }
    }

    #[tokio::test]
    async fn sync_file_range_accepts_any_range() {
        let temp_dir = tempdir().unwrap();
        let path = file(temp_dir.path());
        let f = std::fs::OpenOptions::new()
            .truncate(false)
            .write(true)
            .open(&path)
            .unwrap();

        let test_cases = vec![(0, 13), (7, 5), (0, 0), (13, 100)];

        for (offset, length) in test_cases {
            sync_file_range(&f, offset, length).await.unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), CONTENT);
        }
    }
}
