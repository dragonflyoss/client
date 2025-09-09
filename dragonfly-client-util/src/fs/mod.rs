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

/// fallocate allocates the space for the file and fills it with zero, only on Linux.
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
        let fd = f.as_fd();
        let offset = 0;
        let flags = FallocateFlags::KEEP_SIZE;

        loop {
            match fallocate(fd, flags, offset, length) {
                Ok(_) => return Ok(()),
                Err(rustix::io::Errno::INTR) => continue,
                Err(err)
                    if err == rustix::io::Errno::NOTSUP || err == rustix::io::Errno::OPNOTSUP =>
                {
                    log::warn!("fallocate not supported, skipping preallocation");
                    return Ok(());
                }
                Err(err) => {
                    return Err(Error::IO(io::Error::from_raw_os_error(err.raw_os_error())))
                }
            }
        }
    }

    #[cfg(not(target_os = "linux"))]
    Ok(())
}
