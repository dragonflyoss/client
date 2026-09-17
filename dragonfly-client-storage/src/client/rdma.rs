/*
 *     Copyright 2026 The Dragonfly Authors
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

use crate::rdma::fabric::{Fabric, OpHandle, PooledBuf, TAG_RANGE_SIZE};
use crate::rdma::rendezvous::{
    read_frame, write_frame, Frame, PieceKind, PieceReady, PieceRequest, RdmaAdvertisement,
    WireCapability, ERROR_CODE_BUSY,
};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use socket2::{SockRef, TcpKeepalive};
use std::collections::VecDeque;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;
use tracing::{debug, error, instrument, Span};

/// MAX_CHUNKS caps the number of fabric messages (and thus posted receives) per piece.
pub(crate) const MAX_CHUNKS: u64 = TAG_RANGE_SIZE;

/// RDMAStreamReader exposes completed receive windows as an [`AsyncRead`]. The registered
/// receive ring stays bounded by the negotiated window, while the consumer can write and hash
/// one window concurrently with the fabric receiving the next one.
pub struct RDMAStreamReader {
    receiver: mpsc::Receiver<io::Result<PooledBuf>>,
    current: Option<PooledBuf>,
    position: usize,
    length: u64,
}

impl std::fmt::Debug for RDMAStreamReader {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RDMAStreamReader")
            .field(
                "current_window_length",
                &self.current.as_ref().map_or(0, PooledBuf::len),
            )
            .field("position", &self.position)
            .finish()
    }
}

impl RDMAStreamReader {
    /// length is the peer's advertised total, checked against scheduler metadata before writing.
    pub fn length(&self) -> u64 {
        self.length
    }

    fn new(receiver: mpsc::Receiver<io::Result<PooledBuf>>) -> Self {
        Self {
            receiver,
            current: None,
            position: 0,
            length: 0,
        }
    }

    /// next_window returns the next completed receive window, or None at end of stream. Callers
    /// hash and write straight out of registered memory, skipping the [`AsyncRead`] bounce buffer.
    pub async fn next_window(&mut self) -> io::Result<Option<ReceivedWindow>> {
        // A window already partially drained by poll_read yields only its remaining bytes, so the
        // two APIs can be mixed without losing or re-delivering data.
        if let Some(buf) = self.current.take() {
            let consumed = std::mem::take(&mut self.position);
            if consumed < buf.len() {
                return Ok(Some(ReceivedWindow { buf, consumed }));
            }
        }

        match self.receiver.recv().await {
            Some(Ok(buf)) => Ok(Some(ReceivedWindow { buf, consumed: 0 })),
            Some(Err(err)) => Err(err),
            None => Ok(None),
        }
    }
}

/// ReceivedWindow is one completed receive window still resident in the registered ring. Dropping
/// it returns the registration to the buffer pool, so the fabric cannot receive the window after
/// next until the consumer is done with this one.
pub struct ReceivedWindow {
    buf: PooledBuf,
    consumed: usize,
}

impl ReceivedWindow {
    /// bytes returns the registered bytes that no earlier [`AsyncRead`] read already consumed. The
    /// borrow is shared so that a digest and a write can read the same window on two threads.
    pub fn bytes(&self) -> &[u8] {
        // Safety: the fabric task publishes a window only after every receive completion over it
        // has been reaped, so the reader owns the registration exclusively and nothing mutates it
        // until it is dropped. The read-only accessor keeps two concurrent callers from each
        // creating a mutable borrow over the same bytes.
        unsafe { &self.buf.as_slice()[self.consumed..] }
    }
}

impl std::fmt::Debug for ReceivedWindow {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ReceivedWindow")
            .field("length", &(self.buf.len() - self.consumed))
            .finish()
    }
}

#[cfg(test)]
impl RDMAStreamReader {
    /// from_windows builds a reader over windows that are already "received", so tests can exercise
    /// the consumer side without a peer.
    pub(crate) fn from_windows(windows: Vec<PooledBuf>) -> Self {
        let (sender, receiver) = mpsc::channel(windows.len().max(1));
        for window in windows {
            sender.try_send(Ok(window)).expect("test channel capacity");
        }
        Self::new(receiver)
    }
}

impl AsyncRead for RDMAStreamReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if self
                .current
                .as_ref()
                .is_some_and(|window| self.position < window.len())
            {
                let position = self.position;
                let window = self.current.as_mut().expect("checked current window");
                let read_len = (window.len() - position).min(output.remaining());
                // Safety: the fabric task sends a window only after all receive completions
                // have been reaped, and the reader exclusively owns it until it is consumed.
                let available = unsafe { &window.as_mut_slice()[position..position + read_len] };
                output.put_slice(available);
                self.position += read_len;
                return Poll::Ready(Ok(()));
            }

            // Release an exhausted registration before waiting for the producer to acquire the
            // next one. This permits progress even when the memory budget holds one window.
            self.current = None;
            match self.receiver.poll_recv(cx) {
                Poll::Ready(Some(Ok(window))) => {
                    // Dropping the consumed window returns its registration to the pool.
                    self.current = Some(window);
                    self.position = 0;
                }
                Poll::Ready(Some(Err(err))) => return Poll::Ready(Err(err)),
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// discover asks the parent's already-advertised TCP piece endpoint for its live RDMA
/// capability and rendezvous port. Older and non-RDMA peers simply fail this optional probe.
pub async fn discover(addr: &str, timeout: std::time::Duration) -> ClientResult<RdmaAdvertisement> {
    time::timeout(timeout, async {
        let stream = TcpStream::connect(addr).await?;
        let socket = SockRef::from(&stream);
        socket.set_tcp_nodelay(true)?;
        socket.set_tcp_keepalive(
            &TcpKeepalive::new()
                .with_interval(super::DEFAULT_KEEPALIVE_INTERVAL)
                .with_time(super::DEFAULT_KEEPALIVE_TIME)
                .with_retries(super::DEFAULT_KEEPALIVE_RETRIES),
        )?;
        let (mut reader, mut writer) = stream.into_split();
        write_frame(&mut writer, &Frame::Discover).await?;
        match read_frame(&mut reader).await? {
            Frame::Capability(advertisement) if advertisement.port != 0 => Ok(advertisement),
            Frame::Capability(_) => Err(ClientError::Unsupported(
                "parent advertised an invalid rdma rendezvous port".to_string(),
            )),
            Frame::Error(err) => Err(ClientError::RdmaRejected {
                code: err.code,
                message: err.message,
            }),
            frame => Err(ClientError::Unknown(format!(
                "unexpected rdma discovery frame: {frame:?}"
            ))),
        }
    })
    .await?
}

/// RDMAClient downloads pieces over the libfabric transport: control messages ride a TCP
/// rendezvous connection to the parent's RDMA port, bulk bytes arrive as tagged fabric
/// messages into a pinned buffer. Any error must make the caller fall back to the TCP
/// piece transport; RDMA never has to succeed for a piece to complete.
#[derive(Clone)]
pub struct RDMAClient {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// fabric is the process-shared libfabric endpoint.
    fabric: Arc<Fabric>,

    /// capability is the local side of capability negotiation.
    capability: WireCapability,

    /// addr is the address of the parent's RDMA rendezvous server.
    addr: String,

    /// deadline bounds discovery, rendezvous and every receive window in one attempt.
    deadline: Option<time::Instant>,
}

/// RDMAClient implements the libfabric piece download client.
impl RDMAClient {
    /// Creates a new RDMAClient for one parent address.
    pub fn new(
        config: Arc<Config>,
        fabric: Arc<Fabric>,
        capability: WireCapability,
        addr: String,
    ) -> Self {
        Self {
            config,
            fabric,
            capability,
            addr,
            deadline: None,
        }
    }

    /// Reuses the deadline established before discovery instead of starting a new budget.
    pub fn with_deadline(mut self, deadline: time::Instant) -> Self {
        self.deadline = Some(deadline);
        self
    }

    /// fabric_failed reports whether the shared endpoint has been retired and should be
    /// recreated by the downloader before another RDMA attempt.
    pub fn fabric_failed(&self) -> bool {
        self.fabric.is_failed()
    }

    /// Downloads a piece from the parent, returning the piece content reader, offset, and
    /// digest exactly like the TCP and QUIC clients so digest verification upstream is
    /// byte-identical.
    #[instrument(skip_all, fields(parent_addr))]
    pub async fn download_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(RDMAStreamReader, u64, String)> {
        Span::current().record("parent_addr", self.addr.as_str());
        let deadline = self.deadline.unwrap_or_else(|| {
            time::Instant::now()
                + self
                    .config
                    .download
                    .piece_timeout
                    .min(self.config.storage.server.rdma.transfer_timeout)
        });
        time::timeout_at(
            deadline,
            self.handle_download(PieceKind::Piece, number, task_id, deadline),
        )
        .await
        .inspect_err(|err| {
            error!("rdma download timeout from {}: {}", self.addr, err);
        })?
    }

    /// Downloads a persistent piece from the parent.
    #[instrument(skip_all, fields(parent_addr))]
    pub async fn download_persistent_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(RDMAStreamReader, u64, String)> {
        Span::current().record("parent_addr", self.addr.as_str());
        let deadline = self.deadline.unwrap_or_else(|| {
            time::Instant::now()
                + self
                    .config
                    .download
                    .piece_timeout
                    .min(self.config.storage.server.rdma.transfer_timeout)
        });
        time::timeout_at(
            deadline,
            self.handle_download(PieceKind::PersistentPiece, number, task_id, deadline),
        )
        .await
        .inspect_err(|err| {
            error!("rdma download timeout from {}: {}", self.addr, err);
        })?
    }

    /// Downloads a persistent cache piece from the parent.
    #[instrument(skip_all, fields(parent_addr))]
    pub async fn download_persistent_cache_piece(
        &self,
        number: u32,
        task_id: &str,
    ) -> ClientResult<(RDMAStreamReader, u64, String)> {
        Span::current().record("parent_addr", self.addr.as_str());
        let deadline = self.deadline.unwrap_or_else(|| {
            time::Instant::now()
                + self
                    .config
                    .download
                    .piece_timeout
                    .min(self.config.storage.server.rdma.transfer_timeout)
        });
        time::timeout_at(
            deadline,
            self.handle_download(PieceKind::PersistentCachePiece, number, task_id, deadline),
        )
        .await
        .inspect_err(|err| {
            error!("rdma download timeout from {}: {}", self.addr, err);
        })?
    }

    /// Runs one piece transfer: rendezvous, post receives, signal readiness, await
    /// completions, and hand the landed bytes back as a reader.
    async fn handle_download(
        &self,
        kind: PieceKind,
        number: u32,
        task_id: &str,
        deadline: time::Instant,
    ) -> ClientResult<(RDMAStreamReader, u64, String)> {
        let stream = TcpStream::connect(self.addr.clone()).await?;
        let socket = SockRef::from(&stream);
        socket.set_tcp_nodelay(true)?;
        socket.set_tcp_keepalive(
            &TcpKeepalive::new()
                .with_interval(super::DEFAULT_KEEPALIVE_INTERVAL)
                .with_time(super::DEFAULT_KEEPALIVE_TIME)
                .with_retries(super::DEFAULT_KEEPALIVE_RETRIES),
        )?;
        let (mut reader, mut writer) = stream.into_split();

        let tag = self.fabric.next_tag()?;
        let configured_chunk_size = self.config.storage.server.rdma.chunk_size.as_u64();
        if configured_chunk_size == 0 {
            return Err(ClientError::InvalidParameter);
        }
        let max_inflight_chunks = self.config.storage.server.rdma.max_inflight_chunks;
        if max_inflight_chunks == 0 || u64::from(max_inflight_chunks) > MAX_CHUNKS {
            return Err(ClientError::InvalidParameter);
        }
        let chunk_size = configured_chunk_size.min(self.fabric.max_msg_size() as u64);
        write_frame(
            &mut writer,
            &Frame::Request(PieceRequest {
                kind,
                task_id: task_id.to_string(),
                piece_number: number,
                capability: self.capability.clone(),
                client_endpoint: self.fabric.local_endpoint().to_vec(),
                tag,
                chunk_size,
                max_inflight_chunks,
            }),
        )
        .await?;

        let ready = match read_frame(&mut reader).await? {
            Frame::Ready(ready) => ready,
            Frame::Error(err) => {
                return Err(ClientError::RdmaRejected {
                    code: err.code,
                    message: err.message,
                });
            }
            frame => {
                return Err(ClientError::Unknown(format!(
                    "unexpected rendezvous frame: {frame:?}"
                )));
            }
        };
        debug!(
            "rdma piece ready: offset {}, length {}, chunk size {}, inflight chunks {}",
            ready.offset, ready.length, ready.chunk_size, ready.max_inflight_chunks
        );

        if !valid_piece_digest(&ready.digest) {
            return Err(ClientError::Unsupported(
                "rdma requires a valid CRC32 piece digest; use TCP for legacy peers".to_string(),
            ));
        }
        if ready.length == 0
            || ready.chunk_size == 0
            || ready.chunk_size > chunk_size
            || ready.max_inflight_chunks == 0
            || ready.max_inflight_chunks > max_inflight_chunks
        {
            return Err(ClientError::Unknown(format!(
                "invalid rdma piece metadata: length {}, chunk size {}, inflight chunks {}",
                ready.length, ready.chunk_size, ready.max_inflight_chunks
            )));
        }
        let chunk_count = ready.length.div_ceil(ready.chunk_size);
        if chunk_count > MAX_CHUNKS {
            return Err(ClientError::Unknown(format!(
                "piece needs {chunk_count} rdma chunks, exceeding the {MAX_CHUNKS} chunk cap"
            )));
        }
        let window_length = ready.length.min(
            ready
                .chunk_size
                .saturating_mul(u64::from(ready.max_inflight_chunks)),
        );
        let window_length = usize::try_from(window_length).map_err(|_| {
            ClientError::Unknown("rdma receive window exceeds addressable memory".to_string())
        })?;
        let buf = acquire_receive_window(
            &self.fabric,
            window_length,
            deadline,
            self.config.storage.server.rdma.transfer_timeout,
        )
        .await?;
        let (window_tx, window_rx) = mpsc::channel(2);
        let fabric = self.fabric.clone();
        let transfer_timeout = self.config.storage.server.rdma.transfer_timeout;
        let result_length = ready.length;
        let result_offset = ready.offset;
        let result_digest = ready.digest.clone();

        tokio::spawn(async move {
            let transfer = receive_stream(
                fabric,
                buf,
                reader,
                writer,
                ready,
                chunk_count,
                tag,
                transfer_timeout,
                window_tx.clone(),
            );
            let result = tokio::select! {
                result = time::timeout_at(deadline, transfer) => result,
                _ = window_tx.closed() => return,
            };
            let error = match result {
                Ok(Ok(())) => return,
                Ok(Err(err)) => err,
                Err(_) => {
                    ClientError::Unknown("complete rdma piece transfer timed out".to_string())
                }
            };
            let _ = window_tx
                .send(Err(io::Error::other(error.to_string())))
                .await;
        });

        let mut reader = RDMAStreamReader::new(window_rx);
        reader.length = result_length;
        Ok((reader, result_offset, result_digest))
    }
}

/// acquire_receive_window reserves the first window's registered memory. Budget pressure is
/// capacity, not a broken fabric, so a busy budget is waited on for a bounded time before the
/// piece declines to TCP. Waiting here cannot deadlock against other transfers: this transfer
/// holds no registration yet, so it is not part of any cycle. The wait is capped by the peer's
/// own per-window patience as well as the attempt deadline, because a reservation the parent has
/// already stopped waiting for is worthless.
///
/// Local admission failures must not penalize a peer.
async fn acquire_receive_window(
    fabric: &Fabric,
    length: usize,
    deadline: time::Instant,
    transfer_timeout: std::time::Duration,
) -> ClientResult<PooledBuf> {
    match fabric.try_acquire_buffer(length) {
        Ok(Some(buffer)) => return Ok(buffer),
        Ok(None) => {}
        Err(err) => {
            return Err(ClientError::RdmaRejected {
                code: ERROR_CODE_BUSY,
                message: format!("local RDMA receive buffer unavailable: {err}"),
            });
        }
    }

    let admission_deadline = deadline.min(time::Instant::now() + transfer_timeout);
    match time::timeout_at(admission_deadline, fabric.acquire_buffer(length)).await {
        Ok(Ok(buffer)) => Ok(buffer),
        Ok(Err(err)) => Err(ClientError::RdmaRejected {
            code: ERROR_CODE_BUSY,
            message: format!("local RDMA receive buffer unavailable: {err}"),
        }),
        Err(_) => Err(ClientError::RdmaRejected {
            code: ERROR_CODE_BUSY,
            message: "local RDMA registered-memory budget is busy".to_string(),
        }),
    }
}

/// Piece storage uses a canonical decimal CRC32 digest, not an authentication token.
fn valid_piece_digest(digest: &str) -> bool {
    digest.strip_prefix("crc32:").is_some_and(|encoded| {
        encoded
            .parse::<u32>()
            .is_ok_and(|crc| encoded == crc.to_string())
    })
}

/// RECEIVE_PIPELINE_DEPTH is how many receive windows are kept posted at once.
const RECEIVE_PIPELINE_DEPTH: usize = 2;

/// PostedWindow is a receive window whose chunks are posted to the fabric and named in a
/// RecvPosted frame, but whose completions have not been reaped yet.
struct PostedWindow {
    /// buf is the registered memory the NIC writes into.
    buf: PooledBuf,

    /// ops pairs each posted chunk with the length it must complete with.
    ops: Vec<(usize, OpHandle)>,

    /// chunk_count is how many chunks of the piece this window covers.
    chunk_count: u32,
}

/// window_buf_length returns the byte length of the receive window that starts at `start_chunk`.
fn window_buf_length(ready: &PieceReady, start_chunk: u64) -> ClientResult<usize> {
    let length = ready
        .length
        .saturating_sub(start_chunk.saturating_mul(ready.chunk_size))
        .min(
            ready
                .chunk_size
                .saturating_mul(u64::from(ready.max_inflight_chunks)),
        );
    usize::try_from(length).map_err(|_| ClientError::InvalidParameter)
}

#[allow(clippy::too_many_arguments)]
async fn receive_stream(
    fabric: Arc<Fabric>,
    buf: PooledBuf,
    mut reader: OwnedReadHalf,
    mut writer: OwnedWriteHalf,
    ready: PieceReady,
    chunk_count: u64,
    tag: u64,
    transfer_timeout: std::time::Duration,
    window_tx: mpsc::Sender<io::Result<PooledBuf>>,
) -> ClientResult<()> {
    let control = read_frame(&mut reader);
    tokio::pin!(control);
    let mut server_done = false;

    // The parent may not send a window until the RecvPosted frame naming it arrives. Posting one
    // window at a time therefore leaves the fabric idle for a control-plane round trip between
    // every window, and defeats the parent's two-window send ring: it has the next window staged
    // but nowhere to put it. Keeping a second window posted means the parent can start sending it
    // the moment the first is on the wire.
    let mut posted: VecDeque<PostedWindow> = VecDeque::with_capacity(RECEIVE_PIPELINE_DEPTH);
    let mut next_buf = Some(buf);
    let mut posted_chunk = 0u64;
    let mut drained_chunk = 0u64;

    while drained_chunk < chunk_count {
        // Post as far ahead as the pipeline depth and the registration budget allow.
        while posted.len() < RECEIVE_PIPELINE_DEPTH && posted_chunk < chunk_count {
            let window_buf = match next_buf.take() {
                Some(buf) => buf,
                None => {
                    let length = window_buf_length(&ready, posted_chunk)?;
                    match fabric.try_acquire_buffer(length)? {
                        Some(buf) => buf,
                        // The budget has nothing spare. Carry on with whatever is already
                        // posted rather than blocking for memory while holding some, which is
                        // how concurrent transfers deadlock each other.
                        None if !posted.is_empty() => break,
                        // Nothing is posted, so there is no progress to make without a window
                        // and no registration held that another transfer could be waiting on.
                        None => fabric.acquire_buffer(length).await?,
                    }
                }
            };
            let window_count =
                (chunk_count - posted_chunk).min(ready.max_inflight_chunks as u64) as u32;
            let window_piece_offset = posted_chunk * ready.chunk_size;
            let mut window_length = 0usize;
            let mut ops = Vec::with_capacity(window_count as usize);

            for chunk in posted_chunk..posted_chunk + u64::from(window_count) {
                let piece_offset = chunk * ready.chunk_size;
                let local_offset = usize::try_from(piece_offset - window_piece_offset)
                    .map_err(|_| ClientError::InvalidParameter)?;
                let len = ready.chunk_size.min(ready.length - piece_offset);
                let len = usize::try_from(len).map_err(|_| ClientError::InvalidParameter)?;
                window_length = window_length
                    .checked_add(len)
                    .ok_or(ClientError::InvalidParameter)?;
                ops.push((
                    len,
                    fabric
                        .post_recv(window_buf.buffer(), local_offset, len, tag + chunk)
                        .await?,
                ));
            }

            write_frame(
                &mut writer,
                &Frame::RecvPosted {
                    start_chunk: posted_chunk,
                    chunk_count: window_count,
                },
            )
            .await?;

            debug_assert_eq!(window_length, window_buf.len());
            posted.push_back(PostedWindow {
                buf: window_buf,
                ops,
                chunk_count: window_count,
            });
            posted_chunk += u64::from(window_count);
        }

        let window = posted.pop_front().expect("posted receive window");
        for (expected_len, op) in window.ops {
            let wait = fabric.wait(op, transfer_timeout);
            tokio::pin!(wait);
            let len = if server_done {
                wait.await?
            } else {
                tokio::select! {
                    result = &mut wait => result?,
                    frame = &mut control => {
                        match frame {
                            Ok(Frame::Error(err)) => {
                                return Err(ClientError::Unknown(format!(
                                    "rdma transfer failed on parent: {}",
                                    err.message
                                )));
                            }
                            // The parent sends Done once every chunk has been handed to the
                            // fabric, which it can only reach after the last RecvPosted. Nothing
                            // orders that frame against the remaining local completions, so Done
                            // is legitimate here whenever every window has been posted.
                            Ok(Frame::Done) if posted_chunk == chunk_count => {
                                server_done = true;
                                wait.await?
                            }
                            Ok(frame) => {
                                return Err(ClientError::Unknown(format!(
                                    "unexpected rendezvous frame during transfer: {frame:?}"
                                )));
                            }
                            Err(err) => return Err(err),
                        }
                    }
                }
            };
            if len != expected_len {
                return Err(ClientError::Unknown(format!(
                    "rdma chunk length mismatch: expected {expected_len}, got {len}"
                )));
            }
        }

        drained_chunk += u64::from(window.chunk_count);
        // The storage writer stops after the expected byte count. Do not publish the final
        // bytes until the control path confirms success, otherwise it could mark a piece
        // finished before discovering a missing Done or a terminal server error.
        if drained_chunk == chunk_count && !server_done {
            match time::timeout(transfer_timeout, &mut control).await?? {
                Frame::Done => server_done = true,
                Frame::Error(err) => {
                    return Err(ClientError::Unknown(format!(
                        "rdma transfer failed on parent: {}",
                        err.message
                    )))
                }
                frame => {
                    return Err(ClientError::Unknown(format!(
                        "unexpected final rendezvous frame: {frame:?}"
                    )))
                }
            }
        }
        if window_tx.send(Ok(window.buf)).await.is_err() {
            return Err(ClientError::Unknown(
                "rdma stream consumer closed early".to_string(),
            ));
        }
    }

    if !server_done {
        match time::timeout(transfer_timeout, &mut control).await? {
            Ok(Frame::Done) => {}
            Ok(Frame::Error(err)) => {
                return Err(ClientError::Unknown(format!(
                    "rdma transfer failed on parent: {}",
                    err.message
                )));
            }
            Ok(frame) => {
                return Err(ClientError::Unknown(format!(
                    "unexpected rendezvous frame: {frame:?}"
                )));
            }
            Err(err) => return Err(err),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rdma::fabric::Fabric;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn local_receive_memory_pressure_declines_after_bounded_admission() {
        // The shared budget charges each allocation in 64 KiB units.
        let fabric = Fabric::new(None, None, 64 * 1024, true).unwrap();
        let held = fabric.acquire_buffer(4096).await.unwrap();

        // A budget that stays busy declines to TCP once the admission bound elapses, rather
        // than spending the whole piece attempt waiting for memory.
        let deadline = time::Instant::now() + std::time::Duration::from_millis(200);
        assert!(matches!(
            acquire_receive_window(&fabric, 4096, deadline, std::time::Duration::from_secs(30))
                .await,
            Err(ClientError::RdmaRejected {
                code: ERROR_CODE_BUSY,
                ..
            })
        ));

        drop(held);
        assert!(acquire_receive_window(
            &fabric,
            4096,
            time::Instant::now() + std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(30),
        )
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn first_receive_window_waits_for_capacity_within_the_deadline() {
        let fabric = Fabric::new(None, None, 64 * 1024, true).unwrap();
        let held = fabric.acquire_buffer(4096).await.unwrap();
        let releaser = tokio::spawn(async move {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            drop(held);
        });

        // Budget pressure is capacity, not a broken fabric: room that appears inside the
        // deadline must be used instead of failing the piece over to TCP.
        let buffer = acquire_receive_window(
            &fabric,
            4096,
            time::Instant::now() + std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(30),
        )
        .await;
        assert!(buffer.is_ok());
        releaser.await.unwrap();
    }

    #[test]
    fn requires_a_canonical_crc32_digest() {
        for digest in [
            "",
            "crc32:",
            "crc32:abc",
            "crc32:4294967296",
            "crc32:+1",
            "crc32:01",
            "sha256:123",
        ] {
            assert!(!valid_piece_digest(digest), "{digest}");
        }
        assert!(valid_piece_digest("crc32:0"));
        assert!(valid_piece_digest("crc32:4294967295"));
    }

    /// windows fills freshly registered buffers with the given payloads.
    async fn windows(fabric: &Fabric, payloads: &[&[u8]]) -> Vec<PooledBuf> {
        let mut windows = Vec::new();
        for payload in payloads {
            let mut window = fabric.acquire_buffer(payload.len()).await.unwrap();
            // Safety: this lease has not been posted.
            unsafe { window.as_mut_slice() }.copy_from_slice(payload);
            windows.push(window);
        }
        windows
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn next_window_yields_every_received_window_then_ends() {
        let fabric = Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint");
        let mut reader =
            RDMAStreamReader::from_windows(windows(&fabric, &[b"first", b"second"]).await);

        let mut received = Vec::new();
        while let Some(window) = reader.next_window().await.unwrap() {
            received.push(window.bytes().to_vec());
        }

        assert_eq!(received, vec![b"first".to_vec(), b"second".to_vec()]);
    }

    /// The two consumer APIs must agree on a cursor, so a window that poll_read left half-drained
    /// hands back only its remaining bytes rather than repeating or dropping any.
    #[tokio::test(flavor = "multi_thread")]
    async fn next_window_returns_only_the_bytes_async_read_left_behind() {
        let fabric = Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint");
        let mut reader =
            RDMAStreamReader::from_windows(windows(&fabric, &[b"abcdef", b"ghi"]).await);

        let mut prefix = [0u8; 2];
        reader.read_exact(&mut prefix).await.unwrap();
        assert_eq!(&prefix, b"ab");

        let mut remainder = Vec::new();
        while let Some(window) = reader.next_window().await.unwrap() {
            remainder.extend_from_slice(window.bytes());
        }

        assert_eq!(remainder, b"cdefghi".to_vec());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn next_window_skips_a_window_async_read_fully_drained() {
        let fabric = Fabric::new(None, None, 1024 * 1024, true).expect("libfabric endpoint");
        let mut reader = RDMAStreamReader::from_windows(windows(&fabric, &[b"ab", b"cd"]).await);

        let mut prefix = [0u8; 2];
        reader.read_exact(&mut prefix).await.unwrap();
        assert_eq!(&prefix, b"ab");

        let window = reader.next_window().await.unwrap().expect("second window");
        assert_eq!(window.bytes(), b"cd");
        assert!(reader.next_window().await.unwrap().is_none());
    }
}
