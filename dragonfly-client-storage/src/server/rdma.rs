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

use crate::client::rdma::MAX_CHUNKS;
use crate::content::MappedPiece;
use crate::rdma::fabric::Fabric;
use crate::rdma::rendezvous::{
    read_frame, write_frame, CapabilityRegistry, Frame, PieceKind, PieceReady, PieceRequest,
    RdmaAdvertisement, RendezvousError, WireCapability, ERROR_CODE_BUSY, ERROR_CODE_INCOMPATIBLE,
    ERROR_CODE_INTERNAL, ERROR_CODE_NOT_FOUND, ERROR_CODE_TOO_LARGE,
};
use crate::Storage;
use dragonfly_client_config::dfdaemon::{Config, RdmaProvider};
use dragonfly_client_core::{Error as ClientError, Result as ClientResult};
use dragonfly_client_metric::{
    collect_upload_piece_failure_metrics, collect_upload_piece_finished_metrics,
    collect_upload_piece_started_metrics, collect_upload_piece_traffic_metrics,
};
use dragonfly_client_util::{id_generator::IDGenerator, shutdown};
use leaky_bucket::RateLimiter;
use socket2::{Domain, Protocol, Socket, TcpKeepalive, Type};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::{
    tcp::{OwnedReadHalf, OwnedWriteHalf},
    TcpListener, TcpStream,
};
use tokio::sync::{mpsc, Semaphore};
use tokio::time;
use tracing::{debug, error, info, instrument, warn, Span};

/// RDMAServer serves piece content over the libfabric transport. It accepts rendezvous
/// connections on a TCP port, negotiates fabric compatibility fail-closed, and pushes bulk
/// piece bytes as tagged fabric messages. The TCP piece server remains the mandatory
/// fallback; this server failing to start must never take the daemon down.
pub struct RDMAServer {
    /// config is the configuration of the dfdaemon.
    config: Arc<Config>,

    /// addr is the rendezvous listen address.
    addr: SocketAddr,

    /// id_generator generates host ids for tracing spans.
    id_generator: Arc<IDGenerator>,

    /// storage is the local storage.
    storage: Arc<Storage>,

    /// upload_bandwidth_limiter limits upload bandwidth in bytes per second.
    upload_bandwidth_limiter: Arc<RateLimiter>,

    /// shutdown is used to shutdown the RDMA server.
    shutdown: shutdown::Shutdown,

    /// _shutdown_complete is used to notify the RDMA server is shutdown.
    _shutdown_complete: mpsc::UnboundedSender<()>,

    /// capability_registry exposes readiness through the normal TCP piece server.
    capability_registry: Option<CapabilityRegistry>,
}

/// PublishedCapability clears a registry entry when its listener exits on any path.
struct PublishedCapability(CapabilityRegistry);

impl Drop for PublishedCapability {
    fn drop(&mut self) {
        self.0.clear();
    }
}

/// RDMAServer implements the rendezvous accept loop over a shared fabric endpoint.
impl RDMAServer {
    /// Creates a new RDMAServer.
    pub fn new(
        config: Arc<Config>,
        addr: SocketAddr,
        id_generator: Arc<IDGenerator>,
        storage: Arc<Storage>,
        upload_bandwidth_limiter: Arc<RateLimiter>,
        shutdown: shutdown::Shutdown,
        shutdown_complete_tx: mpsc::UnboundedSender<()>,
    ) -> Self {
        Self {
            config,
            addr,
            id_generator,
            storage,
            upload_bandwidth_limiter,
            shutdown,
            _shutdown_complete: shutdown_complete_tx,
            capability_registry: None,
        }
    }

    /// with_capability_registry publishes the listener only after fabric setup and bind succeed.
    pub fn with_capability_registry(mut self, registry: CapabilityRegistry) -> Self {
        self.capability_registry = Some(registry);
        self
    }

    /// Starts the storage RDMA server. Initialization failures (no usable fabric device,
    /// missing fabric tag) disable the server but keep the daemon running: peers simply use
    /// the TCP piece server.
    pub async fn run(&mut self) -> ClientResult<()> {
        let rdma_config = &self.config.storage.server.rdma;

        let Some(fabric_tag) = rdma_config
            .fabric_tag
            .as_deref()
            .filter(|tag| !tag.is_empty())
        else {
            error!(
                "rdma server disabled: storage.server.rdma.fabricTag is required so peers \
                 only attempt rdma within one reachability domain"
            );
            self.shutdown.recv().await;
            return Ok(());
        };

        let provider = match rdma_config.provider {
            RdmaProvider::Auto => None,
            provider => Some(provider.to_string()),
        };
        let fabric = match Fabric::new(
            provider.as_deref(),
            rdma_config.device.as_deref(),
            rdma_config.max_registered_bytes.as_u64(),
            rdma_config.allow_software_provider,
        ) {
            Ok(fabric) => Arc::new(fabric),
            Err(err) => {
                error!(
                    "rdma server disabled, failed to open fabric endpoint: {}",
                    err
                );
                self.shutdown.recv().await;
                return Ok(());
            }
        };

        let handler = RDMAServerHandler {
            id_generator: self.id_generator.clone(),
            storage: self.storage.clone(),
            upload_bandwidth_limiter: self.upload_bandwidth_limiter.clone(),
            capability: WireCapability {
                provider: fabric.provider().to_string(),
                fabric_tag: fabric_tag.to_string(),
            },
            fabric,
            chunk_size: rdma_config.chunk_size.as_u64(),
            max_inflight_chunks: rdma_config.max_inflight_chunks,
            max_registered_bytes: rdma_config.max_registered_bytes.as_u64(),
            transfer_timeout: rdma_config.transfer_timeout,
            piece_timeout: self.config.download.piece_timeout,
            mmap_content: rdma_config.mmap_content,
        };
        let handler = Arc::new(handler);
        let transfer_admission = Arc::new(Semaphore::new(
            rdma_config.max_concurrent_transfers as usize,
        ));

        let socket = Socket::new(
            Domain::for_address(self.addr),
            Type::STREAM,
            Some(Protocol::TCP),
        )?;
        socket.set_tcp_nodelay(true)?;
        socket.set_nonblocking(true)?;
        socket.set_tcp_keepalive(
            &TcpKeepalive::new()
                .with_interval(super::DEFAULT_KEEPALIVE_INTERVAL)
                .with_time(super::DEFAULT_KEEPALIVE_TIME)
                .with_retries(super::DEFAULT_KEEPALIVE_RETRIES),
        )?;
        socket.bind(&self.addr.into())?;
        socket.listen(1024)?;
        let std_listener: std::net::TcpListener = socket.into();
        let listener = TcpListener::from_std(std_listener).inspect_err(|err| {
            error!("failed to bind rdma rendezvous server: {}", err);
        })?;
        info!(
            "storage rdma server listening on {}, provider {}",
            self.addr, handler.capability.provider
        );
        let _published_capability = self.capability_registry.as_ref().map(|registry| {
            registry.publish(RdmaAdvertisement {
                capability: handler.capability.clone(),
                port: self.addr.port(),
            });
            PublishedCapability(registry.clone())
        });

        loop {
            tokio::select! {
                tcp_accepted = listener.accept() => {
                    let (tcp, remote_address) = tcp_accepted?;
                    debug!("accepted rdma rendezvous connection from {}", remote_address);

                    let Ok(admission) = transfer_admission.clone().try_acquire_owned() else {
                        debug!(
                            "rdma rendezvous admission full, rejecting connection from {}",
                            remote_address
                        );
                        // Dropping the socket here would surface on the client as a connection
                        // reset, which is indistinguishable from a parent whose fabric is broken.
                        // Saying "busy" instead lets the client fall back to TCP for this piece
                        // and keep treating the parent as RDMA-capable.
                        let reject_timeout = rdma_config.transfer_timeout;
                        tokio::spawn(async move {
                            let (_, mut writer) = tcp.into_split();
                            let _ = time::timeout(
                                reject_timeout,
                                write_frame(
                                    &mut writer,
                                    &Frame::Error(RendezvousError {
                                        code: ERROR_CODE_BUSY,
                                        message: "rdma transfer admission is full".to_string(),
                                    }),
                                ),
                            )
                            .await;
                        });
                        continue;
                    };
                    let handler = handler.clone();
                    tokio::spawn(async move {
                        let _admission = admission;
                        if let Err(err) = handler.handle(tcp, remote_address.to_string()).await {
                           error!("failed to serve rdma connection from {}: {}", remote_address, err);
                        }
                    });
                },
                _ = self.shutdown.recv() => {
                    info!("rdma server shutting down");
                    break;
                }
            }
        }

        Ok(())
    }
}

/// RDMAServerHandler handles rendezvous connections and fabric transfers.
struct RDMAServerHandler {
    /// id_generator generates host ids for tracing spans.
    id_generator: Arc<IDGenerator>,

    /// storage is the local storage.
    storage: Arc<Storage>,

    /// upload_bandwidth_limiter limits upload bandwidth in bytes per second.
    upload_bandwidth_limiter: Arc<RateLimiter>,

    /// capability is the local side of capability negotiation.
    capability: WireCapability,

    /// fabric is the shared libfabric endpoint.
    fabric: Arc<Fabric>,

    /// chunk_size is the server's preferred maximum tagged-message size.
    chunk_size: u64,

    /// max_inflight_chunks bounds posted operations and registered staging memory per transfer.
    max_inflight_chunks: u32,

    /// max_registered_bytes is the fabric-wide registration budget. The sender uses a
    /// double-buffered ring only when one transfer's ring fits that budget.
    max_registered_bytes: u64,

    /// transfer_timeout bounds each fabric operation and rendezvous wait.
    transfer_timeout: std::time::Duration,

    /// piece_timeout bounds the complete server-side operation, including storage staging and
    /// registered-memory admission.
    piece_timeout: std::time::Duration,

    /// mmap_content fills the send ring from a memory-mapped content file when possible.
    mmap_content: bool,
}

/// RDMAServerHandler implements the per-connection transfer flow.
impl RDMAServerHandler {
    /// Handles one rendezvous connection: negotiate, stage bounded windows into registered
    /// memory, wait for the client's receives, and send the bytes over the fabric.
    #[instrument(skip_all, fields(host_id, remote_address, task_id, piece_id))]
    async fn handle(&self, stream: TcpStream, remote_address: String) -> ClientResult<()> {
        let (mut reader, mut writer) = stream.into_split();
        let request = match time::timeout(self.transfer_timeout, read_frame(&mut reader)).await? {
            Ok(Frame::Request(request)) => request,
            Ok(frame) => {
                return Err(ClientError::Unknown(format!(
                    "unexpected rendezvous frame: {frame:?}"
                )));
            }
            Err(err) => return Err(err),
        };

        Span::current().record("host_id", self.id_generator.host_id());
        Span::current().record("remote_address", remote_address.as_str());
        Span::current().record("task_id", request.task_id.as_str());
        Span::current().record(
            "piece_id",
            self.storage
                .piece_id(&request.task_id, request.piece_number)
                .as_str(),
        );

        if let Err(reason) = self.capability.compatible(&request.capability) {
            return self
                .abort(&mut writer, ERROR_CODE_INCOMPATIBLE, reason)
                .await;
        }

        collect_upload_piece_started_metrics();
        info!("start upload piece content over rdma");
        match time::timeout(
            self.piece_timeout,
            self.handle_piece(&request, &mut reader, &mut writer),
        )
        .await
        {
            Ok(Ok(length)) => {
                collect_upload_piece_finished_metrics();
                collect_upload_piece_traffic_metrics(length);
                Ok(())
            }
            Ok(Err(err)) => {
                collect_upload_piece_failure_metrics();
                Err(err)
            }
            Err(err) => {
                collect_upload_piece_failure_metrics();
                let message = format!(
                    "rdma piece transfer timed out after {:?}",
                    self.piece_timeout
                );
                let _ = self.abort(&mut writer, ERROR_CODE_INTERNAL, message).await;
                Err(err.into())
            }
        }
    }

    /// Serves one piece over the fabric, returning the piece length for traffic metrics.
    async fn handle_piece(
        &self,
        request: &PieceRequest,
        reader: &mut OwnedReadHalf,
        writer: &mut OwnedWriteHalf,
    ) -> ClientResult<u64> {
        let piece_id = self
            .storage
            .piece_id(&request.task_id, request.piece_number);

        // Fetch the piece metadata for the requested namespace.
        let piece = match request.kind {
            PieceKind::Piece => self.storage.get_piece(&piece_id),
            PieceKind::PersistentPiece => self.storage.get_persistent_piece(&piece_id),
            PieceKind::PersistentCachePiece => self.storage.get_persistent_cache_piece(&piece_id),
        };
        let piece = match piece {
            Ok(Some(piece)) => piece,
            Ok(None) => {
                self.abort(
                    writer,
                    ERROR_CODE_NOT_FOUND,
                    format!("piece {piece_id} not found"),
                )
                .await?;
                return Err(ClientError::PieceNotFound(piece_id));
            }
            Err(err) => {
                self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                    .await?;
                return Err(err);
            }
        };

        let chunk_size = request
            .chunk_size
            .min(self.chunk_size)
            .min(self.fabric.max_msg_size() as u64);
        let max_inflight_chunks = request.max_inflight_chunks.min(self.max_inflight_chunks);
        if piece.length == 0
            || chunk_size == 0
            || max_inflight_chunks == 0
            || u64::from(max_inflight_chunks) > MAX_CHUNKS
        {
            self.abort(
                writer,
                ERROR_CODE_INTERNAL,
                format!(
                    "piece {} has invalid transfer parameters: length {}, chunk size {}, \
                     inflight chunks {}",
                    piece_id, piece.length, chunk_size, max_inflight_chunks
                ),
            )
            .await?;
            return Err(ClientError::Unknown(
                "invalid rdma transfer parameters".to_string(),
            ));
        }
        let chunk_count = piece.length.div_ceil(chunk_size);
        if chunk_count > MAX_CHUNKS {
            self.abort(
                writer,
                ERROR_CODE_TOO_LARGE,
                format!("piece needs {chunk_count} chunks, cap is {MAX_CHUNKS}"),
            )
            .await?;
            return Err(ClientError::Unknown("piece too large for rdma".to_string()));
        }
        if request
            .tag
            .checked_add(chunk_count.saturating_sub(1))
            .is_none()
        {
            self.abort(
                writer,
                ERROR_CODE_INTERNAL,
                "rdma transfer tag range wraps around".to_string(),
            )
            .await?;
            return Err(ClientError::Unknown(
                "rdma transfer tag range wraps around".to_string(),
            ));
        }
        let piece_length = match usize::try_from(piece.length) {
            Ok(length) => length,
            Err(_) => {
                self.abort(
                    writer,
                    ERROR_CODE_TOO_LARGE,
                    "piece exceeds addressable memory".to_string(),
                )
                .await?;
                return Err(ClientError::Unknown(
                    "piece exceeds addressable memory".to_string(),
                ));
            }
        };

        // Resolve the downloader's fabric address before consuming upload-bandwidth tokens or
        // promising readiness. Invalid provider addresses fail without throttling legitimate
        // transfers.
        let dest = match self.fabric.resolve(&request.client_endpoint) {
            Ok(dest) => dest,
            Err(err) => {
                self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                    .await?;
                return Err(err);
            }
        };

        // Acquire the upload bandwidth limiter, matching the TCP server.
        self.upload_bandwidth_limiter.acquire(piece_length).await;

        let mut source = match self.open_piece_source(request, &piece_id).await {
            Ok(source) => source,
            Err(err) => {
                self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                    .await?;
                return Err(err);
            }
        };

        // Use a two-window registered ring when the piece spans multiple windows. While the NIC
        // sends one half, the storage path fills the other. A one-window piece still allocates
        // only its logical length.
        let window_capacity = piece
            .length
            .min(chunk_size.saturating_mul(u64::from(max_inflight_chunks)));
        let ring_windows = if piece.length > window_capacity
            && window_capacity.saturating_mul(2) <= self.max_registered_bytes
        {
            2
        } else {
            1
        };
        let staging_length = piece
            .length
            .min(window_capacity.saturating_mul(ring_windows));
        let window_capacity = usize::try_from(window_capacity).map_err(|_| {
            ClientError::Unknown("rdma staging window exceeds addressable memory".to_string())
        })?;
        let staging_length = usize::try_from(staging_length).map_err(|_| {
            ClientError::Unknown("rdma staging ring exceeds addressable memory".to_string())
        })?;
        // The registration budget is shared across concurrent transfers, so this can block behind
        // peers rather than return. Without a bound the task would sit here holding an admission
        // slot until the client's own timeout fired, which turns budget pressure into a slow
        // shrink of the server's effective concurrency.
        let acquired = time::timeout(
            self.transfer_timeout,
            self.fabric.acquire_buffer(staging_length),
        )
        .await;
        let mut buf = match acquired {
            Ok(Ok(buf)) => buf,
            Ok(Err(err)) => {
                self.abort(writer, ERROR_CODE_TOO_LARGE, err.to_string())
                    .await?;
                return Err(err);
            }
            Err(_) => {
                let err = ClientError::Unknown(format!(
                    "rdma registration budget unavailable after {:?}",
                    self.transfer_timeout
                ));
                self.abort(writer, ERROR_CODE_BUSY, err.to_string()).await?;
                return Err(err);
            }
        };

        let first_window_count = chunk_count.min(u64::from(max_inflight_chunks)) as u32;
        let first_window_end = piece
            .length
            .min(u64::from(first_window_count).saturating_mul(chunk_size));
        let first_window_length = usize::try_from(first_window_end).map_err(|_| {
            ClientError::Unknown("rdma staging window exceeds addressable memory".to_string())
        })?;
        // Safety: no fabric operation has been posted over the staging ring.
        let first_window = unsafe { &mut buf.as_mut_slice()[..first_window_length] };
        if let Err(err) = source.fill(0, first_window).await {
            self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                .await?;
            return Err(err);
        }

        write_frame(
            writer,
            &Frame::Ready(PieceReady {
                offset: piece.offset,
                length: piece.length,
                digest: piece.digest.clone(),
                server_endpoint: self.fabric.local_endpoint().to_vec(),
                chunk_size,
                max_inflight_chunks,
            }),
        )
        .await?;

        let mut start_chunk = 0;
        let mut window_index = 0usize;
        while start_chunk < chunk_count {
            let window_count =
                (chunk_count - start_chunk).min(u64::from(max_inflight_chunks)) as u32;
            match time::timeout(self.transfer_timeout, read_frame(reader)).await? {
                Ok(Frame::RecvPosted {
                    start_chunk: posted_start,
                    chunk_count: posted_count,
                }) if posted_start == start_chunk && posted_count == window_count => {}
                Ok(frame) => {
                    let message =
                        format!("invalid rdma receive window at chunk {start_chunk}: {frame:?}");
                    self.abort(writer, ERROR_CODE_INTERNAL, message.clone())
                        .await?;
                    return Err(ClientError::Unknown(message));
                }
                Err(err) => return Err(err),
            }

            let buffer_offset = (window_index % ring_windows as usize) * window_capacity;
            let send_buffer = buf.buffer().clone();
            let send_window = async {
                let mut ops = Vec::with_capacity(window_count as usize);
                for chunk in start_chunk..start_chunk + u64::from(window_count) {
                    let piece_offset = chunk * chunk_size;
                    let offset_in_window = piece_offset - start_chunk * chunk_size;
                    let len = chunk_size.min(piece.length - piece_offset);
                    ops.push(
                        self.fabric
                            .post_send(
                                &send_buffer,
                                buffer_offset + offset_in_window as usize,
                                len as usize,
                                request.tag + chunk,
                                dest,
                            )
                            .await?,
                    );
                }
                for op in ops {
                    self.fabric.wait(op, self.transfer_timeout).await?;
                }
                ClientResult::Ok(())
            };

            let next_start_chunk = start_chunk + u64::from(window_count);
            if next_start_chunk < chunk_count {
                let next_window_count =
                    (chunk_count - next_start_chunk).min(u64::from(max_inflight_chunks)) as u32;
                let next_window_offset =
                    ((window_index + 1) % ring_windows as usize) * window_capacity;
                let next_piece_offset = next_start_chunk * chunk_size;
                let next_piece_end = piece.length.min(
                    (next_start_chunk + u64::from(next_window_count)).saturating_mul(chunk_size),
                );
                let next_window_length = usize::try_from(next_piece_end - next_piece_offset)
                    .map_err(|_| {
                        ClientError::Unknown(
                            "rdma staging window exceeds addressable memory".to_string(),
                        )
                    })?;
                let next_piece_offset = usize::try_from(next_piece_offset).map_err(|_| {
                    ClientError::Unknown(
                        "rdma staging window exceeds addressable memory".to_string(),
                    )
                })?;
                if ring_windows == 2 {
                    // Safety: this half of the ring is disjoint from the window currently
                    // visible to the provider. Its previous send, if any, completed two
                    // iterations earlier.
                    let next_window = unsafe {
                        buf.as_mut_range(
                            next_window_offset..next_window_offset + next_window_length,
                        )
                    };
                    let read_window = source.fill(next_piece_offset, next_window);
                    if let Err(err) = tokio::try_join!(send_window, read_window) {
                        self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                            .await?;
                        return Err(err);
                    }
                } else {
                    // With only one window of registration budget, wait for its sends before
                    // safely refilling the same prefix.
                    if let Err(err) = send_window.await {
                        self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                            .await?;
                        return Err(err);
                    }
                    // Safety: every send over the single window completed above.
                    let next_window = unsafe { &mut buf.as_mut_slice()[..next_window_length] };
                    if let Err(err) = source.fill(next_piece_offset, next_window).await {
                        self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                            .await?;
                        return Err(err);
                    }
                }
            } else if let Err(err) = send_window.await {
                self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                    .await?;
                return Err(err);
            }

            start_chunk = next_start_chunk;
            window_index += 1;
        }

        write_frame(writer, &Frame::Done).await?;
        debug!("finished uploading piece content over rdma");
        Ok(piece.length)
    }

    /// open_piece_source prefers a content mmap when configured, otherwise streams through the
    /// existing upload readers. Cache-resident pieces always use the reader path.
    async fn open_piece_source(
        &self,
        request: &PieceRequest,
        piece_id: &str,
    ) -> ClientResult<PieceSource> {
        if self.mmap_content {
            match self
                .storage
                .map_upload_piece(piece_id, &request.task_id, request.kind)
                .await
            {
                Ok(mapped) => {
                    debug!("rdma upload using mmap content for piece {}", piece_id);
                    return Ok(PieceSource::Mapped(mapped));
                }
                Err(err) => {
                    warn!(
                        "rdma mmap upload unavailable for piece {}, falling back to reader: {}",
                        piece_id, err
                    );
                }
            }
        }

        let content_reader: ClientResult<Box<dyn tokio::io::AsyncRead + Send + Unpin>> =
            match request.kind {
                PieceKind::Piece => self
                    .storage
                    .upload_piece(piece_id, &request.task_id, None)
                    .await
                    .map(|reader| Box::new(reader) as Box<dyn tokio::io::AsyncRead + Send + Unpin>),
                PieceKind::PersistentPiece => self
                    .storage
                    .upload_persistent_piece(piece_id, &request.task_id, None)
                    .await
                    .map(|reader| Box::new(reader) as Box<dyn tokio::io::AsyncRead + Send + Unpin>),
                PieceKind::PersistentCachePiece => self
                    .storage
                    .upload_persistent_cache_piece(piece_id, &request.task_id, None)
                    .await
                    .map(|reader| Box::new(reader) as Box<dyn tokio::io::AsyncRead + Send + Unpin>),
            };
        match content_reader {
            Ok(reader) => Ok(PieceSource::Reader(reader)),
            Err(err) => Err(err),
        }
    }

    /// abort reports an error to the client over the rendezvous channel.
    async fn abort(
        &self,
        writer: &mut OwnedWriteHalf,
        code: u32,
        message: String,
    ) -> ClientResult<()> {
        error!("aborting rdma transfer: {}", message);
        // The write is bounded here rather than at each call site: a peer that stops reading holds
        // this connection's admission permit for as long as the write blocks, and a permit held
        // that way is never returned, so enough such peers switch RDMA serving off entirely.
        time::timeout(
            self.transfer_timeout,
            write_frame(writer, &Frame::Error(RendezvousError { code, message })),
        )
        .await?
    }
}

/// PieceSource supplies bytes for the registered send ring.
enum PieceSource {
    /// Mapped copies directly from a content-file memory map.
    Mapped(MappedPiece),
    /// Reader streams through the existing upload path, including cache hits.
    Reader(Box<dyn tokio::io::AsyncRead + Send + Unpin>),
}

impl PieceSource {
    /// Fills `dst` with `dst.len()` bytes beginning at `piece_offset` within the piece.
    async fn fill(&mut self, piece_offset: usize, dst: &mut [u8]) -> ClientResult<()> {
        match self {
            Self::Mapped(mapped) => {
                let end = piece_offset
                    .checked_add(dst.len())
                    .ok_or(ClientError::InvalidParameter)?;
                let Some(src) = mapped.as_slice().get(piece_offset..end) else {
                    return Err(ClientError::Unknown(format!(
                        "mmap piece underflow at offset {} length {}",
                        piece_offset,
                        dst.len()
                    )));
                };
                dst.copy_from_slice(src);
                Ok(())
            }
            Self::Reader(reader) => reader.read_exact(dst).await.map(|_| ()).map_err(Into::into),
        }
    }
}
