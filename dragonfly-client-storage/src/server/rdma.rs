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
use dragonfly_client_config::dfdaemon::{Config, RdmaProvider, RDMA_MIN_CHUNK_SIZE};
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
use tokio::task::JoinSet;
use tokio::time;
use tracing::{debug, error, info, instrument, warn, Span};

/// Failed endpoint generations cannot immediately consume another initialization budget.
const RECOVERY_COOLDOWN: std::time::Duration = std::time::Duration::from_secs(300);

/// How long an orderly close waits for the peer's unread control bytes before giving up.
const DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

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

    /// Runs optional RDMA serving until shutdown, retrying failed endpoint generations only
    /// after the recovery cooldown. TCP remains available while readiness is withdrawn.
    pub async fn run(&mut self) -> ClientResult<()> {
        if let Some(registry) = &self.capability_registry {
            registry.clear();
        }
        let Some(fabric_tag) = self
            .config
            .storage
            .server
            .rdma
            .fabric_tag
            .as_deref()
            .filter(|tag| !tag.is_empty())
            .map(str::to_owned)
        else {
            error!(
                "rdma server disabled: storage.server.rdma.fabricTag is required so peers \
                 only attempt rdma within one reachability domain"
            );
            self.shutdown.recv().await;
            return Ok(());
        };

        loop {
            let result = self.run_generation(&fabric_tag).await;
            if self.shutdown.is_shutdown() {
                return Ok(());
            }
            if let Err(err) = result {
                error!(
                    "rdma server unavailable, retrying after recovery cooldown: {}",
                    err
                );
            }
            // Measure from completion of cleanup, so even a slow teardown cannot shorten
            // the required delay after the failed initialization or endpoint retirement.
            tokio::select! {
                biased;
                _ = self.shutdown.recv() => return Ok(()),
                _ = time::sleep(RECOVERY_COOLDOWN) => {}
            }
        }
    }

    /// Runs one endpoint generation and retires its handlers before another can initialize.
    async fn run_generation(&mut self, fabric_tag: &str) -> ClientResult<()> {
        let rdma_config = &self.config.storage.server.rdma;
        let provider = match rdma_config.provider {
            RdmaProvider::Auto => None,
            provider => Some(provider.to_string()),
        };
        let device = rdma_config.device.clone();
        let budget = self.storage.rdma_memory_budget();
        let allow_software_provider = rdma_config.allow_software_provider;
        // Provider initialization may block in native code. Shutdown does not wait for it;
        // a late result is dropped on the blocking worker with its original shared budget.
        let initialize = tokio::task::spawn_blocking(move || {
            Fabric::new_with_budget(
                provider.as_deref(),
                device.as_deref(),
                budget,
                allow_software_provider,
            )
        });
        let fabric = tokio::select! {
            biased;
            _ = self.shutdown.recv() => return Ok(()),
            result = initialize => {
                Arc::new(result.map_err(|err| ClientError::Unknown(
                    format!("rdma endpoint initialization task failed: {err}")
                ))??)
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

        let listener = match bind_listener(self.addr) {
            Ok(listener) => listener,
            Err(err) => {
                retire_handler(handler, &mut self.shutdown).await?;
                return Err(err.into());
            }
        };
        let listening_addr = match listener.local_addr() {
            Ok(addr) => addr,
            Err(err) => {
                retire_handler(handler, &mut self.shutdown).await?;
                return Err(err.into());
            }
        };
        if handler.fabric.is_failed() {
            retire_handler(handler, &mut self.shutdown).await?;
            return Err(ClientError::Unknown(
                "rdma endpoint failed before publication".to_string(),
            ));
        }
        info!(
            "storage rdma server listening on {}, provider {}",
            listening_addr, handler.capability.provider
        );
        let published_capability = self.capability_registry.as_ref().map(|registry| {
            registry.publish(RdmaAdvertisement {
                capability: handler.capability.clone(),
                port: listening_addr.port(),
            });
            PublishedCapability(registry.clone())
        });
        let mut transfers = JoinSet::new();
        let result = loop {
            tokio::select! {
                biased;
                _ = self.shutdown.recv() => {
                    info!("rdma server shutting down");
                    break Ok(());
                },
                _ = handler.fabric.wait_failed() => {
                    break Err(ClientError::Unknown("rdma serving endpoint failed".to_string()));
                },
                _ = transfers.join_next(), if !transfers.is_empty() => {},
                tcp_accepted = listener.accept() => {
                    let (tcp, remote_address) = match tcp_accepted {
                        Ok(accepted) => accepted,
                        Err(err) => {
                            warn!("rdma rendezvous accept failed: {}", err);
                            tokio::select! {
                                biased;
                                _ = self.shutdown.recv() => break Ok(()),
                                _ = handler.fabric.wait_failed() => {
                                    break Err(ClientError::Unknown("rdma serving endpoint failed".to_string()));
                                },
                                _ = time::sleep(std::time::Duration::from_millis(100)) => {}
                            }
                            continue;
                        }
                    };
                    debug!("accepted rdma rendezvous connection from {}", remote_address);

                    let Ok(admission) = transfer_admission.clone().try_acquire_owned() else {
                        debug!(
                            "rdma rendezvous admission full, rejecting connection from {}",
                            remote_address
                        );
                        // Bound rejection tasks as well as transfers. An overloaded listener
                        // must not allocate one detached task for every incoming connection.
                        if transfers.len() >= rdma_config.max_concurrent_transfers as usize * 2 {
                            continue;
                        }
                        let reject_timeout = rdma_config.transfer_timeout;
                        transfers.spawn(async move {
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
                    transfers.spawn(async move {
                        let _admission = admission;
                        if let Err(err) = handler.handle(tcp, remote_address.to_string()).await {
                           error!("failed to serve rdma connection from {}: {}", remote_address, err);
                        }
                    });
                }
            }
        };

        // Withdraw first, close the listener, then cancel and join every producer. Fabric
        // operation handles retain DMA buffers through completion or safe endpoint close.
        drop(published_capability);
        drop(listener);
        transfers.shutdown().await;
        retire_handler(handler, &mut self.shutdown).await?;
        result
    }
}

/// Binds each generation to the same rendezvous port, including after TCP TIME_WAIT.
fn bind_listener(addr: SocketAddr) -> std::io::Result<TcpListener> {
    let socket = Socket::new(Domain::for_address(addr), Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_tcp_nodelay(true)?;
    socket.set_nonblocking(true)?;
    socket.set_tcp_keepalive(
        &TcpKeepalive::new()
            .with_interval(super::DEFAULT_KEEPALIVE_INTERVAL)
            .with_time(super::DEFAULT_KEEPALIVE_TIME)
            .with_retries(super::DEFAULT_KEEPALIVE_RETRIES),
    )?;
    socket.bind(&addr.into())?;
    socket.listen(1024)?;
    let std_listener: std::net::TcpListener = socket.into();
    TcpListener::from_std(std_listener)
}

/// Native endpoint teardown must finish before recovery, while shutdown remains responsive.
async fn retire_handler(
    handler: Arc<RDMAServerHandler>,
    shutdown: &mut shutdown::Shutdown,
) -> ClientResult<()> {
    let retire = tokio::task::spawn_blocking(move || drop(handler));
    tokio::select! {
        biased;
        _ = shutdown.recv() => Ok(()),
        retired = retire => retired.map_err(|err| ClientError::Unknown(
            format!("rdma endpoint retirement task failed: {err}")
        )),
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
        // Every decline says why. Returning here without a frame closes the socket, which the
        // client reports as a bare connection reset that is indistinguishable from a broken
        // fabric, so the operator loses the one piece of evidence that identifies the cause.
        let request = match time::timeout(self.transfer_timeout, read_frame(&mut reader)).await {
            Ok(Ok(Frame::Request(request))) => request,
            Ok(Ok(frame)) => {
                let message = format!("unexpected rendezvous frame: {frame:?}");
                let _ = self
                    .abort(&mut writer, ERROR_CODE_INTERNAL, message.clone())
                    .await;
                return Err(ClientError::Unknown(message));
            }
            Ok(Err(err)) => {
                // A request this server cannot parse is most often a peer on another protocol
                // version, which is an incompatibility rather than a transport fault.
                let message = format!("unreadable rendezvous request: {err}");
                let _ = self
                    .abort(&mut writer, ERROR_CODE_INCOMPATIBLE, message)
                    .await;
                return Err(err);
            }
            Err(err) => {
                let message = format!(
                    "timed out after {:?} reading the rendezvous request",
                    self.transfer_timeout
                );
                let _ = self.abort(&mut writer, ERROR_CODE_INTERNAL, message).await;
                return Err(err.into());
            }
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

        // Probe the requested namespace so a piece this node never started is declined without
        // waiting. The metadata itself is read again after the wait below, because this snapshot
        // is not yet committed.
        let piece = match request.kind {
            PieceKind::Piece => self.storage.get_piece(&piece_id),
            PieceKind::PersistentPiece => self.storage.get_persistent_piece(&piece_id),
            PieceKind::PersistentCachePiece => self.storage.get_persistent_cache_piece(&piece_id),
        };
        match piece {
            Ok(Some(_)) => {}
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
        }

        // The metadata above only proves the piece exists; a piece this node is still downloading
        // carries no digest yet. Wait for it to be committed exactly as the TCP piece server does,
        // then serve from the committed metadata. Reading the pre-wait snapshot would both refuse
        // RDMA for every in-flight piece and describe the piece to the peer with a stale digest.
        let piece = match self
            .storage
            .wait_for_rdma_piece_finished(&piece_id, request.kind)
            .await
        {
            Ok(piece) => piece,
            Err(err) => {
                self.abort(
                    writer,
                    ERROR_CODE_NOT_FOUND,
                    format!("piece {piece_id} did not finish: {err}"),
                )
                .await?;
                return Err(err);
            }
        };

        // Every RDMA piece must have an independently stored expected CRC. In particular,
        // old metadata with an empty digest must use TCP instead of becoming an unverified
        // successful fabric transfer. The existing piece format is canonical decimal CRC32.
        if !has_supported_digest(&piece.digest) {
            self.abort(
                writer,
                // Unavailable for this piece attempt, not incompatible with every piece
                // from this parent. Existing peers already retry NOT_FOUND through TCP.
                ERROR_CODE_NOT_FOUND,
                "piece has no supported CRC32 digest".to_string(),
            )
            .await?;
            return Err(ClientError::Unknown(
                "piece has no supported CRC32 digest".to_string(),
            ));
        }

        let chunk_size = request
            .chunk_size
            .min(self.chunk_size)
            .min(self.fabric.max_msg_size() as u64);
        let max_inflight_chunks = request.max_inflight_chunks.min(self.max_inflight_chunks);
        // The configuration enforces a floor on this daemon's own chunk size because below it the
        // per-operation posting and completion cost dominates. A peer proposes this value, so the
        // floor has to hold on the wire too: otherwise a peer can ask for a large piece in
        // 256-byte messages and spend thousands of posts and rendezvous round trips on it. A piece
        // smaller than the floor is exempt, since splitting it at all is already cheap and
        // requiring one chunk would reject a legitimate request.
        let undersized_chunk = chunk_size < RDMA_MIN_CHUNK_SIZE.as_u64()
            && piece.length > RDMA_MIN_CHUNK_SIZE.as_u64();
        if piece.length == 0
            || chunk_size == 0
            || undersized_chunk
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
        if let Err(err) = self.fabric.validate_tag_range(request.tag, chunk_count) {
            self.abort(writer, ERROR_CODE_INTERNAL, err.to_string())
                .await?;
            return Err(err);
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
                // Capacity or a retired local fabric, not a piece the client should stop asking
                // for. Reporting TOO_LARGE here told the client the piece itself was the problem.
                self.abort(writer, ERROR_CODE_BUSY, err.to_string()).await?;
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
            let posted = match time::timeout(self.transfer_timeout, read_frame(reader)).await {
                Ok(posted) => posted,
                Err(_) => {
                    // A client that is slower than this timeout is the expected cause. Name the
                    // window so the fallback is attributable instead of arriving as a reset.
                    let message = format!(
                        "timed out after {:?} waiting for the client to post receives for the \
                         window at chunk {start_chunk}",
                        self.transfer_timeout
                    );
                    self.abort(writer, ERROR_CODE_INTERNAL, message.clone())
                        .await?;
                    return Err(ClientError::Unknown(message));
                }
            };
            match posted {
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
                Err(err) => {
                    let message =
                        format!("unreadable receive-window frame at chunk {start_chunk}: {err}");
                    self.abort(writer, ERROR_CODE_INTERNAL, message).await?;
                    return Err(err);
                }
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
        self.close_orderly(reader, writer).await;
        debug!("finished uploading piece content over rdma");
        Ok(piece.length)
    }

    /// close_orderly ends a rendezvous connection with a FIN rather than a reset.
    ///
    /// The client keeps two windows posted, so it can send the next `RecvPosted` before learning
    /// the transfer is over. Closing a socket that still has unread bytes queued makes the kernel
    /// send RST instead of FIN, and the client surfaces that as `Connection reset by peer` on a
    /// transfer that actually succeeded. Draining first keeps a completed transfer from being
    /// reported as a transport failure, which would also back off a healthy parent.
    async fn close_orderly(&self, reader: &mut OwnedReadHalf, writer: &mut OwnedWriteHalf) {
        use tokio::io::AsyncWriteExt;

        let _ = writer.shutdown().await;
        let _ = time::timeout(DRAIN_TIMEOUT, async {
            let mut scratch = [0u8; 512];
            while let Ok(read) = reader.read(&mut scratch).await {
                if read == 0 {
                    break;
                }
            }
        })
        .await;
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
                    .map(|(_, reader)| {
                        Box::new(reader) as Box<dyn tokio::io::AsyncRead + Send + Unpin>
                    }),
                PieceKind::PersistentPiece => self
                    .storage
                    .upload_persistent_piece(piece_id, &request.task_id, None)
                    .await
                    .map(|(_, reader)| {
                        Box::new(reader) as Box<dyn tokio::io::AsyncRead + Send + Unpin>
                    }),
                PieceKind::PersistentCachePiece => self
                    .storage
                    .upload_persistent_cache_piece(piece_id, &request.task_id, None)
                    .await
                    .map(|(_, reader)| {
                        Box::new(reader) as Box<dyn tokio::io::AsyncRead + Send + Unpin>
                    }),
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

/// Matches the existing piece writer's decimal CRC32 representation.
fn has_supported_digest(digest: &str) -> bool {
    digest.strip_prefix("crc32:").is_some_and(|encoded| {
        encoded
            .parse::<u32>()
            .is_ok_and(|crc| crc.to_string() == encoded)
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    #[test]
    fn requires_a_supported_stored_piece_digest() {
        for digest in ["crc32:0", "crc32:4294967295", "crc32:123456"] {
            assert!(has_supported_digest(digest), "{digest}");
        }
        for digest in [
            "",
            "crc32:",
            "crc32:bad",
            "crc32:-1",
            "crc32:4294967296",
            "crc32:01",
            "crc32:+1",
            "sha256:abcd",
        ] {
            assert!(!has_supported_digest(digest), "{digest}");
        }
    }

    #[tokio::test]
    async fn rendezvous_port_can_rebind_after_an_accepted_connection() {
        let listener = bind_listener("127.0.0.1:0".parse().unwrap()).unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = TcpStream::connect(addr).await.unwrap();
        let (mut server, _) = listener.accept().await.unwrap();
        drop(listener);

        // The server actively closes, leaving its port in TIME_WAIT after the peer exits.
        server.shutdown().await.unwrap();
        client.read_to_end(&mut Vec::new()).await.unwrap();
        drop(server);
        drop(client);
        let restarted = bind_listener(addr).unwrap();
        assert_eq!(restarted.local_addr().unwrap(), addr);
    }

    #[tokio::test]
    async fn failed_initialization_withdraws_readiness_and_remains_shutdown_responsive() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.server.rdma.fabric_tag = Some("test-fabric".to_string());
        // Reject the name before calling native provider initialization; no hardware needed.
        config.storage.server.rdma.device = Some("invalid\0device".to_string());
        let config = Arc::new(config);
        let storage = Arc::new(
            Storage::new(config.clone(), dir.path(), dir.path().to_path_buf())
                .await
                .unwrap(),
        );
        let registry = CapabilityRegistry::default();
        registry.publish(RdmaAdvertisement {
            capability: WireCapability {
                provider: "old-provider".to_string(),
                fabric_tag: "test-fabric".to_string(),
            },
            port: 4007,
        });
        let shutdown = shutdown::Shutdown::new();
        let (complete, _completed) = mpsc::unbounded_channel();
        let mut server = RDMAServer::new(
            config,
            "127.0.0.1:0".parse().unwrap(),
            Arc::new(IDGenerator::new(
                "127.0.0.1".to_string(),
                "localhost".to_string(),
                false,
            )),
            storage,
            Arc::new(RateLimiter::builder().build()),
            shutdown.clone(),
            complete,
        )
        .with_capability_registry(registry.clone());
        let run = server.run();
        tokio::pin!(run);

        assert!(time::timeout(Duration::from_millis(50), &mut run)
            .await
            .is_err());
        assert!(registry.get().is_none());
        shutdown.trigger();
        time::timeout(Duration::from_secs(1), run)
            .await
            .unwrap()
            .unwrap();
        assert!(registry.get().is_none());
    }
}
