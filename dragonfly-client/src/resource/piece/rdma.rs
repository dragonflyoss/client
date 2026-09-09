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

use super::*;
use dragonfly_client_storage::client::PieceContentStream;
use dragonfly_client_storage::rdma::rendezvous::PieceKind;
use futures::StreamExt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::time::{timeout, timeout_at, Instant as Deadline};
use tokio_util::sync::CancellationToken;

impl Piece {
    /// Keep ownership in an independent task until every disk write has drained. Dropping
    /// the caller cancels network reads, but cannot release the piece to another writer.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn download_from_parent_with_rdma(
        &self,
        kind: PieceKind,
        piece_id: &str,
        host_id: &str,
        task_id: &str,
        number: u32,
        offset: u64,
        length: u64,
        parent_id: &str,
        addr: &str,
        is_prefetch: bool,
    ) -> Result<metadata::Piece> {
        let manager = self.clone();
        let piece_id = piece_id.to_owned();
        let host_id = host_id.to_owned();
        let task_id = task_id.to_owned();
        let parent_id = parent_id.to_owned();
        let addr = addr.to_owned();
        let cancellation = CancellationToken::new();
        let _cancel_on_drop = cancellation.clone().drop_guard();
        let download = async move {
            let piece = tokio::select! {
                _ = cancellation.cancelled() => return Err(cancelled()),
                piece = manager.claim_rdma_piece(kind, &piece_id, number, offset, length) => piece?,
            };
            if piece.is_finished() {
                collect_download_piece_traffic_metrics(&TrafficType::LocalPeer, length);
                return Ok(piece);
            }
            let guard = scopeguard::guard((), |_| {
                if let Err(err) = manager.fail_rdma_piece(kind, &piece_id) {
                    error!("set piece metadata failed: {err}");
                }
            });
            tokio::select! {
                _ = cancellation.cancelled() => return Err(cancelled()),
                _ = async {
                    if is_prefetch {
                        manager.prefetch_bandwidth_limiter.acquire(length as usize).await;
                    }
                    manager.download_bandwidth_limiter.acquire(length as usize).await;
                } => {}
            }

            let start_time = Instant::now();
            let deadline = Deadline::now()
                + manager
                    .config
                    .download
                    .piece_timeout
                    .min(manager.config.storage.server.rdma.transfer_timeout);
            let downloader = manager.rdma_downloader.as_ref().expect("RDMA configured");
            let downloaded = tokio::select! {
                _ = cancellation.cancelled() => return Err(cancelled()),
                result = downloader.download_stream(kind, &addr, number, &task_id, offset, length, deadline) => result,
            };
            if let Ok((stream, received_offset, digest)) = downloaded {
                let transport_failed = Arc::new(AtomicBool::new(false));
                let mut stream = checked_stream(
                    stream,
                    cancellation.clone(),
                    manager
                        .config
                        .download
                        .piece_timeout
                        .min(manager.config.storage.server.rdma.transfer_timeout),
                    transport_failed.clone(),
                );
                match manager
                    .finish_rdma_piece(
                        kind,
                        &piece_id,
                        &task_id,
                        received_offset,
                        length,
                        &digest,
                        &parent_id,
                        &mut stream,
                    )
                    .await
                {
                    Ok(piece) => {
                        downloader.record_success(&addr);
                        collect_download_piece_traffic_metrics(&TrafficType::RemotePeer, length);
                        collect_download_piece_duration_metrics(
                            &TrafficType::RemotePeer,
                            start_time.elapsed(),
                        );
                        scopeguard::ScopeGuard::into_inner(guard);
                        return Ok(piece);
                    }
                    Err(err) => {
                        if cancellation.is_cancelled() {
                            return Err(err);
                        }
                        if transport_failed.load(Ordering::Relaxed)
                            || matches!(err, Error::DigestMismatch(..))
                        {
                            downloader.record_transfer_failure(&addr).await;
                            warn!("rdma piece transfer failed, restarting over tcp: {err}");
                        } else {
                            // A local disk or metadata failure is not evidence of an unhealthy
                            // parent and cannot be fixed by repeating the same write over TCP.
                            return Err(err);
                        }
                    }
                }
            } else if let Err(err) = downloaded {
                warn!("rdma download failed, falling back to tcp: {err}");
            }
            if cancellation.is_cancelled() {
                return Err(cancelled());
            }

            // Retain the original claim across fallback. Publishing a failed piece and
            // reacquiring it would let another writer complete it before this TCP retry.
            let tcp_deadline = Deadline::now() + manager.config.download.piece_timeout;
            let downloaded = match kind {
                PieceKind::Piece => manager
                    .tcp_downloader
                    .download_piece(&addr, number, &host_id, &task_id),
                PieceKind::PersistentPiece => manager
                    .tcp_downloader
                    .download_persistent_piece(&addr, number, &host_id, &task_id),
                PieceKind::PersistentCachePiece => manager
                    .tcp_downloader
                    .download_persistent_cache_piece(&addr, number, &host_id, &task_id),
            };
            let (stream, received_offset, digest) = tokio::select! {
                _ = cancellation.cancelled() => return Err(cancelled()),
                result = timeout_at(tcp_deadline, downloaded) => result??,
            };
            if received_offset != offset {
                return Err(Error::Unknown(format!(
                    "TCP fallback piece offset mismatch: expected {offset}, got {received_offset}"
                )));
            }
            let mut stream = checked_stream(
                stream,
                cancellation.clone(),
                manager.config.download.piece_timeout,
                Arc::new(AtomicBool::new(false)),
            );
            let piece = manager
                .finish_rdma_piece(
                    kind,
                    &piece_id,
                    &task_id,
                    received_offset,
                    length,
                    &digest,
                    &parent_id,
                    &mut stream,
                )
                .await?;
            collect_download_piece_traffic_metrics(&TrafficType::RemotePeer, length);
            collect_download_piece_duration_metrics(&TrafficType::RemotePeer, start_time.elapsed());
            scopeguard::ScopeGuard::into_inner(guard);
            Ok(piece)
        };
        tokio::spawn(download)
            .await
            .map_err(Error::TokioJoinError)?
    }

    async fn claim_rdma_piece(
        &self,
        kind: PieceKind,
        piece_id: &str,
        number: u32,
        offset: u64,
        length: u64,
    ) -> Result<metadata::Piece> {
        match kind {
            PieceKind::Piece => {
                self.storage
                    .download_piece_started(piece_id, number, offset, length)
                    .await
            }
            PieceKind::PersistentPiece => {
                self.storage
                    .download_persistent_piece_started(piece_id, number, offset, length)
                    .await
            }
            PieceKind::PersistentCachePiece => {
                self.storage
                    .download_persistent_cache_piece_started(piece_id, number, offset, length)
                    .await
            }
        }
    }

    fn fail_rdma_piece(&self, kind: PieceKind, piece_id: &str) -> Result<()> {
        match kind {
            PieceKind::Piece => self.storage.download_piece_failed(piece_id),
            PieceKind::PersistentPiece => self.storage.download_persistent_piece_failed(piece_id),
            PieceKind::PersistentCachePiece => self
                .storage
                .download_persistent_cache_piece_failed(piece_id),
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn finish_rdma_piece(
        &self,
        kind: PieceKind,
        piece_id: &str,
        task_id: &str,
        offset: u64,
        length: u64,
        digest: &str,
        parent_id: &str,
        stream: &mut PieceContentStream,
    ) -> Result<metadata::Piece> {
        match kind {
            PieceKind::Piece => {
                self.storage
                    .download_piece_from_parent_finished_without_timeout(
                        piece_id, task_id, offset, length, digest, parent_id, stream,
                    )
                    .await
            }
            PieceKind::PersistentPiece => {
                self.storage
                    .download_persistent_piece_from_parent_finished(
                        piece_id, task_id, offset, length, digest, parent_id, stream,
                    )
                    .await
            }
            PieceKind::PersistentCachePiece => {
                self.storage
                    .download_persistent_cache_piece_from_parent_finished(
                        piece_id, task_id, offset, length, digest, parent_id, stream,
                    )
                    .await
            }
        }
    }
}

fn cancelled() -> Error {
    std::io::Error::new(std::io::ErrorKind::Interrupted, "piece download cancelled").into()
}

/// Cancellation and the receive budget interrupt receiving, never an in-flight write.
///
/// The budget is spent only while waiting on the producer. Each chunk is drained to storage
/// between polls, and that time is deliberately not charged: an absolute deadline covering
/// both would report local write latency as a transport failure, which penalizes a healthy
/// parent and starts a whole-piece TCP retry the same slow disk has to absorb again. The RFC
/// is explicit that this budget "bounds added RDMA waiting, not kernel filesystem execution
/// time". The budget still does not restart per chunk, so a producer that stalls repeatedly
/// exhausts it.
fn checked_stream(
    stream: PieceContentStream,
    cancellation: CancellationToken,
    budget: Duration,
    failed: Arc<AtomicBool>,
) -> PieceContentStream {
    futures::stream::unfold((stream, cancellation, failed, budget, false), move |(mut stream, cancellation, failed, budget, done)| async move {
        if done { return None; }
        let waiting_since = Instant::now();
        let item = tokio::select! {
            biased;
            _ = cancellation.cancelled() => Some(Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "piece download cancelled"))),
            result = timeout(budget, stream.next()) => match result {
                Ok(item) => {
                    if item.as_ref().is_some_and(|item| item.is_err()) || item.is_none() {
                        failed.store(true, Ordering::Relaxed);
                    }
                    item
                },
                Err(_) => {
                    failed.store(true, Ordering::Relaxed);
                    Some(Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "piece receive budget elapsed")))
                }
            }
        };
        let budget = budget.saturating_sub(waiting_since.elapsed());
        item.map(|item| {
            let done = item.is_err();
            (item, (stream, cancellation, failed, budget, done))
        })
    }).boxed()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use dragonfly_client_storage::rdma::fabric::Fabric;
    use dragonfly_client_storage::rdma::rendezvous::{
        read_frame, write_frame, CapabilityRegistry, Frame, PieceReady, RdmaAdvertisement,
        RendezvousError, WireCapability, ERROR_CODE_INTERNAL,
    };
    use dragonfly_client_storage::server::tcp::TCPServer;
    use dragonfly_client_util::{id_generator::IDGenerator, shutdown::Shutdown};
    use std::time::Duration;
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::mpsc;

    fn bytes_stream(bytes: &[u8]) -> PieceContentStream {
        futures::stream::iter([Ok(Bytes::copy_from_slice(bytes))]).boxed()
    }

    fn limiter() -> Arc<RateLimiter> {
        Arc::new(
            RateLimiter::builder()
                .initial(1024 * 1024)
                .max(1024 * 1024)
                .refill(1024 * 1024)
                .interval(Duration::from_secs(1))
                .build(),
        )
    }

    async fn create_task(storage: &Storage, kind: PieceKind, task_id: &str, length: u64) -> String {
        match kind {
            PieceKind::Piece => {
                storage
                    .download_task_started(task_id, length, length, None)
                    .await
                    .unwrap();
                storage.piece_id(task_id, 0)
            }
            PieceKind::PersistentPiece => {
                storage
                    .create_persistent_task_started(
                        task_id,
                        Duration::from_secs(3600),
                        length,
                        length,
                    )
                    .await
                    .unwrap();
                storage
                    .create_persistent_task(task_id, length)
                    .await
                    .unwrap();
                storage.persistent_piece_id(task_id, 0)
            }
            PieceKind::PersistentCachePiece => {
                storage
                    .create_persistent_cache_task_started(
                        task_id,
                        Duration::from_secs(3600),
                        length,
                        length,
                    )
                    .await
                    .unwrap();
                storage
                    .create_persistent_cache_task(task_id, length)
                    .await
                    .unwrap();
                storage.persistent_cache_piece_id(task_id, 0)
            }
        }
    }

    async fn manager(config: Arc<Config>, path: &std::path::Path) -> Piece {
        let storage = Arc::new(
            Storage::new(config.clone(), path, path.to_owned())
                .await
                .unwrap(),
        );
        Piece::new(
            config.clone(),
            storage,
            Arc::new(BackendFactory::new(config, None).unwrap()),
            limiter(),
            limiter(),
            limiter(),
        )
        .unwrap()
    }

    /// Exercises the actual fabric reader, partial storage write, Vortex TCP retry and
    /// final byte/digest verification in each namespace.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn partial_rdma_transfer_restarts_over_tcp_in_every_namespace() {
        for kind in [
            PieceKind::Piece,
            PieceKind::PersistentPiece,
            PieceKind::PersistentCachePiece,
        ] {
            let dir = tempfile::tempdir().unwrap();
            let mut config = Config::default();
            config.download.protocol = "rdma".into();
            config.download.piece_timeout = Duration::from_secs(10);
            config.storage.write_buffer_size = 4;
            config.storage.server.rdma.allow_software_provider = true;
            config.storage.server.rdma.fabric_tag = Some("fallback-test".into());
            config.storage.server.rdma.chunk_size = bytesize::ByteSize(4);
            config.storage.server.rdma.max_inflight_chunks = 1;
            let config = Arc::new(config);
            let source = manager(config.clone(), &dir.path().join("source")).await;
            let target = manager(config.clone(), &dir.path().join("target")).await;
            let task_id = "d869ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
            let content = b"xxxxxxxx";
            let source_id = create_task(&source.storage, kind, task_id, 8).await;
            let piece_id = create_task(&target.storage, kind, task_id, 8).await;
            source
                .claim_rdma_piece(kind, &source_id, 0, 0, 8)
                .await
                .unwrap();
            let seeded = source
                .finish_rdma_piece(
                    kind,
                    &source_id,
                    task_id,
                    0,
                    8,
                    "",
                    "seed",
                    &mut bytes_stream(content),
                )
                .await
                .unwrap();

            let fabric = Arc::new(Fabric::new(None, None, 1024 * 1024, true).unwrap());
            let rdma_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let capabilities = CapabilityRegistry::default();
            capabilities.publish(RdmaAdvertisement {
                port: rdma_listener.local_addr().unwrap().port(),
                capability: WireCapability {
                    provider: fabric.provider().into(),
                    fabric_tag: "fallback-test".into(),
                },
            });
            let tcp_reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let tcp_addr = tcp_reservation.local_addr().unwrap();
            drop(tcp_reservation);
            let shutdown = Shutdown::new();
            let (shutdown_tx, _shutdown_rx) = mpsc::unbounded_channel();
            let mut server = TCPServer::new(
                config.clone(),
                tcp_addr,
                Arc::new(IDGenerator::new(
                    "127.0.0.1".into(),
                    "localhost".into(),
                    false,
                )),
                source.storage.clone(),
                limiter(),
                shutdown.clone(),
                shutdown_tx,
            )
            .with_rdma_capabilities(capabilities);
            let serving = tokio::spawn(async move {
                server.run().await.unwrap();
            });
            for _ in 0..100 {
                if TcpStream::connect(tcp_addr).await.is_ok() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }

            let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let proxy_addr = proxy.local_addr().unwrap().to_string();
            let (fallback_ready_tx, fallback_ready_rx) = tokio::sync::oneshot::channel();
            let (release_fallback_tx, release_fallback_rx) = tokio::sync::oneshot::channel();
            let forwarding = tokio::spawn(async move {
                let (mut discovery, _) = proxy.accept().await.unwrap();
                let mut source = TcpStream::connect(tcp_addr).await.unwrap();
                tokio::io::copy_bidirectional(&mut discovery, &mut source)
                    .await
                    .unwrap();
                let (mut fallback, _) = proxy.accept().await.unwrap();
                fallback_ready_tx.send(()).unwrap();
                release_fallback_rx.await.unwrap();
                let mut source = TcpStream::connect(tcp_addr).await.unwrap();
                // The downloader pool may keep the TCP connection alive after the piece.
                let _ = tokio::io::copy_bidirectional(&mut fallback, &mut source).await;
            });
            let (first_claim_tx, first_claim_rx) = tokio::sync::oneshot::channel();
            let observed_storage = target.storage.clone();
            let observed_piece_id = piece_id.clone();
            let task_dir = match kind {
                PieceKind::Piece => "tasks",
                PieceKind::PersistentPiece => "persistent-tasks",
                PieceKind::PersistentCachePiece => "persistent-cache-tasks",
            };
            let partial_path = dir
                .path()
                .join("target/content")
                .join(task_dir)
                .join(&task_id[..3])
                .join(task_id);
            let digest = seeded.digest.clone();
            let fault_peer = tokio::spawn(async move {
                let (mut control, _) = rdma_listener.accept().await.unwrap();
                let Frame::Request(request) = read_frame(&mut control).await.unwrap() else {
                    panic!("expected request")
                };
                assert_eq!(request.kind, kind);
                let destination = fabric.resolve(&request.client_endpoint).unwrap();
                write_frame(
                    &mut control,
                    &Frame::Ready(PieceReady {
                        offset: 0,
                        length: 8,
                        digest,
                        server_endpoint: fabric.local_endpoint().to_vec(),
                        chunk_size: 4,
                        max_inflight_chunks: 1,
                    }),
                )
                .await
                .unwrap();
                assert!(matches!(
                    read_frame(&mut control).await.unwrap(),
                    Frame::RecvPosted { start_chunk: 0, .. }
                ));
                let buffer = fabric.alloc_buffer(4).await.unwrap();
                // This buffer has not been posted and is exclusively owned by the fixture.
                unsafe { buffer.as_mut_slice() }.copy_from_slice(b"yyyy");
                let sent = fabric
                    .post_send(&buffer, 0, 4, request.tag, destination)
                    .await
                    .unwrap();
                fabric.wait(sent, Duration::from_secs(5)).await.unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        if tokio::fs::read(&partial_path)
                            .await
                            .unwrap()
                            .starts_with(b"yyyy")
                        {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .expect("first RDMA window must reach storage before injecting the failure");
                first_claim_tx
                    .send(
                        observed_storage
                            .in_flight_piece_notifier(&observed_piece_id)
                            .expect("RDMA owns the piece"),
                    )
                    .unwrap();
                write_frame(
                    &mut control,
                    &Frame::Error(RendezvousError {
                        code: ERROR_CODE_INTERNAL,
                        message: "injected after first window".into(),
                    }),
                )
                .await
                .unwrap();
            });
            let downloading = target.clone();
            let downloading_id = piece_id.clone();
            let download = tokio::spawn(async move {
                downloading
                    .download_from_parent_with_rdma(
                        kind,
                        &downloading_id,
                        "test-host",
                        task_id,
                        0,
                        0,
                        8,
                        "test-parent",
                        &proxy_addr,
                        false,
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(10), fallback_ready_rx)
                .await
                .unwrap()
                .unwrap();
            let first_claim = first_claim_rx.await.unwrap();
            let fallback_claim = target
                .storage
                .in_flight_piece_notifier(&piece_id)
                .expect("TCP fallback retains ownership");
            assert!(
                Arc::ptr_eq(&first_claim, &fallback_claim),
                "fallback must not release and reacquire the piece"
            );
            let waiter = target.claim_rdma_piece(kind, &piece_id, 0, 0, 8);
            tokio::pin!(waiter);
            assert!(
                tokio::time::timeout(Duration::from_millis(20), &mut waiter)
                    .await
                    .is_err(),
                "a concurrent claimant must wait while fallback is blocked"
            );
            release_fallback_tx.send(()).unwrap();
            let piece = tokio::time::timeout(Duration::from_secs(10), download)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
            let waited = waiter.await.unwrap();
            assert!(waited.is_finished());
            assert_eq!(waited.digest, piece.digest);
            assert!(piece.is_finished());
            assert_eq!(piece.digest, seeded.digest);
            let (_, mut reader) = match kind {
                PieceKind::Piece => target.storage.upload_piece(&piece_id, task_id, None).await,
                PieceKind::PersistentPiece => {
                    target
                        .storage
                        .upload_persistent_piece(&piece_id, task_id, None)
                        .await
                }
                PieceKind::PersistentCachePiece => {
                    target
                        .storage
                        .upload_persistent_cache_piece(&piece_id, task_id, None)
                        .await
                }
            }
            .unwrap();
            let mut actual = Vec::new();
            reader.read_to_end(&mut actual).await.unwrap();
            assert_eq!(actual, content);
            fault_peer.await.unwrap();
            shutdown.trigger();
            serving.await.unwrap();
            forwarding.abort();
        }
    }

    #[tokio::test]
    async fn cancellation_stops_receiving_without_penalizing_the_parent() {
        let cancel = CancellationToken::new();
        let failed = Arc::new(AtomicBool::new(false));
        let mut stream = checked_stream(
            futures::stream::pending().boxed(),
            cancel.clone(),
            Duration::from_secs(30),
            failed.clone(),
        );
        cancel.cancel();
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().kind(),
            std::io::ErrorKind::Interrupted
        );
        assert!(!failed.load(Ordering::Relaxed));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn receiving_does_not_restart_an_expired_attempt_deadline() {
        let failed = Arc::new(AtomicBool::new(false));
        let mut stream = checked_stream(
            futures::stream::pending().boxed(),
            CancellationToken::new(),
            Duration::ZERO,
            failed.clone(),
        );
        assert_eq!(
            stream.next().await.unwrap().unwrap_err().kind(),
            std::io::ErrorKind::TimedOut
        );
        assert!(failed.load(Ordering::Relaxed));
    }

    /// A slow consumer must not spend the receive budget. The budget covers waiting on the
    /// producer; draining each chunk to storage happens between polls and is the filesystem's
    /// time, not the fabric's. Charging it would blame a healthy parent for a slow disk.
    #[tokio::test]
    async fn draining_to_storage_does_not_spend_the_receive_budget() {
        let failed = Arc::new(AtomicBool::new(false));
        let chunks = vec![
            Ok(Bytes::from_static(b"first")),
            Ok(Bytes::from_static(b"second")),
        ];
        let mut stream = checked_stream(
            futures::stream::iter(chunks).boxed(),
            CancellationToken::new(),
            Duration::from_millis(300),
            failed.clone(),
        );

        assert_eq!(stream.next().await.unwrap().unwrap(), "first");
        // Stand in for a write that takes longer than the whole budget.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_eq!(stream.next().await.unwrap().unwrap(), "second");
        assert!(
            !failed.load(Ordering::Relaxed),
            "local write latency must not be recorded as a transport failure"
        );
    }
}
