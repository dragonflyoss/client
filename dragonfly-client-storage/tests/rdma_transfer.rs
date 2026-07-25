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

//! End-to-end test of the RDMA piece transport: a real RDMAServer serving a piece from
//! real storage to a real RDMAClient over libfabric. On hosts without RDMA hardware,
//! libfabric selects its tcp/sockets provider, exercising the same application and shim path as
//! the efa and verbs providers while not reproducing hardware-provider behavior.

#![cfg(feature = "rdma")]

use bytes::Bytes;
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::Error;
use dragonfly_client_storage::client::rdma::{discover, RDMAClient};
use dragonfly_client_storage::rdma::fabric::Fabric;
use dragonfly_client_storage::rdma::rendezvous::{
    read_frame, write_frame, CapabilityRegistry, Frame, PieceKind, PieceReady, PieceRequest,
    RendezvousError, WireCapability, ERROR_CODE_BUSY, ERROR_CODE_INTERNAL,
};
use dragonfly_client_storage::server::{rdma::RDMAServer, tcp::TCPServer};
use dragonfly_client_storage::Storage;
use dragonfly_client_util::{id_generator::IDGenerator, shutdown};
use futures::Stream;
use leaky_bucket::RateLimiter;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;

/// FABRIC_TAG is the shared reachability-domain label for this in-process test pair.
const FABRIC_TAG: &str = "test-fabric";

/// free_port grabs an ephemeral TCP port for the rendezvous listener.
/// NEXT_TEST_PORT hands out ports from a range the OS does not use for ephemeral allocation.
///
/// Binding to port 0 and closing the socket is not enough here: the kernel readily hands the same
/// port to the next probe, so two tests running in parallel can be told to use it and whichever
/// server binds second fails with `AddrInUse`. A counter that never repeats removes that race
/// between tests in this process.
static NEXT_TEST_PORT: AtomicU16 = AtomicU16::new(21000);

/// free_port returns a port no other test in this process has been given, and that is bindable now.
fn free_port() -> u16 {
    for _ in 0..1000 {
        let port = NEXT_TEST_PORT.fetch_add(1, Ordering::Relaxed);
        if std::net::TcpListener::bind(("127.0.0.1", port)).is_ok() {
            return port;
        }
    }
    panic!("no free port available for test");
}

/// test_config builds a config with the RDMA transport enabled.
fn test_config() -> Config {
    let mut config = Config::default();
    config.storage.server.rdma.enable = true;
    config.storage.server.rdma.allow_software_provider = true;
    config.storage.server.rdma.fabric_tag = Some(FABRIC_TAG.to_string());
    config.storage.server.rdma.transfer_timeout = Duration::from_secs(10);
    config.storage.server.rdma.mmap_content = true;
    config
}

/// assign_free_ports gives the TCP discovery and RDMA rendezvous listeners distinct ports.
fn assign_free_ports(config: &mut Config) {
    config.storage.server.rdma.port = free_port();
    config.storage.server.tcp_port = free_port();
}

/// content_stream wraps fixture bytes as the single-chunk stream the storage write paths take.
fn content_stream(content: &[u8]) -> impl Stream<Item = std::io::Result<Bytes>> + Unpin {
    futures::stream::iter([Ok(Bytes::copy_from_slice(content))])
}

/// write_piece stores one finished piece and returns its digest.
async fn write_piece(storage: &Storage, task_id: &str, number: u32, content: &[u8]) -> String {
    storage
        .download_task_started(task_id, content.len() as u64, content.len() as u64, None)
        .await
        .unwrap();
    let piece_id = storage.piece_id(task_id, number);
    storage
        .download_piece_started(&piece_id, number)
        .await
        .unwrap();
    let piece = storage
        .download_piece_from_source_finished(
            &piece_id,
            task_id,
            0,
            content.len() as u64,
            &mut content_stream(content),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    piece.digest
}

/// write_persistent_piece stores one finished persistent-task piece and returns its digest.
async fn write_persistent_piece(
    storage: &Storage,
    task_id: &str,
    number: u32,
    content: &[u8],
) -> String {
    storage
        .create_persistent_task_started(
            task_id,
            Duration::from_secs(3600),
            content.len() as u64,
            content.len() as u64,
        )
        .await
        .unwrap();
    storage
        .create_persistent_task(task_id, content.len() as u64)
        .await
        .unwrap();
    let piece_id = storage.persistent_piece_id(task_id, number);
    storage
        .download_persistent_piece_started(&piece_id, number)
        .await
        .unwrap();
    let piece = storage
        .download_persistent_piece_from_source_finished(
            &piece_id,
            task_id,
            0,
            content.len() as u64,
            &mut content_stream(content),
            Duration::from_secs(10),
        )
        .await
        .unwrap();
    storage
        .create_persistent_task_finished(task_id)
        .await
        .unwrap();
    piece.digest
}

/// write_persistent_cache_piece stores one finished persistent-cache piece and returns its digest.
async fn write_persistent_cache_piece(
    storage: &Storage,
    task_id: &str,
    number: u32,
    content: &[u8],
) -> String {
    storage
        .create_persistent_cache_task_started(
            task_id,
            Duration::from_secs(3600),
            content.len() as u64,
            content.len() as u64,
        )
        .await
        .unwrap();
    storage
        .create_persistent_cache_task(task_id, content.len() as u64)
        .await
        .unwrap();
    let piece_id = storage.persistent_cache_piece_id(task_id, number);
    storage
        .download_persistent_cache_piece_started(&piece_id, number)
        .await
        .unwrap();
    let piece = storage
        .download_persistent_cache_piece_from_parent_finished(
            &piece_id,
            task_id,
            0,
            content.len() as u64,
            "",
            "fixture-parent",
            &mut content_stream(content),
        )
        .await
        .unwrap();
    storage
        .create_persistent_cache_task_finished(task_id)
        .await
        .unwrap();
    piece.digest
}

/// start_server spawns the advertised TCP discovery endpoint and the RDMAServer.
async fn start_server(
    config: Arc<Config>,
    storage: Arc<Storage>,
) -> (
    String,
    String,
    shutdown::Shutdown,
    mpsc::UnboundedReceiver<()>,
) {
    let rdma_addr: SocketAddr = format!("127.0.0.1:{}", config.storage.server.rdma.port)
        .parse()
        .unwrap();
    let tcp_addr: SocketAddr = format!("127.0.0.1:{}", config.storage.server.tcp_port)
        .parse()
        .unwrap();
    let shutdown = shutdown::Shutdown::new();
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::unbounded_channel();
    let id_generator = Arc::new(IDGenerator::new(
        "127.0.0.1".to_string(),
        "localhost".to_string(),
        false,
    ));
    let limiter = Arc::new(
        RateLimiter::builder()
            .initial(1024 * 1024 * 1024)
            .refill(1024 * 1024 * 1024)
            .max(1024 * 1024 * 1024)
            .interval(Duration::from_secs(1))
            .fair(false)
            .build(),
    );
    let capabilities = CapabilityRegistry::default();

    let mut tcp_server = TCPServer::new(
        config.clone(),
        tcp_addr,
        id_generator.clone(),
        storage.clone(),
        limiter.clone(),
        shutdown.clone(),
        shutdown_complete_tx.clone(),
    )
    .with_rdma_capabilities(capabilities.clone());
    tokio::spawn(async move {
        tcp_server.run().await.unwrap();
    });

    let mut server = RDMAServer::new(
        config.clone(),
        rdma_addr,
        id_generator,
        storage,
        limiter,
        shutdown.clone(),
        shutdown_complete_tx,
    )
    .with_capability_registry(capabilities);
    tokio::spawn(async move {
        server.run().await.unwrap();
    });

    // Wait for both listeners to come up.
    let rdma_addr = rdma_addr.to_string();
    let tcp_addr = tcp_addr.to_string();
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(&rdma_addr).await.is_ok()
            && tokio::net::TcpStream::connect(&tcp_addr).await.is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (tcp_addr, rdma_addr, shutdown, shutdown_complete_rx)
}

/// client_fabric opens a downloader-side fabric endpoint with the given fabric tag.
fn client_fabric(fabric_tag: &str) -> (Arc<Fabric>, WireCapability) {
    let fabric = Arc::new(Fabric::new(None, None, 512 * 1024 * 1024, true).unwrap());
    let capability = WireCapability {
        provider: fabric.provider().to_string(),
        fabric_tag: fabric_tag.to_string(),
    };
    (fabric, capability)
}

#[tokio::test(flavor = "multi_thread")]
async fn downloads_piece_over_rdma() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    let config = Arc::new(config);

    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );

    // 10 MiB piece: the client requests 1 MiB chunks and a two-chunk operation window while the
    // server permits larger defaults, exercising negotiation and five consecutive windows.
    let task_id = "b969ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
    let content: Vec<u8> = (0..10 * 1024 * 1024).map(|i| (i % 249) as u8).collect();
    let digest = write_piece(&storage, task_id, 0, &content).await;

    let (tcp_addr, addr, shutdown, _shutdown_complete_rx) =
        start_server(config.clone(), storage).await;

    let fabric = Arc::new(Fabric::new(None, None, 2 * 1024 * 1024, true).unwrap());
    let capability = WireCapability {
        provider: fabric.provider().to_string(),
        fabric_tag: FABRIC_TAG.to_string(),
    };
    let advertisement = discover(&tcp_addr, Duration::from_secs(5)).await.unwrap();
    assert_eq!(advertisement.port, config.storage.server.rdma.port);
    assert!(capability.compatible(&advertisement.capability).is_ok());
    let mut client_config = config.as_ref().clone();
    client_config.storage.server.rdma.chunk_size = bytesize::ByteSize::mib(1);
    client_config.storage.server.rdma.max_inflight_chunks = 2;
    let client = RDMAClient::new(
        Arc::new(client_config),
        fabric.clone(),
        capability,
        addr.clone(),
    );
    for _ in 0..2 {
        let (mut reader, offset, got_digest) = client.download_piece(0, task_id).await.unwrap();
        assert_eq!(offset, 0);
        assert_eq!(got_digest, digest);
        let mut downloaded = Vec::new();
        reader.read_to_end(&mut downloaded).await.unwrap();
        assert_eq!(downloaded, content);
        // Consuming the stream lets the bounded receive-window registration return to the pool.
        drop(reader);
    }
    let stats = fabric.buffer_pool_stats();
    assert!(stats.misses <= 3);
    assert!(stats.hits >= 1);
    assert!(stats.cached_buffers <= 3);
    assert!(stats.cached_bytes <= 3 * 2 * 1024 * 1024);
    assert!(stats.cached_bytes < content.len());

    shutdown.trigger();
}

#[tokio::test(flavor = "multi_thread")]
async fn downloads_persistent_namespaces_over_rdma() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    config.storage.server.rdma.chunk_size = bytesize::ByteSize::kib(4);
    config.storage.server.rdma.max_inflight_chunks = 2;
    let config = Arc::new(config);
    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );
    let persistent_task_id = "9269ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
    let persistent_cache_task_id =
        "9369ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
    let persistent_content: Vec<u8> = (0..12_345).map(|index| (index % 239) as u8).collect();
    let persistent_cache_content: Vec<u8> = (0..15_432).map(|index| (index % 233) as u8).collect();
    let persistent_digest =
        write_persistent_piece(&storage, persistent_task_id, 0, &persistent_content).await;
    let persistent_cache_digest = write_persistent_cache_piece(
        &storage,
        persistent_cache_task_id,
        0,
        &persistent_cache_content,
    )
    .await;
    let (_tcp_addr, addr, shutdown, _shutdown_complete_rx) =
        start_server(config.clone(), storage).await;
    let (fabric, capability) = client_fabric(FABRIC_TAG);
    let client = RDMAClient::new(config, fabric, capability, addr);

    let (mut reader, offset, digest) = client
        .download_persistent_piece(0, persistent_task_id)
        .await
        .unwrap();
    let mut downloaded = Vec::new();
    reader.read_to_end(&mut downloaded).await.unwrap();
    assert_eq!(offset, 0);
    assert_eq!(digest, persistent_digest);
    assert_eq!(downloaded, persistent_content);

    let (mut reader, offset, digest) = client
        .download_persistent_cache_piece(0, persistent_cache_task_id)
        .await
        .unwrap();
    let mut downloaded = Vec::new();
    reader.read_to_end(&mut downloaded).await.unwrap();
    assert_eq!(offset, 0);
    assert_eq!(digest, persistent_cache_digest);
    assert_eq!(downloaded, persistent_cache_content);

    shutdown.trigger();
}

#[tokio::test(flavor = "multi_thread")]
async fn downloads_piece_with_single_window_registration_budget() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    // A two-window ring would need 4 MiB with the client settings below. Restricting the server
    // to 2 MiB exercises safe sequential reuse of one registered window.
    config.storage.server.rdma.max_registered_bytes = bytesize::ByteSize::mib(2);
    let config = Arc::new(config);
    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );
    let task_id = "a169ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
    let content = vec![0x7c; 5 * 1024 * 1024];
    let digest = write_piece(&storage, task_id, 0, &content).await;
    let (_tcp_addr, addr, shutdown, _shutdown_complete_rx) =
        start_server(config.clone(), storage).await;

    let (fabric, capability) = client_fabric(FABRIC_TAG);
    let mut client_config = config.as_ref().clone();
    client_config.storage.server.rdma.chunk_size = bytesize::ByteSize::mib(1);
    client_config.storage.server.rdma.max_inflight_chunks = 2;
    let client = RDMAClient::new(Arc::new(client_config), fabric, capability, addr);
    let (mut reader, offset, got_digest) = client.download_piece(0, task_id).await.unwrap();
    let mut downloaded = Vec::new();
    reader.read_to_end(&mut downloaded).await.unwrap();
    assert_eq!(offset, 0);
    assert_eq!(got_digest, digest);
    assert_eq!(downloaded, content);

    shutdown.trigger();
}

#[tokio::test(flavor = "multi_thread")]
async fn downloads_piece_across_window_boundaries() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    let config = Arc::new(config);
    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );
    let task_id = "b269ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
    let content: Vec<u8> = (0..3 * 1024 * 1024 + 123)
        .map(|index| (index % 251) as u8)
        .collect();
    let digest = write_piece(&storage, task_id, 0, &content).await;
    let (_tcp_addr, addr, shutdown, _shutdown_complete_rx) =
        start_server(config.clone(), storage).await;
    let (fabric, capability) = client_fabric(FABRIC_TAG);

    for (chunk_size, max_inflight_chunks) in [
        (bytesize::ByteSize::kib(64), 1),
        (bytesize::ByteSize::kib(64), 4),
        (bytesize::ByteSize::kib(64), 16),
        (bytesize::ByteSize::mib(1), 2),
        (bytesize::ByteSize::mib(1), 3),
    ] {
        let mut client_config = config.as_ref().clone();
        client_config.storage.server.rdma.chunk_size = chunk_size;
        client_config.storage.server.rdma.max_inflight_chunks = max_inflight_chunks;
        let client = RDMAClient::new(
            Arc::new(client_config),
            fabric.clone(),
            capability.clone(),
            addr.clone(),
        );
        let (mut reader, offset, got_digest) = client.download_piece(0, task_id).await.unwrap();
        let mut downloaded = Vec::new();
        reader.read_to_end(&mut downloaded).await.unwrap();
        assert_eq!(offset, 0);
        assert_eq!(got_digest, digest);
        assert_eq!(downloaded, content);
    }

    shutdown.trigger();
}

#[tokio::test(flavor = "multi_thread")]
async fn falls_back_when_fabric_tags_mismatch() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    let config = Arc::new(config);

    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );

    let task_id = "c869ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
    write_piece(&storage, task_id, 0, b"content").await;

    let (_tcp_addr, addr, shutdown, _shutdown_complete_rx) =
        start_server(config.clone(), storage).await;

    // A downloader from a different reachability domain must be refused with an
    // incompatibility error (which the downloader maps to TCP fallback).
    let (fabric, capability) = client_fabric("other-fabric");
    let client = RDMAClient::new(config.clone(), fabric, capability, addr.clone());
    let err = client.download_piece(0, task_id).await.unwrap_err();
    assert!(
        matches!(err, Error::Unsupported(_)),
        "expected Unsupported, got: {err:?}"
    );

    shutdown.trigger();
}

#[tokio::test(flavor = "multi_thread")]
async fn reports_missing_piece() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    let config = Arc::new(config);

    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );

    let (_tcp_addr, addr, shutdown, _shutdown_complete_rx) =
        start_server(config.clone(), storage).await;

    let (fabric, capability) = client_fabric(FABRIC_TAG);
    let client = RDMAClient::new(config.clone(), fabric, capability, addr.clone());
    let err = client
        .download_piece(
            0,
            "d769ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4",
        )
        .await
        .unwrap_err();
    assert!(
        !matches!(err, Error::Unsupported(_)),
        "a missing piece must not mark the parent incompatible: {err:?}"
    );

    shutdown.trigger();
}

#[tokio::test(flavor = "multi_thread")]
async fn reports_control_error_while_receive_is_pending() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let (mut reader, mut writer) = stream.into_split();
        let request = match read_frame(&mut reader).await.unwrap() {
            Frame::Request(request) => request,
            frame => panic!("expected Request, got {frame:?}"),
        };
        write_frame(
            &mut writer,
            &Frame::Ready(PieceReady {
                offset: 0,
                length: 4096,
                digest: "crc32:00000000".to_string(),
                server_endpoint: vec![1],
                chunk_size: request.chunk_size.min(4096),
                max_inflight_chunks: 1,
            }),
        )
        .await
        .unwrap();
        assert!(matches!(
            read_frame(&mut reader).await.unwrap(),
            Frame::RecvPosted {
                start_chunk: 0,
                chunk_count: 1
            }
        ));
        write_frame(
            &mut writer,
            &Frame::Error(RendezvousError {
                code: ERROR_CODE_INTERNAL,
                message: "injected staging failure".to_string(),
            }),
        )
        .await
        .unwrap();
    });

    let mut config = test_config();
    config.storage.server.rdma.chunk_size = bytesize::ByteSize::kib(4);
    config.storage.server.rdma.max_inflight_chunks = 1;
    config.storage.server.rdma.transfer_timeout = Duration::from_secs(5);
    let (fabric, capability) = client_fabric(FABRIC_TAG);
    let client = RDMAClient::new(Arc::new(config), fabric, capability, addr.to_string());

    let (mut piece_reader, _, _) = client
        .download_piece(0, "control-error-task")
        .await
        .expect("stream setup should complete after Ready");
    let mut content = Vec::new();
    let err = tokio::time::timeout(
        Duration::from_secs(1),
        piece_reader.read_to_end(&mut content),
    )
    .await
    .expect("control error should interrupt the pending fabric receive")
    .unwrap_err();
    assert!(
        err.to_string().contains("injected staging failure"),
        "unexpected error: {err}"
    );
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_out_of_order_receive_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    let config = Arc::new(config);
    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );
    let task_id = "e869ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
    write_piece(&storage, task_id, 0, &[0x5a; 8192]).await;
    let (_tcp_addr, addr, shutdown, _shutdown_complete_rx) = start_server(config, storage).await;

    let (fabric, capability) = client_fabric(FABRIC_TAG);
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();
    write_frame(
        &mut writer,
        &Frame::Request(PieceRequest {
            kind: PieceKind::Piece,
            task_id: task_id.to_string(),
            piece_number: 0,
            capability,
            client_endpoint: fabric.local_endpoint().to_vec(),
            tag: fabric.next_tag().unwrap(),
            chunk_size: 4096,
            max_inflight_chunks: 1,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(
        read_frame(&mut reader).await.unwrap(),
        Frame::Ready(_)
    ));

    write_frame(
        &mut writer,
        &Frame::RecvPosted {
            start_chunk: 1,
            chunk_count: 1,
        },
    )
    .await
    .unwrap();
    let frame = tokio::time::timeout(Duration::from_secs(1), read_frame(&mut reader))
        .await
        .expect("server should reject the invalid window immediately")
        .unwrap();
    match frame {
        Frame::Error(err) => {
            assert_eq!(err.code, ERROR_CODE_INTERNAL);
            assert!(err.message.contains("invalid rdma receive window"));
        }
        frame => panic!("expected Error, got {frame:?}"),
    }

    shutdown.trigger();
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_a_wrapping_transfer_tag_range() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    let config = Arc::new(config);
    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );
    let task_id = "d869ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
    write_piece(&storage, task_id, 0, &[0x6b; 8192]).await;
    let (_tcp_addr, addr, shutdown, _shutdown_complete_rx) = start_server(config, storage).await;

    let (fabric, capability) = client_fabric(FABRIC_TAG);
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();
    write_frame(
        &mut writer,
        &Frame::Request(PieceRequest {
            kind: PieceKind::Piece,
            task_id: task_id.to_string(),
            piece_number: 0,
            capability,
            client_endpoint: fabric.local_endpoint().to_vec(),
            tag: u64::MAX,
            chunk_size: 4096,
            max_inflight_chunks: 2,
        }),
    )
    .await
    .unwrap();

    let frame = tokio::time::timeout(Duration::from_secs(1), read_frame(&mut reader))
        .await
        .expect("server should reject a wrapping tag range immediately")
        .unwrap();
    match frame {
        Frame::Error(err) => {
            assert_eq!(err.code, ERROR_CODE_INTERNAL);
            assert!(err.message.contains("tag range wraps around"));
        }
        frame => panic!("expected Error, got {frame:?}"),
    }

    shutdown.trigger();
}

#[tokio::test(flavor = "multi_thread")]
async fn times_out_stalled_piece_transfer() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    config.download.piece_timeout = Duration::from_millis(200);
    config.storage.server.rdma.transfer_timeout = Duration::from_secs(5);
    let config = Arc::new(config);
    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );
    let task_id = "f869ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
    write_piece(&storage, task_id, 0, &[0x31; 8192]).await;
    let (_tcp_addr, addr, shutdown, _shutdown_complete_rx) = start_server(config, storage).await;

    let (fabric, capability) = client_fabric(FABRIC_TAG);
    let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();
    write_frame(
        &mut writer,
        &Frame::Request(PieceRequest {
            kind: PieceKind::Piece,
            task_id: task_id.to_string(),
            piece_number: 0,
            capability,
            client_endpoint: fabric.local_endpoint().to_vec(),
            tag: fabric.next_tag().unwrap(),
            chunk_size: 4096,
            max_inflight_chunks: 1,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(
        read_frame(&mut reader).await.unwrap(),
        Frame::Ready(_)
    ));

    let frame = tokio::time::timeout(Duration::from_secs(1), read_frame(&mut reader))
        .await
        .expect("server should time out a client that never posts receives")
        .unwrap();
    match frame {
        Frame::Error(err) => assert!(err.message.contains("piece transfer timed out")),
        frame => panic!("expected Error, got {frame:?}"),
    }

    shutdown.trigger();
}

#[tokio::test(flavor = "multi_thread")]
async fn rejects_connections_over_the_transfer_admission_limit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut config = test_config();
    assign_free_ports(&mut config);
    config.storage.server.rdma.max_concurrent_transfers = 1;
    config.storage.server.rdma.transfer_timeout = Duration::from_secs(5);
    let config = Arc::new(config);
    let storage = Arc::new(
        Storage::new(
            config.clone(),
            temp_dir.path(),
            temp_dir.path().to_path_buf(),
        )
        .await
        .unwrap(),
    );
    let task_id = "f869ba82f1ba1c1c5eb27f0b7aa051dcaf72e9a8dd574a04e60247f8d0a5f2b4";
    write_piece(&storage, task_id, 0, &[0x31; 8192]).await;
    let (_tcp_addr, addr, shutdown, _shutdown_complete_rx) = start_server(config, storage).await;

    // Take the sole admission permit, and read far enough to know the server has it. Merely
    // opening a connection would leave the test racing the accept loop.
    let (fabric, capability) = client_fabric(FABRIC_TAG);
    let held = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let (mut held_reader, mut held_writer) = held.into_split();
    write_frame(
        &mut held_writer,
        &Frame::Request(PieceRequest {
            kind: PieceKind::Piece,
            task_id: task_id.to_string(),
            piece_number: 0,
            capability,
            client_endpoint: fabric.local_endpoint().to_vec(),
            tag: fabric.next_tag().unwrap(),
            chunk_size: 4096,
            max_inflight_chunks: 1,
        }),
    )
    .await
    .unwrap();
    assert!(matches!(
        read_frame(&mut held_reader).await.unwrap(),
        Frame::Ready(_)
    ));

    // The listener rejects overload rather than spawning an unbounded waiting task, and says so
    // explicitly: a bare close would look to the client like a parent whose fabric is broken.
    let second = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let (mut reader, _writer) = second.into_split();
    let frame = tokio::time::timeout(Duration::from_secs(1), read_frame(&mut reader))
        .await
        .expect("over-limit connection should be answered immediately")
        .unwrap();
    match frame {
        Frame::Error(err) => {
            assert_eq!(err.code, ERROR_CODE_BUSY);
            assert!(err.message.contains("admission"));
        }
        frame => panic!("expected Error, got {frame:?}"),
    }

    shutdown.trigger();
}
