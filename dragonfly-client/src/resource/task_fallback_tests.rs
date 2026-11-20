/*
 * Copyright 2026 The Dragonfly Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use super::*;
use crate::dynconfig::Dynconfig;
use dragonfly_api::common::v2 as common;
use dragonfly_api::scheduler::v2::{self as scheduler, scheduler_server};
use tempfile::{tempdir, TempDir};
use tokio::io::AsyncWriteExt;
use tokio_stream::wrappers::TcpListenerStream;

#[derive(Clone, Default)]
struct TestScheduler {
    requests: Arc<Mutex<Vec<AnnouncePeerRequest>>>,
    reject_registration: bool,
    reject_completion: bool,
    stall_completion: bool,
}

// Generate the unused RPC stubs together with the impl so async_trait sees them.
macro_rules! scheduler_impl {
    ($($method:ident: $request:ty => $response:ty),* $(,)?) => {
        #[tonic::async_trait]
        impl scheduler_server::Scheduler for TestScheduler {
            type AnnouncePeerStream = ReceiverStream<Result<scheduler::AnnouncePeerResponse, Status>>;
            type AnnounceCachePeerStream = ReceiverStream<Result<scheduler::AnnounceCachePeerResponse, Status>>;
            type AnnouncePersistentPeerStream = ReceiverStream<Result<scheduler::AnnouncePersistentPeerResponse, Status>>;
            type AnnouncePersistentCachePeerStream = ReceiverStream<Result<scheduler::AnnouncePersistentCachePeerResponse, Status>>;

            async fn announce_peer(
                &self,
                request: Request<tonic::Streaming<AnnouncePeerRequest>>,
            ) -> Result<tonic::Response<Self::AnnouncePeerStream>, Status> {
                let this = self.clone();
                let mut incoming = request.into_inner();
                let (tx, rx) = mpsc::channel(4);
                tokio::spawn(async move {
                    while let Some(message) = incoming.message().await.unwrap() {
                        let event = message.request.clone().unwrap();
                        this.requests.lock().await.push(message);
                        match event {
                            announce_peer_request::Request::RegisterPeerRequest(register) => {
                                let cached = register.download.unwrap().metadata_only;
                                if !cached && this.reject_registration {
                                    tx.send(Err(Status::unavailable("registration rejected"))).await.unwrap();
                                    return;
                                }
                                let response = if cached {
                                    announce_peer_response::Response::MetadataOnlyResponse(scheduler::MetadataOnlyResponse {})
                                } else {
                                    announce_peer_response::Response::NeedBackToSourceResponse(scheduler::NeedBackToSourceResponse { description: None })
                                };
                                tx.send(Ok(scheduler::AnnouncePeerResponse { response: Some(response) })).await.unwrap();
                            }
                            announce_peer_request::Request::DownloadPeerBackToSourceFailedRequest(_) => return,
                            announce_peer_request::Request::DownloadPeerBackToSourceFinishedRequest(_)
                            | announce_peer_request::Request::DownloadPeerFinishedRequest(_) => {
                                if this.stall_completion {
                                    // Hold the response stream open past the client's deadline.
                                    let _tx = tx;
                                    std::future::pending::<()>().await;
                                } else if this.reject_completion {
                                    tx.send(Err(Status::internal("completion rejected"))).await.unwrap();
                                }
                                return;
                            }
                            _ => {}
                        }
                    }
                });
                Ok(tonic::Response::new(ReceiverStream::new(rx)))
            }

            $(
                async fn $method(&self, _: Request<$request>) -> Result<tonic::Response<$response>, Status> {
                    Err(Status::unimplemented("unused test RPC"))
                }
            )*

            async fn announce_cache_peer(&self, _: Request<tonic::Streaming<scheduler::AnnounceCachePeerRequest>>) -> Result<tonic::Response<Self::AnnounceCachePeerStream>, Status> {
                Err(Status::unimplemented("unused test RPC"))
            }
            async fn announce_persistent_peer(&self, _: Request<tonic::Streaming<scheduler::AnnouncePersistentPeerRequest>>) -> Result<tonic::Response<Self::AnnouncePersistentPeerStream>, Status> {
                Err(Status::unimplemented("unused test RPC"))
            }
            async fn announce_persistent_cache_peer(&self, _: Request<tonic::Streaming<scheduler::AnnouncePersistentCachePeerRequest>>) -> Result<tonic::Response<Self::AnnouncePersistentCachePeerStream>, Status> {
                Err(Status::unimplemented("unused test RPC"))
            }
        }
    };
}

scheduler_impl! {
    stat_peer: scheduler::StatPeerRequest => common::Peer,
    delete_peer: scheduler::DeletePeerRequest => (),
    stat_task: scheduler::StatTaskRequest => common::Task,
    delete_task: scheduler::DeleteTaskRequest => (),
    announce_host: scheduler::AnnounceHostRequest => (),
    list_hosts: scheduler::ListHostsRequest => scheduler::ListHostsResponse,
    delete_host: scheduler::DeleteHostRequest => (),
    stat_cache_peer: scheduler::StatCachePeerRequest => common::CachePeer,
    delete_cache_peer: scheduler::DeleteCachePeerRequest => (),
    stat_cache_task: scheduler::StatCacheTaskRequest => common::CacheTask,
    delete_cache_task: scheduler::DeleteCacheTaskRequest => (),
    stat_persistent_peer: scheduler::StatPersistentPeerRequest => common::PersistentPeer,
    delete_persistent_peer: scheduler::DeletePersistentPeerRequest => (),
    upload_persistent_task_started: scheduler::UploadPersistentTaskStartedRequest => (),
    upload_persistent_task_finished: scheduler::UploadPersistentTaskFinishedRequest => common::PersistentTask,
    upload_persistent_task_failed: scheduler::UploadPersistentTaskFailedRequest => (),
    stat_persistent_task: scheduler::StatPersistentTaskRequest => common::PersistentTask,
    delete_persistent_task: scheduler::DeletePersistentTaskRequest => (),
    stat_persistent_cache_peer: scheduler::StatPersistentCachePeerRequest => common::PersistentCachePeer,
    delete_persistent_cache_peer: scheduler::DeletePersistentCachePeerRequest => (),
    upload_persistent_cache_task_started: scheduler::UploadPersistentCacheTaskStartedRequest => (),
    upload_persistent_cache_task_finished: scheduler::UploadPersistentCacheTaskFinishedRequest => common::PersistentCacheTask,
    upload_persistent_cache_task_failed: scheduler::UploadPersistentCacheTaskFailedRequest => (),
    stat_persistent_cache_task: scheduler::StatPersistentCacheTaskRequest => common::PersistentCacheTask,
    delete_persistent_cache_task: scheduler::DeletePersistentCacheTaskRequest => (),
    preheat_image: scheduler::PreheatImageRequest => (),
    stat_image: scheduler::StatImageRequest => scheduler::StatImageResponse,
    preheat_file: scheduler::PreheatFileRequest => (),
    stat_file: scheduler::StatFileRequest => scheduler::StatFileResponse,
}

struct Fixture {
    manager: Task,
    task: metadata::Task,
    scheduler: TestScheduler,
    server: tokio::task::JoinHandle<()>,
    _dir: TempDir,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.server.abort();
    }
}

impl Fixture {
    async fn new(scheduler: TestScheduler) -> Self {
        let dir = tempdir().unwrap();
        let log_dir = dir.path().join("log");
        std::fs::create_dir_all(&log_dir).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (health, health_service) = tonic_health::server::health_reporter();
        health
            .set_service_status("", tonic_health::ServingStatus::Serving)
            .await;
        let service = scheduler_server::SchedulerServer::new(scheduler.clone());
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(health_service)
                .add_service(service)
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        let path = dir.path().join("dynconfig.yaml");
        tokio::fs::write(&path, format!("scheduler:\n  addrs:\n    - '{addr}'\n"))
            .await
            .unwrap();
        let mut config = Config::default();
        config.manager.addr = None;
        config.backend.max_retries = 0;
        let config = Arc::new(config);
        let shutdown = shutdown::Shutdown::new();
        let (shutdown_tx, _shutdown_rx) = mpsc::unbounded_channel();
        let dynconfig = Arc::new(
            Dynconfig::new(config.clone(), path, shutdown.clone(), shutdown_tx.clone())
                .await
                .unwrap(),
        );
        let client = Arc::new(
            SchedulerClient::new(config.clone(), dynconfig)
                .await
                .unwrap(),
        );
        let storage = Arc::new(
            Storage::new(config.clone(), dir.path(), log_dir)
                .await
                .unwrap(),
        );
        storage
            .download_task_started("fallback-task", 4096, 4096, None)
            .await
            .unwrap();
        let task = storage.get_task("fallback-task").unwrap().unwrap();
        let manager = Task::new(
            config.clone(),
            Arc::new(IDGenerator::new(
                "127.0.0.1".into(),
                "test-host".into(),
                false,
            )),
            storage,
            client,
            Arc::new(BackendFactory::new(config, None).unwrap()),
            Arc::new(RateLimiter::builder().initial(1 << 20).build()),
            Arc::new(RateLimiter::builder().initial(1 << 20).build()),
            Arc::new(RateLimiter::builder().initial(1 << 20).build()),
            shutdown,
            shutdown_tx,
        )
        .unwrap();
        Self {
            manager,
            task,
            scheduler,
            server,
            _dir: dir,
        }
    }
}

async fn source(
    truncate_first: bool,
) -> (
    String,
    Arc<std::sync::atomic::AtomicUsize>,
    tokio::task::JoinHandle<()>,
) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let requests = count.clone();
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut header = Vec::new();
            while !header.ends_with(b"\r\n\r\n") {
                header.push(socket.read_u8().await.unwrap());
            }
            let first = requests.fetch_add(1, Ordering::SeqCst) == 0;
            socket.write_all(b"HTTP/1.1 206 Partial Content\r\nContent-Length: 4096\r\nContent-Range: bytes 0-4095/4096\r\nConnection: close\r\n\r\n").await.unwrap();
            let length = if truncate_first && first { 2048 } else { 4096 };
            socket.write_all(&vec![42; length]).await.unwrap();
            socket.shutdown().await.unwrap();
        }
    });
    (format!("http://{addr}/file"), count, server)
}

fn assert_fallback_report(requests: &[AnnouncePeerRequest], range: Option<common::Range>) {
    let reports: Vec<_> = requests
        .iter()
        .filter(|message| message.peer_id != "failed-peer")
        .collect();
    assert_eq!(reports.len(), 3, "{requests:?}");
    assert!(reports
        .iter()
        .all(|message| message.peer_id == reports[0].peer_id
            && message.host_id == "test-host"
            && message.task_id == "fallback-task"));
    let Some(announce_peer_request::Request::RegisterPeerRequest(register)) = &reports[0].request
    else {
        panic!("expected register");
    };
    let download = register.download.as_ref().unwrap();
    assert!(download.metadata_only);
    assert!(!download.need_back_to_source);
    assert_eq!(download.range, range);
    assert_eq!(download.actual_content_length, Some(4096));
    assert_eq!(download.actual_piece_length, Some(4096));
    assert_eq!(download.actual_piece_count, Some(1));
    assert!(matches!(
        reports[1].request,
        Some(announce_peer_request::Request::DownloadPeerBackToSourceStartedRequest(_))
    ));
    assert!(matches!(
        reports[2].request,
        Some(announce_peer_request::Request::DownloadPeerBackToSourceFinishedRequest(_))
    ));
}

#[tokio::test]
async fn source_failure_then_fallback_announces_new_peer() {
    let fixture = Fixture::new(TestScheduler::default()).await;
    let (url, count, server) = source(true).await;
    let (progress, _rx) = mpsc::channel(16);
    fixture
        .manager
        .download(
            &fixture.task,
            "test-host",
            "failed-peer",
            Download {
                url,
                scheduling_policy: SchedulingPolicy::Always as i32,
                ..Default::default()
            },
            progress,
        )
        .await
        .unwrap();
    server.abort();
    assert_eq!(count.load(Ordering::SeqCst), 2);
    let requests = fixture.scheduler.requests.lock().await;
    assert!(requests.iter().any(|message| matches!(
        message.request,
        Some(announce_peer_request::Request::DownloadPeerBackToSourceFailedRequest(_))
    )));
    assert_fallback_report(&requests, None);
    assert!(fixture
        .manager
        .storage
        .get_piece(&fixture.manager.piece.id("fallback-task", 0))
        .unwrap()
        .unwrap()
        .is_finished());
}

#[tokio::test]
async fn registration_failure_then_fallback_announces_new_peer() {
    let fixture = Fixture::new(TestScheduler {
        reject_registration: true,
        ..Default::default()
    })
    .await;
    let (url, count, server) = source(false).await;
    let (progress, _rx) = mpsc::channel(16);
    fixture
        .manager
        .download(
            &fixture.task,
            "test-host",
            "failed-peer",
            Download {
                url,
                scheduling_policy: SchedulingPolicy::Always as i32,
                ..Default::default()
            },
            progress,
        )
        .await
        .unwrap();
    server.abort();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_fallback_report(&fixture.scheduler.requests.lock().await, None);
}

#[tokio::test]
async fn range_fallback_keeps_range_in_registration() {
    let fixture = Fixture::new(TestScheduler::default()).await;
    let range = Some(common::Range {
        start: 1024,
        length: 1024,
    });
    fixture
        .manager
        .announce_fallback_finished(
            &fixture.task,
            "test-host",
            "failed-peer",
            Download {
                range,
                ..Default::default()
            },
        )
        .await;
    assert_fallback_report(&fixture.scheduler.requests.lock().await, range);
}

#[tokio::test]
async fn completion_rejection_does_not_fail_download() {
    let fixture = Fixture::new(TestScheduler {
        reject_registration: true,
        reject_completion: true,
        ..Default::default()
    })
    .await;
    let (url, _, server) = source(false).await;
    let (progress, _rx) = mpsc::channel(16);
    fixture
        .manager
        .download(
            &fixture.task,
            "test-host",
            "failed-peer",
            Download {
                url,
                scheduling_policy: SchedulingPolicy::Always as i32,
                ..Default::default()
            },
            progress,
        )
        .await
        .unwrap();
    server.abort();
    assert_fallback_report(&fixture.scheduler.requests.lock().await, None);
    // The reporting helper must still expose the final gRPC error to its caller.
    assert!(fixture
        .manager
        .announce_completed_peer(
            &fixture.task,
            "test-host",
            "another-peer",
            Download::default(),
            true
        )
        .await
        .is_err());
}

#[tokio::test]
async fn completion_timeout_is_bounded() {
    let fixture = Fixture::new(TestScheduler {
        stall_completion: true,
        ..Default::default()
    })
    .await;
    let start = Instant::now();
    fixture
        .manager
        .announce_fallback_finished(
            &fixture.task,
            "test-host",
            "failed-peer",
            Download::default(),
        )
        .await;
    assert!(start.elapsed() < REQUEST_TIMEOUT + Duration::from_secs(2));
    assert_fallback_report(&fixture.scheduler.requests.lock().await, None);
}

#[tokio::test]
async fn failed_fallback_does_not_announce_success() {
    let fixture = Fixture::new(TestScheduler {
        reject_registration: true,
        ..Default::default()
    })
    .await;
    let (url, count, server) = source(true).await;
    let (progress, _rx) = mpsc::channel(16);
    let result = fixture
        .manager
        .download(
            &fixture.task,
            "test-host",
            "failed-peer",
            Download {
                url,
                scheduling_policy: SchedulingPolicy::Always as i32,
                ..Default::default()
            },
            progress,
        )
        .await;
    server.abort();
    assert!(result.is_err());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(fixture
        .scheduler
        .requests
        .lock()
        .await
        .iter()
        .all(|message| message.peer_id == "failed-peer"));
}

#[tokio::test]
async fn intentional_scheduler_bypass_does_not_announce() {
    let fixture = Fixture::new(TestScheduler::default()).await;
    let (url, count, server) = source(false).await;
    let (progress, _rx) = mpsc::channel(16);
    fixture
        .manager
        .download(
            &fixture.task,
            "test-host",
            "failed-peer",
            Download {
                url,
                ..Default::default()
            },
            progress,
        )
        .await
        .unwrap();
    server.abort();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(fixture.scheduler.requests.lock().await.is_empty());
}

#[tokio::test]
async fn local_cache_announcement_keeps_normal_peer_events() {
    let fixture = Fixture::new(TestScheduler::default()).await;
    fixture
        .manager
        .download_partial_with_scheduler_from_local(
            &fixture.task,
            "test-host",
            "cached-peer",
            Download::default(),
        )
        .await
        .unwrap();
    let requests = fixture.scheduler.requests.lock().await;
    assert_eq!(requests.len(), 3);
    assert!(matches!(
        requests[1].request,
        Some(announce_peer_request::Request::DownloadPeerStartedRequest(
            _
        ))
    ));
    assert!(matches!(
        requests[2].request,
        Some(announce_peer_request::Request::DownloadPeerFinishedRequest(
            _
        ))
    ));
}
