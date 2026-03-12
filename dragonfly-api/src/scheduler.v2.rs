/// RegisterPeerRequest represents peer registered request of AnnouncePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterPeerRequest {
    /// Download information.
    #[prost(message, optional, tag = "1")]
    pub download: ::core::option::Option<super::super::common::v2::Download>,
}
/// DownloadPeerStartedRequest represents peer download started request of AnnouncePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPeerStartedRequest {}
/// DownloadPeerBackToSourceStartedRequest represents peer download back-to-source started request of AnnouncePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPeerBackToSourceStartedRequest {
    /// The description of the back-to-source reason.
    #[prost(string, optional, tag = "1")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// ReschedulePeerRequest represents reschedule request of AnnouncePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReschedulePeerRequest {
    /// Candidate parent ids.
    #[prost(message, repeated, tag = "1")]
    pub candidate_parents: ::prost::alloc::vec::Vec<super::super::common::v2::Peer>,
    /// The description of the reschedule reason.
    #[prost(string, optional, tag = "2")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// DownloadPeerFinishedRequest represents peer download finished request of AnnouncePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPeerFinishedRequest {}
/// DownloadPeerBackToSourceFinishedRequest represents peer download back-to-source finished request of AnnouncePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPeerBackToSourceFinishedRequest {}
/// DownloadPeerFailedRequest represents peer download failed request of AnnouncePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPeerFailedRequest {
    /// The description of the download failed.
    #[prost(string, optional, tag = "1")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// DownloadPeerBackToSourceFailedRequest represents peer download back-to-source failed request of AnnouncePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPeerBackToSourceFailedRequest {
    /// The description of the download back-to-source failed.
    #[prost(string, optional, tag = "1")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// DownloadPieceFinishedRequest represents piece download finished request of AnnouncePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPieceFinishedRequest {
    /// Piece info.
    #[prost(message, optional, tag = "1")]
    pub piece: ::core::option::Option<super::super::common::v2::Piece>,
}
/// DownloadPieceBackToSourceFinishedRequest represents piece download back-to-source finished request of AnnouncePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPieceBackToSourceFinishedRequest {
    /// Piece info.
    #[prost(message, optional, tag = "1")]
    pub piece: ::core::option::Option<super::super::common::v2::Piece>,
}
/// DownloadPieceFailedRequest downloads piece failed request of AnnouncePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPieceFailedRequest {
    /// Piece number.
    #[prost(uint32, optional, tag = "1")]
    pub piece_number: ::core::option::Option<u32>,
    /// Parent id.
    #[prost(string, tag = "2")]
    pub parent_id: ::prost::alloc::string::String,
    /// Temporary indicates whether the error is temporary.
    #[prost(bool, tag = "3")]
    pub temporary: bool,
}
/// DownloadPieceBackToSourceFailedRequest downloads piece back-to-source failed request of AnnouncePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DownloadPieceBackToSourceFailedRequest {
    /// Piece number.
    #[prost(uint32, optional, tag = "1")]
    pub piece_number: ::core::option::Option<u32>,
    #[prost(
        oneof = "download_piece_back_to_source_failed_request::Response",
        tags = "2, 3"
    )]
    pub response: ::core::option::Option<
        download_piece_back_to_source_failed_request::Response,
    >,
}
/// Nested message and enum types in `DownloadPieceBackToSourceFailedRequest`.
pub mod download_piece_back_to_source_failed_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Response {
        #[prost(message, tag = "2")]
        Backend(super::super::super::errordetails::v2::Backend),
        #[prost(message, tag = "3")]
        Unknown(super::super::super::errordetails::v2::Unknown),
    }
}
/// AnnouncePeerRequest represents request of AnnouncePeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnnouncePeerRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
    #[prost(
        oneof = "announce_peer_request::Request",
        tags = "4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15"
    )]
    pub request: ::core::option::Option<announce_peer_request::Request>,
}
/// Nested message and enum types in `AnnouncePeerRequest`.
pub mod announce_peer_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[allow(clippy::large_enum_variant)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Request {
        #[prost(message, tag = "4")]
        RegisterPeerRequest(super::RegisterPeerRequest),
        #[prost(message, tag = "5")]
        DownloadPeerStartedRequest(super::DownloadPeerStartedRequest),
        #[prost(message, tag = "6")]
        DownloadPeerBackToSourceStartedRequest(
            super::DownloadPeerBackToSourceStartedRequest,
        ),
        #[prost(message, tag = "7")]
        ReschedulePeerRequest(super::ReschedulePeerRequest),
        #[prost(message, tag = "8")]
        DownloadPeerFinishedRequest(super::DownloadPeerFinishedRequest),
        #[prost(message, tag = "9")]
        DownloadPeerBackToSourceFinishedRequest(
            super::DownloadPeerBackToSourceFinishedRequest,
        ),
        #[prost(message, tag = "10")]
        DownloadPeerFailedRequest(super::DownloadPeerFailedRequest),
        #[prost(message, tag = "11")]
        DownloadPeerBackToSourceFailedRequest(
            super::DownloadPeerBackToSourceFailedRequest,
        ),
        #[prost(message, tag = "12")]
        DownloadPieceFinishedRequest(super::DownloadPieceFinishedRequest),
        #[prost(message, tag = "13")]
        DownloadPieceBackToSourceFinishedRequest(
            super::DownloadPieceBackToSourceFinishedRequest,
        ),
        #[prost(message, tag = "14")]
        DownloadPieceFailedRequest(super::DownloadPieceFailedRequest),
        #[prost(message, tag = "15")]
        DownloadPieceBackToSourceFailedRequest(
            super::DownloadPieceBackToSourceFailedRequest,
        ),
    }
}
/// EmptyTaskResponse represents empty task response of AnnouncePeerResponse.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct EmptyTaskResponse {}
/// NormalTaskResponse represents normal task response of AnnouncePeerResponse.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NormalTaskResponse {
    /// Candidate parents.
    #[prost(message, repeated, tag = "1")]
    pub candidate_parents: ::prost::alloc::vec::Vec<super::super::common::v2::Peer>,
}
/// NeedBackToSourceResponse represents need back-to-source response of AnnouncePeerResponse.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct NeedBackToSourceResponse {
    /// The description of the back-to-source reason.
    #[prost(string, optional, tag = "1")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// AnnouncePeerResponse represents response of AnnouncePeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnnouncePeerResponse {
    #[prost(oneof = "announce_peer_response::Response", tags = "1, 2, 3")]
    pub response: ::core::option::Option<announce_peer_response::Response>,
}
/// Nested message and enum types in `AnnouncePeerResponse`.
pub mod announce_peer_response {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Response {
        #[prost(message, tag = "1")]
        EmptyTaskResponse(super::EmptyTaskResponse),
        #[prost(message, tag = "2")]
        NormalTaskResponse(super::NormalTaskResponse),
        #[prost(message, tag = "3")]
        NeedBackToSourceResponse(super::NeedBackToSourceResponse),
    }
}
/// StatPeerRequest represents request of StatPeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct StatPeerRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
}
/// DeletePeerRequest represents request of DeletePeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DeletePeerRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
}
/// StatTaskRequest represents request of StatTask.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct StatTaskRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
}
/// DeleteTaskRequest represents request of DeleteTask.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DeleteTaskRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
}
/// AnnounceHostRequest represents request of AnnounceHost.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnnounceHostRequest {
    /// Host information.
    #[prost(message, optional, tag = "1")]
    pub host: ::core::option::Option<super::super::common::v2::Host>,
    /// The interval between dfdaemon announces to scheduler.
    #[prost(message, optional, tag = "2")]
    pub interval: ::core::option::Option<::prost_wkt_types::Duration>,
}
/// ListHostsRequest represents request of ListHosts.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct ListHostsRequest {
    /// Type to filter hosts.
    #[prost(uint32, optional, tag = "1")]
    pub r#type: ::core::option::Option<u32>,
}
/// ListHostsResponse represents response of ListHosts.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListHostsResponse {
    /// Hosts info.
    #[prost(message, repeated, tag = "1")]
    pub hosts: ::prost::alloc::vec::Vec<super::super::common::v2::Host>,
}
/// DeleteHostRequest represents request of DeleteHost.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DeleteHostRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
}
/// RegisterCachePeerRequest represents cache peer registered request of AnnounceCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RegisterCachePeerRequest {
    /// Download url.
    #[prost(string, tag = "1")]
    pub url: ::prost::alloc::string::String,
    /// Digest of the task digest, for example :xxx or sha256:yyy.
    #[prost(string, optional, tag = "2")]
    pub digest: ::core::option::Option<::prost::alloc::string::String>,
    /// Range is url range of request. If protocol is http, range
    /// will set in request header. If protocol is others, range
    /// will set in range field.
    #[prost(message, optional, tag = "3")]
    pub range: ::core::option::Option<super::super::common::v2::Range>,
    /// Task type.
    #[prost(enumeration = "super::super::common::v2::TaskType", tag = "4")]
    pub r#type: i32,
    /// URL tag identifies different task for same url.
    #[prost(string, optional, tag = "5")]
    pub tag: ::core::option::Option<::prost::alloc::string::String>,
    /// Application of task.
    #[prost(string, optional, tag = "6")]
    pub application: ::core::option::Option<::prost::alloc::string::String>,
    /// Peer priority.
    #[prost(enumeration = "super::super::common::v2::Priority", tag = "7")]
    pub priority: i32,
    /// Filtered query params to generate the task id.
    /// When filter is \["Signature", "Expires", "ns"\], for example:
    /// <http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io> and <http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io>
    /// will generate the same task id.
    /// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
    #[prost(string, repeated, tag = "8")]
    pub filtered_query_params: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Task request headers.
    #[prost(map = "string, string", tag = "9")]
    pub request_header: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// Piece Length is the piece length(bytes) for downloading file. The value needs to
    /// be greater than 4MiB (4,194,304 bytes) and less than 64MiB (67,108,864 bytes),
    /// for example: 4194304(4mib), 8388608(8mib). If the piece length is not specified,
    /// the piece length will be calculated according to the file size.
    #[prost(uint64, optional, tag = "10")]
    pub piece_length: ::core::option::Option<u64>,
    /// File path to be downloaded. If output_path is set, the downloaded file will be saved to the specified path.
    /// Dfdaemon will try to create hard link to the output path before starting the download. If hard link creation fails,
    /// it will copy the file to the output path after the download is completed.
    /// For more details refer to <https://github.com/dragonflyoss/design/blob/main/systems-analysis/file-download-workflow-with-hard-link/README.md.>
    #[prost(string, optional, tag = "11")]
    pub output_path: ::core::option::Option<::prost::alloc::string::String>,
    /// Download timeout.
    #[prost(message, optional, tag = "12")]
    pub timeout: ::core::option::Option<::prost_wkt_types::Duration>,
    /// Dfdaemon cannot download the task from the source if disable_back_to_source is true.
    #[prost(bool, tag = "13")]
    pub disable_back_to_source: bool,
    /// Scheduler needs to schedule the task downloads from the source if need_back_to_source is true.
    #[prost(bool, tag = "14")]
    pub need_back_to_source: bool,
    /// certificate_chain is the client certs with DER format for the backend client to download back-to-source.
    #[prost(bytes = "vec", repeated, tag = "15")]
    pub certificate_chain: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    /// Prefetch pre-downloads all pieces of the task when the download task request is a range request.
    #[prost(bool, tag = "16")]
    pub prefetch: bool,
    /// Object storage protocol information.
    #[prost(message, optional, tag = "17")]
    pub object_storage: ::core::option::Option<super::super::common::v2::ObjectStorage>,
    /// HDFS protocol information.
    #[prost(message, optional, tag = "18")]
    pub hdfs: ::core::option::Option<super::super::common::v2::Hdfs>,
    /// is_prefetch is the flag to indicate whether the request is a prefetch request.
    #[prost(bool, tag = "19")]
    pub is_prefetch: bool,
    /// need_piece_content is the flag to indicate whether the response needs to return piece content.
    #[prost(bool, tag = "20")]
    pub need_piece_content: bool,
    /// content_for_calculating_task_id is the content used to calculate the task id.
    /// If content_for_calculating_task_id is set, use its value to calculate the task ID.
    /// Otherwise, calculate the task ID based on url, piece_length, tag, application, and filtered_query_params.
    #[prost(string, optional, tag = "21")]
    pub content_for_calculating_task_id: ::core::option::Option<
        ::prost::alloc::string::String,
    >,
    /// remote_ip represents the IP address of the client initiating the download request.
    /// For proxy requests, it is set to the IP address of the request source.
    /// For dfget requests, it is set to the IP address of the dfget.
    #[prost(string, optional, tag = "22")]
    pub remote_ip: ::core::option::Option<::prost::alloc::string::String>,
    /// concurrent_piece_count is the number of pieces that can be downloaded concurrently.
    #[prost(uint32, optional, tag = "23")]
    pub concurrent_piece_count: ::core::option::Option<u32>,
    /// Actual piece length by calculating based on the piece_length field and content length.
    #[prost(uint64, optional, tag = "24")]
    pub actual_piece_length: ::core::option::Option<u64>,
    /// Actual content length by getting from the backend.
    #[prost(uint64, optional, tag = "25")]
    pub actual_content_length: ::core::option::Option<u64>,
    /// Actual piece count by calculating.
    #[prost(uint64, optional, tag = "26")]
    pub actual_piece_count: ::core::option::Option<u64>,
}
/// DownloadCachePeerStartedRequest represents cache peer download started request of AnnounceCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadCachePeerStartedRequest {}
/// DownloadCachePeerBackToSourceStartedRequest represents cache peer download back-to-source started request of AnnounceCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadCachePeerBackToSourceStartedRequest {
    /// The description of the back-to-source reason.
    #[prost(string, optional, tag = "1")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// RescheduleCachePeerRequest represents reschedule request of AnnounceCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RescheduleCachePeerRequest {
    /// Candidate parent ids.
    #[prost(message, repeated, tag = "1")]
    pub candidate_parents: ::prost::alloc::vec::Vec<super::super::common::v2::CachePeer>,
    /// The description of the reschedule reason.
    #[prost(string, optional, tag = "2")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// DownloadCachePeerFinishedRequest represents cache peer download finished request of AnnounceCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadCachePeerFinishedRequest {}
/// DownloadCachePeerBackToSourceFinishedRequest represents cache peer download back-to-source finished request of AnnounceCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadCachePeerBackToSourceFinishedRequest {}
/// DownloadCachePeerFailedRequest represents cache peer download failed request of AnnounceCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadCachePeerFailedRequest {
    /// The description of the download failed.
    #[prost(string, optional, tag = "1")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// DownloadCachePeerBackToSourceFailedRequest represents cache peer download back-to-source failed request of AnnounceCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadCachePeerBackToSourceFailedRequest {
    /// The description of the download back-to-source failed.
    #[prost(string, optional, tag = "1")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// AnnounceCachePeerRequest represents request of AnnounceCachePeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnnounceCachePeerRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
    #[prost(
        oneof = "announce_cache_peer_request::Request",
        tags = "4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15"
    )]
    pub request: ::core::option::Option<announce_cache_peer_request::Request>,
}
/// Nested message and enum types in `AnnounceCachePeerRequest`.
pub mod announce_cache_peer_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[allow(clippy::large_enum_variant)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Request {
        #[prost(message, tag = "4")]
        RegisterCachePeerRequest(super::RegisterCachePeerRequest),
        #[prost(message, tag = "5")]
        DownloadCachePeerStartedRequest(super::DownloadCachePeerStartedRequest),
        #[prost(message, tag = "6")]
        DownloadCachePeerBackToSourceStartedRequest(
            super::DownloadCachePeerBackToSourceStartedRequest,
        ),
        #[prost(message, tag = "7")]
        RescheduleCachePeerRequest(super::RescheduleCachePeerRequest),
        #[prost(message, tag = "8")]
        DownloadCachePeerFinishedRequest(super::DownloadCachePeerFinishedRequest),
        #[prost(message, tag = "9")]
        DownloadCachePeerBackToSourceFinishedRequest(
            super::DownloadCachePeerBackToSourceFinishedRequest,
        ),
        #[prost(message, tag = "10")]
        DownloadCachePeerFailedRequest(super::DownloadCachePeerFailedRequest),
        #[prost(message, tag = "11")]
        DownloadCachePeerBackToSourceFailedRequest(
            super::DownloadCachePeerBackToSourceFailedRequest,
        ),
        #[prost(message, tag = "12")]
        DownloadPieceFinishedRequest(super::DownloadPieceFinishedRequest),
        #[prost(message, tag = "13")]
        DownloadPieceBackToSourceFinishedRequest(
            super::DownloadPieceBackToSourceFinishedRequest,
        ),
        #[prost(message, tag = "14")]
        DownloadPieceFailedRequest(super::DownloadPieceFailedRequest),
        #[prost(message, tag = "15")]
        DownloadPieceBackToSourceFailedRequest(
            super::DownloadPieceBackToSourceFailedRequest,
        ),
    }
}
/// EmptyCacheTaskResponse represents empty cache task response of AnnounceCachePeerResponse.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct EmptyCacheTaskResponse {}
/// NormalCacheTaskResponse represents normal cache task response of AnnounceCachePeerResponse.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NormalCacheTaskResponse {
    /// Candidate parents.
    #[prost(message, repeated, tag = "1")]
    pub candidate_parents: ::prost::alloc::vec::Vec<super::super::common::v2::CachePeer>,
}
/// AnnounceCachePeerResponse represents response of AnnounceCachePeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnnounceCachePeerResponse {
    #[prost(oneof = "announce_cache_peer_response::Response", tags = "1, 2, 3")]
    pub response: ::core::option::Option<announce_cache_peer_response::Response>,
}
/// Nested message and enum types in `AnnounceCachePeerResponse`.
pub mod announce_cache_peer_response {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Response {
        #[prost(message, tag = "1")]
        EmptyCacheTaskResponse(super::EmptyCacheTaskResponse),
        #[prost(message, tag = "2")]
        NormalCacheTaskResponse(super::NormalCacheTaskResponse),
        #[prost(message, tag = "3")]
        NeedBackToSourceResponse(super::NeedBackToSourceResponse),
    }
}
/// StatCachePeerRequest represents request of StatCachePeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct StatCachePeerRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
}
/// DeleteCachePeerRequest represents request of DeleteCachePeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DeleteCachePeerRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
}
/// StatCacheTaskRequest represents request of StatCacheTask.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct StatCacheTaskRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
}
/// DeleteCacheTaskRequest represents request of DeleteCacheTask.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DeleteCacheTaskRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
}
/// RegisterPersistentPeerRequest represents persistent peer registered request of AnnouncePersistentPeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct RegisterPersistentPeerRequest {
    /// This is the url of the object storage protocal where the persistent task will be stored,
    /// for example: `s3://<bucket>/path`, `gcs://<bucket>/path`. The combination of url,
    /// object_storage.endpoint and object_storage.region must be unique, because
    /// the persistent task cannot be overwritten once it is uploaded.
    #[prost(string, tag = "1")]
    pub url: ::prost::alloc::string::String,
    /// Object storage protocol information.
    #[prost(message, optional, tag = "2")]
    pub object_storage: ::core::option::Option<super::super::common::v2::ObjectStorage>,
    /// Persistent represents whether the persistent task is persistent.
    /// If the persistent task is persistent, the persistent peer will
    /// not be deleted when dfdaemon runs garbage collection.
    #[prost(bool, tag = "3")]
    pub persistent: bool,
    /// File path to be exported.
    #[prost(string, optional, tag = "4")]
    pub output_path: ::core::option::Option<::prost::alloc::string::String>,
    /// concurrent_piece_count is the number of pieces that can be downloaded concurrently.
    #[prost(uint32, optional, tag = "5")]
    pub concurrent_piece_count: ::core::option::Option<u32>,
    /// Task piece count.
    #[prost(uint64, tag = "6")]
    pub piece_count: u64,
    /// NeedBackToSource needs downloaded from source.
    #[prost(bool, tag = "7")]
    pub need_back_to_source: bool,
}
/// DownloadPersistentPeerStartedRequest represents persistent peer download started request of AnnouncePersistentPeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPersistentPeerStartedRequest {}
/// DownloadPersistentPeerBackToSourceStartedRequest represents cache peer download back-to-source started request of AnnouncePersistentPeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPersistentPeerBackToSourceStartedRequest {
    /// The description of the back-to-source reason.
    #[prost(string, optional, tag = "1")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// ReschedulePersistentPeerRequest represents reschedule request of AnnouncePersistentPeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReschedulePersistentPeerRequest {
    /// Candidate parent ids.
    #[prost(message, repeated, tag = "1")]
    pub candidate_parents: ::prost::alloc::vec::Vec<
        super::super::common::v2::PersistentPeer,
    >,
    /// The description of the reschedule reason.
    #[prost(string, optional, tag = "2")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// DownloadPersistentPeerFinishedRequest represents persistent peer download finished request of AnnouncePersistentPeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPersistentPeerFinishedRequest {}
/// DownloadPersistentPeerBackToSourceFinishedRequest represents cache peer download back-to-source finished request of AnnouncePersistentPeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPersistentPeerBackToSourceFinishedRequest {}
/// DownloadPersistentPeerFailedRequest represents persistent peer download failed request of AnnouncePersistentPeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPersistentPeerFailedRequest {
    /// The description of the download failed.
    #[prost(string, optional, tag = "1")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// DownloadPersistentPeerBackToSourceFailedRequest represents cache peer download back-to-source failed request of AnnouncePersistentPeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPersistentPeerBackToSourceFailedRequest {
    /// The description of the download back-to-source failed.
    #[prost(string, optional, tag = "1")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// AnnouncePersistentPeerRequest represents request of AnnouncePersistentPeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnnouncePersistentPeerRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
    #[prost(
        oneof = "announce_persistent_peer_request::Request",
        tags = "4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15"
    )]
    pub request: ::core::option::Option<announce_persistent_peer_request::Request>,
}
/// Nested message and enum types in `AnnouncePersistentPeerRequest`.
pub mod announce_persistent_peer_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Request {
        #[prost(message, tag = "4")]
        RegisterPersistentPeerRequest(super::RegisterPersistentPeerRequest),
        #[prost(message, tag = "5")]
        DownloadPersistentPeerStartedRequest(
            super::DownloadPersistentPeerStartedRequest,
        ),
        #[prost(message, tag = "6")]
        DownloadPersistentPeerBackToSourceStartedRequest(
            super::DownloadPersistentPeerBackToSourceStartedRequest,
        ),
        #[prost(message, tag = "7")]
        ReschedulePersistentPeerRequest(super::ReschedulePersistentPeerRequest),
        #[prost(message, tag = "8")]
        DownloadPersistentPeerFinishedRequest(
            super::DownloadPersistentPeerFinishedRequest,
        ),
        #[prost(message, tag = "9")]
        DownloadPersistentPeerBackToSourceFinishedRequest(
            super::DownloadPersistentPeerBackToSourceFinishedRequest,
        ),
        #[prost(message, tag = "10")]
        DownloadPersistentPeerFailedRequest(super::DownloadPersistentPeerFailedRequest),
        #[prost(message, tag = "11")]
        DownloadPersistentPeerBackToSourceFailedRequest(
            super::DownloadPersistentPeerBackToSourceFailedRequest,
        ),
        #[prost(message, tag = "12")]
        DownloadPieceFinishedRequest(super::DownloadPieceFinishedRequest),
        #[prost(message, tag = "13")]
        DownloadPieceBackToSourceFinishedRequest(
            super::DownloadPieceBackToSourceFinishedRequest,
        ),
        #[prost(message, tag = "14")]
        DownloadPieceFailedRequest(super::DownloadPieceFailedRequest),
        #[prost(message, tag = "15")]
        DownloadPieceBackToSourceFailedRequest(
            super::DownloadPieceBackToSourceFailedRequest,
        ),
    }
}
/// EmptyPersistentTaskResponse represents empty persistent task response of AnnouncePersistentPeerResponse.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct EmptyPersistentTaskResponse {}
/// NormalPersistentTaskResponse represents normal persistent task response of AnnouncePersistentPeerResponse.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NormalPersistentTaskResponse {
    /// Candidate parents.
    #[prost(message, repeated, tag = "1")]
    pub candidate_parents: ::prost::alloc::vec::Vec<
        super::super::common::v2::PersistentPeer,
    >,
}
/// AnnouncePersistentPeerResponse represents response of AnnouncePersistentPeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnnouncePersistentPeerResponse {
    #[prost(oneof = "announce_persistent_peer_response::Response", tags = "1, 2, 3")]
    pub response: ::core::option::Option<announce_persistent_peer_response::Response>,
}
/// Nested message and enum types in `AnnouncePersistentPeerResponse`.
pub mod announce_persistent_peer_response {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Response {
        #[prost(message, tag = "1")]
        EmptyPersistentTaskResponse(super::EmptyPersistentTaskResponse),
        #[prost(message, tag = "2")]
        NormalPersistentTaskResponse(super::NormalPersistentTaskResponse),
        #[prost(message, tag = "3")]
        NeedBackToSourceResponse(super::NeedBackToSourceResponse),
    }
}
/// StatPersistentPeerRequest represents request of StatPersistentPeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct StatPersistentPeerRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
}
/// DeletePersistentPeerRequest represents request of DeletePersistentPeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DeletePersistentPeerRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
}
/// UploadPersistentTaskStartedRequest represents upload persistent task started request of UploadPersistentTaskStarted.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UploadPersistentTaskStartedRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
    /// This is the key of the object storage where the persistent task will be stored,
    /// for example: `file.txt` or `dir/file.txt`. The combination of object_storage_key,
    /// object_storage.endpoint and object_storage.region must be unique, because
    /// the persistent task cannot be overwritten once it is uploaded.
    #[prost(string, tag = "4")]
    pub url: ::prost::alloc::string::String,
    /// Object storage protocol information.
    #[prost(message, optional, tag = "5")]
    pub object_storage: ::core::option::Option<super::super::common::v2::ObjectStorage>,
    /// Replica count of the persistent task.
    #[prost(uint64, tag = "6")]
    pub persistent_replica_count: u64,
    /// Task content length.
    #[prost(uint64, tag = "7")]
    pub content_length: u64,
    /// Task piece count.
    #[prost(uint32, tag = "8")]
    pub piece_count: u32,
    /// TTL of the persistent task.
    #[prost(message, optional, tag = "9")]
    pub ttl: ::core::option::Option<::prost_wkt_types::Duration>,
}
/// UploadPersistentTaskFinishedRequest represents upload persistent task finished request of UploadPersistentTaskFinished.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UploadPersistentTaskFinishedRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
}
/// UploadPersistentTaskFailedRequest represents upload persistent task failed request of UploadPersistentTaskFailed.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UploadPersistentTaskFailedRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
    /// The description of the upload failed.
    #[prost(string, optional, tag = "4")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// StatPersistentTaskRequest represents request of StatPersistentTask.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct StatPersistentTaskRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
}
/// DeletePersistentTaskRequest represents request of DeletePersistentTask.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DeletePersistentTaskRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
}
/// RegisterPersistentCachePeerRequest represents persistent cache peer registered request of AnnouncePersistentCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct RegisterPersistentCachePeerRequest {
    /// Persistent represents whether the persistent cache task is persistent.
    /// If the persistent cache task is persistent, the persistent cache peer will
    /// not be deleted when dfdaemon runs garbage collection.
    #[prost(bool, tag = "1")]
    pub persistent: bool,
    /// Tag is used to distinguish different persistent cache tasks.
    #[prost(string, optional, tag = "2")]
    pub tag: ::core::option::Option<::prost::alloc::string::String>,
    /// Application of task.
    #[prost(string, optional, tag = "3")]
    pub application: ::core::option::Option<::prost::alloc::string::String>,
    /// Task piece length, the value needs to be greater than or equal to 4194304(4MiB).
    #[prost(uint64, tag = "4")]
    pub piece_length: u64,
    /// File path to be exported.
    #[prost(string, optional, tag = "5")]
    pub output_path: ::core::option::Option<::prost::alloc::string::String>,
    /// concurrent_piece_count is the number of pieces that can be downloaded concurrently.
    #[prost(uint32, optional, tag = "6")]
    pub concurrent_piece_count: ::core::option::Option<u32>,
    /// Task piece count.
    #[prost(uint64, tag = "7")]
    pub piece_count: u64,
}
/// DownloadPersistentCachePeerStartedRequest represents persistent cache peer download started request of AnnouncePersistentCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPersistentCachePeerStartedRequest {}
/// ReschedulePersistentCachePeerRequest represents reschedule request of AnnouncePersistentCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ReschedulePersistentCachePeerRequest {
    /// Candidate parent ids.
    #[prost(message, repeated, tag = "1")]
    pub candidate_parents: ::prost::alloc::vec::Vec<
        super::super::common::v2::PersistentCachePeer,
    >,
    /// The description of the reschedule reason.
    #[prost(string, optional, tag = "2")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// DownloadPersistentCachePeerFinishedRequest represents persistent cache peer download finished request of AnnouncePersistentCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPersistentCachePeerFinishedRequest {}
/// DownloadPersistentCachePeerFailedRequest represents persistent cache peer download failed request of AnnouncePersistentCachePeerRequest.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DownloadPersistentCachePeerFailedRequest {
    /// The description of the download failed.
    #[prost(string, optional, tag = "1")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// AnnouncePersistentCachePeerRequest represents request of AnnouncePersistentCachePeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnnouncePersistentCachePeerRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
    #[prost(
        oneof = "announce_persistent_cache_peer_request::Request",
        tags = "4, 5, 6, 7, 8, 9, 10"
    )]
    pub request: ::core::option::Option<announce_persistent_cache_peer_request::Request>,
}
/// Nested message and enum types in `AnnouncePersistentCachePeerRequest`.
pub mod announce_persistent_cache_peer_request {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Request {
        #[prost(message, tag = "4")]
        RegisterPersistentCachePeerRequest(super::RegisterPersistentCachePeerRequest),
        #[prost(message, tag = "5")]
        DownloadPersistentCachePeerStartedRequest(
            super::DownloadPersistentCachePeerStartedRequest,
        ),
        #[prost(message, tag = "6")]
        ReschedulePersistentCachePeerRequest(
            super::ReschedulePersistentCachePeerRequest,
        ),
        #[prost(message, tag = "7")]
        DownloadPersistentCachePeerFinishedRequest(
            super::DownloadPersistentCachePeerFinishedRequest,
        ),
        #[prost(message, tag = "8")]
        DownloadPersistentCachePeerFailedRequest(
            super::DownloadPersistentCachePeerFailedRequest,
        ),
        #[prost(message, tag = "9")]
        DownloadPieceFinishedRequest(super::DownloadPieceFinishedRequest),
        #[prost(message, tag = "10")]
        DownloadPieceFailedRequest(super::DownloadPieceFailedRequest),
    }
}
/// EmptyPersistentCacheTaskResponse represents empty persistent cache task response of AnnouncePersistentCachePeerResponse.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct EmptyPersistentCacheTaskResponse {}
/// NormalPersistentCacheTaskResponse represents normal persistent cache task response of AnnouncePersistentCachePeerResponse.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct NormalPersistentCacheTaskResponse {
    /// Candidate parents.
    #[prost(message, repeated, tag = "1")]
    pub candidate_cache_parents: ::prost::alloc::vec::Vec<
        super::super::common::v2::PersistentCachePeer,
    >,
}
/// AnnouncePersistentCachePeerResponse represents response of AnnouncePersistentCachePeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct AnnouncePersistentCachePeerResponse {
    #[prost(oneof = "announce_persistent_cache_peer_response::Response", tags = "1, 2")]
    pub response: ::core::option::Option<
        announce_persistent_cache_peer_response::Response,
    >,
}
/// Nested message and enum types in `AnnouncePersistentCachePeerResponse`.
pub mod announce_persistent_cache_peer_response {
    #[derive(serde::Serialize, serde::Deserialize)]
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum Response {
        #[prost(message, tag = "1")]
        EmptyPersistentCacheTaskResponse(super::EmptyPersistentCacheTaskResponse),
        #[prost(message, tag = "2")]
        NormalPersistentCacheTaskResponse(super::NormalPersistentCacheTaskResponse),
    }
}
/// StatPersistentCachePeerRequest represents request of StatPersistentCachePeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct StatPersistentCachePeerRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
}
/// DeletePersistentCachePeerRequest represents request of DeletePersistentCachePeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DeletePersistentCachePeerRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
}
/// UploadPersistentCacheTaskStartedRequest represents upload persistent cache task started request of UploadPersistentCacheTaskStarted.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UploadPersistentCacheTaskStartedRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
    /// Replica count of the persistent cache task.
    #[prost(uint64, tag = "4")]
    pub persistent_replica_count: u64,
    /// Tag is used to distinguish different persistent cache tasks.
    #[prost(string, optional, tag = "5")]
    pub tag: ::core::option::Option<::prost::alloc::string::String>,
    /// Application of task.
    #[prost(string, optional, tag = "6")]
    pub application: ::core::option::Option<::prost::alloc::string::String>,
    /// Task piece length, the value needs to be greater than or equal to 4194304(4MiB).
    #[prost(uint64, tag = "7")]
    pub piece_length: u64,
    /// Task content length.
    #[prost(uint64, tag = "8")]
    pub content_length: u64,
    /// Task piece count.
    #[prost(uint32, tag = "9")]
    pub piece_count: u32,
    /// TTL of the persistent cache task.
    #[prost(message, optional, tag = "10")]
    pub ttl: ::core::option::Option<::prost_wkt_types::Duration>,
}
/// UploadPersistentCacheTaskFinishedRequest represents upload persistent cache task finished request of UploadPersistentCacheTaskFinished.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UploadPersistentCacheTaskFinishedRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
}
/// UploadPersistentCacheTaskFailedRequest represents upload persistent cache task failed request of UploadPersistentCacheTaskFailed.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UploadPersistentCacheTaskFailedRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
    /// Peer id.
    #[prost(string, tag = "3")]
    pub peer_id: ::prost::alloc::string::String,
    /// The description of the upload failed.
    #[prost(string, optional, tag = "4")]
    pub description: ::core::option::Option<::prost::alloc::string::String>,
}
/// StatPersistentCacheTaskRequest represents request of StatPersistentCacheTask.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct StatPersistentCacheTaskRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
}
/// DeletePersistentCacheTaskRequest represents request of DeletePersistentCacheTask.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DeletePersistentCacheTaskRequest {
    /// Host id.
    #[prost(string, tag = "1")]
    pub host_id: ::prost::alloc::string::String,
    /// Task id.
    #[prost(string, tag = "2")]
    pub task_id: ::prost::alloc::string::String,
}
/// PreheatImageRequest represents request of PreheatImage.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreheatImageRequest {
    /// URL is the image manifest url for preheating.
    #[prost(string, tag = "1")]
    pub url: ::prost::alloc::string::String,
    /// Piece Length is the piece length(bytes) for downloading file. The value needs to
    /// be greater than 4MiB (4,194,304 bytes) and less than 64MiB (67,108,864 bytes),
    /// for example: 4194304(4mib), 8388608(8mib). If the piece length is not specified,
    /// the piece length will be calculated according to the file size.
    #[prost(uint64, optional, tag = "2")]
    pub piece_length: ::core::option::Option<u64>,
    /// Tag is the tag for preheating.
    #[prost(string, optional, tag = "3")]
    pub tag: ::core::option::Option<::prost::alloc::string::String>,
    /// Application is the application string for preheating.
    #[prost(string, optional, tag = "4")]
    pub application: ::core::option::Option<::prost::alloc::string::String>,
    /// Filtered query params to generate the task id.
    /// When filter is \["Signature", "Expires", "ns"\], for example:
    /// <http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io> and <http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io>
    /// will generate the same task id.
    /// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
    #[prost(string, repeated, tag = "5")]
    pub filtered_query_params: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Header is the http headers for authentication.
    #[prost(map = "string, string", tag = "6")]
    pub header: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// Username is the username for authentication.
    #[prost(string, optional, tag = "7")]
    pub username: ::core::option::Option<::prost::alloc::string::String>,
    /// Password is the password for authentication.
    #[prost(string, optional, tag = "8")]
    pub password: ::core::option::Option<::prost::alloc::string::String>,
    /// Platform is the platform for preheating, such as linux/amd64, linux/arm64, etc.
    #[prost(string, optional, tag = "9")]
    pub platform: ::core::option::Option<::prost::alloc::string::String>,
    /// Scope is the scope for preheating, it can be one of the following values:
    ///
    /// * single_seed_peer: preheat from a single seed peer.
    /// * all_peers: preheat from all available peers.
    /// * all_seed_peers: preheat from all seed peers.
    #[prost(string, tag = "10")]
    pub scope: ::prost::alloc::string::String,
    /// IPs is a list of specific peer IPs for preheating.
    /// This field has the highest priority: if provided, both 'Count' and 'Percentage' will be ignored.
    /// Applies to 'all_peers' and 'all_seed_peers' scopes.
    #[prost(string, repeated, tag = "11")]
    pub ips: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Percentage is the percentage of available peers to preheat.
    /// This field has the lowest priority and is only used if both 'IPs' and 'Count' are not provided.
    /// It must be a value between 1 and 100 (inclusive) if provided.
    /// Applies to 'all_peers' and 'all_seed_peers' scopes.
    #[prost(uint32, optional, tag = "12")]
    pub percentage: ::core::option::Option<u32>,
    /// Count is the desired number of peers to preheat.
    /// This field is used only when 'IPs' is not specified. It has priority over 'Percentage'.
    /// It must be a value between 1 and 200 (inclusive) if provided.
    /// Applies to 'all_peers' and 'all_seed_peers' scopes.
    #[prost(uint32, optional, tag = "13")]
    pub count: ::core::option::Option<u32>,
    /// ConcurrentTaskCount specifies the maximum number of tasks (e.g., image layers) to preheat concurrently.
    /// For example, if preheating 100 layers with ConcurrentTaskCount set to 10, up to 10 layers are processed simultaneously.
    /// If ConcurrentPeerCount is 10 for 1000 peers, each layer is preheated by 10 peers concurrently.
    /// Default is 8, maximum is 100.
    #[prost(int64, optional, tag = "14")]
    pub concurrent_task_count: ::core::option::Option<i64>,
    /// ConcurrentPeerCount specifies the maximum number of peers to preheat concurrently for a single task (e.g., an image layer).
    /// For example, if preheating a layer with ConcurrentPeerCount set to 10, up to 10 peers process that layer simultaneously.
    /// Default is 500, maximum is 1000.
    #[prost(int64, optional, tag = "15")]
    pub concurrent_peer_count: ::core::option::Option<i64>,
    /// Timeout is the timeout for preheating, default is 30 minutes.
    #[prost(message, optional, tag = "16")]
    pub timeout: ::core::option::Option<::prost_wkt_types::Duration>,
    /// Preheat priority.
    #[prost(enumeration = "super::super::common::v2::Priority", tag = "17")]
    pub priority: i32,
    /// certificate_chain is the client certs with DER format for the backend client.
    #[prost(bytes = "vec", repeated, tag = "18")]
    pub certificate_chain: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    /// insecure_skip_verify indicates whether to skip TLS verification.
    #[prost(bool, tag = "19")]
    pub insecure_skip_verify: bool,
}
/// StatImageRequest represents request of StatImage.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatImageRequest {
    /// URL is the image manifest url for preheating.
    #[prost(string, tag = "1")]
    pub url: ::prost::alloc::string::String,
    /// Piece Length is the piece length(bytes) for downloading file. The value needs to
    /// be greater than 4MiB (4,194,304 bytes) and less than 64MiB (67,108,864 bytes),
    /// for example: 4194304(4mib), 8388608(8mib). If the piece length is not specified,
    /// the piece length will be calculated according to the file size.
    #[prost(uint64, optional, tag = "2")]
    pub piece_length: ::core::option::Option<u64>,
    /// Tag is the tag for preheating.
    #[prost(string, optional, tag = "3")]
    pub tag: ::core::option::Option<::prost::alloc::string::String>,
    /// Application is the application string for preheating.
    #[prost(string, optional, tag = "4")]
    pub application: ::core::option::Option<::prost::alloc::string::String>,
    /// Filtered query params to generate the task id.
    /// When filter is \["Signature", "Expires", "ns"\], for example:
    /// <http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io> and <http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io>
    /// will generate the same task id.
    /// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
    #[prost(string, repeated, tag = "5")]
    pub filtered_query_params: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Header is the http headers for authentication.
    #[prost(map = "string, string", tag = "6")]
    pub header: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// Username is the username for authentication.
    #[prost(string, optional, tag = "7")]
    pub username: ::core::option::Option<::prost::alloc::string::String>,
    /// Password is the password for authentication.
    #[prost(string, optional, tag = "8")]
    pub password: ::core::option::Option<::prost::alloc::string::String>,
    /// Platform is the platform for preheating, such as linux/amd64, linux/arm64, etc.
    #[prost(string, optional, tag = "9")]
    pub platform: ::core::option::Option<::prost::alloc::string::String>,
    /// ConcurrentLayerCount specifies the maximum number of layers to get concurrently.
    /// For example, if stat 100 layers with ConcurrentLayerCount set to 10, up to 10 layers are processed simultaneously.
    /// If ConcurrentPeerCount is 10 for 1000 peers, each layer is stated by 10 peers concurrently.
    /// Default is 8, maximum is 100.
    #[prost(int64, optional, tag = "10")]
    pub concurrent_layer_count: ::core::option::Option<i64>,
    /// ConcurrentPeerCount specifies the maximum number of peers stat concurrently for a single task (e.g., an image layer).
    /// For example, if stat a layer with ConcurrentPeerCount set to 10, up to 10 peers process that layer simultaneously.
    /// Default is 500, maximum is 1000.
    #[prost(int64, optional, tag = "11")]
    pub concurrent_peer_count: ::core::option::Option<i64>,
    /// Timeout is the timeout for preheating, default is 30 minutes.
    #[prost(message, optional, tag = "12")]
    pub timeout: ::core::option::Option<::prost_wkt_types::Duration>,
    /// certificate_chain is the client certs with DER format for the backend client.
    #[prost(bytes = "vec", repeated, tag = "13")]
    pub certificate_chain: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    /// insecure_skip_verify indicates whether to skip TLS verification.
    #[prost(bool, tag = "14")]
    pub insecure_skip_verify: bool,
}
/// StatImageResponse represents response of StatImage.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatImageResponse {
    /// Image is the image information.
    #[prost(message, optional, tag = "1")]
    pub image: ::core::option::Option<Image>,
    /// Peers is the peers that have downloaded the image.
    #[prost(message, repeated, tag = "2")]
    pub peers: ::prost::alloc::vec::Vec<PeerImage>,
}
/// PeerImage represents a peer in the get image job.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerImage {
    /// IP is the IP address of the peer.
    #[prost(string, tag = "1")]
    pub ip: ::prost::alloc::string::String,
    /// Hostname is the hostname of the peer.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// CachedLayers is the list of layers that the peer has downloaded.
    #[prost(message, repeated, tag = "3")]
    pub cached_layers: ::prost::alloc::vec::Vec<Layer>,
}
/// Image represents the image information.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Image {
    /// Layers is the list of layers of the image.
    #[prost(message, repeated, tag = "1")]
    pub layers: ::prost::alloc::vec::Vec<Layer>,
}
/// Layer represents a layer of the image.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Layer {
    /// URL is the URL of the layer.
    #[prost(string, tag = "1")]
    pub url: ::prost::alloc::string::String,
    /// IsFinished indicates whether the layer has finished downloading on the peer.
    #[prost(bool, optional, tag = "2")]
    pub is_finished: ::core::option::Option<bool>,
}
/// PreheatFileRequest represents request of PreheatFile.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreheatFileRequest {
    /// URL is the file url for preheating.
    #[prost(string, tag = "1")]
    pub url: ::prost::alloc::string::String,
    /// Piece Length is the piece length(bytes) for downloading file. The value needs to
    /// be greater than 4MiB (4,194,304 bytes) and less than 64MiB (67,108,864 bytes),
    /// for example: 4194304(4mib), 8388608(8mib). If the piece length is not specified,
    /// the piece length will be calculated according to the file size.
    #[prost(uint64, optional, tag = "2")]
    pub piece_length: ::core::option::Option<u64>,
    /// Tag is the tag for preheating.
    #[prost(string, optional, tag = "3")]
    pub tag: ::core::option::Option<::prost::alloc::string::String>,
    /// Application is the application string for preheating.
    #[prost(string, optional, tag = "4")]
    pub application: ::core::option::Option<::prost::alloc::string::String>,
    /// Filtered query params to generate the task id.
    /// When filter is \["Signature", "Expires", "ns"\], for example:
    /// <http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io> and <http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io>
    /// will generate the same task id.
    /// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
    #[prost(string, repeated, tag = "5")]
    pub filtered_query_params: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Header is the http headers for authentication.
    #[prost(map = "string, string", tag = "6")]
    pub header: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// Scope is the scope for preheating, it can be one of the following values:
    ///
    /// * single_seed_peer: preheat from a single seed peer.
    /// * all_peers: preheat from all available peers.
    /// * all_seed_peers: preheat from all seed peers.
    #[prost(string, tag = "7")]
    pub scope: ::prost::alloc::string::String,
    /// IPs is a list of specific peer IPs for preheating.
    /// This field has the highest priority: if provided, both 'Count' and 'Percentage' will be ignored.
    /// Applies to 'all_peers' and 'all_seed_peers' scopes.
    #[prost(string, repeated, tag = "8")]
    pub ips: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Percentage is the percentage of available peers to preheat.
    /// This field has the lowest priority and is only used if both 'IPs' and 'Count' are not provided.
    /// It must be a value between 1 and 100 (inclusive) if provided.
    /// Applies to 'all_peers' and 'all_seed_peers' scopes.
    #[prost(uint32, optional, tag = "9")]
    pub percentage: ::core::option::Option<u32>,
    /// Count is the desired number of peers to preheat.
    /// This field is used only when 'IPs' is not specified. It has priority over 'Percentage'.
    /// It must be a value between 1 and 200 (inclusive) if provided.
    /// Applies to 'all_peers' and 'all_seed_peers' scopes.
    #[prost(uint32, optional, tag = "10")]
    pub count: ::core::option::Option<u32>,
    /// ConcurrentTaskCount specifies the maximum number of tasks to preheat concurrently.
    /// For example, if preheating 100 files with ConcurrentTaskCount set to 10, up to 10 files are processed simultaneously.
    /// If ConcurrentPeerCount is 10 for 1000 peers, each file is preheated by 10 peers concurrently.
    /// Default is 8, maximum is 100.
    #[prost(int64, optional, tag = "11")]
    pub concurrent_task_count: ::core::option::Option<i64>,
    /// ConcurrentPeerCount specifies the maximum number of peers to preheat concurrently for a single task.
    /// For example, if preheating a file with ConcurrentPeerCount set to 10, up to 10 peers process that file simultaneously.
    /// Default is 500, maximum is 1000.
    #[prost(int64, optional, tag = "12")]
    pub concurrent_peer_count: ::core::option::Option<i64>,
    /// Timeout is the timeout for preheating, default is 30 minutes.
    #[prost(message, optional, tag = "13")]
    pub timeout: ::core::option::Option<::prost_wkt_types::Duration>,
    /// Preheat priority.
    #[prost(enumeration = "super::super::common::v2::Priority", tag = "14")]
    pub priority: i32,
    /// certificate_chain is the client certs with DER format for the backend client.
    #[prost(bytes = "vec", repeated, tag = "15")]
    pub certificate_chain: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    /// insecure_skip_verify indicates whether to skip TLS verification.
    #[prost(bool, tag = "16")]
    pub insecure_skip_verify: bool,
    /// Object storage protocol information.
    #[prost(message, optional, tag = "17")]
    pub object_storage: ::core::option::Option<super::super::common::v2::ObjectStorage>,
    /// HDFS protocol information.
    #[prost(message, optional, tag = "18")]
    pub hdfs: ::core::option::Option<super::super::common::v2::Hdfs>,
    /// output_path is the path to preheat the file or directory to destination.
    #[prost(string, optional, tag = "19")]
    pub output_path: ::core::option::Option<::prost::alloc::string::String>,
}
/// StatFileRequest represents request of StatFile.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatFileRequest {
    /// URL is the file url for stat.
    #[prost(string, tag = "1")]
    pub url: ::prost::alloc::string::String,
    /// Piece Length is the piece length(bytes) for downloading file. The value needs to
    /// be greater than 4MiB (4,194,304 bytes) and less than 64MiB (67,108,864 bytes),
    /// for example: 4194304(4mib), 8388608(8mib). If the piece length is not specified,
    /// the piece length will be calculated according to the file size.
    #[prost(uint64, optional, tag = "2")]
    pub piece_length: ::core::option::Option<u64>,
    /// Tag is the tag for stat.
    #[prost(string, optional, tag = "3")]
    pub tag: ::core::option::Option<::prost::alloc::string::String>,
    /// Application is the application string for stat.
    #[prost(string, optional, tag = "4")]
    pub application: ::core::option::Option<::prost::alloc::string::String>,
    /// Filtered query params to generate the task id.
    /// When filter is \["Signature", "Expires", "ns"\], for example:
    /// <http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io> and <http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io>
    /// will generate the same task id.
    /// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
    #[prost(string, repeated, tag = "5")]
    pub filtered_query_params: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Header is the http headers for authentication.
    #[prost(map = "string, string", tag = "6")]
    pub header: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// ConcurrentPeerCount specifies the maximum number of peers stat concurrently for a single task.
    /// For example, if stat a file with ConcurrentPeerCount set to 10, up to 10 peers process that file simultaneously.
    /// Default is 500, maximum is 1000.
    #[prost(int64, optional, tag = "7")]
    pub concurrent_peer_count: ::core::option::Option<i64>,
    /// Timeout is the timeout for stat, default is 30 minutes.
    #[prost(message, optional, tag = "8")]
    pub timeout: ::core::option::Option<::prost_wkt_types::Duration>,
    /// certificate_chain is the client certs with DER format for the backend client.
    #[prost(bytes = "vec", repeated, tag = "9")]
    pub certificate_chain: ::prost::alloc::vec::Vec<::prost::alloc::vec::Vec<u8>>,
    /// insecure_skip_verify indicates whether to skip TLS verification.
    #[prost(bool, tag = "10")]
    pub insecure_skip_verify: bool,
    /// Object storage protocol information.
    #[prost(message, optional, tag = "11")]
    pub object_storage: ::core::option::Option<super::super::common::v2::ObjectStorage>,
    /// HDFS protocol information.
    #[prost(message, optional, tag = "12")]
    pub hdfs: ::core::option::Option<super::super::common::v2::Hdfs>,
}
/// StatFileResponse represents response of StatFile.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StatFileResponse {
    /// Peers is the peers that have downloaded the file.
    #[prost(message, repeated, tag = "1")]
    pub peers: ::prost::alloc::vec::Vec<PeerFile>,
}
/// PeerFile represents a peer in the get file job.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PeerFile {
    /// IP is the IP address of the peer.
    #[prost(string, tag = "1")]
    pub ip: ::prost::alloc::string::String,
    /// Hostname is the hostname of the peer.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// CachedFiles is the list of files that the peer has downloaded.
    #[prost(message, repeated, tag = "3")]
    pub cached_files: ::prost::alloc::vec::Vec<File>,
}
/// File represents the file information.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct File {
    /// URL is the url of the file.
    #[prost(string, tag = "1")]
    pub url: ::prost::alloc::string::String,
    /// IsFinished indicates whether the file has finished downloading on the peer.
    #[prost(bool, optional, tag = "2")]
    pub is_finished: ::core::option::Option<bool>,
}
/// Generated client implementations.
pub mod scheduler_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    /// Scheduler RPC Service.
    #[derive(Debug, Clone)]
    pub struct SchedulerClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl SchedulerClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> SchedulerClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::Body>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + std::marker::Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + std::marker::Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_origin(inner: T, origin: Uri) -> Self {
            let inner = tonic::client::Grpc::with_origin(inner, origin);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> SchedulerClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::Body>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::Body>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::Body>,
            >>::Error: Into<StdError> + std::marker::Send + std::marker::Sync,
        {
            SchedulerClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with the given encoding.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.send_compressed(encoding);
            self
        }
        /// Enable decompressing responses.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.inner = self.inner.accept_compressed(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_decoding_message_size(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.inner = self.inner.max_encoding_message_size(limit);
            self
        }
        /// AnnouncePeer announces peer to scheduler.
        pub async fn announce_peer(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::AnnouncePeerRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::AnnouncePeerResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/AnnouncePeer",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "AnnouncePeer"));
            self.inner.streaming(req, path, codec).await
        }
        /// Checks information of peer.
        pub async fn stat_peer(
            &mut self,
            request: impl tonic::IntoRequest<super::StatPeerRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::Peer>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/StatPeer",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "StatPeer"));
            self.inner.unary(req, path, codec).await
        }
        /// DeletePeer releases peer in scheduler.
        pub async fn delete_peer(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePeerRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/DeletePeer",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "DeletePeer"));
            self.inner.unary(req, path, codec).await
        }
        /// Checks information of task.
        pub async fn stat_task(
            &mut self,
            request: impl tonic::IntoRequest<super::StatTaskRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::Task>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/StatTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "StatTask"));
            self.inner.unary(req, path, codec).await
        }
        /// DeleteTask releases task in scheduler.
        pub async fn delete_task(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteTaskRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/DeleteTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "DeleteTask"));
            self.inner.unary(req, path, codec).await
        }
        /// AnnounceHost announces host to scheduler.
        pub async fn announce_host(
            &mut self,
            request: impl tonic::IntoRequest<super::AnnounceHostRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/AnnounceHost",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "AnnounceHost"));
            self.inner.unary(req, path, codec).await
        }
        /// ListHosts lists hosts in scheduler.
        pub async fn list_hosts(
            &mut self,
            request: impl tonic::IntoRequest<super::ListHostsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListHostsResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/ListHosts",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "ListHosts"));
            self.inner.unary(req, path, codec).await
        }
        /// DeleteHost releases host in scheduler.
        pub async fn delete_host(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteHostRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/DeleteHost",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "DeleteHost"));
            self.inner.unary(req, path, codec).await
        }
        /// AnnounceCachePeer announces cache peer to scheduler.
        pub async fn announce_cache_peer(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::AnnounceCachePeerRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<tonic::codec::Streaming<super::AnnounceCachePeerResponse>>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/AnnounceCachePeer",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "AnnounceCachePeer"));
            self.inner.streaming(req, path, codec).await
        }
        /// Checks information of cache peer.
        pub async fn stat_cache_peer(
            &mut self,
            request: impl tonic::IntoRequest<super::StatCachePeerRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::CachePeer>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/StatCachePeer",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "StatCachePeer"));
            self.inner.unary(req, path, codec).await
        }
        /// DeletCacheePeer releases cache peer in scheduler.
        pub async fn delete_cache_peer(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteCachePeerRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/DeleteCachePeer",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "DeleteCachePeer"));
            self.inner.unary(req, path, codec).await
        }
        /// Checks information of cache task.
        pub async fn stat_cache_task(
            &mut self,
            request: impl tonic::IntoRequest<super::StatCacheTaskRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::CacheTask>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/StatCacheTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "StatCacheTask"));
            self.inner.unary(req, path, codec).await
        }
        /// DeleteCacheTask releases cache task in scheduler.
        pub async fn delete_cache_task(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteCacheTaskRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/DeleteCacheTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "DeleteCacheTask"));
            self.inner.unary(req, path, codec).await
        }
        /// AnnouncePersistentPeer announces persistent peer to scheduler.
        pub async fn announce_persistent_peer(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::AnnouncePersistentPeerRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<
                tonic::codec::Streaming<super::AnnouncePersistentPeerResponse>,
            >,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/AnnouncePersistentPeer",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("scheduler.v2.Scheduler", "AnnouncePersistentPeer"),
                );
            self.inner.streaming(req, path, codec).await
        }
        /// Checks information of persistent peer.
        pub async fn stat_persistent_peer(
            &mut self,
            request: impl tonic::IntoRequest<super::StatPersistentPeerRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::PersistentPeer>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/StatPersistentPeer",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "StatPersistentPeer"));
            self.inner.unary(req, path, codec).await
        }
        /// DeletePersistentPeer releases persistent peer in scheduler.
        pub async fn delete_persistent_peer(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePersistentPeerRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/DeletePersistentPeer",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("scheduler.v2.Scheduler", "DeletePersistentPeer"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// UploadPersistentTaskStarted uploads persistent task started to scheduler.
        pub async fn upload_persistent_task_started(
            &mut self,
            request: impl tonic::IntoRequest<super::UploadPersistentTaskStartedRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/UploadPersistentTaskStarted",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "scheduler.v2.Scheduler",
                        "UploadPersistentTaskStarted",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// UploadPersistentTaskFinished uploads persistent task finished to scheduler.
        pub async fn upload_persistent_task_finished(
            &mut self,
            request: impl tonic::IntoRequest<super::UploadPersistentTaskFinishedRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::PersistentTask>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/UploadPersistentTaskFinished",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "scheduler.v2.Scheduler",
                        "UploadPersistentTaskFinished",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// UploadPersistentTaskFailed uploads persistent task failed to scheduler.
        pub async fn upload_persistent_task_failed(
            &mut self,
            request: impl tonic::IntoRequest<super::UploadPersistentTaskFailedRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/UploadPersistentTaskFailed",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "scheduler.v2.Scheduler",
                        "UploadPersistentTaskFailed",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Checks information of persistent task.
        pub async fn stat_persistent_task(
            &mut self,
            request: impl tonic::IntoRequest<super::StatPersistentTaskRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::PersistentTask>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/StatPersistentTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "StatPersistentTask"));
            self.inner.unary(req, path, codec).await
        }
        /// DeletePersistentTask releases persistent task in scheduler.
        pub async fn delete_persistent_task(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePersistentTaskRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/DeletePersistentTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("scheduler.v2.Scheduler", "DeletePersistentTask"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// AnnouncePersistentCachePeer announces persistent cache peer to scheduler.
        pub async fn announce_persistent_cache_peer(
            &mut self,
            request: impl tonic::IntoStreamingRequest<
                Message = super::AnnouncePersistentCachePeerRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<
                tonic::codec::Streaming<super::AnnouncePersistentCachePeerResponse>,
            >,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/AnnouncePersistentCachePeer",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "scheduler.v2.Scheduler",
                        "AnnouncePersistentCachePeer",
                    ),
                );
            self.inner.streaming(req, path, codec).await
        }
        /// Checks information of persistent cache peer.
        pub async fn stat_persistent_cache_peer(
            &mut self,
            request: impl tonic::IntoRequest<super::StatPersistentCachePeerRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::PersistentCachePeer>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/StatPersistentCachePeer",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("scheduler.v2.Scheduler", "StatPersistentCachePeer"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// DeletePersistentCachePeer releases persistent cache peer in scheduler.
        pub async fn delete_persistent_cache_peer(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePersistentCachePeerRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/DeletePersistentCachePeer",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "scheduler.v2.Scheduler",
                        "DeletePersistentCachePeer",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// UploadPersistentCacheTaskStarted uploads persistent cache task started to scheduler.
        pub async fn upload_persistent_cache_task_started(
            &mut self,
            request: impl tonic::IntoRequest<
                super::UploadPersistentCacheTaskStartedRequest,
            >,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/UploadPersistentCacheTaskStarted",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "scheduler.v2.Scheduler",
                        "UploadPersistentCacheTaskStarted",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// UploadPersistentCacheTaskFinished uploads persistent cache task finished to scheduler.
        pub async fn upload_persistent_cache_task_finished(
            &mut self,
            request: impl tonic::IntoRequest<
                super::UploadPersistentCacheTaskFinishedRequest,
            >,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::PersistentCacheTask>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/UploadPersistentCacheTaskFinished",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "scheduler.v2.Scheduler",
                        "UploadPersistentCacheTaskFinished",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// UploadPersistentCacheTaskFailed uploads persistent cache task failed to scheduler.
        pub async fn upload_persistent_cache_task_failed(
            &mut self,
            request: impl tonic::IntoRequest<
                super::UploadPersistentCacheTaskFailedRequest,
            >,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/UploadPersistentCacheTaskFailed",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "scheduler.v2.Scheduler",
                        "UploadPersistentCacheTaskFailed",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// Checks information of persistent cache task.
        pub async fn stat_persistent_cache_task(
            &mut self,
            request: impl tonic::IntoRequest<super::StatPersistentCacheTaskRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::PersistentCacheTask>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/StatPersistentCacheTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new("scheduler.v2.Scheduler", "StatPersistentCacheTask"),
                );
            self.inner.unary(req, path, codec).await
        }
        /// DeletePersistentCacheTask releases persistent cache task in scheduler.
        pub async fn delete_persistent_cache_task(
            &mut self,
            request: impl tonic::IntoRequest<super::DeletePersistentCacheTaskRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/DeletePersistentCacheTask",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(
                    GrpcMethod::new(
                        "scheduler.v2.Scheduler",
                        "DeletePersistentCacheTask",
                    ),
                );
            self.inner.unary(req, path, codec).await
        }
        /// PreheatImage synchronously resolves an image manifest and triggers an asynchronous preheat task.
        ///
        /// This is a blocking call. The RPC will not return until the server has completed the
        /// initial synchronous work: resolving the image manifest and preparing all layer URLs.
        ///
        /// After this call successfully returns, a scheduler on the server begins the actual
        /// preheating process, instructing peers to download the layers in the background.
        ///
        /// A successful response (google.protobuf.Empty) confirms that the preparation is complete
        /// and the asynchronous download task has been scheduled.
        pub async fn preheat_image(
            &mut self,
            request: impl tonic::IntoRequest<super::PreheatImageRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/PreheatImage",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "PreheatImage"));
            self.inner.unary(req, path, codec).await
        }
        /// StatImage provides detailed status for a container image's distribution in peers.
        ///
        /// This is a blocking call that first resolves the image manifest and then queries
        /// all peers to collect the image's download state across the network.
        /// The response includes both layer information and the status on each peer.
        pub async fn stat_image(
            &mut self,
            request: impl tonic::IntoRequest<super::StatImageRequest>,
        ) -> std::result::Result<
            tonic::Response<super::StatImageResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/StatImage",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "StatImage"));
            self.inner.unary(req, path, codec).await
        }
        /// PreheatFile synchronously resolves a file URL and triggers an asynchronous preheat task.
        ///
        /// This is a blocking call. The RPC will not return until the server has completed the
        /// initial synchronous work: resolving the file URL.
        ///
        /// After this call successfully returns, a scheduler on the server begins the actual
        /// preheating process, instructing peers to download the file in the background.
        ///
        /// A successful response (google.protobuf.Empty) confirms that the preparation is complete
        /// and the asynchronous download task has been scheduled.
        pub async fn preheat_file(
            &mut self,
            request: impl tonic::IntoRequest<super::PreheatFileRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/PreheatFile",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "PreheatFile"));
            self.inner.unary(req, path, codec).await
        }
        /// StatFile provides detailed status for a file's distribution in peers.
        ///
        /// This is a blocking call that first resolves the file URL and then queries
        /// all peers to collect the file's download state across the network.
        /// The response includes both file information and the status on each peer.
        pub async fn stat_file(
            &mut self,
            request: impl tonic::IntoRequest<super::StatFileRequest>,
        ) -> std::result::Result<
            tonic::Response<super::StatFileResponse>,
            tonic::Status,
        > {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::unknown(
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic_prost::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/scheduler.v2.Scheduler/StatFile",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("scheduler.v2.Scheduler", "StatFile"));
            self.inner.unary(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod scheduler_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with SchedulerServer.
    #[async_trait]
    pub trait Scheduler: std::marker::Send + std::marker::Sync + 'static {
        /// Server streaming response type for the AnnouncePeer method.
        type AnnouncePeerStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<super::AnnouncePeerResponse, tonic::Status>,
            >
            + std::marker::Send
            + 'static;
        /// AnnouncePeer announces peer to scheduler.
        async fn announce_peer(
            &self,
            request: tonic::Request<tonic::Streaming<super::AnnouncePeerRequest>>,
        ) -> std::result::Result<
            tonic::Response<Self::AnnouncePeerStream>,
            tonic::Status,
        >;
        /// Checks information of peer.
        async fn stat_peer(
            &self,
            request: tonic::Request<super::StatPeerRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::Peer>,
            tonic::Status,
        >;
        /// DeletePeer releases peer in scheduler.
        async fn delete_peer(
            &self,
            request: tonic::Request<super::DeletePeerRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// Checks information of task.
        async fn stat_task(
            &self,
            request: tonic::Request<super::StatTaskRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::Task>,
            tonic::Status,
        >;
        /// DeleteTask releases task in scheduler.
        async fn delete_task(
            &self,
            request: tonic::Request<super::DeleteTaskRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// AnnounceHost announces host to scheduler.
        async fn announce_host(
            &self,
            request: tonic::Request<super::AnnounceHostRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// ListHosts lists hosts in scheduler.
        async fn list_hosts(
            &self,
            request: tonic::Request<super::ListHostsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListHostsResponse>,
            tonic::Status,
        >;
        /// DeleteHost releases host in scheduler.
        async fn delete_host(
            &self,
            request: tonic::Request<super::DeleteHostRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// Server streaming response type for the AnnounceCachePeer method.
        type AnnounceCachePeerStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<
                    super::AnnounceCachePeerResponse,
                    tonic::Status,
                >,
            >
            + std::marker::Send
            + 'static;
        /// AnnounceCachePeer announces cache peer to scheduler.
        async fn announce_cache_peer(
            &self,
            request: tonic::Request<tonic::Streaming<super::AnnounceCachePeerRequest>>,
        ) -> std::result::Result<
            tonic::Response<Self::AnnounceCachePeerStream>,
            tonic::Status,
        >;
        /// Checks information of cache peer.
        async fn stat_cache_peer(
            &self,
            request: tonic::Request<super::StatCachePeerRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::CachePeer>,
            tonic::Status,
        >;
        /// DeletCacheePeer releases cache peer in scheduler.
        async fn delete_cache_peer(
            &self,
            request: tonic::Request<super::DeleteCachePeerRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// Checks information of cache task.
        async fn stat_cache_task(
            &self,
            request: tonic::Request<super::StatCacheTaskRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::CacheTask>,
            tonic::Status,
        >;
        /// DeleteCacheTask releases cache task in scheduler.
        async fn delete_cache_task(
            &self,
            request: tonic::Request<super::DeleteCacheTaskRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// Server streaming response type for the AnnouncePersistentPeer method.
        type AnnouncePersistentPeerStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<
                    super::AnnouncePersistentPeerResponse,
                    tonic::Status,
                >,
            >
            + std::marker::Send
            + 'static;
        /// AnnouncePersistentPeer announces persistent peer to scheduler.
        async fn announce_persistent_peer(
            &self,
            request: tonic::Request<
                tonic::Streaming<super::AnnouncePersistentPeerRequest>,
            >,
        ) -> std::result::Result<
            tonic::Response<Self::AnnouncePersistentPeerStream>,
            tonic::Status,
        >;
        /// Checks information of persistent peer.
        async fn stat_persistent_peer(
            &self,
            request: tonic::Request<super::StatPersistentPeerRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::PersistentPeer>,
            tonic::Status,
        >;
        /// DeletePersistentPeer releases persistent peer in scheduler.
        async fn delete_persistent_peer(
            &self,
            request: tonic::Request<super::DeletePersistentPeerRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// UploadPersistentTaskStarted uploads persistent task started to scheduler.
        async fn upload_persistent_task_started(
            &self,
            request: tonic::Request<super::UploadPersistentTaskStartedRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// UploadPersistentTaskFinished uploads persistent task finished to scheduler.
        async fn upload_persistent_task_finished(
            &self,
            request: tonic::Request<super::UploadPersistentTaskFinishedRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::PersistentTask>,
            tonic::Status,
        >;
        /// UploadPersistentTaskFailed uploads persistent task failed to scheduler.
        async fn upload_persistent_task_failed(
            &self,
            request: tonic::Request<super::UploadPersistentTaskFailedRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// Checks information of persistent task.
        async fn stat_persistent_task(
            &self,
            request: tonic::Request<super::StatPersistentTaskRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::PersistentTask>,
            tonic::Status,
        >;
        /// DeletePersistentTask releases persistent task in scheduler.
        async fn delete_persistent_task(
            &self,
            request: tonic::Request<super::DeletePersistentTaskRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// Server streaming response type for the AnnouncePersistentCachePeer method.
        type AnnouncePersistentCachePeerStream: tonic::codegen::tokio_stream::Stream<
                Item = std::result::Result<
                    super::AnnouncePersistentCachePeerResponse,
                    tonic::Status,
                >,
            >
            + std::marker::Send
            + 'static;
        /// AnnouncePersistentCachePeer announces persistent cache peer to scheduler.
        async fn announce_persistent_cache_peer(
            &self,
            request: tonic::Request<
                tonic::Streaming<super::AnnouncePersistentCachePeerRequest>,
            >,
        ) -> std::result::Result<
            tonic::Response<Self::AnnouncePersistentCachePeerStream>,
            tonic::Status,
        >;
        /// Checks information of persistent cache peer.
        async fn stat_persistent_cache_peer(
            &self,
            request: tonic::Request<super::StatPersistentCachePeerRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::PersistentCachePeer>,
            tonic::Status,
        >;
        /// DeletePersistentCachePeer releases persistent cache peer in scheduler.
        async fn delete_persistent_cache_peer(
            &self,
            request: tonic::Request<super::DeletePersistentCachePeerRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// UploadPersistentCacheTaskStarted uploads persistent cache task started to scheduler.
        async fn upload_persistent_cache_task_started(
            &self,
            request: tonic::Request<super::UploadPersistentCacheTaskStartedRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// UploadPersistentCacheTaskFinished uploads persistent cache task finished to scheduler.
        async fn upload_persistent_cache_task_finished(
            &self,
            request: tonic::Request<super::UploadPersistentCacheTaskFinishedRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::PersistentCacheTask>,
            tonic::Status,
        >;
        /// UploadPersistentCacheTaskFailed uploads persistent cache task failed to scheduler.
        async fn upload_persistent_cache_task_failed(
            &self,
            request: tonic::Request<super::UploadPersistentCacheTaskFailedRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// Checks information of persistent cache task.
        async fn stat_persistent_cache_task(
            &self,
            request: tonic::Request<super::StatPersistentCacheTaskRequest>,
        ) -> std::result::Result<
            tonic::Response<super::super::super::common::v2::PersistentCacheTask>,
            tonic::Status,
        >;
        /// DeletePersistentCacheTask releases persistent cache task in scheduler.
        async fn delete_persistent_cache_task(
            &self,
            request: tonic::Request<super::DeletePersistentCacheTaskRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// PreheatImage synchronously resolves an image manifest and triggers an asynchronous preheat task.
        ///
        /// This is a blocking call. The RPC will not return until the server has completed the
        /// initial synchronous work: resolving the image manifest and preparing all layer URLs.
        ///
        /// After this call successfully returns, a scheduler on the server begins the actual
        /// preheating process, instructing peers to download the layers in the background.
        ///
        /// A successful response (google.protobuf.Empty) confirms that the preparation is complete
        /// and the asynchronous download task has been scheduled.
        async fn preheat_image(
            &self,
            request: tonic::Request<super::PreheatImageRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// StatImage provides detailed status for a container image's distribution in peers.
        ///
        /// This is a blocking call that first resolves the image manifest and then queries
        /// all peers to collect the image's download state across the network.
        /// The response includes both layer information and the status on each peer.
        async fn stat_image(
            &self,
            request: tonic::Request<super::StatImageRequest>,
        ) -> std::result::Result<
            tonic::Response<super::StatImageResponse>,
            tonic::Status,
        >;
        /// PreheatFile synchronously resolves a file URL and triggers an asynchronous preheat task.
        ///
        /// This is a blocking call. The RPC will not return until the server has completed the
        /// initial synchronous work: resolving the file URL.
        ///
        /// After this call successfully returns, a scheduler on the server begins the actual
        /// preheating process, instructing peers to download the file in the background.
        ///
        /// A successful response (google.protobuf.Empty) confirms that the preparation is complete
        /// and the asynchronous download task has been scheduled.
        async fn preheat_file(
            &self,
            request: tonic::Request<super::PreheatFileRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// StatFile provides detailed status for a file's distribution in peers.
        ///
        /// This is a blocking call that first resolves the file URL and then queries
        /// all peers to collect the file's download state across the network.
        /// The response includes both file information and the status on each peer.
        async fn stat_file(
            &self,
            request: tonic::Request<super::StatFileRequest>,
        ) -> std::result::Result<
            tonic::Response<super::StatFileResponse>,
            tonic::Status,
        >;
    }
    /// Scheduler RPC Service.
    #[derive(Debug)]
    pub struct SchedulerServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> SchedulerServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
                max_decoding_message_size: None,
                max_encoding_message_size: None,
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
        /// Enable decompressing requests with the given encoding.
        #[must_use]
        pub fn accept_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.accept_compression_encodings.enable(encoding);
            self
        }
        /// Compress responses with the given encoding, if the client supports it.
        #[must_use]
        pub fn send_compressed(mut self, encoding: CompressionEncoding) -> Self {
            self.send_compression_encodings.enable(encoding);
            self
        }
        /// Limits the maximum size of a decoded message.
        ///
        /// Default: `4MB`
        #[must_use]
        pub fn max_decoding_message_size(mut self, limit: usize) -> Self {
            self.max_decoding_message_size = Some(limit);
            self
        }
        /// Limits the maximum size of an encoded message.
        ///
        /// Default: `usize::MAX`
        #[must_use]
        pub fn max_encoding_message_size(mut self, limit: usize) -> Self {
            self.max_encoding_message_size = Some(limit);
            self
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for SchedulerServer<T>
    where
        T: Scheduler,
        B: Body + std::marker::Send + 'static,
        B::Error: Into<StdError> + std::marker::Send + 'static,
    {
        type Response = http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<std::result::Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            match req.uri().path() {
                "/scheduler.v2.Scheduler/AnnouncePeer" => {
                    #[allow(non_camel_case_types)]
                    struct AnnouncePeerSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::StreamingService<super::AnnouncePeerRequest>
                    for AnnouncePeerSvc<T> {
                        type Response = super::AnnouncePeerResponse;
                        type ResponseStream = T::AnnouncePeerStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::AnnouncePeerRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::announce_peer(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = AnnouncePeerSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/StatPeer" => {
                    #[allow(non_camel_case_types)]
                    struct StatPeerSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::StatPeerRequest>
                    for StatPeerSvc<T> {
                        type Response = super::super::super::common::v2::Peer;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StatPeerRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::stat_peer(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = StatPeerSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/DeletePeer" => {
                    #[allow(non_camel_case_types)]
                    struct DeletePeerSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::DeletePeerRequest>
                    for DeletePeerSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeletePeerRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::delete_peer(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DeletePeerSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/StatTask" => {
                    #[allow(non_camel_case_types)]
                    struct StatTaskSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::StatTaskRequest>
                    for StatTaskSvc<T> {
                        type Response = super::super::super::common::v2::Task;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StatTaskRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::stat_task(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = StatTaskSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/DeleteTask" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteTaskSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::DeleteTaskRequest>
                    for DeleteTaskSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteTaskRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::delete_task(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DeleteTaskSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/AnnounceHost" => {
                    #[allow(non_camel_case_types)]
                    struct AnnounceHostSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::AnnounceHostRequest>
                    for AnnounceHostSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::AnnounceHostRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::announce_host(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = AnnounceHostSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/ListHosts" => {
                    #[allow(non_camel_case_types)]
                    struct ListHostsSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::ListHostsRequest>
                    for ListHostsSvc<T> {
                        type Response = super::ListHostsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListHostsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::list_hosts(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = ListHostsSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/DeleteHost" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteHostSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::DeleteHostRequest>
                    for DeleteHostSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteHostRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::delete_host(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DeleteHostSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/AnnounceCachePeer" => {
                    #[allow(non_camel_case_types)]
                    struct AnnounceCachePeerSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::StreamingService<super::AnnounceCachePeerRequest>
                    for AnnounceCachePeerSvc<T> {
                        type Response = super::AnnounceCachePeerResponse;
                        type ResponseStream = T::AnnounceCachePeerStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::AnnounceCachePeerRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::announce_cache_peer(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = AnnounceCachePeerSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/StatCachePeer" => {
                    #[allow(non_camel_case_types)]
                    struct StatCachePeerSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::StatCachePeerRequest>
                    for StatCachePeerSvc<T> {
                        type Response = super::super::super::common::v2::CachePeer;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StatCachePeerRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::stat_cache_peer(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = StatCachePeerSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/DeleteCachePeer" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteCachePeerSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::DeleteCachePeerRequest>
                    for DeleteCachePeerSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteCachePeerRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::delete_cache_peer(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DeleteCachePeerSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/StatCacheTask" => {
                    #[allow(non_camel_case_types)]
                    struct StatCacheTaskSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::StatCacheTaskRequest>
                    for StatCacheTaskSvc<T> {
                        type Response = super::super::super::common::v2::CacheTask;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StatCacheTaskRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::stat_cache_task(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = StatCacheTaskSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/DeleteCacheTask" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteCacheTaskSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::DeleteCacheTaskRequest>
                    for DeleteCacheTaskSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteCacheTaskRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::delete_cache_task(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DeleteCacheTaskSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/AnnouncePersistentPeer" => {
                    #[allow(non_camel_case_types)]
                    struct AnnouncePersistentPeerSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::StreamingService<
                        super::AnnouncePersistentPeerRequest,
                    > for AnnouncePersistentPeerSvc<T> {
                        type Response = super::AnnouncePersistentPeerResponse;
                        type ResponseStream = T::AnnouncePersistentPeerStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::AnnouncePersistentPeerRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::announce_persistent_peer(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = AnnouncePersistentPeerSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/StatPersistentPeer" => {
                    #[allow(non_camel_case_types)]
                    struct StatPersistentPeerSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::StatPersistentPeerRequest>
                    for StatPersistentPeerSvc<T> {
                        type Response = super::super::super::common::v2::PersistentPeer;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StatPersistentPeerRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::stat_persistent_peer(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = StatPersistentPeerSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/DeletePersistentPeer" => {
                    #[allow(non_camel_case_types)]
                    struct DeletePersistentPeerSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::DeletePersistentPeerRequest>
                    for DeletePersistentPeerSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeletePersistentPeerRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::delete_persistent_peer(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DeletePersistentPeerSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/UploadPersistentTaskStarted" => {
                    #[allow(non_camel_case_types)]
                    struct UploadPersistentTaskStartedSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<
                        super::UploadPersistentTaskStartedRequest,
                    > for UploadPersistentTaskStartedSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::UploadPersistentTaskStartedRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::upload_persistent_task_started(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = UploadPersistentTaskStartedSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/UploadPersistentTaskFinished" => {
                    #[allow(non_camel_case_types)]
                    struct UploadPersistentTaskFinishedSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<
                        super::UploadPersistentTaskFinishedRequest,
                    > for UploadPersistentTaskFinishedSvc<T> {
                        type Response = super::super::super::common::v2::PersistentTask;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::UploadPersistentTaskFinishedRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::upload_persistent_task_finished(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = UploadPersistentTaskFinishedSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/UploadPersistentTaskFailed" => {
                    #[allow(non_camel_case_types)]
                    struct UploadPersistentTaskFailedSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<
                        super::UploadPersistentTaskFailedRequest,
                    > for UploadPersistentTaskFailedSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::UploadPersistentTaskFailedRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::upload_persistent_task_failed(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = UploadPersistentTaskFailedSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/StatPersistentTask" => {
                    #[allow(non_camel_case_types)]
                    struct StatPersistentTaskSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::StatPersistentTaskRequest>
                    for StatPersistentTaskSvc<T> {
                        type Response = super::super::super::common::v2::PersistentTask;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StatPersistentTaskRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::stat_persistent_task(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = StatPersistentTaskSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/DeletePersistentTask" => {
                    #[allow(non_camel_case_types)]
                    struct DeletePersistentTaskSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::DeletePersistentTaskRequest>
                    for DeletePersistentTaskSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeletePersistentTaskRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::delete_persistent_task(&inner, request)
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DeletePersistentTaskSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/AnnouncePersistentCachePeer" => {
                    #[allow(non_camel_case_types)]
                    struct AnnouncePersistentCachePeerSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::StreamingService<
                        super::AnnouncePersistentCachePeerRequest,
                    > for AnnouncePersistentCachePeerSvc<T> {
                        type Response = super::AnnouncePersistentCachePeerResponse;
                        type ResponseStream = T::AnnouncePersistentCachePeerStream;
                        type Future = BoxFuture<
                            tonic::Response<Self::ResponseStream>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::AnnouncePersistentCachePeerRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::announce_persistent_cache_peer(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = AnnouncePersistentCachePeerSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.streaming(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/StatPersistentCachePeer" => {
                    #[allow(non_camel_case_types)]
                    struct StatPersistentCachePeerSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::StatPersistentCachePeerRequest>
                    for StatPersistentCachePeerSvc<T> {
                        type Response = super::super::super::common::v2::PersistentCachePeer;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::StatPersistentCachePeerRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::stat_persistent_cache_peer(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = StatPersistentCachePeerSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/DeletePersistentCachePeer" => {
                    #[allow(non_camel_case_types)]
                    struct DeletePersistentCachePeerSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<
                        super::DeletePersistentCachePeerRequest,
                    > for DeletePersistentCachePeerSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::DeletePersistentCachePeerRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::delete_persistent_cache_peer(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DeletePersistentCachePeerSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/UploadPersistentCacheTaskStarted" => {
                    #[allow(non_camel_case_types)]
                    struct UploadPersistentCacheTaskStartedSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<
                        super::UploadPersistentCacheTaskStartedRequest,
                    > for UploadPersistentCacheTaskStartedSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::UploadPersistentCacheTaskStartedRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::upload_persistent_cache_task_started(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = UploadPersistentCacheTaskStartedSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/UploadPersistentCacheTaskFinished" => {
                    #[allow(non_camel_case_types)]
                    struct UploadPersistentCacheTaskFinishedSvc<T: Scheduler>(
                        pub Arc<T>,
                    );
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<
                        super::UploadPersistentCacheTaskFinishedRequest,
                    > for UploadPersistentCacheTaskFinishedSvc<T> {
                        type Response = super::super::super::common::v2::PersistentCacheTask;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::UploadPersistentCacheTaskFinishedRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::upload_persistent_cache_task_finished(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = UploadPersistentCacheTaskFinishedSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/UploadPersistentCacheTaskFailed" => {
                    #[allow(non_camel_case_types)]
                    struct UploadPersistentCacheTaskFailedSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<
                        super::UploadPersistentCacheTaskFailedRequest,
                    > for UploadPersistentCacheTaskFailedSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::UploadPersistentCacheTaskFailedRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::upload_persistent_cache_task_failed(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = UploadPersistentCacheTaskFailedSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/StatPersistentCacheTask" => {
                    #[allow(non_camel_case_types)]
                    struct StatPersistentCacheTaskSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::StatPersistentCacheTaskRequest>
                    for StatPersistentCacheTaskSvc<T> {
                        type Response = super::super::super::common::v2::PersistentCacheTask;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::StatPersistentCacheTaskRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::stat_persistent_cache_task(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = StatPersistentCacheTaskSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/DeletePersistentCacheTask" => {
                    #[allow(non_camel_case_types)]
                    struct DeletePersistentCacheTaskSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<
                        super::DeletePersistentCacheTaskRequest,
                    > for DeletePersistentCacheTaskSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                super::DeletePersistentCacheTaskRequest,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::delete_persistent_cache_task(
                                        &inner,
                                        request,
                                    )
                                    .await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = DeletePersistentCacheTaskSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/PreheatImage" => {
                    #[allow(non_camel_case_types)]
                    struct PreheatImageSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::PreheatImageRequest>
                    for PreheatImageSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PreheatImageRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::preheat_image(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = PreheatImageSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/StatImage" => {
                    #[allow(non_camel_case_types)]
                    struct StatImageSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::StatImageRequest>
                    for StatImageSvc<T> {
                        type Response = super::StatImageResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StatImageRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::stat_image(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = StatImageSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/PreheatFile" => {
                    #[allow(non_camel_case_types)]
                    struct PreheatFileSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::PreheatFileRequest>
                    for PreheatFileSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::PreheatFileRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::preheat_file(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = PreheatFileSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                "/scheduler.v2.Scheduler/StatFile" => {
                    #[allow(non_camel_case_types)]
                    struct StatFileSvc<T: Scheduler>(pub Arc<T>);
                    impl<
                        T: Scheduler,
                    > tonic::server::UnaryService<super::StatFileRequest>
                    for StatFileSvc<T> {
                        type Response = super::StatFileResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::StatFileRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Scheduler>::stat_file(&inner, request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let max_decoding_message_size = self.max_decoding_message_size;
                    let max_encoding_message_size = self.max_encoding_message_size;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let method = StatFileSvc(inner);
                        let codec = tonic_prost::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            )
                            .apply_max_message_size_config(
                                max_decoding_message_size,
                                max_encoding_message_size,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        let mut response = http::Response::new(
                            tonic::body::Body::default(),
                        );
                        let headers = response.headers_mut();
                        headers
                            .insert(
                                tonic::Status::GRPC_STATUS,
                                (tonic::Code::Unimplemented as i32).into(),
                            );
                        headers
                            .insert(
                                http::header::CONTENT_TYPE,
                                tonic::metadata::GRPC_CONTENT_TYPE,
                            );
                        Ok(response)
                    })
                }
            }
        }
    }
    impl<T> Clone for SchedulerServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
                max_decoding_message_size: self.max_decoding_message_size,
                max_encoding_message_size: self.max_encoding_message_size,
            }
        }
    }
    /// Generated gRPC service name
    pub const SERVICE_NAME: &str = "scheduler.v2.Scheduler";
    impl<T> tonic::server::NamedService for SchedulerServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
