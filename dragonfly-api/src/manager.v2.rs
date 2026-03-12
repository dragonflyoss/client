/// SeedPeerCluster represents cluster of seed peer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct SeedPeerCluster {
    /// Cluster id.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// Cluster name.
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    /// Cluster biography.
    #[prost(string, tag = "3")]
    pub bio: ::prost::alloc::string::String,
    /// Cluster configuration.
    #[prost(bytes = "vec", tag = "4")]
    pub config: ::prost::alloc::vec::Vec<u8>,
}
/// SeedPeer represents seed peer for network.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct SeedPeer {
    /// Seed peer id.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// Seed peer hostname.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// Seed peer type.
    #[prost(string, tag = "3")]
    pub r#type: ::prost::alloc::string::String,
    /// Seed peer idc.
    #[prost(string, optional, tag = "4")]
    pub idc: ::core::option::Option<::prost::alloc::string::String>,
    /// Seed peer location.
    #[prost(string, optional, tag = "5")]
    pub location: ::core::option::Option<::prost::alloc::string::String>,
    /// Seed peer ip.
    #[prost(string, tag = "6")]
    pub ip: ::prost::alloc::string::String,
    /// Seed peer grpc port.
    #[prost(int32, tag = "7")]
    pub port: i32,
    /// Seed peer download port.
    #[prost(int32, tag = "8")]
    pub download_port: i32,
    /// Seed peer state.
    #[prost(string, tag = "9")]
    pub state: ::prost::alloc::string::String,
    /// ID of the cluster to which the seed peer belongs.
    #[prost(uint64, tag = "10")]
    pub seed_peer_cluster_id: u64,
    /// Cluster to which the seed peer belongs.
    #[prost(message, optional, tag = "11")]
    pub seed_peer_cluster: ::core::option::Option<SeedPeerCluster>,
    /// Schedulers included in seed peer.
    #[prost(message, repeated, tag = "12")]
    pub schedulers: ::prost::alloc::vec::Vec<Scheduler>,
}
/// GetSeedPeerRequest represents request of GetSeedPeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct GetSeedPeerRequest {
    /// Request source type.
    #[prost(enumeration = "SourceType", tag = "1")]
    pub source_type: i32,
    /// Seed peer hostname.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// ID of the cluster to which the seed peer belongs.
    #[prost(uint64, tag = "3")]
    pub seed_peer_cluster_id: u64,
    /// Seed peer ip.
    #[prost(string, tag = "4")]
    pub ip: ::prost::alloc::string::String,
}
/// ListSeedPeersRequest represents request of ListSeedPeers.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct ListSeedPeersRequest {
    /// Request source type.
    #[prost(enumeration = "SourceType", tag = "1")]
    pub source_type: i32,
    /// Source service hostname.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// Source service ip.
    #[prost(string, tag = "3")]
    pub ip: ::prost::alloc::string::String,
    /// Dfdaemon version.
    #[prost(string, tag = "4")]
    pub version: ::prost::alloc::string::String,
    /// Dfdaemon commit.
    #[prost(string, tag = "5")]
    pub commit: ::prost::alloc::string::String,
}
/// ListSeedPeersResponse represents response of ListSeedPeers.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSeedPeersResponse {
    /// Seed peers to which the source service belongs.
    #[prost(message, repeated, tag = "1")]
    pub seed_peers: ::prost::alloc::vec::Vec<SeedPeer>,
}
/// UpdateSeedPeerRequest represents request of UpdateSeedPeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UpdateSeedPeerRequest {
    /// Request source type.
    #[prost(enumeration = "SourceType", tag = "1")]
    pub source_type: i32,
    /// Seed peer hostname.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// Seed peer type.
    #[prost(string, tag = "3")]
    pub r#type: ::prost::alloc::string::String,
    /// Seed peer idc.
    #[prost(string, optional, tag = "4")]
    pub idc: ::core::option::Option<::prost::alloc::string::String>,
    /// Seed peer location.
    #[prost(string, optional, tag = "5")]
    pub location: ::core::option::Option<::prost::alloc::string::String>,
    /// Seed peer ip.
    #[prost(string, tag = "6")]
    pub ip: ::prost::alloc::string::String,
    /// Seed peer port.
    #[prost(int32, tag = "7")]
    pub port: i32,
    /// Seed peer download port.
    #[prost(int32, tag = "8")]
    pub download_port: i32,
    /// ID of the cluster to which the seed peer belongs.
    #[prost(uint64, tag = "9")]
    pub seed_peer_cluster_id: u64,
}
/// DeleteSeedPeerRequest represents request of DeleteSeedPeer.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct DeleteSeedPeerRequest {
    /// Request source type.
    #[prost(enumeration = "SourceType", tag = "1")]
    pub source_type: i32,
    /// Seed peer hostname.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// ID of the cluster to which the seed peer belongs.
    #[prost(uint64, tag = "3")]
    pub seed_peer_cluster_id: u64,
    /// Seed peer ip.
    #[prost(string, tag = "4")]
    pub ip: ::prost::alloc::string::String,
}
/// SeedPeerCluster represents cluster of scheduler.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct SchedulerCluster {
    /// Cluster id.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// Cluster name.
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    /// Cluster biography.
    #[prost(string, tag = "3")]
    pub bio: ::prost::alloc::string::String,
    /// Cluster config.
    #[prost(bytes = "vec", tag = "4")]
    pub config: ::prost::alloc::vec::Vec<u8>,
    /// Cluster client config.
    #[prost(bytes = "vec", tag = "5")]
    pub client_config: ::prost::alloc::vec::Vec<u8>,
    /// Cluster scopes.
    #[prost(bytes = "vec", tag = "6")]
    pub scopes: ::prost::alloc::vec::Vec<u8>,
    /// Cluster seed client config.
    #[prost(bytes = "vec", tag = "7")]
    pub seed_client_config: ::prost::alloc::vec::Vec<u8>,
}
/// SeedPeerCluster represents scheduler for network.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Scheduler {
    /// Scheduler id.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// Scheduler hostname.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// Scheduler idc.
    #[prost(string, optional, tag = "3")]
    pub idc: ::core::option::Option<::prost::alloc::string::String>,
    /// Scheduler location.
    #[prost(string, optional, tag = "4")]
    pub location: ::core::option::Option<::prost::alloc::string::String>,
    /// Scheduler ip.
    #[prost(string, tag = "5")]
    pub ip: ::prost::alloc::string::String,
    /// Scheduler grpc port.
    #[prost(int32, tag = "6")]
    pub port: i32,
    /// Scheduler state.
    #[prost(string, tag = "7")]
    pub state: ::prost::alloc::string::String,
    /// ID of the cluster to which the scheduler belongs.
    #[prost(uint64, tag = "8")]
    pub scheduler_cluster_id: u64,
    /// Cluster to which the scheduler belongs.
    #[prost(message, optional, tag = "9")]
    pub scheduler_cluster: ::core::option::Option<SchedulerCluster>,
    /// Seed peers to which the scheduler belongs.
    #[prost(message, repeated, tag = "10")]
    pub seed_peers: ::prost::alloc::vec::Vec<SeedPeer>,
    /// Feature flags of scheduler.
    #[prost(bytes = "vec", tag = "11")]
    pub features: ::prost::alloc::vec::Vec<u8>,
}
/// GetSchedulerRequest represents request of GetScheduler.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct GetSchedulerRequest {
    /// Request source type.
    #[prost(enumeration = "SourceType", tag = "1")]
    pub source_type: i32,
    /// Scheduler hostname.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// ID of the cluster to which the scheduler belongs.
    #[prost(uint64, tag = "3")]
    pub scheduler_cluster_id: u64,
    /// Scheduler ip.
    #[prost(string, tag = "4")]
    pub ip: ::prost::alloc::string::String,
}
/// UpdateSchedulerRequest represents request of UpdateScheduler.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UpdateSchedulerRequest {
    /// Request source type.
    #[prost(enumeration = "SourceType", tag = "1")]
    pub source_type: i32,
    /// Scheduler hostname.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// ID of the cluster to which the scheduler belongs.
    #[prost(uint64, tag = "3")]
    pub scheduler_cluster_id: u64,
    /// Scheduler idc.
    #[prost(string, optional, tag = "4")]
    pub idc: ::core::option::Option<::prost::alloc::string::String>,
    /// Scheduler location.
    #[prost(string, optional, tag = "5")]
    pub location: ::core::option::Option<::prost::alloc::string::String>,
    /// Scheduler ip.
    #[prost(string, tag = "6")]
    pub ip: ::prost::alloc::string::String,
    /// Scheduler port.
    #[prost(int32, tag = "7")]
    pub port: i32,
    /// Scheduler features.
    #[prost(string, repeated, tag = "8")]
    pub features: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Scheduler Configuration.
    #[prost(bytes = "vec", tag = "9")]
    pub config: ::prost::alloc::vec::Vec<u8>,
}
/// ListSchedulersRequest represents request of ListSchedulers.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct ListSchedulersRequest {
    /// Request source type.
    #[prost(enumeration = "SourceType", tag = "1")]
    pub source_type: i32,
    /// Source service hostname.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// Source service ip.
    #[prost(string, tag = "3")]
    pub ip: ::prost::alloc::string::String,
    /// Source idc.
    #[prost(string, optional, tag = "4")]
    pub idc: ::core::option::Option<::prost::alloc::string::String>,
    /// Source location.
    #[prost(string, optional, tag = "5")]
    pub location: ::core::option::Option<::prost::alloc::string::String>,
    /// Dfdaemon version.
    #[prost(string, tag = "6")]
    pub version: ::prost::alloc::string::String,
    /// Dfdaemon commit.
    #[prost(string, tag = "7")]
    pub commit: ::prost::alloc::string::String,
    /// ID of the cluster to which the scheduler belongs.
    #[prost(uint64, tag = "8")]
    pub scheduler_cluster_id: u64,
}
/// ListSchedulersResponse represents response of ListSchedulers.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListSchedulersResponse {
    /// Schedulers to which the source service belongs.
    #[prost(message, repeated, tag = "1")]
    pub schedulers: ::prost::alloc::vec::Vec<Scheduler>,
}
/// URLPriority represents config of url priority.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct UrlPriority {
    /// URL regex.
    #[prost(string, tag = "1")]
    pub regex: ::prost::alloc::string::String,
    /// URL priority value.
    #[prost(enumeration = "super::super::common::v2::Priority", tag = "2")]
    pub value: i32,
}
/// ApplicationPriority represents config of application priority.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ApplicationPriority {
    /// Priority value.
    #[prost(enumeration = "super::super::common::v2::Priority", tag = "1")]
    pub value: i32,
    /// URL priority.
    #[prost(message, repeated, tag = "2")]
    pub urls: ::prost::alloc::vec::Vec<UrlPriority>,
}
/// Application represents config of application.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Application {
    /// Application id.
    #[prost(uint64, tag = "1")]
    pub id: u64,
    /// Application name.
    #[prost(string, tag = "2")]
    pub name: ::prost::alloc::string::String,
    /// Application url.
    #[prost(string, tag = "3")]
    pub url: ::prost::alloc::string::String,
    /// Application biography.
    #[prost(string, tag = "4")]
    pub bio: ::prost::alloc::string::String,
    /// Application priority.
    #[prost(message, optional, tag = "5")]
    pub priority: ::core::option::Option<ApplicationPriority>,
}
/// ListApplicationsRequest represents request of ListApplications.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct ListApplicationsRequest {
    /// Request source type.
    #[prost(enumeration = "SourceType", tag = "1")]
    pub source_type: i32,
    /// Source service hostname.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// Source service ip.
    #[prost(string, tag = "3")]
    pub ip: ::prost::alloc::string::String,
}
/// ListApplicationsResponse represents response of ListApplications.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ListApplicationsResponse {
    /// Application configs.
    #[prost(message, repeated, tag = "1")]
    pub applications: ::prost::alloc::vec::Vec<Application>,
}
/// KeepAliveRequest represents request of KeepAlive.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct KeepAliveRequest {
    /// Request source type.
    #[prost(enumeration = "SourceType", tag = "1")]
    pub source_type: i32,
    /// Source service hostname.
    #[prost(string, tag = "2")]
    pub hostname: ::prost::alloc::string::String,
    /// ID of the cluster to which the source service belongs.
    #[prost(uint64, tag = "3")]
    pub cluster_id: u64,
    /// Source service ip.
    #[prost(string, tag = "4")]
    pub ip: ::prost::alloc::string::String,
}
/// Request source type.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SourceType {
    /// Scheduler service.
    SchedulerSource = 0,
    /// Peer service.
    PeerSource = 1,
    /// SeedPeer service.
    SeedPeerSource = 2,
}
impl SourceType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::SchedulerSource => "SCHEDULER_SOURCE",
            Self::PeerSource => "PEER_SOURCE",
            Self::SeedPeerSource => "SEED_PEER_SOURCE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "SCHEDULER_SOURCE" => Some(Self::SchedulerSource),
            "PEER_SOURCE" => Some(Self::PeerSource),
            "SEED_PEER_SOURCE" => Some(Self::SeedPeerSource),
            _ => None,
        }
    }
}
/// Generated client implementations.
pub mod manager_client {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    use tonic::codegen::http::Uri;
    /// Manager RPC Service.
    #[derive(Debug, Clone)]
    pub struct ManagerClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ManagerClient<tonic::transport::Channel> {
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
    impl<T> ManagerClient<T>
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
        ) -> ManagerClient<InterceptedService<T, F>>
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
            ManagerClient::new(InterceptedService::new(inner, interceptor))
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
        /// Get SeedPeer and SeedPeer cluster configuration.
        pub async fn get_seed_peer(
            &mut self,
            request: impl tonic::IntoRequest<super::GetSeedPeerRequest>,
        ) -> std::result::Result<tonic::Response<super::SeedPeer>, tonic::Status> {
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
                "/manager.v2.Manager/GetSeedPeer",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("manager.v2.Manager", "GetSeedPeer"));
            self.inner.unary(req, path, codec).await
        }
        /// List acitve schedulers configuration.
        pub async fn list_seed_peers(
            &mut self,
            request: impl tonic::IntoRequest<super::ListSeedPeersRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSeedPeersResponse>,
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
                "/manager.v2.Manager/ListSeedPeers",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("manager.v2.Manager", "ListSeedPeers"));
            self.inner.unary(req, path, codec).await
        }
        /// Update SeedPeer configuration.
        pub async fn update_seed_peer(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateSeedPeerRequest>,
        ) -> std::result::Result<tonic::Response<super::SeedPeer>, tonic::Status> {
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
                "/manager.v2.Manager/UpdateSeedPeer",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("manager.v2.Manager", "UpdateSeedPeer"));
            self.inner.unary(req, path, codec).await
        }
        /// Delete SeedPeer configuration.
        pub async fn delete_seed_peer(
            &mut self,
            request: impl tonic::IntoRequest<super::DeleteSeedPeerRequest>,
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
                "/manager.v2.Manager/DeleteSeedPeer",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("manager.v2.Manager", "DeleteSeedPeer"));
            self.inner.unary(req, path, codec).await
        }
        /// Get Scheduler and Scheduler cluster configuration.
        pub async fn get_scheduler(
            &mut self,
            request: impl tonic::IntoRequest<super::GetSchedulerRequest>,
        ) -> std::result::Result<tonic::Response<super::Scheduler>, tonic::Status> {
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
                "/manager.v2.Manager/GetScheduler",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("manager.v2.Manager", "GetScheduler"));
            self.inner.unary(req, path, codec).await
        }
        /// Update scheduler configuration.
        pub async fn update_scheduler(
            &mut self,
            request: impl tonic::IntoRequest<super::UpdateSchedulerRequest>,
        ) -> std::result::Result<tonic::Response<super::Scheduler>, tonic::Status> {
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
                "/manager.v2.Manager/UpdateScheduler",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("manager.v2.Manager", "UpdateScheduler"));
            self.inner.unary(req, path, codec).await
        }
        /// List acitve schedulers configuration.
        pub async fn list_schedulers(
            &mut self,
            request: impl tonic::IntoRequest<super::ListSchedulersRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSchedulersResponse>,
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
                "/manager.v2.Manager/ListSchedulers",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("manager.v2.Manager", "ListSchedulers"));
            self.inner.unary(req, path, codec).await
        }
        /// List applications configuration.
        pub async fn list_applications(
            &mut self,
            request: impl tonic::IntoRequest<super::ListApplicationsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListApplicationsResponse>,
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
                "/manager.v2.Manager/ListApplications",
            );
            let mut req = request.into_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("manager.v2.Manager", "ListApplications"));
            self.inner.unary(req, path, codec).await
        }
        /// KeepAlive with manager.
        pub async fn keep_alive(
            &mut self,
            request: impl tonic::IntoStreamingRequest<Message = super::KeepAliveRequest>,
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
                "/manager.v2.Manager/KeepAlive",
            );
            let mut req = request.into_streaming_request();
            req.extensions_mut()
                .insert(GrpcMethod::new("manager.v2.Manager", "KeepAlive"));
            self.inner.client_streaming(req, path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod manager_server {
    #![allow(
        unused_variables,
        dead_code,
        missing_docs,
        clippy::wildcard_imports,
        clippy::let_unit_value,
    )]
    use tonic::codegen::*;
    /// Generated trait containing gRPC methods that should be implemented for use with ManagerServer.
    #[async_trait]
    pub trait Manager: std::marker::Send + std::marker::Sync + 'static {
        /// Get SeedPeer and SeedPeer cluster configuration.
        async fn get_seed_peer(
            &self,
            request: tonic::Request<super::GetSeedPeerRequest>,
        ) -> std::result::Result<tonic::Response<super::SeedPeer>, tonic::Status>;
        /// List acitve schedulers configuration.
        async fn list_seed_peers(
            &self,
            request: tonic::Request<super::ListSeedPeersRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSeedPeersResponse>,
            tonic::Status,
        >;
        /// Update SeedPeer configuration.
        async fn update_seed_peer(
            &self,
            request: tonic::Request<super::UpdateSeedPeerRequest>,
        ) -> std::result::Result<tonic::Response<super::SeedPeer>, tonic::Status>;
        /// Delete SeedPeer configuration.
        async fn delete_seed_peer(
            &self,
            request: tonic::Request<super::DeleteSeedPeerRequest>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
        /// Get Scheduler and Scheduler cluster configuration.
        async fn get_scheduler(
            &self,
            request: tonic::Request<super::GetSchedulerRequest>,
        ) -> std::result::Result<tonic::Response<super::Scheduler>, tonic::Status>;
        /// Update scheduler configuration.
        async fn update_scheduler(
            &self,
            request: tonic::Request<super::UpdateSchedulerRequest>,
        ) -> std::result::Result<tonic::Response<super::Scheduler>, tonic::Status>;
        /// List acitve schedulers configuration.
        async fn list_schedulers(
            &self,
            request: tonic::Request<super::ListSchedulersRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListSchedulersResponse>,
            tonic::Status,
        >;
        /// List applications configuration.
        async fn list_applications(
            &self,
            request: tonic::Request<super::ListApplicationsRequest>,
        ) -> std::result::Result<
            tonic::Response<super::ListApplicationsResponse>,
            tonic::Status,
        >;
        /// KeepAlive with manager.
        async fn keep_alive(
            &self,
            request: tonic::Request<tonic::Streaming<super::KeepAliveRequest>>,
        ) -> std::result::Result<tonic::Response<()>, tonic::Status>;
    }
    /// Manager RPC Service.
    #[derive(Debug)]
    pub struct ManagerServer<T> {
        inner: Arc<T>,
        accept_compression_encodings: EnabledCompressionEncodings,
        send_compression_encodings: EnabledCompressionEncodings,
        max_decoding_message_size: Option<usize>,
        max_encoding_message_size: Option<usize>,
    }
    impl<T> ManagerServer<T> {
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
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ManagerServer<T>
    where
        T: Manager,
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
                "/manager.v2.Manager/GetSeedPeer" => {
                    #[allow(non_camel_case_types)]
                    struct GetSeedPeerSvc<T: Manager>(pub Arc<T>);
                    impl<
                        T: Manager,
                    > tonic::server::UnaryService<super::GetSeedPeerRequest>
                    for GetSeedPeerSvc<T> {
                        type Response = super::SeedPeer;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetSeedPeerRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Manager>::get_seed_peer(&inner, request).await
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
                        let method = GetSeedPeerSvc(inner);
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
                "/manager.v2.Manager/ListSeedPeers" => {
                    #[allow(non_camel_case_types)]
                    struct ListSeedPeersSvc<T: Manager>(pub Arc<T>);
                    impl<
                        T: Manager,
                    > tonic::server::UnaryService<super::ListSeedPeersRequest>
                    for ListSeedPeersSvc<T> {
                        type Response = super::ListSeedPeersResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListSeedPeersRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Manager>::list_seed_peers(&inner, request).await
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
                        let method = ListSeedPeersSvc(inner);
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
                "/manager.v2.Manager/UpdateSeedPeer" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateSeedPeerSvc<T: Manager>(pub Arc<T>);
                    impl<
                        T: Manager,
                    > tonic::server::UnaryService<super::UpdateSeedPeerRequest>
                    for UpdateSeedPeerSvc<T> {
                        type Response = super::SeedPeer;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateSeedPeerRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Manager>::update_seed_peer(&inner, request).await
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
                        let method = UpdateSeedPeerSvc(inner);
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
                "/manager.v2.Manager/DeleteSeedPeer" => {
                    #[allow(non_camel_case_types)]
                    struct DeleteSeedPeerSvc<T: Manager>(pub Arc<T>);
                    impl<
                        T: Manager,
                    > tonic::server::UnaryService<super::DeleteSeedPeerRequest>
                    for DeleteSeedPeerSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::DeleteSeedPeerRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Manager>::delete_seed_peer(&inner, request).await
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
                        let method = DeleteSeedPeerSvc(inner);
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
                "/manager.v2.Manager/GetScheduler" => {
                    #[allow(non_camel_case_types)]
                    struct GetSchedulerSvc<T: Manager>(pub Arc<T>);
                    impl<
                        T: Manager,
                    > tonic::server::UnaryService<super::GetSchedulerRequest>
                    for GetSchedulerSvc<T> {
                        type Response = super::Scheduler;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetSchedulerRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Manager>::get_scheduler(&inner, request).await
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
                        let method = GetSchedulerSvc(inner);
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
                "/manager.v2.Manager/UpdateScheduler" => {
                    #[allow(non_camel_case_types)]
                    struct UpdateSchedulerSvc<T: Manager>(pub Arc<T>);
                    impl<
                        T: Manager,
                    > tonic::server::UnaryService<super::UpdateSchedulerRequest>
                    for UpdateSchedulerSvc<T> {
                        type Response = super::Scheduler;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::UpdateSchedulerRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Manager>::update_scheduler(&inner, request).await
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
                        let method = UpdateSchedulerSvc(inner);
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
                "/manager.v2.Manager/ListSchedulers" => {
                    #[allow(non_camel_case_types)]
                    struct ListSchedulersSvc<T: Manager>(pub Arc<T>);
                    impl<
                        T: Manager,
                    > tonic::server::UnaryService<super::ListSchedulersRequest>
                    for ListSchedulersSvc<T> {
                        type Response = super::ListSchedulersResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListSchedulersRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Manager>::list_schedulers(&inner, request).await
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
                        let method = ListSchedulersSvc(inner);
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
                "/manager.v2.Manager/ListApplications" => {
                    #[allow(non_camel_case_types)]
                    struct ListApplicationsSvc<T: Manager>(pub Arc<T>);
                    impl<
                        T: Manager,
                    > tonic::server::UnaryService<super::ListApplicationsRequest>
                    for ListApplicationsSvc<T> {
                        type Response = super::ListApplicationsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::ListApplicationsRequest>,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Manager>::list_applications(&inner, request).await
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
                        let method = ListApplicationsSvc(inner);
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
                "/manager.v2.Manager/KeepAlive" => {
                    #[allow(non_camel_case_types)]
                    struct KeepAliveSvc<T: Manager>(pub Arc<T>);
                    impl<
                        T: Manager,
                    > tonic::server::ClientStreamingService<super::KeepAliveRequest>
                    for KeepAliveSvc<T> {
                        type Response = ();
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<
                                tonic::Streaming<super::KeepAliveRequest>,
                            >,
                        ) -> Self::Future {
                            let inner = Arc::clone(&self.0);
                            let fut = async move {
                                <T as Manager>::keep_alive(&inner, request).await
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
                        let method = KeepAliveSvc(inner);
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
                        let res = grpc.client_streaming(method, req).await;
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
    impl<T> Clone for ManagerServer<T> {
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
    pub const SERVICE_NAME: &str = "manager.v2.Manager";
    impl<T> tonic::server::NamedService for ManagerServer<T> {
        const NAME: &'static str = SERVICE_NAME;
    }
}
