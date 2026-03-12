/// Peer metadata.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Peer {
    /// Peer id.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// Range is url range of request.
    #[prost(message, optional, tag = "2")]
    pub range: ::core::option::Option<Range>,
    /// Peer priority.
    #[prost(enumeration = "Priority", tag = "3")]
    pub priority: i32,
    /// Pieces of peer.
    #[prost(message, repeated, tag = "4")]
    pub pieces: ::prost::alloc::vec::Vec<Piece>,
    /// Peer downloads costs time.
    #[prost(message, optional, tag = "5")]
    pub cost: ::core::option::Option<::prost_wkt_types::Duration>,
    /// Peer state.
    #[prost(string, tag = "6")]
    pub state: ::prost::alloc::string::String,
    /// Task info.
    #[prost(message, optional, tag = "7")]
    pub task: ::core::option::Option<Task>,
    /// Host info.
    #[prost(message, optional, tag = "8")]
    pub host: ::core::option::Option<Host>,
    /// NeedBackToSource needs downloaded from source.
    #[prost(bool, tag = "9")]
    pub need_back_to_source: bool,
    /// Peer create time.
    #[prost(message, optional, tag = "10")]
    pub created_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// Peer update time.
    #[prost(message, optional, tag = "11")]
    pub updated_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// ConcurrentPieceCount is the number of pieces that can be downloaded concurrently.
    #[prost(uint32, optional, tag = "12")]
    pub concurrent_piece_count: ::core::option::Option<u32>,
}
/// CachePeer metadata.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CachePeer {
    /// Peer id.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// Range is url range of request.
    #[prost(message, optional, tag = "2")]
    pub range: ::core::option::Option<Range>,
    /// Peer priority.
    #[prost(enumeration = "Priority", tag = "3")]
    pub priority: i32,
    /// Pieces of peer.
    #[prost(message, repeated, tag = "4")]
    pub pieces: ::prost::alloc::vec::Vec<Piece>,
    /// Peer downloads costs time.
    #[prost(message, optional, tag = "5")]
    pub cost: ::core::option::Option<::prost_wkt_types::Duration>,
    /// Peer state.
    #[prost(string, tag = "6")]
    pub state: ::prost::alloc::string::String,
    /// Cache Task info.
    #[prost(message, optional, tag = "7")]
    pub task: ::core::option::Option<CacheTask>,
    /// Host info.
    #[prost(message, optional, tag = "8")]
    pub host: ::core::option::Option<Host>,
    /// NeedBackToSource needs downloaded from source.
    #[prost(bool, tag = "9")]
    pub need_back_to_source: bool,
    /// Peer create time.
    #[prost(message, optional, tag = "10")]
    pub created_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// Peer update time.
    #[prost(message, optional, tag = "11")]
    pub updated_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// ConcurrentPieceCount is the number of pieces that can be downloaded concurrently.
    #[prost(uint32, optional, tag = "12")]
    pub concurrent_piece_count: ::core::option::Option<u32>,
}
/// PersistentPeer metadata.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistentPeer {
    /// Peer id.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// Persistent represents whether the persistent peer is persistent.
    /// If the persistent peer is persistent, the persistent peer will
    /// not be deleted when dfdaemon runs garbage collection. It only be deleted
    /// when the task is deleted by the user.
    #[prost(bool, tag = "2")]
    pub persistent: bool,
    /// Peer downloads costs time.
    #[prost(message, optional, tag = "3")]
    pub cost: ::core::option::Option<::prost_wkt_types::Duration>,
    /// Peer state.
    #[prost(string, tag = "4")]
    pub state: ::prost::alloc::string::String,
    /// Persistent task info.
    #[prost(message, optional, tag = "5")]
    pub task: ::core::option::Option<PersistentTask>,
    /// Host info.
    #[prost(message, optional, tag = "6")]
    pub host: ::core::option::Option<Host>,
    /// Peer create time.
    #[prost(message, optional, tag = "7")]
    pub created_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// Peer update time.
    #[prost(message, optional, tag = "8")]
    pub updated_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// ConcurrentPieceCount is the number of pieces that can be downloaded concurrently.
    #[prost(uint32, tag = "9")]
    pub concurrent_piece_count: u32,
}
/// PersistentCachePeer metadata.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PersistentCachePeer {
    /// Peer id.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// Persistent represents whether the persistent cache peer is persistent.
    /// If the persistent cache peer is persistent, the persistent cache peer will
    /// not be deleted when dfdaemon runs garbage collection. It only be deleted
    /// when the task is deleted by the user.
    #[prost(bool, tag = "2")]
    pub persistent: bool,
    /// Peer downloads costs time.
    #[prost(message, optional, tag = "3")]
    pub cost: ::core::option::Option<::prost_wkt_types::Duration>,
    /// Peer state.
    #[prost(string, tag = "4")]
    pub state: ::prost::alloc::string::String,
    /// Persistent task info.
    #[prost(message, optional, tag = "5")]
    pub task: ::core::option::Option<PersistentCacheTask>,
    /// Host info.
    #[prost(message, optional, tag = "6")]
    pub host: ::core::option::Option<Host>,
    /// Peer create time.
    #[prost(message, optional, tag = "7")]
    pub created_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// Peer update time.
    #[prost(message, optional, tag = "8")]
    pub updated_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// ConcurrentPieceCount is the number of pieces that can be downloaded concurrently.
    #[prost(uint32, tag = "9")]
    pub concurrent_piece_count: u32,
}
/// Task metadata.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Task {
    /// Task id.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// Task type.
    #[prost(enumeration = "TaskType", tag = "2")]
    pub r#type: i32,
    /// Download url.
    #[prost(string, tag = "3")]
    pub url: ::prost::alloc::string::String,
    /// Verifies task data integrity after download using a digest. Supports CRC32, SHA256, and SHA512 algorithms.
    /// Format: `<algorithm>:<hash>`, e.g., `crc32:xxx`, `sha256:yyy`, `sha512:zzz`.
    /// Returns an error if the computed digest mismatches the expected value.
    ///
    /// Performance
    /// Digest calculation increases processing time. Enable only when data integrity verification is critical.
    #[prost(string, optional, tag = "4")]
    pub digest: ::core::option::Option<::prost::alloc::string::String>,
    /// URL tag identifies different task for same url.
    #[prost(string, optional, tag = "5")]
    pub tag: ::core::option::Option<::prost::alloc::string::String>,
    /// Application of task.
    #[prost(string, optional, tag = "6")]
    pub application: ::core::option::Option<::prost::alloc::string::String>,
    /// Filtered query params to generate the task id.
    /// When filter is \["Signature", "Expires", "ns"\], for example:
    /// <http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io> and <http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io>
    /// will generate the same task id.
    /// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
    #[prost(string, repeated, tag = "7")]
    pub filtered_query_params: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Task request headers.
    #[prost(map = "string, string", tag = "8")]
    pub request_header: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// Task content length.
    #[prost(uint64, tag = "9")]
    pub content_length: u64,
    /// Task piece count.
    #[prost(uint32, tag = "10")]
    pub piece_count: u32,
    /// Task size scope.
    #[prost(enumeration = "SizeScope", tag = "11")]
    pub size_scope: i32,
    /// Pieces of task.
    #[prost(message, repeated, tag = "12")]
    pub pieces: ::prost::alloc::vec::Vec<Piece>,
    /// Task state.
    #[prost(string, tag = "13")]
    pub state: ::prost::alloc::string::String,
    /// Task peer count.
    #[prost(uint32, tag = "14")]
    pub peer_count: u32,
    /// Task contains available peer.
    #[prost(bool, tag = "15")]
    pub has_available_peer: bool,
    /// Task create time.
    #[prost(message, optional, tag = "16")]
    pub created_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// Task update time.
    #[prost(message, optional, tag = "17")]
    pub updated_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
}
/// CacheTask metadata.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CacheTask {
    /// Task id.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// Task type.
    #[prost(enumeration = "TaskType", tag = "2")]
    pub r#type: i32,
    /// Download url.
    #[prost(string, tag = "3")]
    pub url: ::prost::alloc::string::String,
    /// Verifies task data integrity after download using a digest. Supports CRC32, SHA256, and SHA512 algorithms.
    /// Format: `<algorithm>:<hash>`, e.g., `crc32:xxx`, `sha256:yyy`, `sha512:zzz`.
    /// Returns an error if the computed digest mismatches the expected value.
    ///
    /// Performance
    /// Digest calculation increases processing time. Enable only when data integrity verification is critical.
    #[prost(string, optional, tag = "4")]
    pub digest: ::core::option::Option<::prost::alloc::string::String>,
    /// URL tag identifies different task for same url.
    #[prost(string, optional, tag = "5")]
    pub tag: ::core::option::Option<::prost::alloc::string::String>,
    /// Application of task.
    #[prost(string, optional, tag = "6")]
    pub application: ::core::option::Option<::prost::alloc::string::String>,
    /// Filtered query params to generate the task id.
    /// When filter is \["Signature", "Expires", "ns"\], for example:
    /// <http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io> and <http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io>
    /// will generate the same task id.
    /// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
    #[prost(string, repeated, tag = "7")]
    pub filtered_query_params: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    /// Task request headers.
    #[prost(map = "string, string", tag = "8")]
    pub request_header: ::std::collections::HashMap<
        ::prost::alloc::string::String,
        ::prost::alloc::string::String,
    >,
    /// Task content length.
    #[prost(uint64, tag = "9")]
    pub content_length: u64,
    /// Task piece count.
    #[prost(uint32, tag = "10")]
    pub piece_count: u32,
    /// Task size scope.
    #[prost(enumeration = "SizeScope", tag = "11")]
    pub size_scope: i32,
    /// Pieces of task.
    #[prost(message, repeated, tag = "12")]
    pub pieces: ::prost::alloc::vec::Vec<Piece>,
    /// Task state.
    #[prost(string, tag = "13")]
    pub state: ::prost::alloc::string::String,
    /// Task peer count.
    #[prost(uint32, tag = "14")]
    pub peer_count: u32,
    /// Task contains available peer.
    #[prost(bool, tag = "15")]
    pub has_available_peer: bool,
    /// Task create time.
    #[prost(message, optional, tag = "16")]
    pub created_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// Task update time.
    #[prost(message, optional, tag = "17")]
    pub updated_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
}
/// PersistentTask metadata.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct PersistentTask {
    /// Task id.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// Replica count of the persistent task. The persistent task will
    /// not be deleted when dfdamon runs garbage collection. It only be deleted
    /// when the task is deleted by the user.
    #[prost(uint64, tag = "2")]
    pub persistent_replica_count: u64,
    /// Current replica count of the persistent task. The persistent task
    /// will not be deleted when dfdaemon runs garbage collection. It only be deleted
    /// when the task is deleted by the user.
    #[prost(uint64, tag = "3")]
    pub current_persistent_replica_count: u64,
    /// Current replica count of the task. If task is not persistent,
    /// the persistent task will be deleted when dfdaemon runs garbage collection.
    #[prost(uint64, tag = "4")]
    pub current_replica_count: u64,
    /// Task content length.
    #[prost(uint64, tag = "5")]
    pub content_length: u64,
    /// Task piece count.
    #[prost(uint32, tag = "6")]
    pub piece_count: u32,
    /// Task state.
    #[prost(string, tag = "7")]
    pub state: ::prost::alloc::string::String,
    /// TTL of the persistent task.
    #[prost(message, optional, tag = "8")]
    pub ttl: ::core::option::Option<::prost_wkt_types::Duration>,
    /// Task create time.
    #[prost(message, optional, tag = "9")]
    pub created_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// Task update time.
    #[prost(message, optional, tag = "10")]
    pub updated_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
}
/// PersistentCacheTask metadata.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct PersistentCacheTask {
    /// Task id.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// Replica count of the persistent cache task. The persistent cache task will
    /// not be deleted when dfdamon runs garbage collection. It only be deleted
    /// when the task is deleted by the user.
    #[prost(uint64, tag = "2")]
    pub persistent_replica_count: u64,
    /// Current replica count of the persistent cache task. The persistent cache task
    /// will not be deleted when dfdaemon runs garbage collection. It only be deleted
    /// when the task is deleted by the user.
    #[prost(uint64, tag = "3")]
    pub current_persistent_replica_count: u64,
    /// Current replica count of the cache task. If cache task is not persistent,
    /// the persistent cache task will be deleted when dfdaemon runs garbage collection.
    #[prost(uint64, tag = "4")]
    pub current_replica_count: u64,
    /// Tag is used to distinguish different persistent cache tasks.
    #[prost(string, optional, tag = "5")]
    pub tag: ::core::option::Option<::prost::alloc::string::String>,
    /// Application of task.
    #[prost(string, optional, tag = "6")]
    pub application: ::core::option::Option<::prost::alloc::string::String>,
    /// Task piece length.
    #[prost(uint64, tag = "7")]
    pub piece_length: u64,
    /// Task content length.
    #[prost(uint64, tag = "8")]
    pub content_length: u64,
    /// Task piece count.
    #[prost(uint32, tag = "9")]
    pub piece_count: u32,
    /// Task state.
    #[prost(string, tag = "10")]
    pub state: ::prost::alloc::string::String,
    /// TTL of the persistent cache task.
    #[prost(message, optional, tag = "11")]
    pub ttl: ::core::option::Option<::prost_wkt_types::Duration>,
    /// Task create time.
    #[prost(message, optional, tag = "12")]
    pub created_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
    /// Task update time.
    #[prost(message, optional, tag = "13")]
    pub updated_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
}
/// Host metadata.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Host {
    /// Host id.
    #[prost(string, tag = "1")]
    pub id: ::prost::alloc::string::String,
    /// Host type.
    #[prost(uint32, tag = "2")]
    pub r#type: u32,
    /// Hostname.
    #[prost(string, tag = "3")]
    pub hostname: ::prost::alloc::string::String,
    /// Host ip.
    #[prost(string, tag = "4")]
    pub ip: ::prost::alloc::string::String,
    /// Port of grpc service.
    #[prost(int32, tag = "5")]
    pub port: i32,
    /// Port of download server.
    #[prost(int32, tag = "6")]
    pub download_port: i32,
    /// Host OS.
    #[prost(string, tag = "7")]
    pub os: ::prost::alloc::string::String,
    /// Host platform.
    #[prost(string, tag = "8")]
    pub platform: ::prost::alloc::string::String,
    /// Host platform family.
    #[prost(string, tag = "9")]
    pub platform_family: ::prost::alloc::string::String,
    /// Host platform version.
    #[prost(string, tag = "10")]
    pub platform_version: ::prost::alloc::string::String,
    /// Host kernel version.
    #[prost(string, tag = "11")]
    pub kernel_version: ::prost::alloc::string::String,
    /// CPU Stat.
    #[prost(message, optional, tag = "12")]
    pub cpu: ::core::option::Option<Cpu>,
    /// Memory Stat.
    #[prost(message, optional, tag = "13")]
    pub memory: ::core::option::Option<Memory>,
    /// Network Stat.
    #[prost(message, optional, tag = "14")]
    pub network: ::core::option::Option<Network>,
    /// Disk Stat.
    #[prost(message, optional, tag = "15")]
    pub disk: ::core::option::Option<Disk>,
    /// Build information.
    #[prost(message, optional, tag = "16")]
    pub build: ::core::option::Option<Build>,
    /// ID of the cluster to which the host belongs.
    #[prost(uint64, tag = "17")]
    pub scheduler_cluster_id: u64,
    /// Disable shared data for other peers.
    #[prost(bool, tag = "18")]
    pub disable_shared: bool,
    /// Port of proxy server.
    #[prost(int32, tag = "19")]
    pub proxy_port: i32,
    /// Name of the host.
    /// Format: ${POD_NAMESPACE}-${POD_NAME}
    #[prost(string, tag = "20")]
    pub name: ::prost::alloc::string::String,
}
/// CPU Stat.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Cpu {
    /// Number of logical cores in the system.
    #[prost(uint32, tag = "1")]
    pub logical_count: u32,
    /// Number of physical cores in the system
    #[prost(uint32, tag = "2")]
    pub physical_count: u32,
    /// Percent calculates the percentage of cpu used.
    #[prost(double, tag = "3")]
    pub percent: f64,
    /// Calculates the percentage of cpu used by process.
    #[prost(double, tag = "4")]
    pub process_percent: f64,
    /// CPUTimes contains the amounts of time the CPU has spent performing different kinds of work.
    #[prost(message, optional, tag = "5")]
    pub times: ::core::option::Option<CpuTimes>,
    /// Cgroup CPU Stat.
    #[prost(message, optional, tag = "6")]
    pub cgroup: ::core::option::Option<CgroupCpu>,
}
/// Cgroup CPU Stat.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct CgroupCpu {
    /// CFS period in microseconds.
    #[prost(uint64, tag = "1")]
    pub period: u64,
    /// CFS quota in microseconds.
    #[prost(int64, tag = "2")]
    pub quota: i64,
    /// Calculates the percentage of cpu used by cgroup.
    #[prost(double, tag = "3")]
    pub used_percent: f64,
}
/// CPUTimes contains the amounts of time the CPU has spent performing different
/// kinds of work. Time units are in seconds.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct CpuTimes {
    /// CPU time of user.
    #[prost(double, tag = "1")]
    pub user: f64,
    /// CPU time of system.
    #[prost(double, tag = "2")]
    pub system: f64,
    /// CPU time of idle.
    #[prost(double, tag = "3")]
    pub idle: f64,
    /// CPU time of nice.
    #[prost(double, tag = "4")]
    pub nice: f64,
    /// CPU time of iowait.
    #[prost(double, tag = "5")]
    pub iowait: f64,
    /// CPU time of irq.
    #[prost(double, tag = "6")]
    pub irq: f64,
    /// CPU time of softirq.
    #[prost(double, tag = "7")]
    pub softirq: f64,
    /// CPU time of steal.
    #[prost(double, tag = "8")]
    pub steal: f64,
    /// CPU time of guest.
    #[prost(double, tag = "9")]
    pub guest: f64,
    /// CPU time of guest nice.
    #[prost(double, tag = "10")]
    pub guest_nice: f64,
}
/// Memory Stat.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Memory {
    /// Total amount of RAM on this system.
    #[prost(uint64, tag = "1")]
    pub total: u64,
    /// RAM available for programs to allocate.
    #[prost(uint64, tag = "2")]
    pub available: u64,
    /// RAM used by programs.
    #[prost(uint64, tag = "3")]
    pub used: u64,
    /// Percentage of RAM used by programs.
    #[prost(double, tag = "4")]
    pub used_percent: f64,
    /// Calculates the percentage of memory used by process.
    #[prost(double, tag = "5")]
    pub process_used_percent: f64,
    /// This is the kernel's notion of free memory.
    #[prost(uint64, tag = "6")]
    pub free: u64,
    /// Cgroup Memory Stat.
    #[prost(message, optional, tag = "7")]
    pub cgroup: ::core::option::Option<CgroupMemory>,
}
/// Cgroup Memory Stat.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct CgroupMemory {
    /// Limit is the memory limit in bytes.
    #[prost(int64, tag = "1")]
    pub limit: i64,
    /// Usage is the current memory usage in bytes.
    #[prost(uint64, tag = "2")]
    pub usage: u64,
    /// Calculates the percentage of memory used by cgroup.
    #[prost(double, tag = "3")]
    pub used_percent: f64,
}
/// Network Stat.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Network {
    /// Return count of tcp connections opened and status is ESTABLISHED.
    #[prost(uint32, tag = "1")]
    pub tcp_connection_count: u32,
    /// Return count of upload tcp connections opened and status is ESTABLISHED.
    #[prost(uint32, tag = "2")]
    pub upload_tcp_connection_count: u32,
    /// Location path(area|country|province|city|...).
    #[prost(string, optional, tag = "3")]
    pub location: ::core::option::Option<::prost::alloc::string::String>,
    /// IDC where the peer host is located
    #[prost(string, optional, tag = "4")]
    pub idc: ::core::option::Option<::prost::alloc::string::String>,
    /// Maximum bandwidth of the network interface, in bps (bits per second).
    #[prost(uint64, tag = "9")]
    pub max_rx_bandwidth: u64,
    /// Receive bandwidth of the network interface, in bps (bits per second).
    #[prost(uint64, optional, tag = "10")]
    pub rx_bandwidth: ::core::option::Option<u64>,
    /// Maximum bandwidth of the network interface for transmission, in bps (bits per second).
    #[prost(uint64, tag = "11")]
    pub max_tx_bandwidth: u64,
    /// Transmit bandwidth of the network interface, in bps (bits per second).
    #[prost(uint64, optional, tag = "12")]
    pub tx_bandwidth: ::core::option::Option<u64>,
}
/// Disk Stat.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, ::prost::Message)]
pub struct Disk {
    /// Total amount of disk on the data path of dragonfly.
    #[prost(uint64, tag = "1")]
    pub total: u64,
    /// Free amount of disk on the data path of dragonfly.
    #[prost(uint64, tag = "2")]
    pub free: u64,
    /// Used amount of disk on the data path of dragonfly.
    #[prost(uint64, tag = "3")]
    pub used: u64,
    /// Used percent of disk on the data path of dragonfly directory.
    #[prost(double, tag = "4")]
    pub used_percent: f64,
    /// Total amount of indoes on the data path of dragonfly directory.
    #[prost(uint64, tag = "5")]
    pub inodes_total: u64,
    /// Used amount of indoes on the data path of dragonfly directory.
    #[prost(uint64, tag = "6")]
    pub inodes_used: u64,
    /// Free amount of indoes on the data path of dragonfly directory.
    #[prost(uint64, tag = "7")]
    pub inodes_free: u64,
    /// Used percent of indoes on the data path of dragonfly directory.
    #[prost(double, tag = "8")]
    pub inodes_used_percent: f64,
    /// Disk read bandwidth, in bytes per second.
    #[prost(uint64, tag = "9")]
    pub read_bandwidth: u64,
    /// Disk write bandwidth, in bytes per second.
    #[prost(uint64, tag = "10")]
    pub write_bandwidth: u64,
}
/// Build information.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Build {
    /// Git version.
    #[prost(string, tag = "1")]
    pub git_version: ::prost::alloc::string::String,
    /// Git commit.
    #[prost(string, optional, tag = "2")]
    pub git_commit: ::core::option::Option<::prost::alloc::string::String>,
    /// Golang version.
    #[prost(string, optional, tag = "3")]
    pub go_version: ::core::option::Option<::prost::alloc::string::String>,
    /// Rust version.
    #[prost(string, optional, tag = "4")]
    pub rust_version: ::core::option::Option<::prost::alloc::string::String>,
    /// Build platform.
    #[prost(string, optional, tag = "5")]
    pub platform: ::core::option::Option<::prost::alloc::string::String>,
}
/// Download information.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Download {
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
    pub range: ::core::option::Option<Range>,
    /// Task type.
    #[prost(enumeration = "TaskType", tag = "4")]
    pub r#type: i32,
    /// URL tag identifies different task for same url.
    #[prost(string, optional, tag = "5")]
    pub tag: ::core::option::Option<::prost::alloc::string::String>,
    /// Application of task.
    #[prost(string, optional, tag = "6")]
    pub application: ::core::option::Option<::prost::alloc::string::String>,
    /// Peer priority.
    #[prost(enumeration = "Priority", tag = "7")]
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
    pub object_storage: ::core::option::Option<ObjectStorage>,
    /// HDFS protocol information.
    #[prost(message, optional, tag = "18")]
    pub hdfs: ::core::option::Option<Hdfs>,
    /// is_prefetch is the flag to indicate whether the request is a prefetch request.
    #[prost(bool, tag = "19")]
    pub is_prefetch: bool,
    /// need_piece_content is the flag to indicate whether the response needs to return piece content.
    #[prost(bool, tag = "20")]
    pub need_piece_content: bool,
    /// force_hard_link is the flag to indicate whether the download file must be hard linked to the output path.
    /// For more details refer to <https://github.com/dragonflyoss/design/blob/main/systems-analysis/file-download-workflow-with-hard-link/README.md.>
    #[prost(bool, tag = "22")]
    pub force_hard_link: bool,
    /// content_for_calculating_task_id is the content used to calculate the task id.
    /// If content_for_calculating_task_id is set, use its value to calculate the task ID.
    /// Otherwise, calculate the task ID based on url, piece_length, tag, application, and filtered_query_params.
    #[prost(string, optional, tag = "23")]
    pub content_for_calculating_task_id: ::core::option::Option<
        ::prost::alloc::string::String,
    >,
    /// remote_ip represents the IP address of the client initiating the download request.
    /// For proxy requests, it is set to the IP address of the request source.
    /// For dfget requests, it is set to the IP address of the dfget.
    #[prost(string, optional, tag = "24")]
    pub remote_ip: ::core::option::Option<::prost::alloc::string::String>,
    /// ConcurrentPieceCount is the number of pieces that can be downloaded concurrently.
    #[prost(uint32, optional, tag = "25")]
    pub concurrent_piece_count: ::core::option::Option<u32>,
    /// Overwrite indicates whether to overwrite the existing file at output path.
    #[prost(bool, tag = "26")]
    pub overwrite: bool,
    /// Actual piece length by calculating based on the piece_length field and content length.
    #[prost(uint64, optional, tag = "27")]
    pub actual_piece_length: ::core::option::Option<u64>,
    /// Actual content length by getting from the backend.
    #[prost(uint64, optional, tag = "28")]
    pub actual_content_length: ::core::option::Option<u64>,
    /// Actual piece count by calculating.
    #[prost(uint64, optional, tag = "29")]
    pub actual_piece_count: ::core::option::Option<u64>,
    /// enable_task_id_based_blob_digest indicates whether to use the blob digest for task ID calculation
    /// when downloading from OCI registries. When enabled for OCI blob URLs (e.g., /v2/<name>/blobs/sha256:<digest>),
    /// the task ID is derived from the blob digest rather than the full URL. This enables deduplication across
    /// registries - the same blob from different registries shares one task ID, eliminating redundant downloads
    /// and storage.
    #[prost(bool, tag = "30")]
    pub enable_task_id_based_blob_digest: bool,
    /// HuggingFace protocol information.
    #[prost(message, optional, tag = "31")]
    pub hugging_face: ::core::option::Option<HuggingFace>,
    /// ModelScope protocol information.
    #[prost(message, optional, tag = "32")]
    pub model_scope: ::core::option::Option<ModelScope>,
}
/// Object Storage related information.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct ObjectStorage {
    /// Region is the region of the object storage service.
    #[prost(string, optional, tag = "1")]
    pub region: ::core::option::Option<::prost::alloc::string::String>,
    /// Endpoint is the endpoint of the object storage service.
    #[prost(string, optional, tag = "2")]
    pub endpoint: ::core::option::Option<::prost::alloc::string::String>,
    /// Access key that used to access the object storage service.
    #[prost(string, optional, tag = "3")]
    pub access_key_id: ::core::option::Option<::prost::alloc::string::String>,
    /// Access secret that used to access the object storage service.
    #[prost(string, optional, tag = "4")]
    pub access_key_secret: ::core::option::Option<::prost::alloc::string::String>,
    /// Session token that used to access s3 storage service.
    #[prost(string, optional, tag = "5")]
    pub session_token: ::core::option::Option<::prost::alloc::string::String>,
    /// Local path to credential file for Google Cloud Storage service OAuth2 authentication.
    #[prost(string, optional, tag = "6")]
    pub credential_path: ::core::option::Option<::prost::alloc::string::String>,
    /// Predefined ACL that used for the Google Cloud Storage service.
    #[prost(string, optional, tag = "7")]
    pub predefined_acl: ::core::option::Option<::prost::alloc::string::String>,
    /// Temporary STS security token for accessing OSS.
    #[prost(string, optional, tag = "8")]
    pub security_token: ::core::option::Option<::prost::alloc::string::String>,
    /// Insecure skip verify indicates whether to skip verifying the server's certificate chain.
    #[prost(bool, optional, tag = "9")]
    pub insecure_skip_verify: ::core::option::Option<bool>,
}
/// HDFS related information.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Hdfs {
    /// Delegation token for Web HDFS operator.
    #[prost(string, optional, tag = "1")]
    pub delegation_token: ::core::option::Option<::prost::alloc::string::String>,
}
/// HuggingFace related information.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct HuggingFace {
    /// Access token for HuggingFace Hub.
    #[prost(string, optional, tag = "1")]
    pub token: ::core::option::Option<::prost::alloc::string::String>,
    /// Revision of the HuggingFace model, dataset, or space. It can be a branch name, tag name, or commit hash. If not specified,
    /// it defaults to the repository's default main branch.
    #[prost(string, tag = "2")]
    pub revision: ::prost::alloc::string::String,
}
/// ModelScope related information.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct ModelScope {
    /// Access token for ModelScope Hub.
    #[prost(string, optional, tag = "1")]
    pub token: ::core::option::Option<::prost::alloc::string::String>,
    /// Revision of the ModelScope model, dataset, or space. It can be a branch name, tag name, or commit hash. If not specified,
    /// it defaults to the repository's default main branch.
    #[prost(string, tag = "2")]
    pub revision: ::prost::alloc::string::String,
}
/// Range represents download range.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Range {
    /// Start of range.
    #[prost(uint64, tag = "1")]
    pub start: u64,
    /// Length of range.
    #[prost(uint64, tag = "2")]
    pub length: u64,
}
/// Piece represents information of piece.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, PartialEq, Eq, Hash, ::prost::Message)]
pub struct Piece {
    /// Piece number.
    #[prost(uint32, tag = "1")]
    pub number: u32,
    /// Parent peer id.
    #[prost(string, optional, tag = "2")]
    pub parent_id: ::core::option::Option<::prost::alloc::string::String>,
    /// Piece offset.
    #[prost(uint64, tag = "3")]
    pub offset: u64,
    /// Piece length.
    #[prost(uint64, tag = "4")]
    pub length: u64,
    /// Digest of the piece data, for example blake3:xxx or sha256:yyy.
    #[prost(string, tag = "5")]
    pub digest: ::prost::alloc::string::String,
    /// Piece content.
    #[prost(bytes = "vec", optional, tag = "6")]
    pub content: ::core::option::Option<::prost::alloc::vec::Vec<u8>>,
    /// Traffic type.
    #[prost(enumeration = "TrafficType", optional, tag = "7")]
    pub traffic_type: ::core::option::Option<i32>,
    /// Downloading piece costs time.
    #[prost(message, optional, tag = "8")]
    pub cost: ::core::option::Option<::prost_wkt_types::Duration>,
    /// Piece create time.
    #[prost(message, optional, tag = "9")]
    pub created_at: ::core::option::Option<::prost_wkt_types::Timestamp>,
}
/// SizeScope represents size scope of task.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum SizeScope {
    /// size > one piece size.
    Normal = 0,
    /// 128 byte \< size \<= one piece size and be plain type.
    Small = 1,
    /// size \<= 128 byte and be plain type.
    Tiny = 2,
    /// size == 0 byte and be plain type.
    Empty = 3,
}
impl SizeScope {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Normal => "NORMAL",
            Self::Small => "SMALL",
            Self::Tiny => "TINY",
            Self::Empty => "EMPTY",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "NORMAL" => Some(Self::Normal),
            "SMALL" => Some(Self::Small),
            "TINY" => Some(Self::Tiny),
            "EMPTY" => Some(Self::Empty),
            _ => None,
        }
    }
}
/// TaskType represents type of task.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TaskType {
    /// STANDARD is standard type of task, it can download from source, remote peer and
    /// local peer(local cache). When the standard task is never downloaded in the
    /// P2P cluster, dfdaemon will download the task from the source. When the standard
    /// task is downloaded in the P2P cluster, dfdaemon will download the task from
    /// the remote peer or local peer(local cache), where peers use disk storage to store tasks.
    Standard = 0,
    /// PERSISTENT is persistent type of task, it can import file and export file in P2P cluster.
    /// When the persistent task is imported into the P2P cluster, dfdaemon will store
    /// the task in the peer's disk and copy multiple replicas to remote peers to
    /// prevent data loss.
    Persistent = 1,
    /// PERSISTENT_CACHE is persistent cache type of task, it can import file and export file in P2P cluster.
    /// When the persistent cache task is imported into the P2P cluster, dfdaemon will store
    /// the task in the peer's disk and copy multiple replicas to remote peers to prevent data loss.
    /// When the expiration time is reached, task will be deleted in the P2P cluster.
    PersistentCache = 2,
    /// CACHE is cache type of task, it can download from source, remote peer and
    /// local peer(local cache). When the cache task is never downloaded in the
    /// P2P cluster, dfdaemon will download the cache task from the source. When the cache
    /// task is downloaded in the P2P cluster, dfdaemon will download the cache task from
    /// the remote peer or local peer(local cache), where peers use memory storage to store cache tasks.
    Cache = 3,
}
impl TaskType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Standard => "STANDARD",
            Self::Persistent => "PERSISTENT",
            Self::PersistentCache => "PERSISTENT_CACHE",
            Self::Cache => "CACHE",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "STANDARD" => Some(Self::Standard),
            "PERSISTENT" => Some(Self::Persistent),
            "PERSISTENT_CACHE" => Some(Self::PersistentCache),
            "CACHE" => Some(Self::Cache),
            _ => None,
        }
    }
}
/// TrafficType represents type of traffic.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum TrafficType {
    /// BACK_TO_SOURCE is to download traffic from the source.
    BackToSource = 0,
    /// REMOTE_PEER is to download traffic from the remote peer.
    RemotePeer = 1,
    /// LOCAL_PEER is to download traffic from the local peer.
    LocalPeer = 2,
}
impl TrafficType {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::BackToSource => "BACK_TO_SOURCE",
            Self::RemotePeer => "REMOTE_PEER",
            Self::LocalPeer => "LOCAL_PEER",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "BACK_TO_SOURCE" => Some(Self::BackToSource),
            "REMOTE_PEER" => Some(Self::RemotePeer),
            "LOCAL_PEER" => Some(Self::LocalPeer),
            _ => None,
        }
    }
}
/// Priority represents priority of application.
#[derive(serde::Serialize, serde::Deserialize)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum Priority {
    /// LEVEL0 has no special meaning for scheduler.
    Level0 = 0,
    /// LEVEL1 represents the download task is forbidden,
    /// and an error code is returned during the registration.
    Level1 = 1,
    /// LEVEL2 represents when the task is downloaded for the first time,
    /// allow peers to download from the other peers,
    /// but not back-to-source. When the task is not downloaded for
    /// the first time, it is scheduled normally.
    Level2 = 2,
    /// LEVEL3 represents when the task is downloaded for the first time,
    /// the normal peer is first to download back-to-source.
    /// When the task is not downloaded for the first time, it is scheduled normally.
    Level3 = 3,
    /// LEVEL4 represents when the task is downloaded for the first time,
    /// the super peer is first triggered to back-to-source.
    /// When the task is not downloaded for the first time, it is scheduled normally.
    Level4 = 4,
    /// LEVEL5 represents when the task is downloaded for the first time,
    /// the super peer is first triggered to back-to-source.
    /// When the task is not downloaded for the first time, it is scheduled normally.
    Level5 = 5,
    /// LEVEL6 represents when the task is downloaded for the first time,
    /// the super peer is first triggered to back-to-source.
    /// When the task is not downloaded for the first time, it is scheduled normally.
    Level6 = 6,
}
impl Priority {
    /// String value of the enum field names used in the ProtoBuf definition.
    ///
    /// The values are not transformed in any way and thus are considered stable
    /// (if the ProtoBuf definition does not change) and safe for programmatic use.
    pub fn as_str_name(&self) -> &'static str {
        match self {
            Self::Level0 => "LEVEL0",
            Self::Level1 => "LEVEL1",
            Self::Level2 => "LEVEL2",
            Self::Level3 => "LEVEL3",
            Self::Level4 => "LEVEL4",
            Self::Level5 => "LEVEL5",
            Self::Level6 => "LEVEL6",
        }
    }
    /// Creates an enum from field names used in the ProtoBuf definition.
    pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
        match value {
            "LEVEL0" => Some(Self::Level0),
            "LEVEL1" => Some(Self::Level1),
            "LEVEL2" => Some(Self::Level2),
            "LEVEL3" => Some(Self::Level3),
            "LEVEL4" => Some(Self::Level4),
            "LEVEL5" => Some(Self::Level5),
            "LEVEL6" => Some(Self::Level6),
            _ => None,
        }
    }
}
