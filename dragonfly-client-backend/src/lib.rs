/*
 *     Copyright 2024 The Dragonfly Authors
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

use async_trait::async_trait;
use bytes::Bytes;
use dragonfly_api::common::v2::{Hdfs, HuggingFace, ModelScope, ObjectStorage, Range};
use dragonfly_client_config::dfdaemon::Config;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use futures::stream::BoxStream;
use libloading::Library;
use reqwest::header::HeaderMap;
use rustls_pki_types::CertificateDer;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::{collections::HashMap, pin::Pin, time::Duration};
use std::{fmt::Debug, fs};
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio_util::io::StreamReader;
use tracing::{error, info, warn};
use url::Url;

pub mod hdfs;
pub mod http;
pub mod hugging_face;
pub mod model_scope;
pub mod object_storage;
pub mod oci;

/// The max idle connections per host.
const POOL_MAX_IDLE_PER_HOST: usize = 1024;

/// The keep alive interval for TCP connection.
const KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(60);

/// The interval for HTTP2 keep alive.
const HTTP2_KEEP_ALIVE_INTERVAL: Duration = Duration::from_secs(300);

/// The timeout for HTTP2 keep alive.
const HTTP2_KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(20);

/// The stream window size for HTTP2 connection.
const HTTP2_STREAM_WINDOW_SIZE: u32 = 16 * 1024 * 1024;

/// The connection window size for HTTP2 connection.
const HTTP2_CONNECTION_WINDOW_SIZE: u32 = 16 * 1024 * 1024;

/// The default user agent.
const DEFAULT_USER_AGENT: &str = concat!("dragonfly", "/", env!("CARGO_PKG_VERSION"));

/// The name of the package.
pub const NAME: &str = "backend";

/// The body of the response. It implements AsyncRead for sequential readers,
/// and `into_inner` exposes the underlying stream of bytes chunks so writers
/// can consume the chunks without copying.
pub type Body = StreamReader<BoxStream<'static, std::io::Result<Bytes>>, Bytes>;

/// empty_body returns an empty body of the response.
pub fn empty_body() -> Body {
    StreamReader::new(Box::pin(futures::stream::empty()))
}

/// The stat request for backend.
pub struct StatRequest {
    /// The id of the task.
    pub task_id: String,

    /// The url of the request.
    pub url: String,

    /// The headers of the request.
    pub http_header: Option<HeaderMap>,

    /// The timeout of the request.
    pub timeout: Duration,

    /// The client certificates for the request.
    pub client_cert: Option<Vec<CertificateDer<'static>>>,

    /// The object storage related information.
    pub object_storage: Option<ObjectStorage>,

    /// The hdfs related information.
    pub hdfs: Option<Hdfs>,

    /// Hugging Face is the hugging face related information.
    pub hugging_face: Option<HuggingFace>,

    /// Model Scope is the model scope related information.
    pub model_scope: Option<ModelScope>,
}

/// The stat response for backend.
#[derive(Debug)]
pub struct StatResponse {
    /// The success of the response.
    pub success: bool,

    /// The content length of the response.
    pub content_length: Option<u64>,

    /// The headers of the response.
    pub http_header: Option<HeaderMap>,

    /// The status code of the response.
    pub http_status_code: Option<reqwest::StatusCode>,

    /// The information of the entries in the directory.
    pub entries: Vec<DirEntry>,

    /// The error message of the response.
    pub error_message: Option<String>,
}

/// The get request for backend.
#[derive(Debug, Clone)]
pub struct GetRequest {
    /// The id of the task.
    pub task_id: String,

    /// The id of the piece.
    pub piece_id: String,

    /// The url of the request.
    pub url: String,

    /// The range of the request.
    pub range: Option<Range>,

    /// The headers of the request.
    pub http_header: Option<HeaderMap>,

    /// The timeout of the request.
    pub timeout: Duration,

    /// The client certificates for the request.
    pub client_cert: Option<Vec<CertificateDer<'static>>>,

    /// Object storage related information.
    pub object_storage: Option<ObjectStorage>,

    /// The hdfs related information.
    pub hdfs: Option<Hdfs>,

    /// Hugging Face is the hugging face related information.
    pub hugging_face: Option<HuggingFace>,

    /// Model Scope is the model scope related information.
    pub model_scope: Option<ModelScope>,
}

/// The get response for backend.
pub struct GetResponse<R>
where
    R: AsyncRead + Unpin,
{
    /// The success of the response.
    pub success: bool,

    /// The headers of the response.
    pub http_header: Option<HeaderMap>,

    /// The status code of the response.
    pub http_status_code: Option<reqwest::StatusCode>,

    /// The content of the response.
    pub reader: R,

    /// The error message of the response.
    pub error_message: Option<String>,
}

/// Implements the response functions.
impl<R> GetResponse<R>
where
    R: AsyncRead + Unpin,
{
    pub async fn text(&mut self) -> Result<String> {
        let mut buffer = String::new();
        Pin::new(&mut self.reader)
            .read_to_string(&mut buffer)
            .await?;
        Ok(buffer)
    }
}

/// The File Entry of a directory, including some relevant file metadata.
#[derive(Debug, PartialEq, Eq, Hash, Clone)]
pub struct DirEntry {
    /// The url of the entry.
    pub url: String,

    /// The content length of the entry.
    pub content_length: usize,

    /// Indicates whether the entry is a directory.
    pub is_dir: bool,
}

/// The exists request for backend.
pub struct ExistsRequest {
    /// The id of the task.
    pub task_id: String,

    /// The url of the request.
    pub url: String,

    /// The headers of the request.
    pub http_header: Option<HeaderMap>,

    /// The timeout of the request.
    pub timeout: Duration,

    /// The client certificates for the request.
    pub client_cert: Option<Vec<CertificateDer<'static>>>,

    /// The object storage related information.
    pub object_storage: Option<ObjectStorage>,

    /// The hdfs related information.
    pub hdfs: Option<Hdfs>,

    /// Hugging Face is the hugging face related information.
    pub hugging_face: Option<HuggingFace>,

    /// Model Scope is the model scope related information.
    pub model_scope: Option<ModelScope>,
}

/// The put request for backend.
pub struct PutRequest {
    /// The id of the task.
    pub task_id: String,

    /// The url of the request.
    pub url: String,

    /// The local file path of the request.
    pub path: PathBuf,

    /// The headers of the request.
    pub http_header: Option<HeaderMap>,

    /// The timeout of the request.
    pub timeout: Duration,

    /// The client certificates for the request.
    pub client_cert: Option<Vec<CertificateDer<'static>>>,

    /// The object storage related information.
    pub object_storage: Option<ObjectStorage>,

    /// The hdfs related information.
    pub hdfs: Option<Hdfs>,

    /// Hugging Face is the hugging face related information.
    pub hugging_face: Option<HuggingFace>,

    /// Model Scope is the model scope related information.
    pub model_scope: Option<ModelScope>,
}

/// The put response for backend.
#[derive(Debug)]
pub struct PutResponse {
    /// The success of the response.
    pub success: bool,

    /// The content length of the response.
    pub content_length: Option<u64>,

    /// The headers of the response.
    pub http_header: Option<HeaderMap>,

    /// The status code of the response.
    pub http_status_code: Option<reqwest::StatusCode>,

    /// The error message of the response.
    pub error_message: Option<String>,
}

/// The interface of the backend.
#[async_trait]
pub trait Backend {
    /// Returns the scheme of the backend.
    fn scheme(&self) -> String;

    /// Stat gets the metadata from the backend.
    async fn stat(&self, request: StatRequest) -> Result<StatResponse>;

    /// Get gets the content from the backend.
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>>;

    /// Put puts the content to the backend.
    async fn put(&self, request: PutRequest) -> Result<PutResponse>;

    /// Exists checks whether the file exists in the backend.
    async fn exists(&self, request: ExistsRequest) -> Result<bool>;
}

/// The factory of the backend.
#[derive(Default)]
pub struct BackendFactory {
    /// The configuration of the dfdaemon.
    config: Arc<Config>,

    /// The backends of the factory, including the plugin backends and
    /// the builtin backends.
    backends: HashMap<String, Box<dyn Backend + Send + Sync>>,

    /// Used to store the plugin's dynamic library, because when not saving the `Library`,
    /// it will drop when out of scope, resulting in the null pointer error.
    libraries: Vec<Library>,
}

/// Implements the factory of the backend. It supports loading builtin
/// backends and plugin backends.
///
/// The builtin backends are http, https, etc., which are implemented
/// by the HTTP struct.
///
/// The plugin backends are shared libraries, which are loaded
/// by the `register_plugin` function. The file name of the shared
/// library is the scheme of the backend. The shared library
/// should implement the Backend trait. Default plugin directory
/// is `/var/lib/dragonfly/plugins/` in linux and `~/.dragonfly/plugins`
/// in macos. The plugin directory can be set by the dfdaemon configuration.
///
/// For example:
/// If implement a plugin backend named `hdfs`, the shared library
/// should be named `libhdfs.so` or `libhdfs.dylib` and move the file to the backend plugin directory
/// `/var/lib/dragonfly/plugins/backend/` in linux or `~/.dragonfly/plugins/backend/`
/// in macos. When the dfdaemon starts, it will load the `hdfs` plugin backend in the
/// backend plugin directory. So the dfdaemon or dfget can use the `hdfs` plugin backend
/// to download the file by the url `hdfs://example.com/file`.
/// The backend plugin implementation can refer to
/// https://github.com/dragonflyoss/client/tree/main/dragonfly-client-backend/examples/plugin/.
impl BackendFactory {
    /// Returns a new BackendFactory.
    pub fn new(config: Arc<Config>, plugin_dir: Option<&Path>) -> Result<Self> {
        let mut backend_factory = Self {
            config: config.clone(),
            backends: HashMap::new(),
            libraries: Vec::new(),
        };
        backend_factory.load_builtin_backends(
            config.backend.enable_cache_temporary_redirect,
            config.backend.cache_temporary_redirect_ttl,
        )?;
        if let Some(plugin_dir) = plugin_dir {
            backend_factory
                .load_plugin_backends(plugin_dir)
                .inspect_err(|err| {
                    error!("failed to load plugin backends: {}", err);
                })?;
        }

        Ok(backend_factory)
    }

    /// Returns whether the scheme does not support directory
    /// download.
    pub fn unsupported_download_directory(scheme: &str) -> bool {
        scheme == http::HTTP_SCHEME || scheme == http::HTTPS_SCHEME
    }

    /// Returns the backend by the scheme of the url.
    pub fn build(&self, url: &str) -> Result<&(dyn Backend + Send + Sync)> {
        let url = Url::parse(url).or_err(ErrorType::ParseError)?;
        let scheme = url.scheme();
        self.backends
            .get(scheme)
            .map(|boxed_backend| &**boxed_backend)
            .ok_or(Error::InvalidParameter)
            .inspect_err(|_err| {
                error!("unsupported backend scheme: {}", scheme);
            })
    }

    /// Load backends loads the backends by the configuration of the dfdaemon. It includes
    /// loading the builtin backends and the plugin backends.
    fn load_builtin_backends(
        &mut self,
        enable_cache_temporary_redirect: bool,
        cache_temporary_redirect_ttl: Duration,
    ) -> Result<()> {
        self.backends.insert(
            "http".to_string(),
            Box::new(http::HTTP::new(
                http::HTTP_SCHEME,
                self.config.backend.clone().request_header,
                self.config.backend.clone().max_retries,
                enable_cache_temporary_redirect,
                cache_temporary_redirect_ttl,
                self.config.backend.enable_hickory_dns,
            )?),
        );
        info!("load [http] builtin backend");

        self.backends.insert(
            "https".to_string(),
            Box::new(http::HTTP::new(
                http::HTTPS_SCHEME,
                self.config.backend.clone().request_header,
                self.config.backend.clone().max_retries,
                enable_cache_temporary_redirect,
                cache_temporary_redirect_ttl,
                self.config.backend.enable_hickory_dns,
            )?),
        );
        info!("load [https] builtin backend");

        self.backends.insert(
            "s3".to_string(),
            Box::new(object_storage::ObjectStorage::new(
                object_storage::Scheme::S3,
                self.config.clone(),
            )?),
        );
        info!("load [s3] builtin backend");

        self.backends.insert(
            "gs".to_string(),
            Box::new(object_storage::ObjectStorage::new(
                object_storage::Scheme::GCS,
                self.config.clone(),
            )?),
        );
        info!("load [gcs] builtin backend");

        self.backends.insert(
            "abs".to_string(),
            Box::new(object_storage::ObjectStorage::new(
                object_storage::Scheme::ABS,
                self.config.clone(),
            )?),
        );
        info!("load [abs] builtin backend");

        self.backends.insert(
            "oss".to_string(),
            Box::new(object_storage::ObjectStorage::new(
                object_storage::Scheme::OSS,
                self.config.clone(),
            )?),
        );
        info!("load [oss] builtin backend");

        self.backends.insert(
            "obs".to_string(),
            Box::new(object_storage::ObjectStorage::new(
                object_storage::Scheme::OBS,
                self.config.clone(),
            )?),
        );
        info!("load [obs] builtin backend");

        self.backends.insert(
            "cos".to_string(),
            Box::new(object_storage::ObjectStorage::new(
                object_storage::Scheme::COS,
                self.config.clone(),
            )?),
        );
        info!("load [cos] builtin backend");

        self.backends
            .insert("hdfs".to_string(), Box::new(hdfs::Hdfs::new()));
        info!("load [hdfs] builtin backend");

        self.backends.insert(
            model_scope::SCHEME.to_string(),
            Box::new(model_scope::ModelScope::new(self.config.clone())?),
        );
        info!("load [modelscope] builtin backend");

        self.backends.insert(
            "hf".to_string(),
            Box::new(hugging_face::HuggingFace::new(self.config.clone())?),
        );
        info!("load [hf] builtin backend");

        Ok(())
    }

    /// Load plugin backends loads the plugin backends by the plugin directory.
    fn load_plugin_backends(&mut self, plugin_dir: &Path) -> Result<()> {
        let backend_plugin_dir = plugin_dir.join(NAME);
        if !backend_plugin_dir.exists() {
            warn!(
                "skip loading plugin backends, because the plugin directory {} does not exist",
                backend_plugin_dir.display()
            );
            return Ok(());
        }

        for entry in fs::read_dir(backend_plugin_dir)? {
            let path = entry?.path();

            // Load shared libraries by register_plugin function,
            // file name is the scheme of the backend.
            unsafe {
                self.libraries
                    .push(Library::new(path.as_os_str()).or_err(ErrorType::PluginError)?);
                let lib = &self.libraries[self.libraries.len() - 1];

                let register_plugin: libloading::Symbol<
                    unsafe extern "C" fn() -> Box<dyn Backend + Send + Sync>,
                > = lib.get(b"register_plugin").or_err(ErrorType::PluginError)?;

                if let Some(file_stem) = path.file_stem() {
                    if let Some(plugin_name) =
                        file_stem.to_string_lossy().to_string().strip_prefix("lib")
                    {
                        self.backends
                            .insert(plugin_name.to_string(), register_plugin());
                        info!("load [{}] plugin backend", plugin_name);
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn should_create_backend_factory_without_plugin_dir() {
        let result = BackendFactory::new(Arc::new(Config::default()), None);
        assert!(result.is_ok());
    }

    #[test]
    fn should_load_builtin_backends() {
        let factory = BackendFactory::new(Arc::new(Config::default()), None).unwrap();
        let expected_backends = vec![
            "http",
            "https",
            "s3",
            "gs",
            "abs",
            "oss",
            "obs",
            "cos",
            "hdfs",
            "hf",
            "modelscope",
        ];
        for backend in expected_backends {
            assert!(factory.backends.contains_key(backend));
        }
    }

    #[test]
    fn should_load_plugin_backends() {
        // Create plugin directory.
        let dir = tempdir().unwrap();
        let plugin_dir = dir.path().join("plugin");
        std::fs::create_dir(&plugin_dir).unwrap();

        let backend_dir = plugin_dir.join(NAME);
        std::fs::create_dir(&backend_dir).unwrap();

        build_example_plugin(&backend_dir);

        let result = BackendFactory::new(Arc::new(Config::default()), Some(&plugin_dir));
        assert!(result.is_ok());

        let factory = result.unwrap();
        assert!(factory.backends.contains_key("hdfs"));
    }

    #[test]
    fn should_skip_loading_plugins_when_plugin_dir_is_invalid() {
        let dir = tempdir().unwrap();
        let plugin_dir = dir.path().join("non_existent_plugin_dir");

        let factory = BackendFactory::new(Arc::new(Config::default()), Some(&plugin_dir)).unwrap();
        assert_eq!(factory.backends.len(), 11);
    }

    #[test]
    fn should_return_error_when_plugin_loading_fails() {
        let dir = tempdir().unwrap();
        let plugin_dir = dir.path().join("plugin");
        std::fs::create_dir(&plugin_dir).unwrap();

        let backend_dir = plugin_dir.join(NAME);
        std::fs::create_dir(&backend_dir).unwrap();

        // Invalid plugin that cannot be loaded.
        let lib_path = backend_dir.join("libinvalid_plugin.so");
        std::fs::write(&lib_path, b"invalid content").unwrap();

        let result = BackendFactory::new(Arc::new(Config::default()), Some(&plugin_dir));
        assert!(result.is_err());
        let err_msg = format!("{}", result.err().unwrap());

        assert!(
            err_msg.starts_with("PluginError cause:"),
            "error message should start with 'PluginError cause:'"
        );
        assert!(
            err_msg.contains(&lib_path.display().to_string()),
            "error message should contain library path"
        );
    }

    #[test]
    fn should_build_correct_backend() {
        // Create plugin directory.
        let dir = tempdir().unwrap();
        let plugin_dir = dir.path().join("plugin");
        std::fs::create_dir(&plugin_dir).unwrap();

        let backend_dir = plugin_dir.join(NAME);
        std::fs::create_dir(&backend_dir).unwrap();

        build_example_plugin(&backend_dir);

        let factory = BackendFactory::new(Arc::new(Config::default()), Some(&plugin_dir)).unwrap();
        let schemes = vec![
            "http", "https", "s3", "gs", "abs", "oss", "obs", "cos", "hdfs", "hf",
        ];

        for scheme in schemes {
            let result = factory.build(&format!("{scheme}://example.com/key"));
            assert!(result.is_ok());

            let backend = result.unwrap();
            assert_eq!(backend.scheme(), scheme);
        }
    }

    #[test]
    fn should_return_error_when_backend_scheme_is_not_support() {
        let factory = BackendFactory::new(Arc::new(Config::default()), None).unwrap();
        let result = factory.build("github://example.com");
        assert!(result.is_err());
        assert_eq!(format!("{}", result.err().unwrap()), "invalid parameter");
    }

    #[test]
    fn should_return_error_when_backend_scheme_is_invalid() {
        let factory = BackendFactory::new(Arc::new(Config::default()), None).unwrap();
        let result = factory.build("invalid_scheme://example.com");
        assert!(result.is_err());
        assert_eq!(
            format!("{}", result.err().unwrap()),
            "ParseError cause: relative URL without a base",
        );
    }

    // build_example_plugin builds the example plugin.
    fn build_example_plugin(backend_dir: &Path) {
        // Build example plugin.
        let status = std::process::Command::new("cargo")
            .arg("build")
            .current_dir("./examples/plugin")
            .status()
            .unwrap();
        assert!(status.success());

        let plugin_file = if cfg!(target_os = "macos") {
            "libhdfs.dylib"
        } else {
            "libhdfs.so"
        };

        std::fs::rename(
            format!("../target/debug/{plugin_file}"),
            backend_dir.join(plugin_file),
        )
        .unwrap();
    }
}
