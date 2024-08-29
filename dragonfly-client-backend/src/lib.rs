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

use dragonfly_api::common::v2::{ObjectStorage, Range};
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use libloading::Library;
use reqwest::header::HeaderMap;
use rustls_pki_types::CertificateDer;
use std::path::Path;
use std::{collections::HashMap, pin::Pin, time::Duration};
use std::{fmt::Debug, fs};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{error, info, instrument, warn};
use url::Url;

pub mod http;
pub mod object_storage;

// NAME is the name of the package.
pub const NAME: &str = "backend";

// Body is the body of the response.
pub type Body = Box<dyn AsyncRead + Send + Unpin>;

// HeadRequest is the head request for backend.
pub struct HeadRequest {
    // task_id is the id of the task.
    pub task_id: String,

    // url is the url of the request.
    pub url: String,

    // http_header is the headers of the request.
    pub http_header: Option<HeaderMap>,

    // timeout is the timeout of the request.
    pub timeout: Duration,

    // client_certs is the client certificates for the request.
    pub client_certs: Option<Vec<CertificateDer<'static>>>,

    // object_storage is the object storage related information.
    pub object_storage: Option<ObjectStorage>,
}

// HeadResponse is the head response for backend.
#[derive(Debug)]
pub struct HeadResponse {
    // success is the success of the response.
    pub success: bool,

    // content_length is the content length of the response.
    pub content_length: Option<u64>,

    // http_header is the headers of the response.
    pub http_header: Option<HeaderMap>,

    // http_status_code is the status code of the response.
    pub http_status_code: Option<reqwest::StatusCode>,

    // Entries is the information of the entries in the directory.
    pub entries: Vec<DirEntry>,

    // error_message is the error message of the response.
    pub error_message: Option<String>,
}

// GetRequest is the get request for backend.
pub struct GetRequest {
    // task_id is the id of the task.
    pub task_id: String,

    // piece_id is the id of the piece.
    pub piece_id: String,

    // url is the url of the request.
    pub url: String,

    // range is the range of the request.
    pub range: Option<Range>,

    // http_header is the headers of the request.
    pub http_header: Option<HeaderMap>,

    // timeout is the timeout of the request.
    pub timeout: Duration,

    // client_certs is the client certificates for the request.
    pub client_certs: Option<Vec<CertificateDer<'static>>>,

    // the object storage related information.
    pub object_storage: Option<ObjectStorage>,
}

// GetResponse is the get response for backend.
pub struct GetResponse<R>
where
    R: AsyncRead + Unpin,
{
    // success is the success of the response.
    pub success: bool,

    // http_header is the headers of the response.
    pub http_header: Option<HeaderMap>,

    // http_status_code is the status code of the response.
    pub http_status_code: Option<reqwest::StatusCode>,

    // body is the content of the response.
    pub reader: R,

    // error_message is the error message of the response.
    pub error_message: Option<String>,
}

// GetResponse implements the response functions.
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
#[derive(Debug, PartialEq, Eq)]
pub struct DirEntry {
    // url is the url of the entry.
    pub url: String,

    // content_length is the content length of the entry.
    pub content_length: usize,

    // is_dir is the flag of the entry is a directory.
    pub is_dir: bool,
}

// Backend is the interface of the backend.
#[tonic::async_trait]
pub trait Backend {
    // scheme returns the scheme of the backend.
    fn scheme(&self) -> String;

    // head gets the header of the request.
    async fn head(&self, request: HeadRequest) -> Result<HeadResponse>;

    // get gets the content of the request.
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>>;
}

// BackendFactory is the factory of the backend.
#[derive(Default)]
pub struct BackendFactory {
    // backends is the backends of the factory, including the plugin backends and
    // the builtin backends.
    backends: HashMap<String, Box<dyn Backend + Send + Sync>>,
    // libraries is used to store the plugin's dynamic library, because when not saving the `Library`,
    // it will drop when out of scope, resulting in the null pointer error.
    libraries: Vec<Library>,
}

// BackendFactory implements the factory of the backend. It supports loading builtin
// backends and plugin backends.
//
// The builtin backends are http, https, etc, which are implemented
// by the HTTP struct.
//
// The plugin backends are shared libraries, which are loaded
// by the `register_plugin` function. The file name of the shared
// library is the scheme of the backend. The shared library
// should implement the Backend trait. Default plugin directory
// is `/var/lib/dragonfly/plugins/` in linux and `~/.dragonfly/plugins`
// in macos. The plugin directory can be set by the dfdaemon configuration.
//
// For example:
// If implement a plugin backend named `hdfs`, the shared library
// should be named `libhdfs.so` or `libhdfs.dylib` and move the file to the backend plugin directory
// `/var/lib/dragonfly/plugins/backend/` in linux or `~/.dragonfly/plugins/backend/`
// in macos. When the dfdaemon starts, it will load the `hdfs` plugin backend in the
// backend plugin directory. So the dfdaemon or dfget can use the `hdfs` plugin backend
// to download the file by the url `hdfs://example.com/file`.
// The backend plugin implementation can refer to
// https://github.com/dragonflyoss/client/tree/main/dragonfly-client-backend/examples/plugin/.
impl BackendFactory {
    // new returns a new BackendFactory.
    #[instrument(skip_all)]
    pub fn new(plugin_dir: Option<&Path>) -> Result<Self> {
        let mut backend_factory = Self::default();

        backend_factory.load_builtin_backends();

        if let Some(plugin_dir) = plugin_dir {
            backend_factory
                .load_plugin_backends(plugin_dir)
                .map_err(|err| {
                    error!("failed to load plugin backends: {}", err);
                    err
                })?;
        }

        Ok(backend_factory)
    }

    // build returns the backend by the scheme of the url.
    #[instrument(skip_all)]
    pub fn build(&self, url: &str) -> Result<&(dyn Backend + Send + Sync)> {
        let url = Url::parse(url).or_err(ErrorType::ParseError)?;
        let scheme = url.scheme();
        self.backends
            .get(scheme)
            .map(|boxed_backend| &**boxed_backend)
            .ok_or(Error::InvalidParameter)
    }

    // load_builtin_backends loads the builtin backends.
    #[instrument(skip_all)]
    fn load_builtin_backends(&mut self) {
        self.backends
            .insert("http".to_string(), Box::new(http::HTTP::new("http")));
        info!("load [http] builtin backend");

        self.backends
            .insert("https".to_string(), Box::new(http::HTTP::new("https")));
        info!("load [https] builtin backend ");

        self.backends.insert(
            "s3".to_string(),
            Box::new(object_storage::ObjectStorage::new(
                object_storage::Scheme::S3,
            )),
        );
        info!("load [s3] builtin backend");

        self.backends.insert(
            "gs".to_string(),
            Box::new(object_storage::ObjectStorage::new(
                object_storage::Scheme::GCS,
            )),
        );
        info!("load [gcs] builtin backend");

        self.backends.insert(
            "abs".to_string(),
            Box::new(object_storage::ObjectStorage::new(
                object_storage::Scheme::ABS,
            )),
        );
        info!("load [abs] builtin backend");

        self.backends.insert(
            "oss".to_string(),
            Box::new(object_storage::ObjectStorage::new(
                object_storage::Scheme::OSS,
            )),
        );
        info!("load [oss] builtin backend ");

        self.backends.insert(
            "obs".to_string(),
            Box::new(object_storage::ObjectStorage::new(
                object_storage::Scheme::OBS,
            )),
        );
        info!("load [obs] builtin backend");

        self.backends.insert(
            "cos".to_string(),
            Box::new(object_storage::ObjectStorage::new(
                object_storage::Scheme::COS,
            )),
        );
        info!("load [cos] builtin backend");
    }

    // load_plugin_backends loads the plugin backends.
    #[instrument(skip_all)]
    fn load_plugin_backends(&mut self, plugin_dir: &Path) -> Result<()> {
        let backend_plugin_dir = plugin_dir.join(NAME);
        if !backend_plugin_dir.exists() {
            warn!(
                "skip loading plugin backends, because the plugin directory {} does not exist",
                plugin_dir.display()
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
        let factory = BackendFactory::new(None);
        assert!(
            factory.is_ok(),
            "Expected BackendFactory to be created successfully."
        );
    }

    #[test]
    fn should_load_builtin_backends() {
        let factory = BackendFactory::new(None).unwrap();
        let expected_backends = vec!["http", "https", "s3", "gs", "abs", "oss", "obs", "cos"];
        for backend in expected_backends {
            assert!(
                factory.backends.contains_key(backend),
                "Expected '{}' backend to be loaded.",
                backend
            );
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

        let result = BackendFactory::new(Some(&plugin_dir));
        assert!(
            result.is_ok(),
            "Expected BackendFactory to be created successfully."
        );
        let factory = result.unwrap();
        assert!(
            factory.backends.contains_key("hdfs"),
            "Expected to get plugin backend."
        );
    }

    #[test]
    fn should_skip_loading_plugins_when_plugin_dir_is_invalid() {
        let dir = tempdir().unwrap();
        let plugin_dir = dir.path().join("non_existent_plugin_dir");

        let factory = BackendFactory::new(Some(&plugin_dir)).unwrap();
        assert_eq!(factory.backends.len(), 8);
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

        let result = BackendFactory::new(Some(&plugin_dir));
        assert!(
            result.is_err(),
            "Expected an error when plugin loading fails."
        );
        let error = result.err().unwrap();
        assert_eq!(
            format!("{}", error),
            format!("PluginError cause: {}: file too short", lib_path.display()),
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

        let factory = BackendFactory::new(Some(&plugin_dir)).unwrap();
        let schemes = vec![
            "http", "https", "s3", "gs", "abs", "oss", "obs", "cos", "hdfs",
        ];

        for scheme in schemes {
            let result = factory.build(&format!("{}://example.com/key", scheme));
            assert!(
                result.is_ok(),
                "Expected `{}` backend to be built successfully.",
                scheme
            );
            let backend = result.unwrap();
            assert_eq!(backend.scheme(), scheme);
        }
    }

    #[test]
    fn should_return_error_when_backend_scheme_is_not_support() {
        let factory = BackendFactory::new(None).unwrap();
        let result = factory.build("github://example.com");
        assert!(
            result.is_err(),
            "Expected an error when backend scheme is invalid."
        );
        let error = result.err().unwrap();
        assert_eq!(format!("{}", error), "invalid parameter");
    }

    #[test]
    fn should_return_error_when_backend_scheme_is_invalid() {
        let factory = BackendFactory::new(None).unwrap();
        let result = factory.build("invalid_scheme://example.com");
        assert!(
            result.is_err(),
            "Expected an error when backend scheme is invalid."
        );
        let error = result.err().unwrap();
        assert_eq!(
            format!("{}", error),
            "ParseError cause: relative URL without a base",
        );
    }

    fn build_example_plugin(backend_dir: &Path) {
        // Build example plugin.
        let status = std::process::Command::new("cargo")
            .arg("build")
            .current_dir("./examples/plugin")
            .status()
            .unwrap();
        assert!(status.success());

        // Move example plugin to temporary plugin directory.
        std::fs::rename("../target/debug/libhdfs.so", backend_dir.join("libhdfs.so")).unwrap();
    }
}
