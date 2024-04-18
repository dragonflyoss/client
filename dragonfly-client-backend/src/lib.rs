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

use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use libloading::Library;
use reqwest::header::HeaderMap;
use rustls_pki_types::CertificateDer;
use std::fs;
use std::path::Path;
use std::{collections::HashMap, pin::Pin, time::Duration};
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{error, info, warn};
use url::Url;

pub mod http;

// NAME is the name of the package.
pub const NAME: &str = "backend";

// Body is the body of the response.
pub type Body = Box<dyn AsyncRead + Send + Unpin>;

// HeadRequest is the head request for backend.
pub struct HeadRequest {
    // url is the url of the request.
    pub url: String,

    // http_header is the headers of the request.
    pub http_header: Option<HeaderMap>,

    // timeout is the timeout of the request.
    pub timeout: Duration,

    // client_certs is the client certificates for the request.
    pub client_certs: Option<Vec<CertificateDer<'static>>>,
}

// HeadResponse is the head response for backend.
pub struct HeadResponse {
    // http_header is the headers of the response.
    pub http_header: Option<HeaderMap>,

    // http_status_code is the status code of the response.
    pub http_status_code: Option<reqwest::StatusCode>,
}

// GetRequest is the get request for backend.
pub struct GetRequest {
    // url is the url of the request.
    pub url: String,

    // http_header is the headers of the request.
    pub http_header: Option<HeaderMap>,

    // timeout is the timeout of the request.
    pub timeout: Duration,

    // client_certs is the client certificates for the request.
    pub client_certs: Option<Vec<CertificateDer<'static>>>,
}

// GetResponse is the get response for backend.
pub struct GetResponse<R>
where
    R: AsyncRead + Unpin,
{
    // http_header is the headers of the response.
    pub http_header: Option<HeaderMap>,

    // http_status_code is the status code of the response.
    pub http_status_code: Option<reqwest::StatusCode>,

    // body is the content of the response.
    pub reader: R,
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

// Backend is the interface of the backend.
#[tonic::async_trait]
pub trait Backend {
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
// If implement a plugin backend named `ftp`, the shared library
// should be named `ftp.so` and move the file to the backend plugin directory
// `/var/lib/dragonfly/plugins/backend/` in linux or `~/.dragonfly/plugins/backend/`
// in macos. When the dfdaemon starts, it will load the `ftp` plugin backend in the
// backend plugin directory. So the dfdaemon or dfget can use the `ftp` plugin backend
// to download the file by the url `ftp://example.com/file`.
// The backend plugin implementation can refer to
// https://github.com/dragonflyoss/client/tree/main/dragonfly-client-backend/examples/ftp/.
impl BackendFactory {
    // new returns a new BackendFactory.
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
    pub fn build(&self, url: &str) -> Result<&(dyn Backend + Send + Sync)> {
        let url = Url::parse(url).or_err(ErrorType::ParseError)?;
        let scheme = url.scheme();
        self.backends
            .get(scheme)
            .map(|boxed_backend| &**boxed_backend)
            .ok_or(Error::InvalidParameter)
    }

    // load_builtin_backends loads the builtin backends.
    fn load_builtin_backends(&mut self) {
        self.backends
            .insert("http".to_string(), Box::new(http::HTTP::new()));
        info!("load [http] builtin backend");

        self.backends
            .insert("https".to_string(), Box::new(http::HTTP::new()));
        info!("load [https] builtin backend ");
    }

    // load_plugin_backends loads the plugin backends.
    fn load_plugin_backends(&mut self, plugin_dir: &Path) -> Result<()> {
        if !plugin_dir.exists() {
            warn!("skip loading plugin backends, because the plugin directory does not exist");
            return Ok(());
        }

        for entry in fs::read_dir(plugin_dir.join(NAME))? {
            let path = entry?.path();

            // Load shared libraries by register_plugin function,
            // file name is the scheme of the backend.
            unsafe {
                let lib = Library::new(path.as_os_str()).or_err(ErrorType::PluginError)?;
                let register_plugin: libloading::Symbol<
                    unsafe extern "C" fn() -> Box<dyn Backend + Send + Sync>,
                > = lib.get(b"register_plugin").or_err(ErrorType::PluginError)?;

                if let Some(file_name) = path.file_name() {
                    let file_name = file_name.to_string_lossy();
                    self.backends
                        .insert(file_name.to_string(), register_plugin());
                    info!("load [{}] plugin backend", file_name.to_string());
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn should_return_http_backend() {
        let backend_factory =
            BackendFactory::new(Some(Path::new("/var/lib/dragonfly/plugins/backend/"))).unwrap();
        let backend = backend_factory.build("http://example.com");
        assert!(backend.is_ok());
    }
}
