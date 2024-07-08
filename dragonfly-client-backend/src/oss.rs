use std::time::Duration;

use dragonfly_client_core::*;
use dragonfly_client_util::tls::NoVerifier;
use error::BackendError;
use opendal::{raw::HttpClient, Metakey, Operator};
use reqwest::{header::HeaderMap, StatusCode};
use rustls_pki_types::CertificateDer;
use tokio_util::io::StreamReader;
use tracing::info;
use url::Url;

use crate::*;

/// the config that parsed from the url
#[derive(Debug)]
struct OSSConfig {
    url: Url,
    bucket: String,
    endpoint: String,
    version: Option<String>,
}

impl OSSConfig {
    fn is_dir(&self) -> bool {
        self.url.path().ends_with('/')
    }

    #[inline]
    fn key(&self) -> &str {
        self.url.path().trim_start_matches('/')
    }

    fn build_url_with_same_endpoint(&self, path: &str) -> String {
        let mut url = self.url.clone();
        url.set_path(path);
        url.to_string()
    }
}

impl TryFrom<Url> for OSSConfig {
    type Error = Error;

    fn try_from(url: Url) -> std::result::Result<Self, Self::Error> {
        let host_str = url
            .host_str()
            .ok_or_else(|| Error::InvalidURI(url.to_string()))?;

        // Check the url is valid.
        if url.scheme() != "oss"
            || url.path().is_empty()
            || !host_str.ends_with("aliyuncs.com")
            || host_str.matches('.').count() < 3
        {
            return Err(Error::InvalidURI(url.to_string()));
        }

        // Parse the bucket and endpoint
        let (bucket, endpoint) = host_str
            .split_once('.')
            .map(|(s1, s2)| (s1.to_string(), format!("https://{}", s2))) // Add scheme for endpoint.
            .ok_or_else(|| Error::InvalidURI(url.to_string()))?;

        // Parse the possible version
        let version = url
            .query_pairs()
            .find(|(key, _)| key == "versionId")
            .map(|(_, version)| version.to_string());

        Ok(Self {
            url,
            endpoint,
            bucket,
            version,
        })
    }
}

#[derive(Default)]
pub struct OSS;

impl OSS {
    pub fn new() -> Self {
        Self
    }

    // client_builder returns a new reqwest client builder.
    fn client_builder(
        &self,
        client_certs: Option<Vec<CertificateDer<'static>>>,
    ) -> reqwest::ClientBuilder {
        let client_config_builder = match client_certs.as_ref() {
            Some(client_certs) => {
                let mut root_cert_store = rustls::RootCertStore::empty();
                root_cert_store.add_parsable_certificates(client_certs.to_owned());

                // TLS client config using the custom CA store for lookups.
                rustls::ClientConfig::builder()
                    .with_root_certificates(root_cert_store)
                    .with_no_client_auth()
            }
            // Default TLS client config with native roots.
            None => rustls::ClientConfig::builder()
                .dangerous()
                .with_custom_certificate_verifier(NoVerifier::new())
                .with_no_client_auth(),
        };

        reqwest::Client::builder().use_preconfigured_tls(client_config_builder)
    }

    // Set up the operator of OSS and OSSConfig.
    fn setup_operator_with_config(
        &self,
        header: HeaderMap,
        certs: Option<Vec<CertificateDer<'static>>>,
        timeout: Duration,
        url: String,
        object_storage: Option<ObjectStorage>,
    ) -> Result<(OSSConfig, Operator)> {
        let client = self
            .client_builder(certs)
            .timeout(timeout)
            .default_headers(header)
            .build()?;

        let url: Url = url.parse().map_err(|_| Error::InvalidURI(url))?;

        let config = OSSConfig::try_from(url)?;

        let mut oss_builder = opendal::services::Oss::default();

        let ObjectStorage {
            access_key_id,
            access_key_secret,
        } = object_storage.ok_or(Error::InvalidParameter)?;

        oss_builder
            .access_key_id(&access_key_id)
            .access_key_secret(&access_key_secret)
            .http_client(HttpClient::with(client))
            .root("/")
            .bucket(&config.bucket)
            .endpoint(&config.endpoint);

        Ok((
            config,
            Operator::new(oss_builder).map_err(map_sdk_error)?.finish(),
        ))
    }
}

#[tonic::async_trait]
impl Backend for OSS {
    async fn head(&self, request: HeadRequest) -> Result<HeadResponse> {
        info!(
            "head request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        let (config, op) = self.setup_operator_with_config(
            request.http_header.clone().unwrap_or_default(),
            request.client_certs,
            request.timeout,
            request.url,
            request.object_storage,
        )?;

        // Get the entries if url point to a directory.
        let entries = if config.is_dir() {
            Some(
                op.list_with(config.key())
                    .recursive(request.recursive)
                    .metakey(Metakey::ContentLength | Metakey::Mode | Metakey::Version)
                    .await // Do the list op here.
                    .map_err(map_sdk_error)?
                    .into_iter()
                    .map(|entry| {
                        let path = entry.path();
                        let metadata = entry.metadata();

                        let content_length = metadata.content_length() as usize;
                        let is_dir = metadata.is_dir();
                        let version = metadata.version().map(|v| v.into());

                        DirEntry {
                            url: config.build_url_with_same_endpoint(path),
                            content_length,
                            is_dir,
                            version,
                        }
                    })
                    .collect(),
            )
        } else {
            None
        };

        let mut stat_op = op.stat_with(config.key());

        if let Some(ref version) = config.version {
            stat_op = stat_op.version(version);
        }

        let stat = stat_op.await.map_err(map_sdk_error)?;

        Ok(HeadResponse {
            success: true,
            content_length: Some(stat.content_length()),
            http_header: request.http_header,
            http_status_code: Some(reqwest::StatusCode::OK),
            error_message: None,
            entries,
        })
    }

    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>> {
        info!(
            "get request {} {}: {:?}",
            request.piece_id, request.url, request.http_header
        );

        let (config, op) = self.setup_operator_with_config(
            request.http_header.clone().unwrap_or_default(),
            request.client_certs,
            request.timeout,
            request.url,
            request.object_storage,
        )?;

        let mut reader_op = op.reader_with(config.key()).chunk(8 << 10);

        if let Some(ref version) = config.version {
            reader_op = reader_op.version(version);
        }

        let reader = reader_op.await.map_err(map_sdk_error)?;

        let stream = match request.range {
            Some(range) => reader
                .into_bytes_stream(range.start..range.start + range.length)
                .await
                .map_err(map_sdk_error)?,
            None => reader.into_bytes_stream(..).await.map_err(map_sdk_error)?,
        };

        Ok(GetResponse {
            success: true,
            http_header: None,
            http_status_code: Some(reqwest::StatusCode::OK),
            reader: Box::new(StreamReader::new(stream)),
            error_message: None,
        })
    }
}

fn map_sdk_error(e: opendal::Error) -> Error {
    Error::BackendError(BackendError {
        message: e.to_string(),
        status_code: StatusCode::INTERNAL_SERVER_ERROR,
        header: HeaderMap::default(),
    })
}
