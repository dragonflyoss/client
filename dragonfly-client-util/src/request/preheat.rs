/*
 *     Copyright 2025 The Dragonfly Authors
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

use super::errors::Error;
use super::{GetRequest, Request, Result};
use oci_client::client::ClientConfig;
use oci_client::manifest::ImageIndexEntry;
use oci_client::secrets::RegistryAuth;
use oci_client::{Client as OciClient, Reference, RegistryOperation};
use oci_spec::image::{Arch, Os};
use reqwest::header::{HeaderMap, HeaderValue};
use rustls_pki_types::CertificateDer;
use std::time::Duration;
use tracing::{debug, info};

/// PreheatRequest represents a request to preheat an OCI image through the
/// Dragonfly seed client. The preheat downloads all blobs (config and layers)
/// of the specified image via the Dragonfly proxy, effectively caching them
/// in the P2P network for faster subsequent access.
pub struct PreheatRequest {
    /// image is the OCI image reference (e.g., "docker.io/library/nginx:latest").
    pub image: String,

    /// username for registry authentication. If not provided, anonymous access is used.
    pub username: Option<String>,

    /// password for registry authentication. If not provided, anonymous access is used.
    pub password: Option<String>,

    /// platform specifies the target platform in the format "os/arch"
    /// (e.g., "linux/amd64", "linux/arm64"). This is used to select the correct
    /// manifest from a multi-platform image index.
    pub platform: String,

    /// piece_length is the optional piece length for the Dragonfly task.
    pub piece_length: Option<u64>,

    /// tag identifies different tasks for the same URL.
    pub tag: Option<String>,

    /// application identifies different tasks for the same URL.
    pub application: Option<String>,

    /// timeout is the timeout for each blob download request.
    pub timeout: Duration,

    /// client_cert is the optional client certificates for the request.
    pub client_cert: Option<Vec<CertificateDer<'static>>>,
}

/// Preheats an OCI image by downloading all its blobs through the Dragonfly proxy.
///
/// This function performs the following steps:
/// 1. Parses the image reference and authenticates with the OCI registry.
/// 2. Pulls the image manifest (handling multi-platform image indexes).
/// 3. Downloads each blob (config + layers) through the Dragonfly proxy using the
///    provided `Request` implementation, discarding the content.
///
/// The downloads go through the Dragonfly seed client's proxy, which caches the
/// content in the P2P network for faster subsequent access.
pub async fn preheat<R: Request>(request: &R, preheat_req: &PreheatRequest) -> Result<()> {
    // Parse image reference.
    let reference: Reference =
        preheat_req
            .image
            .parse()
            .map_err(|err: oci_client::ParseError| {
                Error::InvalidArgument(format!("invalid image reference: {}", err))
            })?;

    // Create registry authentication.
    let auth = match (&preheat_req.username, &preheat_req.password) {
        (Some(username), Some(password)) => RegistryAuth::Basic(username.clone(), password.clone()),
        _ => RegistryAuth::Anonymous,
    };

    // Parse platform (os/arch).
    let (os, arch) = parse_platform(&preheat_req.platform)?;

    // Create OCI client with a platform resolver that matches the requested os/arch.
    let oci_config = ClientConfig {
        platform_resolver: Some(Box::new(move |manifests: &[ImageIndexEntry]| {
            manifests
                .iter()
                .find(|entry| {
                    entry
                        .platform
                        .as_ref()
                        .is_some_and(|p| p.os == os && p.architecture == arch)
                })
                .map(|e| e.digest.clone())
        })),
        ..Default::default()
    };
    let oci_client = OciClient::new(oci_config);

    // Authenticate with the registry and get a bearer token if available.
    let token = oci_client
        .auth(&reference, &auth, RegistryOperation::Pull)
        .await
        .map_err(|err| Error::Internal(format!("failed to authenticate with registry: {}", err)))?;

    // Pull image manifest. This handles multi-platform image index manifests
    // by selecting the platform-specific manifest using our resolver.
    let (manifest, digest) = oci_client
        .pull_image_manifest(&reference, &auth)
        .await
        .map_err(|err| Error::Internal(format!("failed to pull image manifest: {}", err)))?;

    info!(
        "pulled manifest for image {} with digest {}, layers: {}",
        preheat_req.image,
        digest,
        manifest.layers.len()
    );

    // Build authorization header for blob downloads through the Dragonfly proxy.
    let mut auth_headers = HeaderMap::new();
    if let Some(ref token) = token {
        auth_headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {}", token))
                .map_err(|err| Error::Internal(format!("invalid auth token: {}", err)))?,
        );
    } else if let (Some(username), Some(password)) = (&preheat_req.username, &preheat_req.password)
    {
        let credentials = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            format!("{}:{}", username, password),
        );
        auth_headers.insert(
            "Authorization",
            HeaderValue::from_str(&format!("Basic {}", credentials))
                .map_err(|err| Error::Internal(format!("invalid credentials: {}", err)))?,
        );
    }

    // Construct blob download URLs and download through Dragonfly.
    let registry = reference.resolve_registry();
    let repository = reference.repository();

    // Collect all blob digests: config + layers.
    let config_digest = manifest.config.digest.clone();
    let mut blob_digests: Vec<String> = vec![config_digest];
    for layer in &manifest.layers {
        blob_digests.push(layer.digest.clone());
    }

    for blob_digest in &blob_digests {
        let blob_url = format!(
            "https://{}/v2/{}/blobs/{}",
            registry, repository, blob_digest
        );

        debug!("preheating blob: {}", blob_url);

        let get_request = GetRequest {
            url: blob_url.clone(),
            header: Some(auth_headers.clone()),
            piece_length: preheat_req.piece_length,
            tag: preheat_req.tag.clone(),
            application: preheat_req.application.clone(),
            filtered_query_params: vec![],
            content_for_calculating_task_id: None,
            priority: None,
            timeout: preheat_req.timeout,
            client_cert: preheat_req.client_cert.clone(),
        };

        let response = request.get(get_request).await?;

        // Read and discard the body to complete the download through Dragonfly.
        if let Some(mut reader) = response.reader {
            tokio::io::copy(&mut reader, &mut tokio::io::sink())
                .await
                .map_err(|err| {
                    Error::Internal(format!("failed to read blob {}: {}", blob_digest, err))
                })?;
        }

        info!("preheated blob: {}", blob_url);
    }

    info!("preheat completed for image: {}", preheat_req.image);
    Ok(())
}

/// Parses a platform string in the format "os/arch" into Os and Arch types.
fn parse_platform(platform: &str) -> Result<(Os, Arch)> {
    let parts: Vec<&str> = platform.split('/').collect();
    if parts.len() != 2 {
        return Err(Error::InvalidArgument(format!(
            "invalid platform format '{}', expected 'os/arch' (e.g., 'linux/amd64')",
            platform
        )));
    }

    let os: Os = serde_json::from_str(&format!("\"{}\"", parts[0]))
        .map_err(|_| Error::InvalidArgument(format!("unsupported OS: {}", parts[0])))?;
    let arch: Arch = serde_json::from_str(&format!("\"{}\"", parts[1]))
        .map_err(|_| Error::InvalidArgument(format!("unsupported architecture: {}", parts[1])))?;

    Ok((os, arch))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_platform_valid() {
        let (os, arch) = parse_platform("linux/amd64").unwrap();
        assert_eq!(os, Os::Linux);
        assert_eq!(arch, Arch::Amd64);
    }

    #[test]
    fn test_parse_platform_arm64() {
        let (os, arch) = parse_platform("linux/arm64").unwrap();
        assert_eq!(os, Os::Linux);
        assert_eq!(arch, Arch::ARM64);
    }

    #[test]
    fn test_parse_platform_invalid_format() {
        let result = parse_platform("linux");
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::InvalidArgument(_))));
    }

    #[test]
    fn test_parse_platform_unknown_os() {
        // Unknown OS values are accepted as Os::Other.
        let (os, arch) = parse_platform("unknown_os/amd64").unwrap();
        assert_eq!(os, Os::Other("unknown_os".to_string()));
        assert_eq!(arch, Arch::Amd64);
    }

    #[test]
    fn test_parse_platform_unknown_arch() {
        // Unknown architecture values are accepted as Arch::Other.
        let (os, arch) = parse_platform("linux/unknown_arch").unwrap();
        assert_eq!(os, Os::Linux);
        assert_eq!(arch, Arch::Other("unknown_arch".to_string()));
    }

    #[test]
    fn test_parse_platform_too_many_parts() {
        let result = parse_platform("linux/amd64/extra");
        assert!(result.is_err());
        assert!(matches!(result, Err(Error::InvalidArgument(_))));
    }
}
