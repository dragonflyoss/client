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

//! Debug-safe wrappers for gRPC messages that carry authentication material.
//!
//! The dfdaemon download/upload handlers log the incoming `Download` payload
//! at `info!` level for operator visibility. That payload carries the S3 /
//! OSS / HDFS / HuggingFace / ModelScope credentials supplied by the caller,
//! and the auto-derived `Debug` impl on the generated protobuf types prints
//! those credential strings verbatim. Running dfdaemon at the default
//! `info` log level therefore persists caller-provided AWS secret access
//! keys and STS session tokens in plaintext to
//! `/var/log/dragonfly/dfdaemon/dfdaemon.log`.
//!
//! This module provides newtype wrappers that implement `Debug` by scrubbing
//! the sensitive leaf fields before delegating back to the auto-derived
//! formatter. The scrub clones the message, so the cost is only paid when
//! the log event actually fires at the current tracing level -- callers
//! below the enabled level do not allocate.

use dragonfly_api::common::v2::Download;
use std::fmt;

/// Placeholder inserted in place of redacted secret material.
const REDACTED: &str = "***REDACTED***";

/// Debug-safe wrapper around [`Download`].
///
/// Use at log sites that would otherwise Debug-print the full `Download`
/// payload (e.g. `info!("download task started: {:?}", download)`):
///
/// ```ignore
/// use crate::grpc::debug::RedactedDownload;
/// info!("download task started: {:?}", RedactedDownload(&download));
/// ```
///
/// The following fields are replaced with [`REDACTED`] when present:
///
/// - `object_storage.access_key_secret`
/// - `object_storage.session_token`
/// - `object_storage.security_token`
/// - `object_storage.credential_path`  (GCS OAuth2 credential file path)
/// - `hdfs.delegation_token`
/// - `hugging_face.token`
/// - `model_scope.token`
/// - sensitive request headers such as `authorization`, `cookie`, and
///   object-storage session-token headers
///
/// Non-secret identifiers such as `object_storage.access_key_id` are left
/// intact because an AKIA/ASIA-style ID on its own is not sufficient to
/// authenticate and is commonly used for request correlation in operator
/// logs.
pub struct RedactedDownload<'a>(pub &'a Download);

impl fmt::Debug for RedactedDownload<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = self.0.clone();
        scrub_download(&mut d);
        fmt::Debug::fmt(&d, f)
    }
}

/// Replace sensitive credential fields on `download` in place with a fixed
/// placeholder. Exposed for unit tests; production call sites should go
/// through [`RedactedDownload`].
pub(crate) fn scrub_download(download: &mut Download) {
    if let Some(os) = download.object_storage.as_mut() {
        if os.access_key_secret.is_some() {
            os.access_key_secret = Some(REDACTED.to_string());
        }
        if os.session_token.is_some() {
            os.session_token = Some(REDACTED.to_string());
        }
        if os.security_token.is_some() {
            os.security_token = Some(REDACTED.to_string());
        }
        if os.credential_path.is_some() {
            os.credential_path = Some(REDACTED.to_string());
        }
    }
    for (key, value) in download.request_header.iter_mut() {
        if is_sensitive_header(key) {
            *value = REDACTED.to_string();
        }
    }
    if let Some(hdfs) = download.hdfs.as_mut() {
        if hdfs.delegation_token.is_some() {
            hdfs.delegation_token = Some(REDACTED.to_string());
        }
    }
    if let Some(hf) = download.hugging_face.as_mut() {
        if hf.token.is_some() {
            hf.token = Some(REDACTED.to_string());
        }
    }
    if let Some(ms) = download.model_scope.as_mut() {
        if ms.token.is_some() {
            ms.token = Some(REDACTED.to_string());
        }
    }
}

fn is_sensitive_header(key: &str) -> bool {
    matches!(
        key.to_ascii_lowercase().as_str(),
        "authorization"
            | "proxy-authorization"
            | "cookie"
            | "set-cookie"
            | "x-amz-security-token"
            | "x-oss-security-token"
            | "x-auth-token"
            | "x-api-key"
            | "x-goog-api-key"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use dragonfly_api::common::v2::{Hdfs, HuggingFace, ModelScope, ObjectStorage};

    fn sample_download() -> Download {
        let mut download = Download {
            url: "s3://bucket/key".to_string(),
            object_storage: Some(ObjectStorage {
                region: Some("us-west-2".to_string()),
                access_key_id: Some("AKIAEXAMPLEID".to_string()),
                access_key_secret: Some("very-secret-key".to_string()),
                session_token: Some("session-token-value".to_string()),
                security_token: Some("security-token-value".to_string()),
                credential_path: Some("/etc/gcs/creds.json".to_string()),
                endpoint: None,
                predefined_acl: None,
                insecure_skip_verify: None,
            }),
            hdfs: Some(Hdfs {
                delegation_token: Some("hdfs-delegation-token".to_string()),
            }),
            hugging_face: Some(HuggingFace {
                token: Some("hf_secret".to_string()),
                revision: "main".to_string(),
            }),
            model_scope: Some(ModelScope {
                token: Some("ms_secret".to_string()),
                revision: "main".to_string(),
            }),
            ..Default::default()
        };
        download.request_header.insert(
            "Authorization".to_string(),
            "Bearer header-token-secret".to_string(),
        );
        download.request_header.insert(
            "X-Amz-Security-Token".to_string(),
            "header-session-token".to_string(),
        );
        download
            .request_header
            .insert("User-Agent".to_string(), "dfget-test".to_string());
        download
    }

    #[test]
    fn redacts_object_storage_credentials() {
        let mut d = sample_download();
        scrub_download(&mut d);
        let os = d.object_storage.unwrap();
        assert_eq!(os.access_key_secret.as_deref(), Some(REDACTED));
        assert_eq!(os.session_token.as_deref(), Some(REDACTED));
        assert_eq!(os.security_token.as_deref(), Some(REDACTED));
        assert_eq!(os.credential_path.as_deref(), Some(REDACTED));
        // Non-secret ID must be preserved.
        assert_eq!(os.access_key_id.as_deref(), Some("AKIAEXAMPLEID"));
    }

    #[test]
    fn redacts_hdfs_hf_modelscope_tokens() {
        let mut d = sample_download();
        scrub_download(&mut d);
        assert_eq!(
            d.hdfs.unwrap().delegation_token.as_deref(),
            Some(REDACTED)
        );
        assert_eq!(d.hugging_face.unwrap().token.as_deref(), Some(REDACTED));
        assert_eq!(d.model_scope.unwrap().token.as_deref(), Some(REDACTED));
    }

    #[test]
    fn redacts_sensitive_request_headers_case_insensitively() {
        let mut d = sample_download();
        scrub_download(&mut d);
        assert_eq!(
            d.request_header.get("Authorization").map(String::as_str),
            Some(REDACTED)
        );
        assert_eq!(
            d.request_header.get("X-Amz-Security-Token").map(String::as_str),
            Some(REDACTED)
        );
        assert_eq!(
            d.request_header.get("User-Agent").map(String::as_str),
            Some("dfget-test")
        );
    }

    #[test]
    fn does_not_introduce_fields_that_were_none() {
        // If a field was None before scrubbing, it must remain None (i.e. the
        // scrubber must not leak information about whether the caller set
        // each field).
        let mut d = Download {
            url: "s3://bucket/key".to_string(),
            object_storage: Some(ObjectStorage {
                region: Some("us-west-2".to_string()),
                access_key_id: Some("AKIAEXAMPLEID".to_string()),
                access_key_secret: None,
                session_token: None,
                security_token: None,
                credential_path: None,
                endpoint: None,
                predefined_acl: None,
                insecure_skip_verify: None,
            }),
            ..Default::default()
        };
        scrub_download(&mut d);
        let os = d.object_storage.unwrap();
        assert!(os.access_key_secret.is_none());
        assert!(os.session_token.is_none());
        assert!(os.security_token.is_none());
        assert!(os.credential_path.is_none());
    }

    #[test]
    fn redacted_debug_does_not_contain_secrets() {
        let d = sample_download();
        let rendered = format!("{:?}", RedactedDownload(&d));
        assert!(
            !rendered.contains("very-secret-key"),
            "debug output leaked access_key_secret: {}",
            rendered
        );
        assert!(
            !rendered.contains("session-token-value"),
            "debug output leaked session_token: {}",
            rendered
        );
        assert!(
            !rendered.contains("security-token-value"),
            "debug output leaked security_token: {}",
            rendered
        );
        assert!(
            !rendered.contains("hdfs-delegation-token"),
            "debug output leaked hdfs delegation_token: {}",
            rendered
        );
        assert!(
            !rendered.contains("hf_secret"),
            "debug output leaked huggingface token: {}",
            rendered
        );
        assert!(
            !rendered.contains("ms_secret"),
            "debug output leaked modelscope token: {}",
            rendered
        );
        assert!(
            !rendered.contains("header-token-secret"),
            "debug output leaked authorization header: {}",
            rendered
        );
        assert!(
            !rendered.contains("header-session-token"),
            "debug output leaked object storage session-token header: {}",
            rendered
        );
        // Sanity: identifiers that are safe to log should still be present.
        assert!(rendered.contains("AKIAEXAMPLEID"));
        assert!(rendered.contains("dfget-test"));
    }
}
