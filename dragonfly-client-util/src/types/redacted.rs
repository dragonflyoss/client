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

//! Debug-safe wrappers for messages that carry authentication material.
//!
//! This module provides newtype wrappers that implement `Debug` by scrubbing
//! the sensitive leaf fields before delegating back to the auto-derived
//! formatter. The scrub clones the message, so the cost is only paid when
//! the log event actually fires at the current tracing level -- callers
//! below the enabled level do not allocate.

use dragonfly_api::common::v2::{Download, Hdfs, HuggingFace, ModelScope, ObjectStorage};
use dragonfly_api::dfdaemon::v2::DownloadPersistentTaskRequest;
use http::header::{AUTHORIZATION, COOKIE, PROXY_AUTHORIZATION, SET_COOKIE};
use std::collections::HashMap;
use std::fmt;

/// Placeholder inserted in place of redacted secret material.
const REDACTED: &str = "[REDACTED]";

/// Debug-safe wrapper around [`Download`].
///
/// Use at log sites that would otherwise Debug-print the full `Download`
/// payload (e.g. `info!("download task started: {:?}", download)`):
///
/// ```ignore
/// use crate::types::redacted::RedactedDownload;
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

/// Debug implementation that redacts sensitive fields before delegating to the auto-derived formatter.
impl fmt::Debug for RedactedDownload<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = self.0.clone();
        scrub_object_storage(&mut d.object_storage);
        scrub_hdfs(&mut d.hdfs);
        scrub_hugging_face(&mut d.hugging_face);
        scrub_model_scope(&mut d.model_scope);
        scrub_sensitive_header(&mut d.request_header);
        fmt::Debug::fmt(&d, f)
    }
}

/// Debug-safe wrapper around [`DownloadPersistentTaskRequest`].
///
/// Use at log sites that would otherwise Debug-print the full
/// `DownloadPersistentTaskRequest` payload (e.g.
/// `info!("persistent task started: {:?}", request)`):
///
/// ```ignore
/// use crate::types::redacted::RedactedDownloadPersistentTaskRequest;
/// info!(
///     "persistent task started: {:?}",
///     RedactedDownloadPersistentTaskRequest(&request),
/// );
/// ```
///
/// The following fields are replaced with [`REDACTED`] when present:
///
/// - `object_storage.access_key_secret`
/// - `object_storage.session_token`
/// - `object_storage.security_token`
/// - `object_storage.credential_path`  (GCS OAuth2 credential file path)
///
/// Non-secret identifiers such as `object_storage.access_key_id` are left
/// intact because an AKIA/ASIA-style ID on its own is not sufficient to
/// authenticate and is commonly used for request correlation in operator
/// logs.
pub struct RedactedDownloadPersistentTaskRequest<'a>(pub &'a DownloadPersistentTaskRequest);

/// Debug implementation that redacts sensitive fields before delegating to the auto-derived
/// formatter.
impl fmt::Debug for RedactedDownloadPersistentTaskRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = self.0.clone();
        scrub_object_storage(&mut d.object_storage);
        fmt::Debug::fmt(&d, f)
    }
}

/// Mutates the given `ObjectStorage` by replacing any present secrets with [`REDACTED`].
fn scrub_object_storage(os: &mut Option<ObjectStorage>) {
    if let Some(os) = os.as_mut() {
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
}

/// Mutates the given `Hdfs` by replacing any present secrets with [`REDACTED`].
fn scrub_hdfs(hdfs: &mut Option<Hdfs>) {
    if let Some(hdfs) = hdfs.as_mut() {
        if hdfs.delegation_token.is_some() {
            hdfs.delegation_token = Some(REDACTED.to_string());
        }
    }
}

/// Mutates the given `HuggingFace` by replacing any present secrets with [`REDACTED`].
fn scrub_hugging_face(hf: &mut Option<HuggingFace>) {
    if let Some(hf) = hf.as_mut() {
        if hf.token.is_some() {
            hf.token = Some(REDACTED.to_string());
        }
    }
}

/// Mutates the given `ModelScope` by replacing any present secrets with [`REDACTED`].
fn scrub_model_scope(ms: &mut Option<ModelScope>) {
    if let Some(ms) = ms.as_mut() {
        if ms.token.is_some() {
            ms.token = Some(REDACTED.to_string());
        }
    }
}

/// Mutates the given request header map by replacing any sensitive headers with [`REDACTED`].
fn scrub_sensitive_header(header: &mut HashMap<String, String>) {
    for (key, value) in header.iter_mut() {
        let lower = key.to_ascii_lowercase();
        let is_sensitive = [
            AUTHORIZATION.as_str(),
            PROXY_AUTHORIZATION.as_str(),
            COOKIE.as_str(),
            SET_COOKIE.as_str(),
        ]
        .contains(&lower.as_str())
            || matches!(
                lower.as_str(),
                "x-amz-security-token"
                    | "x-oss-security-token"
                    | "x-auth-token"
                    | "x-api-key"
                    | "x-goog-api-key"
            );

        if is_sensitive {
            *value = REDACTED.to_string();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_object_storage() -> ObjectStorage {
        ObjectStorage {
            access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            access_key_secret: Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string()),
            session_token: Some("FQoGZXIvYXdzEJr...".to_string()),
            security_token: Some("sec-token-xyz".to_string()),
            credential_path: Some("/etc/gcp/creds.json".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn scrub_object_storage_redacts_all_secret_fields() {
        let mut os = Some(make_object_storage());
        scrub_object_storage(&mut os);
        let os = os.unwrap();

        assert_eq!(os.access_key_secret.as_deref(), Some(REDACTED));
        assert_eq!(os.session_token.as_deref(), Some(REDACTED));
        assert_eq!(os.security_token.as_deref(), Some(REDACTED));
        assert_eq!(os.credential_path.as_deref(), Some(REDACTED));
        assert_eq!(
            os.access_key_id.as_deref(),
            Some("AKIAIOSFODNN7EXAMPLE"),
            "access_key_id must not be redacted"
        );
    }

    #[test]
    fn scrub_object_storage_leaves_none_fields_as_none() {
        let mut os = Some(ObjectStorage {
            access_key_id: Some("AKIA...".to_string()),
            access_key_secret: None,
            session_token: None,
            security_token: None,
            credential_path: None,
            ..Default::default()
        });
        scrub_object_storage(&mut os);
        let os = os.unwrap();

        assert!(os.access_key_secret.is_none());
        assert!(os.session_token.is_none());
        assert!(os.security_token.is_none());
        assert!(os.credential_path.is_none());
        assert_eq!(os.access_key_id.as_deref(), Some("AKIA..."));
    }

    #[test]
    fn scrub_object_storage_handles_none_outer() {
        let mut os: Option<ObjectStorage> = None;
        scrub_object_storage(&mut os);
        assert!(os.is_none());
    }

    #[test]
    fn scrub_hdfs_redacts_delegation_token() {
        let mut hdfs = Some(Hdfs {
            delegation_token: Some("super-secret-token".to_string()),
        });

        scrub_hdfs(&mut hdfs);
        assert_eq!(hdfs.unwrap().delegation_token.as_deref(), Some(REDACTED));
    }

    #[test]
    fn scrub_hdfs_leaves_none_token_alone() {
        let mut hdfs = Some(Hdfs {
            delegation_token: None,
        });

        scrub_hdfs(&mut hdfs);
        assert!(hdfs.unwrap().delegation_token.is_none());
    }

    #[test]
    fn scrub_hugging_face_redacts_token() {
        let mut hf = Some(HuggingFace {
            token: Some("hf_xxxxx".to_string()),
            ..Default::default()
        });

        scrub_hugging_face(&mut hf);
        assert_eq!(hf.unwrap().token.as_deref(), Some(REDACTED));
    }

    #[test]
    fn scrub_model_scope_redacts_token() {
        let mut ms = Some(ModelScope {
            token: Some("ms_xxxxx".to_string()),
            ..Default::default()
        });

        scrub_model_scope(&mut ms);
        assert_eq!(ms.unwrap().token.as_deref(), Some(REDACTED));
    }

    #[test]
    fn scrub_sensitive_header_redacts_standard_auth_headers_case_insensitively() {
        let mut h = HashMap::new();
        h.insert(
            "Authorization".to_string(),
            "Bearer abc.def.ghi".to_string(),
        );
        h.insert(
            "PROXY-AUTHORIZATION".to_string(),
            "Basic dXNlcjpwYXNz".to_string(),
        );
        h.insert("cookie".to_string(), "session=xyz".to_string());
        h.insert(
            "Set-Cookie".to_string(),
            "session=xyz; HttpOnly".to_string(),
        );
        scrub_sensitive_header(&mut h);

        assert_eq!(h["Authorization"], REDACTED);
        assert_eq!(h["PROXY-AUTHORIZATION"], REDACTED);
        assert_eq!(h["cookie"], REDACTED);
        assert_eq!(h["Set-Cookie"], REDACTED);
    }

    #[test]
    fn scrub_sensitive_header_redacts_object_storage_token_headers() {
        let mut h = HashMap::new();
        h.insert("x-amz-security-token".to_string(), "amz-tok".to_string());
        h.insert("X-OSS-Security-Token".to_string(), "oss-tok".to_string());
        h.insert("X-Auth-Token".to_string(), "auth-tok".to_string());
        h.insert("x-api-key".to_string(), "api-key".to_string());
        h.insert("X-Goog-Api-Key".to_string(), "goog-key".to_string());
        scrub_sensitive_header(&mut h);

        for v in h.values() {
            assert_eq!(v, REDACTED);
        }
    }

    #[test]
    fn scrub_sensitive_header_preserves_non_sensitive_headers() {
        let mut h = HashMap::new();
        h.insert("Content-Type".to_string(), "application/json".to_string());
        h.insert("User-Agent".to_string(), "dragonfly/1.0".to_string());
        h.insert("X-Request-Id".to_string(), "req-123".to_string());
        h.insert("X-Custom-Author".to_string(), "alice".to_string());
        scrub_sensitive_header(&mut h);

        assert_eq!(h["Content-Type"], "application/json");
        assert_eq!(h["User-Agent"], "dragonfly/1.0");
        assert_eq!(h["X-Request-Id"], "req-123");
        assert_eq!(h["X-Custom-Author"], "alice");
    }

    #[test]
    fn scrub_sensitive_header_preserves_keys() {
        let mut h = HashMap::new();
        h.insert("Authorization".to_string(), "Bearer x".to_string());
        h.insert("Content-Type".to_string(), "text/plain".to_string());
        scrub_sensitive_header(&mut h);

        assert_eq!(h.len(), 2);
        assert!(h.contains_key("Authorization"));
        assert!(h.contains_key("Content-Type"));
    }

    #[test]
    fn redacted_download_debug_does_not_leak_secrets() {
        let mut header = HashMap::new();
        header.insert(
            "Authorization".to_string(),
            "Bearer SECRET_BEARER".to_string(),
        );
        header.insert("X-Request-Id".to_string(), "req-42".to_string());

        let download = Download {
            object_storage: Some(make_object_storage()),
            hdfs: Some(Hdfs {
                delegation_token: Some("HDFS_SECRET".to_string()),
            }),
            hugging_face: Some(HuggingFace {
                token: Some("HF_SECRET".to_string()),
                ..Default::default()
            }),
            model_scope: Some(ModelScope {
                token: Some("MS_SECRET".to_string()),
                ..Default::default()
            }),
            request_header: header,
            ..Default::default()
        };

        let download = format!("{:?}", RedactedDownload(&download));
        for secret in [
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "FQoGZXIvYXdzEJr",
            "sec-token-xyz",
            "/etc/gcp/creds.json",
            "HDFS_SECRET",
            "HF_SECRET",
            "MS_SECRET",
            "SECRET_BEARER",
        ] {
            assert!(!download.contains(secret),);
        }
        assert!(download.contains(REDACTED));
        assert!(download.contains("AKIAIOSFODNN7EXAMPLE"),);
        assert!(download.contains("req-42"),);
    }

    #[test]
    fn redacted_download_debug_does_not_mutate_original() {
        let download = Download {
            object_storage: Some(make_object_storage()),
            hugging_face: Some(HuggingFace {
                token: Some("HF_SECRET".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let _ = format!("{:?}", RedactedDownload(&download));
        let os = download.object_storage.as_ref().unwrap();

        assert_eq!(
            os.access_key_secret.as_deref(),
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
        );
        assert_eq!(
            download.hugging_face.as_ref().unwrap().token.as_deref(),
            Some("HF_SECRET")
        );
    }

    #[test]
    fn redacted_persistent_task_request_debug_redacts_object_storage_only() {
        let request = DownloadPersistentTaskRequest {
            object_storage: Some(make_object_storage()),
            ..Default::default()
        };

        let download = format!("{:?}", RedactedDownloadPersistentTaskRequest(&request));
        for secret in [
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "FQoGZXIvYXdzEJr",
            "sec-token-xyz",
            "/etc/gcp/creds.json",
        ] {
            assert!(!download.contains(secret), "leaked {secret:?}: {download}");
        }
        assert!(download.contains(REDACTED));
        assert!(download.contains("AKIAIOSFODNN7EXAMPLE"));
        assert_eq!(
            request
                .object_storage
                .as_ref()
                .unwrap()
                .access_key_secret
                .as_deref(),
            Some("wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")
        );
    }

    #[test]
    fn redacted_download_handles_all_optional_fields_absent() {
        let download = format!("{:?}", RedactedDownload(&Download::default()));
        assert!(!download.contains(REDACTED));
    }
}
