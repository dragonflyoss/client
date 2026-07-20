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

/// Debug implementation that redacts sensitive fields without cloning the
/// whole download, since it runs on the per-request log path.
impl fmt::Debug for RedactedDownload<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Destructure exhaustively, so adding a field to `Download` breaks this
        // impl at compile time instead of silently dropping the field from logs.
        let Download {
            url,
            digest,
            range,
            r#type,
            tag,
            application,
            priority,
            filtered_query_params,
            request_header,
            piece_length,
            output_path,
            timeout,
            disable_back_to_source,
            need_back_to_source,
            certificate_chain,
            prefetch,
            object_storage,
            hdfs,
            is_prefetch,
            need_piece_content,
            force_hard_link,
            content_for_calculating_task_id,
            remote_ip,
            concurrent_piece_count,
            overwrite,
            actual_piece_length,
            actual_content_length,
            actual_piece_count,
            enable_task_id_based_blob_digest,
            hugging_face,
            model_scope,
            hit_local_cache,
        } = self.0;

        f.debug_struct("Download")
            .field("url", url)
            .field("digest", digest)
            .field("range", range)
            .field("type", r#type)
            .field("tag", tag)
            .field("application", application)
            .field("priority", priority)
            .field("filtered_query_params", filtered_query_params)
            .field("request_header", &RedactedHeaderMap(request_header))
            .field("piece_length", piece_length)
            .field("output_path", output_path)
            .field("timeout", timeout)
            .field("disable_back_to_source", disable_back_to_source)
            .field("need_back_to_source", need_back_to_source)
            .field("certificate_chain", certificate_chain)
            .field("prefetch", prefetch)
            .field(
                "object_storage",
                &scrubbed(object_storage, scrub_object_storage),
            )
            .field("hdfs", &scrubbed(hdfs, scrub_hdfs))
            .field("is_prefetch", is_prefetch)
            .field("need_piece_content", need_piece_content)
            .field("force_hard_link", force_hard_link)
            .field(
                "content_for_calculating_task_id",
                content_for_calculating_task_id,
            )
            .field("remote_ip", remote_ip)
            .field("concurrent_piece_count", concurrent_piece_count)
            .field("overwrite", overwrite)
            .field("actual_piece_length", actual_piece_length)
            .field("actual_content_length", actual_content_length)
            .field("actual_piece_count", actual_piece_count)
            .field(
                "enable_task_id_based_blob_digest",
                enable_task_id_based_blob_digest,
            )
            .field("hugging_face", &scrubbed(hugging_face, scrub_hugging_face))
            .field("model_scope", &scrubbed(model_scope, scrub_model_scope))
            .field("hit_local_cache", hit_local_cache)
            .finish()
    }
}

/// Clones the optional sub-message only when present and scrubs its secrets,
/// so the common absent case costs nothing.
fn scrubbed<T: Clone>(value: &Option<T>, scrub: fn(&mut Option<T>)) -> Option<T> {
    let mut value = value.clone();
    scrub(&mut value);
    value
}

/// Debug wrapper over the request header map that prints sensitive header
/// values as [`REDACTED`] without cloning the map.
struct RedactedHeaderMap<'a>(&'a HashMap<String, String>);

/// Debug implementation that redacts sensitive header values.
impl fmt::Debug for RedactedHeaderMap<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.0.iter().map(|(key, value)| {
                if is_sensitive_header(key) {
                    (key, REDACTED)
                } else {
                    (key, value.as_str())
                }
            }))
            .finish()
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

/// Returns whether the header carries secret material and must be redacted,
/// matched case-insensitively without allocating.
fn is_sensitive_header(key: &str) -> bool {
    [
        AUTHORIZATION.as_str(),
        PROXY_AUTHORIZATION.as_str(),
        COOKIE.as_str(),
        SET_COOKIE.as_str(),
        "x-amz-security-token",
        "x-oss-security-token",
        "x-auth-token",
        "x-api-key",
        "x-goog-api-key",
    ]
    .iter()
    .any(|sensitive| key.eq_ignore_ascii_case(sensitive))
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
    fn is_sensitive_header_matches_standard_auth_headers_case_insensitively() {
        assert!(is_sensitive_header("Authorization"));
        assert!(is_sensitive_header("PROXY-AUTHORIZATION"));
        assert!(is_sensitive_header("cookie"));
        assert!(is_sensitive_header("Set-Cookie"));
    }

    #[test]
    fn is_sensitive_header_matches_object_storage_token_headers() {
        assert!(is_sensitive_header("x-amz-security-token"));
        assert!(is_sensitive_header("X-OSS-Security-Token"));
        assert!(is_sensitive_header("X-Auth-Token"));
        assert!(is_sensitive_header("x-api-key"));
        assert!(is_sensitive_header("X-Goog-Api-Key"));
    }

    #[test]
    fn is_sensitive_header_ignores_non_sensitive_headers() {
        assert!(!is_sensitive_header("Content-Type"));
        assert!(!is_sensitive_header("User-Agent"));
        assert!(!is_sensitive_header("X-Request-Id"));
        assert!(!is_sensitive_header("X-Custom-Author"));
    }

    #[test]
    fn redacted_header_map_redacts_values_and_preserves_keys() {
        let mut h = HashMap::new();
        h.insert("Authorization".to_string(), "Bearer x".to_string());
        h.insert("Content-Type".to_string(), "text/plain".to_string());
        let out = format!("{:?}", RedactedHeaderMap(&h));

        assert!(out.contains("Authorization"));
        assert!(out.contains(REDACTED));
        assert!(!out.contains("Bearer x"));
        assert!(out.contains("Content-Type"));
        assert!(out.contains("text/plain"));
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
