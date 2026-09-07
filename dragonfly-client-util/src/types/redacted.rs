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

use dragonfly_api::common::v2::{Download, Hdfs, HuggingFace, ModelScope, ObjectStorage, OpenCsg};
use dragonfly_api::dfdaemon::v2::DownloadPersistentTaskRequest;
use http::header::{HOST, RANGE, USER_AGENT};
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
/// Only the key fields are printed, since it runs on the per-request log
/// path. `request_header` prints only the key headers, bulky fields such
/// as `certificate_chain` and `content_for_calculating_task_id` are omitted
/// entirely, and the secrets of the printed backend configs are
/// replaced with [`REDACTED`]:
///
/// - `object_storage.access_key_secret`
/// - `object_storage.session_token`
/// - `object_storage.security_token`
/// - `object_storage.credential_path`  (GCS OAuth2 credential file path)
/// - `hdfs.delegation_token`
/// - `hugging_face.token`
/// - `model_scope.token`
/// - `open_csg.token`
///
/// Non-secret identifiers such as `object_storage.access_key_id` are left
/// intact because an AKIA/ASIA-style ID on its own is not sufficient to
/// authenticate and is commonly used for request correlation in operator
/// logs.
pub struct RedactedDownload<'a>(pub &'a Download);

/// Debug implementation that prints the key fields without cloning the
/// whole download, since it runs on the per-request log path.
impl fmt::Debug for RedactedDownload<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Destructure exhaustively, so adding a field to `Download` breaks this
        // impl at compile time instead of silently dropping the field from logs.
        let Download {
            url,
            range,
            tag,
            application,
            piece_length,
            request_header,
            output_path,
            prefetch,
            object_storage,
            hdfs,
            is_prefetch,
            remote_ip,
            hugging_face,
            model_scope,
            open_csg,
            digest: _,
            r#type: _,
            priority: _,
            filtered_query_params: _,
            timeout: _,
            disable_back_to_source: _,
            need_back_to_source: _,
            scheduling_policy: _,
            certificate_chain: _,
            need_piece_content: _,
            force_hard_link: _,
            content_for_calculating_task_id: _,
            concurrent_piece_count: _,
            overwrite: _,
            actual_piece_length: _,
            actual_content_length: _,
            actual_piece_count: _,
            enable_task_id_based_blob_digest: _,
            metadata_only: _,
        } = self.0;

        f.debug_struct("Download")
            .field("url", url)
            .field("range", range)
            .field("tag", tag)
            .field("application", application)
            .field("piece_length", piece_length)
            .field("output_path", output_path)
            .field("prefetch", prefetch)
            .field("is_prefetch", is_prefetch)
            .field("remote_ip", remote_ip)
            .field("request_header", &KeyRequestHeader(request_header))
            .field(
                "object_storage",
                &scrubbed(object_storage, scrub_object_storage),
            )
            .field("hdfs", &scrubbed(hdfs, scrub_hdfs))
            .field("hugging_face", &scrubbed(hugging_face, scrub_hugging_face))
            .field("model_scope", &scrubbed(model_scope, scrub_model_scope))
            .field("open_csg", &scrubbed(open_csg, scrub_open_csg))
            .finish_non_exhaustive()
    }
}

/// Clones the optional sub-message only when present and scrubs its secrets,
/// so the common absent case costs nothing.
fn scrubbed<T: Clone>(value: &Option<T>, scrub: fn(&mut Option<T>)) -> Option<T> {
    let mut value = value.clone();
    scrub(&mut value);
    value
}

/// Debug wrapper over the request header map that prints only the key
/// headers, so the hot log line stays small and the sensitive headers never
/// reach the logs.
struct KeyRequestHeader<'a>(&'a HashMap<String, String>);

/// Debug implementation that filters the headers by the key header names.
impl fmt::Debug for KeyRequestHeader<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_map()
            .entries(self.0.iter().filter(|(key, _)| is_key_request_header(key)))
            .finish()
    }
}

/// Returns whether the header is worth printing on the per-request log path,
/// matched case-insensitively without allocating.
fn is_key_request_header(key: &str) -> bool {
    [
        RANGE.as_str(),
        USER_AGENT.as_str(),
        HOST.as_str(),
        "x-request-id",
    ]
    .iter()
    .any(|key_header| key.eq_ignore_ascii_case(key_header))
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
/// Only the key fields are printed, and the secrets of the printed
/// `object_storage` are replaced with [`REDACTED`] when present:
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

/// Debug implementation that prints the key fields without cloning the
/// whole request.
impl fmt::Debug for RedactedDownloadPersistentTaskRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Destructure exhaustively, so adding a field to the request breaks
        // this impl at compile time instead of silently dropping the field
        // from logs.
        let DownloadPersistentTaskRequest {
            url,
            persistent,
            output_path,
            remote_ip,
            object_storage,
            // The remaining fields are omitted to keep the log line small.
            timeout: _,
            need_piece_content: _,
            force_hard_link: _,
            digest: _,
            overwrite: _,
        } = self.0;

        f.debug_struct("DownloadPersistentTaskRequest")
            .field("url", url)
            .field("persistent", persistent)
            .field("output_path", output_path)
            .field("remote_ip", remote_ip)
            .field(
                "object_storage",
                &scrubbed(object_storage, scrub_object_storage),
            )
            .finish_non_exhaustive()
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

/// Mutates the given `OpenCsg` by replacing any present token with [`REDACTED`].
fn scrub_open_csg(csg: &mut Option<OpenCsg>) {
    if let Some(csg) = csg.as_mut() {
        if csg.token.is_some() {
            csg.token = Some(REDACTED.to_string());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type ExpectObjectStorage = fn(Option<ObjectStorage>);
    type ScrubToken = fn() -> Option<String>;

    const ACCESS_KEY_SECRET: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

    fn object_storage() -> ObjectStorage {
        ObjectStorage {
            access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
            access_key_secret: Some(ACCESS_KEY_SECRET.to_string()),
            session_token: Some("FQoGZXIvYXdzEJr...".to_string()),
            security_token: Some("sec-token-xyz".to_string()),
            credential_path: Some("/etc/gcp/creds.json".to_string()),
            ..Default::default()
        }
    }

    fn header(name: &str, value: &str) -> HashMap<String, String> {
        HashMap::from([(name.to_string(), value.to_string())])
    }

    #[test]
    fn scrub_object_storage_redacts_only_present_secrets() {
        let test_cases: Vec<(Option<ObjectStorage>, ExpectObjectStorage)> = vec![
            (Some(object_storage()), |object_storage| {
                let object_storage = object_storage.unwrap();
                assert_eq!(object_storage.access_key_secret.as_deref(), Some(REDACTED));
                assert_eq!(object_storage.session_token.as_deref(), Some(REDACTED));
                assert_eq!(object_storage.security_token.as_deref(), Some(REDACTED));
                assert_eq!(object_storage.credential_path.as_deref(), Some(REDACTED));
                assert_eq!(
                    object_storage.access_key_id.as_deref(),
                    Some("AKIAIOSFODNN7EXAMPLE")
                );
            }),
            (
                Some(ObjectStorage {
                    access_key_id: Some("AKIA...".to_string()),
                    ..Default::default()
                }),
                |object_storage| {
                    let object_storage = object_storage.unwrap();
                    assert!(object_storage.access_key_secret.is_none());
                    assert!(object_storage.session_token.is_none());
                    assert!(object_storage.security_token.is_none());
                    assert!(object_storage.credential_path.is_none());
                    assert_eq!(object_storage.access_key_id.as_deref(), Some("AKIA..."));
                },
            ),
            (None, |object_storage| assert!(object_storage.is_none())),
        ];

        for (mut object_storage, expect) in test_cases {
            scrub_object_storage(&mut object_storage);
            expect(object_storage);
        }
    }

    #[test]
    fn scrub_backends_redact_only_present_tokens() {
        let test_cases: Vec<(ScrubToken, Option<&str>)> = vec![
            (
                || {
                    let mut hdfs = Some(Hdfs {
                        delegation_token: Some("super-secret-token".to_string()),
                    });
                    scrub_hdfs(&mut hdfs);
                    hdfs.unwrap().delegation_token
                },
                Some(REDACTED),
            ),
            (
                || {
                    let mut hdfs = Some(Hdfs {
                        delegation_token: None,
                    });
                    scrub_hdfs(&mut hdfs);
                    hdfs.unwrap().delegation_token
                },
                None,
            ),
            (
                || {
                    let mut hugging_face = Some(HuggingFace {
                        token: Some("hf_xxxxx".to_string()),
                        ..Default::default()
                    });
                    scrub_hugging_face(&mut hugging_face);
                    hugging_face.unwrap().token
                },
                Some(REDACTED),
            ),
            (
                || {
                    let mut hugging_face = Some(HuggingFace::default());
                    scrub_hugging_face(&mut hugging_face);
                    hugging_face.unwrap().token
                },
                None,
            ),
            (
                || {
                    let mut model_scope = Some(ModelScope {
                        token: Some("ms_xxxxx".to_string()),
                        ..Default::default()
                    });
                    scrub_model_scope(&mut model_scope);
                    model_scope.unwrap().token
                },
                Some(REDACTED),
            ),
            (
                || {
                    let mut model_scope = Some(ModelScope::default());
                    scrub_model_scope(&mut model_scope);
                    model_scope.unwrap().token
                },
                None,
            ),
            (
                || {
                    let mut open_csg = Some(OpenCsg {
                        token: Some("csg_xxxxx".to_string()),
                        ..Default::default()
                    });
                    scrub_open_csg(&mut open_csg);
                    open_csg.unwrap().token
                },
                Some(REDACTED),
            ),
            (
                || {
                    let mut open_csg = Some(OpenCsg::default());
                    scrub_open_csg(&mut open_csg);
                    open_csg.unwrap().token
                },
                None,
            ),
        ];

        for (scrub, expected) in test_cases {
            assert_eq!(scrub().as_deref(), expected);
        }
    }

    #[test]
    fn is_key_request_header_matches_case_insensitively() {
        let test_cases = vec![
            ("Range", true),
            ("RANGE", true),
            ("user-agent", true),
            ("Host", true),
            ("X-Request-Id", true),
            ("x-request-id", true),
            ("Accept", false),
            ("Authorization", false),
            ("", false),
        ];

        for (name, expected) in test_cases {
            assert_eq!(is_key_request_header(name), expected);
        }
    }

    #[test]
    fn redacted_download_debug_does_not_leak_secrets() {
        let download = Download {
            object_storage: Some(object_storage()),
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
            open_csg: Some(OpenCsg {
                token: Some("CSG_SECRET".to_string()),
                ..Default::default()
            }),
            request_header: HashMap::from([
                (
                    "Authorization".to_string(),
                    "Bearer SECRET_BEARER".to_string(),
                ),
                ("X-Request-Id".to_string(), "req-42".to_string()),
            ]),
            ..Default::default()
        };

        let output = format!("{:?}", RedactedDownload(&download));
        for secret in [
            ACCESS_KEY_SECRET,
            "FQoGZXIvYXdzEJr",
            "sec-token-xyz",
            "/etc/gcp/creds.json",
            "HDFS_SECRET",
            "HF_SECRET",
            "MS_SECRET",
            "CSG_SECRET",
            "SECRET_BEARER",
        ] {
            assert!(!output.contains(secret));
        }

        assert!(output.contains(REDACTED));
        assert!(output.contains("AKIAIOSFODNN7EXAMPLE"));
        assert!(output.contains("req-42"));
        assert!(!output.contains("Authorization"));
    }

    #[test]
    fn redacted_download_debug_prints_only_key_request_headers() {
        let test_cases = vec![
            ("Range", "bytes=0-1023", true),
            ("User-Agent", "dfget/2.3", true),
            ("host", "registry.example.com", true),
            ("X-Request-Id", "req-42", true),
            ("Accept", "application/octet-stream", false),
            ("Authorization", "Bearer SECRET_BEARER", false),
        ];

        for (name, value, expected) in test_cases {
            let download = Download {
                request_header: header(name, value),
                ..Default::default()
            };

            let output = format!("{:?}", RedactedDownload(&download));
            assert_eq!(output.contains(name), expected);
            assert_eq!(output.contains(value), expected);
        }
    }

    #[test]
    fn redacted_download_debug_does_not_mutate_the_original() {
        let download = Download {
            object_storage: Some(object_storage()),
            hugging_face: Some(HuggingFace {
                token: Some("HF_SECRET".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let _ = format!("{:?}", RedactedDownload(&download));

        let object_storage = download.object_storage.as_ref().unwrap();
        assert_eq!(
            object_storage.access_key_secret.as_deref(),
            Some(ACCESS_KEY_SECRET)
        );
        assert_eq!(
            download.hugging_face.as_ref().unwrap().token.as_deref(),
            Some("HF_SECRET")
        );
    }

    #[test]
    fn redacted_download_debug_omits_redacted_without_secrets() {
        let output = format!("{:?}", RedactedDownload(&Download::default()));
        assert!(!output.contains(REDACTED));
    }

    #[test]
    fn redacted_persistent_task_request_debug_redacts_object_storage_only() {
        let request = DownloadPersistentTaskRequest {
            object_storage: Some(object_storage()),
            ..Default::default()
        };

        let output = format!("{:?}", RedactedDownloadPersistentTaskRequest(&request));
        for secret in [
            ACCESS_KEY_SECRET,
            "FQoGZXIvYXdzEJr",
            "sec-token-xyz",
            "/etc/gcp/creds.json",
        ] {
            assert!(!output.contains(secret));
        }

        assert!(output.contains(REDACTED));
        assert!(output.contains("AKIAIOSFODNN7EXAMPLE"));
        assert_eq!(
            request
                .object_storage
                .as_ref()
                .unwrap()
                .access_key_secret
                .as_deref(),
            Some(ACCESS_KEY_SECRET)
        );
    }
}
