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

use std::time::Duration;

use dragonfly_client_core::*;
use error::BackendError;
use opendal::{raw::HttpClient, Metakey, Operator};
use opendal::layers::LoggingLayer;
use tokio_util::io::StreamReader;
use tracing::info;
use url::Url;

use crate::*;

// ParsedURL is a struct that contains the parsed URL, bucket, and path.
#[derive(Debug)]
pub struct ParsedURL {
    // url is the requested URL of the S3.
    url: Url,

    // bucket is the bucket of the S3.
    bucket: String,

    // key is the key of the S3.
    key: String,
}

// ParsedURL implements the ParsedURL trait.
impl ParsedURL {
    // is_dir returns true if the URL path ends with a slash.
    fn is_dir(&self) -> bool {
        self.url.path().ends_with('/')
    }

    // make_url_by_entry_path makes a URL by the entry path when the URL is a directory.
    fn make_url_by_entry_path(&self, entry_path: &str) -> Url {
        let mut url = self.url.clone();
        url.set_path(entry_path);
        url
    }
}

// ParsedURL implements the TryFrom trait for the URL.
//
// The S3 URl should be in the format of `s3://<bucket>/<path>`.
impl TryFrom<Url> for ParsedURL {
    type Error = Error;

    // try_from parses the URL and returns a ParsedURL.
    fn try_from(url: Url) -> std::result::Result<Self, Self::Error> {
        let bucket = url
            .host_str()
            .ok_or_else(|| Error::InvalidURI(url.to_string()))?
            .to_string();

        let key = url
            .path()
            .strip_prefix('/')
            .ok_or_else(|| Error::InvalidURI(url.to_string()))?
            .to_string();

        Ok(Self { 
            url, 
            bucket, 
            key 
        })
    }
}

// S3 is a struct that implements the backend trait. 
#[derive(Default)]
pub struct S3;

// S3 implements the S3 trait.
impl S3 {
    /// Returns S3 that implements the Backend trait.
    pub fn new() -> S3 {
        Self
    }

    // operator initializes the operator with the parsed URL and object storage.
    pub fn operator(
        &self,
        parsed_url: &ParsedURL,
        object_storage: Option<ObjectStorage>,
        timeout: Duration,
    ) -> Result<Operator> {
        // Retrieve the access key ID, access key secret, and session token (optional) from the object storage.
        let Some(ObjectStorage {
            access_key_id,
            access_key_secret,
            session_token, 
        }) = object_storage
        else {
            error!("need access_key_id and access_key_secret");
            return Err(Error::BackendError(BackendError {
                message: "need access_key_id and access_key_secret".to_string(),
                status_code: None,
                header: None,
            }));
        };

        // Create a reqwest http client.
        let client = reqwest::Client::builder().timeout(timeout).build()?;

        // Set up operator builder.
        let mut builder = opendal::services::S3::default();

        builder
            .access_key_id(&access_key_id)
            .secret_access_key(&access_key_secret)
            .http_client(HttpClient::with(client))
            .bucket(&parsed_url.bucket);

        // Configure the session token if it is provided.
        if let Some(token) = session_token.as_deref() {
            builder.security_token(token);
        }

        let operator = Operator::new(builder)
            .map_err(|err| {
                Error::BackendError(BackendError {
                    message: err.to_string(),
                    status_code: None, 
                    header: None,
                })
            })?
            .layer(LoggingLayer::default())
            .finish();

        Ok(operator)
    }
}

// Backend implements the Backend trait.
#[tonic::async_trait]
impl crate::Backend for S3 {
    //head gets the header of the request.
    async fn head(&self, request: HeadRequest) -> Result<HeadResponse> {
        info!(
            "head request {} {}: {:?}",
            request.task_id, request.url, request.http_header
        );

        // Parse the URL and convert it to a ParsedURL for create the S3 operator.
        let url: Url = request
            .url
            .parse()
            .map_err(|_| Error::InvalidURI(request.url.clone()))?;
        let parsed_url: ParsedURL = url.try_into().map_err(|err| {
            error!(
                "parse head request url failed {} {}: {}",
                request.task_id, request.url, err
            );
            err
        })?;

        // Initialize the operator with the parsed URL, object storage, and timeout.
        let operator = self.operator(&parsed_url, request.object_storage, request.timeout)?;

        // Get the entries if url point to a directory.
        let entries = if parsed_url.is_dir() {
            Some(
                operator
                    .list_with(&parsed_url.key)
                    .recursive(true)
                    .metakey(Metakey::ContentLength | Metakey::Mode)
                    .await // Do the list op here.
                    .map_err(|err| {
                        error!(
                            "list request failed {} {}: {}",
                            request.task_id, request.url, err
                        );
                        Error::BackendError(BackendError {
                            message: err.to_string(),
                            status_code: None,
                            header: None,
                        })
                    })?
                    .into_iter()
                    .map(|entry| {
                        let metadata = entry.metadata();
                        DirEntry {
                            url: parsed_url.make_url_by_entry_path(entry.path()).to_string(),
                            content_length: metadata.content_length() as usize,
                            is_dir: metadata.is_dir(),
                        }
                    })
                    .collect(),
            )
        } else {
            None
        };

        // Stat the object to get the response from the S3.
        let response = operator.stat_with(&parsed_url.key).await.map_err(|err| {
            error!(
                "stat request failed {} {}: {}",
                request.task_id, request.url, err
            );
            Error::BackendError(BackendError {
                message: err.to_string(),
                status_code: None,
                header: None,
            })
        })?;

        info!(
            "head response {} {}: {}",
            request.task_id,
            request.url,
            response.content_length()
        );

        Ok(HeadResponse {
            success: true,
            content_length: Some(response.content_length()),
            http_header: None,
            http_status_code: None,
            error_message: None,
            entries,
        })
    }

    // Returns content of requested file.
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>> {
        info!(
            "get request {} {}: {:?}",
            request.piece_id, request.url, request.http_header
        );

        // Parse the URL and convert it to a ParsedURL for create the S3 operator.
        let url: Url = request
            .url
            .parse()
            .map_err(|_| Error::InvalidURI(request.url.clone()))?;
        let parsed_url: ParsedURL = url.try_into().map_err(|err| {
            error!(
                "parse head request url failed {} {}: {}",
                request.piece_id, request.url, err
            );
            err
        })?;

        // Initialize the operator with the parsed URL, object storage, and timeout.
        let operator_reader = self
            .operator(&parsed_url, request.object_storage, request.timeout)?
            .reader(&parsed_url.key)
            .await
            .map_err(|err| {
                error!(
                    "get request failed {} {}: {}",
                    request.piece_id, request.url, err
                );
                Error::BackendError(BackendError {
                    message: err.to_string(),
                    status_code: None,
                    header: None,
                })
            })?;

            let stream = match request.range {
                Some(range) => operator_reader
                    .into_bytes_stream(range.start..range.start + range.length)
                    .await
                    .map_err(|err| {
                        error!(
                            "get request failed {} {}: {}",
                            request.piece_id, request.url, err
                        );
                        Error::BackendError(BackendError {
                            message: err.to_string(),
                            status_code: None,
                            header: None,
                        })
                    })?,
                None => operator_reader.into_bytes_stream(..).await.map_err(|err| {
                    error!(
                        "get request failed {} {}: {}",
                        request.piece_id, request.url, err
                    );
                    Error::BackendError(BackendError {
                        message: err.to_string(),
                        status_code: None,
                        header: None,
                    })
                })?,
            };

        Ok(crate::GetResponse {
            success: true,
            http_header: None,
            http_status_code: Some(reqwest::StatusCode::OK),
            reader: Box::new(StreamReader::new(stream)),
            error_message: None,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use url::Url;

    #[test]
    fn test_valid_s3_url() {
        let url = Url::parse("s3://my-bucket/path/to/object.txt").unwrap();
        let config = Config::try_from(url).unwrap();
        assert_eq!(config.bucket, "my-bucket");
        assert_eq!(config.path, "path/to/object.txt");
    }
}
