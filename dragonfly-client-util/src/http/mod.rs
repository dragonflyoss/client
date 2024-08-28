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

use dragonfly_api::common::v2::Range;
use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Error, Result,
};
use reqwest::header::{HeaderMap, HeaderValue};
use std::collections::HashMap;
use tracing::{error, instrument};

// reqwest_headermap_to_hashmap converts a reqwest headermap to a hashmap.
#[instrument(skip_all)]
pub fn reqwest_headermap_to_hashmap(header: &HeaderMap<HeaderValue>) -> HashMap<String, String> {
    let mut hashmap: HashMap<String, String> = HashMap::new();
    for (k, v) in header {
        let Some(v) = v.to_str().ok() else {
            continue;
        };

        hashmap.entry(k.to_string()).or_insert(v.to_string());
    }

    hashmap
}

// hashmap_to_reqwest_headermap converts a hashmap to a reqwest headermap.
#[instrument(skip_all)]
pub fn hashmap_to_reqwest_headermap(
    header: &HashMap<String, String>,
) -> Result<HeaderMap<HeaderValue>> {
    let header: HeaderMap = (header).try_into().or_err(ErrorType::ParseError)?;
    Ok(header)
}

// hashmap_to_hyper_header_map converts a hashmap to a hyper header map.
#[instrument(skip_all)]
pub fn hashmap_to_hyper_header_map(
    header: &HashMap<String, String>,
) -> Result<hyper::header::HeaderMap> {
    let header: hyper::header::HeaderMap = (header).try_into().or_err(ErrorType::ParseError)?;
    Ok(header)
}

// TODO: Remove the conversion after the http crate version is the same.
// Convert the Reqwest header to the Hyper header, because of the http crate
// version is different. Reqwest header depends on the http crate
// version 0.2, but the Hyper header depends on the http crate version 0.1.
#[instrument(skip_all)]
pub fn hyper_headermap_to_reqwest_headermap(
    hyper_header: &hyper::header::HeaderMap,
) -> reqwest::header::HeaderMap {
    let mut reqwest_header = reqwest::header::HeaderMap::new();
    for (hyper_header_key, hyper_header_value) in hyper_header.iter() {
        let reqwest_header_name: reqwest::header::HeaderName =
            match hyper_header_key.to_string().parse() {
                Ok(reqwest_header_name) => reqwest_header_name,
                Err(err) => {
                    error!("parse header name error: {}", err);
                    continue;
                }
            };

        let reqwest_header_value: reqwest::header::HeaderValue = match hyper_header_value.to_str() {
            Ok(reqwest_header_value) => match reqwest_header_value.parse() {
                Ok(reqwest_header_value) => reqwest_header_value,
                Err(err) => {
                    error!("parse header value error: {}", err);
                    continue;
                }
            },
            Err(err) => {
                error!("parse header value error: {}", err);
                continue;
            }
        };

        reqwest_header.insert(reqwest_header_name, reqwest_header_value);
    }

    reqwest_header
}

// header_vec_to_hashmap converts a vector of header string to a hashmap.
#[instrument(skip_all)]
pub fn header_vec_to_hashmap(raw_header: Vec<String>) -> Result<HashMap<String, String>> {
    let mut header = HashMap::new();
    for h in raw_header {
        let mut parts = h.splitn(2, ':');
        let key = parts.next().unwrap().trim();
        let value = parts.next().unwrap().trim();
        header.insert(key.to_string(), value.to_string());
    }

    Ok(header)
}

// header_vec_to_reqwest_headermap converts a vector of header string to a reqwest headermap.
#[instrument(skip_all)]
pub fn header_vec_to_reqwest_headermap(
    raw_header: Vec<String>,
) -> Result<reqwest::header::HeaderMap> {
    hashmap_to_reqwest_headermap(&header_vec_to_hashmap(raw_header)?)
}

// get_range gets the range from http header.
#[instrument(skip_all)]
pub fn get_range(header: &HeaderMap, content_length: u64) -> Result<Option<Range>> {
    match header.get(reqwest::header::RANGE) {
        Some(range) => {
            let range = range.to_str().or_err(ErrorType::ParseError)?;
            Ok(Some(parse_range_header(range, content_length)?))
        }
        None => Ok(None),
    }
}

// parse_range_header parses a Range header string as per RFC 7233,
// supported Range Header: "Range": "bytes=100-200", "Range": "bytes=-50",
// "Range": "bytes=150-", "Range": "bytes=0-0,-1".
#[instrument(skip_all)]
pub fn parse_range_header(range_header_value: &str, content_length: u64) -> Result<Range> {
    let parsed_ranges =
        http_range_header::parse_range_header(range_header_value).or_err(ErrorType::ParseError)?;
    let valid_ranges = parsed_ranges
        .validate(content_length)
        .or_err(ErrorType::ParseError)?;

    // Not support multiple ranges.
    let valid_range = valid_ranges
        .first()
        .ok_or_else(|| Error::EmptyHTTPRangeError)?;

    let start = valid_range.start().to_owned();
    let length = valid_range.end() - start + 1;
    Ok(Range { start, length })
}
