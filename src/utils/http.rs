/*
 *     Copyright 2023 The Dragonfly Authors
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

use crate::{Error, Result};
use dragonfly_api::common::v2::Range;
use reqwest::header::{HeaderMap, HeaderValue};
use std::collections::HashMap;

// headermap_to_hashmap converts a headermap to a hashmap.
pub fn headermap_to_hashmap(header: &HeaderMap<HeaderValue>) -> HashMap<String, String> {
    let mut hashmap: HashMap<String, String> = HashMap::new();
    for (k, v) in header {
        let Some(v) = v.to_str().ok() else {
            continue;
        };

        hashmap.entry(k.to_string()).or_insert(v.to_string());
    }

    hashmap
}

// hashmap_to_headermap converts a hashmap to a headermap.
pub fn hashmap_to_headermap(header: &HashMap<String, String>) -> Result<HeaderMap<HeaderValue>> {
    let header: HeaderMap = (header).try_into()?;
    Ok(header)
}

// header_vec_to_hashmap converts a vector of header string to a hashmap.
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

// parse_range_header parses a Range header string as per RFC 7233,
// supported Range Header: "Range": "bytes=100-200", "Range": "bytes=-50",
// "Range": "bytes=150-", "Range": "bytes=0-0,-1".
pub fn parse_range_header(range_header_value: &str, content_length: u64) -> Result<Range> {
    let parsed_ranges = http_range_header::parse_range_header(range_header_value)?;
    let valid_ranges = parsed_ranges.validate(content_length)?;

    // Not support multiple ranges.
    let valid_range = valid_ranges.first().ok_or(Error::RangeUnsatisfiableError(
        http_range_header::RangeUnsatisfiableError::Empty,
    ))?;

    let start = valid_range.start().to_owned();
    let length = valid_range.end() - start;
    Ok(Range { start, length })
}
