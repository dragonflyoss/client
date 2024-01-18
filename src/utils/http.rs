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

use crate::Result;
use reqwest::header::{self, HeaderMap, HeaderValue};
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

// get_content_length gets the content length from the header.
pub fn get_content_length(header: &HeaderMap<HeaderValue>) -> Option<u64> {
    header
        .get(header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
}
