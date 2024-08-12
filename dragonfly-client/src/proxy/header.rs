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

use dragonfly_api::common::v2::Priority;
use reqwest::header::HeaderMap;
use tracing::error;

// DRAGONFLY_TAG_HEADER is the header key of tag in http request.
pub const DRAGONFLY_TAG_HEADER: &str = "X-Dragonfly-Tag";

// DRAGONFLY_APPLICATION_HEADER is the header key of application in http request.
pub const DRAGONFLY_APPLICATION_HEADER: &str = "X-Dragonfly-Application";

// DRAGONFLY_PRIORITY_HEADER is the header key of priority in http request,
// refer to https://github.com/dragonflyoss/api/blob/main/proto/common.proto#L67.
pub const DRAGONFLY_PRIORITY_HEADER: &str = "X-Dragonfly-Priority";

// DRAGONFLY_REGISTRY_HEADER is the header key of custom address of container registry.
pub const DRAGONFLY_REGISTRY_HEADER: &str = "X-Dragonfly-Registry";

// DRAGONFLY_FILTERS_HEADER is the header key of filters in http request,
// it is the filtered query params to generate the task id.
// When filter is "X-Dragonfly-Filtered-Query-Params: Signature,Expires,ns" for example:
// http://example.com/xyz?Expires=e1&Signature=s1&ns=docker.io and http://example.com/xyz?Expires=e2&Signature=s2&ns=docker.io
// will generate the same task id.
// Default value includes the filtered query params of s3, gcs, oss, obs, cos.
pub const DRAGONFLY_FILTERED_QUERY_PARAMS_HEADER: &str = "X-Dragonfly-Filtered-Query-Params";

// get_tag gets the tag from http header.
pub fn get_tag(header: &HeaderMap) -> Option<String> {
    match header.get(DRAGONFLY_TAG_HEADER) {
        Some(tag) => match tag.to_str() {
            Ok(tag) => Some(tag.to_string()),
            Err(err) => {
                error!("get tag from header failed: {}", err);
                None
            }
        },
        None => None,
    }
}

// get_application gets the application from http header.
pub fn get_application(header: &HeaderMap) -> Option<String> {
    match header.get(DRAGONFLY_APPLICATION_HEADER) {
        Some(application) => match application.to_str() {
            Ok(application) => Some(application.to_string()),
            Err(err) => {
                error!("get application from header failed: {}", err);
                None
            }
        },
        None => None,
    }
}

// get_priority gets the priority from http header.
pub fn get_priority(header: &HeaderMap) -> i32 {
    let default_priority = Priority::Level6 as i32;
    match header.get(DRAGONFLY_PRIORITY_HEADER) {
        Some(priority) => match priority.to_str() {
            Ok(priority) => match priority.parse::<i32>() {
                Ok(priority) => priority,
                Err(err) => {
                    error!("parse priority from header failed: {}", err);
                    default_priority
                }
            },
            Err(err) => {
                error!("get priority from header failed: {}", err);
                default_priority
            }
        },
        None => default_priority,
    }
}

// get_registry gets the custom address of container registry from http header.
pub fn get_registry(header: &HeaderMap) -> Option<String> {
    match header.get(DRAGONFLY_REGISTRY_HEADER) {
        Some(registry) => match registry.to_str() {
            Ok(registry) => Some(registry.to_string()),
            Err(err) => {
                error!("get registry from header failed: {}", err);
                None
            }
        },
        None => None,
    }
}

// get_filters gets the filters from http header.
pub fn get_filtered_query_params(
    header: &HeaderMap,
    default_filtered_query_params: Vec<String>,
) -> Vec<String> {
    match header.get(DRAGONFLY_FILTERED_QUERY_PARAMS_HEADER) {
        Some(filters) => match filters.to_str() {
            Ok(filters) => filters.split(',').map(|s| s.to_string()).collect(),
            Err(err) => {
                error!("get filters from header failed: {}", err);
                default_filtered_query_params
            }
        },
        None => default_filtered_query_params,
    }
}
