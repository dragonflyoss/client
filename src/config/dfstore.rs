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

use crate::config::*;
use std::path::PathBuf;

// default_dfstore_config_path is the default config path for dfstore.
pub fn default_dfstore_config_path() -> PathBuf {
    default_config_dir().join("dfstore.yaml")
}

// default_dfstore_log_dir is the default log directory for dfstore.
pub fn default_dfstore_log_dir() -> PathBuf {
    default_log_dir().join("dfstore")
}
