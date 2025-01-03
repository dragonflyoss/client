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

use std::path::PathBuf;

/// NAME is the name of dfget.
pub const NAME: &str = "dfget";

// DEFAULT_OUTPUT_FILE_MODE defines the default file mode for output files when downloading with dfget
// using the `--transfer-from-dfdaemon=true` option.
pub const DEFAULT_OUTPUT_FILE_MODE: u32 = 0o644;

/// default_dfget_log_dir is the default log directory for dfget.
pub fn default_dfget_log_dir() -> PathBuf {
    crate::default_log_dir().join(NAME)
}
