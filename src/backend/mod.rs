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

mod http;

pub enum BackendConfig {
    HTTP(http::HTTPConfig),
}

// Error is the error for Backend.
#[derive(Debug, thiserror::Error)]
pub enum Error {}

// Result is the result for Backend.
pub type Result<T> = std::result::Result<T, Error>;

// Backend is the interface for backend.
pub trait Backend {
    // get gets the content of the key.
    fn get(&self) -> Result<Vec<u8>>;
}

pub struct BackendFactory {}

impl BackendFactory {
    pub fn new(config: BackendConfig) -> Box<dyn Backend> {
        match config {
            BackendConfig::HTTP(config) => Box::new(http::HTTPBackend::new(config)),
        }
    }
}
