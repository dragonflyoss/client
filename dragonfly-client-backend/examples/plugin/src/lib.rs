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

use dragonfly_client_backend::{Backend, Body, GetRequest, GetResponse, StatRequest, StatResponse};
use dragonfly_client_core::{Error, Result};

/// Hdfs is a struct that implements the Backend trait
struct Hdfs;

/// Hdfs implements the Backend trait
impl Hdfs {
    pub fn new() -> Self {
        Self {}
    }
}

/// Implement the Backend trait for Hdfs.
#[tonic::async_trait]
impl Backend for Hdfs {
    /// scheme returns the scheme of the backend.
    fn scheme(&self) -> String {
        "hdfs".to_string()
    }

    /// stat gets the metadata from the backend.
    async fn stat(&self, request: StatRequest) -> Result<StatResponse> {
        println!("HDFS stat url: {}", request.url);
        Err(Error::Unimplemented)
    }

    /// get gets the content from the backend.
    async fn get(&self, request: GetRequest) -> Result<GetResponse<Body>> {
        println!("HDFS get url: {}", request.url);
        Err(Error::Unimplemented)
    }
}

/// register_plugin is a function that returns a Box<dyn Backend + Send + Sync>.
/// This function is used to register the HDFS plugin to the Backend.
#[no_mangle]
pub fn register_plugin() -> Box<dyn Backend + Send + Sync> {
    Box::new(Hdfs::new())
}
