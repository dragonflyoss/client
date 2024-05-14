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

use dragonfly_client_core::{
    error::{ErrorType, OrErr},
    Result,
};
use serde::{de::DeserializeOwned, Serialize};

pub mod rocksdb;

/// DatabaseObject marks a type can be stored in database, which has a namespace.
/// The namespace is used to separate different types of objects, for example
/// column families in rocksdb.
pub trait DatabaseObject: Serialize + DeserializeOwned {
    /// NAMESPACE is the namespace of the object.
    const NAMESPACE: &'static str;

    /// serialized serializes the object to bytes.
    fn serialized(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(self).or_err(ErrorType::SerializeError)?)
    }

    /// deserialize_from deserializes the object from bytes.
    fn deserialize_from(bytes: &[u8]) -> Result<Self> {
        Ok(serde_json::from_slice(bytes).or_err(ErrorType::SerializeError)?)
    }
}

/// StorageEngine defines basic storage engine operations.
pub trait StorageEngine<'db>: Operations {}

/// StorageEngineOwned is a marker trait to indicate the storage engine is owned.
pub trait StorageEngineOwned: for<'db> StorageEngine<'db> {}
impl<T: for<'db> StorageEngine<'db>> StorageEngineOwned for T {}

/// Operations defines basic crud operations.
pub trait Operations {
    /// get gets the object by key.
    fn get<O: DatabaseObject>(&self, key: &[u8]) -> Result<Option<O>>;

    /// put puts the object by key.
    fn put<O: DatabaseObject>(&self, key: &[u8], value: &O) -> Result<()>;

    /// delete deletes the object by key.
    fn delete<O: DatabaseObject>(&self, key: &[u8]) -> Result<()>;

    /// iter iterates all objects.
    fn iter<O: DatabaseObject>(&self) -> Result<impl Iterator<Item = Result<(Box<[u8]>, O)>>>;

    /// prefix_iter iterates all objects with prefix.
    fn prefix_iter<O: DatabaseObject>(
        &self,
        prefix: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Box<[u8]>, O)>>>;
}
