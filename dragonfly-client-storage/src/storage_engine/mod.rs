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
    /// The namespace of the object.
    const NAMESPACE: &'static str;

    /// serialized serializes the object to bytes.
    fn serialized(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self).or_err(ErrorType::SerializeError)?)
    }

    /// Deserializes the object from bytes.
    fn deserialize_from(bytes: &[u8]) -> Result<Self> {
        Ok(bincode::deserialize(bytes).or_err(ErrorType::SerializeError)?)
    }
}

/// Defines basic storage engine operations.
pub trait StorageEngine<'db>: Operations {}

/// A marker trait to indicate the storage engine is owned.
pub trait StorageEngineOwned: for<'db> StorageEngine<'db> {}
impl<T: for<'db> StorageEngine<'db>> StorageEngineOwned for T {}

/// Defines basic crud operations.
pub trait Operations {
    /// Gets the object by key.
    fn get<O: DatabaseObject>(&self, key: &[u8]) -> Result<Option<O>>;

    /// Gets the objects by keys, returning the values in the order of the keys.
    fn multi_get<O: DatabaseObject>(&self, keys: &[&[u8]]) -> Result<Vec<Option<O>>>;

    /// Checks if the object exists by key.
    fn exists<O: DatabaseObject>(&self, key: &[u8]) -> Result<bool>;

    /// Puts the object by key.
    fn put<O: DatabaseObject>(&self, key: &[u8], value: &O) -> Result<()>;

    /// Deletes the object by key.
    fn delete<O: DatabaseObject>(&self, key: &[u8]) -> Result<()>;

    /// Iterates all objects.
    fn iter<O: DatabaseObject>(&self) -> Result<impl Iterator<Item = Result<(Box<[u8]>, O)>>>;

    /// Iterates all objects without serialization.
    #[allow(clippy::type_complexity)]
    fn iter_raw<O: DatabaseObject>(
        &self,
    ) -> Result<impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>>>;

    /// Iterates all objects with prefix.
    fn prefix_iter<O: DatabaseObject>(
        &self,
        prefix: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Box<[u8]>, O)>>>;

    /// Iterates all objects with prefix without serialization.
    #[allow(clippy::type_complexity)]
    fn prefix_iter_raw<O: DatabaseObject>(
        &self,
        prefix: &[u8],
    ) -> Result<impl Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>>>;

    // batch_delete deletes objects by keys.
    fn batch_delete<O: DatabaseObject>(&self, keys: Vec<&[u8]>) -> Result<()>;
}
