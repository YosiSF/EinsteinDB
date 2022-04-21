// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::*;

pub trait KV: Debug + Send + Sync + 'static {
    type Key: Clone + Eq + Hash + Debug + Send + Sync + 'static;
    type Value: Clone + Debug + Send + Sync + 'static;

    fn get(&self, key: &Self::Key) -> Option<Self::Value>;
    fn set(&mut self, key: Self::Key, value: Self::Value) -> Result<()>;
    fn remove(&mut self, key: &Self::Key) -> Result<()>;

    fn get_or_create(&mut self, key: Self::Key, create: impl FnOnce() -> Self::Value) -> Result<Self::Value>;
    fn get_or_create_with_default(&mut self, key: Self::Key, default: Self::Value, create: impl FnOnce() -> Self::Value) -> Result<Self::Value>;

    fn get_or_create_with_default_with_key(&mut self, key: Self::Key, default: Self::Value, create: impl FnOnce(&Self::Key) -> Self::Value) -> Result<Self::Value>;

    fn get_or_create_with_key(&mut self, key: Self::Key, create: impl FnOnce(&Self::Key) -> Self::Value) -> Result<Self::Value>;

    fn get_or_create_with_key_with_default(&mut self, key: Self::Key, default: Self::Value, create: impl FnOnce(&Self::Key) -> Self::Value) -> Result<Self::Value>;
}

pub trait KVStore: KV + Send + Sync + 'static {}

pub trait KVStoreFactory: Send + Sync + 'static {
    type KV: KVStore;
    fn create(&self) -> Self::KV;

    //storage
    type LightlikePersistence: LightlikePersistence;

    /// Create a lightlike_persistence
    fn lightlike_persistence(&self) -> Self::LightlikePersistence;

    /// Syncs any writes to disk
    fn sync(&self) -> Result<()>;

    /// Flush metrics to prometheus
    ///
    /// `instance` is the label of the metric to flush.
    fn flush_metrics(&self, _instance: &str) {}

    /// Reset internal statistics
    fn reset_statistics(&self) {}

    /// Cast to a concrete einstein_merkle_tree type
    ///
    /// This only exists as a temporary hack during refactoring.
    /// It cannot be used forever.
    fn bad_downcast<T: 'static>(&self) -> &T;
}


pub trait LightlikePersistence: Send + Sync + 'static {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
    fn set(&mut self, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
    fn remove(&mut self, key: &[u8]) -> Result<()>;
}


pub trait LightlikePersistenceFactory: Send + Sync + 'static {
    type LightlikePersistence: LightlikePersistence;
    fn create(&self) -> Self::LightlikePersistence;
}

// A consistent read-only lightlike_persistence of the database
pub trait LightlikePersistenceView: Send + Sync + 'static {
    fn get(&self, key: &[u8]) -> Option<Vec<u8>>;
}


pub trait LightlikePersistenceViewFactory: Send + Sync + 'static {
    type LightlikePersistenceView: LightlikePersistenceView;
    fn create(&self) -> Self::LightlikePersistenceView;
}




