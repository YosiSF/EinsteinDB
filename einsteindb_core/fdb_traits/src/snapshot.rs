// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::iterable::Iterable;
use crate::peekable::Peekable;
use std::fmt::Debug;

/// A consistent read-only view of the database.
///
/// LightlikePersistences can be sent and shared, but not cloned. To make a lightlike_persistence
/// clonable, call `into_sync` to create a `SyncLightlikePersistence`.
pub trait LightlikePersistence
where
    Self: 'static + Peekable + Iterable + Send + Sync + Sized + Debug,
{
    fn namespaced_names(&self) -> Vec<&str>;
}
