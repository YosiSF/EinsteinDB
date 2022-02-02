// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::iterable::Iterable;
use crate::peekable::Peekable;
use std::fmt::Debug;

/// A consistent read-only view of the database.
///
/// Snapshots can be sent and shared, but not cloned. To make a snapshot
/// clonable, call `into_sync` to create a `SyncSnapshot`.
pub trait Snapshot
where
    Self: 'static + Peekable + Iterable + Send + Sync + Sized + Debug,
{
    fn namespaced_names(&self) -> Vec<&str>;
}
