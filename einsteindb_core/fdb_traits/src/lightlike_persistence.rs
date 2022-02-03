// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::iterable::Iterable;
use crate::peekable::Peekable;
use std::fmt::Debug;

/// A consistent read-only  `into_sync` to create a `SyncLightlikePersistence`.
pub trait LightlikePersistence
where
    Self: 'static + Peekable + Iterable + Send + Sync + Sized + Debug,
{
    fn namespaced_names(&self) -> Vec<&str>;
}
