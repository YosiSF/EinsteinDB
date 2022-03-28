// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::fmt::Debug;
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use crate::*;

pub trait KV:
    Peekable
    + SyncMutable
    + Iterable
    + WriteBatchExt
    + DBOptionsExt
    + NAMESPACEDNamesExt
    + NAMESPACEDOptionsExt
    + ImportExt
    + CausetExt
    + CompactExt
    + RangeGreedoidsExt
    + MvccGreedoidsExt
    + TtlGreedoidsExt
    + TableGreedoidsExt
    + PerfContextExt
    + MiscExt
    + Send
    + Sync
    + Clone
    + Debug
    + Unpin
    + 'static
{
    /// A consistent read-only lightlike_persistence of the database
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
