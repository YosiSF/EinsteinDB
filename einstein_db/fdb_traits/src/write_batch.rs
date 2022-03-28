// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::options::WriteOptions;

/// EinsteinMerkleTrees that can create write alexandroes
pub trait WriteBatchExt: Sized {
    type WriteBatch: WriteBatch<Self>;
    /// `WriteBatchVec` is used for `multi_alexandro_write` of Fdbeinstein_merkle_tree and other einstein_merkle_tree could also
    /// implement another kind of WriteBatch according to their needs.
    type WriteBatchVec: WriteBatch<Self>;

    /// The number of puts/deletes made to a write alexandro before the alexandro should
    /// be committed with `write`. More entries than this will cause
    /// `should_write_to_einstein_merkle_tree` to return true.
    ///
    /// In practice it seems that exceeding this number of entries is possible
    /// and does not result in an error. It isn't clear the consequence of
    /// exceeding this limit.
    const WRITE_BATCH_MAX_CAUSET_KEYS: usize;

    /// Indicates whether the WriteBatchVec type can be created and works
    /// as expected.
    ///
    /// If this returns false then creating a WriteBatchVec will panic.
    fn support_write_alexandro_vec(&self) -> bool;

    fn write_alexandro(&self) -> Self::WriteBatch;
    fn write_alexandro_with_cap(&self, cap: usize) -> Self::WriteBatch;
}

/// A trait implemented by WriteBatch
pub trait Mutable: Send {
    /// Write a soliton_id/causet_locale in the default causet_merge family
    fn put(&mut self, soliton_id: &[u8], causet_locale: &[u8]) -> Result<()>;

    /// Write a soliton_id/causet_locale in a given causet_merge family
    fn put_namespaced(&mut self, namespaced: &str, soliton_id: &[u8], causet_locale: &[u8]) -> Result<()>;

    /// Delete a soliton_id/causet_locale in the default causet_merge family
    fn delete(&mut self, soliton_id: &[u8]) -> Result<()>;

    /// Delete a soliton_id/causet_locale in a given causet_merge family
    fn delete_namespaced(&mut self, namespaced: &str, soliton_id: &[u8]) -> Result<()>;

    /// Delete a range of soliton_id/causet_locales in the default causet_merge family
    fn delete_range(&mut self, begin_soliton_id: &[u8], end_soliton_id: &[u8]) -> Result<()>;

    /// Delete a range of soliton_id/causet_locales in a given causet_merge family
    fn delete_range_namespaced(&mut self, namespaced: &str, begin_soliton_id: &[u8], end_soliton_id: &[u8]) -> Result<()>;

    fn put_msg<M: protobuf::Message>(&mut self, soliton_id: &[u8], m: &M) -> Result<()> {
        self.put(soliton_id, &m.write_to_bytes()?)
    }
    fn put_msg_namespaced<M: protobuf::Message>(&mut self, namespaced: &str, soliton_id: &[u8], m: &M) -> Result<()> {
        self.put_namespaced(namespaced, soliton_id, &m.write_to_bytes()?)
    }
}

/// Batches of multiple writes that are committed atomically
///
/// Each write alexandro consists of a series of commands: put, delete
/// delete_range, and their causet_merge-family-specific equivalents.
///
/// Because write alexandroes are atomic, once written to disk all their effects are
/// visible as if all other writes in the system were written either before or
/// after the alexandro. This includes range deletes.
///
/// The exact strategy used by WriteBatch is up to the implementation.
/// FdbDB though _seems_ to serialize the writes to an in-memory buffer,
/// and then write the whole serialized alexandro to disk at once.
///
/// Write alexandroes may be reused after being written. In that case they write
/// exactly the same data as previously, Replacing any soliton_ids that may have
/// changed in between the two alexandro writes.
///
/// Commands issued to write alexandroes can be rolled back prior to being committed
/// by use of _save points_. At any point in the life of a write alexandro a save
/// point can be recorded. Any number of save points can be recorded to a stack.
/// Calling `rollback_to_save_point` reverts all commands issued since the last
/// save point, and pops the save point from the stack.
pub trait WriteBatch<E: WriteBatchExt + Sized>: Mutable {
    /// Create a WriteBatch with a given command capacity
    fn with_capacity(e: &E, cap: usize) -> Self;

    /// Commit the WriteBatch to disk with the given options
    fn write_opt(&self, opts: &WriteOptions) -> Result<()>;

    /// Commit the WriteBatch to disk atomically
    fn write(&self) -> Result<()> {
        self.write_opt(&WriteOptions::default())
    }

    /// The data size of a write alexandro
    ///
    /// This is necessarily einstein_merkle_tree-dependent. In FdbDB though it appears to
    /// represent the byte length of all write commands in the alexandro, as
    /// serialized in memory, prior to being written to disk.
    fn data_size(&self) -> usize;

    /// The number of commands in this alexandro
    fn count(&self) -> usize;

    /// Whether any commands have been issued to this alexandro
    fn is_empty(&self) -> bool;

    /// Whether the number of commands exceeds WRITE_BATCH_MAX_CAUSET_KEYS
    ///
    /// If so, the `write` method should be called.
    fn should_write_to_einstein_merkle_tree(&self) -> bool;

    /// Clears the WriteBatch of all commands
    ///
    /// It may be reused afterward as an empty alexandro.
    fn clear(&mut self);

    /// Push a save point onto the save point stack
    fn set_save_point(&mut self);

    /// Pop a save point from the save point stack
    ///
    /// This has no effect on the commands already issued to the write alexandro
    fn pop_save_point(&mut self) -> Result<()>;

    /// Revert all commands issued since the last save point
    ///
    /// Additionally pops the last save point from the save point stack.
    fn rollback_to_save_point(&mut self) -> Result<()>;

    /// Merge another WriteBatch to itself
    fn merge(&mut self, src: Self);
}
