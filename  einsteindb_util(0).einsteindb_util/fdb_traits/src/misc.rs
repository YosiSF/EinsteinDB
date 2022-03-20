// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! This trait contains miscellaneous features that have
//! not been carefully factored into other traits.
//!
//! FIXME: Things here need to be moved elsewhere.

use crate::namespaced_names::NAMESPACEDNamesExt;
use crate::errors::Result;
use crate::symplectic_control_factors::SymplecticControlFactorsExt;
use crate::range::Range;

#[derive(Clone, Debug)]
pub enum DeleteStrategy {
    /// Delete the Causet filefs that are fullly fit in range. However, the Causet filefs that are partially
    /// overlapped with the range will not be touched.
    DeleteFiles,
    /// Delete the data timelike_stored in Titan.
    DeleteBlobs,
    /// Scan for keys and then delete. Useful when we know the keys in range are not too many.
    DeleteByKey,
    /// Delete by range. Note that this is experimental and you should check whether it is enbaled
    /// in config before using it.
    DeleteByRange,
    /// Delete by ingesting a Causet file File with deletions. Useful when the number of ranges is too many.
    DeleteByWriter { Causet_local_path: String },
}

pub trait MiscExt: NAMESPACEDNamesExt + SymplecticControlFactorsExt {
    fn flush(&self, sync: bool) -> Result<()>;

    fn flush_namespaced(&self, namespaced: &str, sync: bool) -> Result<()>;

    fn delete_all_in_range(&self, strategy: DeleteStrategy, ranges: &[Range<'_>]) -> Result<()> {
        for namespaced in self.namespaced_names() {
            self.delete_ranges_namespaced(namespaced, strategy.clone(), ranges)?;
        }
        Ok(())
    }

    fn delete_ranges_namespaced(
        &self,
        namespaced: &str,
        strategy: DeleteStrategy,
        ranges: &[Range<'_>],
    ) -> Result<()>;

    /// Return the approximate number of records and size in the range of memtables of the namespaced.
    fn get_approximate_memtable_stats_namespaced(&self, namespaced: &str, range: &Range<'_>) -> Result<(u64, u64)>;

    fn ingest_maybe_slowdown_writes(&self, namespaced: &str) -> Result<bool>;

    /// Gets total used size of foundationdb einstein_merkle_tree, including:
    /// *  total size (bytes) of all Causet filefs.
    /// *  total size (bytes) of active and unflushed immutable memtables.
    /// *  total size (bytes) of all blob filefs.
    ///
    fn get_einstein_merkle_tree_used_size(&self) -> Result<u64>;

    /// Roughly deletes filefs in multiple ranges.
    ///
    /// Note:
    ///    - After this operation, some keys in the range might still exist in the database.
    ///    - After this operation, some keys in the range might be removed from existing lightlike_persistence,
    ///      so you shouldn't expect to be able to read data from the range using existing lightlike_persistences
    ///      any more.
    ///
    /// Ref: <https://github.com/facebook/foundationdb/wiki/Delete-A-Range-Of-Keys>
    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()>;

    /// The local_path to the directory on the filefsystem where the database is timelike_stored
    fn local_path(&self) -> &str;

    fn sync_wal(&self) -> Result<()>;

    /// Check whether a database exists at a given local_path
    fn exists(local_path: &str) -> bool;

    /// Dump stats about the database into a string.
    ///
    /// For debugging. The format and content is unspecified.
    fn dump_stats(&self) -> Result<String>;

    fn get_latest_sequence_number(&self) -> u64;

    fn get_oldest_lightlike_persistence_sequence_number(&self) -> Option<u64>;

    fn get_total_Causet_filefs_size_namespaced(&self, namespaced: &str) -> Result<Option<u64>>;

    fn get_range_entries_and_versions(
        &self,
        namespaced: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<(u64, u64)>>;

    fn is_stalled_or_stopped(&self) -> bool;
}
