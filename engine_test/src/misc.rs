// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! This trait contains miscellaneous features that have
//! not been carefully factored into other traits.
//!
//! FIXME: Things here need to be moved elsewhere.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::{cmp, u64};
use einstein_db_alexandrov_processing::{
    index::{
        Index,
        IndexIterator,
        IndexIteratorOptions,
        IndexIteratorOptionsBuilder,
    },
};

use fdb_traits::Result;
use fdb_traits::FdbTrait;




use fdb_traits::Result;


#[derive(Debug, Clone)]
pub struct CompactOptions {
    pub causetq_upstream_interlock_threshold: u64,
    pub causetq_upstream_interlock_compaction_interval: u64,
    pub causetq_upstream_interlock_compaction_threshold: u64,
    pub block_size: u64,
    pub block_cache_size: u64,
    pub block_cache_shard_bits: u8,
    pub enable_bloom_filter: bool,
    pub enable_indexing: bool,
    pub index_block_size: u64,
    pub index_block_cache_size: u64,
    pub index_block_cache_shard_bits: u8,
    pub index_block_restart_interval: u64,
    pub compression_type: String,
    pub compression_level: i32,
    pub compression_block_size: u64,
    pub compression_strategy: String,
    pub compression_dict: Vec<u8>,
    pub enable_statistics: bool,
    pub statistics_interval: u64,
    pub statistics_block_size: u64,
    pub statistics_block_cache_size: u64,
    pub statistics_block_cache_shard_bits: u8,
    pub statistics_block_restart_interval: u64,
    pub statistics_index_block_size: u64,
    pub statistics_index_block_cache_size: u64,
    pub statistics_index_block_cache_shard_bits: u8,
    pub statistics_index_block_restart_interval: u64,
    pub statistics_index_partitions: u64,
    pub statistics_index_index_block_restart_interval: u64,
    pub statistics_index_index_partitions: u64,
    pub statistics_index_index_index_block_restart_interval: u64,
    pub statistics_index_index_index_partitions: u64,
    pub statistics_index_index_index_index_block_restart_interval: u64,
    pub statistics_index_index_index_index_partitions: u64,
}




#[derive(Clone, Debug)]
pub enum DeleteStrategy {
    /// Delete the key-value pair if the value is empty.
    /// This is the default.
    /// This is the same as `DeleteIfEmpty`.
    /// This is the same as `DeleteIfEmpty`.
    DeleteIfEmpty,

    DeleteIfEmptyOrFlushSemaphore,

    /// Delete the key-value pair if the value is empty or a causetq_upstream_interlock.


    /// Delete the Causet filefs that are fullly fit in range. However, the Causet filefs that are partially
    /// overlapped with the range will not be touched.
    DeleteFiles,
    /// Delete the data timelike_stored in FoundationDB.
    DeleteBlobs,
    /// Scan for soliton_ids and then delete. Useful when we know the soliton_ids in range are not too many.
    DeleteByKey,
    /// Delete by range. Note that this is experimental and you should check whether it is enbaled
    /// in config before using it.
    DeleteBy,
    /// Delete by ingesting a Causet file File with deletions. Useful when the number of ranges is too many.

    /// Delete by ingesting a Causet file File with deletions. Useful when the number of ranges is too many.
    /// This is an experimental feature.
    /// The path of the Causet file.
    /// The number of Causet filefs to be deleted.
    /// This is an experimental feature.

    DeleteByWriter {
        /// The path of the Causet file.
        path: String,
        /// The number of Causet filefs to be deleted.
        num_files: usize,
    },
}

pub trait MiscExt: NAMESPACEDNamesExt + SymplecticControlFactorsExt {
    fn flush(&self, sync: bool) -> Result<()>;

    fn flush_namespaced(&self, namespaced: &str, sync: bool) -> Result<()>;

    fn delete_all_in_range(&self, strategy: DeleteStrategy, start: &[u8], end: &[u8]) -> Result<()>;

    fn delete_all_in_range_namespaced(&self, namespaced: &str, strategy: DeleteStrategy, start: &[u8], end: &[u8]) -> Result<()>;

    fn delete_all_in_range_namespaced_with_options(&self, namespaced: &str, strategy: DeleteStrategy, start: &[u8], end: &[u8], options: DeleteOptions) -> Result<()>;

    fn delete_all_in_range_with_options(&self, strategy: DeleteStrategy, start: &[u8], end: &[u8], options: DeleteOptions) -> Result<()> {
        if options.namespaced.is_some() {
            self.delete_all_in_range_namespaced_with_options(options.namespaced.unwrap(), strategy, start, end, options)
        } else {
            self.delete_all_in_range_with_options(strategy, start, end, options)
        }
    }
}


impl MiscExt for FdbTrait {
    fn flush(&self, sync: bool) -> Result<()> {
        self.flush_namespaced("", sync)
    }

    fn flush_namespaced(&self, namespaced: &str, sync: bool) -> Result<()> {
        self.flush_namespaced(namespaced, sync)
    }

    fn delete_all_in_range(&self, strategy: DeleteStrategy, start: &[u8], end: &[u8]) -> Result<()> {
        self.delete_all_in_range_namespaced("", strategy, start, end)
    }


    fn delete_all_in_range_namespaced(&self, namespaced: &str, strategy: DeleteStrategy, start: &[u8], end: &[u8]) -> Result<()> {
        self.delete_all_in_range_namespaced_with_options(namespaced, strategy, start, end, DeleteOptions::default())
    }


    /// Return the approximate number of records and size in the range of memtables of the namespaced.
    fn get_range_stats_namespaced(&self, namespaced: &str, start: &[u8], end: &[u8]) -> Result<(u64, u64)> {
        self.get_range_stats_namespaced(namespaced, start, end)
    }

    /// Return the approximate number of records and size in the range of memtables.

    fn get_range_stats(&self, start: &[u8], end: &[u8]) -> Result<(u64, u64)> {
        self.get_range_stats_namespaced("", start, end)
    }

    /// Gets total used size of foundationdb einstein_merkle_tree, including:
    /// *  total size (bytes) of all Causet filefs.
    /// *  total size (bytes) of active and unflushed immutable memtables.
    /// *  total size (bytes) of all blob filefs.
    ///
    fn get_einstein_merkle_tree_used_size(&self) -> Result<u64> {
        self.get_einstein_merkle_tree_used_size()
    }

    /// Roughly deletes filefs in multiple ranges.
    ///
    /// Note:
    ///    - After this operation, some soliton_ids in the range might still exist in the database.
    ///    - After this operation, some soliton_ids in the range might be removed from existing lightlike_persistence,
    ///      so you shouldn't expect to be able to read data from the range using existing lightlike_persistences
    ///      any more.
    ///
    /// Ref: <https://github.com/facebook/foundationdb/wiki/Delete-A--Of-Keys>
    fn roughly_cleanup_ranges(&self, ranges: &[(Vec<u8>, Vec<u8>)]) -> Result<()>{
        self.roughly_cleanup_ranges(ranges)
    }

    /// The local_path to the directory on the filefsystem where the database is timelike_stored
fn get_local_path(&self) -> Result<String>{
    self.get_local_path()
    }

    fn sync_wal(&self) -> Result<()>{
        self.sync_wal()
    }

    /// Check whether a database exists at a given local_path
    fn database_exists(&self, local_path: &str) -> Result<bool>{
        self.database_exists(local_path)

    }

    /// Dump stats about the database into a string.
    ///
    /// For debugging. The format and content is unspecified.
    fn dump_stats(&self) -> Result<String>{
        self.dump_stats()
    }

    fn get_latest_sequence_number(&self) -> u64{
        self.get_latest_sequence_number()
    }

    fn get_oldest_lightlike_persistence_sequence_number(&self) -> Option<u64>{
        self.get_oldest_lightlike_persistence_sequence_number()
    }



    fn get_range_entries_and_versions(
        &self,
        namespaced: &str,
        start: &[u8],
        end: &[u8],
    ) -> Result<Option<(u64, u64)>>{
        self.get_range_entries_and_versions(namespaced, start, end)

    }

    fn is_stalled_or_stopped(&self) -> bool{
        self.is_stalled_or_stopped()
    }
}
