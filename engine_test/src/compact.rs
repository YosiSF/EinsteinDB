// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.
//

//! # Compact
//! 
//! ## Overview
//! 
//! This module provides the `Compact` trait and its implementations.
//! compaction-related with bloom filter
//! and indexing.


use einstein_db_alexandrov_processing::{
    index::{
        Index,
        IndexIterator,
        IndexIteratorOptions,
        IndexIteratorOptionsBuilder,
    }
};


/// `Compact` is a trait that provides compacting operations.
/// It is implemented by `Engine` and `Directory`.
/// `Compact` is used by `Engine` to compacting operations.
/// `Compact` is used by `Directory` to compacting operations.
/// `Compact` is used by `Snapshot` to compacting operations.
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::{cmp, u64};




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

impl Default for CompactOptions {
    fn default() -> Self {
        CompactOptions {
            causetq_upstream_interlock_threshold: 1,
            causetq_upstream_interlock_compaction_interval: 1,
            causetq_upstream_interlock_compaction_threshold: 1,
            block_size: 1 << 20,
            block_cache_size: 1 << 20,
            block_cache_shard_bits: 0,
            enable_bloom_filter: true,
            enable_indexing: true,
            index_block_size: 1 << 20,
            index_block_cache_size: 1 << 20,
            index_block_cache_shard_bits: 0,
            index_block_restart_interval: 16,
            compression_type: String::from("snappy"),
            compression_level: -1,
            compression_block_size: 0,
            compression_strategy: String::from("default"),
            compression_dict: vec![],
            enable_statistics: true,
            statistics_interval: 1,
            statistics_block_size: 1 << 20,
            statistics_block_cache_size: 1 << 20,
            statistics_block_cache_shard_bits: 0,
            statistics_block_restart_interval: 16,
            statistics_index_block_size: 1 << 20,
            statistics_index_block_cache_size: 1 << 20,
            statistics_index_block_cache_shard_bits: 0,
            statistics_index_block_restart_interval: 16,
            statistics_index_partitions: 1,
            statistics_index_index_block_restart_interval: 16,
            statistics_index_index_partitions: 1,
            statistics_index_index_index_block_restart_interval: 16,
            statistics_index_index_index_partitions: 1,
            statistics_index_index_index_index_block_restart_interval: 16,
            statistics_index_index_index_index_partitions: 1,
        }
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionOptions {
    pub level: i32,
    pub output_file_size_limit: u64,
    pub output_file_size_base: u64,
    pub output_file_size_multiplier: u64,
    pub output_file_size_max: u64,
    pub output_file_size_min: u64,

    pub max_output_file_size: u64,
    pub min_output_file_size: u64,
    pub max_output_file_size_base: u64,
    pub max_output_file_size_multiplier: u64,
    pub max_output_file_size_max: u64,

    pub max_output_file_size_min: u64,

    pub max_output_file_size_base_min: u64,
    pub max_output_file_size_base_max: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionOptionsBuilder {
    pub level: i32,
    pub output_file_size_limit: u64,
}

impl Default for CompactionOptionsBuilder {
    /// Checks whether any causet_merge family sets `disable_auto_jet_bundles` to `True` or not.
    fn auto_jet_bundles_is_disabled(&self) -> Result<bool, Error> {
        let mut disabled = false;
        for cf_opts in self.cf_opts.iter() {
            if cf_opts.disable_auto_jet_bundles {
                disabled = true;
                break;
            }
        }
        Ok(disabled)
    }

    fn default() -> Self {
        CompactionOptionsBuilder {
            level: 0,
            output_file_size_limit: 0,
        }
    }

}






    /// Compacts the causet_merge families in the specified range by manual or not.
    /// If `exclusive_manual` is `true`, the compaction will be exclusive.
    /// If `exclusive_manual` is `false`, the compaction will be inclusive.
    /// If `manual_compaction_type` is `ManualCompactionType::Manual`, the compaction will be manual.
    /// If `manual_compaction_type` is `ManualCompactionType::Auto`, the compaction will be automatic.



    /// Compacts the causet_merge families in the specified range by manual or not.
    ///
    /// If `exclusive_manual` is `true`, the compaction will be exclusive.
    /// If `exclusive_manual` is `false`, the compaction will be inclusive.
    /// If `manual_compaction_type` is `ManualCompactionType::Manual`, the compaction will be manual.
    /// If `manual_compaction_type` is `ManualCompactionType::Auto`, the compaction will be automatic.
    ///


    /// Compacts the causet_merge families in the specified range by manual or not.



    /// Compacts the causet_merge families in the specified range by manual or not.
    /// If `exclusive_manual` is `true`, the compaction will be exclusive.

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionOptionsManual {
    pub exclusive_manual: bool,
    pub manual_compaction_type: ManualCompactionType,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionOptionsUniversal {
    pub size_ratio: u64,
    pub min_merge_width: u64,
    pub max_merge_width: u64,
    pub max_size_amplification_percent: u64,
    pub stop_style: StopStyle,
    pub allow_trivial_move: bool,
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompactionOptionsFIFO {
    pub allow_trivial_move: bool,
}


impl Default for CompactionOptionsFIFO {
    fn default() -> Self {
        CompactionOptionsFIFO {

            // TODO: default value
            allow_trivial_move: false,
        }
    }
}

impl Default for CompactionOptionsUniversal {
    fn default() -> Self {
        CompactionOptionsUniversal {
            size_ratio: 0,
            min_merge_width: 0,
            max_merge_width: 0,
            max_size_amplification_percent: 0,
            stop_style: StopStyle::StopStyleNone,
            allow_trivial_move: false,
        }
    }
}


impl Default for CompactionOptionsManual {
    fn default() -> Self {
        CompactionOptionsManual {
            exclusive_manual: false,
            manual_compaction_type: ManualCompactionType::Manual,
        }
    }
}