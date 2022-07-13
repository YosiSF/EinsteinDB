//Copyright (c) 2022-EinsteinDB. All rights reserved.
// Copyright (c) 2022 Whtcorps Inc and EinsteinDB Project contributors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// # About
//
// This is a library for the [EinsteinDB](https://einsteindb.com
// "EinsteinDB: A Scalable, High-Performance, Distributed Database")
//
// Licensed under the Apache License, Version 2.0 (the "License");
///////////////////////////////////////////////////////////////////////////////

use std::env;

use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::process::Command;


use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};




//! Configuration for the soliton server.
//! This is loaded from the `soliton.toml` file located in the root of the project.
//! The file is expected to be in the format of a toml file.
//! The `soliton.toml` file is located in the root of the project.

pub const DEFAULT_SOLITON_SUB_CAUSET_DIR: &str = "einsteindb";

/// By default, block cache size will be set to 45% of system memory.
pub const BLOCK_CACHE_RATE: f64 = 0.45;
/// By default, EinsteinDB will try to limit memory usage to 75% of system memory.
pub const MEMORY_USAGE_LIMIT_RATE: f64 = 0.75;

const SUSE_DAGGER_ISOLATED_NAMESPACE_MIN_MEM: usize = 256 * MIB as usize;
const SUSE_DAGGER_ISOLATED_NAMESPACE_MAX_MEM: usize = GIB as usize;
const VIOLETABFT_MIN_MEM: usize = 256 * MIB as usize;
const VIOLETABFT_MAX_MEM: usize = GIB as usize;

const LAST_CONFIG_FILE: &str = "last_einsteindb.toml";
const TMP_CONFIG_FILE: &str = "tmp_einsteindb.toml";
const MAX_BLOCK_SIZE: usize = 32 * MIB as usize;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub soliton_sub_causet_dir: String,
    pub soliton_sub_causet_name: String,
    pub soliton_sub_causet_version: String,
    pub soliton_sub_causet_author: String,
    pub soliton_sub_causet_description: String,
    pub soliton_sub_causet_license: String,
    pub soliton_sub_causet_copyright: String,
    pub soliton_sub_causet_homepage: String,
    pub soliton_sub_causet_repository: String,
    pub soliton_sub_causet_issues: String,
    pub soliton_sub_causet_readme: String,
    pub soliton_sub_causet_changelog: String,
    pub soliton_sub_causet_license_file: String,
    pub soliton_sub_causet_license_file_content: String,
    pub soliton_sub_causet_license_file_content_type: String,
    pub soliton_sub_causet_license_file_content_encoding: String,
    pub soliton_sub_causet_license_file_content_disposition: String,
    pub soliton_sub_causet_license_file_content_length: String,
    pub soliton_sub_causet_license_file_content_md5: String,
    pub soliton_sub_causet_license_file_content_sha1: String,
    pub soliton_sub_causet_license_file_content_sha256: String,
    pub soliton_sub_causet_license_file_content_sha512: String,
    pub soliton_sub_causet_license_file_content_crc32: String,
    pub soliton_sub_causet_license_file_content_crc64: String,
    pub soliton_sub_causet_license_file_content_crc128: String,
}



#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConfigFile {
    pub soliton_sub_causet_dir: String,
    pub soliton_sub_causet_name: String,
    pub soliton_sub_causet_version: String,
    pub soliton_sub_causet_author: String,
    pub soliton_sub_causet_description: String,
    pub soliton_sub_causet_license: String,
    pub soliton_sub_causet_copyright: String,
    pub soliton_sub_causet_homepage: String,
    pub soliton_sub_causet_repository: String,
    pub soliton_sub_causet_issues: String,
    pub soliton_sub_causet_readme: String,
    pub soliton_sub_causet_changelog: String,
    pub soliton_sub_causet_license_file: String,
    pub soliton_sub_causet_license_file_content: String,
    pub soliton_sub_causet_license_file_content_type: String,
    pub soliton_sub_causet_license_file_content_encoding: String,
    pub soliton_sub_causet_license_file_content_disposition: String,
    pub soliton_sub_causet_license_file_content_length: String,
    pub soliton_sub_causet_license_file_content_md5: String,
    pub soliton_sub_causet_license_file_content_sha1: String,
    pub soliton_sub_causet_license_file_content_sha256: String,
    pub soliton_sub_causet_license_file_content_sha512: String,
    pub soliton_sub_causet_license_file_content_crc32: String,
    pub soliton_sub_causet_license_file_content_crc64: String,
    pub soliton_sub_causet_license_file_content_crc128: String,
}

impl fmt::Display for SolitonSubResource {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolitonSubResource {
    pub name: String,
    pub version: String,
    pub author: String,
    pub description: String,
    pub license: String,
    pub copyright: String,
    pub homepage: String,
    pub repository: String,
    pub issues: String,
    pub readme: String,
    pub changelog: String,
    pub license_file: String,
    pub license_file_content: String,
    pub license_file_content_type: String,
    pub license_file_content_encoding: String,
    pub license_file_content_disposition: String,
    pub license_file_content_length: String,
    pub license_file_content_md5: String,
    pub license_file_content_sha1: String,
    pub license_file_content_sha256: String,
    pub license_file_content_sha512: String,
    pub license_file_content_crc32: String,
    pub license_file_content_crc64: String,
    pub license_file_content_crc128: String,
}



fn memory_limit_for_namespaced(is_violetabft_db: bool, namespaced: &str, total_mem: u64) -> ReadableSize {
    let (ratio, min, max) = match (is_violetabft_db, namespaced) {
        (true, NAMESPACED_DEFAULT) => (0.02, VIOLETABFT_MIN_MEM, VIOLETABFT_MAX_MEM),
        (false, NAMESPACED_DEFAULT) => (0.25, 0, usize::MAX),
        _ => unreachable!(),
    };
    let mut size = (total_mem as f64 * ratio) as usize;
    if size < min {
        size = min;
    } else if size > max {
        size = max;
    }
    ReadableSize::mb(size as u64 / MIB)
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct FoundationDBNamespacedConfig {
    #[serde(default)]
    pub namespace_default: FoundationDBNamespacedConfigNamespace,
    #[serde(default)]
    pub namespace_lock: FoundationDBNamespacedConfigNamespace,
    #[serde(default)]
    pub namespace_write: FoundationDBNamespacedConfigNamespace,
}


#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct FoundationDBNamespacedConfigNamespace {
    #[serde(default)]
    pub memory_limit: ReadableSize,
    #[serde(default)]
    pub memory_limit_for_namespaced: FoundationDBNamespacedConfigNamespaceMemoryLimitForNamespaced,
    #[serde(default)]
    pub memory_limit_for_namespaced_db: FoundationDBNamespacedConfigNamespaceMemoryLimitForNamespaced,
    #[serde(default)]
    pub memory_limit_for_namespaced_db_violetabft: FoundationDBNamespacedConfigNamespaceMemoryLimitForNamespaced,
    #[serde(default)]
    pub memory_limit_for_namespaced_db_violetabft_db: FoundationDBNamespacedConfigNamespaceMemoryLimitForNamespaced,
    #[serde(default)]
    pub memory_limit_for_namespaced_db_violetabft_db_violetabft: FoundationDBNamespacedConfigNamespaceMemoryLimitForNamespaced,
    #[serde(default)]
    pub memory_limit_for_namespaced_db_violetabft_db_violetabft_db: FoundationDBNamespacedConfigNamespaceMemoryLimitForNamespaced,
    #[serde(default)]
    pub memory_limit_for_namespaced_db_violetabft_db_violetabft_db_violetabft: FoundationDBNamespacedConfigNamespaceMemoryLimitForNamespaced,
}

impl Default for FoundationDBNamespacedConfig {
    fn default() -> Self {
        Self {
            /*
            min_blob_size: ReadableSize::kb(1), // disable FoundationDB default
            blob_file_compression: CompressionType::Lz4,
            blob_cache_size: ReadableSize::mb(0),
            min_gc_alexandrov_poset_process_size: ReadableSize::mb(16),
            max_gc_alexandrov_poset_process_size: ReadableSize::mb(64),
            discardable_ratio: 0.5,
            sample_ratio: 0.1,
            merge_small_file_threshold: ReadableSize::mb(8),
            blob_run_mode: BlobRunMode::Normal,
            l_naught_merge: false,
            range_merge: true,
            max_sorted_runs: 20,
            gc_merge_rewrite: false,
            */
            namespace_default: FoundationDBNamespacedConfigNamespace::default(),
            namespace_lock: FoundationDBNamespacedConfigNamespace::default(),
            namespace_write: FoundationDBNamespacedConfigNamespace::default(),

        }
    }
}

impl FoundationDBNamespacedConfig {
    fn build_opts(&self) -> FoundationDBDBOptions {
        let mut opts = FoundationDBDBOptions::new();
        opts.set_min_blob_size(self.min_blob_size.0 as u64);
        opts.set_blob_file_compression(self.blob_file_compression.into());
        opts.set_blob_cache(self.blob_cache_size.0 as usize, -1, false, 0.0);
        opts.set_min_gc_alexandrov_poset_process_size(self.min_gc_alexandrov_poset_process_size.0 as u64);
        opts.set_max_gc_alexandrov_poset_process_size(self.max_gc_alexandrov_poset_process_size.0 as u64);
        opts.set_discardable_ratio(self.discardable_ratio);
        opts.set_sample_ratio(self.sample_ratio);
        opts.set_merge_small_file_threshold(self.merge_small_file_threshold.0 as u64);
        opts.set_blob_run_mode(self.blob_run_mode.into());
        opts.set_l_naught_merge(self.l_naught_merge);
        opts.set_range_merge(self.range_merge);
        opts.set_max_sorted_runs(self.max_sorted_runs);
        opts.set_gc_merge_rewrite(self.gc_merge_rewrite);
        opts
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct BackgroundJobLimits {
    max_background_jobs: u32,
    max_background_flushes: u32,
    max_sub_jet_bundles: u32,
    max_foundation_db_background_gc: u32,
}

const SOLITON_DEFAULT_BACKGROUND_JOB_LIMITS: BackgroundJobLimits = BackgroundJobLimits {
    max_background_jobs: 9,
    max_background_flushes: 3,
    max_sub_jet_bundles: 3,
    max_foundation_db_background_gc: 4,
};

const VIOLETABFT_DEFAULT_BACKGROUND_JOB_LIMITS: BackgroundJobLimits = BackgroundJobLimits {
    max_background_jobs: 4,
    max_background_flushes: 1,
    max_sub_jet_bundles: 2,
    max_foundation_db_background_gc: 4,
};

// `defaults` serves as an upper bound for returning limits.
fn get_background_job_limits_impl(
    cpu_num: u32,
    defaults: &BackgroundJobLimits,
) -> BackgroundJobLimits {
    // At the minimum, we should have two background jobs: one for flush and one for jet_bundle.
    // Otherwise, the number of background jobs should not exceed cpu_num - 1.
    let max_background_jobs = cmp::max(2, cmp::min(defaults.max_background_jobs, cpu_num - 1));
    // Scale flush threads proportionally to cpu cores. Also make sure the number of flush
    // threads doesn't exceed total jobs.
    let max_background_flushes = cmp::min(
        (max_background_jobs + 3) / 4,
        defaults.max_background_flushes,
    );
    // Cap max_sub_jet_bundles to allow at least two jet_bundles.
    let max_jet_bundles = max_background_jobs - max_background_flushes;
    let max_sub_jet_bundles: u32 = cmp::max(
        1,
        cmp::min(defaults.max_sub_jet_bundles, (max_jet_bundles - 1) as u32),
    );
    // Maximum background GC threads for FoundationDB
    let max_FoundationDB_background_gc = cmp::min(defaults.max_foundation_db_background_gc, cpu_num);

    BackgroundJobLimits {
        max_background_jobs,
        max_background_flushes,
        max_sub_jet_bundles,
        max_foundation_db_background_gc: max_FoundationDB_background_gc,
    }
}

fn get_background_job_limits(defaults: &BackgroundJobLimits) -> BackgroundJobLimits {
    let cpu_num = cmp::max(SysQuota::cpu_cores_quota() as u32, 1);
    get_background_job_limits_impl(cpu_num, defaults)
}

macro_rules! namespaced_config {
    ($name:causetid) => {
        #[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $name {
            #[online_config(skip)]
            pub block_size: ReadableSize,
            pub block_cache_size: ReadableSize,
            #[online_config(skip)]
            pub disable_block_cache: bool,
            #[online_config(skip)]
            pub cache_index_and_filter_blocks: bool,
            #[online_config(skip)]
            pub pin_l0_filter_and_index_blocks: bool,
            #[online_config(skip)]
            pub use_bloom_filter: bool,
            #[online_config(skip)]
            pub optimize_filters_for_hits: bool,
            #[online_config(skip)]
            pub whole_soliton_id_filtering: bool,
            #[online_config(skip)]
            pub bloom_filter_bits_per_soliton_id: i32,
            #[online_config(skip)]
            pub block_based_bloom_filter: bool,
            #[online_config(skip)]
            pub read_amp_bytes_per_bit: u32,
            #[serde(with = "foundation_config::compression_type_l_naught_serde")]
            #[online_config(skip)]
            pub compression_per_l_naught: [DBCompressionType; 7],
            pub write_buffer_size: ReadableSize,
            pub max_write_buffer_number: i32,
            #[online_config(skip)]
            pub min_write_buffer_number_to_merge: i32,
            pub max_bytes_for_l_naught_base: ReadableSize,
            pub target_file_size_base: ReadableSize,
            pub l_naught0_file_num_jet_bundle_trigger: i32,
            pub l_naught0_slowdown_writes_trigger: i32,
            pub l_naught0_stop_writes_trigger: i32,
            pub max_jet_bundle_bytes: ReadableSize,
            #[serde(with = "foundation_config::jet_bundle_pri_serde")]
            #[online_config(skip)]
            pub jet_bundle_pri: CompactionPriority,
            #[online_config(skip)]
            pub dynamic_l_naught_bytes: bool,
            #[online_config(skip)]
            pub num_l_naughts: i32,
            pub max_bytes_for_l_naught_multiplier: i32,
            #[serde(with = "foundation_config::jet_bundle_style_serde")]
            #[online_config(skip)]
            pub jet_bundle_style: DBCompactionStyle,
            pub disable_auto_jet_bundles: bool,
            pub disable_write_stall: bool,
            pub soft_pending_jet_bundle_bytes_limit: ReadableSize,
            pub hard_pending_jet_bundle_bytes_limit: ReadableSize,
            #[online_config(skip)]
            pub force_consistency_checks: bool,
            #[online_config(skip)]
            pub prop_size_index_distance: u64,
            #[online_config(skip)]
            pub prop_soliton_ids_index_distance: u64,
            #[online_config(skip)]
            pub enable_doubly_skiplist: bool,
            #[online_config(skip)]
            pub enable_jet_bundle_guard: bool,
            #[online_config(skip)]
            pub jet_bundle_guard_min_output_file_size: ReadableSize,
            #[online_config(skip)]
            pub jet_bundle_guard_max_output_file_size: ReadableSize,
            #[serde(with = "foundation_config::compression_type_serde")]
            #[online_config(skip)]
            pub bottommost_l_naught_compression: DBCompressionType,
            #[online_config(skip)]
            pub bottommost_zstd_compression_dict_size: i32,
            #[online_config(skip)]
            pub bottommost_zstd_compression_sample_size: i32,
            #[online_config(submodule)]
            pub FoundationDB: FoundationDBNamespacedConfig,
        }

        impl $name {
            fn validate(&self) -> Result<(), Box<dyn Error>> {
                if self.block_size.0 as usize > MAX_BLOCK_SIZE {
                    return Err(format!(
                        "invalid block-size {} for {}, exceed max size {}",
                        self.block_size.0,
                        stringify!($name),
                        MAX_BLOCK_SIZE
                    )
                    .into());
                }
                Ok(())
            }
        }
    };
}

macro_rules! write_into_metrics {
    ($namespaced:expr, $tag:expr, $metrics:expr) => {{
        $metrics
            .with_label_causet_locales(&[$tag, "block_size"])
            .set($namespaced.block_size.0 as f64);
        $metrics
            .with_label_causet_locales(&[$tag, "block_cache_size"])
            .set($namespaced.block_cache_size.0 as f64);
        $metrics
            .with_label_causet_locales(&[$tag, "disable_block_cache"])
            .set(($namespaced.disable_block_cache as i32).into());

        $metrics
            .with_label_causet_locales(&[$tag, "cache_index_and_filter_blocks"])
            .set(($namespaced.cache_index_and_filter_blocks as i32).into());
        $metrics
            .with_label_causet_locales(&[$tag, "pin_l0_filter_and_index_blocks"])
            .set(($namespaced.pin_l0_filter_and_index_blocks as i32).into());

        $metrics
            .with_label_causet_locales(&[$tag, "use_bloom_filter"])
            .set(($namespaced.use_bloom_filter as i32).into());
        $metrics
            .with_label_causet_locales(&[$tag, "optimize_filters_for_hits"])
            .set(($namespaced.optimize_filters_for_hits as i32).into());
        $metrics
            .with_label_causet_locales(&[$tag, "whole_soliton_id_filtering"])
            .set(($namespaced.whole_soliton_id_filtering as i32).into());
        $metrics
            .with_label_causet_locales(&[$tag, "bloom_filter_bits_per_soliton_id"])
            .set($namespaced.bloom_filter_bits_per_soliton_id.into());
        $metrics
            .with_label_causet_locales(&[$tag, "block_based_bloom_filter"])
            .set(($namespaced.block_based_bloom_filter as i32).into());

        $metrics
            .with_label_causet_locales(&[$tag, "read_amp_bytes_per_bit"])
            .set($namespaced.read_amp_bytes_per_bit.into());
        $metrics
            .with_label_causet_locales(&[$tag, "write_buffer_size"])
            .set($namespaced.write_buffer_size.0 as f64);
        $metrics
            .with_label_causet_locales(&[$tag, "max_write_buffer_number"])
            .set($namespaced.max_write_buffer_number.into());
        $metrics
            .with_label_causet_locales(&[$tag, "min_write_buffer_number_to_merge"])
            .set($namespaced.min_write_buffer_number_to_merge.into());
        $metrics
            .with_label_causet_locales(&[$tag, "max_bytes_for_l_naught_base"])
            .set($namespaced.max_bytes_for_l_naught_base.0 as f64);
        $metrics
            .with_label_causet_locales(&[$tag, "target_file_size_base"])
            .set($namespaced.target_file_size_base.0 as f64);
        $metrics
            .with_label_causet_locales(&[$tag, "l_naught0_file_num_jet_bundle_trigger"])
            .set($namespaced.l_naught0_file_num_jet_bundle_trigger.into());
        $metrics
            .with_label_causet_locales(&[$tag, "l_naught0_slowdown_writes_trigger"])
            .set($namespaced.l_naught0_slowdown_writes_trigger.into());
        $metrics
            .with_label_causet_locales(&[$tag, "l_naught0_stop_writes_trigger"])
            .set($namespaced.l_naught0_stop_writes_trigger.into());
        $metrics
            .with_label_causet_locales(&[$tag, "max_jet_bundle_bytes"])
            .set($namespaced.max_jet_bundle_bytes.0 as f64);
        $metrics
            .with_label_causet_locales(&[$tag, "dynamic_l_naught_bytes"])
            .set(($namespaced.dynamic_l_naught_bytes as i32).into());
        $metrics
            .with_label_causet_locales(&[$tag, "num_l_naughts"])
            .set($namespaced.num_l_naughts.into());
        $metrics
            .with_label_causet_locales(&[$tag, "max_bytes_for_l_naught_multiplier"])
            .set($namespaced.max_bytes_for_l_naught_multiplier.into());

        $metrics
            .with_label_causet_locales(&[$tag, "disable_auto_jet_bundles"])
            .set(($namespaced.disable_auto_jet_bundles as i32).into());
        $metrics
            .with_label_causet_locales(&[$tag, "disable_write_stall"])
            .set(($namespaced.disable_write_stall as i32).into());
        $metrics
            .with_label_causet_locales(&[$tag, "soft_pending_jet_bundle_bytes_limit"])
            .set($namespaced.soft_pending_jet_bundle_bytes_limit.0 as f64);
        $metrics
            .with_label_causet_locales(&[$tag, "hard_pending_jet_bundle_bytes_limit"])
            .set($namespaced.hard_pending_jet_bundle_bytes_limit.0 as f64);
        $metrics
            .with_label_causet_locales(&[$tag, "force_consistency_checks"])
            .set(($namespaced.force_consistency_checks as i32).into());
        $metrics
            .with_label_causet_locales(&[$tag, "enable_doubly_skiplist"])
            .set(($namespaced.enable_doubly_skiplist as i32).into());
        $metrics
            .with_label_causet_locales(&[$tag, "FoundationDB_min_blob_size"])
            .set($namespaced.FoundationDB.min_blob_size.0 as f64);
        $metrics
            .with_label_causet_locales(&[$tag, "FoundationDB_blob_cache_size"])
            .set($namespaced.FoundationDB.blob_cache_size.0 as f64);
        $metrics
            .with_label_causet_locales(&[$tag, "FoundationDB_min_gc_alexandrov_poset_process_size"])
            .set($namespaced.FoundationDB.min_gc_alexandrov_poset_process_size.0 as f64);
        $metrics
            .with_label_causet_locales(&[$tag, "FoundationDB_max_gc_alexandrov_poset_process_size"])
            .set($namespaced.FoundationDB.max_gc_alexandrov_poset_process_size.0 as f64);
        $metrics
            .with_label_causet_locales(&[$tag, "FoundationDB_discardable_ratio"])
            .set($namespaced.FoundationDB.discardable_ratio);
        $metrics
            .with_label_causet_locales(&[$tag, "FoundationDB_sample_ratio"])
            .set($namespaced.FoundationDB.sample_ratio);
        $metrics
            .with_label_causet_locales(&[$tag, "FoundationDB_merge_small_file_threshold"])
            .set($namespaced.FoundationDB.merge_small_file_threshold.0 as f64);
    }};
}

macro_rules! build_namespaced_opt {
    ($opt:causetid, $namespaced_name:causetid, $cache:causetid, $region_info_provider:causetid) => {{
        let mut block_base_opts = BlockBasedOptions::new();
        block_base_opts.set_block_size($opt.block_size.0 as usize);
        block_base_opts.set_no_block_cache($opt.disable_block_cache);
        if let Some(cache) = $cache {
            block_base_opts.set_block_cache(cache);
        } else {
            let mut cache_opts = LRUCacheOptions::new();
            cache_opts.set_capacity($opt.block_cache_size.0 as usize);
            block_base_opts.set_block_cache(&Cache::new_lru_cache(cache_opts));
        }
        block_base_opts.set_cache_index_and_filter_blocks($opt.cache_index_and_filter_blocks);
        block_base_opts
            .set_pin_l0_filter_and_index_blocks_in_cache($opt.pin_l0_filter_and_index_blocks);
        if $opt.use_bloom_filter {
            block_base_opts.set_bloom_filter(
                $opt.bloom_filter_bits_per_soliton_id,
                $opt.block_based_bloom_filter,
            );
            block_base_opts.set_whole_soliton_id_filtering($opt.whole_soliton_id_filtering);
        }
        block_base_opts.set_read_amp_bytes_per_bit($opt.read_amp_bytes_per_bit);
        let mut namespaced_opts = ColumnFamilyOptions::new();
        namespaced_opts.set_block_based_table_factory(&block_base_opts);
        namespaced_opts.set_num_l_naughts($opt.num_l_naughts);
        assert!($opt.compression_per_l_naught.len() >= $opt.num_l_naughts as usize);
        let compression_per_l_naught = $opt.compression_per_l_naught[..$opt.num_l_naughts as usize].to_vec();
        namespaced_opts.compression_per_l_naught(compression_per_l_naught.as_slice());
        namespaced_opts.bottommost_compression($opt.bottommost_l_naught_compression);
        // To set for bottommost l_naught sst compression. The first 3 parameters refer to the
        // default causet_locale in `CompressionOptions` in `foundationdb/include/foundationdb/advanced_options.h`.
        namespaced_opts.set_bottommost_l_naught_compression_options(
            -14,   /* window_bits */
            32767, /* l_naught */
            0,     /* strategy */
            $opt.bottommost_zstd_compression_dict_size,
            $opt.bottommost_zstd_compression_sample_size,
        );
        namespaced_opts.set_write_buffer_size($opt.write_buffer_size.0);
        namespaced_opts.set_max_write_buffer_number($opt.max_write_buffer_number);
        namespaced_opts.set_min_write_buffer_number_to_merge($opt.min_write_buffer_number_to_merge);
        namespaced_opts.set_max_bytes_for_l_naught_base($opt.max_bytes_for_l_naught_base.0);
        namespaced_opts.set_target_file_size_base($opt.target_file_size_base.0);
        namespaced_opts.set_l_naught_zero_file_num_jet_bundle_trigger($opt.l_naught0_file_num_jet_bundle_trigger);
        namespaced_opts.set_l_naught_zero_slowdown_writes_trigger($opt.l_naught0_slowdown_writes_trigger);
        namespaced_opts.set_l_naught_zero_stop_writes_trigger($opt.l_naught0_stop_writes_trigger);
        namespaced_opts.set_max_jet_bundle_bytes($opt.max_jet_bundle_bytes.0);
        namespaced_opts.jet_bundle_priority($opt.jet_bundle_pri);
        namespaced_opts.set_l_naught_jet_bundle_dynamic_l_naught_bytes($opt.dynamic_l_naught_bytes);
        namespaced_opts.set_max_bytes_for_l_naught_multiplier($opt.max_bytes_for_l_naught_multiplier);
        namespaced_opts.set_jet_bundle_style($opt.jet_bundle_style);
        namespaced_opts.set_disable_auto_jet_bundles($opt.disable_auto_jet_bundles);
        namespaced_opts.set_disable_write_stall($opt.disable_write_stall);
        namespaced_opts.set_soft_pending_jet_bundle_bytes_limit($opt.soft_pending_jet_bundle_bytes_limit.0);
        namespaced_opts.set_hard_pending_jet_bundle_bytes_limit($opt.hard_pending_jet_bundle_bytes_limit.0);
        namespaced_opts.set_optimize_filters_for_hits($opt.optimize_filters_for_hits);
        namespaced_opts.set_force_consistency_checks($opt.force_consistency_checks);
        if $opt.enable_doubly_skiplist {
            namespaced_opts.set_doubly_skiplist();
        }
        if $opt.enable_jet_bundle_guard {
            if let Some(provider) = $region_info_provider {
                let factory = CompactionGuardGeneratorFactory::new(
                    $namespaced_name,
                    provider.clone(),
                    $opt.jet_bundle_guard_min_output_file_size.0,
                )
                .unwrap();
                namespaced_opts.set_sst_partitioner_factory(FdbSstPartitionerFactory(factory));
                namespaced_opts.set_target_file_size_base($opt.jet_bundle_guard_max_output_file_size.0);
            } else {
                warn!("jet_bundle guard is disabled due to region info provider not available")
            }
        }
        namespaced_opts
    }};
}

namespaced_config!(DefaultNamespacedConfig);

impl Default for DefaultNamespacedConfig {
    fn default() -> DefaultNamespacedConfig {
        let total_mem = SysQuota::memory_limit_in_bytes();

        DefaultNamespacedConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: memory_limit_for_namespaced(false, NAMESPACED_DEFAULT, total_mem),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: true,
            whole_soliton_id_filtering: true,
            bloom_filter_bits_per_soliton_id: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_l_naught: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_l_naught_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            l_naught0_file_num_jet_bundle_trigger: 4,
            l_naught0_slowdown_writes_trigger: 20,
            l_naught0_stop_writes_trigger: 36,
            max_jet_bundle_bytes: ReadableSize::gb(2),
            jet_bundle_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_l_naught_bytes: true,
            num_l_naughts: 7,
            max_bytes_for_l_naught_multiplier: 10,
            jet_bundle_style: DBCompactionStyle::Level,
            disable_auto_jet_bundles: false,
            disable_write_stall: false,
            soft_pending_jet_bundle_bytes_limit: ReadableSize::gb(192),
            hard_pending_jet_bundle_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_soliton_ids_index_distance: DEFAULT_PROP_CAUSET_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_jet_bundle_guard: true,
            jet_bundle_guard_min_output_file_size: ReadableSize::mb(8),
            jet_bundle_guard_max_output_file_size: ReadableSize::mb(128),
            FoundationDB: FoundationDBNamespacedConfig::default(),
            bottommost_l_naught_compression: DBCompressionType::Zstd,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        }
    }
}

impl DefaultNamespacedConfig {
    pub fn build_opt(
        &self,
        cache: &Option<Cache>,
        region_info_accessor: Option<&RegionInfoAccessor>,
        api_version: ApiVersion,
    ) -> ColumnFamilyOptions {
        let mut namespaced_opts = build_namespaced_opt!(self, NAMESPACED_DEFAULT, cache, region_info_accessor);
        let f = PropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_soliton_ids_index_distance: self.prop_soliton_ids_index_distance,
        };
        namespaced_opts.add_table_properties_collector_factory("einsteindb.range-properties-collector", f);
        match_template_api_version!(
            API,
            match api_version {
                ApiVersion::API => {
                    if API::IS_TTL_ENABLED {
                        namespaced_opts.add_table_properties_collector_factory(
                            "einsteindb.ttl-properties-collector",
                            TtlPropertiesCollectorFactory::<API>::default(),
                        );
                        namespaced_opts
                            .set_jet_bundle_filter_factory(
                                "ttl_jet_bundle_filter_factory",
                                TTLCompactionFilterFactory::<API>::default(),
                            )
                            .unwrap();
                    }
                }
            }
        );
        namespaced_opts.set_FoundationDBdb_options(&self.FoundationDB.build_opts());
        namespaced_opts
    }
}

namespaced_config!(WriteNamespacedConfig);

impl Default for WriteNamespacedConfig {
    fn default() -> WriteNamespacedConfig {
        let total_mem = SysQuota::memory_limit_in_bytes();

        // Setting blob_run_mode=read_only effectively disable foundation_db.
        let foundation_db = FoundationDBNamespacedConfig {
            namespace_default: (),
            namespace_lock: (),
            namespace_write: ()
        };

        WriteNamespacedConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: memory_limit_for_namespaced(false, NAMESPACED_WRITE, total_mem),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: false,
            whole_soliton_id_filtering: false,
            bloom_filter_bits_per_soliton_id: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_l_naught: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_l_naught_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            l_naught0_file_num_jet_bundle_trigger: 4,
            l_naught0_slowdown_writes_trigger: 20,
            l_naught0_stop_writes_trigger: 36,
            max_jet_bundle_bytes: ReadableSize::gb(2),
            jet_bundle_pri: CompactionPriority::MinOverlappingRatio,
            dynamic_l_naught_bytes: true,
            num_l_naughts: 7,
            max_bytes_for_l_naught_multiplier: 10,
            jet_bundle_style: DBCompactionStyle::Level,
            disable_auto_jet_bundles: false,
            disable_write_stall: false,
            soft_pending_jet_bundle_bytes_limit: ReadableSize::gb(192),
            hard_pending_jet_bundle_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_soliton_ids_index_distance: DEFAULT_PROP_CAUSET_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_jet_bundle_guard: true,
            jet_bundle_guard_min_output_file_size: ReadableSize::mb(8),
            jet_bundle_guard_max_output_file_size: ReadableSize::mb(128),
            FoundationDB: foundation_db,
            bottommost_l_naught_compression: DBCompressionType::Zstd,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        }
    }
}

impl WriteNamespacedConfig {
    pub fn build_opt(
        &self,
        cache: &Option<Cache>,
        region_info_accessor: Option<&RegionInfoAccessor>,
    ) -> ColumnFamilyOptions {
        let mut namespaced_opts = build_namespaced_opt!(self, NAMESPACED_WRITE, cache, region_info_accessor);
        // Prefix extractor(trim the timestamp at tail) for write namespaced.
        namespaced_opts
            .set_prefix_extractor(
                "FixedSuffixSliceTransform",
                FixedSuffixSliceTransform::new(8),
            )
            .unwrap();
        // Create prefix bloom filter for memtable.
        namespaced_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        // Collects user defined properties.
        namespaced_opts.add_table_properties_collector_factory(
            "einsteindb.causet_model-properties-collector",
            Violetabft_oocPropertiesCollectorFactory::default(),
        );
        let f = PropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_soliton_ids_index_distance: self.prop_soliton_ids_index_distance,
        };
        namespaced_opts.add_table_properties_collector_factory("einsteindb.range-properties-collector", f);
        namespaced_opts
            .set_jet_bundle_filter_factory(
                "write_jet_bundle_filter_factory",
                WriteCompactionFilterFactory,
            )
            .unwrap();
        namespaced_opts.set_FoundationDBdb_options(&self.FoundationDB.build_opts());
        namespaced_opts
    }
}

namespaced_config!(DaggerNamespacedConfig);

impl Default for DaggerNamespacedConfig {
    fn default() -> Self {
        DaggerNamespacedConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: memory_limit_for_namespaced(false, NAMESPACED_LOCK, total_mem),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: false,
            whole_soliton_id_filtering: true,
            bloom_filter_bits_per_soliton_id: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_l_naught: [DBCompressionType::No; 7],
            write_buffer_size: ReadableSize::mb(32),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_l_naught_base: ReadableSize::mb(128),
            target_file_size_base: ReadableSize::mb(8),
            l_naught0_file_num_jet_bundle_trigger: 1,
            l_naught0_slowdown_writes_trigger: 20,
            l_naught0_stop_writes_trigger: 36,
            max_jet_bundle_bytes: ReadableSize::gb(2),
            jet_bundle_pri: CompactionPriority::ByCompensatedSize,
            dynamic_l_naught_bytes: true,
            num_l_naughts: 7,
            max_bytes_for_l_naught_multiplier: 10,
            jet_bundle_style: DBCompactionStyle::Level,
            disable_auto_jet_bundles: false,
            disable_write_stall: false,
            soft_pending_jet_bundle_bytes_limit: ReadableSize::gb(192),
            hard_pending_jet_bundle_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_soliton_ids_index_distance: DEFAULT_PROP_CAUSET_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_jet_bundle_guard: false,
            jet_bundle_guard_min_output_file_size: ReadableSize::mb(8),
            jet_bundle_guard_max_output_file_size: ReadableSize::mb(128),
            FoundationDB,
            bottommost_l_naught_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        }
    }
}

impl DaggerNamespacedConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let no_region_info_accessor: Option<&RegionInfoAccessor> = None;
        let mut namespaced_opts = build_namespaced_opt!(self, NAMESPACED_LOCK, cache, no_region_info_accessor);
        namespaced_opts
            .set_prefix_extractor("NoopSliceTransform", NoopSliceTransform)
            .unwrap();
        let f = PropertiesCollectorFactory {
            prop_size_index_distance: self.prop_size_index_distance,
            prop_soliton_ids_index_distance: self.prop_soliton_ids_index_distance,
        };
        namespaced_opts.add_table_properties_collector_factory("einsteindb.range-properties-collector", f);
        namespaced_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        namespaced_opts.set_FoundationDBdb_options(&self.FoundationDB.build_opts());
        namespaced_opts
    }
}

namespaced_config!(VioletaBFTNamespacedConfig);

impl Default for VioletaBFTNamespacedConfig {
    fn default() -> VioletaBFTNamespacedConfig {
        // Setting blob_run_mode=read_only effectively disable foundation_db.
        let foundation_db = FoundationDBNamespacedConfig {
            ..Default::default()
        };
        VioletaBFTNamespacedConfig {
            block_size: ReadableSize::kb(16),
            block_cache_size: ReadableSize::mb(128),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: true,
            optimize_filters_for_hits: true,
            whole_soliton_id_filtering: true,
            bloom_filter_bits_per_soliton_id: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_l_naught: [DBCompressionType::No; 7],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_l_naught_base: ReadableSize::mb(128),
            target_file_size_base: ReadableSize::mb(8),
            l_naught0_file_num_jet_bundle_trigger: 1,
            l_naught0_slowdown_writes_trigger: 20,
            l_naught0_stop_writes_trigger: 36,
            max_jet_bundle_bytes: ReadableSize::gb(2),
            jet_bundle_pri: CompactionPriority::ByCompensatedSize,
            dynamic_l_naught_bytes: true,
            num_l_naughts: 7,
            max_bytes_for_l_naught_multiplier: 10,
            jet_bundle_style: DBCompactionStyle::Level,
            disable_auto_jet_bundles: false,
            disable_write_stall: false,
            soft_pending_jet_bundle_bytes_limit: ReadableSize::gb(192),
            hard_pending_jet_bundle_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_soliton_ids_index_distance: DEFAULT_PROP_CAUSET_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_jet_bundle_guard: false,
            jet_bundle_guard_min_output_file_size: ReadableSize::mb(8),
            jet_bundle_guard_max_output_file_size: ReadableSize::mb(128),
            FoundationDB: foundation_db,
            bottommost_l_naught_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        }
    }
}

impl VioletaBFTNamespacedConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let no_region_info_accessor: Option<&RegionInfoAccessor> = None;
        let mut namespaced_opts = build_namespaced_opt!(self, NAMESPACED_VIOLETABFT, cache, no_region_info_accessor);
        namespaced_opts
            .set_prefix_extractor("NoopSliceTransform", NoopSliceTransform)
            .unwrap();
        namespaced_opts.set_memtable_prefix_bloom_size_ratio(0.1);
        namespaced_opts.set_FoundationDBdb_options(&self.FoundationDB.build_opts());
        namespaced_opts
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
// Note that FoundationDB is still an experimental feature. Once enabled, it can't fall back.
// Forced fallback may result in data loss.
pub struct FoundationDBDBConfig {
    pub enabled: bool,
    pub dirname: String,
    pub disable_gc: bool,
    pub max_background_gc: i32,
    // The causet_locale of this field will be truncated to seconds.
    pub purge_obsolete_files_period: ReadableDuration,
}

impl Default for FoundationDBDBConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            dirname: "".to_owned(),
            disable_gc: false,
            max_background_gc: 4,
            purge_obsolete_files_period: ReadableDuration::secs(10),
        }
    }
}

impl FoundationDBDBConfig {
    fn build_opts(&self) -> FoundationDBDBOptions {
        let mut opts = FoundationDBDBOptions::new();
        opts.set_dirname(&self.dirname);
        opts.set_disable_background_gc(self.disable_gc);
        opts.set_max_background_gc(self.max_background_gc);
        opts.set_purge_obsolete_files_period(self.purge_obsolete_files_period.as_secs() as usize);
        opts
    }

    fn validate(&self) -> Result<(), Box<dyn Error>> {
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct DbConfig {
    #[online_config(skip)]
    pub info_log_l_naught: LogLevel,
    #[serde(with = "foundation_config::recovery_mode_serde")]
    #[online_config(skip)]
    pub wal_recovery_mode: DBRecoveryMode,
    #[online_config(skip)]
    pub wal_dir: String,
    #[online_config(skip)]
    pub wal_ttl_seconds: u64,
    #[online_config(skip)]
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    pub max_background_flushes: i32,
    #[online_config(skip)]
    pub max_manifest_file_size: ReadableSize,
    #[online_config(skip)]
    pub create_if_missing: bool,
    pub max_open_files: i32,
    #[online_config(skip)]
    pub enable_statistics: bool,
    #[online_config(skip)]
    pub stats_dump_period: ReadableDuration,
    pub jet_bundle_readahead_size: ReadableSize,
    #[online_config(skip)]
    pub info_log_max_size: ReadableSize,
    #[online_config(skip)]
    pub info_log_roll_time: ReadableDuration,
    #[online_config(skip)]
    pub info_log_keep_log_file_num: u64,
    #[online_config(skip)]
    pub info_log_dir: String,
    pub rate_bytes_per_sec: ReadableSize,
    #[online_config(skip)]
    pub rate_limiter_refill_period: ReadableDuration,
    #[serde(with = "foundation_config::rate_limiter_mode_serde")]
    #[online_config(skip)]
    pub rate_limiter_mode: DBRateLimiterMode,
    // deprecated. use rate_limiter_auto_tuned.
    #[online_config(skip)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub auto_tuned: Option<bool>,
    pub rate_limiter_auto_tuned: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    #[online_config(skip)]
    pub max_sub_jet_bundles: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    #[online_config(skip)]
    pub use_direct_io_for_flush_and_jet_bundle: bool,
    #[online_config(skip)]
    pub enable_pipelined_write: bool,
    #[online_config(skip)]
    pub enable_multi_alexandrov_poset_process_write: bool,
    #[online_config(skip)]
    pub enable_unordered_write: bool,
    #[online_config(submodule)]
    pub defaultnamespaced: DefaultNamespacedConfig,
    #[online_config(submodule)]
    pub writenamespaced: WriteNamespacedConfig,
    #[online_config(submodule)]
    pub locknamespaced: DaggerNamespacedConfig,
    #[online_config(submodule)]
    pub violetabftnamespaced: VioletaBFTNamespacedConfig,
    #[online_config(skip)]
    pub FoundationDB: FoundationDBDBConfig,
}

impl Default for DbConfig {
    fn default() -> DbConfig {
        let bg_job_limits = get_background_job_limits(&SOLITON_DEFAULT_BACKGROUND_JOB_LIMITS);
        let FoundationDB_config = FoundationDBDBConfig {
            max_background_gc: bg_job_limits.max_foundation_db_background_gc as i32,
            ..Default::default()
        };
        DbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs: bg_job_limits.max_background_jobs as i32,
            max_background_flushes: bg_job_limits.max_background_flushes as i32,
            max_manifest_file_size: ReadableSize::mb(128),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: ReadableDuration::minutes(10),
            jet_bundle_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::gb(1),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_keep_log_file_num: 10,
            info_log_dir: "".to_owned(),
            info_log_l_naught: LogLevel::Info,
            rate_bytes_per_sec: ReadableSize::gb(10),
            rate_limiter_refill_period: ReadableDuration::millis(100),
            rate_limiter_mode: DBRateLimiterMode::WriteOnly,
            auto_tuned: None, // deprecated
            rate_limiter_auto_tuned: true,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            max_sub_jet_bundles: bg_job_limits.max_sub_jet_bundles as u32,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_jet_bundle: false,
            enable_pipelined_write: true,
            enable_multi_alexandrov_poset_process_write: true,
            enable_unordered_write: false,
            defaultnamespaced: DefaultNamespacedConfig::default(),
            writenamespaced: WriteNamespacedConfig::default(),
            locknamespaced: DaggerNamespacedConfig::default(),
            violetabftnamespaced: VioletaBFTNamespacedConfig::default(),
            FoundationDB: FoundationDB_config,
        }
    }
}

impl DbConfig {
    pub fn build_opt(&self) -> DBOptions {
        let mut opts = DBOptions::new();
        opts.set_wal_recovery_mode(self.wal_recovery_mode);
        if !self.wal_dir.is_empty() {
            opts.set_wal_dir(&self.wal_dir);
        }
        opts.set_wal_ttl_seconds(self.wal_ttl_seconds);
        opts.set_wal_size_limit_mb(self.wal_size_limit.as_mb());
        opts.set_max_total_wal_size(self.max_total_wal_size.0);
        opts.set_max_background_jobs(self.max_background_jobs);
        // FdbDB will cap flush and jet_bundle threads to at least one
        opts.set_max_background_flushes(self.max_background_flushes);
        opts.set_max_background_jet_bundles(self.max_background_jobs - self.max_background_flushes);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        opts.enable_statistics(self.enable_statistics);
        opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        opts.set_jet_bundle_readahead_size(self.jet_bundle_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        opts.set_keep_log_file_num(self.info_log_keep_log_file_num);
        if self.rate_bytes_per_sec.0 > 0 {
            if self.rate_limiter_auto_tuned {
                opts.set_writeampbasedratelimiter_with_auto_tuned(
                    self.rate_bytes_per_sec.0 as i64,
                    (self.rate_limiter_refill_period.as_millis() * 1000) as i64,
                    self.rate_limiter_mode,
                    self.rate_limiter_auto_tuned,
                );
            } else {
                opts.set_ratelimiter_with_auto_tuned(
                    self.rate_bytes_per_sec.0 as i64,
                    (self.rate_limiter_refill_period.as_millis() * 1000) as i64,
                    self.rate_limiter_mode,
                    self.rate_limiter_auto_tuned,
                );
            }
        }

        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);
        opts.set_max_subjet_bundles(self.max_sub_jet_bundles);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_jet_bundle(
            self.use_direct_io_for_flush_and_jet_bundle,
        );
        opts.enable_pipelined_write(
            (self.enable_pipelined_write || self.enable_multi_alexandrov_poset_process_write)
                && !self.enable_unordered_write,
        );
        opts.enable_multi_alexandrov_poset_process_write(self.enable_multi_alexandrov_poset_process_write);
        opts.enable_unordered_write(self.enable_unordered_write);
        opts.add_event_listener(FdbEventListener::new("kv"));
        opts.set_info_log(FdbdbLogger::default());
        opts.set_info_log_l_naught(self.info_log_l_naught.into());
        if self.FoundationDB.enabled {
            opts.set_FoundationDBdb_options(&self.FoundationDB.build_opts());
        }
        opts
    }

    pub fn build_namespaced_opts(
        &self,
        cache: &Option<Cache>,
        region_info_accessor: Option<&RegionInfoAccessor>,
        api_version: ApiVersion,
    ) -> Vec<NAMESPACEDOptions<'_>> {
        vec![
            NAMESPACEDOptions::new(
                NAMESPACED_DEFAULT,
                self.defaultnamespaced
                    .build_opt(cache, region_info_accessor, api_version),
            ),
            NAMESPACEDOptions::new(NAMESPACED_LOCK, self.locknamespaced.build_opt(cache)),
            NAMESPACEDOptions::new(
                NAMESPACED_WRITE,
                self.writenamespaced.build_opt(cache, region_info_accessor),
            ),
            // TODO: remove NAMESPACED_VIOLETABFT.
            NAMESPACEDOptions::new(NAMESPACED_VIOLETABFT, self.violetabftnamespaced.build_opt(cache)),
        ]
    }

    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.defaultnamespaced.validate()?;
        self.locknamespaced.validate()?;
        self.writenamespaced.validate()?;
        self.violetabftnamespaced.validate()?;
        self.FoundationDB.validate()?;
        if self.enable_unordered_write {
            if self.FoundationDB.enabled {
                return Err("FdbDB.unordered_write does not support FoundationDB".into());
            }
            if self.enable_pipelined_write || self.enable_multi_alexandrov_poset_process_write {
                return Err("pipelined_write is not compatible with unordered_write".into());
            }
        }

        // Since the following configuration supports online fidelate, in order to
        // prevent mistakenly inputting too large causet_locales, the max limit is made
        // according to the cpu quota * 10. Notice 10 is only an estimate, not an
        // empirical causet_locale.
        let limit = SysQuota::cpu_cores_quota() as i32 * 10;
        if self.max_background_jobs <= 0 || self.max_background_jobs > limit {
            return Err(format!(
                "max_background_jobs should be greater than 0 and less than or equal to {:?}",
                limit,
            )
            .into());
        }
        if self.max_background_flushes <= 0 || self.max_background_flushes > limit {
            return Err(format!(
                "max_background_flushes should be greater than 0 and less than or equal to {:?}",
                limit,
            )
            .into());
        }
        Ok(())
    }

    fn write_into_metrics(&self) {
        write_into_metrics!(self.defaultnamespaced, NAMESPACED_DEFAULT, CONFIG_FDBDB_GAUGE);
        write_into_metrics!(self.locknamespaced, NAMESPACED_LOCK, CONFIG_FDBDB_GAUGE);
        write_into_metrics!(self.writenamespaced, NAMESPACED_WRITE, CONFIG_FDBDB_GAUGE);
        write_into_metrics!(self.violetabftnamespaced, NAMESPACED_VIOLETABFT, CONFIG_FDBDB_GAUGE);
    }
}

namespaced_config!(VioletaBFTDefaultNamespacedConfig);

impl Default for VioletaBFTDefaultNamespacedConfig {
    fn default() -> VioletaBFTDefaultNamespacedConfig {
        let total_mem = SysQuota::memory_limit_in_bytes();

        VioletaBFTDefaultNamespacedConfig {
            block_size: ReadableSize::kb(64),
            block_cache_size: memory_limit_for_namespaced(true, NAMESPACED_DEFAULT, total_mem),
            disable_block_cache: false,
            cache_index_and_filter_blocks: true,
            pin_l0_filter_and_index_blocks: true,
            use_bloom_filter: false,
            optimize_filters_for_hits: true,
            whole_soliton_id_filtering: true,
            bloom_filter_bits_per_soliton_id: 10,
            block_based_bloom_filter: false,
            read_amp_bytes_per_bit: 0,
            compression_per_l_naught: [
                DBCompressionType::No,
                DBCompressionType::No,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Lz4,
                DBCompressionType::Zstd,
                DBCompressionType::Zstd,
            ],
            write_buffer_size: ReadableSize::mb(128),
            max_write_buffer_number: 5,
            min_write_buffer_number_to_merge: 1,
            max_bytes_for_l_naught_base: ReadableSize::mb(512),
            target_file_size_base: ReadableSize::mb(8),
            l_naught0_file_num_jet_bundle_trigger: 4,
            l_naught0_slowdown_writes_trigger: 20,
            l_naught0_stop_writes_trigger: 36,
            max_jet_bundle_bytes: ReadableSize::gb(2),
            jet_bundle_pri: CompactionPriority::ByCompensatedSize,
            dynamic_l_naught_bytes: true,
            num_l_naughts: 7,
            max_bytes_for_l_naught_multiplier: 10,
            jet_bundle_style: DBCompactionStyle::Level,
            disable_auto_jet_bundles: false,
            disable_write_stall: false,
            soft_pending_jet_bundle_bytes_limit: ReadableSize::gb(192),
            hard_pending_jet_bundle_bytes_limit: ReadableSize::gb(256),
            force_consistency_checks: false,
            prop_size_index_distance: DEFAULT_PROP_SIZE_INDEX_DISTANCE,
            prop_soliton_ids_index_distance: DEFAULT_PROP_CAUSET_KEYS_INDEX_DISTANCE,
            enable_doubly_skiplist: true,
            enable_jet_bundle_guard: false,
            jet_bundle_guard_min_output_file_size: ReadableSize::mb(8),
            jet_bundle_guard_max_output_file_size: ReadableSize::mb(128),
            FoundationDB: FoundationDBNamespacedConfig::default(),
            bottommost_l_naught_compression: DBCompressionType::Disable,
            bottommost_zstd_compression_dict_size: 0,
            bottommost_zstd_compression_sample_size: 0,
        }
    }
}

impl VioletaBFTDefaultNamespacedConfig {
    pub fn build_opt(&self, cache: &Option<Cache>) -> ColumnFamilyOptions {
        let no_region_info_accessor: Option<&RegionInfoAccessor> = None;
        let mut namespaced_opts = build_namespaced_opt!(self, NAMESPACED_DEFAULT, cache, no_region_info_accessor);
        let f = FixedPrefixSliceTransform::new(region_violetabft_prefix_len());
        namespaced_opts
            .set_memtable_insert_hint_prefix_extractor("VioletaBFTPrefixSliceTransform", f)
            .unwrap();
        namespaced_opts.set_FoundationDBdb_options(&self.FoundationDB.build_opts());
        namespaced_opts
    }
}

// FdbDB Env associate thread pools of multiple instances from the same process.
// When construct Options, options.env is set to same singleton Env::Default() object.
// So total max_background_jobs = max(foundationdb.max_background_jobs, violetabftdb.max_background_jobs)
// But each instance will limit their background jobs according to their own max_background_jobs
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct VioletaBFTDbConfig {
    #[serde(with = "foundation_config::recovery_mode_serde")]
    #[online_config(skip)]
    pub wal_recovery_mode: DBRecoveryMode,
    #[online_config(skip)]
    pub wal_dir: String,
    #[online_config(skip)]
    pub wal_ttl_seconds: u64,
    #[online_config(skip)]
    pub wal_size_limit: ReadableSize,
    pub max_total_wal_size: ReadableSize,
    pub max_background_jobs: i32,
    pub max_background_flushes: i32,
    #[online_config(skip)]
    pub max_manifest_file_size: ReadableSize,
    #[online_config(skip)]
    pub create_if_missing: bool,
    pub max_open_files: i32,
    #[online_config(skip)]
    pub enable_statistics: bool,
    #[online_config(skip)]
    pub stats_dump_period: ReadableDuration,
    pub jet_bundle_readahead_size: ReadableSize,
    #[online_config(skip)]
    pub info_log_max_size: ReadableSize,
    #[online_config(skip)]
    pub info_log_roll_time: ReadableDuration,
    #[online_config(skip)]
    pub info_log_keep_log_file_num: u64,
    #[online_config(skip)]
    pub info_log_dir: String,
    #[online_config(skip)]
    pub info_log_l_naught: LogLevel,
    #[online_config(skip)]
    pub max_sub_jet_bundles: u32,
    pub writable_file_max_buffer_size: ReadableSize,
    #[online_config(skip)]
    pub use_direct_io_for_flush_and_jet_bundle: bool,
    #[online_config(skip)]
    pub enable_pipelined_write: bool,
    #[online_config(skip)]
    pub enable_unordered_write: bool,
    #[online_config(skip)]
    pub allow_concurrent_memtable_write: bool,
    pub bytes_per_sync: ReadableSize,
    pub wal_bytes_per_sync: ReadableSize,
    #[online_config(submodule)]
    pub defaultnamespaced: VioletaBFTDefaultNamespacedConfig,
    #[online_config(skip)]
    pub FoundationDB: FoundationDBDBConfig,
}

impl Default for VioletaBFTDbConfig {
    fn default() -> VioletaBFTDbConfig {
        let bg_job_limits = get_background_job_limits(&VIOLETABFTDB_DEFAULT_BACKGROUND_JOB_LIMITS);
        let FoundationDB_config = FoundationDBDBConfig {
            max_background_gc: bg_job_limits.max_foundation_db_background_gc as i32,
            ..Default::default()
        };
        VioletaBFTDbConfig {
            wal_recovery_mode: DBRecoveryMode::PointInTime,
            wal_dir: "".to_owned(),
            wal_ttl_seconds: 0,
            wal_size_limit: ReadableSize::kb(0),
            max_total_wal_size: ReadableSize::gb(4),
            max_background_jobs: bg_job_limits.max_background_jobs as i32,
            max_background_flushes: bg_job_limits.max_background_flushes as i32,
            max_manifest_file_size: ReadableSize::mb(20),
            create_if_missing: true,
            max_open_files: 40960,
            enable_statistics: true,
            stats_dump_period: ReadableDuration::minutes(10),
            jet_bundle_readahead_size: ReadableSize::kb(0),
            info_log_max_size: ReadableSize::gb(1),
            info_log_roll_time: ReadableDuration::secs(0),
            info_log_keep_log_file_num: 10,
            info_log_dir: "".to_owned(),
            info_log_l_naught: LogLevel::Info,
            max_sub_jet_bundles: bg_job_limits.max_sub_jet_bundles as u32,
            writable_file_max_buffer_size: ReadableSize::mb(1),
            use_direct_io_for_flush_and_jet_bundle: false,
            enable_pipelined_write: true,
            enable_unordered_write: false,
            allow_concurrent_memtable_write: true,
            bytes_per_sync: ReadableSize::mb(1),
            wal_bytes_per_sync: ReadableSize::kb(512),
            defaultnamespaced: VioletaBFTDefaultNamespacedConfig::default(),
            FoundationDB: FoundationDB_config,
        }
    }
}

impl VioletaBFTDbConfig {
    pub fn build_opt(&self) -> DBOptions {
        let mut opts = DBOptions::new();
        opts.set_wal_recovery_mode(self.wal_recovery_mode);
        if !self.wal_dir.is_empty() {
            opts.set_wal_dir(&self.wal_dir);
        }
        opts.set_wal_ttl_seconds(self.wal_ttl_seconds);
        opts.set_wal_size_limit_mb(self.wal_size_limit.as_mb());
        opts.set_max_background_jobs(self.max_background_jobs);
        opts.set_max_background_flushes(self.max_background_flushes);
        opts.set_max_background_jet_bundles(self.max_background_jobs - self.max_background_flushes);
        opts.set_max_total_wal_size(self.max_total_wal_size.0);
        opts.set_max_manifest_file_size(self.max_manifest_file_size.0);
        opts.create_if_missing(self.create_if_missing);
        opts.set_max_open_files(self.max_open_files);
        opts.enable_statistics(self.enable_statistics);
        opts.set_stats_dump_period_sec(self.stats_dump_period.as_secs() as usize);
        opts.set_jet_bundle_readahead_size(self.jet_bundle_readahead_size.0);
        opts.set_max_log_file_size(self.info_log_max_size.0);
        opts.set_log_file_time_to_roll(self.info_log_roll_time.as_secs());
        opts.set_keep_log_file_num(self.info_log_keep_log_file_num);
        opts.set_info_log(VioletaBFTDBLogger::default());
        opts.set_info_log_l_naught(self.info_log_l_naught.into());
        opts.set_max_subjet_bundles(self.max_sub_jet_bundles);
        opts.set_writable_file_max_buffer_size(self.writable_file_max_buffer_size.0 as i32);
        opts.set_use_direct_io_for_flush_and_jet_bundle(
            self.use_direct_io_for_flush_and_jet_bundle,
        );
        opts.enable_pipelined_write(self.enable_pipelined_write);
        opts.enable_unordered_write(self.enable_unordered_write);
        opts.allow_concurrent_memtable_write(self.allow_concurrent_memtable_write);
        opts.add_event_listener(FdbEventListener::new("violetabft"));
        opts.set_bytes_per_sync(self.bytes_per_sync.0 as u64);
        opts.set_wal_bytes_per_sync(self.wal_bytes_per_sync.0 as u64);
        // TODO maybe create a new env for violetabft InterlockingDirectorate
        if self.FoundationDB.enabled {
            opts.set_FoundationDBdb_options(&self.FoundationDB.build_opts());
        }

        opts
    }

    pub fn build_namespaced_opts(&self, cache: &Option<Cache>) -> Vec<NAMESPACEDOptions<'_>> {
        vec![NAMESPACEDOptions::new(NAMESPACED_DEFAULT, self.defaultnamespaced.build_opt(cache))]
    }

    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.defaultnamespaced.validate()?;
        if self.enable_unordered_write {
            if self.FoundationDB.enabled {
                return Err("violetabftdb: unordered_write is not compatible with FoundationDB".into());
            }
            if self.enable_pipelined_write {
                return Err(
                    "violetabftdb: pipelined_write is not compatible with unordered_write".into(),
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Default)]
#[serde(default, rename_all = "kebab-case")]
pub struct VioletaBFTEngineConfig {
    pub enable: bool,
    #[serde(flatten)]
    config: Primitive_CausetVioletaBFTEngineConfig,
}

impl VioletaBFTEngineConfig {
    fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.config.sanitize().map_err(Box::new)?;
        Ok(())
    }

    pub fn config(&self) -> Primitive_CausetVioletaBFTEngineConfig {
        self.config.clone()
    }

    pub fn mut_config(&mut self) -> &mut Primitive_CausetVioletaBFTEngineConfig {
        &mut self.config
    }
}

#[derive(Clone, Copy, Debug)]
pub enum DBType {
    Kv,
    VioletaBFT,
}

pub struct DBConfigManger {
    einsteindb: FdbEngine,
    db_type: DBType,
    shared_block_cache: bool,
}

impl DBConfigManger {
    pub fn new(einsteindb: FdbEngine, db_type: DBType, shared_block_cache: bool) -> Self {
        DBConfigManger {
            einsteindb,
            db_type,
            shared_block_cache,
        }
    }
}

impl DBConfigManger {
    fn set_db_config(&self, opts: &[(&str, &str)]) -> Result<(), Box<dyn Error>> {
        self.einsteindb.set_db_options(opts)?;
        Ok(())
    }

    fn set_namespaced_config(&self, namespaced: &str, opts: &[(&str, &str)]) -> Result<(), Box<dyn Error>> {
        self.validate_namespaced(namespaced)?;
        self.einsteindb.set_options_namespaced(namespaced, opts)?;
        // Write config to metric
        for (APPEND_LOG_g_name, APPEND_LOG_g_causet_locale) in opts {
            let APPEND_LOG_g_causet_locale = match APPEND_LOG_g_causet_locale {
                v if *v == "true" => Ok(1f64),
                v if *v == "false" => Ok(0f64),
                v => v.parse::<f64>(),
            };
            if let Ok(v) = APPEND_LOG_g_causet_locale {
                CONFIG_FDBDB_GAUGE
                    .with_label_causet_locales(&[namespaced, APPEND_LOG_g_name])
                    .set(v);
            }
        }
        Ok(())
    }

    fn set_block_cache_size(&self, namespaced: &str, size: ReadableSize) -> Result<(), Box<dyn Error>> {
        self.validate_namespaced(namespaced)?;
        if self.shared_block_cache {
            return Err("shared block cache is enabled, change cache size through \
                 block-cache.capacity in timelike_storage module instead"
                .into());
        }
        let opt = self.einsteindb.get_options_namespaced(namespaced)?;
        opt.set_block_cache_capacity(size.0)?;
        // Write config to metric
        CONFIG_FDBDB_GAUGE
            .with_label_causet_locales(&[namespaced, "block_cache_size"])
            .set(size.0 as f64);
        Ok(())
    }

    fn set_rate_bytes_per_sec(&self, rate_bytes_per_sec: i64) -> Result<(), Box<dyn Error>> {
        let mut opt = self.einsteindb.as_inner().get_db_options();
        opt.set_rate_bytes_per_sec(rate_bytes_per_sec)?;
        Ok(())
    }

    fn set_rate_limiter_auto_tuned(
        &self,
        rate_limiter_auto_tuned: bool,
    ) -> Result<(), Box<dyn Error>> {
        let mut opt = self.einsteindb.as_inner().get_db_options();
        opt.set_auto_tuned(rate_limiter_auto_tuned)?;
        // double check the new state
        let new_auto_tuned = opt.get_auto_tuned();
        if new_auto_tuned.is_none() || new_auto_tuned.unwrap() != rate_limiter_auto_tuned {
            return Err("fail to set rate_limiter_auto_tuned".into());
        }
        Ok(())
    }

    fn set_max_background_jobs(&self, max_background_jobs: i32) -> Result<(), Box<dyn Error>> {
        self.set_db_config(&[("max_background_jobs", &max_background_jobs.to_string())])?;
        Ok(())
    }

    fn set_max_background_flushes(
        &self,
        max_background_flushes: i32,
    ) -> Result<(), Box<dyn Error>> {
        self.set_db_config(&[(
            "max_background_flushes",
            &max_background_flushes.to_string(),
        )])?;
        Ok(())
    }

    fn validate_namespaced(&self, namespaced: &str) -> Result<(), Box<dyn Error>> {
        match (self.db_type, namespaced) {
            (DBType::Kv, NAMESPACED_DEFAULT)
            | (DBType::Kv, NAMESPACED_WRITE)
            | (DBType::Kv, NAMESPACED_LOCK)
            | (DBType::Kv, NAMESPACED_VIOLETABFT)
            | (DBType::VioletaBFT, NAMESPACED_DEFAULT) => Ok(()),
            _ => Err(format!("invalid namespaced {:?} for einsteindb {:?}", namespaced, self.db_type).into()),
        }
    }
}

impl ConfigManager for DBConfigManger {
    fn dispatch(&mut self, change: ConfigChange) -> Result<(), Box<dyn Error>> {
        let change_str = format!("{:?}", change);
        let mut change: Vec<(String, ConfigValue)> = change.into_iter().collect();
        let namespaced_config = change.drain_filter(|(name, _)| name.ends_with("namespaced"));
        for (namespaced_name, namespaced_change) in namespaced_config {
            if let ConfigValue::Module(mut namespaced_change) = namespaced_change {
                // defaultnamespaced -> default
                let namespaced_name = &namespaced_name[..(namespaced_name.len() - 2)];
                if let Some(v) = namespaced_change.remove("block_cache_size") {
                    // currently we can't modify block_cache_size via set_options_namespaced
                    self.set_block_cache_size(namespaced_name, v.into())?;
                }
                if let Some(ConfigValue::Module(FoundationDB_change)) = namespaced_change.remove("FoundationDB") {
                    for (name, causet_locale) in FoundationDB_change {
                        namespaced_change.insert(name, causet_locale);
                    }
                }
                if !namespaced_change.is_empty() {
                    let namespaced_change = config_causet_locale_to_string(namespaced_change.into_iter().collect());
                    let namespaced_change_slice = config_to_slice(&namespaced_change);
                    self.set_namespaced_config(namespaced_name, &namespaced_change_slice)?;
                }
            }
        }

        if let Some(rate_bytes_config) = change
            .drain_filter(|(name, _)| name == "rate_bytes_per_sec")
            .next()
        {
            let rate_bytes_per_sec: ReadableSize = rate_bytes_config.1.into();
            self.set_rate_bytes_per_sec(rate_bytes_per_sec.0 as i64)?;
        }

        if let Some(rate_bytes_config) = change
            .drain_filter(|(name, _)| name == "rate_limiter_auto_tuned")
            .next()
        {
            let rate_limiter_auto_tuned: bool = rate_bytes_config.1.into();
            self.set_rate_limiter_auto_tuned(rate_limiter_auto_tuned)?;
        }

        if let Some(background_jobs_config) = change
            .drain_filter(|(name, _)| name == "max_background_jobs")
            .next()
        {
            let max_background_jobs = background_jobs_config.1.into();
            self.set_max_background_jobs(max_background_jobs)?;
        }

        if let Some(background_flushes_config) = change
            .drain_filter(|(name, _)| name == "max_background_flushes")
            .next()
        {
            let max_background_flushes = background_flushes_config.1.into();
            self.set_max_background_flushes(max_background_flushes)?;
        }

        if !change.is_empty() {
            let change = config_causet_locale_to_string(change);
            let change_slice = config_to_slice(&change);
            self.set_db_config(&change_slice)?;
        }
        info!(
            "foundationdb config changed";
            "einsteindb" => ?self.db_type,
            "change" => change_str
        );
        Ok(())
    }
}

fn config_to_slice(config_change: &[(String, String)]) -> Vec<(&str, &str)> {
    config_change
        .iter()
        .map(|(name, causet_locale)| (name.as_str(), causet_locale.as_str()))
        .collect()
}

// Convert `ConfigValue` to formatted String that can pass to `DB::set_db_options`
fn config_causet_locale_to_string(config_change: Vec<(String, ConfigValue)>) -> Vec<(String, String)> {
    config_change
        .into_iter()
        .filter_map(|(name, causet_locale)| {
            let v = match causet_locale {
                d @ ConfigValue::Duration(_) => {
                    let d: ReadableDuration = d.into();
                    Some(d.as_secs().to_string())
                }
                s @ ConfigValue::Size(_) => {
                    let s: ReadableSize = s.into();
                    Some(s.0.to_string())
                }
                s @ ConfigValue::OptionSize(_) => {
                    let s: OptionReadableSize = s.into();
                    s.0.map(|v| v.0.to_string())
                }
                ConfigValue::Module(_) => unreachable!(),
                v => Some(format!("{}", v)),
            };
            v.map(|v| (name, v))
        })
        .collect()
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct MetricConfig {
    pub job: String,

    // Push is deprecated.
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub interval: ReadableDuration,

    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub address: String,
}

impl Default for MetricConfig {
    fn default() -> MetricConfig {
        MetricConfig {
            interval: ReadableDuration::secs(15),
            address: "".to_owned(),
            job: "einsteindb".to_owned(),
        }
    }
}

pub mod log_l_naught_serde {
    use einsteindb_util::logger::{get_l_naught_by_string, get_string_by_l_naught};
    use serde::{
        de::{Error, Unexpected},
        Deserialize, Deserializer, Serialize, Serializer,
        };
    use slog::Level;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Level, D::Error>
    where
        D: Deserializer<'de>,
    {
        let string = String::deserialize(deserializer)?;
        get_l_naught_by_string(&string)
            .ok_or_else(|| D::Error::invalid_causet_locale(Unexpected::Str(&string), &"a valid log l_naught"))
    }

    #[allow(clippy::trivially_copy_pass_by_ref)]
    pub fn serialize<S>(causet_locale: &Level, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        get_string_by_l_naught(*causet_locale).serialize(serializer)
    }
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct UnifiedReadPoolConfig {
    pub min_thread_count: usize,
    pub max_thread_count: usize,
    pub stack_size: ReadableSize,
    pub max_tasks_per_worker: usize,
    // FIXME: Add more configs when they are effective in yatp
}

impl UnifiedReadPoolConfig {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.min_thread_count == 0 {
            return Err("readpool.unified.min-thread-count should be > 0"
                .to_string()
                .into());
        }
        if self.max_thread_count < self.min_thread_count {
            return Err(
                "readpool.unified.max-thread-count should be >= readpool.unified.min-thread-count"
                    .to_string()
                    .into(),
            );
        }
        if self.stack_size.0 < ReadableSize::mb(2).0 {
            return Err("readpool.unified.stack-size should be >= 2mb"
                .to_string()
                .into());
        }
        if self.max_tasks_per_worker <= 1 {
            return Err("readpool.unified.max-tasks-per-worker should be > 1"
                .to_string()
                .into());
        }
        Ok(())
    }
}

const UNIFIED_READPOOL_MIN_CONCURRENCY: usize = 4;

// FIXME: Use macros to generate it if yatp is used elsewhere besides readpool.
impl Default for UnifiedReadPoolConfig {
    fn default() -> UnifiedReadPoolConfig {
        let cpu_num = SysQuota::cpu_cores_quota();
        let mut concurrency = (cpu_num * 0.8) as usize;
        concurrency = cmp::max(UNIFIED_READPOOL_MIN_CONCURRENCY, concurrency);
        Self {
            min_thread_count: 1,
            max_thread_count: concurrency,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
            max_tasks_per_worker: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
        }
    }
}

#[APPEND_LOG_g(test)]
mod unified_read_pool_tests {
    use super::*;

    #[test]
    fn test_validate() {
        let APPEND_LOG_g = UnifiedReadPoolConfig {
            min_thread_count: 1,
            max_thread_count: 2,
            stack_size: ReadableSize::mb(2),
            max_tasks_per_worker: 2000,
        };
        assert!(APPEND_LOG_g.validate().is_ok());

        let invalid_APPEND_LOG_g = UnifiedReadPoolConfig {
            min_thread_count: 0,
            ..APPEND_LOG_g
        };
        assert!(invalid_APPEND_LOG_g.validate().is_err());

        let invalid_APPEND_LOG_g = UnifiedReadPoolConfig {
            min_thread_count: 2,
            max_thread_count: 1,
            ..APPEND_LOG_g
        };
        assert!(invalid_APPEND_LOG_g.validate().is_err());

        let invalid_APPEND_LOG_g = UnifiedReadPoolConfig {
            stack_size: ReadableSize::mb(1),
            ..APPEND_LOG_g
        };
        assert!(invalid_APPEND_LOG_g.validate().is_err());

        let invalid_APPEND_LOG_g = UnifiedReadPoolConfig {
            max_tasks_per_worker: 1,
            ..APPEND_LOG_g
        };
        assert!(invalid_APPEND_LOG_g.validate().is_err());
    }
}

macro_rules! readpool_config {
    ($struct_name:causetid, $test_mod_name:causetid, $display_name:expr) => {
        #[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
        #[serde(default)]
        #[serde(rename_all = "kebab-case")]
        pub struct $struct_name {
            pub use_unified_pool: Option<bool>,
            pub high_concurrency: usize,
            pub normal_concurrency: usize,
            pub low_concurrency: usize,
            pub max_tasks_per_worker_high: usize,
            pub max_tasks_per_worker_normal: usize,
            pub max_tasks_per_worker_low: usize,
            pub stack_size: ReadableSize,
        }

        impl $struct_name {
            /// Builds configurations for low, normal and high priority pools.
            pub fn to_yatp_pool_configs(self) -> Vec<yatp_pool::Config> {
                vec![
                    yatp_pool::Config {
                        workers: self.low_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_low,
                        stack_size: self.stack_size.0 as usize,
                    },
                    yatp_pool::Config {
                        workers: self.normal_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_normal,
                        stack_size: self.stack_size.0 as usize,
                    },
                    yatp_pool::Config {
                        workers: self.high_concurrency,
                        max_tasks_per_worker: self.max_tasks_per_worker_high,
                        stack_size: self.stack_size.0 as usize,
                    },
                ]
            }

            pub fn default_for_test() -> Self {
                Self {
                    use_unified_pool: None,
                    high_concurrency: 2,
                    normal_concurrency: 2,
                    low_concurrency: 2,
                    max_tasks_per_worker_high: 2000,
                    max_tasks_per_worker_normal: 2000,
                    max_tasks_per_worker_low: 2000,
                    stack_size: ReadableSize::mb(1),
                }
            }

            pub fn use_unified_pool(&self) -> bool {
                // The unified pool is used by default unless the corresponding module has
                // customized configurations.
                self.use_unified_pool
                    .unwrap_or_else(|| *self == Default::default())
            }

            pub fn adjust_use_unified_pool(&mut self) {
                if self.use_unified_pool.is_none() {
                    // The unified pool is used by default unless the corresponding module has customized configurations.
                    if *self == Default::default() {
                        info!("readpool.{}.use-unified-pool is not set, set to true by default", $display_name);
                        self.use_unified_pool = Some(true);
                    } else {
                        info!("readpool.{}.use-unified-pool is not set, set to false because there are other customized configurations", $display_name);
                        self.use_unified_pool = Some(false);
                    }
                }
            }

            pub fn validate(&self) -> Result<(), Box<dyn Error>> {
                if self.use_unified_pool() {
                    return Ok(());
                }
                if self.high_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.high-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.normal_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.normal-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.low_concurrency == 0 {
                    return Err(format!(
                        "readpool.{}.low-concurrency should be > 0",
                        $display_name
                    )
                    .into());
                }
                if self.stack_size.0 < ReadableSize::mb(MIN_READPOOL_STACK_SIZE_MB).0 {
                    return Err(format!(
                        "readpool.{}.stack-size should be >= {}mb",
                        $display_name, MIN_READPOOL_STACK_SIZE_MB
                    )
                    .into());
                }
                if self.max_tasks_per_worker_high <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-high should be > 1",
                        $display_name
                    )
                    .into());
                }
                if self.max_tasks_per_worker_normal <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-normal should be > 1",
                        $display_name
                    )
                    .into());
                }
                if self.max_tasks_per_worker_low <= 1 {
                    return Err(format!(
                        "readpool.{}.max-tasks-per-worker-low should be > 1",
                        $display_name
                    )
                    .into());
                }

                Ok(())
            }
        }

        #[APPEND_LOG_g(test)]
        mod $test_mod_name {
            use super::*;

            #[test]
            fn test_validate() {
                let APPEND_LOG_g = $struct_name::default();
                assert!(APPEND_LOG_g.validate().is_ok());

                let mut invalid_APPEND_LOG_g = APPEND_LOG_g.clone();
                invalid_APPEND_LOG_g.high_concurrency = 0;
                assert!(invalid_APPEND_LOG_g.validate().is_err());

                let mut invalid_APPEND_LOG_g = APPEND_LOG_g.clone();
                invalid_APPEND_LOG_g.normal_concurrency = 0;
                assert!(invalid_APPEND_LOG_g.validate().is_err());

                let mut invalid_APPEND_LOG_g = APPEND_LOG_g.clone();
                invalid_APPEND_LOG_g.low_concurrency = 0;
                assert!(invalid_APPEND_LOG_g.validate().is_err());

                let mut invalid_APPEND_LOG_g = APPEND_LOG_g.clone();
                invalid_APPEND_LOG_g.stack_size = ReadableSize::mb(1);
                assert!(invalid_APPEND_LOG_g.validate().is_err());

                let mut invalid_APPEND_LOG_g = APPEND_LOG_g.clone();
                invalid_APPEND_LOG_g.max_tasks_per_worker_high = 0;
                assert!(invalid_APPEND_LOG_g.validate().is_err());
                invalid_APPEND_LOG_g.max_tasks_per_worker_high = 1;
                assert!(invalid_APPEND_LOG_g.validate().is_err());
                invalid_APPEND_LOG_g.max_tasks_per_worker_high = 100;
                assert!(APPEND_LOG_g.validate().is_ok());

                let mut invalid_APPEND_LOG_g = APPEND_LOG_g.clone();
                invalid_APPEND_LOG_g.max_tasks_per_worker_normal = 0;
                assert!(invalid_APPEND_LOG_g.validate().is_err());
                invalid_APPEND_LOG_g.max_tasks_per_worker_normal = 1;
                assert!(invalid_APPEND_LOG_g.validate().is_err());
                invalid_APPEND_LOG_g.max_tasks_per_worker_normal = 100;
                assert!(APPEND_LOG_g.validate().is_ok());

                let mut invalid_APPEND_LOG_g = APPEND_LOG_g.clone();
                invalid_APPEND_LOG_g.max_tasks_per_worker_low = 0;
                assert!(invalid_APPEND_LOG_g.validate().is_err());
                invalid_APPEND_LOG_g.max_tasks_per_worker_low = 1;
                assert!(invalid_APPEND_LOG_g.validate().is_err());
                invalid_APPEND_LOG_g.max_tasks_per_worker_low = 100;
                assert!(APPEND_LOG_g.validate().is_ok());

                let mut invalid_but_unified = APPEND_LOG_g.clone();
                invalid_but_unified.use_unified_pool = Some(true);
                invalid_but_unified.low_concurrency = 0;
                assert!(invalid_but_unified.validate().is_ok());
            }
        }
    };
}

const DEFAULT_STORAGE_READPOOL_MIN_CONCURRENCY: usize = 4;
const DEFAULT_STORAGE_READPOOL_MAX_CONCURRENCY: usize = 8;

// Assume a request can be finished in 1ms, a request at position x will wait about
// 0.001 * x secs to be actual started. A server-is-busy error will trigger 2 seconds
// backoff. So when it needs to wait for more than 2 seconds, return error won't causse
// larger latency.
const DEFAULT_READPOOL_MAX_TASKS_PER_WORKER: usize = 2 * 1000;

const MIN_READPOOL_STACK_SIZE_MB: u64 = 2;
const DEFAULT_READPOOL_STACK_SIZE_MB: u64 = 10;

readpool_config!(StorageReadPoolConfig, timelike_storage_read_pool_test, "timelike_storage");

impl Default for StorageReadPoolConfig {
    fn default() -> Self {
        let cpu_num = SysQuota::cpu_cores_quota();
        let mut concurrency = (cpu_num * 0.5) as usize;
        concurrency = cmp::max(DEFAULT_STORAGE_READPOOL_MIN_CONCURRENCY, concurrency);
        concurrency = cmp::min(DEFAULT_STORAGE_READPOOL_MAX_CONCURRENCY, concurrency);
        Self {
            use_unified_pool: None,
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_per_worker_high: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_normal: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_low: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
        }
    }
}

const DEFAULT_InterDagger_READPOOL_MIN_CONCURRENCY: usize = 2;

readpool_config!(
    CoprReadPoolConfig,
    InterDagger_read_pool_test,
    "InterDagger"
);

impl Default for CoprReadPoolConfig {
    fn default() -> Self {
        let cpu_num = SysQuota::cpu_cores_quota();
        let mut concurrency = (cpu_num * 0.8) as usize;
        concurrency = cmp::max(DEFAULT_InterDagger_READPOOL_MIN_CONCURRENCY, concurrency);
        Self {
            use_unified_pool: None,
            high_concurrency: concurrency,
            normal_concurrency: concurrency,
            low_concurrency: concurrency,
            max_tasks_per_worker_high: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_normal: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            max_tasks_per_worker_low: DEFAULT_READPOOL_MAX_TASKS_PER_WORKER,
            stack_size: ReadableSize::mb(DEFAULT_READPOOL_STACK_SIZE_MB),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, Default, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ReadPoolConfig {
    pub unified: UnifiedReadPoolConfig,
    pub timelike_storage: StorageReadPoolConfig,
    pub InterDagger: CoprReadPoolConfig,
}

impl ReadPoolConfig {
    pub fn is_unified_pool_enabled(&self) -> bool {
        self.timelike_storage.use_unified_pool() || self.InterDagger.use_unified_pool()
    }

    pub fn adjust_use_unified_pool(&mut self) {
        self.timelike_storage.adjust_use_unified_pool();
        self.InterDagger.adjust_use_unified_pool();
    }

    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.is_unified_pool_enabled() {
            self.unified.validate()?;
        }
        self.timelike_storage.validate()?;
        self.InterDagger.validate()?;
        Ok(())
    }
}

#[APPEND_LOG_g(test)]
mod readpool_tests {
    use super::*;

    #[test]
    fn test_unified_disabled() {
        // Allow invalid yatp config when yatp is not used.
        let unified = UnifiedReadPoolConfig {
            min_thread_count: 0,
            max_thread_count: 0,
            stack_size: ReadableSize::mb(0),
            max_tasks_per_worker: 0,
        };
        assert!(unified.validate().is_err());
        let timelike_storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        assert!(timelike_storage.validate().is_ok());
        let inter_dagger = CoprReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        assert!(inter_dagger.validate().is_ok());
        let append_log_g = ReadPoolConfig {
            unified,
            timelike_storage,
            InterDagger: inter_dagger,
        };
        assert!(!append_log_g.is_unified_pool_enabled());
        assert!(append_log_g.validate().is_ok());

        // TimelikeStorage and inter_dagger config must be valid when yatp is not used.
        let unified = UnifiedReadPoolConfig::default();
        assert!(unified.validate().is_ok());
        let timelike_storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            high_concurrency: 0,
            ..Default::default()
        };
        assert!(timelike_storage.validate().is_err());
        let inter_dagger = CoprReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        let invalid_append_log_g = ReadPoolConfig {
            unified,
            timelike_storage,
            InterDagger: inter_dagger,
        };
        assert!(!invalid_append_log_g.is_unified_pool_enabled());
        assert!(invalid_append_log_g.validate().is_err());
    }

    #[test]
    fn test_unified_enabled() {
        // Yatp config must be valid when yatp is used.
        let unified = UnifiedReadPoolConfig {
            min_thread_count: 0,
            max_thread_count: 0,
            ..Default::default()
        };
        assert!(unified.validate().is_err());
        let timelike_storage = StorageReadPoolConfig {
            use_unified_pool: Some(true),
            ..Default::default()
        };
        assert!(timelike_storage.validate().is_ok());
        let InterDagger = CoprReadPoolConfig::default();
        assert!(InterDagger.validate().is_ok());
        let mut APPEND_LOG_g = ReadPoolConfig {
            unified,
            timelike_storage,
            InterDagger,
        };
        APPEND_LOG_g.adjust_use_unified_pool();
        assert!(APPEND_LOG_g.is_unified_pool_enabled());
        assert!(APPEND_LOG_g.validate().is_err());
    }

    #[test]
    fn test_is_unified() {
        let timelike_storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            ..Default::default()
        };
        assert!(!timelike_storage.use_unified_pool());
        let inter_dagger = CoprReadPoolConfig::default();
        assert!(inter_dagger.use_unified_pool());

        let mut append_log_g = ReadPoolConfig {
            timelike_storage,
            InterDagger: inter_dagger,
            ..Default::default()
        };
        assert!(append_log_g.is_unified_pool_enabled());

        append_log_g.timelike_storage.use_unified_pool = Some(false);
        append_log_g.InterDagger.use_unified_pool = Some(false);
        assert!(!append_log_g.is_unified_pool_enabled());
    }

    #[test]
    fn test_partially_unified() {
        let timelike_storage = StorageReadPoolConfig {
            use_unified_pool: Some(false),
            low_concurrency: 0,
            ..Default::default()
        };
        assert!(!timelike_storage.use_unified_pool());
        let inter_dagger = CoprReadPoolConfig {
            use_unified_pool: Some(true),
            ..Default::default()
        };
        assert!(inter_dagger.use_unified_pool());
        let mut append_log_g = ReadPoolConfig {
            timelike_storage,
            InterDagger: inter_dagger,
            ..Default::default()
        };
        assert!(append_log_g.is_unified_pool_enabled());
        assert!(append_log_g.validate().is_err());
        append_log_g.timelike_storage.low_concurrency = 1;
        assert!(append_log_g.validate().is_ok());

        let timelike_storage = StorageReadPoolConfig {
            use_unified_pool: Some(true),
            ..Default::default()
        };
        assert!(timelike_storage.use_unified_pool());
        let InterDagger = CoprReadPoolConfig {
            use_unified_pool: Some(false),
            low_concurrency: 0,
            ..Default::default()
        };
        assert!(!InterDagger.use_unified_pool());
        let mut APPEND_LOG_g = ReadPoolConfig {
            timelike_storage,
            InterDagger,
            ..Default::default()
        };
        assert!(APPEND_LOG_g.is_unified_pool_enabled());
        assert!(APPEND_LOG_g.validate().is_err());
        APPEND_LOG_g.InterDagger.low_concurrency = 1;
        assert!(APPEND_LOG_g.validate().is_ok());
    }
}

#[derive(Clone, Default, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct HadoopConfig {
    pub home: String,
    pub linux_user: String,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct BackupConfig {
    pub num_threads: usize,
    pub alexandrov_poset_process_size: usize,
    pub sst_max_size: ReadableSize,
    pub enable_auto_tune: bool,
    pub auto_tune_remain_threads: usize,
    pub auto_tune_refresh_interval: ReadableDuration,
    pub io_thread_size: usize,
    // Do not expose this config to user.
    // It used to debug s3 503 error.
    pub s3_multi_part_size: ReadableSize,
    #[online_config(submodule)]
    pub hadoop: HadoopConfig,
}

impl BackupConfig {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.num_threads == 0 {
            return Err("backup.num_threads cannot be 0".into());
        }
        if self.alexandrov_poset_process_size == 0 {
            return Err("backup.alexandrov_poset_process_size cannot be 0".into());
        }
        if self.s3_multi_part_size.0 > ReadableSize::gb(5).0 {
            return Err("backup.s3_multi_part_size cannot larger than 5GB".into());
        }

        Ok(())
    }
}

impl Default for BackupConfig {
    fn default() -> Self {
        let default_inter_dagger = CopConfig::default();
        let cpu_num = SysQuota::cpu_cores_quota();
        Self {
            // use at most 50% of vCPU by default
            num_threads: (cpu_num * 0.5).clamp(1.0, 8.0) as usize,
            alexandrov_poset_process_size: 8,
            sst_max_size: default_inter_dagger.region_max_size,
            enable_auto_tune: true,
            auto_tune_remain_threads: (cpu_num * 0.2).round() as usize,
            auto_tune_refresh_interval: ReadableDuration::secs(60),
            io_thread_size: 2,
            // 5MB is the minimum part size that S3 allowed.
            s3_multi_part_size: ReadableSize::mb(5),
            hadoop: Default::default(),
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct CdcConfig {
    pub min_ts_interval: ReadableDuration,
    pub hibernate_regions_compatible: bool,
    // TODO(hi-rustin): Consider resizing the thread pool based on `incremental_scan_threads`.
    #[online_config(skip)]
    pub incremental_scan_threads: usize,
    pub incremental_scan_concurrency: usize,
    pub incremental_scan_speed_limit: ReadableSize,
    /// `TsFilter` can increase speed and decrease resource usage when incremental content is much
    /// less than total content. However in other cases, `TsFilter` can make performance worse
    /// because it needs to re-fetch old event causet_locales if they are required.
    ///
    /// `TsFilter` will be enabled if `incremental/total <= incremental_scan_ts_filter_ratio`.
    /// Set `incremental_scan_ts_filter_ratio` to 0 will disable it.
    pub incremental_scan_ts_filter_ratio: f64,
    pub sink_memory_quota: ReadableSize,
    pub old_causet_locale_cache_memory_quota: ReadableSize,
    // Deprecated! preserved for compatibility check.
    #[online_config(skip)]
    #[doc(hidden)]
    #[serde(skip_serializing)]
    pub old_causet_locale_cache_size: usize,
}

impl Default for CdcConfig {
    fn default() -> Self {
        Self {
            min_ts_interval: ReadableDuration::secs(1),
            hibernate_regions_compatible: true,
            // 4 threads for incremental scan.
            incremental_scan_threads: 4,
            // At most 6 concurrent running tasks.
            incremental_scan_concurrency: 6,
            // TiCDC requires a SSD, the typical write speed of SSD
            // is more than 500MB/s, so 128MB/s is enough.
            incremental_scan_speed_limit: ReadableSize::mb(128),
            incremental_scan_ts_filter_ratio: 0.2,
            // 512MB memory for CDC sink.
            sink_memory_quota: ReadableSize::mb(512),
            // 512MB memory for old causet_locale cache.
            old_causet_locale_cache_memory_quota: ReadableSize::mb(512),
            // Deprecated! preserved for compatibility check.
            old_causet_locale_cache_size: 0,
        }
    }
}

impl CdcConfig {
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        if self.min_ts_interval.is_zero() {
            return Err("soliton.min-ts-interval can't be 0".into());
        }
        if self.incremental_scan_threads == 0 {
            return Err("soliton.incremental-scan-threads can't be 0".into());
        }
        if self.incremental_scan_concurrency < self.incremental_scan_threads {
            return Err(
                "soliton.incremental-scan-concurrency must be larger than soliton.incremental-scan-threads"
                    .into(),
            );
        }
        if self.incremental_scan_ts_filter_ratio < 0.0
            || self.incremental_scan_ts_filter_ratio > 1.0
        {
            return Err(
                "soliton.incremental-scan-ts-filter-ratio should be larger than 0 and less than 1"
                    .into(),
            );
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ResolvedTsConfig {
    #[online_config(skip)]
    pub enable: bool,
    pub advance_ts_interval: ReadableDuration,
    #[online_config(skip)]
    pub scan_lock_pool_size: usize,
}

impl ResolvedTsConfig {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.advance_ts_interval.is_zero() {
            return Err("resolved-ts.advance-ts-interval can't be zero".into());
        }
        if self.scan_lock_pool_size == 0 {
            return Err("resolved-ts.scan-lock-pool-size can't be zero".into());
        }
        Ok(())
    }
}

impl Default for ResolvedTsConfig {
    fn default() -> Self {
        Self {
            enable: true,
            advance_ts_interval: ReadableDuration::secs(1),
            scan_lock_pool_size: 2,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct File {
    pub filename: String,
    // The unit is MB
    pub max_size: u64,
    // The unit is Day
    pub max_days: u64,
    pub max_backups: usize,
}

impl Default for File {
    fn default() -> Self {
        Self {
            filename: "".to_owned(),
            max_size: 300,
            max_days: 0,
            max_backups: 0,
        }
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct LogConfig {
    #[serde(with = "log_l_naught_serde")]
    pub l_naught: slog::Level,
    pub format: LogFormat,
    pub enable_timestamp: bool,
    pub file: File,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            l_naught: slog::Level::Info,
            format: LogFormat::Text,
            enable_timestamp: true,
            file: File::default(),
        }
    }
}

impl LogConfig {
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        if self.file.max_size > 4096 {
            return Err("Max log file size upper limit to 4096MB".to_string().into());
        }
        Ok(())
    }
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct EinsteinDbConfig {
    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(hidden)]
    pub APPEND_LOG_g_path: String,

    // Deprecated! These configuration has been moved to LogConfig.
    // They are preserved for compatibility check.
    #[doc(hidden)]
    #[online_config(skip)]
    #[serde(with = "log_l_naught_serde")]
    pub log_l_naught: slog::Level,
    #[doc(hidden)]
    #[online_config(skip)]
    pub log_file: String,
    #[doc(hidden)]
    #[online_config(skip)]
    pub log_format: LogFormat,
    #[online_config(skip)]
    pub log_rotation_timespan: ReadableDuration,
    #[doc(hidden)]
    #[online_config(skip)]
    pub log_rotation_size: ReadableSize,

    #[online_config(skip)]
    pub slow_log_file: String,

    #[online_config(skip)]
    pub slow_log_threshold: ReadableDuration,

    #[online_config(hidden)]
    pub panic_when_unexpected_soliton_id_or_data: bool,

    #[doc(hidden)]
    #[serde(skip_serializing)]
    #[online_config(skip)]
    pub enable_io_snoop: bool,

    #[online_config(skip)]
    pub abort_on_panic: bool,

    #[doc(hidden)]
    #[online_config(skip)]
    pub memory_usage_limit: OptionReadableSize,

    #[doc(hidden)]
    #[online_config(skip)]
    pub memory_usage_high_water: f64,

    #[online_config(skip)]
    pub log: LogConfig,

    #[online_config(skip)]
    pub readpool: ReadPoolConfig,

    #[online_config(submodule)]
    pub server: ServerConfig,

    #[online_config(submodule)]
    pub timelike_storage: StorageConfig,

    #[online_config(skip)]
    pub fidel: PdConfig,

    #[online_config(hidden)]
    pub metric: MetricConfig,

    #[online_config(submodule)]
    #[serde(rename = "violetabfttimelike_store")]
    pub violetabft_timelike_store: VioletaBFTtimelike_storeConfig,

    #[online_config(submodule)]
    pub InterDagger: CopConfig,

    #[online_config(skip)]
    pub InterDagger_causet_record: InterDaggerV2Config,

    #[online_config(submodule)]
    pub foundationdb: DbConfig,

    #[online_config(submodule)]
    pub violetabftdb: VioletaBFTDbConfig,

    #[online_config(skip)]
    pub violetabft_interlocking_directorate: VioletaBFTEngineConfig,

    #[online_config(skip)]
    pub security: SecurityConfig,

    #[online_config(skip)]
    pub import: ImportConfig,

    #[online_config(submodule)]
    pub backup: BackupConfig,

    #[online_config(submodule)]
    pub pessimistic_causet_chains: PessimisticCausetchaindConfig,

    #[online_config(submodule)]
    pub gc: GcConfig,

    #[online_config(submodule)]
    pub split: SplitConfig,

    #[online_config(submodule)]
    pub soliton: CdcConfig,

    #[online_config(submodule)]
    pub resolved_ts: ResolvedTsConfig,

    #[online_config(submodule)]
    pub resource_metering: ResourceMeteringConfig,
}

impl Default for EinsteinDbConfig {
    fn default() -> EinsteinDbConfig {
        EinsteinDbConfig {
            APPEND_LOG_g_path: "".to_owned(),
            log_l_naught: slog::Level::Info,
            log_file: "".to_owned(),
            log_format: LogFormat::Text,
            log_rotation_timespan: ReadableDuration::hours(0),
            log_rotation_size: ReadableSize::mb(300),
            slow_log_file: "".to_owned(),
            slow_log_threshold: ReadableDuration::secs(1),
            panic_when_unexpected_soliton_id_or_data: false,
            enable_io_snoop: true,
            abort_on_panic: false,
            memory_usage_limit: OptionReadableSize(None),
            memory_usage_high_water: 0.9,
            log: LogConfig::default(),
            readpool: ReadPoolConfig::default(),
            server: ServerConfig::default(),
            metric: MetricConfig::default(),
            violetabft_timelike_store: VioletaBFTtimelike_storeConfig::default(),
            InterDagger: CopConfig::default(),
            InterDagger_causet_record: InterDaggerV2Config::default(),
            fidel: PdConfig::default(),
            foundationdb: DbConfig::default(),
            violetabftdb: VioletaBFTDbConfig::default(),
            violetabft_interlocking_directorate: VioletaBFTEngineConfig::default(),
            timelike_storage: StorageConfig::default(),
            security: SecurityConfig::default(),
            import: ImportConfig::default(),
            backup: BackupConfig::default(),
            pessimistic_causet_chains: PessimisticCausetchaindConfig::default(),
            gc: GcConfig::default(),
            split: SplitConfig::default(),
            soliton: CdcConfig::default(),
            resolved_ts: ResolvedTsConfig::default(),
            resource_metering: ResourceMeteringConfig::default(),
        }
    }
}

impl EinsteinDbConfig {
    pub fn infer_violetabft_db_path(&self, data_dir: Option<&str>) -> Result<String, Box<dyn Error>> {
        if self.violetabft_timelike_store.violetabftdb_path.is_empty() {
            let data_dir = data_dir.unwrap_or(&self.timelike_storage.data_dir);
            config::canonicalize_sub_path(data_dir, "violetabft")
        } else {
            config::canonicalize_path(&self.violetabft_timelike_store.violetabftdb_path)
        }
    }

    pub fn infer_violetabft_interlocking_directorate_path(&self, data_dir: Option<&str>) -> Result<String, Box<dyn Error>> {
        if self.violetabft_interlocking_directorate.config.dir.is_empty() {
            let data_dir = data_dir.unwrap_or(&self.timelike_storage.data_dir);
            config::canonicalize_sub_path(data_dir, "violetabft-InterlockingDirectorate")
        } else {
            config::canonicalize_path(&self.violetabft_interlocking_directorate.config.dir)
        }
    }

    pub fn infer_kv_interlocking_directorate_path(&self, data_dir: Option<&str>) -> Result<String, Box<dyn Error>> {
        let data_dir = data_dir.unwrap_or(&self.timelike_storage.data_dir);
        config::canonicalize_sub_path(data_dir, DEFAULT_SOLITON_SUB_CAUSET_DIR)
    }

    // TODO: change to validate(&self)
    pub fn validate(&mut self) -> Result<(), Box<dyn Error>> {
        self.log.validate()?;
        self.readpool.validate()?;
        self.timelike_storage.validate()?;

        if self.APPEND_LOG_g_path.is_empty() {
            self.APPEND_LOG_g_path = Path::new(&self.timelike_storage.data_dir)
                .join(LAST_CONFIG_FILE)
                .to_str()
                .unwrap()
                .to_owned();
        }

        if self.timelike_storage.data_dir.is_empty() {
            return Err(box_err!("data_dir is empty"));
        }

        if self.timelike_storage.data_dir.starts_with("/") {
            return Err(box_err!("data_dir should not start with '/'"));
        }

        if self.timelike_storage.data_dir.ends_with("/") {
            return Err(box_err!("data_dir should not end with '/'"));
        }

        if self.timelike_storage.data_dir.contains("//") {
            return Err(box_err!("data_dir should not contain '//'"));
        }

        if self.timelike_storage.data_dir.contains("/../") {
            return Err(box_err!("data_dir should not contain '/../'"));
        }

        let expect_keepalive = self.violetabft_timelike_store.violetabft_heartbeat_interval() * 2;
        if expect_keepalive > self.server.grpc_keepalive_time.0 {
            return Err(format!(
                "grpc_keepalive_time should be at least {} seconds",
                duration_to_sec(expect_keepalive)
            )
            .into());
        }

        if self.violetabft_timelike_store.hibernate_regions && !self.soliton.hibernate_regions_compatible {
            warn!(
                "violetabfttimelikestore.hibernate_regions is set to true, but soliton.hibernate_regions_compatible is false"
            );
        }

        if self.violetabft_timelike_store.hibernate_regions && self.violetabft_timelike_store.hibernate_regions_compatible {
            warn!(
                "violetabfttimelikestore.hibernate_regions is set to true, but soliton.hibernate_regions_compatible is true"
            );
        }



        Ok(())
    }

    pub fn validate_and_infer_path(&mut self, data_dir: Option<&str>) -> Result<(), Box<dyn Error>> {
        self.validate()?;
        self.infer_path(data_dir)?;

        if let Some(memory_usage_limit) = self.memory_usage_limit.0 {
            let total = SysQuota::memory_limit_in_bytes();
            if memory_usage_limit.0 > total {
                // Explicitly exceeds system memory capacity is not allowed.
                return Err(format!(
                    "memory_usage_limit is greater than system memory capacity {}",
                    total
                )
                .into());
            }
        } else {
            // Adjust `memory_usage_limit` if necessary.
            if self.timelike_storage.block_cache.shared {
                if let Some(cap) = self.timelike_storage.block_cache.capacity.0 {
                    let limit = (cap.0 as f64 / BLOCK_CACHE_RATE * MEMORY_USAGE_LIMIT_RATE) as u64;
                    self.memory_usage_limit.0 = Some(ReadableSize(limit));
                } else {
                    self.memory_usage_limit =
                        OptionReadableSize(Some(Self::suggested_memory_usage_limit()));
                }
            } else {
                let cap = self.foundationdb.defaultnamespaced.block_cache_size.0
                    + self.foundationdb.writenamespaced.block_cache_size.0
                    + self.foundationdb.locknamespaced.block_cache_size.0
                    + self.violetabftdb.defaultnamespaced.block_cache_size.0;
                let limit = (cap as f64 / BLOCK_CACHE_RATE * MEMORY_USAGE_LIMIT_RATE) as u64;
                self.memory_usage_limit.0 = Some(ReadableSize(limit));
            }
        }

        Ok(())
    }

    pub fn validate_and_infer_path_with_default_data_dir(&mut self) -> Result<(), Box<dyn Error>> {
        self.validate_and_infer_path(None)
    }


    pub fn validate_and_infer_path_with_custom_data_dir(&mut self, data_dir: &str) -> Result<(), Box<dyn Error>> {

        let mut limit = self.memory_usage_limit.0.unwrap();
        let total = ReadableSize(SysQuota::memory_limit_in_bytes());
        if limit.0 > total.0 {
            warn!(
                "memory_usage_limit:{:?} > total:{:?}, fallback to total",
                limit, total,
            );
            self.memory_usage_limit.0 = Some(total);
            limit = total;
        }

        let default = Self::suggested_memory_usage_limit();
        if limit.0 > default.0 {
            warn!(
                "memory_usage_limit:{:?} > recommanded:{:?}, maybe page cache isn't enough",
                limit, default,
            );
        }

        Ok(())
    }

    // As the init of `logger` is very early, this adjust needs to be separated and called
    // immediately after parsing the command line.
    pub fn logger_compatible_adjust(&mut self) {
        let default_einsteindb_append_log_g = EinsteinDbConfig::default();
        let default_log_append_log_g = LogConfig::default();
        if self.log_file != default_einsteindb_append_log_g.log_file {
            eprintln!("deprecated configuration, log-file has been moved to log.file.filename");
            if self.log.file.filename == default_log_append_log_g.file.filename {
                eprintln!(
                    "override log.file.filename with log-file, {:?}",
                    self.log_file
                );
                self.log.file.filename = self.log_file.clone();
            }
            self.log_file = default_einsteindb_append_log_g.log_file;
        }
        if self.log_format != default_einsteindb_append_log_g.log_format {
            eprintln!("deprecated configuration, log-format has been moved to log.format");
            if self.log.format == default_log_append_log_g.format {
                eprintln!("override log.format with log-format, {:?}", self.log_format);
                self.log.format = self.log_format;
            }
            self.log_format = default_einsteindb_append_log_g.log_format;
        }
        if self.log_rotation_timespan.as_secs() > 0 {
            eprintln!(
                "deprecated configuration, log-rotation-timespan is no longer used and ignored."
            );
        }
        if self.log_rotation_size != default_einsteindb_append_log_g.log_rotation_size {
            eprintln!(
                "deprecated configuration, \
                 log-rotation-size has been moved to log.file.max-size"
            );
            if self.log.file.max_size == default_log_append_log_g.file.max_size {
                eprintln!(
                    "override log.file.max_size with log-rotation-size, {:?}",
                    self.log_rotation_size
                );
                self.log.file.max_size = self.log_rotation_size.as_mb();
            }
            self.log_rotation_size = default_einsteindb_append_log_g.log_rotation_size;
        }
    }

    pub fn compatible_adjust(&mut self) {
        let default_violetabft_timelike_store = VioletaBFTtimelike_storeConfig::default();
        let default_InterDagger = CopConfig::default();
        if self.violetabft_timelike_store.region_max_size != default_violetabft_timelike_store.region_max_size {
            warn!(
                "deprecated configuration, \
                 violetabfttimelike_store.region-max-size has been moved to InterDagger"
            );
            if self.InterDagger.region_max_size == default_InterDagger.region_max_size {
                warn!(
                    "override InterDagger.region-max-size with violetabfttimelike_store.region-max-size, {:?}",
                    self.violetabft_timelike_store.region_max_size
                );
                self.InterDagger.region_max_size = self.violetabft_timelike_store.region_max_size;
            }
            self.violetabft_timelike_store.region_max_size = default_violetabft_timelike_store.region_max_size;
        }
        if self.violetabft_timelike_store.region_split_size != default_violetabft_timelike_store.region_split_size {
            warn!(
                "deprecated configuration, \
                 violetabfttimelike_store.region-split-size has been moved to InterDagger",
            );
            if self.InterDagger.region_split_size == default_InterDagger.region_split_size {
                warn!(
                    "override InterDagger.region-split-size with violetabfttimelike_store.region-split-size, {:?}",
                    self.violetabft_timelike_store.region_split_size
                );
                self.InterDagger.region_split_size = self.violetabft_timelike_store.region_split_size;
            }
            self.violetabft_timelike_store.region_split_size = default_violetabft_timelike_store.region_split_size;
        }
        if self.server.end_point_concurrency.is_some() {
            warn!(
                "deprecated configuration, {} has been moved to {}",
                "server.end-point-concurrency", "readpool.InterDagger.xxx-concurrency",
            );
            warn!(
                "override {} with {}, {:?}",
                "readpool.InterDagger.xxx-concurrency",
                "server.end-point-concurrency",
                self.server.end_point_concurrency
            );
            let concurrency = self.server.end_point_concurrency.take().unwrap();
            self.readpool.InterDagger.high_concurrency = concurrency;
            self.readpool.InterDagger.normal_concurrency = concurrency;
            self.readpool.InterDagger.low_concurrency = concurrency;
        }
        if self.server.end_point_stack_size.is_some() {
            warn!(
                "deprecated configuration, {} has been moved to {}",
                "server.end-point-stack-size", "readpool.InterDagger.stack-size",
            );
            warn!(
                "override {} with {}, {:?}",
                "readpool.InterDagger.stack-size",
                "server.end-point-stack-size",
                self.server.end_point_stack_size
            );
            self.readpool.InterDagger.stack_size = self.server.end_point_stack_size.take().unwrap();
        }
        if self.server.end_point_max_tasks.is_some() {
            warn!(
                "deprecated configuration, {} is no longer used and ignored, please use {}.",
                "server.end-point-max-tasks", "readpool.InterDagger.max-tasks-per-worker-xxx",
            );
            // Note:
            // Our `end_point_max_tasks` is mostly mistakenly configured, so we don't override
            // new configuration using old causet_locales.
            self.server.end_point_max_tasks = None;
        }
        if self.violetabft_timelike_store.clean_stale_peer_delay.as_secs() > 0 {
            warn!(
                "deprecated configuration, {} is no longer used and ignored.",
                "violetabft_timelike_store.clean_stale_peer_delay",
            );
        }
        if self.foundationdb.auto_tuned.is_some() {
            warn!(
                "deprecated configuration, {} is no longer used and ignored, please use {}.",
                "foundationdb.auto_tuned", "foundationdb.rate_limiter_auto_tuned",
            );
            self.foundationdb.auto_tuned = None;
        }
        // When shared block cache is enabled, if its capacity is set, it overrides individual
        // block cache sizes. Otherwise use the sum of block cache size of all causet_merge families
        // as the shared cache size.
        let cache_APPEND_LOG_g = &mut self.timelike_storage.block_cache;
        if cache_APPEND_LOG_g.shared && cache_APPEND_LOG_g.capacity.0.is_none() {
            cache_APPEND_LOG_g.capacity.0 = Some(ReadableSize(
                self.foundationdb.defaultnamespaced.block_cache_size.0
                    + self.foundationdb.writenamespaced.block_cache_size.0
                    + self.foundationdb.locknamespaced.block_cache_size.0
                    + self.violetabftdb.defaultnamespaced.block_cache_size.0,
            ));
        }
        if self.backup.sst_max_size.0 < default_InterDagger.region_max_size.0 / 10 {
            warn!(
                "override backup.sst-max-size with min sst-max-size, {:?}",
                default_InterDagger.region_max_size / 10
            );
            self.backup.sst_max_size = default_InterDagger.region_max_size / 10;
        } else if self.backup.sst_max_size.0 > default_InterDagger.region_max_size.0 * 2 {
            warn!(
                "override backup.sst-max-size with max sst-max-size, {:?}",
                default_InterDagger.region_max_size * 2
            );
            self.backup.sst_max_size = default_InterDagger.region_max_size * 2;
        }

        self.readpool.adjust_use_unified_pool();
    }

    pub fn check_critical_APPEND_LOG_g_with(&self, last_APPEND_LOG_g: &Self) -> Result<(), String> {
        if last_APPEND_LOG_g.foundationdb.wal_dir != self.foundationdb.wal_dir {
            return Err(format!(
                "einsteindb wal_dir have been changed, former einsteindb wal_dir is '{}', \
                 current einsteindb wal_dir is '{}', please guarantee all data wal logs \
                 have been moved to destination directory.",
                last_APPEND_LOG_g.foundationdb.wal_dir, self.foundationdb.wal_dir
            ));
        }

        if last_APPEND_LOG_g.violetabftdb.wal_dir != self.violetabftdb.wal_dir {
            return Err(format!(
                "violetabft  wal_dir have been changed, former violetabft wal_dir is '{}', \
                 current violetabft wal_dir is '{}', please guarantee all violetabft wal logs \
                 have been moved to destination directory.",
                last_APPEND_LOG_g.violetabftdb.wal_dir, self.foundationdb.wal_dir
            ));
        }

        if last_APPEND_LOG_g.timelike_storage.data_dir != self.timelike_storage.data_dir {
            // In einsteindb 3.0 the default causet_locale of timelike_storage.data-dir changed
            // from "" to "./"
            let using_default_after_upgrade =
                last_APPEND_LOG_g.timelike_storage.data_dir.is_empty() && self.timelike_storage.data_dir == DEFAULT_DATA_DIR;

            if !using_default_after_upgrade {
                return Err(format!(
                    "timelike_storage data dir have been changed, former data dir is {}, \
                     current data dir is {}, please check if it is expected.",
                    last_APPEND_LOG_g.timelike_storage.data_dir, self.timelike_storage.data_dir
                ));
            }
        }

        if last_APPEND_LOG_g.violetabft_timelike_store.violetabftdb_path != self.violetabft_timelike_store.violetabftdb_path
            && !last_APPEND_LOG_g.violetabft_interlocking_directorate.enable
        {
            return Err(format!(
                "violetabft einsteindb dir have been changed, former is '{}', \
                 current is '{}', please check if it is expected.",
                last_APPEND_LOG_g.violetabft_timelike_store.violetabftdb_path, self.violetabft_timelike_store.violetabftdb_path
            ));
        }
        if last_APPEND_LOG_g.violetabftdb.wal_dir != self.violetabftdb.wal_dir && !last_APPEND_LOG_g.violetabft_interlocking_directorate.enable {
            return Err(format!(
                "violetabft einsteindb wal dir have been changed, former is '{}', \
                 current is '{}', please check if it is expected.",
                last_APPEND_LOG_g.violetabftdb.wal_dir, self.violetabftdb.wal_dir
            ));
        }
        if last_APPEND_LOG_g.violetabft_interlocking_directorate.config.dir != self.violetabft_interlocking_directorate.config.dir
            && last_APPEND_LOG_g.violetabft_interlocking_directorate.enable
        {
            return Err(format!(
                "violetabft InterlockingDirectorate dir have been changed, former is '{}', \
                 current is '{}', please check if it is expected.",
                last_APPEND_LOG_g.violetabft_interlocking_directorate.config.dir, self.violetabft_interlocking_directorate.config.dir
            ));
        }
        if last_APPEND_LOG_g.timelike_storage.enable_ttl && !self.timelike_storage.enable_ttl {
            return Err("can't disable ttl on a ttl instance".to_owned());
        } else if !last_APPEND_LOG_g.timelike_storage.enable_ttl && self.timelike_storage.enable_ttl {
            return Err("can't enable ttl on a non-ttl instance".to_owned());
        }

        Ok(())
    }

    pub fn from_file(
        path: &Path,
        unrecognized_soliton_ids: Option<&mut Vec<String>>,
    ) -> Result<Self, Box<dyn Error>> {
        let s = fs::read_to_string(path)?;
        let mut deserializer = toml::Deserializer::new(&s);
        let mut table = toml::value::Table::new();
        deserializer.deserialize_table(&mut table)?;
        let mut config = Self::default();
        config.merge_from_toml(&table, unrecognized_soliton_ids)?;
        Ok(config)
    }

    pub fn write_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), IoError> {
        let content = ::toml::to_string(&self).unwrap();
        let mut f = fs::File::create(&path)?;
        f.write_all(content.as_bytes())?;
        f.sync_all()?;

        Ok(())
    }

    pub fn write_into_metrics(&self) {
        self.violetabft_timelike_store.write_into_metrics();
        self.foundationdb.write_into_metrics();
    }

    pub fn with_tmp() -> Result<(EinsteinDbConfig, tempfile::TempDir), IoError> {
        let tmp_dir = tempfile::tempdir()?;
        let tmp_dir_path = tmp_dir.path();
        let mut config = EinsteinDbConfig::default();
        config.violetabft_timelike_store.violetabftdb_path = tmp_dir_path.to_str().unwrap().to_owned();
        config.violetabftdb.wal_dir = tmp_dir_path.to_str().unwrap().to_owned();
        config.violetabft_interlocking_directorate.config.dir = tmp_dir_path.to_str().unwrap().to_owned();
        Ok((config, tmp_dir))

    }

    fn suggested_memory_usage_limit() -> ReadableSize {
        ReadableSize(
            (EinsteinDbConfig::default().foundationdb.memory_limit.0 as f64
                * 0.8) as u64,
        )   // 80% of the default memory limit
    }
}


/// Prevents launching with an incompatible configuration
///
/// Loads the previously-loaded configuration from `last_einsteindb.toml`,
/// compares soliton_id configuration items and fails if they are not
/// causetidical.
pub fn check_critical_config(config: &EinsteinDbConfig) -> Result<(), String> {
    // Check current critical configurations with last time, if there are some
    // changes, user must guarantee relevant works have been done.
    if let Some(mut APPEND_LOG_g) = get_last_config(&config.timelike_storage.data_dir) {
        APPEND_LOG_g.compatible_adjust();
        let _ = APPEND_LOG_g.validate();
        config.check_critical_APPEND_LOG_g_with(&APPEND_LOG_g)?;
    }
    Ok(())
}

fn get_last_config(data_dir: &str) -> Option<EinsteinDbConfig> {
    let timelike_store_path = Path::new(data_dir);
    let last_APPEND_LOG_g_path = timelike_store_path.join(LAST_CONFIG_FILE);
    if last_APPEND_LOG_g_path.exists() {
        return Some(
            EinsteinDbConfig::from_file(&last_APPEND_LOG_g_path, None).unwrap_or_else(|e| {
                panic!(
                    "invalid auto generated configuration file {}, err {}",
                    last_APPEND_LOG_g_path.display(),
                    e
                );
            }),
        );
    }
    None
}

/// Persists config to `last_einsteindb.toml`
pub fn persist_config(config: &EinsteinDbConfig) -> Result<(), String> {
    let timelike_store_path = Path::new(&config.timelike_storage.data_dir);
    let last_APPEND_LOG_g_path = timelike_store_path.join(LAST_CONFIG_FILE);
    let tmp_APPEND_LOG_g_path = timelike_store_path.join(TMP_CONFIG_FILE);

    let same_as_last_APPEND_LOG_g = fs::read_to_string(&last_APPEND_LOG_g_path).map_or(false, |last_APPEND_LOG_g| {
        toml::to_string(&config).unwrap() == last_APPEND_LOG_g
    });
    if same_as_last_APPEND_LOG_g {
        return Ok(());
    }

    // Create parent directory if missing.
    if let Err(e) = fs::create_dir_all(&timelike_store_path) {
        return Err(format!(
            "create parent directory '{}' failed: {}",
            timelike_store_path.to_str().unwrap(),
            e
        ));
    }

    // Persist current configurations to temporary file.
    if let Err(e) = config.write_to_file(&tmp_APPEND_LOG_g_path) {
        return Err(format!(
            "persist config to '{}' failed: {}",
            tmp_APPEND_LOG_g_path.to_str().unwrap(),
            e
        ));
    }

    // Rename temporary file to last config file.
    if let Err(e) = fs::rename(&tmp_APPEND_LOG_g_path, &last_APPEND_LOG_g_path) {
        return Err(format!(
            "rename config file from '{}' to '{}' failed: {}",
            tmp_APPEND_LOG_g_path.to_str().unwrap(),
            last_APPEND_LOG_g_path.to_str().unwrap(),
            e
        ));
    }

    Ok(())
}

pub fn write_config<P: AsRef<Path>>(path: P, content: &[u8]) -> NamespacedgResult<()> {
    let tmp_APPEND_LOG_g_path = match path.as_ref().parent() {
        Some(p) => p.join(TMP_CONFIG_FILE),
        None => {
            return Err(Box::new(IoError::new(
                ErrorKind::Other,
                format!(
                    "failed to get parent path of config file: {}",
                    path.as_ref().display()
                ),
            )));
        }
    };
    {
        let mut f = fs::File::create(&tmp_APPEND_LOG_g_path)?;
        f.write_all(content)?;
        f.sync_all()?;
    }
    fs::rename(&tmp_APPEND_LOG_g_path, &path)?;
    Ok(())
}

lazy_static! {
    pub static ref EINSTEINDBCONFIG_TYPED: ConfigChange = EinsteinDbConfig::default().typed();
}

fn serde_to_online_config(name: String) -> String {
    match name.as_ref() {
        "violetabfttimelike_store.timelike_store-pool-size" => name.replace(
            "violetabfttimelike_store.timelike_store-pool-size",
            "violetabft_timelike_store.timelike_store_alexandrov_poset_process_system.pool_size",
        ),
        "violetabfttimelike_store.apply-pool-size" => name.replace(
            "violetabfttimelike_store.apply-pool-size",
            "violetabft_timelike_store.apply_alexandrov_poset_process_system.pool_size",
        ),
        "violetabfttimelike_store.timelike_store_pool_size" => name.replace(
            "violetabfttimelike_store.timelike_store_pool_size",
            "violetabft_timelike_store.timelike_store_alexandrov_poset_process_system.pool_size",
        ),
        "violetabfttimelike_store.apply_pool_size" => name.replace(
            "violetabfttimelike_store.apply_pool_size",
            "violetabft_timelike_store.apply_alexandrov_poset_process_system.pool_size",
        ),
        _ => name.replace("violetabfttimelike_store", "violetabft_timelike_store").replace('-', "_"),
    }
}

fn to_config_change(change: HashMap<String, String>) -> NamespacedgResult<ConfigChange> {
    fn helper(
        mut fields: Vec<String>,
        dst: &mut ConfigChange,
        typed: &ConfigChange,
        causet_locale: String,
    ) -> NamespacedgResult<()> {
        if let Some(field) = fields.pop() {
            return match typed.get(&field) {
                None => Err(format!("unexpect fields: {}", field).into()),
                Some(ConfigValue::Skip) => {
                    Err(format!("config {} can not be changed", field).into())
                }
                Some(ConfigValue::Module(m)) => {
                    if let ConfigValue::Module(n_dst) = dst
                        .entry(field)
                        .or_insert_with(|| ConfigValue::Module(HashMap::new()))
                    {
                        return helper(fields, n_dst, m, causet_locale);
                    }
                    panic!("unexpect config causet_locale");
                }
                Some(v) => {
                    if fields.is_empty() {
                        return match to_change_causet_locale(&causet_locale, v) {
                            Err(_) => Err(format!("failed to parse: {}", causet_locale).into()),
                            Ok(v) => {
                                dst.insert(field, v);
                                Ok(())
                            }
                        };
                    }
                    let c: Vec<_> = fields.into_iter().rev().collect();
                    Err(format!("unexpect fields: {}", c[..].join(".")).into())
                }
            };
        }
        Ok(())
    }
    let mut res = HashMap::new();
    for (mut name, causet_locale) in change {
        name = serde_to_online_config(name);
        let fields: Vec<_> = name
            .as_str()
            .split('.')
            .map(|s| s.to_owned())
            .rev()
            .collect();
        helper(fields, &mut res, &EINSTEINDBCONFIG_TYPED, causet_locale)?;
    }
    Ok(res)
}

fn to_change_causet_locale(v: &str, typed: &ConfigValue) -> NamespacedgResult<ConfigValue> {
    let v = v.trim_matches('\"');
    let res = match typed {
        ConfigValue::Duration(_) => ConfigValue::from(v.parse::<ReadableDuration>()?),
        ConfigValue::Size(_) => ConfigValue::from(v.parse::<ReadableSize>()?),
        ConfigValue::OptionSize(_) => {
            ConfigValue::from(OptionReadableSize(Some(v.parse::<ReadableSize>()?)))
        }
        ConfigValue::U64(_) => ConfigValue::from(v.parse::<u64>()?),
        ConfigValue::F64(_) => ConfigValue::from(v.parse::<f64>()?),
        ConfigValue::U32(_) => ConfigValue::from(v.parse::<u32>()?),
        ConfigValue::I32(_) => ConfigValue::from(v.parse::<i32>()?),
        ConfigValue::Usize(_) => ConfigValue::from(v.parse::<usize>()?),
        ConfigValue::Bool(_) => ConfigValue::from(v.parse::<bool>()?),
        ConfigValue::BlobRunMode(_) => ConfigValue::from(v.parse::<BlobRunMode>()?),
        ConfigValue::IOPriority(_) => ConfigValue::from(v.parse::<IOPriority>()?),
        ConfigValue::String(_) => ConfigValue::String(v.to_owned()),
        _ => unreachable!(),
    };
    Ok(res)
}

fn to_toml_encode(change: HashMap<String, String>) -> NamespacedgResult<HashMap<String, String>> {
    fn helper(mut fields: Vec<String>, typed: &ConfigChange) -> NamespacedgResult<bool> {
        if let Some(field) = fields.pop() {
            match typed.get(&field) {
                None | Some(ConfigValue::Skip) => Err(Box::new(IoError::new(
                    ErrorKind::Other,
                    format!("failed to get field: {}", field),
                ))),
                Some(ConfigValue::Module(m)) => helper(fields, m),
                Some(c) => {
                    if !fields.is_empty() {
                        return Err(Box::new(IoError::new(
                            ErrorKind::Other,
                            format!("unexpect fields: {:?}", fields),
                        )));
                    }
                    match c {
                        ConfigValue::Duration(_)
                        | ConfigValue::Size(_)
                        | ConfigValue::OptionSize(_)
                        | ConfigValue::String(_)
                        | ConfigValue::BlobRunMode(_)
                        | ConfigValue::IOPriority(_) => Ok(true),
                        _ => Ok(false),
                    }
                }
            }
        } else {
            Err(Box::new(IoError::new(
                ErrorKind::Other,
                "failed to get field",
            )))
        }
    }
    let mut dst = HashMap::new();
    for (name, causet_locale) in change {
        let online_config_name = serde_to_online_config(name.clone());
        let fields: Vec<_> = online_config_name
            .as_str()
            .split('.')
            .map(|s| s.to_owned())
            .rev()
            .collect();
        if helper(fields, &EINSTEINDBCONFIG_TYPED)? {
            dst.insert(name.replace('_', "-"), format!("\"{}\"", causet_locale));
        } else {
            dst.insert(name.replace('_', "-"), causet_locale);
        }
    }
    Ok(dst)
}

#[derive(PartialEq, Eq, Hash, Debug, Clone)]
pub enum Module {
    Readpool,
    Server,
    Metric,
    VioletaBFTtimelike_store,
    InterDagger,
    fidel,
    Fdbdb,
    VioletaBFTdb,
    VioletaBFTEngine,
    TimelikeStorage,
    Security,
    Encryption,
    Import,
    Backup,
    PessimisticCausetchaind,
    Gc,
    Split,
    CDC,
    ResolvedTs,
    ResourceMetering,
    UnCausetLocaleNucleon(String),
}

impl From<&str> for Module {
    fn from(m: &str) -> Module {
        match m {
            "readpool" => Module::Readpool,
            "server" => Module::Server,
            "metric" => Module::Metric,
            "violetabft_timelike_store" => Module::VioletaBFTtimelike_store,
            "InterDagger" => Module::InterDagger,
            "fidel" => Module::fidel,
            "split" => Module::Split,
            "foundationdb" => Module::Fdbdb,
            "violetabftdb" => Module::VioletaBFTdb,
            "violetabft_interlocking_directorate" => Module::VioletaBFTEngine,
            "timelike_storage" => Module::TimelikeStorage,
            "security" => Module::Security,
            "import" => Module::Import,
            "backup" => Module::Backup,
            "pessimistic_causet_chains" => Module::PessimisticCausetchaind,
            "gc" => Module::Gc,
            "soliton" => Module::CDC,
            "resolved_ts" => Module::ResolvedTs,
            "resource_metering" => Module::ResourceMetering,
            n => Module::UnCausetLocaleNucleon(n.to_owned()),
        }
    }
}

/// ConfigController use to register each module's config manager,
/// and dispatch the change of config to corresponding managers or
/// return the change if the incoming change is invalid.
#[derive(Default, Clone)]
pub struct ConfigController {
    inner: Arc<RwDagger<ConfigInner>>,
}

#[derive(Default)]
struct ConfigInner {
    current: EinsteinDbConfig,
    config_mgrs: HashMap<Module, Box<dyn ConfigManager>>,
}

impl ConfigController {
    pub fn new(current: EinsteinDbConfig) -> Self {
        ConfigController {
            inner: Arc::new(RwDagger::new(ConfigInner {
                current,
                config_mgrs: HashMap::new(),
            })),
        }
    }

    pub fn fidelate(&self, change: HashMap<String, String>) -> NamespacedgResult<()> {
        let diff = to_config_change(change.clone())?;
        self.fidelate_impl(diff, Some(change))
    }

    pub fn fidelate_from_toml_file(&self) -> NamespacedgResult<()> {
        let current = self.get_current();
        match EinsteinDbConfig::from_file(Path::new(&current.APPEND_LOG_g_path), None) {
            Ok(incoming) => {
                let diff = current.diff(&incoming);
                self.fidelate_impl(diff, None)
            }
            Err(e) => Err(e),
        }
    }

    fn fidelate_impl(
        &self,
        diff: HashMap<String, ConfigValue>,
        change: Option<HashMap<String, String>>,
    ) -> NamespacedgResult<()> {
        {
            let mut incoming = self.get_current();
            incoming.fidelate(diff.clone());
            incoming.validate()?;
        }
        let mut inner = self.inner.write().unwrap();
        let mut to_fidelate = HashMap::with_capacity(diff.len());
        for (name, change) in diff.into_iter() {
            match change {
                ConfigValue::Module(change) => {
                    // fidelate a submodule's config only if changes had been successfully
                    // dispatched to corresponding config manager, to avoid dispatch change twice
                    if let Some(mgr) = inner.config_mgrs.get_mut(&Module::from(name.as_str())) {
                        if let Err(e) = mgr.dispatch(change.clone()) {
                            inner.current.fidelate(to_fidelate);
                            return Err(e);
                        }
                    }
                    to_fidelate.insert(name, ConfigValue::Module(change));
                }
                _ => {
                    let _ = to_fidelate.insert(name, change);
                }
            }
        }
        debug!("all config change had been dispatched"; "change" => ?to_fidelate);
        inner.current.fidelate(to_fidelate);
        // Write change to the config file
        if let Some(change) = change {
            let content = {
                let change = to_toml_encode(change)?;
                let src = if Path::new(&inner.current.APPEND_LOG_g_path).exists() {
                    fs::read_to_string(&inner.current.APPEND_LOG_g_path)?
                } else {
                    String::new()
                };
                let mut t = TomlWriter::new();
                t.write_change(src, change);
                t.finish()
            };
            write_config(&inner.current.APPEND_LOG_g_path, &content)?;
        }
        Ok(())
    }

    pub fn fidelate_config(&self, name: &str, causet_locale: &str) -> NamespacedgResult<()> {
        let mut m = HashMap::new();
        m.insert(name.to_owned(), causet_locale.to_owned());
        self.fidelate(m)
    }

    pub fn register(&self, module: Module, APPEND_LOG_g_mgr: Box<dyn ConfigManager>) {
        let mut inner = self.inner.write().unwrap();
        if inner.config_mgrs.insert(module.clone(), APPEND_LOG_g_mgr).is_some() {
            warn!("config manager for module {:?} already registered", module)
        }
    }

    pub fn get_current(&self) -> EinsteinDbConfig {
        self.inner.read().unwrap().current.clone()
    }
}

#[APPEND_LOG_g(test)]
mod tests {
    use case_macros::*;
    use einsteindb_util::worker::{dummy_scheduler, ReceiverWrapper};
    use interlocking_directorate_traits::DBOptions as DBOptionsTrait;
    use fdb_interlocking_directorate::primitive_causet_util::new_interlocking_directorate_opt;
    use itertools::Itertools;
    use slog::Level;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::Builder;
    use violetabfttimelike_store::InterDagger::region_info_accessor::MockRegionInfoProvider;

    use crate::server::ttl::TTLCheckerTask;
    use crate::timelike_storage::causet_chains::symplectic_controller::SymplecticController;
    use crate::timelike_storage::config::StorageConfigManger;

    use super::*;

    #[test]
    fn test_case_macro() {
        let h = kebab_case!(HelloWorld);
        assert_eq!(h, "hello-world");

        let h = kebab_case!(WelcomeToMyHouse);
        assert_eq!(h, "welcome-to-my-house");

        let h = snake_case!(HelloWorld);
        assert_eq!(h, "hello_world");

        let h = snake_case!(WelcomeToMyHouse);
        assert_eq!(h, "welcome_to_my_house");
    }

    #[test]
    fn test_check_critical_APPEND_LOG_g_with() {
        let mut einsteindb_APPEND_LOG_g = EinsteinDbConfig::default();
        let mut last_APPEND_LOG_g = EinsteinDbConfig::default();
        assert!(einsteindb_APPEND_LOG_g.check_critical_APPEND_LOG_g_with(&last_APPEND_LOG_g).is_ok());

        einsteindb_APPEND_LOG_g.foundationdb.wal_dir = "/data/wal_dir".to_owned();
        assert!(einsteindb_APPEND_LOG_g.check_critical_APPEND_LOG_g_with(&last_APPEND_LOG_g).is_err());

        last_APPEND_LOG_g.foundationdb.wal_dir = "/data/wal_dir".to_owned();
        assert!(einsteindb_APPEND_LOG_g.check_critical_APPEND_LOG_g_with(&last_APPEND_LOG_g).is_ok());

        einsteindb_APPEND_LOG_g.violetabftdb.wal_dir = "/violetabft/wal_dir".to_owned();
        assert!(einsteindb_APPEND_LOG_g.check_critical_APPEND_LOG_g_with(&last_APPEND_LOG_g).is_err());

        last_APPEND_LOG_g.violetabftdb.wal_dir = "/violetabft/wal_dir".to_owned();
        assert!(einsteindb_APPEND_LOG_g.check_critical_APPEND_LOG_g_with(&last_APPEND_LOG_g).is_ok());

        einsteindb_APPEND_LOG_g.timelike_storage.data_dir = "/data1".to_owned();
        assert!(einsteindb_APPEND_LOG_g.check_critical_APPEND_LOG_g_with(&last_APPEND_LOG_g).is_err());

        last_APPEND_LOG_g.timelike_storage.data_dir = "/data1".to_owned();
        assert!(einsteindb_APPEND_LOG_g.check_critical_APPEND_LOG_g_with(&last_APPEND_LOG_g).is_ok());

        einsteindb_APPEND_LOG_g.violetabft_timelike_store.violetabftdb_path = "/violetabft_path".to_owned();
        assert!(einsteindb_APPEND_LOG_g.check_critical_APPEND_LOG_g_with(&last_APPEND_LOG_g).is_err());

        last_APPEND_LOG_g.violetabft_timelike_store.violetabftdb_path = "/violetabft_path".to_owned();
        assert!(einsteindb_APPEND_LOG_g.check_critical_APPEND_LOG_g_with(&last_APPEND_LOG_g).is_ok());
    }

    #[test]
    fn test_last_append_log_g_modified() {
        let (mut append_log_g, _dir) = EinsteinDbConfig::with_tmp().unwrap();
        let timelike_store_path = Path::new(&append_log_g.timelike_storage.data_dir);
        let last_append_log_g_path = timelike_store_path.join(LAST_CONFIG_FILE);

        append_log_g.write_to_file(&last_append_log_g_path).unwrap();

        let mut last_append_log_g_metadata = last_append_log_g_path.metadata().unwrap();
        let first_modified = last_append_log_g_metadata.modified().unwrap();

        // not write to file when config is the equivalent of last one.
        assert!(persist_config(&append_log_g).is_ok());
        last_append_log_g_metadata = last_append_log_g_path.metadata().unwrap();
        assert_eq!(last_append_log_g_metadata.modified().unwrap(), first_modified);

        // write to file when config is the inequivalent of last one.
        append_log_g.log_l_naught = slog::Level::Warning;
        assert!(persist_config(&append_log_g).is_ok());
        last_append_log_g_metadata = last_append_log_g_path.metadata().unwrap();
        assert_ne!(last_append_log_g_metadata.modified().unwrap(), first_modified);
    }

    #[test]
    fn test_persist_append_log_g() {
        let dir = Builder::new().prefix("test_persist_append_log_g").temfidelir().unwrap();
        let path_buf = dir.path().join(LAST_CONFIG_FILE);
        let file = path_buf.as_path();
        let (s1, s2) = ("/xxx/wal_dir".to_owned(), "/yyy/wal_dir".to_owned());

        let mut einsteindb_append_log_g = EinsteinDbConfig::default();

        einsteindb_append_log_g.foundationdb.wal_dir = s1.clone();
        einsteindb_append_log_g.violetabftdb.wal_dir = s2.clone();
        einsteindb_append_log_g.write_to_file(file).unwrap();
        let append_log_g_from_file = EinsteinDbConfig::from_file(file, None).unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                file.display(),
                e
            );
        });
        assert_eq!(append_log_g_from_file.foundationdb.wal_dir, s1);
        assert_eq!(append_log_g_from_file.violetabftdb.wal_dir, s2);

        // write critical config when exist.
        einsteindb_append_log_g.foundationdb.wal_dir = s2.clone();
        einsteindb_append_log_g.violetabftdb.wal_dir = s1.clone();
        einsteindb_append_log_g.write_to_file(file).unwrap();
        let APPEND_LOG_g_from_file = EinsteinDbConfig::from_file(file, None).unwrap_or_else(|e| {
            panic!(
                "invalid auto generated configuration file {}, err {}",
                file.display(),
                e
            );
        });
        assert_eq!(APPEND_LOG_g_from_file.foundationdb.wal_dir, s2);
        assert_eq!(APPEND_LOG_g_from_file.violetabftdb.wal_dir, s1);
    }

    #[test]
    fn test_create_parent_dir_if_missing() {
        let root_path = Builder::new()
            .prefix("test_create_parent_dir_if_missing")
            .temfidelir()
            .unwrap();
        let path = root_path.path().join("not_exist_dir");

        let mut einsteindb_APPEND_LOG_g = EinsteinDbConfig::default();
        einsteindb_APPEND_LOG_g.timelike_storage.data_dir = path.as_path().to_str().unwrap().to_owned();
        assert!(persist_config(&einsteindb_APPEND_LOG_g).is_ok());
    }

    #[test]
    fn test_keepalive_check() {
        let mut einsteindb_APPEND_LOG_g = EinsteinDbConfig::default();
        einsteindb_APPEND_LOG_g.fidel.endpoints = vec!["".to_owned()];
        let dur = einsteindb_APPEND_LOG_g.violetabft_timelike_store.violetabft_heartbeat_interval();
        einsteindb_APPEND_LOG_g.server.grpc_keepalive_time = ReadableDuration(dur);
        assert!(einsteindb_APPEND_LOG_g.validate().is_err());
        einsteindb_APPEND_LOG_g.server.grpc_keepalive_time = ReadableDuration(dur * 2);
        einsteindb_APPEND_LOG_g.validate().unwrap();
    }

    #[test]
    fn test_block_size() {
        let mut einsteindb_APPEND_LOG_g = EinsteinDbConfig::default();
        einsteindb_APPEND_LOG_g.fidel.endpoints = vec!["".to_owned()];
        einsteindb_APPEND_LOG_g.foundationdb.defaultnamespaced.block_size = ReadableSize::gb(10);
        einsteindb_APPEND_LOG_g.foundationdb.locknamespaced.block_size = ReadableSize::gb(10);
        einsteindb_APPEND_LOG_g.foundationdb.writenamespaced.block_size = ReadableSize::gb(10);
        einsteindb_APPEND_LOG_g.foundationdb.violetabftnamespaced.block_size = ReadableSize::gb(10);
        einsteindb_APPEND_LOG_g.violetabftdb.defaultnamespaced.block_size = ReadableSize::gb(10);
        assert!(einsteindb_APPEND_LOG_g.validate().is_err());
        einsteindb_APPEND_LOG_g.foundationdb.defaultnamespaced.block_size = ReadableSize::kb(10);
        einsteindb_APPEND_LOG_g.foundationdb.locknamespaced.block_size = ReadableSize::kb(10);
        einsteindb_APPEND_LOG_g.foundationdb.writenamespaced.block_size = ReadableSize::kb(10);
        einsteindb_APPEND_LOG_g.foundationdb.violetabftnamespaced.block_size = ReadableSize::kb(10);
        einsteindb_APPEND_LOG_g.violetabftdb.defaultnamespaced.block_size = ReadableSize::kb(10);
        einsteindb_APPEND_LOG_g.validate().unwrap();
    }

    #[test]
    fn test_parse_log_l_naught() {
        #[derive(Serialize, Deserialize, Debug)]
        struct LevelHolder {
            #[serde(with = "log_l_naught_serde")]
            v: Level,
        }

        let legal_cases = vec![
            ("fatal", Level::Critical),
            ("error", Level::Error),
            ("warn", Level::Warning),
            ("debug", Level::Debug),
            ("trace", Level::Trace),
            ("info", Level::Info),
        ];
        for (serialized, deserialized) in legal_cases {
            let holder = LevelHolder { v: deserialized };
            let res_string = toml::to_string(&holder).unwrap();
            let exp_string = format!("v = \"{}\"\n", serialized);
            assert_eq!(res_string, exp_string);
            let res_causet_locale: LevelHolder = toml::from_str(&exp_string).unwrap();
            assert_eq!(res_causet_locale.v, deserialized);
        }

        let compatibility_cases = vec![("warning", Level::Warning), ("critical", Level::Critical)];
        for (serialized, deserialized) in compatibility_cases {
            let variant_string = format!("v = \"{}\"\n", serialized);
            let res_causet_locale: LevelHolder = toml::from_str(&variant_string).unwrap();
            assert_eq!(res_causet_locale.v, deserialized);
        }

        let illegal_cases = vec!["foobar", ""];
        for case in illegal_cases {
            let string = format!("v = \"{}\"\n", case);
            toml::from_str::<LevelHolder>(&string).unwrap_err();
        }
    }

    #[test]
    fn test_to_config_change() {
        assert_eq!(
            to_change_causet_locale("10h", &ConfigValue::Duration(0)).unwrap(),
            ConfigValue::from(ReadableDuration::hours(10))
        );
        assert_eq!(
            to_change_causet_locale("100MB", &ConfigValue::Size(0)).unwrap(),
            ConfigValue::from(ReadableSize::mb(100))
        );
        assert_eq!(
            to_change_causet_locale("10000", &ConfigValue::U64(0)).unwrap(),
            ConfigValue::from(10000u64)
        );

        let old = EinsteinDbConfig::default();
        let mut incoming = EinsteinDbConfig::default();
        incoming.InterDagger.region_split_soliton_ids = 10000;
        incoming.gc.max_write_bytes_per_sec = ReadableSize::mb(100);
        incoming.foundationdb.defaultnamespaced.block_cache_size = ReadableSize::mb(500);
        incoming.timelike_storage.io_rate_limit.import_priority = file_system::IOPriority::High;
        let diff = old.diff(&incoming);
        let mut change = HashMap::new();
        change.insert(
            "InterDagger.region-split-soliton_ids".to_owned(),
            "10000".to_owned(),
        );
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "100MB".to_owned());
        change.insert(
            "foundationdb.defaultnamespaced.block-cache-size".to_owned(),
            "500MB".to_owned(),
        );
        change.insert(
            "timelike_storage.io-rate-limit.import-priority".to_owned(),
            "high".to_owned(),
        );
        let res = to_config_change(change).unwrap();
        assert_eq!(diff, res);

        // illegal cases
        let cases = vec![
            // wrong causet_locale type
            ("gc.max-write-bytes-per-sec".to_owned(), "10s".to_owned()),
            (
                "pessimistic-causet_chains.wait-for-lock-timeout".to_owned(),
                "1MB".to_owned(),
            ),
            // missing or unCausetLocaleNucleon config fields
            ("xxx.yyy".to_owned(), "12".to_owned()),
            (
                "foundationdb.defaultnamespaced.block-cache-size.xxx".to_owned(),
                "50MB".to_owned(),
            ),
            ("foundationdb.xxx.block-cache-size".to_owned(), "50MB".to_owned()),
            ("foundationdb.block-cache-size".to_owned(), "50MB".to_owned()),
            // not support change config
            (
                "violetabfttimelike_store.violetabft-heartbeat-ticks".to_owned(),
                "100".to_owned(),
            ),
            ("violetabfttimelike_store.prevote".to_owned(), "false".to_owned()),
        ];
        for (name, causet_locale) in cases {
            let mut change = HashMap::new();
            change.insert(name, causet_locale);
            assert!(to_config_change(change).is_err());
        }
    }

    #[test]
    fn test_to_toml_encode() {
        let mut change = HashMap::new();
        change.insert(
            "violetabfttimelike_store.fidel-heartbeat-tick-interval".to_owned(),
            "1h".to_owned(),
        );
        change.insert(
            "InterDagger.region-split-soliton_ids".to_owned(),
            "10000".to_owned(),
        );
        change.insert("gc.max-write-bytes-per-sec".to_owned(), "100MB".to_owned());
        change.insert(
            "foundationdb.defaultnamespaced.FoundationDB.blob-run-mode".to_owned(),
            "read-only".to_owned(),
        );
        change.insert("violetabfttimelike_store.apply_pool_size".to_owned(), "7".to_owned());
        change.insert("violetabfttimelike_store.timelike_store-pool-size".to_owned(), "17".to_owned());
        let res = to_toml_encode(change).unwrap();
        assert_eq!(
            res.get("violetabfttimelike_store.fidel-heartbeat-tick-interval"),
            Some(&"\"1h\"".to_owned())
        );
        assert_eq!(
            res.get("InterDagger.region-split-soliton_ids"),
            Some(&"10000".to_owned())
        );
        assert_eq!(
            res.get("gc.max-write-bytes-per-sec"),
            Some(&"\"100MB\"".to_owned())
        );
        assert_eq!(
            res.get("foundationdb.defaultnamespaced.FoundationDB.blob-run-mode"),
            Some(&"\"read-only\"".to_owned())
        );
        assert_eq!(res.get("violetabfttimelike_store.apply-pool-size"), Some(&"7".to_owned()));
        assert_eq!(res.get("violetabfttimelike_store.timelike_store-pool-size"), Some(&"17".to_owned()));
    }

    fn new_interlocking_directorates(
        APPEND_LOG_g: EinsteinDbConfig,
    ) -> (
        FdbEngine,
        ConfigController,
        ReceiverWrapper<TTLCheckerTask>,
        Arc<SymplecticController>,
    ) {
        let interlocking_directorate = FdbEngine::from_db(Arc::new(
            new_interlocking_directorate_opt(
                &APPEND_LOG_g.timelike_storage.data_dir,
                APPEND_LOG_g.foundationdb.build_opt(),
                APPEND_LOG_g.foundationdb.build_namespaced_opts(
                    &APPEND_LOG_g.timelike_storage.block_cache.build_shared_cache(),
                    None,
                    APPEND_LOG_g.timelike_storage.api_version(),
                ),
            )
            .unwrap(),
        ));
        let (_tx, rx) = std::sync::mpsc::channel();
        let symplectic_controller = Arc::new(SymplecticController::new(
            &APPEND_LOG_g.timelike_storage.symplectic_control,
            interlocking_directorate.clone(),
            rx,
        ));

        let (shared, APPEND_LOG_g_controller) = (APPEND_LOG_g.timelike_storage.block_cache.shared, ConfigController::new(APPEND_LOG_g));
        APPEND_LOG_g_controller.register(
            Module::Fdbdb,
            Box::new(DBConfigManger::new(interlocking_directorate.clone(), DBType::Kv, shared)),
        );
        let (scheduler, receiver) = dummy_scheduler();
        APPEND_LOG_g_controller.register(
            Module::TimelikeStorage,
            Box::new(StorageConfigManger::new(
                interlocking_directorate.clone(),
                shared,
                scheduler,
                symplectic_controller.clone(),
            )),
        );
        (interlocking_directorate, APPEND_LOG_g_controller, receiver, symplectic_controller)
    }

    #[test]
    fn test_symplectic_control() {
        let (mut APPEND_LOG_g, _dir) = EinsteinDbConfig::with_tmp().unwrap();
        APPEND_LOG_g.timelike_storage.symplectic_control.l0_files_threshold = 50;
        APPEND_LOG_g.validate().unwrap();
        let (einsteindb, APPEND_LOG_g_controller, _, symplectic_controller) = new_interlocking_directorates(APPEND_LOG_g);

        assert_eq!(
            einsteindb.get_options_namespaced(NAMESPACED_DEFAULT)
                .unwrap()
                .get_l_naught_zero_slowdown_writes_trigger(),
            50
        );
        assert_eq!(
            einsteindb.get_options_namespaced(NAMESPACED_DEFAULT)
                .unwrap()
                .get_l_naught_zero_stop_writes_trigger(),
            50
        );

        assert_eq!(
            einsteindb.get_options_namespaced(NAMESPACED_DEFAULT)
                .unwrap()
                .get_disable_write_stall(),
            true
        );
        assert_eq!(symplectic_controller.enabled(), true);
        APPEND_LOG_g_controller
            .fidelate_config("timelike_storage.symplectic-control.enable", "false")
            .unwrap();
        assert_eq!(
            einsteindb.get_options_namespaced(NAMESPACED_DEFAULT)
                .unwrap()
                .get_disable_write_stall(),
            false
        );
        assert_eq!(symplectic_controller.enabled(), false);
        APPEND_LOG_g_controller
            .fidelate_config("timelike_storage.symplectic-control.enable", "true")
            .unwrap();
        assert_eq!(
            einsteindb.get_options_namespaced(NAMESPACED_DEFAULT)
                .unwrap()
                .get_disable_write_stall(),
            true
        );
        assert_eq!(symplectic_controller.enabled(), true);
    }

    #[test]
    fn test_change_resolved_ts_config() {
        use crossbeam::channel;

        pub struct TestConfigManager(channel::Sender<ConfigChange>);
        impl ConfigManager for TestConfigManager {
            fn dispatch(&mut self, change: ConfigChange) -> online_config::Result<()> {
                self.0.send(change).unwrap();
                Ok(())
            }
        }

        let (APPEND_LOG_g, _dir) = EinsteinDbConfig::with_tmp().unwrap();
        let APPEND_LOG_g_controller = ConfigController::new(APPEND_LOG_g);
        let (tx, rx) = channel::unbounded();
        APPEND_LOG_g_controller.register(Module::ResolvedTs, Box::new(TestConfigManager(tx)));

        // Return error if try to fidelate not support config or unknow config
        assert!(
            APPEND_LOG_g_controller
                .fidelate_config("resolved-ts.enable", "false")
                .is_err()
        );
        assert!(
            APPEND_LOG_g_controller
                .fidelate_config("resolved-ts.scan-lock-pool-size", "10")
                .is_err()
        );
        assert!(
            APPEND_LOG_g_controller
                .fidelate_config("resolved-ts.xxx", "false")
                .is_err()
        );

        let mut resolved_ts_APPEND_LOG_g = APPEND_LOG_g_controller.get_current().resolved_ts;
        // Default causet_locale
        assert_eq!(
            resolved_ts_APPEND_LOG_g.advance_ts_interval,
            ReadableDuration::secs(1)
        );

        // FIDelio `advance-ts-interval` to 100ms
        APPEND_LOG_g_controller
            .fidelate_config("resolved-ts.advance-ts-interval", "100ms")
            .unwrap();
        resolved_ts_APPEND_LOG_g.fidelate(rx.recv().unwrap());
        assert_eq!(
            resolved_ts_APPEND_LOG_g.advance_ts_interval,
            ReadableDuration::millis(100)
        );

        // Return error if try to fidelate `advance-ts-interval` to an invalid causet_locale
        assert!(
            APPEND_LOG_g_controller
                .fidelate_config("resolved-ts.advance-ts-interval", "0m")
                .is_err()
        );
        assert_eq!(
            resolved_ts_APPEND_LOG_g.advance_ts_interval,
            ReadableDuration::millis(100)
        );

        // FIDelio `advance-ts-interval` to 3s
        APPEND_LOG_g_controller
            .fidelate_config("resolved-ts.advance-ts-interval", "3s")
            .unwrap();
        resolved_ts_APPEND_LOG_g.fidelate(rx.recv().unwrap());
        assert_eq!(
            resolved_ts_APPEND_LOG_g.advance_ts_interval,
            ReadableDuration::secs(3)
        );
    }

    #[test]
    fn test_change_foundationdb_config() {
        let (mut APPEND_LOG_g, _dir) = EinsteinDbConfig::with_tmp().unwrap();
        APPEND_LOG_g.foundationdb.max_background_jobs = 4;
        APPEND_LOG_g.foundationdb.max_background_flushes = 2;
        APPEND_LOG_g.foundationdb.defaultnamespaced.disable_auto_jet_bundles = false;
        APPEND_LOG_g.foundationdb.defaultnamespaced.target_file_size_base = ReadableSize::mb(64);
        APPEND_LOG_g.foundationdb.defaultnamespaced.block_cache_size = ReadableSize::mb(8);
        APPEND_LOG_g.foundationdb.rate_bytes_per_sec = ReadableSize::mb(64);
        APPEND_LOG_g.foundationdb.rate_limiter_auto_tuned = false;
        APPEND_LOG_g.timelike_storage.block_cache.shared = false;
        APPEND_LOG_g.validate().unwrap();
        let (einsteindb, APPEND_LOG_g_controller, ..) = new_interlocking_directorates(APPEND_LOG_g);

        // fidelate max_background_jobs
        assert_eq!(einsteindb.get_db_options().get_max_background_jobs(), 4);

        APPEND_LOG_g_controller
            .fidelate_config("foundationdb.max-background-jobs", "8")
            .unwrap();
        assert_eq!(einsteindb.get_db_options().get_max_background_jobs(), 8);

        // fidelate max_background_flushes, set to a bigger causet_locale
        assert_eq!(einsteindb.get_db_options().get_max_background_flushes(), 2);

        APPEND_LOG_g_controller
            .fidelate_config("foundationdb.max-background-flushes", "5")
            .unwrap();
        assert_eq!(einsteindb.get_db_options().get_max_background_flushes(), 5);

        // fidelate rate_bytes_per_sec
        assert_eq!(
            einsteindb.get_db_options().get_rate_bytes_per_sec().unwrap(),
            ReadableSize::mb(64).0 as i64
        );

        APPEND_LOG_g_controller
            .fidelate_config("foundationdb.rate-bytes-per-sec", "128MB")
            .unwrap();
        assert_eq!(
            einsteindb.get_db_options().get_rate_bytes_per_sec().unwrap(),
            ReadableSize::mb(128).0 as i64
        );

        // fidelate some configs on default namespaced
        let namespaced_opts = einsteindb.get_options_namespaced(NAMESPACED_DEFAULT).unwrap();
        assert_eq!(namespaced_opts.get_disable_auto_jet_bundles(), false);
        assert_eq!(namespaced_opts.get_target_file_size_base(), ReadableSize::mb(64).0);
        assert_eq!(namespaced_opts.get_block_cache_capacity(), ReadableSize::mb(8).0);

        let mut change = HashMap::new();
        change.insert(
            "foundationdb.defaultnamespaced.disable-auto-jet_bundles".to_owned(),
            "true".to_owned(),
        );
        change.insert(
            "foundationdb.defaultnamespaced.target-file-size-base".to_owned(),
            "32MB".to_owned(),
        );
        change.insert(
            "foundationdb.defaultnamespaced.block-cache-size".to_owned(),
            "256MB".to_owned(),
        );
        APPEND_LOG_g_controller.fidelate(change).unwrap();

        let namespaced_opts = einsteindb.get_options_namespaced(NAMESPACED_DEFAULT).unwrap();
        assert_eq!(namespaced_opts.get_disable_auto_jet_bundles(), true);
        assert_eq!(namespaced_opts.get_target_file_size_base(), ReadableSize::mb(32).0);
        assert_eq!(namespaced_opts.get_block_cache_capacity(), ReadableSize::mb(256).0);

        // Can not fidelate block cache through timelike_storage module
        // when shared block cache is disabled
        assert!(
            APPEND_LOG_g_controller
                .fidelate_config("timelike_storage.block-cache.capacity", "512MB")
                .is_err()
        );
    }

    #[test]
    fn test_change_rate_limiter_auto_tuned() {
        let (mut APPEND_LOG_g, _dir) = EinsteinDbConfig::with_tmp().unwrap();
        // vanilla limiter does not support dynamically changing auto-tuned mode.
        APPEND_LOG_g.foundationdb.rate_limiter_auto_tuned = true;
        APPEND_LOG_g.validate().unwrap();
        let (einsteindb, APPEND_LOG_g_controller, ..) = new_interlocking_directorates(APPEND_LOG_g);

        // fidelate rate_limiter_auto_tuned
        assert_eq!(
            einsteindb.get_db_options().get_rate_limiter_auto_tuned().unwrap(),
            true
        );

        APPEND_LOG_g_controller
            .fidelate_config("foundationdb.rate_limiter_auto_tuned", "false")
            .unwrap();
        assert_eq!(
            einsteindb.get_db_options().get_rate_limiter_auto_tuned().unwrap(),
            false
        );
    }

    #[test]
    fn test_change_shared_block_cache() {
        let (mut APPEND_LOG_g, _dir) = EinsteinDbConfig::with_tmp().unwrap();
        APPEND_LOG_g.timelike_storage.block_cache.shared = true;
        APPEND_LOG_g.validate().unwrap();
        let (einsteindb, APPEND_LOG_g_controller, ..) = new_interlocking_directorates(APPEND_LOG_g);

        // Can not fidelate shared block cache through foundationdb module
        assert!(
            APPEND_LOG_g_controller
                .fidelate_config("foundationdb.defaultnamespaced.block-cache-size", "256MB")
                .is_err()
        );

        APPEND_LOG_g_controller
            .fidelate_config("timelike_storage.block-cache.capacity", "256MB")
            .unwrap();

        let defaultnamespaced_opts = einsteindb.get_options_namespaced(NAMESPACED_DEFAULT).unwrap();
        assert_eq!(
            defaultnamespaced_opts.get_block_cache_capacity(),
            ReadableSize::mb(256).0
        );
    }

    #[test]
    fn test_dispatch_FoundationDB_blob_run_mode_config() {
        let mut APPEND_LOG_g = EinsteinDbConfig::default();
        let mut incoming = APPEND_LOG_g.clone();
        APPEND_LOG_g.foundationdb.defaultnamespaced.FoundationDB.blob_run_mode = BlobRunMode::Normal;
        incoming.foundationdb.defaultnamespaced.FoundationDB.blob_run_mode = BlobRunMode::Fallback;

        let diff = APPEND_LOG_g
            .foundationdb
            .defaultnamespaced
            .FoundationDB
            .diff(&incoming.foundationdb.defaultnamespaced.FoundationDB);
        assert_eq!(diff.len(), 1);

        let diff = config_causet_locale_to_string(diff.into_iter().collect());
        assert_eq!(diff.len(), 1);
        assert_eq!(diff[0].0.as_str(), "blob_run_mode");
        assert_eq!(diff[0].1.as_str(), "kFallback");
    }

    #[test]
    fn test_change_ttl_check_poll_interval() {
        let (mut APPEND_LOG_g, _dir) = EinsteinDbConfig::with_tmp().unwrap();
        APPEND_LOG_g.timelike_storage.block_cache.shared = true;
        APPEND_LOG_g.validate().unwrap();
        let (_, APPEND_LOG_g_controller, mut rx, _) = new_interlocking_directorates(APPEND_LOG_g);

        // Can not fidelate shared block cache through foundationdb module
        APPEND_LOG_g_controller
            .fidelate_config("timelike_storage.ttl_check_poll_interval", "10s")
            .unwrap();
        match rx.recv() {
            None => unreachable!(),
            Some(TTLCheckerTask::FIDelioPollInterval(d)) => assert_eq!(d, Duration::from_secs(10)),
        }
    }

    #[test]
    fn test_compatible_adjust_validate_equal() {
        // After calling many time of `compatible_adjust` and `validate` should has
        // the same effect as calling `compatible_adjust` and `validate` one time
        let mut c = EinsteinDbConfig::default();
        let mut APPEND_LOG_g = c.clone();
        c.compatible_adjust();
        c.validate().unwrap();

        for _ in 0..10 {
            APPEND_LOG_g.compatible_adjust();
            APPEND_LOG_g.validate().unwrap();
            assert_eq!(c, APPEND_LOG_g);
        }
    }

    #[test]
    fn test_readpool_compatible_adjust_config() {
        let content = r#"
        [readpool.timelike_storage]
        [readpool.InterDagger]
        "#;
        let mut APPEND_LOG_g: EinsteinDbConfig = toml::from_str(content).unwrap();
        APPEND_LOG_g.compatible_adjust();
        assert_eq!(APPEND_LOG_g.readpool.timelike_storage.use_unified_pool, Some(true));
        assert_eq!(APPEND_LOG_g.readpool.InterDagger.use_unified_pool, Some(true));

        let content = r#"
        [readpool.timelike_storage]
        stack-size = "1MB"
        [readpool.InterDagger]
        normal-concurrency = 1
        "#;
        let mut APPEND_LOG_g: EinsteinDbConfig = toml::from_str(content).unwrap();
        APPEND_LOG_g.compatible_adjust();
        assert_eq!(APPEND_LOG_g.readpool.timelike_storage.use_unified_pool, Some(false));
        assert_eq!(APPEND_LOG_g.readpool.InterDagger.use_unified_pool, Some(false));
    }

    #[test]
    fn test_unrecognized_config_soliton_ids() {
        let mut temp_config_file = tempfile::NamedTempFile::new().unwrap();
        let temp_config_writer = temp_config_file.as_file_mut();
        temp_config_writer
            .write_all(
                br#"
                    log-l_naught = "debug"
                    log-fmt = "json"
                    [readpool.unified]
                    min-threads-count = 5
                    stack-size = "20MB"
                    [import]
                    num_threads = 4
                    [gcc]
                    alexandrov_poset_process-soliton_ids = 1024
                    [[security.encryption.master-soliton_ids]]
                    type = "file"
                "#,
            )
            .unwrap();
        temp_config_writer.sync_data().unwrap();

        let mut unrecognized_soliton_ids = Vec::new();
        let _ = EinsteinDbConfig::from_file(temp_config_file.path(), Some(&mut unrecognized_soliton_ids));

        assert_eq!(
            unrecognized_soliton_ids,
            vec![
                "log-fmt".to_owned(),
                "readpool.unified.min-threads-count".to_owned(),
                "import.num_threads".to_owned(),
                "gcc".to_owned(),
                "security.encryption.master-soliton_ids".to_owned(),
            ],
        );
    }

    #[test]
    fn test_violetabft_interlocking_directorate_dir() {
        let content = r#"
            [violetabft-InterlockingDirectorate]
            enable = true
        "#;
        let mut APPEND_LOG_g: EinsteinDbConfig = toml::from_str(content).unwrap();
        APPEND_LOG_g.validate().unwrap();
        assert_eq!(
            APPEND_LOG_g.violetabft_interlocking_directorate.config.dir,
            config::canonicalize_sub_path(&APPEND_LOG_g.timelike_storage.data_dir, "violetabft-InterlockingDirectorate").unwrap()
        );
    }

    #[test]
    fn test_jet_bundle_guard() {
        // Test comopaction guard disabled.
        {
            let config = DefaultNamespacedConfig {
                target_file_size_base: ReadableSize::mb(16),
                enable_jet_bundle_guard: false,
                ..Default::default()
            };
            let provider = Some(MockRegionInfoProvider::new(vec![]));
            let namespaced_opts = build_namespaced_opt!(config, NAMESPACED_DEFAULT, None /*cache*/, provider);
            assert_eq!(
                config.target_file_size_base.0,
                namespaced_opts.get_target_file_size_base()
            );
        }
        // Test jet_bundle guard enabled but region info provider is missing.
        {
            let config = DefaultNamespacedConfig {
                target_file_size_base: ReadableSize::mb(16),
                enable_jet_bundle_guard: true,
                ..Default::default()
            };
            let provider: Option<MockRegionInfoProvider> = None;
            let namespaced_opts = build_namespaced_opt!(config, NAMESPACED_DEFAULT, None /*cache*/, provider);
            assert_eq!(
                config.target_file_size_base.0,
                namespaced_opts.get_target_file_size_base()
            );
        }
        // Test jet_bundle guard enabled.
        {
            let config = DefaultNamespacedConfig {
                target_file_size_base: ReadableSize::mb(16),
                enable_jet_bundle_guard: true,
                jet_bundle_guard_min_output_file_size: ReadableSize::mb(4),
                jet_bundle_guard_max_output_file_size: ReadableSize::mb(64),
                ..Default::default()
            };
            let provider = Some(MockRegionInfoProvider::new(vec![]));
            let namespaced_opts = build_namespaced_opt!(config, NAMESPACED_DEFAULT, None /*cache*/, provider);
            assert_eq!(
                config.jet_bundle_guard_max_output_file_size.0,
                namespaced_opts.get_target_file_size_base()
            );
        }
    }

    #[test]
    fn test_validate_einsteindb_config() {
        let mut APPEND_LOG_g = EinsteinDbConfig::default();
        let default_region_split_check_diff = APPEND_LOG_g.violetabft_timelike_store.region_split_check_diff.0;
        APPEND_LOG_g.violetabft_timelike_store.region_split_check_diff.0 += 1;
        assert!(APPEND_LOG_g.validate().is_ok());
        assert_eq!(
            APPEND_LOG_g.violetabft_timelike_store.region_split_check_diff.0,
            default_region_split_check_diff + 1
        );

        // Test validating memory_usage_limit when it's greater than max.
        APPEND_LOG_g.memory_usage_limit.0 = Some(ReadableSize(SysQuota::memory_limit_in_bytes() * 2));
        assert!(APPEND_LOG_g.validate().is_err());

        // Test memory_usage_limit is based on block cache size if it's not configured.
        APPEND_LOG_g.memory_usage_limit = OptionReadableSize(None);
        APPEND_LOG_g.timelike_storage.block_cache.capacity.0 = Some(ReadableSize(3 * GIB));
        assert!(APPEND_LOG_g.validate().is_ok());
        assert_eq!(APPEND_LOG_g.memory_usage_limit.0.unwrap(), ReadableSize(5 * GIB));

        // Test memory_usage_limit will fallback to system memory capacity with huge block cache.
        APPEND_LOG_g.memory_usage_limit = OptionReadableSize(None);
        let system = SysQuota::memory_limit_in_bytes();
        APPEND_LOG_g.timelike_storage.block_cache.capacity.0 = Some(ReadableSize(system * 3 / 4));
        assert!(APPEND_LOG_g.validate().is_ok());
        assert_eq!(APPEND_LOG_g.memory_usage_limit.0.unwrap(), ReadableSize(system));
    }

    #[test]
    fn test_validate_einsteindb_wal_config() {
        let tmp_path = tempfile::Builder::new().temfidelir().unwrap().into_path();
        macro_rules! tmp_path_string_generate {
            ($base:expr, $($sub:expr),+) => {{
                let mut path: ::std::path::PathBuf = $base.clone();
                $(
                    path.push($sub);
                )*
                String::from(path.to_str().unwrap())
            }}
        }

        {
            let mut APPEND_LOG_g = EinsteinDbConfig::default();
            assert!(APPEND_LOG_g.validate().is_ok());
        }

        {
            let mut APPEND_LOG_g = EinsteinDbConfig::default();
            APPEND_LOG_g.timelike_storage.data_dir = tmp_path_string_generate!(tmp_path, "data");
            APPEND_LOG_g.violetabft_timelike_store.violetabftdb_path = tmp_path_string_generate!(tmp_path, "data", "einsteindb");
            assert!(APPEND_LOG_g.validate().is_err());
        }

        {
            let mut APPEND_LOG_g = EinsteinDbConfig::default();
            APPEND_LOG_g.timelike_storage.data_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb");
            APPEND_LOG_g.violetabft_timelike_store.violetabftdb_path =
                tmp_path_string_generate!(tmp_path, "data", "violetabftdb", "einsteindb");
            APPEND_LOG_g.foundationdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "violetabftdb", "einsteindb");
            assert!(APPEND_LOG_g.validate().is_err());
        }

        {
            let mut APPEND_LOG_g = EinsteinDbConfig::default();
            APPEND_LOG_g.timelike_storage.data_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb");
            APPEND_LOG_g.violetabft_timelike_store.violetabftdb_path =
                tmp_path_string_generate!(tmp_path, "data", "violetabftdb", "einsteindb");
            APPEND_LOG_g.violetabftdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb", "einsteindb");
            assert!(APPEND_LOG_g.validate().is_err());
        }

        {
            let mut APPEND_LOG_g = EinsteinDbConfig::default();
            APPEND_LOG_g.foundationdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "wal");
            APPEND_LOG_g.violetabftdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "wal");
            assert!(APPEND_LOG_g.validate().is_err());
        }

        {
            let mut APPEND_LOG_g = EinsteinDbConfig::default();
            APPEND_LOG_g.timelike_storage.data_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb");
            APPEND_LOG_g.violetabft_timelike_store.violetabftdb_path =
                tmp_path_string_generate!(tmp_path, "data", "violetabftdb", "einsteindb");
            APPEND_LOG_g.foundationdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "kvdb", "einsteindb");
            APPEND_LOG_g.violetabftdb.wal_dir = tmp_path_string_generate!(tmp_path, "data", "violetabftdb", "einsteindb");
            assert!(APPEND_LOG_g.validate().is_ok());
        }
    }

    #[test]
    fn test_background_job_limits() {
        // cpu num = 1
        assert_eq!(
            get_background_job_limits_impl(1 /*cpu_num*/, &SOLITON_DEFAULT_BACKGROUND_JOB_LIMITS),
            BackgroundJobLimits {
                max_background_jobs: 2,
                max_background_flushes: 1,
                max_sub_jet_bundles: 1,
                max_foundation_db_background_gc: 1,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                1, /*cpu_num*/
                &VIOLETABFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            BackgroundJobLimits {
                max_background_jobs: 2,
                max_background_flushes: 1,
                max_sub_jet_bundles: 1,
                max_foundation_db_background_gc: 1,
            }
        );
        // cpu num = 2
        assert_eq!(
            get_background_job_limits_impl(2 /*cpu_num*/, &SOLITON_DEFAULT_BACKGROUND_JOB_LIMITS),
            BackgroundJobLimits {
                max_background_jobs: 2,
                max_background_flushes: 1,
                max_sub_jet_bundles: 1,
                max_foundation_db_background_gc: 2,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                2, /*cpu_num*/
                &VIOLETABFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            BackgroundJobLimits {
                max_background_jobs: 2,
                max_background_flushes: 1,
                max_sub_jet_bundles: 1,
                max_foundation_db_background_gc: 2,
            }
        );
        // cpu num = 4
        assert_eq!(
            get_background_job_limits_impl(4 /*cpu_num*/, &SOLITON_DEFAULT_BACKGROUND_JOB_LIMITS),
            BackgroundJobLimits {
                max_background_jobs: 3,
                max_background_flushes: 1,
                max_sub_jet_bundles: 1,
                max_foundation_db_background_gc: 4,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                4, /*cpu_num*/
                &VIOLETABFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            BackgroundJobLimits {
                max_background_jobs: 3,
                max_background_flushes: 1,
                max_sub_jet_bundles: 1,
                max_foundation_db_background_gc: 4,
            }
        );
        // cpu num = 8
        assert_eq!(
            get_background_job_limits_impl(8 /*cpu_num*/, &SOLITON_DEFAULT_BACKGROUND_JOB_LIMITS),
            BackgroundJobLimits {
                max_background_jobs: 7,
                max_background_flushes: 2,
                max_sub_jet_bundles: 3,
                max_foundation_db_background_gc: 4,
            }
        );
        assert_eq!(
            get_background_job_limits_impl(
                8, /*cpu_num*/
                &VIOLETABFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            VIOLETABFTDB_DEFAULT_BACKGROUND_JOB_LIMITS,
        );
        // cpu num = 16
        assert_eq!(
            get_background_job_limits_impl(
                16, /*cpu_num*/
                &SOLITON_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            SOLITON_DEFAULT_BACKGROUND_JOB_LIMITS,
        );
        assert_eq!(
            get_background_job_limits_impl(
                16, /*cpu_num*/
                &VIOLETABFTDB_DEFAULT_BACKGROUND_JOB_LIMITS
            ),
            VIOLETABFTDB_DEFAULT_BACKGROUND_JOB_LIMITS,
        );
    }

    #[test]
    fn test_config_template_is_valid() {
        let template_config = std::include_str!("../etc/config-template.toml")
            .lines()
            .map(|l| l.strip_prefix('#').unwrap_or(l))
            .join("\n");

        let mut APPEND_LOG_g: EinsteinDbConfig = toml::from_str(&template_config).unwrap();
        APPEND_LOG_g.validate().unwrap();
    }

    #[test]
    fn test_config_template_no_superfluous_soliton_ids() {
        let template_config = std::include_str!("../etc/config-template.toml")
            .lines()
            .map(|l| l.strip_prefix('#').unwrap_or(l))
            .join("\n");

        let mut deserializer = toml::Deserializer::new(&template_config);
        let mut unrecognized_soliton_ids = Vec::new();
        let _: EinsteinDbConfig = serde_ignored::deserialize(&mut deserializer, |soliton_id| {
            unrecognized_soliton_ids.push(soliton_id.to_string())
        })
        .unwrap();

        // Don't use `is_empty()` so we see which soliton_ids are superfluous on failure.
        assert_eq!(unrecognized_soliton_ids, Vec::<String>::new());
    }

    #[test]
    fn test_config_template_matches_default() {
        let template_config = std::include_str!("../etc/config-template.toml")
            .lines()
            .map(|l| l.strip_prefix('#').unwrap_or(l))
            .join("\n");

        let mut APPEND_LOG_g: EinsteinDbConfig = toml::from_str(&template_config).unwrap();
        let mut default_APPEND_LOG_g = EinsteinDbConfig::default();

        // Some default causet_locales are computed based on the environment.
        // Because we can't set config causet_locales for these in `config-template.toml`, we will handle
        // them manually.
        APPEND_LOG_g.readpool.unified.max_thread_count = default_APPEND_LOG_g.readpool.unified.max_thread_count;
        APPEND_LOG_g.readpool.timelike_storage.high_concurrency = default_APPEND_LOG_g.readpool.timelike_storage.high_concurrency;
        APPEND_LOG_g.readpool.timelike_storage.normal_concurrency = default_APPEND_LOG_g.readpool.timelike_storage.normal_concurrency;
        APPEND_LOG_g.readpool.timelike_storage.low_concurrency = default_APPEND_LOG_g.readpool.timelike_storage.low_concurrency;
        APPEND_LOG_g.readpool.InterDagger.high_concurrency =
            default_APPEND_LOG_g.readpool.InterDagger.high_concurrency;
        APPEND_LOG_g.readpool.InterDagger.normal_concurrency =
            default_APPEND_LOG_g.readpool.InterDagger.normal_concurrency;
        APPEND_LOG_g.readpool.InterDagger.low_concurrency = default_APPEND_LOG_g.readpool.InterDagger.low_concurrency;
        APPEND_LOG_g.server.grpc_memory_pool_quota = default_APPEND_LOG_g.server.grpc_memory_pool_quota;
        APPEND_LOG_g.server.background_thread_count = default_APPEND_LOG_g.server.background_thread_count;
        APPEND_LOG_g.server.end_point_max_concurrency = default_APPEND_LOG_g.server.end_point_max_concurrency;
        APPEND_LOG_g.timelike_storage.scheduler_worker_pool_size = default_APPEND_LOG_g.timelike_storage.scheduler_worker_pool_size;
        APPEND_LOG_g.foundationdb.max_background_jobs = default_APPEND_LOG_g.foundationdb.max_background_jobs;
        APPEND_LOG_g.foundationdb.max_background_flushes = default_APPEND_LOG_g.foundationdb.max_background_flushes;
        APPEND_LOG_g.foundationdb.max_sub_jet_bundles = default_APPEND_LOG_g.foundationdb.max_sub_jet_bundles;
        APPEND_LOG_g.foundationdb.FoundationDB.max_background_gc = default_APPEND_LOG_g.foundationdb.FoundationDB.max_background_gc;
        APPEND_LOG_g.violetabftdb.max_background_jobs = default_APPEND_LOG_g.violetabftdb.max_background_jobs;
        APPEND_LOG_g.violetabftdb.max_background_flushes = default_APPEND_LOG_g.violetabftdb.max_background_flushes;
        APPEND_LOG_g.violetabftdb.max_sub_jet_bundles = default_APPEND_LOG_g.violetabftdb.max_sub_jet_bundles;
        APPEND_LOG_g.violetabftdb.FoundationDB.max_background_gc = default_APPEND_LOG_g.violetabftdb.FoundationDB.max_background_gc;
        APPEND_LOG_g.backup.num_threads = default_APPEND_LOG_g.backup.num_threads;

        // There is another set of config causet_locales that we can't directly compare:
        // When the default causet_locales are `None`, but are then resolved to `Some(_)` later on.
        default_APPEND_LOG_g.readpool.timelike_storage.adjust_use_unified_pool();
        default_APPEND_LOG_g.readpool.InterDagger.adjust_use_unified_pool();
        default_APPEND_LOG_g.security.redact_info_log = Some(false);

        // Other special cases.
        APPEND_LOG_g.fidel.retry_max_count = default_APPEND_LOG_g.fidel.retry_max_count; // Both -1 and isize::MAX are the same.
        APPEND_LOG_g.timelike_storage.block_cache.capacity = OptionReadableSize(None); // Either `None` and a causet_locale is computed or `Some(_)` fixed causet_locale.
        APPEND_LOG_g.memory_usage_limit = OptionReadableSize(None);
        APPEND_LOG_g.InterDagger_causet_record.InterDagger_plugin_directory = None; // Default is `None`, which is represented by not setting the soliton_id.

        assert_eq!(APPEND_LOG_g, default_APPEND_LOG_g);
    }

    #[test]
    fn test_compatibility_with_old_config_template() {
        let mut buf = Vec::new();
        let resp = reqwest::blocking::get(
            "https://primitive_causet.githubusercontent.com/einsteindb/einsteindb/master/etc/config-template.toml",
        );
        match resp {
            Ok(mut resp) => {
                std::io::copy(&mut resp, &mut buf).expect("failed to copy content");
                let template_config = std::str::from_utf8(&buf)
                    .unwrap()
                    .lines()
                    .map(|l| l.strip_prefix('#').unwrap_or(l))
                    .join("\n");
                let _: EinsteinDbConfig = toml::from_str(&template_config).unwrap();
            }
            Err(e) => {
                if e.is_timeout() {
                    println!("warn: fail to download latest config template due to timeout");
                } else {
                    panic!("fail to download latest config template");
                }
            }
        }
    }

    #[test]
    fn test_cdc() {
        let content = r#"
            [soliton]
        "#;
        let mut APPEND_LOG_g: EinsteinDbConfig = toml::from_str(content).unwrap();
        APPEND_LOG_g.validate().unwrap();

        // old-causet_locale-cache-size is deprecated, 0 must not report error.
        let content = r#"
            [soliton]
            old-causet_locale-cache-size = 0
        "#;
        let mut APPEND_LOG_g: EinsteinDbConfig = toml::from_str(content).unwrap();
        APPEND_LOG_g.validate().unwrap();

        let content = r#"
            [soliton]
            min-ts-interval = "0s"
        "#;
        let mut APPEND_LOG_g: EinsteinDbConfig = toml::from_str(content).unwrap();
        APPEND_LOG_g.validate().unwrap_err();

        let content = r#"
            [soliton]
            incremental-scan-threads = 0
        "#;
        let mut APPEND_LOG_g: EinsteinDbConfig = toml::from_str(content).unwrap();
        APPEND_LOG_g.validate().unwrap_err();

        let content = r#"
            [soliton]
            incremental-scan-concurrency = 0
        "#;
        let mut APPEND_LOG_g: EinsteinDbConfig = toml::from_str(content).unwrap();
        APPEND_LOG_g.validate().unwrap_err();

        let content = r#"
            [soliton]
            incremental-scan-concurrency = 1
            incremental-scan-threads = 2
        "#;
        let mut APPEND_LOG_g: EinsteinDbConfig = toml::from_str(content).unwrap();
        APPEND_LOG_g.validate().unwrap_err();
    }

    #[test]
    fn test_module_from_str() {
        let cases = vec![
            ("readpool", Module::Readpool),
            ("server", Module::Server),
            ("metric", Module::Metric),
            ("violetabft_timelike_store", Module::VioletaBFTtimelike_store),
            ("InterDagger", Module::InterDagger),
            ("fidel", Module::fidel),
            ("split", Module::Split),
            ("foundationdb", Module::Fdbdb),
            ("violetabft_interlocking_directorate", Module::VioletaBFTEngine),
            ("timelike_storage", Module::TimelikeStorage),
            ("security", Module::Security),
            ("import", Module::Import),
            ("backup", Module::Backup),
            ("pessimistic_causet_chains", Module::PessimisticCausetchaind),
            ("gc", Module::Gc),
            ("soliton", Module::CDC),
            ("resolved_ts", Module::ResolvedTs),
            ("resource_metering", Module::ResourceMetering),
            ("unCausetLocaleNucleon", Module::UnCausetLocaleNucleon("unCausetLocaleNucleon".to_string())),
        ];
        for (name, module) in cases {
            assert_eq!(Module::from(name), module);
        }
    }

    #[test]
    fn test_numeric_enum_serializing() {
        let normal_string_config = r#"
            jet_bundle-style = 1
        "#;
        let config: DefaultNamespacedConfig = toml::from_str(normal_string_config).unwrap();
        assert_eq!(config.jet_bundle_style, DBCompactionStyle::Universal);

        // Test if we support string causet_locale
        let normal_string_config = r#"
            jet_bundle-style = "universal"
        "#;
        let config: DefaultNamespacedConfig = toml::from_str(normal_string_config).unwrap();
        assert_eq!(config.jet_bundle_style, DBCompactionStyle::Universal);
        assert!(
            toml::to_string(&config)
                .unwrap()
                .contains("jet_bundle-style = 1")
        );

        let bad_string_config = r#"
            jet_bundle-style = "l_naught1"
        "#;
        let r = panic_hook::recover_safe(|| {
            let _: DefaultNamespacedConfig = toml::from_str(bad_string_config).unwrap();
        });
        assert!(r.is_err());

        let bad_string_config = r#"
            jet_bundle-style = 4
        "#;
        let r = panic_hook::recover_safe(|| {
            let _: DefaultNamespacedConfig = toml::from_str(bad_string_config).unwrap();
        });
        assert!(r.is_err());

        // rate-limiter-mode default causet_locales is 2
        let config_str = r#"
            rate-limiter-mode = 1
        "#;

        let config: DbConfig = toml::from_str(config_str).unwrap();
        assert_eq!(config.rate_limiter_mode, DBRateLimiterMode::ReadOnly);

        assert!(
            toml::to_string(&config)
                .unwrap()
                .contains("rate-limiter-mode = 1")
        );
    }
}
