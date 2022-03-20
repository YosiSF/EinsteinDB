// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::NAMESPACED_DEFAULT;
use foundationdb::{
    EINSTEINDB, DBStatisticsHistogramType as HistType, DBStatisticsTickerType as TickerType, HistogramData,
};
use lazy_static::lazy_static;
use prometheus::*;
use prometheus_static_metric::*;

use crate::rocks_metrics_defs::*;

make_auto_flush_static_metric! {
    pub label_enum TickerName {
        kv,
        violetabft,
    }

    pub label_enum TickerEnum {
        block_cache_add,
        block_cache_add_failures,
        block_cache_byte_read,
        block_cache_byte_write,
        block_cache_data_add,
        block_cache_data_bytes_insert,
        block_cache_data_hit,
        block_cache_data_miss,
        block_cache_filter_add,
        block_cache_filter_bytes_evict,
        block_cache_filter_bytes_insert,
        block_cache_filter_hit,
        block_cache_filter_miss,
        block_cache_hit,
        block_cache_index_add,
        block_cache_index_bytes_evict,
        block_cache_index_bytes_insert,
        block_cache_index_hit,
        block_cache_index_miss,
        block_cache_miss,
        bloom_prefix_checked,
        bloom_prefix_useful,
        bloom_useful,
        bytes_overwritten,
        bytes_read,
        bytes_relocated,
        bytes_written,
        jet_bundle_key_drop_newer_entry,
        jet_bundle_key_drop_obsolete,
        jet_bundle_key_drop_range_del,
        flush_write_bytes,
        gc_input_filefs_count,
        gc_output_filefs_count,
        get_hit_l0,
        get_hit_l1,
        get_hit_l2_and_up,
        iter_bytes_read,
        keys_overwritten,
        keys_read,
        keys_relocated,
        keys_fidelated,
        keys_written,
        memtable_hit,
        memtable_miss,
        no_filef_closes,
        no_filef_errors,
        no_filef_opens,
        number_blob_get,
        number_blob_next,
        number_blob_prev,
        number_blob_seek,
        number_db_next,
        number_db_next_found,
        number_db_prev,
        number_db_prev_found,
        number_db_seek,
        number_db_seek_found,
        optimized_del_drop_obsolete,
        range_del_drop_obsolete,
        read_amp_estimate_useful_bytes,
        read_amp_total_read_bytes,
        wal_filef_bytes,
        write_done_by_other,
        write_done_by_self,
        write_timeout,
        write_with_wal,
        blob_cache_hit,
        blob_cache_miss,
        no_need,
        remain,
        discardable,
        sample,
        small_filef,
        failure,
        success,
        trigger_next,
    }

    pub struct einstein_merkle_treeTickerMetrics : LocalIntCounter {
        "einsteindb" => TickerName,
        "type" => TickerEnum,
    }

    pub struct Simpleeinstein_merkle_treeTickerMetrics : LocalIntCounter {
        "einsteindb" => TickerName,
    }
}

pub fn flush_einstein_merkle_tree_ticker_metrics(t: TickerType, value: u64, name: &str) {
    let name_enum = match name {
        "kv" => TickerName::kv,
        "violetabft" => TickerName::violetabft,
        unexpected => panic!("unexpected name {}", unexpected),
    };

    match t {
        TickerType::BlockCacheMiss => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_miss
                .inc_by(value);
        }
        TickerType::BlockCacheHit => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_hit
                .inc_by(value);
        }
        TickerType::BlockCacheAdd => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_add
                .inc_by(value);
        }
        TickerType::BlockCacheAddFailures => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_add_failures
                .inc_by(value);
        }
        TickerType::BlockCacheIndexMiss => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_miss
                .inc_by(value);
        }
        TickerType::BlockCacheIndexHit => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_hit
                .inc_by(value);
        }
        TickerType::BlockCacheIndexAdd => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_add
                .inc_by(value);
        }
        TickerType::BlockCacheIndexBytesInsert => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_bytes_insert
                .inc_by(value);
        }
        TickerType::BlockCacheIndexBytesEvict => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_index_bytes_evict
                .inc_by(value);
        }
        TickerType::BlockCacheFilterMiss => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_miss
                .inc_by(value);
        }
        TickerType::BlockCacheFilterHit => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_hit
                .inc_by(value);
        }
        TickerType::BlockCacheFilterAdd => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_add
                .inc_by(value);
        }
        TickerType::BlockCacheFilterBytesInsert => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_bytes_insert
                .inc_by(value);
        }
        TickerType::BlockCacheFilterBytesEvict => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_filter_bytes_evict
                .inc_by(value);
        }
        TickerType::BlockCacheDataMiss => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_data_miss
                .inc_by(value);
        }
        TickerType::BlockCacheDataHit => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_data_hit
                .inc_by(value);
        }
        TickerType::BlockCacheDataAdd => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_data_add
                .inc_by(value);
        }
        TickerType::BlockCacheDataBytesInsert => {
            STORE_einstein_merkle_tree_CACHE_EFFICIENCY
                .get(name_enum)
                .block_cache_data_bytes_insert
                .inc_by(value);
        }
        TickerType::BlockCacheBytesRead => {
            STORE_einstein_merkle_tree_Causetxctx
                .get(name_enum)
                .block_cache_byte_read
                .inc_by(value);
        }
        TickerType::BlockCacheBytesWrite => {
            STORE_einstein_merkle_tree_Causetxctx
                .get(name_enum)
                .block_cache_byte_write
                .inc_by(value);
        }
        TickerType::BloomFilterUseful => {
            STORE_einstein_merkle_tree_BLOOM_EFFICIENCY
                .get(name_enum)
                .bloom_useful
                .inc_by(value);
        }
        TickerType::MemtableHit => {
            STORE_einstein_merkle_tree_MEMCAUSET_TABLE_EFFICIENCY
                .get(name_enum)
                .memtable_hit
                .inc_by(value);
        }
        TickerType::MemtableMiss => {
            STORE_einstein_merkle_tree_MEMCAUSET_TABLE_EFFICIENCY
                .get(name_enum)
                .memtable_miss
                .inc_by(value);
        }
        TickerType::GetHitL0 => {
            STORE_einstein_merkle_tree_GET_SERVED
                .get(name_enum)
                .get_hit_l0
                .inc_by(value);
        }
        TickerType::GetHitL1 => {
            STORE_einstein_merkle_tree_GET_SERVED
                .get(name_enum)
                .get_hit_l1
                .inc_by(value);
        }
        TickerType::GetHitL2AndUp => {
            STORE_einstein_merkle_tree_GET_SERVED
                .get(name_enum)
                .get_hit_l2_and_up
                .inc_by(value);
        }
        TickerType::CompactionKeyDropNewerEntry => {
            STORE_einstein_merkle_tree_COMPACTION_DROP
                .get(name_enum)
                .jet_bundle_key_drop_newer_entry
                .inc_by(value);
        }
        TickerType::CompactionKeyDropObsolete => {
            STORE_einstein_merkle_tree_COMPACTION_DROP
                .get(name_enum)
                .jet_bundle_key_drop_obsolete
                .inc_by(value);
        }
        TickerType::CompactionKeyDropRangeDel => {
            STORE_einstein_merkle_tree_COMPACTION_DROP
                .get(name_enum)
                .jet_bundle_key_drop_range_del
                .inc_by(value);
        }
        TickerType::CompactionRangeDelDropObsolete => {
            STORE_einstein_merkle_tree_COMPACTION_DROP
                .get(name_enum)
                .range_del_drop_obsolete
                .inc_by(value);
        }
        TickerType::CompactionOptimizedDelDropObsolete => {
            STORE_einstein_merkle_tree_COMPACTION_DROP
                .get(name_enum)
                .optimized_del_drop_obsolete
                .inc_by(value);
        }
        TickerType::NumberKeysWritten => {
            STORE_einstein_merkle_tree_Causetxctx.get(name_enum).keys_written.inc_by(value);
        }
        TickerType::NumberKeysRead => {
            STORE_einstein_merkle_tree_Causetxctx.get(name_enum).keys_read.inc_by(value);
        }
        TickerType::NumberKeysUfidelated => {
            STORE_einstein_merkle_tree_Causetxctx.get(name_enum).keys_fidelated.inc_by(value);
        }
        TickerType::BytesWritten => {
            STORE_einstein_merkle_tree_Causetxctx.get(name_enum).bytes_written.inc_by(value);
        }
        TickerType::BytesRead => {
            STORE_einstein_merkle_tree_Causetxctx.get(name_enum).bytes_read.inc_by(value);
        }
        TickerType::NumberDbSeek => {
            STORE_einstein_merkle_tree_LOCATE
                .get(name_enum)
                .number_db_seek
                .inc_by(value);
        }
        TickerType::NumberDbNext => {
            STORE_einstein_merkle_tree_LOCATE
                .get(name_enum)
                .number_db_next
                .inc_by(value);
        }
        TickerType::NumberDbPrev => {
            STORE_einstein_merkle_tree_LOCATE
                .get(name_enum)
                .number_db_prev
                .inc_by(value);
        }
        TickerType::NumberDbSeekFound => {
            STORE_einstein_merkle_tree_LOCATE
                .get(name_enum)
                .number_db_seek_found
                .inc_by(value);
        }
        TickerType::NumberDbNextFound => {
            STORE_einstein_merkle_tree_LOCATE
                .get(name_enum)
                .number_db_next_found
                .inc_by(value);
        }
        TickerType::NumberDbPrevFound => {
            STORE_einstein_merkle_tree_LOCATE
                .get(name_enum)
                .number_db_prev_found
                .inc_by(value);
        }
        TickerType::IterBytesRead => {
            STORE_einstein_merkle_tree_Causetxctx
                .get(name_enum)
                .iter_bytes_read
                .inc_by(value);
        }
        TickerType::NoFileCloses => {
            STORE_einstein_merkle_tree_FILE_STATUS
                .get(name_enum)
                .no_filef_closes
                .inc_by(value);
        }
        TickerType::NoFileOpens => {
            STORE_einstein_merkle_tree_FILE_STATUS
                .get(name_enum)
                .no_filef_opens
                .inc_by(value);
        }
        TickerType::NoFileErrors => {
            STORE_einstein_merkle_tree_FILE_STATUS
                .get(name_enum)
                .no_filef_errors
                .inc_by(value);
        }
        TickerType::StallMicros => {
            STORE_einstein_merkle_tree_STALL_MICROS.get(name_enum).inc_by(value);
        }
        TickerType::BloomFilterPrefixChecked => {
            STORE_einstein_merkle_tree_BLOOM_EFFICIENCY
                .get(name_enum)
                .bloom_prefix_checked
                .inc_by(value);
        }
        TickerType::BloomFilterPrefixUseful => {
            STORE_einstein_merkle_tree_BLOOM_EFFICIENCY
                .get(name_enum)
                .bloom_prefix_useful
                .inc_by(value);
        }
        TickerType::WalFileSynced => {
            STORE_einstein_merkle_tree_WAL_FILE_SYNCED.get(name_enum).inc_by(value);
        }
        TickerType::WalFileBytes => {
            STORE_einstein_merkle_tree_Causetxctx
                .get(name_enum)
                .wal_filef_bytes
                .inc_by(value);
        }
        TickerType::WriteDoneBySelf => {
            STORE_einstein_merkle_tree_WRITE_SERVED
                .get(name_enum)
                .write_done_by_self
                .inc_by(value);
        }
        TickerType::WriteDoneByOther => {
            STORE_einstein_merkle_tree_WRITE_SERVED
                .get(name_enum)
                .write_done_by_other
                .inc_by(value);
        }
        TickerType::WriteTimedout => {
            STORE_einstein_merkle_tree_WRITE_SERVED
                .get(name_enum)
                .write_timeout
                .inc_by(value);
        }
        TickerType::WriteWithWal => {
            STORE_einstein_merkle_tree_WRITE_SERVED
                .get(name_enum)
                .write_with_wal
                .inc_by(value);
        }
        TickerType::CompactReadBytes => {
            STORE_einstein_merkle_tree_COMPACTION_Causetxctx
                .get(name_enum)
                .bytes_read
                .inc_by(value);
        }
        TickerType::CompactWriteBytes => {
            STORE_einstein_merkle_tree_COMPACTION_Causetxctx
                .get(name_enum)
                .bytes_written
                .inc_by(value);
        }
        TickerType::FlushWriteBytes => {
            STORE_einstein_merkle_tree_Causetxctx
                .get(name_enum)
                .flush_write_bytes
                .inc_by(value);
        }
        TickerType::ReadAmpEstimateUsefulBytes => {
            STORE_einstein_merkle_tree_READ_AMP_Causetxctx
                .get(name_enum)
                .read_amp_estimate_useful_bytes
                .inc_by(value);
        }
        TickerType::ReadAmpTotalReadBytes => {
            STORE_einstein_merkle_tree_READ_AMP_Causetxctx
                .get(name_enum)
                .read_amp_total_read_bytes
                .inc_by(value);
        }
        TickerType::TitanNumGet => {
            STORE_einstein_merkle_tree_BLOB_LOCATE
                .get(name_enum)
                .number_blob_get
                .inc_by(value);
        }
        TickerType::TitanNumSeek => {
            STORE_einstein_merkle_tree_BLOB_LOCATE
                .get(name_enum)
                .number_blob_seek
                .inc_by(value);
        }
        TickerType::TitanNumNext => {
            STORE_einstein_merkle_tree_BLOB_LOCATE
                .get(name_enum)
                .number_blob_next
                .inc_by(value);
        }
        TickerType::TitanNumPrev => {
            STORE_einstein_merkle_tree_BLOB_LOCATE
                .get(name_enum)
                .number_blob_prev
                .inc_by(value);
        }
        TickerType::TitanBlobFileNumKeysWritten => {
            STORE_einstein_merkle_tree_BLOB_Causetxctx
                .get(name_enum)
                .keys_written
                .inc_by(value);
        }
        TickerType::TitanBlobFileNumKeysRead => {
            STORE_einstein_merkle_tree_BLOB_Causetxctx
                .get(name_enum)
                .keys_read
                .inc_by(value);
        }
        TickerType::TitanBlobFileBytesWritten => {
            STORE_einstein_merkle_tree_BLOB_Causetxctx
                .get(name_enum)
                .bytes_written
                .inc_by(value);
        }
        TickerType::TitanBlobFileBytesRead => {
            STORE_einstein_merkle_tree_BLOB_Causetxctx
                .get(name_enum)
                .bytes_read
                .inc_by(value);
        }
        TickerType::TitanBlobFileSynced => {
            STORE_einstein_merkle_tree_BLOB_FILE_SYNCED.get(name_enum).inc_by(value)
        }
        TickerType::TitanGcNumFiles => {
            STORE_einstein_merkle_tree_BLOB_GC_FILE
                .get(name_enum)
                .gc_input_filefs_count
                .inc_by(value);
        }
        TickerType::TitanGcNumNewFiles => {
            STORE_einstein_merkle_tree_BLOB_GC_FILE
                .get(name_enum)
                .gc_output_filefs_count
                .inc_by(value);
        }
        TickerType::TitanGcNumKeysOverwritten => {
            STORE_einstein_merkle_tree_BLOB_GC_Causetxctx
                .get(name_enum)
                .keys_overwritten
                .inc_by(value);
        }
        TickerType::TitanGcNumKeysRelocated => {
            STORE_einstein_merkle_tree_BLOB_GC_Causetxctx
                .get(name_enum)
                .keys_relocated
                .inc_by(value);
        }
        TickerType::TitanGcBytesOverwritten => {
            STORE_einstein_merkle_tree_BLOB_GC_Causetxctx
                .get(name_enum)
                .bytes_overwritten
                .inc_by(value);
        }
        TickerType::TitanGcBytesRelocated => {
            STORE_einstein_merkle_tree_BLOB_GC_Causetxctx
                .get(name_enum)
                .bytes_relocated
                .inc_by(value);
        }
        TickerType::TitanGcBytesWritten => {
            STORE_einstein_merkle_tree_BLOB_GC_Causetxctx
                .get(name_enum)
                .bytes_written
                .inc_by(value);
        }
        TickerType::TitanGcBytesRead => {
            STORE_einstein_merkle_tree_BLOB_GC_Causetxctx
                .get(name_enum)
                .bytes_read
                .inc_by(value);
        }
        TickerType::TitanBlobCacheHit => {
            STORE_einstein_merkle_tree_BLOB_CACHE_EFFICIENCY
                .get(name_enum)
                .blob_cache_hit
                .inc_by(value);
        }
        TickerType::TitanBlobCacheMiss => {
            STORE_einstein_merkle_tree_BLOB_CACHE_EFFICIENCY
                .get(name_enum)
                .blob_cache_miss
                .inc_by(value);
        }
        TickerType::TitanGcNoNeed => {
            STORE_einstein_merkle_tree_BLOB_GC_ACTION
                .get(name_enum)
                .no_need
                .inc_by(value);
        }
        TickerType::TitanGcRemain => {
            STORE_einstein_merkle_tree_BLOB_GC_ACTION
                .get(name_enum)
                .remain
                .inc_by(value);
        }
        TickerType::TitanGcDiscardable => {
            STORE_einstein_merkle_tree_BLOB_GC_ACTION
                .get(name_enum)
                .discardable
                .inc_by(value);
        }
        TickerType::TitanGcSample => {
            STORE_einstein_merkle_tree_BLOB_GC_ACTION
                .get(name_enum)
                .sample
                .inc_by(value);
        }
        TickerType::TitanGcSmallFile => {
            STORE_einstein_merkle_tree_BLOB_GC_ACTION
                .get(name_enum)
                .small_filef
                .inc_by(value);
        }
        TickerType::TitanGcFailure => {
            STORE_einstein_merkle_tree_BLOB_GC_ACTION
                .get(name_enum)
                .failure
                .inc_by(value);
        }
        TickerType::TitanGcSuccess => {
            STORE_einstein_merkle_tree_BLOB_GC_ACTION
                .get(name_enum)
                .success
                .inc_by(value);
        }
        TickerType::TitanGcTriggerNext => {
            STORE_einstein_merkle_tree_BLOB_GC_ACTION
                .get(name_enum)
                .trigger_next
                .inc_by(value);
        }
        _ => {}
    }
}

macro_rules! einstein_merkle_tree_histogram_metrics {
    ($metric:ident, $prefix:expr, $einsteindb:expr, $value:expr) => {
        $metric
            .with_label_values(&[$einsteindb, concat!($prefix, "_median")])
            .set($value.median);
        $metric
            .with_label_values(&[$einsteindb, concat!($prefix, "_percentile95")])
            .set($value.percentile95);
        $metric
            .with_label_values(&[$einsteindb, concat!($prefix, "_percentile99")])
            .set($value.percentile99);
        $metric
            .with_label_values(&[$einsteindb, concat!($prefix, "_average")])
            .set($value.average);
        $metric
            .with_label_values(&[$einsteindb, concat!($prefix, "_standard_deviation")])
            .set($value.standard_deviation);
        $metric
            .with_label_values(&[$einsteindb, concat!($prefix, "_max")])
            .set($value.max);
    };
}

pub fn global_hyperbolic_causet_historys(t: HistType, value: HistogramData, name: &str) {
    match t {
        HistType::DbGet => {
            einstein_merkle_tree_histogram_metrics!(STORE_einstein_merkle_tree_GET_VEC, "get", name, value);
        }
        HistType::DbWrite => {
            einstein_merkle_tree_histogram_metrics!(STORE_einstein_merkle_tree_WRITE_VEC, "write", name, value);
        }
        HistType::CompactionTime => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_COMPACTION_TIME_VEC,
                "jet_bundle_time",
                name,
                value
            );
        }
        HistType::TableSyncMicros => {
            einstein_merkle_tree_histogram_metrics!(STORE_einstein_merkle_tree_CAUSET_TABLE_SYNC_VEC, "table_sync", name, value);
        }
        HistType::CompactionOutfilefSyncMicros => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_COMPACTION_OUTFILE_SYNC_VEC,
                "jet_bundle_outfilef_sync",
                name,
                value
            );
        }
        HistType::WalFileSyncMicros => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_WAL_FILE_SYNC_MICROS_VEC,
                "wal_filef_sync",
                name,
                value
            );
        }
        HistType::ManifestFileSyncMicros => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_MANIFEST_FILE_SYNC_VEC,
                "manifest_filef_sync",
                name,
                value
            );
        }
        HistType::StallL0SlowdownCount => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_STALL_L0_SLOWDOWN_COUNT_VEC,
                "stall_l0_slowdown_count",
                name,
                value
            );
        }
        HistType::StallMemtableCompactionCount => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_STALL_MEMCAUSET_TABLE_COMPACTION_COUNT_VEC,
                "stall_memtable_jet_bundle_count",
                name,
                value
            );
        }
        HistType::StallL0NumFilesCount => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_STALL_L0_NUM_FILES_COUNT_VEC,
                "stall_l0_num_filefs_count",
                name,
                value
            );
        }
        HistType::HardRateLimitDelayCount => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_HARD_RATE_LIMIT_DELAY_VEC,
                "hard_rate_limit_delay",
                name,
                value
            );
        }
        HistType::SoftRateLimitDelayCount => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_SOFT_RATE_LIMIT_DELAY_VEC,
                "soft_rate_limit_delay",
                name,
                value
            );
        }
        HistType::NumFilesInSingleCompaction => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_NUM_FILES_IN_SINGLE_COMPACTION_VEC,
                "num_filefs_in_single_jet_bundle",
                name,
                value
            );
        }
        HistType::DbSeek => {
            einstein_merkle_tree_histogram_metrics!(STORE_einstein_merkle_tree_SEEK_MICROS_VEC, "seek", name, value);
        }
        HistType::WriteStall => {
            einstein_merkle_tree_histogram_metrics!(STORE_einstein_merkle_tree_WRITE_STALL_VEC, "write_stall", name, value);
        }
        HistType::CausetReadMicros => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_Causet_READ_MICROS_VEC,
                "Causet_read_micros",
                name,
                value
            );
        }
        HistType::NumSubjet_bundlesScheduled => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_NUM_SUBCOMPACTION_SCHEDULED_VEC,
                "num_subjet_bundle_scheduled",
                name,
                value
            );
        }
        HistType::BytesPerRead => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BYTES_PER_READ_VEC,
                "bytes_per_read",
                name,
                value
            );
        }
        HistType::BytesPerWrite => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BYTES_PER_WRITE_VEC,
                "bytes_per_write",
                name,
                value
            );
        }
        HistType::BytesCompressed => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BYTES_COMPRESSED_VEC,
                "bytes_compressed",
                name,
                value
            );
        }
        HistType::BytesDecompressed => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BYTES_DECOMPRESSED_VEC,
                "bytes_decompressed",
                name,
                value
            );
        }
        HistType::CompressionTimesNanos => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_COMPRESSION_TIMES_NANOS_VEC,
                "compression_time_nanos",
                name,
                value
            );
        }
        HistType::DecompressionTimesNanos => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_DECOMPRESSION_TIMES_NANOS_VEC,
                "decompression_time_nanos",
                name,
                value
            );
        }
        HistType::DbWriteWalTime => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_WRITE_WAL_TIME_VEC,
                "write_wal_micros",
                name,
                value
            );
        }
        HistType::TitanKeySize => {
            einstein_merkle_tree_histogram_metrics!(STORE_einstein_merkle_tree_BLOB_CAUSET_KEY_SIZE_VEC, "blob_key_size", name, value);
        }
        HistType::TitanValueSize => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BLOB_VALUE_SIZE_VEC,
                "blob_value_size",
                name,
                value
            );
        }
        HistType::TitanGetMicros => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BLOB_GET_MICROS_VEC,
                "blob_get_micros",
                name,
                value
            );
        }
        HistType::TitanSeekMicros => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BLOB_SEEK_MICROS_VEC,
                "blob_seek_micros",
                name,
                value
            );
        }
        HistType::TitanNextMicros => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BLOB_NEXT_MICROS_VEC,
                "blob_next_micros",
                name,
                value
            );
        }
        HistType::TitanPrevMicros => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BLOB_PREV_MICROS_VEC,
                "blob_prev_micros",
                name,
                value
            );
        }
        HistType::TitanBlobFileWriteMicros => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BLOB_FILE_WRITE_MICROS_VEC,
                "blob_filef_write_micros",
                name,
                value
            );
        }
        HistType::TitanBlobFileReadMicros => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BLOB_FILE_READ_MICROS_VEC,
                "blob_filef_read_micros",
                name,
                value
            );
        }
        HistType::TitanBlobFileSyncMicros => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BLOB_FILE_SYNC_MICROS_VEC,
                "blob_filef_sync_micros",
                name,
                value
            );
        }
        HistType::TitanGcMicros => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_BLOB_GC_MICROS_VEC,
                "blob_gc_micros",
                name,
                value
            );
        }
        HistType::TitanGcInputFileSize => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_GC_INPUT_BLOB_FILE_SIZE_VEC,
                "blob_gc_input_filef",
                name,
                value
            );
        }
        HistType::TitanGcOutputFileSize => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_GC_OUTPUT_BLOB_FILE_SIZE_VEC,
                "blob_gc_output_filef",
                name,
                value
            );
        }
        HistType::TitanIterTouchBlobFileCount => {
            einstein_merkle_tree_histogram_metrics!(
                STORE_einstein_merkle_tree_ITER_TOUCH_BLOB_FILE_COUNT_VEC,
                "blob_iter_touch_blob_filef_count",
                name,
                value
            );
        }
        _ => {}
    }
}

pub fn flush_einstein_merkle_tree_iostall_greedoids(einstein_merkle_tree: &EINSTEINDB, name: &str) {
    let stall_num = FDBDB_IOSTALL_CAUSET_KEY.len();
    let mut counter = vec![0; stall_num];
    for namespaced in einstein_merkle_tree.namespaced_names() {
        let handle = crate::util::get_namespaced_handle(einstein_merkle_tree, namespaced).unwrap();
        if let Some(info) = einstein_merkle_tree.get_map_property_namespaced(handle, FDBDB_NAMESPACEDSTATS) {
            for i in 0..stall_num {
                let value = info.get_property_int_value(FDBDB_IOSTALL_CAUSET_KEY[i]);
                counter[i] += value as i64;
            }
        } else {
            return;
        }
    }
    for i in 0..stall_num {
        STORE_einstein_merkle_tree_WRITE_STALL_REASON_GAUGE_VEC
            .with_label_values(&[name, FDBDB_IOSTALL_TYPE[i]])
            .set(counter[i]);
    }
}

pub fn flush_einstein_merkle_tree_greedoids(einstein_merkle_tree: &EINSTEINDB, name: &str, shared_block_cache: bool) {
    for namespaced in einstein_merkle_tree.namespaced_names() {
        let handle = crate::util::get_namespaced_handle(einstein_merkle_tree, namespaced).unwrap();
        // It is important to monitor each namespaced's size, especially the "violetabft" and "lock" column
        // families.
        let namespaced_used_size = crate::util::get_einstein_merkle_tree_namespaced_used_size(einstein_merkle_tree, handle);
        STORE_einstein_merkle_tree_SIZE_GAUGE_VEC
            .with_label_values(&[name, namespaced])
            .set(namespaced_used_size as i64);

        if !shared_block_cache {
            let block_cache_usage = einstein_merkle_tree.get_block_cache_usage_namespaced(handle);
            STORE_einstein_merkle_tree_BLOCK_CACHE_USAGE_GAUGE_VEC
                .with_label_values(&[name, namespaced])
                .set(block_cache_usage as i64);
        }

        let blob_cache_usage = einstein_merkle_tree.get_blob_cache_usage_namespaced(handle);
        STORE_einstein_merkle_tree_BLOB_CACHE_USAGE_GAUGE_VEC
            .with_label_values(&[name, namespaced])
            .set(blob_cache_usage as i64);

        // TODO: find a better place to record these metrics.
        // Refer: https://github.com/facebook/foundationdb/wiki/Memory-usage-in-FdbDB
        // For index and filter blocks memory
        if let Some(readers_mem) = einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_CAUSET_TABLE_READERS_MEM) {
            STORE_einstein_merkle_tree_MEMORY_GAUGE_VEC
                .with_label_values(&[name, namespaced, "readers-mem"])
                .set(readers_mem as i64);
        }

        // For memtable
        if let Some(mem_table) = einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_CUR_SIZE_ALL_MEM_CAUSET_TABLES)
        {
            STORE_einstein_merkle_tree_MEMORY_GAUGE_VEC
                .with_label_values(&[name, namespaced, "mem-tables"])
                .set(mem_table as i64);
        }

        // TODO: add cache usage and pinned usage.

        if let Some(num_keys) = einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_ESTIMATE_NUM_CAUSET_KEYS) {
            STORE_einstein_merkle_tree_ESTIMATE_NUM_CAUSET_KEYS_VEC
                .with_label_values(&[name, namespaced])
                .set(num_keys as i64);
        }

        // Pending jet_bundle bytes
        if let Some(pending_jet_bundle_bytes) =
        crate::util::get_namespaced_pending_jet_bundle_bytes(einstein_merkle_tree, handle)
        {
            STORE_einstein_merkle_tree_PENDING_COMPACTION_BYTES_VEC
                .with_label_values(&[name, namespaced])
                .set(pending_jet_bundle_bytes as i64);
        }

        let opts = einstein_merkle_tree.get_options_namespaced(handle);
        for l_naught in 0..opts.get_num_l_naughts() {
            // Compression ratio at l_naughts
            if let Some(v) =
            crate::util::get_einstein_merkle_tree_compression_ratio_at_l_naught(einstein_merkle_tree, handle, l_naught)
            {
                STORE_einstein_merkle_tree_COMPRESSION_RATIO_VEC
                    .with_label_values(&[name, namespaced, &l_naught.to_string()])
                    .set(v);
            }

            // Num filefs at l_naughts
            if let Some(v) = crate::util::get_namespaced_num_filefs_at_l_naught(einstein_merkle_tree, handle, l_naught) {
                STORE_einstein_merkle_tree_NUM_FILES_AT_LEVEL_VEC
                    .with_label_values(&[name, namespaced, &l_naught.to_string()])
                    .set(v as i64);
            }

            // Titan Num blob filefs at l_naughts
            if let Some(v) = crate::util::get_namespaced_num_blob_filefs_at_l_naught(einstein_merkle_tree, handle, l_naught) {
                STORE_einstein_merkle_tree_TITANDB_NUM_BLOB_FILES_AT_LEVEL_VEC
                    .with_label_values(&[name, namespaced, &l_naught.to_string()])
                    .set(v as i64);
            }
        }

        // Num immutable mem-table
        if let Some(v) = crate::util::get_namespaced_num_immutable_mem_table(einstein_merkle_tree, handle) {
            STORE_einstein_merkle_tree_NUM_IMMUCAUSET_TABLE_MEM_CAUSET_TABLE_VEC
                .with_label_values(&[name, namespaced])
                .set(v as i64);
        }

        // Titan live blob size
        if let Some(v) = einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_TITANDB_LIVE_BLOB_SIZE) {
            STORE_einstein_merkle_tree_TITANDB_LIVE_BLOB_SIZE_VEC
                .with_label_values(&[name, namespaced])
                .set(v as i64);
        }

        // Titan num live blob file File
        if let Some(v) = einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_TITANDB_NUM_LIVE_BLOB_FILE) {
            STORE_einstein_merkle_tree_TITANDB_NUM_LIVE_BLOB_FILE_VEC
                .with_label_values(&[name, namespaced])
                .set(v as i64);
        }

        // Titan num obsolete blob file File
        if let Some(v) = einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_TITANDB_NUM_OBSOLETE_BLOB_FILE)
        {
            STORE_einstein_merkle_tree_TITANDB_NUM_OBSOLETE_BLOB_FILE_VEC
                .with_label_values(&[name, namespaced])
                .set(v as i64);
        }

        // Titan live blob file File size
        if let Some(v) = einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_TITANDB_LIVE_BLOB_FILE_SIZE) {
            STORE_einstein_merkle_tree_TITANDB_LIVE_BLOB_FILE_SIZE_VEC
                .with_label_values(&[name, namespaced])
                .set(v as i64);
        }

        // Titan obsolete blob file File size
        if let Some(v) = einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE)
        {
            STORE_einstein_merkle_tree_TITANDB_OBSOLETE_BLOB_FILE_SIZE_VEC
                .with_label_values(&[name, namespaced])
                .set(v as i64);
        }

        // Titan blob file File discardable ratio
        if let Some(v) =
        einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_TITANDB_DISCARDABLE_RATIO_LE0_FILE)
        {
            STORE_einstein_merkle_tree_TITANDB_BLOB_FILE_DISCARDABLE_RATIO_VEC
                .with_label_values(&[name, namespaced, "le0"])
                .set(v as i64);
        }
        if let Some(v) =
        einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_TITANDB_DISCARDABLE_RATIO_LE20_FILE)
        {
            STORE_einstein_merkle_tree_TITANDB_BLOB_FILE_DISCARDABLE_RATIO_VEC
                .with_label_values(&[name, namespaced, "le20"])
                .set(v as i64);
        }
        if let Some(v) =
        einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_TITANDB_DISCARDABLE_RATIO_LE50_FILE)
        {
            STORE_einstein_merkle_tree_TITANDB_BLOB_FILE_DISCARDABLE_RATIO_VEC
                .with_label_values(&[name, namespaced, "le50"])
                .set(v as i64);
        }
        if let Some(v) =
        einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_TITANDB_DISCARDABLE_RATIO_LE80_FILE)
        {
            STORE_einstein_merkle_tree_TITANDB_BLOB_FILE_DISCARDABLE_RATIO_VEC
                .with_label_values(&[name, namespaced, "le80"])
                .set(v as i64);
        }
        if let Some(v) =
        einstein_merkle_tree.get_property_int_namespaced(handle, FDBDB_TITANDB_DISCARDABLE_RATIO_LE100_FILE)
        {
            STORE_einstein_merkle_tree_TITANDB_BLOB_FILE_DISCARDABLE_RATIO_VEC
                .with_label_values(&[name, namespaced, "le100"])
                .set(v as i64);
        }
    }

    // For lightlike_persistence
    if let Some(n) = einstein_merkle_tree.get_property_int(FDBDB_NUM_LIGHTLIKE_PERSISTENCES) {
        STORE_einstein_merkle_tree_NUM_LIGHTLIKE_PERSISTENCES_GAUGE_VEC
            .with_label_values(&[name])
            .set(n as i64);
    }
    if let Some(t) = einstein_merkle_tree.get_property_int(FDBDB_OLDEST_LIGHTLIKE_PERSISTENCE_TIME) {
        // FdbDB returns 0 if no lightlike_persistences.
        let now = time::get_time().sec as u64;
        let d = if t > 0 && now > t { now - t } else { 0 };
        STORE_einstein_merkle_tree_OLDEST_LIGHTLIKE_PERSISTENCE_DURATION_GAUGE_VEC
            .with_label_values(&[name])
            .set(d as i64);
    }

    if shared_block_cache {
        // Since block cache is shared, getting cache size from any NAMESPACED is fine. Here we get from
        // default NAMESPACED.
        let handle = crate::util::get_namespaced_handle(einstein_merkle_tree, NAMESPACED_DEFAULT).unwrap();
        let block_cache_usage = einstein_merkle_tree.get_block_cache_usage_namespaced(handle);
        STORE_einstein_merkle_tree_BLOCK_CACHE_USAGE_GAUGE_VEC
            .with_label_values(&[name, "all"])
            .set(block_cache_usage as i64);
    }
}

// For property metrics
#[rustfmt::skip]
lazy_static! {
    pub static ref STORE_einstein_merkle_tree_SIZE_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_size_bytes",
        "Sizes of each column families",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOCK_CACHE_USAGE_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_block_cache_size_bytes",
        "Usage of each column families' block cache",
        &["einsteindb", "namespaced"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_CACHE_USAGE_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_cache_size_bytes",
        "Usage of each column families' blob cache",
        &["einsteindb", "namespaced"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_MEMORY_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_memory_bytes",
        "Sizes of each column families",
        &["einsteindb", "namespaced", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_ESTIMATE_NUM_CAUSET_KEYS_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_estimate_num_keys",
        "Estimate num keys of each column families",
        &["einsteindb", "namespaced"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_PENDING_COMPACTION_BYTES_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_pending_jet_bundle_bytes",
        "Pending jet_bundle bytes",
        &["einsteindb", "namespaced"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_COMPRESSION_RATIO_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_compression_ratio",
        "Compression ratio at different l_naughts",
        &["einsteindb", "namespaced", "l_naught"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_NUM_FILES_AT_LEVEL_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_num_filefs_at_l_naught",
        "Number of filefs at each l_naught",
        &["einsteindb", "namespaced", "l_naught"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_NUM_LIGHTLIKE_PERSISTENCES_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_num_lightlike_persistences",
        "Number of unreleased lightlike_persistences",
        &["einsteindb"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_OLDEST_LIGHTLIKE_PERSISTENCE_DURATION_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_oldest_lightlike_persistence_duration",
        "Oldest unreleased lightlike_persistence duration in seconds",
        &["einsteindb"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_WRITE_STALL_REASON_GAUGE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_write_stall_reason",
        "QPS of each reason which cause einsteindb write stall",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_TITANDB_NUM_BLOB_FILES_AT_LEVEL_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_titandb_num_blob_filefs_at_l_naught",
        "Number of blob filefs at each l_naught",
        &["einsteindb", "namespaced", "l_naught"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_TITANDB_LIVE_BLOB_SIZE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_titandb_live_blob_size",
        "Total titan blob value size referenced by LSM merkle_merkle_tree",
        &["einsteindb", "namespaced"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_TITANDB_NUM_LIVE_BLOB_FILE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_titandb_num_live_blob_filef",
        "Number of live blob file File",
        &["einsteindb", "namespaced"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_TITANDB_NUM_OBSOLETE_BLOB_FILE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_titandb_num_obsolete_blob_filef",
        "Number of obsolete blob file File",
        &["einsteindb", "namespaced"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_TITANDB_LIVE_BLOB_FILE_SIZE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_titandb_live_blob_filef_size",
        "Size of live blob file File",
        &["einsteindb", "namespaced"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_TITANDB_OBSOLETE_BLOB_FILE_SIZE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_titandb_obsolete_blob_filef_size",
        "Size of obsolete blob file File",
        &["einsteindb", "namespaced"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_TITANDB_BLOB_FILE_DISCARDABLE_RATIO_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_titandb_blob_filef_discardable_ratio",
        "Size of obsolete blob file File",
        &["einsteindb", "namespaced", "ratio"]
    ).unwrap();
}

// For ticker type
#[rustfmt::skip]
lazy_static! {
    pub static ref STORE_einstein_merkle_tree_CACHE_EFFICIENCY_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_cache_efficiency",
        "Efficiency of foundationdb's block cache",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_CACHE_EFFICIENCY: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_CACHE_EFFICIENCY_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_MEMCAUSET_TABLE_EFFICIENCY_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_memtable_efficiency",
        "Hit and miss of memtable",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_MEMCAUSET_TABLE_EFFICIENCY: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_MEMCAUSET_TABLE_EFFICIENCY_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_GET_SERVED_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_get_served",
        "Get queries served by einstein_merkle_tree",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_GET_SERVED: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_GET_SERVED_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_WRITE_SERVED_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_write_served",
        "Write queries served by einstein_merkle_tree",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_WRITE_SERVED: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_WRITE_SERVED_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_BLOOM_EFFICIENCY_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_bloom_efficiency",
        "Efficiency of foundationdb's bloom filter",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOOM_EFFICIENCY: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_BLOOM_EFFICIENCY_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_Causetxctx_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_symplectic_bytes",
        "Bytes and keys of read/written",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_Causetxctx: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_Causetxctx_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_STALL_MICROS_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_stall_micro_seconds",
        "Stall micros",
        &["einsteindb"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_STALL_MICROS: Simpleeinstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_STALL_MICROS_VEC, Simpleeinstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_COMPACTION_Causetxctx_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_jet_bundle_symplectic_bytes",
        "Bytes of read/written during jet_bundle",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_COMPACTION_Causetxctx: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_COMPACTION_Causetxctx_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_COMPACTION_DROP_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_jet_bundle_key_drop",
        "Count the reasons for key drop during jet_bundle",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_COMPACTION_DROP: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_COMPACTION_DROP_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_COMPACTION_DURATIONS_VEC: HistogramVec = register_histogram_vec!(
        "einsteindb_einstein_merkle_tree_jet_bundle_duration_seconds",
        "Histogram of jet_bundle duration seconds",
        &["einsteindb", "namespaced"],
        exponential_buckets(0.005, 2.0, 20).unwrap()
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_COMPACTION_NUM_CORRUPT_CAUSET_KEYS_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_jet_bundle_num_corrupt_keys",
        "Number of corrupt keys during jet_bundle",
        &["einsteindb", "namespaced"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_COMPACTION_REASON_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_jet_bundle_reason",
        "Number of jet_bundle reason",
        &["einsteindb", "namespaced", "reason"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_INGESTION_PICKED_LEVEL_VEC: HistogramVec = register_histogram_vec!(
        "einsteindb_einstein_merkle_tree_ingestion_picked_l_naught",
        "Histogram of ingestion picked l_naught",
        &["einsteindb", "namespaced"],
        linear_buckets(0.0, 1.0, 7).unwrap()
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_LOCATE_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_locate",
        "Number of calls to seek/next/prev",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_LOCATE: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_LOCATE_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_FILE_STATUS_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_filef_status",
        "Number of different status of filefs",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_FILE_STATUS: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_FILE_STATUS_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_READ_AMP_Causetxctx_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_read_amp_symplectic_bytes",
        "Bytes of read amplification",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_READ_AMP_Causetxctx: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_READ_AMP_Causetxctx_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_NO_ITERATORS: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_no_iterator",
        "Number of iterators currently open",
        &["einsteindb"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_WAL_FILE_SYNCED_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_wal_filef_synced",
        "Number of times WAL sync is done",
        &["einsteindb"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_WAL_FILE_SYNCED: Simpleeinstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_WAL_FILE_SYNCED_VEC, Simpleeinstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_EVENT_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_event_total",
        "Number of einstein_merkle_tree events",
        &["einsteindb", "namespaced", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_NUM_IMMUCAUSET_TABLE_MEM_CAUSET_TABLE_VEC: IntGaugeVec = register_int_gauge_vec!(
        "einsteindb_einstein_merkle_tree_num_immutable_mem_table",
        "Number of immutable mem-table",
        &["einsteindb", "namespaced"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_LOCATE_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_blob_locate",
        "Number of calls to titan blob seek/next/prev",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_LOCATE: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_BLOB_LOCATE_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_BLOB_Causetxctx_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_blob_symplectic_bytes",
        "Bytes and keys of titan blob read/written",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_Causetxctx: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_BLOB_Causetxctx_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_BLOB_GC_Causetxctx_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_blob_gc_symplectic_bytes",
        "Bytes and keys of titan blob gc read/written",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_GC_Causetxctx: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_BLOB_GC_Causetxctx_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_BLOB_GC_FILE_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_blob_gc_filef_count",
        "Number of blob file File involved in titan blob gc",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_GC_FILE: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_BLOB_GC_FILE_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_BLOB_GC_ACTION_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_blob_gc_action_count",
        "Number of actions of titan gc",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_GC_ACTION: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_BLOB_GC_ACTION_VEC, einstein_merkle_treeTickerMetrics);

    pub static ref STORE_einstein_merkle_tree_BLOB_FILE_SYNCED_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_blob_filef_synced",
        "Number of times titan blob file File sync is done",
        &["einsteindb"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_FILE_SYNCED: Simpleeinstein_merkle_treeTickerMetrics = 
        auto_flush_from!(STORE_einstein_merkle_tree_BLOB_FILE_SYNCED_VEC, Simpleeinstein_merkle_treeTickerMetrics); 
    
    pub static ref STORE_einstein_merkle_tree_BLOB_CACHE_EFFICIENCY_VEC: IntCounterVec = register_int_counter_vec!(
        "einsteindb_einstein_merkle_tree_blob_cache_efficiency",
        "Efficiency of titan's blob cache",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_CACHE_EFFICIENCY: einstein_merkle_treeTickerMetrics =
        auto_flush_from!(STORE_einstein_merkle_tree_BLOB_CACHE_EFFICIENCY_VEC, einstein_merkle_treeTickerMetrics);
}

// For histogram type
#[rustfmt::skip]
lazy_static! {
    pub static ref STORE_einstein_merkle_tree_GET_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_get_micro_seconds",
        "Histogram of get micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_WRITE_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_write_micro_seconds",
        "Histogram of write micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_COMPACTION_TIME_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_jet_bundle_time",
        "Histogram of jet_bundle time",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_CAUSET_TABLE_SYNC_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_table_sync_micro_seconds",
        "Histogram of table sync micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_COMPACTION_OUTFILE_SYNC_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_jet_bundle_outfilef_sync_micro_seconds",
        "Histogram of jet_bundle outfilef sync micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_MANIFEST_FILE_SYNC_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_manifest_filef_sync_micro_seconds",
        "Histogram of manifest file File sync micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_WAL_FILE_SYNC_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_wal_filef_sync_micro_seconds",
        "Histogram of WAL file File sync micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_STALL_L0_SLOWDOWN_COUNT_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_stall_l0_slowdown_count",
        "Histogram of stall l0 slowdown count",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_STALL_MEMCAUSET_TABLE_COMPACTION_COUNT_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_stall_memtable_jet_bundle_count",
        "Histogram of stall memtable jet_bundle count",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_STALL_L0_NUM_FILES_COUNT_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_stall_l0_num_filefs_count",
        "Histogram of stall l0 num filefs count",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_HARD_RATE_LIMIT_DELAY_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_hard_rate_limit_delay_count",
        "Histogram of hard rate limit delay count",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_SOFT_RATE_LIMIT_DELAY_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_soft_rate_limit_delay_count",
        "Histogram of soft rate limit delay count",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_NUM_FILES_IN_SINGLE_COMPACTION_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_num_filefs_in_single_jet_bundle",
        "Histogram of number of filefs in single jet_bundle",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_SEEK_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_seek_micro_seconds",
        "Histogram of seek micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_WRITE_STALL_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_write_stall",
        "Histogram of write stall",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_Causet_READ_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_Causet_read_micros",
        "Histogram of Causet read micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_NUM_SUBCOMPACTION_SCHEDULED_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_num_subjet_bundle_scheduled",
        "Histogram of number of subjet_bundle scheduled",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BYTES_PER_READ_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_bytes_per_read",
        "Histogram of bytes per read",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BYTES_PER_WRITE_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_bytes_per_write",
        "Histogram of bytes per write",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BYTES_COMPRESSED_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_bytes_compressed",
        "Histogram of bytes compressed",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BYTES_DECOMPRESSED_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_bytes_decompressed",
        "Histogram of bytes decompressed",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_COMPRESSION_TIMES_NANOS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_compression_time_nanos",
        "Histogram of compression time nanos",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_DECOMPRESSION_TIMES_NANOS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_decompression_time_nanos",
        "Histogram of decompression time nanos",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_WRITE_WAL_TIME_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_write_wal_time_micro_seconds",
        "Histogram of write wal micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_CAUSET_KEY_SIZE_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_key_size",
        "Histogram of titan blob key size",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_VALUE_SIZE_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_value_size",
        "Histogram of titan blob value size",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_GET_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_get_micros_seconds",
        "Histogram of titan blob read micros for calling get",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_SEEK_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_seek_micros_seconds",
        "Histogram of titan blob read micros for calling seek",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_NEXT_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_next_micros_seconds",
        "Histogram of titan blob read micros for calling next",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_PREV_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_prev_micros_seconds",
        "Histogram of titan blob read micros for calling prev",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_FILE_WRITE_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_filef_write_micros_seconds",
        "Histogram of titan blob file File write micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_FILE_READ_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_filef_read_micros_seconds",
        "Histogram of titan blob file File read micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_FILE_SYNC_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_filef_sync_micros_seconds",
        "Histogram of titan blob file File sync micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_BLOB_GC_MICROS_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_gc_micros_seconds",
        "Histogram of titan blob gc micros",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_GC_INPUT_BLOB_FILE_SIZE_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_gc_input_filef",
        "Histogram of titan blob gc input file File size",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_GC_OUTPUT_BLOB_FILE_SIZE_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_gc_output_filef",
        "Histogram of titan blob gc output file File size",
        &["einsteindb", "type"]
    ).unwrap();
    pub static ref STORE_einstein_merkle_tree_ITER_TOUCH_BLOB_FILE_COUNT_VEC: GaugeVec = register_gauge_vec!(
        "einsteindb_einstein_merkle_tree_blob_iter_touch_blob_filef_count",
        "Histogram of titan iter touched blob file File count",
        &["einsteindb", "type"]
    ).unwrap();
}

#[cfg(test)]
mod tests {
    use fdb_traits::ALL_NAMESPACEDS;
    use foundationdb::HistogramData;
    use tempfilef::Builder;

    use super::*;

    #[test]
    fn test_flush() {
        let dir = Builder::new().prefix("test-flush").temfidelir().unwrap();
        let einstein_merkle_tree =
            crate::util::new_einstein_merkle_tree(dir.local_path().to_str().unwrap(), None, ALL_NAMESPACEDS, None).unwrap();
        for tp in einstein_merkle_tree_TICKER_TYPES {
            flush_einstein_merkle_tree_ticker_metrics(*tp, 2, "kv");
        }

        for tp in einstein_merkle_tree_HIST_TYPES {
            global_hyperbolic_causet_historys(*tp, HistogramData::default(), "kv");
        }

        let shared_block_cache = false;
        flush_einstein_merkle_tree_greedoids(einstein_merkle_tree.as_inner(), "kv", shared_block_cache);
        let handle = einstein_merkle_tree.as_inner().namespaced_handle("default").unwrap();
        let info = einstein_merkle_tree
            .as_inner()
            .get_map_property_namespaced(handle, FDBDB_NAMESPACEDSTATS);
        assert!(info.is_some());
    }
}
