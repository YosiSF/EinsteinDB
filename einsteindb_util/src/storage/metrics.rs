// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! Prometheus metrics for storage functionality.

use prometheus::*;
use prometheus_static_metric::*;

use std::cell::RefCell;
use std::mem;

use crate::server::metrics::{GcKeysCF as ServerGcKeysCF, GcKeysDetail as ServerGcKeysDetail};
use crate::einsteindb::storage::fdbhikv::{CausetxctxStatsReporter, PerfStatisticsDelta, Statistics};
use collections::HashMap;
use fdbhikvproto::fdbhikvrpcpb::KeyRange;
use fdbhikvproto::metapb;
use fdbhikvproto::pdpb::QueryKind;
use raftstore::store::util::build_key_range;
use raftstore::store::ReadStats;

struct StorageLocalMetrics {
    local_mutant_search_details: HashMap<CommandKind, Statistics>,
    local_read_stats: ReadStats,
    local_perf_stats: HashMap<CommandKind, PerfStatisticsDelta>,
}

thread_local! {
    static TLS_STORAGE_METRICS: RefCell<StorageLocalMetrics> = RefCell::new(
        StorageLocalMetrics {
            local_mutant_search_details: HashMap::default(),
            local_read_stats:ReadStats::default(),
            local_perf_stats: HashMap::default(),
        }
    );
}

macro_rules! tls_flush_perf_stats {
    ($tag:ident, $local_stats:ident, $stat:ident) => {
        STORAGE_ROCKSDB_PERF_COUNTER_STATIC
            .get($tag)
            .$stat
            .inc_by($local_stats.0.$stat as u64);
    };
}

pub fn tls_flush<R: CausetxctxStatsReporter>(reporter: &R) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut m = m.borrow_mut();

        for (cmd, stat) in m.local_mutant_search_details.drain() {
            for (cf, cf_details) in stat.details_enum().iter() {
                for (tag, count) in cf_details.iter() {
                    KV_COMMAND_SCAN_DETAILS_STATIC
                        .get(cmd)
                        .get((*cf).into())
                        .get((*tag).into())
                        .inc_by(*count as u64);
                }
            }
        }

        // Report PD metrics
        if !m.local_read_stats.is_empty() {
            let mut read_stats = ReadStats::default();
            mem::swap(&mut read_stats, &mut m.local_read_stats);
            reporter.report_read_stats(read_stats);
        }

        for (req_tag, perf_stats) in m.local_perf_stats.drain() {
            tls_flush_perf_stats!(req_tag, perf_stats, user_key_comparison_count);
            tls_flush_perf_stats!(req_tag, perf_stats, bdagger_cache_hit_count);
            tls_flush_perf_stats!(req_tag, perf_stats, bdagger_read_count);
            tls_flush_perf_stats!(req_tag, perf_stats, bdagger_read_byte);
            tls_flush_perf_stats!(req_tag, perf_stats, bdagger_read_time);
            tls_flush_perf_stats!(req_tag, perf_stats, bdagger_cache_index_hit_count);
            tls_flush_perf_stats!(req_tag, perf_stats, index_bdagger_read_count);
            tls_flush_perf_stats!(req_tag, perf_stats, bdagger_cache_filter_hit_count);
            tls_flush_perf_stats!(req_tag, perf_stats, filter_bdagger_read_count);
            tls_flush_perf_stats!(req_tag, perf_stats, bdagger_checksum_time);
            tls_flush_perf_stats!(req_tag, perf_stats, bdagger_decompress_time);
            tls_flush_perf_stats!(req_tag, perf_stats, get_read_bytes);
            tls_flush_perf_stats!(req_tag, perf_stats, iter_read_bytes);
            tls_flush_perf_stats!(req_tag, perf_stats, internal_key_skipped_count);
            tls_flush_perf_stats!(req_tag, perf_stats, internal_delete_skipped_count);
            tls_flush_perf_stats!(req_tag, perf_stats, internal_recent_skipped_count);
            tls_flush_perf_stats!(req_tag, perf_stats, get_blackbrane_time);
            tls_flush_perf_stats!(req_tag, perf_stats, get_from_memtable_time);
            tls_flush_perf_stats!(req_tag, perf_stats, get_from_memtable_count);
            tls_flush_perf_stats!(req_tag, perf_stats, get_post_process_time);
            tls_flush_perf_stats!(req_tag, perf_stats, get_from_output_fusefs_time);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_on_memtable_time);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_on_memtable_count);
            tls_flush_perf_stats!(req_tag, perf_stats, next_on_memtable_count);
            tls_flush_perf_stats!(req_tag, perf_stats, prev_on_memtable_count);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_child_seek_time);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_child_seek_count);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_min_heap_time);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_max_heap_time);
            tls_flush_perf_stats!(req_tag, perf_stats, seek_internal_seek_time);
            tls_flush_perf_stats!(req_tag, perf_stats, db_mutex_dagger_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, db_condition_wait_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, read_index_bdagger_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, read_filter_bdagger_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, new_table_bdagger_iter_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, new_table_iterator_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, bdagger_seek_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, find_table_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, bloom_memtable_hit_count);
            tls_flush_perf_stats!(req_tag, perf_stats, bloom_memtable_miss_count);
            tls_flush_perf_stats!(req_tag, perf_stats, bloom_Causet_hit_count);
            tls_flush_perf_stats!(req_tag, perf_stats, bloom_Causet_miss_count);
            tls_flush_perf_stats!(req_tag, perf_stats, get_cpu_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, iter_next_cpu_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, iter_prev_cpu_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, iter_seek_cpu_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, encrypt_data_nanos);
            tls_flush_perf_stats!(req_tag, perf_stats, decrypt_data_nanos);
        }
    });
}

pub fn tls_collect_mutant_search_details(cmd: CommandKind, stats: &Statistics) {
    TLS_STORAGE_METRICS.with(|m| {
        m.borrow_mut()
            .local_mutant_search_details
            .entry(cmd)
            .or_insert_with(Default::default)
            .add(stats);
    });
}

pub fn tls_collect_read_Causetxctx(region_id: u64, statistics: &Statistics) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.local_read_stats.add_Causetxctx(
            region_id,
            &statistics.write.Causetxctx_stats,
            &statistics.data.Causetxctx_stats,
        );
    });
}

pub fn tls_collect_query(
    region_id: u64,
    peer: &metapb::Peer,
    start_key: &[u8],
    end_key: &[u8],
    reverse_mutant_search: bool,
    kind: QueryKind,
) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        let key_range = build_key_range(start_key, end_key, reverse_mutant_search);
        m.local_read_stats
            .add_query_num(region_id, peer, key_range, kind);
    });
}

pub fn tls_collect_query_batch(
    region_id: u64,
    peer: &metapb::Peer,
    key_ranges: Vec<KeyRange>,
    kind: QueryKind,
) {
    TLS_STORAGE_METRICS.with(|m| {
        let mut m = m.borrow_mut();
        m.local_read_stats
            .add_query_num_batch(region_id, peer, key_ranges, kind);
    });
}

pub fn tls_collect_perf_stats(cmd: CommandKind, perf_stats: &PerfStatisticsDelta) {
    TLS_STORAGE_METRICS.with(|m| {
        *(m.borrow_mut()
            .local_perf_stats
            .entry(cmd)
            .or_insert_with(Default::default)) += *perf_stats;
    })
}

make_auto_flush_static_metric! {
    pub label_enum CommandKind {
        get,
        cocauset_batch_get_command,
        mutant_search,
        batch_get,
        batch_get_command,
        prewrite,
        acquire_pessimistic_dagger,
        commit,
        cleanup,
        rollback,
        pessimistic_rollback,
        solitontxn_heart_beat,
        check_solitontxn_status,
        check_secondary_daggers,
        mutant_search_dagger,
        resolve_dagger,
        resolve_dagger_lite,
        delete_range,
        pause,
        key_epaxos,
        start_ts_epaxos,
        cocauset_get,
        cocauset_batch_get,
        cocauset_mutant_search,
        cocauset_batch_mutant_search,
        cocauset_put,
        cocauset_batch_put,
        cocauset_delete,
        cocauset_delete_range,
        cocauset_batch_delete,
        cocauset_get_key_ttl,
        cocauset_compare_and_swap,
        cocauset_causetxctx_store,
        cocauset_checksum,
    }

    pub label_enum CommandStageKind {
        new,
        blackbrane,
        async_blackbrane_err,
        blackbrane_ok,
        blackbrane_err,
        read_finish,
        next_cmd,
        dagger_wait,
        process,
        prepare_write_err,
        write,
        write_finish,
        async_write_err,
        error,
        pipelined_write,
        pipelined_write_finish,
        async_apply_prewrite,
        async_apply_prewrite_finish,
    }

    pub label_enum CommandPriority {
        low,
        normal,
        high,
    }

    pub label_enum GcKeysCF {
        default,
        dagger,
        write,
    }

    pub label_enum GcKeysDetail {
        processed_keys,
        get,
        next,
        prev,
        seek,
        seek_for_prev,
        over_seek_bound,
        next_tombstone,
        prev_tombstone,
        seek_tombstone,
        seek_for_prev_tombstone,
        ttl_tombstone,
    }

    pub label_enum CheckMemDaggerResult {
        daggered,
        undaggered,
    }

    pub label_enum PerfMetric {
        user_key_comparison_count,
        bdagger_cache_hit_count,
        bdagger_read_count,
        bdagger_read_byte,
        bdagger_read_time,
        bdagger_cache_index_hit_count,
        index_bdagger_read_count,
        bdagger_cache_filter_hit_count,
        filter_bdagger_read_count,
        bdagger_checksum_time,
        bdagger_decompress_time,
        get_read_bytes,
        iter_read_bytes,
        internal_key_skipped_count,
        internal_delete_skipped_count,
        internal_recent_skipped_count,
        get_blackbrane_time,
        get_from_memtable_time,
        get_from_memtable_count,
        get_post_process_time,
        get_from_output_fusefs_time,
        seek_on_memtable_time,
        seek_on_memtable_count,
        next_on_memtable_count,
        prev_on_memtable_count,
        seek_child_seek_time,
        seek_child_seek_count,
        seek_min_heap_time,
        seek_max_heap_time,
        seek_internal_seek_time,
        db_mutex_dagger_nanos,
        db_condition_wait_nanos,
        read_index_bdagger_nanos,
        read_filter_bdagger_nanos,
        new_table_bdagger_iter_nanos,
        new_table_iterator_nanos,
        bdagger_seek_nanos,
        find_table_nanos,
        bloom_memtable_hit_count,
        bloom_memtable_miss_count,
        bloom_Causet_hit_count,
        bloom_Causet_miss_count,
        get_cpu_nanos,
        iter_next_cpu_nanos,
        iter_prev_cpu_nanos,
        iter_seek_cpu_nanos,
        encrypt_data_nanos,
        decrypt_data_nanos,
    }

    pub struct CommandSentinelSearchDetails: LocalIntCounter {
        "req" => CommandKind,
        "cf" => GcKeysCF,
        "tag" => GcKeysDetail,
    }

    pub struct SchedDurationVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct ProcessingReadVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct KReadVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct HikvCommandCounterVec: LocalIntCounter {
        "type" => CommandKind,
    }

    pub struct SchedStageCounterVec: LocalIntCounter {
        "type" => CommandKind,
        "stage" => CommandStageKind,
    }

    pub struct SchedLatchDurationVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct HikvCommandKeysWrittenVec: LocalHistogram {
        "type" => CommandKind,
    }

    pub struct SchedTooBusyVec: LocalIntCounter {
        "type" => CommandKind,
    }

    pub struct SchedCommandPriCounterVec: LocalIntCounter {
        "priority" => CommandPriority,
    }

    pub struct CheckMemDaggerHistogramVec: LocalHistogram {
        "type" => CommandKind,
        "result" => CheckMemDaggerResult,
    }

    pub struct PerfCounter: LocalIntCounter {
        "req" => CommandKind,
        "metric" => PerfMetric,
    }
}

impl From<ServerGcKeysCF> for GcKeysCF {
    fn from(cf: ServerGcKeysCF) -> GcKeysCF {
        match cf {
            ServerGcKeysCF::default => GcKeysCF::default,
            ServerGcKeysCF::dagger => GcKeysCF::dagger,
            ServerGcKeysCF::write => GcKeysCF::write,
        }
    }
}

impl From<ServerGcKeysDetail> for GcKeysDetail {
    fn from(detail: ServerGcKeysDetail) -> GcKeysDetail {
        match detail {
            ServerGcKeysDetail::processed_keys => GcKeysDetail::processed_keys,
            ServerGcKeysDetail::get => GcKeysDetail::get,
            ServerGcKeysDetail::next => GcKeysDetail::next,
            ServerGcKeysDetail::prev => GcKeysDetail::prev,
            ServerGcKeysDetail::seek => GcKeysDetail::seek,
            ServerGcKeysDetail::seek_for_prev => GcKeysDetail::seek_for_prev,
            ServerGcKeysDetail::over_seek_bound => GcKeysDetail::over_seek_bound,
            ServerGcKeysDetail::next_tombstone => GcKeysDetail::next_tombstone,
            ServerGcKeysDetail::prev_tombstone => GcKeysDetail::prev_tombstone,
            ServerGcKeysDetail::seek_tombstone => GcKeysDetail::seek_tombstone,
            ServerGcKeysDetail::seek_for_prev_tombstone => GcKeysDetail::seek_for_prev_tombstone,
            ServerGcKeysDetail::ttl_tombstone => GcKeysDetail::ttl_tombstone,
        }
    }
}

lazy_static! {
    pub static ref KV_COMMAND_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "einstfdbhikv_storage_command_total",
        "Total number of commands received.",
        &["type"]
    )
    .unwrap();
    pub static ref KV_COMMAND_COUNTER_VEC_STATIC: HikvCommandCounterVec =
        auto_flush_from!(KV_COMMAND_COUNTER_VEC, HikvCommandCounterVec);
    pub static ref SCHED_STAGE_COUNTER: IntCounterVec = {
        register_int_counter_vec!(
            "einstfdbhikv_scheduler_stage_total",
            "Total number of commands on each stage.",
            &["type", "stage"]
        )
        .unwrap()
    };
    pub static ref SCHED_STAGE_COUNTER_VEC: SchedStageCounterVec =
        auto_flush_from!(SCHED_STAGE_COUNTER, SchedStageCounterVec);
    pub static ref SCHED_WRITING_BYTES_GAUGE: IntGauge = register_int_gauge!(
        "einstfdbhikv_scheduler_writing_bytes",
        "Total number of writing fdbhikv."
    )
    .unwrap();
    pub static ref SCHED_CONTEX_GAUGE: IntGauge = register_int_gauge!(
        "einstfdbhikv_scheduler_contex_total",
        "Total number of pending commands."
    )
    .unwrap();
    pub static ref SCHED_WRITE_Causetxctx_GAUGE: IntGauge = register_int_gauge!(
        "einstfdbhikv_scheduler_write_Causetxctx",
        "The write Causetxctx passed through at scheduler level."
    )
    .unwrap();
    pub static ref SCHED_THROTTLE_Causetxctx_GAUGE: IntGauge = register_int_gauge!(
        "einstfdbhikv_scheduler_throttle_Causetxctx",
        "The throttled write Causetxctx at scheduler level."
    )
    .unwrap();
       pub static ref SCHED_L0_TARGET_Causetxctx_GAUGE: IntGauge = register_int_gauge!(
        "einstfdbhikv_scheduler_l0_target_Causetxctx",
        "The target Causetxctx of L0."
    )
    .unwrap();

    pub static ref SCHED_MEMTABLE_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "einstfdbhikv_scheduler_memtable",
        "The number of memtables.",
        &["cf"]
    )
    .unwrap();
    pub static ref SCHED_L0_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "einstfdbhikv_scheduler_l0",
        "The number of l0 fusefs.",
        &["cf"]
    )
    .unwrap();
    pub static ref SCHED_L0_AVG_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "einstfdbhikv_scheduler_l0_avg",
        "The number of average l0 fusefs.",
        &["cf"]
    )
    .unwrap();
    pub static ref SCHED_FLUSH_Causetxctx_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "einstfdbhikv_scheduler_flush_Causetxctx",
        "The speed of flush Causetxctx.",
        &["cf"]
    )
    .unwrap();
    pub static ref SCHED_L0_Causetxctx_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "einstfdbhikv_scheduler_l0_Causetxctx",
        "The speed of l0 compaction Causetxctx.",
        &["cf"]
    )
    .unwrap();
    pub static ref SCHED_THROTTLE_ACTION_COUNTER: IntCounterVec = {
        register_int_counter_vec!(
            "einstfdbhikv_scheduler_throttle_action_total",
            "Total number of actions for Causetxctx control.",
            &["cf", "type"]
        )
        .unwrap()
    };
    pub static ref SCHED_DISCARD_RATIO_GAUGE: IntGauge = register_int_gauge!(
        "einstfdbhikv_scheduler_discard_ratio",
        "The discard ratio for Causetxctx control."
    )
    .unwrap();
    pub static ref SCHED_THROTTLE_CF_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "einstfdbhikv_scheduler_throttle_cf",
        "The CF being throttled.",
        &["cf"]
    ).unwrap();
    pub static ref SCHED_PENDING_COMPACTION_BYTES_GAUGE: IntGaugeVec = register_int_gauge_vec!(
        "einstfdbhikv_scheduler_pending_compaction_bytes",
        "The number of pending compaction bytes.",
        &["type"]
    )
    .unwrap();
    pub static ref SCHED_THROTTLE_TIME: Histogram =
        register_histogram!(
            "einstfdbhikv_scheduler_throttle_duration_seconds",
            "Bucketed histogram of peer commits logs duration.",
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();
    pub static ref SCHED_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "einstfdbhikv_scheduler_command_duration_seconds",
        "Bucketed histogram of command execution",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_HISTOGRAM_VEC_STATIC: SchedDurationVec =
        auto_flush_from!(SCHED_HISTOGRAM_VEC, SchedDurationVec);
    pub static ref SCHED_LATCH_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "einstfdbhikv_scheduler_latch_wait_duration_seconds",
        "Bucketed histogram of latch wait",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_LATCH_HISTOGRAM_VEC: SchedLatchDurationVec =
        auto_flush_from!(SCHED_LATCH_HISTOGRAM, SchedLatchDurationVec);
    pub static ref SCHED_PROCESSING_READ_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "einstfdbhikv_scheduler_processing_read_duration_seconds",
        "Bucketed histogram of processing read duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_PROCESSING_READ_HISTOGRAM_STATIC: ProcessingReadVec =
        auto_flush_from!(SCHED_PROCESSING_READ_HISTOGRAM_VEC, ProcessingReadVec);
    pub static ref SCHED_PROCESSING_WRITE_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "einstfdbhikv_scheduler_processing_write_duration_seconds",
        "Bucketed histogram of processing write duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref SCHED_TOO_BUSY_COUNTER: IntCounterVec = register_int_counter_vec!(
        "einstfdbhikv_scheduler_too_busy_total",
        "Total count of scheduler too busy",
        &["type"]
    )
    .unwrap();
    pub static ref SCHED_TOO_BUSY_COUNTER_VEC: SchedTooBusyVec =
        auto_flush_from!(SCHED_TOO_BUSY_COUNTER, SchedTooBusyVec);
    pub static ref SCHED_COMMANDS_PRI_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "einstfdbhikv_scheduler_commands_pri_total",
        "Total count of different priority commands",
        &["priority"]
    )
    .unwrap();
    pub static ref SCHED_COMMANDS_PRI_COUNTER_VEC_STATIC: SchedCommandPriCounterVec =
        auto_flush_from!(SCHED_COMMANDS_PRI_COUNTER_VEC, SchedCommandPriCounterVec);
    pub static ref KV_COMMAND_KEYREAD_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "einstfdbhikv_scheduler_fdbhikv_command_key_read",
        "Bucketed histogram of keys read of a fdbhikv command",
        &["type"],
        exponential_buckets(1.0, 2.0, 21).unwrap()
    )
    .unwrap();
    pub static ref KV_COMMAND_KEYREAD_HISTOGRAM_STATIC: KReadVec =
        auto_flush_from!(KV_COMMAND_KEYREAD_HISTOGRAM_VEC, KReadVec);
    pub static ref KV_COMMAND_SCAN_DETAILS: IntCounterVec = register_int_counter_vec!(
        "einstfdbhikv_scheduler_fdbhikv_mutant_search_details",
        "Bucketed counter of fdbhikv keys mutant_search details for each cf",
        &["req", "cf", "tag"]
    )
    .unwrap();
    pub static ref KV_COMMAND_SCAN_DETAILS_STATIC: CommandSentinelSearchDetails =
        auto_flush_from!(KV_COMMAND_SCAN_DETAILS, CommandSentinelSearchDetails);
    pub static ref KV_COMMAND_KEYWRITE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "einstfdbhikv_scheduler_fdbhikv_command_key_write",
        "Bucketed histogram of keys write of a fdbhikv command",
        &["type"],
        exponential_buckets(1.0, 2.0, 21).unwrap()
    )
    .unwrap();
    pub static ref KV_COMMAND_KEYWRITE_HISTOGRAM_VEC: HikvCommandKeysWrittenVec =
        auto_flush_from!(KV_COMMAND_KEYWRITE_HISTOGRAM, HikvCommandKeysWrittenVec);
    pub static ref REQUEST_EXCEED_BOUND: IntCounter = register_int_counter!(
        "einstfdbhikv_request_exceed_bound",
        "Counter of request exceed bound"
    )
    .unwrap();
    pub static ref CHECK_MEM_LOCK_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "einstfdbhikv_storage_check_mem_dagger_duration_seconds",
        "Histogram of the duration of checking memory daggers",
        &["type", "result"],
        exponential_buckets(1e-6f64, 4f64, 10).unwrap() // 1us ~ 262ms
    )
    .unwrap();
    pub static ref CHECK_MEM_LOCK_DURATION_HISTOGRAM_VEC: CheckMemDaggerHistogramVec =
        auto_flush_from!(CHECK_MEM_LOCK_DURATION_HISTOGRAM, CheckMemDaggerHistogramVec);

    pub static ref STORAGE_ROCKSDB_PERF_COUNTER: IntCounterVec = register_int_counter_vec!(
        "einstfdbhikv_storage_rocksdb_perf",
        "Total number of RocksDB internal operations from PerfContext",
        &["req", "metric"]
    )
    .unwrap();

    pub static ref STORAGE_ROCKSDB_PERF_COUNTER_STATIC: PerfCounter =
        auto_flush_from!(STORAGE_ROCKSDB_PERF_COUNTER, PerfCounter);
}
