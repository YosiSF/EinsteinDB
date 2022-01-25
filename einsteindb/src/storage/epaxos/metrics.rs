// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

make_static_metric! {
    pub label_enum EpaxosConflictKind {
        prewrite_write_conflict,
        rolled_back,
        commit_dagger_not_found,
        rollback_committed,
        acquire_pessimistic_dagger_conflict,
        pipelined_acquire_pessimistic_dagger_amend_fail,
        pipelined_acquire_pessimistic_dagger_amend_success,
    }

    pub label_enum EpaxosDuplicateCommandKind {
        prewrite,
        commit,
        rollback,
        acquire_pessimistic_dagger,
    }

    pub label_enum EpaxosCheckTxnStatusKind {
        rollback,
        update_ts,
        get_commit_info,
        pessimistic_rollback,
    }

    pub label_enum EpaxosPrewriteAssertionPerfKind {
        none,
        write_loaded,
        non_data_version_reload,
        write_not_loaded_reload,
        write_not_loaded_skip
    }

    pub struct EpaxosConflictCounterVec: IntCounter {
        "type" => EpaxosConflictKind,
    }

    pub struct EpaxosDuplicateCmdCounterVec: IntCounter {
        "type" => EpaxosDuplicateCommandKind,
    }

    pub struct EpaxosCheckTxnStatusCounterVec: IntCounter {
        "type" => EpaxosCheckTxnStatusKind,
    }

    pub struct EpaxosPrewriteAssertionPerfCounterVec: IntCounter {
        "type" => EpaxosPrewriteAssertionPerfKind,
    }
}

lazy_static! {
    pub static ref EPAXOS_VERSIONS_HISTOGRAM: Histogram = register_histogram!(
        "einstfdbhikv_storage_epaxos_versions",
        "Histogram of versions for each key",
        exponential_buckets(1.0, 2.0, 30).unwrap()
    )
    .unwrap();
    pub static ref GC_DELETE_VERSIONS_HISTOGRAM: Histogram = register_histogram!(
        "einstfdbhikv_storage_epaxos_gc_delete_versions",
        "Histogram of versions deleted by gc for each key",
        exponential_buckets(1.0, 2.0, 30).unwrap()
    )
    .unwrap();
    pub static ref CONCURRENCY_MANAGER_LOCK_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "einstfdbhikv_concurrency_manager_dagger_duration",
        "Histogram of the duration of dagger key in the concurrency manager",
        exponential_buckets(1e-7, 2.0, 20).unwrap() // 100ns ~ 100ms
    )
    .unwrap();
    pub static ref EPAXOS_CONFLICT_COUNTER: EpaxosConflictCounterVec = {
        register_static_int_counter_vec!(
            EpaxosConflictCounterVec,
            "einstfdbhikv_storage_epaxos_conflict_counter",
            "Total number of conflict error",
            &["type"]
        )
        .unwrap()
    };
    pub static ref EPAXOS_DUPLICATE_CMD_COUNTER_VEC: EpaxosDuplicateCmdCounterVec = {
        register_static_int_counter_vec!(
            EpaxosDuplicateCmdCounterVec,
            "einstfdbhikv_storage_epaxos_duplicate_cmd_counter",
            "Total number of duplicated commands",
            &["type"]
        )
        .unwrap()
    };
    pub static ref EPAXOS_CHECK_TXN_STATUS_COUNTER_VEC: EpaxosCheckTxnStatusCounterVec = {
        register_static_int_counter_vec!(
            EpaxosCheckTxnStatusCounterVec,
            "einstfdbhikv_storage_epaxos_check_solitontxn_status",
            "Counter of different results of check_solitontxn_status",
            &["type"]
        )
        .unwrap()
    };
    pub static ref EPAXOS_PREWRITE_ASSERTION_PERF_COUNTER_VEC: EpaxosPrewriteAssertionPerfCounterVec = {
        register_static_int_counter_vec!(
            EpaxosPrewriteAssertionPerfCounterVec,
            "einstfdbhikv_storage_epaxos_prewrite_assertion_perf",
            "Counter of assertion operations in transactions",
            &["type"]
        ).unwrap()
    };
}
