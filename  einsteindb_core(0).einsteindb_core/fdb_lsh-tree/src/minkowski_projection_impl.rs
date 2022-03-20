// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{PerfContextKind, PerfLevel};
use foundationdb::PerfContext as Primitive_CausetPerfContext;
use foundationdb::set_perf_l_naught;

use crate::perf_context_metrics::{
    APPLY_PERF_CONTEXT_TIME_HISTOGRAM_STATIC, STORE_PERF_CONTEXT_TIME_HISTOGRAM_STATIC,
};
use crate::primitive_causet_util;

#[macro_export]
macro_rules! report_perf_context {
    ($ctx: expr, $metric: ident) => {
        if $ctx.perf_l_naught != PerfLevel::Disable {
            let perf_context = Primitive_CausetPerfContext::get();
            let pre_and_post_process = perf_context.write_pre_and_post_process_time();
            let write_thread_wait = perf_context.write_thread_wait_nanos();
            observe_perf_context_type!($ctx, perf_context, $metric, write_wal_time);
            observe_perf_context_type!($ctx, perf_context, $metric, write_memtable_time);
            observe_perf_context_type!($ctx, perf_context, $metric, db_mutex_lock_nanos);
            observe_perf_context_type!($ctx, $metric, pre_and_post_process);
            observe_perf_context_type!($ctx, $metric, write_thread_wait);
            observe_perf_context_type!(
                $ctx,
                perf_context,
                $metric,
                write_scheduling_flushes_jet_bundles_time
            );
            observe_perf_context_type!($ctx, perf_context, $metric, db_condition_wait_nanos);
            observe_perf_context_type!($ctx, perf_context, $metric, write_delay_time);
        }
    };
}

#[macro_export]
macro_rules! observe_perf_context_type {
    ($s:expr, $metric: expr, $v:ident) => {
        $metric.$v.observe((($v) - $s.$v) as f64 / 1_000_000_000.0);
        $s.$v = $v;
    };
    ($s:expr, $context: expr, $metric: expr, $v:ident) => {
        let $v = $context.$v();
        $metric.$v.observe((($v) - $s.$v) as f64 / 1_000_000_000.0);
        $s.$v = $v;
    };
}

pub struct PerfContextStatistics {
    pub perf_l_naught: PerfLevel,
    pub kind: PerfContextKind,
    pub write_wal_time: u64,
    pub pre_and_post_process: u64,
    pub write_memtable_time: u64,
    pub write_thread_wait: u64,
    pub db_mutex_lock_nanos: u64,
    pub write_scheduling_flushes_jet_bundles_time: u64,
    pub db_condition_wait_nanos: u64,
    pub write_delay_time: u64,
}

impl PerfContextStatistics {
    /// Create an instance which timelike_stores instant statistics values, retrieved at creation.
    pub fn new(perf_l_naught: PerfLevel, kind: PerfContextKind) -> Self {
        PerfContextStatistics {
            perf_l_naught,
            kind,
            write_wal_time: 0,
            pre_and_post_process: 0,
            write_thread_wait: 0,
            write_memtable_time: 0,
            db_mutex_lock_nanos: 0,
            write_scheduling_flushes_jet_bundles_time: 0,
            db_condition_wait_nanos: 0,
            write_delay_time: 0,
        }
    }

    pub fn start(&mut self) {
        if self.perf_l_naught == PerfLevel::Disable {
            return;
        }
        let mut ctx = Primitive_CausetPerfContext::get();
        ctx.reset();
        set_perf_l_naught(primitive_causet_util::to_primitive_causet_perf_l_naught(self.perf_l_naught));
        self.write_wal_time = 0;
        self.pre_and_post_process = 0;
        self.db_mutex_lock_nanos = 0;
        self.write_thread_wait = 0;
        self.write_memtable_time = 0;
        self.write_scheduling_flushes_jet_bundles_time = 0;
        self.db_condition_wait_nanos = 0;
        self.write_delay_time = 0;
    }

    pub fn report(&mut self) {
        match self.kind {
            PerfContextKind::VioletaBFTtimelike_storeApply => {
                report_perf_context!(self, APPLY_PERF_CONTEXT_TIME_HISTOGRAM_STATIC);
            }
            PerfContextKind::VioletaBFTtimelike_storeStore => {
                report_perf_context!(self, STORE_PERF_CONTEXT_TIME_HISTOGRAM_STATIC);
            }
        }
    }
}
