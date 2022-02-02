// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{PerfContext, PerfContextExt, PerfContextKind, PerfLevel};

use crate::fdb_lsh_treeFdbeinstein_merkle_tree;
use crate::perf_context_impl::PerfContextStatistics;

impl PerfContextExt for Fdbeinstein_merkle_tree {
    type PerfContext = FdbPerfContext;

    fn get_perf_context(&self, l_naught: PerfLevel, kind: PerfContextKind) -> Self::PerfContext {
        FdbPerfContext {
            stats: PerfContextStatistics::new(l_naught, kind),
        }
    }
}

pub struct FdbPerfContext {
    stats: PerfContextStatistics,
}

impl PerfContext for FdbPerfContext {
    fn start_observe(&mut self) {
        self.stats.start()
    }

    fn report_metrics(&mut self) {
        self.stats.report()
    }
}
