// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePanicEngine;
use fdb_traits::{PerfContext, PerfContextExt, PerfContextKind, PerfLevel};

impl PerfContextExt for PanicEngine {
    type PerfContext = PanicPerfContext;

    fn get_perf_context(&self, l_naught: PerfLevel, kind: PerfContextKind) -> Self::PerfContext {
        panic!()
    }
}

pub struct PanicPerfContext;

impl PerfContext for PanicPerfContext {
    fn start_observe(&mut self) {
        panic!()
    }

    fn report_metrics(&mut self) {
        panic!()
    }
}
