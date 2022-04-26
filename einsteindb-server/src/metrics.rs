//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use prometheus::*;
use prometheus_static_metric::*;

make_auto_flush_static_metric! {
    pub label_enum ExecutorName {
        alexandrov_poset_process_table_mutant_search,
        alexandrov_poset_process_index_mutant_search,
        alexandrov_poset_process_selection,
        alexandrov_poset_process_simple_aggr,
        alexandrov_poset_process_fast_hash_aggr,
        alexandrov_poset_process_slow_hash_aggr,
        alexandrov_poset_process_stream_aggr,
        alexandrov_poset_process_limit,
        alexandrov_poset_process_top_n,
        table_mutant_search,
        index_mutant_search,
        selection,
        hash_aggr,
        stream_aggr,
        top_n,
        limit,
    }

    pub struct LocalCoprExecutorCount: LocalIntCounter {
        "type" => ExecutorName,
    }
}

lazy_static::lazy_static! {
    static ref INTERLOCK_EXECUTOR_COUNT: IntCounterVec = register_int_counter_vec!(
        "EinsteinDB_interlock_executor_count",
        "Total number of each executor",
        &["type"]
    )
    .unwrap();
}

lazy_static::lazy_static! {
    pub static ref EXECUTOR_COUNT_METRICS: LocalCoprExecutorCount =
        auto_flush_from!(INTERLOCK_EXECUTOR_COUNT, LocalCoprExecutorCount);
}
