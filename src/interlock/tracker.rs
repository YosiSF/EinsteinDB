//Copyright 2019 Venire Labs Inc.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use crate::storage::einstein::{PerfStatisticsDelta, PerfStatisticsInstant};
use crate::util::futurepool;
use crate::util::petriclock::{Self, Duration, Instant};

use crate::interlock::posetDAG::async_executor::ExecutorMetrics;
use crate::interlock::*;

const   SLOW_QUERY_LOWER_BOUND: f64 = 1.0; //1 second

#[derive(Debug, Clone, Copy, PartialEq)]
enum MealyTS {

    //Tracker is not initialized - thus not a net.
    NetNotInit,
    
    //Initialized 
    NetInit,

    //All nets are running
    AllNetsBegan,

    //single net just began
    NetBegan,

    NetFinished,

    AllNetsFinished,

    //No more operations past this point.
    Marked,

}

//Track interlocking director requests to update stats and provide slow logs.
#[derive(Debug)]
pub struct MealyT {
    request_begin_at: Instant,
    net_begin_at: Instant,
    perf_statistics_start: Option<PerfStatisticsInstant>,

    current_stage: MealyTS,
    wait_time: Duration,
    req_time: Duration,
    item_process_time: Duration,
    total_process_time: Duration,
    total_exec_metrics: ExecutorMetrics,
    total_perf_statistics: PerfStatisticsDelta,

    pub req_ctx: ReqContext,

    ctxd: Option<futurepool::ComtextDelegators<ReadPoolContext>>,
}

impl MealyT {


    
}