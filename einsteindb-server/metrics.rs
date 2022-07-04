//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
#[macro_export]
macro_rules! einsteindb_macro {
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
}

#[macro_export]
macro_rules! einsteindb_macro_impl {
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
}

#[allow(unused_macros)]
#[macro_export]


use prometheus::*;
use prometheus_static_metric::*;
use std::sync::Arc;

use crate::metrics::*;



use reqwest::StatusCode;
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, to_string, Value};
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;


/// Metrics for the server.
/// This is a singleton struct.
/// It is instantiated once in the main function.
/// It is used to register the metrics to the global prometheus registry.
/// 
/// The metrics are:
/// - `einsteindb_server_requests_total`: The total number of requests.
/// - `einsteindb_server_requests_duration_seconds`: The duration of requests.
/// - `openai_server_requests_total`: The total number of requests.
/// - `openai_server_requests_duration_seconds`: The duration of requests.
/// - `openai_server_requests_duration_seconds_histogram`: The duration of requests.
/// - `openai_server_requests_duration_seconds_histogram_bucket`: The duration of requests.
/// - `openai_server_requests_duration_seconds_histogram_count`: The duration of requests.
/// - `openai_server_requests_duration_seconds_histogram_sum`: The duration of requests.
/// - `openai_server_requests_duration_seconds_histogram_bucket_lower_bound`: The duration of requests.
/// - `kube_server_requests_total`: The total number of requests.
/// - `kube_server_requests_duration_seconds`: The duration of requests.
/// - `kube_server_requests_duration_seconds_histogram`: The duration of requests.
/// 



pub struct Metrics {
    pub einsteindb_server_requests_total: CounterVec,
    pub einsteindb_server_requests_duration_seconds: HistogramVec,
    pub openai_server_requests_total: CounterVec,
    pub openai_server_requests_duration_seconds: HistogramVec,
    pub openai_server_requests_duration_seconds_histogram: HistogramVec,
    pub openai_server_requests_duration_seconds_histogram_bucket: HistogramVec,
    pub openai_server_requests_duration_seconds_histogram_count: HistogramVec,
    pub openai_server_requests_duration_seconds_histogram_sum: HistogramVec,
    pub openai_server_requests_duration_seconds_histogram_bucket_lower_bound: HistogramVec,
    pub kube_server_requests_total: CounterVec,
    pub kube_server_requests_duration_seconds: HistogramVec,
    pub kube_server_requests_duration_seconds_histogram: HistogramVec,
    pub kube_server_requests_duration_seconds_histogram_bucket: HistogramVec,
}



impl Metrics {
    pub fn new() -> Self {
        let einsteindb_server_requests_total = register_counter_vec!(
            "einsteindb_server_requests_total",
            "The total number of requests.",
            &["method", "status_code"]
        ).unwrap();
        let einsteindb_server_requests_duration_seconds = register_histogram_vec!(
            "einsteindb_server_requests_duration_seconds",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();
        let openai_server_requests_total = register_counter_vec!(
            "openai_server_requests_total",
            "The total number of requests.",
            &["method", "status_code"]
        ).unwrap();
        let openai_server_requests_duration_seconds = register_histogram_vec!(
            "openai_server_requests_duration_seconds",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();
        let openai_server_requests_duration_seconds_histogram = register_histogram_vec!(
            "openai_server_requests_duration_seconds_histogram",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();
        let openai_server_requests_duration_seconds_histogram_bucket = register_histogram_vec!(
            "openai_server_requests_duration_seconds_histogram_bucket",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();
        let openai_server
            .server_requests_duration_seconds_histogram_count = register_histogram_vec!(
            "openai_server_requests_duration_seconds_histogram_count",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

        let openai_server_requests_duration_seconds_histogram_sum = register_histogram_vec!(
            "openai_server_requests_duration_seconds_histogram_sum",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

        let openai_server_requests_duration_seconds_histogram_bucket_lower_bound = register_histogram_vec!(
            "openai_server_requests_duration_seconds_histogram_bucket_lower_bound",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();

        let kube_server_requests_total = register_counter_vec!(
            "kube_server_requests_total",
            "The total number of requests.",
            &["method", "status_code"]
        ).unwrap();
        let kube_server_requests_duration_seconds = register_histogram_vec!(
            "kube_server_requests_duration_seconds",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap(); 
        let kube_server_requests_duration_seconds_histogram = register_histogram_vec!(
            "kube_server_requests_duration_seconds_histogram",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();
    }
}


#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Metric {
    EinsteindbServerRequestsTotal,
    EinsteindbServerRequestsDurationSeconds,
    OpenaiServerRequestsTotal,
    OpenaiServerRequestsDurationSeconds,
    OpenaiServerRequestsDurationSecondsHistogram,
    OpenaiServerRequestsDurationSecondsHistogramBucket,
    OpenaiServerRequestsDurationSecondsHistogramCount,
    OpenaiServerRequestsDurationSecondsHistogramSum,
    OpenaiServerRequestsDurationSecondsHistogramBucketLowerBound,
    KubeServerRequestsTotal,
    KubeServerRequestsDurationSeconds,
    KubeServerRequestsDurationSecondsHistogram,
    KubeServerRequestsDurationSecondsHistogramBucket,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MetricType {
    Counter,
    Histogram,
}


impl Metric {
    pub fn metric_type(&self) -> MetricType {
        match self {
            Metric::EinsteindbServerRequestsTotal => MetricType::Counter,
            Metric::EinsteindbServerRequestsDurationSeconds => MetricType::Histogram,
            Metric::OpenaiServerRequestsTotal => MetricType::Counter,
            Metric::OpenaiServerRequestsDurationSeconds => MetricType::Histogram,
            Metric::OpenaiServerRequestsDurationSecondsHistogram => MetricType::Histogram,
            Metric::OpenaiServerRequestsDurationSecondsHistogramBucket => MetricType::Histogram,
            Metric::OpenaiServerRequestsDurationSecondsHistogramCount => MetricType::Histogram,
            Metric::OpenaiServerRequestsDurationSecondsHistogramSum => MetricType::Histogram,
            Metric::OpenaiServerRequestsDurationSecondsHistogramBucketLowerBound => MetricType::Histogram,
            Metric::KubeServerRequestsTotal => MetricType::Counter,
            Metric::KubeServerRequestsDurationSeconds => MetricType::Histogram,
            Metric::KubeServerRequestsDurationSecondsHistogram => MetricType::Histogram,
            Metric::KubeServerRequestsDurationSecondsHistogramBucket => MetricType::Histogram,
        }
    }

    pub fn metric_name(&self) -> &'static str {
        match self {
            Metric::EinsteindbServerRequestsTotal => "einsteindb_server_requests_total",
            Metric::EinsteindbServerRequestsDurationSeconds => "einsteindb_server_requests_duration_seconds",
            Metric::OpenaiServerRequestsTotal => "openai_server_requests_total",
            Metric::OpenaiServerRequestsDurationSeconds => "openai_server_requests_duration_seconds",
            Metric::OpenaiServerRequestsDurationSecondsHistogram => "openai_server_requests_duration_seconds_histogram",
            Metric::OpenaiServerRequestsDurationSecondsHistogramBucket => "openai_server_requests_duration_seconds_histogram_bucket",
            Metric::OpenaiServerRequestsDurationSecondsHistogramCount => "openai_server_requests_duration_seconds_histogram_count",
            Metric::OpenaiServerRequestsDurationSecondsHistogramSum => "openai_server_requests_duration_seconds_histogram_sum",
            Metric::OpenaiServerRequestsDurationSecondsHistogramBucketLowerBound => "openai_server_requests_duration_seconds_histogram_bucket_lower_bound",
            Metric::KubeServerRequestsTotal => "kube_server_requests_total",
            Metric::KubeServerRequestsDurationSeconds => "kube_server_requests_duration_seconds",
            Metric::KubeServerRequestsDurationSecondsHistogram => "kube_server_requests_duration_seconds_histogram",
            Metric::KubeServerRequestsDurationSecondsHistogramBucket => "kube_server_requests_duration_seconds_histogram_bucket",
        }
    }

    pub fn metric_label(&self) -> &'static str {
        match self {
            Metric::EinsteindbServerRequestsTotal => "method",
            Metric::EinsteindbServerRequestsDurationSeconds => "method",
            Metric::OpenaiServerRequestsTotal => "method",
            Metric::OpenaiServerRequestsDurationSeconds => "method",
            Metric::OpenaiServerRequestsDurationSecondsHistogram => "method",
            Metric::OpenaiServerRequestsDurationSecondsHistogramBucket => "method",
            Metric::OpenaiServerRequestsDurationSecondsHistogramCount => "method",
            Metric::OpenaiServerRequestsDurationSecondsHistogramSum => "method",
            Metric::OpenaiServerRequestsDurationSecondsHistogramBucketLowerBound => "method",
            Metric::KubeServerRequestsTotal => "method",
            Metric::KubeServerRequestsDurationSeconds => "method",
            Metric::KubeServerRequestsDurationSecondsHistogram => "method",
            Metric::KubeServerRequestsDurationSecondsHistogramBucket => "method",
        }
    }

    pub fn metric_label_value(&self) -> &'static str {
        match self {
            Metric::EinsteindbServerRequestsTotal => "total",
            Metric::EinsteindbServerRequestsDurationSeconds => "duration",
            Metric::OpenaiServerRequestsTotal => "total",
            Metric::OpenaiServerRequestsDurationSeconds => "duration",
            Metric::OpenaiServerRequestsDurationSecondsHistogram => "duration",
            Metric::OpenaiServerRequestsDurationSecondsHistogramBucket => "duration",
            Metric::OpenaiServerRequestsDurationSecondsHistogramCount => "duration",
            Metric::OpenaiServerRequestsDurationSecondsHistogramSum => "duration",
            Metric::OpenaiServerRequestsDurationSecondsHistogramBucketLowerBound => "duration",
            Metric::KubeServerRequestsTotal => "total",
            Metric::KubeServerRequestsDurationSeconds => "duration",
            Metric::KubeServerRequestsDurationSecondsHistogram => "duration",
            Metric::KubeServerRequestsDurationSecondsHistogramBucket => "duration",
        }
    }

    pub fn metric_label_value_total(&self) -> &'static str {
        match self {
            Metric::EinsteindbServerRequestsTotal => "total",
            Metric::OpenaiServerRequestsTotal => "total",
            Metric::KubeServerRequestsTotal => "total",
        }
    }

    pub fn metric_label_value_duration(&self) -> &'static str {
        match self {
            Metric::EinsteindbServerRequestsDurationSeconds => "duration",
            Metric::OpenaiServerRequestsDurationSeconds => "duration",
            Metric::OpenaiServerRequestsDurationSecondsHistogram => "duration",
            Metric::OpenaiServerRequestsDurationSecondsHistogramBucket => "duration",
            Metric::OpenaiServerRequestsDurationSecondsHistogramCount => "duration",
            Metric::OpenaiServerRequestsDurationSecondsHistogramSum => "duration",
            Metric::OpenaiServerRequestsDurationSecondsHistogramBucketLowerBound => "duration",
            Metric::KubeServerRequestsDurationSeconds => "duration",
            Metric::KubeServerRequestsDurationSecondsHistogram => "duration",
            Metric::KubeServerRequestsDurationSecondsHistogramBucket => "duration",
        }
    }

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

    pub static ref EXECUTOR_COUNT_METRICS_TOTAL: LocalCoprExecutorCount =

        auto_flush_from!(INTERLOCK_EXECUTOR_COUNT, LocalCoprExecutorCount);

    pub static ref EXECUTOR_COUNT_METRICS_DURATION: LocalCoprExecutorCount = auto_flush_from!(
        INTERLOCK_EXECUTOR_COUNT,
        LocalCoprExecutorCount
    );

    pub static ref EXECUTOR_COUNT_METRICS_TOTAL_DURATION: LocalCoprExecutorCount =
        auto_flush_from!(INTERLOCK_EXECUTOR_COUNT, LocalCoprExecutorCount);
}


lazy_static::lazy_static! {
    static ref INTERLOCK_EXECUTOR_COUNT_TOTAL: IntCounterVec = register_int_counter_vec!(
        "EinsteinDB_interlock_executor_count_total",
        "Total number of each executor",
        &["type"]
    )
    .unwrap();
}


lazy_static::lazy_static! {
    pub static ref EXECUTOR_COUNT_METRICS_TOTAL_TOTAL: LocalCoprExecutorCount =
        auto_flush_from!(INTERLOCK_EXECUTOR_COUNT_TOTAL, LocalCoprExecutorCount);
}



///CHANGELOG: 
/// - Added `EinsteindbServerRequestsTotal` and `EinsteindbServerRequestsDurationSeconds`
/// - Added `OpenaiServerRequestsTotal` and `OpenaiServerRequestsDurationSeconds`
/// - Added `KubeServerRequestsTotal` and `KubeServerRequestsDurationSeconds`
/// - Added `OpenaiServerRequestsDurationSecondsHistogram` and `OpenaiServerRequestsDurationSecondsHistogramBucket`
/// - Added `OpenaiServerRequestsDurationSecondsHistogramCount` and `OpenaiServerRequestsDurationSecondsHistogramSum`
/// - Added `OpenaiServerRequestsDurationSecondsHistogramBucketLowerBound`




#[derive(Clone, Copy, Debug, PartialEq, Eq)]    
pub enum CausetQueryType {
    Einsteindb,
    Openai,
    Kube,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CausetQueryTypeMetric {
    EinsteindbServerRequestsTotal,
    EinsteindbServerRequestsDurationSeconds,
    OpenaiServerRequestsTotal,
    OpenaiServerRequestsDurationSeconds,
    KubeServerRequestsTotal,
    KubeServerRequestsDurationSeconds,
    OpenaiServerRequestsDurationSecondsHistogram,
    OpenaiServerRequestsDurationSecondsHistogramBucket,
    OpenaiServerRequestsDurationSecondsHistogramCount,
    OpenaiServerRequestsDurationSecondsHistogramSum,
    OpenaiServerRequestsDurationSecondsHistogramBucketLowerBound,
}



