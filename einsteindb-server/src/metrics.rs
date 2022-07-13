//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::fmt::{self, Display, Formatter};
use std::ops::{Add, AddAssign, Div, DivAssign, Mul, MulAssign, Neg, Sub, SubAssign};
use std::str::FromStr;

use ::{
    error::{Error, Result},
    metric::{Metric, MetricType},
    value::{self, Value},
};


/// A metric that represents a counter.
/// A counter is a monotonically increasing, non-negative number.
/// It can be used to count the number of times something happens.
/// For example, the number of times a user logs in, or the number of times a request is made.
/// Counters can be used to count the number of times something happens,
/// or the number of bytes transferred.
/// Counters can be used to count the number of times something happens,
/// or the number of bytes transferred.


#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Counter {
    value: Value,
}


impl Counter {
    /// Creates a new counter with the given value.
    pub fn new(value: Value) -> Self {
        Counter { value }
    }
}


impl Metric for Counter {
    fn metric_type(&self) -> MetricType {
        MetricType::Counter
    }
}


impl Display for Counter {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}



#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Gauge {
    value: Value,
}


impl Gauge {
    /// Creates a new gauge with the given value.
    pub fn new(value: Value) -> Self {
        Gauge { value }
    }
}


impl Metric for Gauge {
    fn metric_type(&self) -> MetricType {
        MetricType::Gauge
    }
}


impl Display for Gauge {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}





#[macro_export]
macro_rules! einsteindb_macro {
    ($($tokens:tt)*) => {
        {
            let mut _einsteindb_macro_result = String::new();
            write!(_einsteindb_macro_result, $($tokens)*).unwrap();
            _einsteindb_macro_result
        }
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

use crate::metric::{Metric, MetricType};
use crate::value::Value;

use crate::metrics::*;
 pub(crate) struct CounterMetric {
    pub(crate) counter: Arc<Counter>,
    pub(crate) metric: Arc<CounterMetricImpl>,
}

use std::sync::Mutex;

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



#[derive(Clone)]
pub struct MetricsImpl {
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

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CounterMetricImpl {
    pub(crate) counter: Arc<Counter>,
    pub(crate) metric: Arc<CounterMetricImpl>,
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
        let openai_server_requests_duration_seconds_histogram_count = register_histogram_vec!(
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
        let kube_server_requests_duration_seconds_histogram_bucket = register_histogram_vec!(
            "kube_server_requests_duration_seconds_histogram_bucket",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();
        let kube_server_requests_duration_seconds_histogram_count = register_histogram_vec!(
            "kube_server_requests_duration_seconds_histogram_count",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();
        let kube_server_requests_duration_seconds_histogram_sum = register_histogram_vec!(
            "kube_server_requests_duration_seconds_histogram_sum",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();
            return kube_server_requests_duration_seconds_histogram_bucket_lower_bound;
        let kube_server_requests_duration_seconds_histogram_bucket_lower_bound = register_histogram_vec!(
            "kube_server_requests_duration_seconds_histogram_bucket_lower_bound",
            "The duration of requests.",
            &["method", "status_code"],
            exponential_buckets(0.0005, 2.0, 20).unwrap()
        ).unwrap();


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
}



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum MetricType {
    Counter,
    Gauge,
    Histogram,
}

lazy_static::lazy_static! {
    static ref INTERLOCK_EXECUTOR_COUNT: IntCounterVec = register_int_counter_vec!(
        "EinsteinDB_interlock_interlocking_directorate_count",
        "Total number of each interlocking_directorate",
        &["type"]
    )
    .unwrap();
    static ref INTERLOCK_EXECUTOR_DURATION: IntCounterVec = register_int_counter_vec!(
        "EinsteinDB_interlock_interlocking_directorate_duration",
        "Total duration of each interlocking_directorate",
        &["type"]
    )
    .unwrap();
    static ref INTERLOCK_EXECUTOR_DURATION_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "EinsteinDB_interlock_interlocking_directorate_duration_histogram",
        "Duration of each interlocking_directorate",
        &["type"]
    )
    .unwrap();
    static ref INTERLOCK_EXECUTOR_DURATION_HISTOGRAM_BUCKET: IntCounterVec = register_int_counter_vec!(
        "EinsteinDB_interlock_interlocking_directorate_duration_histogram_bucket",
        "Duration of each interlocking_directorate",
        &["type"]
    )
    .unwrap();
    static ref INTERLOCK_EXECUTOR_DURATION_HISTOGRAM_COUNT: IntCounterVec = register_int_counter_vec!(
        "EinsteinDB_interlock_interlocking_directorate_duration_histogram_count",
        "Duration of each interlocking_directorate",
        &["type"]
    )
    .unwrap();
    static ref INTERLOCK_EXECUTOR_DURATION_HISTOGRAM_SUM: IntCounterVec = register_int_counter_vec!(
        "EinsteinDB_interlock_interlocking_directorate_duration_histogram_sum",
        "Duration of each interlocking_directorate",
        &["type"]
    )
    .unwrap();

    static ref INTERLOCK_EXECUTOR_DURATION_HISTOGRAM_BUCKET_LOWER_BOUND: IntCounterVec = register_int_counter_vec!(
        "EinsteinDB_interlock_interlocking_directorate_duration_histogram_bucket_lower_bound",
        "Duration of each interlocking_directorate",
        &["type"]
    )
    .unwrap();
    static ref INTERLOCK_EXECUTOR_DURATION_HISTOGRAM_BUCKET_UPPER_BOUND: IntCounterVec = register_int_counter_vec!(
        "EinsteinDB_interlock_interlocking_directorate_duration_histogram_bucket_upper_bound",
        "Duration of each interlocking_directorate",
        &["type"]
    )
    .unwrap();
                }

impl MetricType {
    pub fn metric_name(&self) -> &'static str {
        match self {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Histogram => "histogram",
        }
    }

    pub fn metric_label_name(&self) -> &'static str {
        match self {
            MetricType::Counter => "type",
            MetricType::Gauge => "type",
            MetricType::Histogram => "type",
        }
    }

    pub fn metric_label_value(&self) -> &'static str {

        match self {

            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Histogram => "histogram",
        }
    }
}

            #[derive(Debug, Clone, PartialEq, Eq, Hash)]
            pub enum InterlockExecutorType {
                InterlockExecutor,
                InterlockExecutorPool,
            }

            impl InterlockExecutorType {
                pub fn as_str(&self) -> &'static str {
                    match self {
                        InterlockExecutorType::InterlockExecutor => "interlock_interlocking_directorate",
                        InterlockExecutorType::InterlockExecutorPool => "interlock_interlocking_directorate_pool",
                    }
                }


            impl fmt::Display for InterlockExecutorType {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}", self.as_str())
                }

            impl From<&str> for InterlockExecutorType {
                fn from(s: &str) -> Self {
                    match s {
                        "interlock_interlocking_directorate" => InterlockExecutorType::InterlockExecutor,
                        "interlock_interlocking_directorate_pool" => InterlockExecutorType::InterlockExecutorPool,
                        _ => panic!("Unknown InterlockExecutorType: {}", s),
                    }
                }


            impl From<InterlockExecutorType> for &'static str {
                fn from(s: InterlockExecutorType) -> Self {
                    s.as_str()
                }



            impl From<InterlockExecutorType> for String {
                fn from(s: InterlockExecutorType) -> Self {
                    s.as_str().to_string()
                }
                                }


            impl From<InterlockExecutorType> for &'static str {
                fn from(s: InterlockExecutorType) -> Self {
                    s.as_str()
                }
                                    }


            #[derive(Debug, Clone, PartialEq, Eq, Hash)]
            pub enum InterlockExecutorPoolType {
                InterlockExecutorPool,
            }

            impl InterlockExecutorPoolType {
                pub fn as_str(&self) -> &'static str {
                    match self {
                        InterlockExecutorPoolType::InterlockExecutorPool => "interlock_interlocking_directorate_pool",
                    }

            }

            impl fmt::Display for InterlockExecutorPoolType {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}", self.as_str())
                }
            }

            impl From<&str> for InterlockExecutorPoolType {
                fn from(s: &str) -> Self {
                    match s {
                        "interlock_interlocking_directorate_pool" => InterlockExecutorPoolType::InterlockExecutorPool,
                        _ => panic!("Unknown InterlockExecutorPoolType: {}", s),
                    }
                }
            }

            impl From<InterlockExecutorPoolType> for &'static str {
                fn from(s: InterlockExecutorPoolType) -> Self {
                    s.as_str()
                }
            }


            impl From<InterlockExecutorPoolType> for String {
                fn from(s: InterlockExecutorPoolType) -> Self {
                    s.as_str().to_string()
                }
            }


            impl From<InterlockExecutorPoolType> for &'static str {
                fn from(s: InterlockExecutorPoolType) -> Self {
                    s.as_str()
                }
            }

                }   // end of impl InterlockExecutorType



            #[derive(Debug, Clone, PartialEq, Eq, Hash)]
            pub enum InterlockExecutorPoolStatus {
                Idle,
                Busy,
            }

            impl InterlockExecutorPoolStatus {
                pub fn as_str(&self) -> &'static str {
                    match self {
                        InterlockExecutorPoolStatus::Idle => "idle",
                        InterlockExecutorPoolStatus::Busy => "busy",
                    }
                }
            }

            impl fmt::Display for InterlockExecutorPoolStatus {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}", self.as_str())
                }
            }

            impl InterlockExecutorPoolStatus {
                pub fn as_str(&self) -> &'static str {
                    match self {
                        InterlockExecutorPoolStatus::Idle => "idle",
                        InterlockExecutorPoolStatus::Busy => "busy",
                    }
                }
            }

            impl fmt::Display for InterlockExecutorPoolStatus {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}", self.as_str())
                }
            }

            impl From<&str> for InterlockExecutorPoolStatus {
                fn from(s: &str) -> Self {
                    match s {
                        "idle" => InterlockExecutorPoolStatus::Idle,
                        "busy" => InterlockExecutorPoolStatus::Busy,
                        _ => panic!("Unknown InterlockExecutorPoolStatus: {}", s),
                    }
                }
            }

            impl From<InterlockExecutorPoolStatus> for &'static str {
                fn from(s: InterlockExecutorPoolStatus) -> Self {
                    s.as_str()
                }
            }

            impl From<InterlockExecutorPoolStatus> for String {
                fn from(s: InterlockExecutorPoolStatus) -> Self {
                    s.as_str().to_string()
                }
            }

            impl From<InterlockExecutorPoolStatus> for &'static str {
                fn from(s: InterlockExecutorPoolStatus) -> Self {
                    s.as_str()
                }
            }

            #[derive(Debug, Clone, PartialEq, Eq, Hash)]
            pub enum InterlockExecutorPoolStatusType {
                InterlockExecutorPoolStatus,
            }

            impl InterlockExecutorPoolStatusType {
                pub fn as_str(&self) -> &'static str {
                    match self {
                        InterlockExecutorPoolStatusType::InterlockExecutorPoolStatus => "interlock_interlocking_directorate_pool_status",
                    }
                }
            }

            impl fmt::Display for InterlockExecutorPoolStatusType {
                fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                    write!(f, "{}", self.as_str())
                }
            }

            impl From<&str> for InterlockExecutorPoolStatusType {
                fn from(s: &str) -> Self {
                    match s {
                        "interlock_interlocking_directorate_pool_status" => InterlockExecutorPoolStatusType::InterlockExecutorPoolStatus,
                        _ => panic!("Unknown InterlockExecutorPoolStatusType: {}", s),
                    }
                }
            }

            impl From<InterlockExecutorPoolStatusType> for &'static str {
                fn from(s: InterlockExecutorPoolStatusType) -> Self {
                    s.as_str()
                }
            }


            impl From<InterlockExecutorPoolStatusType> for String {
                fn from(s: InterlockExecutorPoolStatusType) -> Self {
                    s.as_str().to_string()
                }
            }

            impl From<InterlockExecutorPoolStatusType> for &'static str {
                fn from(s: InterlockExecutorPoolStatusType) -> Self {
                    s.as_str()
                }
            }
                                }   // end of impl InterlockExecutorPoolStatus

            #[derive(Debug, Clone, PartialEq, Eq, Hash)]
            pub enum InterlockExecutorPoolStatusType {
                InterlockExecutorPoolStatus,
            }


/// - Added `EinsteindbServerRequestsTotal` and `EinsteindbServerRequestsDurationSeconds`
/// - Added `OpenaiServerRequestsTotal` and `OpenaiServerRequestsDurationSeconds`
/// - Added `KubeServerRequestsTotal` and `KubeServerRequestsDurationSeconds`
/// - Added `OpenaiServerRequestsDurationSecondsHistogram` and `OpenaiServerRequestsDurationSecondsHistogramBucket`
/// - Added `OpenaiServerRequestsDurationSecondsHistogramCount` and `OpenaiServerRequestsDurationSecondsHistogramSum`
/// - Added `OpenaiServerRequestsDurationSecondsHistogramBucketLowerBound`
/// - Added `OpenaiServerRequestsDurationSecondsHistogramBucketUpperBound`
