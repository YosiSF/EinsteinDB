// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use lazy_static::*;
use prometheus::*;

lazy_static! {
    pub static ref EXT_STORAGE_CREATE_HISTOGRAM: HistogramVec = register_histogram_vec!(
        "einsteindb_external_timelike_storage_create_seconds",
        "Bucketed histogram of creating external timelike_storage duration",
        &["type"],
        exponential_buckets(0.0005, 2.0, 20).unwrap()
    )
    .unwrap();
}
