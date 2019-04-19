//Copyright 2019 All Rights Reserved. Licensed under Apache-2.0

use prometheus::*;

lazy_static! {
    pub static ref ID_REQ_HISTOGRAM_VEC: HistogramVec = register_histogram_vec!(
        "einsteindb_interlocking_directorate_request_duration_seconds",
        "Histogram of interlocking directorate request duration",
        &["req"],
        exponential_buckets(0.0020, 1.1, 11).unwrap()
        )
        .unwrap();
        pub static ref OUTDATED_REQ_WAIT_TIME: HistogramVec = register_histogram_vec!(
            "einsteindb_interlocking_directorate_request_wait_seconds",
            "Histogram of interlocking directorate request wait duration",
            &["req"],
            exponential_buckets(0.0020, 1.1, 11).unwrap()
        )
    )
}