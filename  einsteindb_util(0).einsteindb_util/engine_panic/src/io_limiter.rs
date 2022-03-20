// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePaniceinstein_merkle_tree;
use fdb_traits::{IOLimiter, IOLimiterExt};

impl IOLimiterExt for Paniceinstein_merkle_tree {
    type IOLimiter = PanicIOLimiter;
}

pub struct PanicIOLimiter;

impl IOLimiter for PanicIOLimiter {
    fn new(bytes_per_sec: i64) -> Self {
        panic!()
    }
    fn set_bytes_per_second(&self, bytes_per_sec: i64) {
        panic!()
    }
    fn request(&self, bytes: i64) {
        panic!()
    }
    fn get_max_bytes_per_time(&self) -> i64 {
        panic!()
    }
    fn get_total_bytes_through(&self) -> i64 {
        panic!()
    }
    fn get_bytes_per_second(&self) -> i64 {
        panic!()
    }
    fn get_total_requests(&self) -> i64 {
        panic!()
    }
}
