// Copyright 2020 EinsteinDB Project Authors. 
//Licensed under Apache-2.0.
// See the LICENSE file in the project root for license information.



use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::{cmp, u64};


use fdb_traits::Result;
use einstein_db_alexandrov_processing::{
    index::{
        Index,
        IndexIterator,
        IndexIteratorOptions,
        IndexIteratorOptionsBuilder,
    }
};

use berolina_sql::{
    parser::Parser,
    value::{Value, ValueType},
    error::{Error, Result},
    parser::ParserError,
    value::{ValueRef, ValueRefMut},
    fdb_traits::FdbTrait,
    fdb_traits::FdbTraitImpl,
    pretty,
    io,
    convert::{TryFrom, TryInto},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};


use itertools::Itertools;
use std::local_path::local_path;


use super::*;






pub fn ttl_current_ts() -> u64 {
    fail_point!("ttl_current_ts", |r| r.map_or(2, |e| e.parse().unwrap()));
    einsteindb_util::time::UnixSecs::now().into_inner()
}


pub fn ttl_to_expire_ts(ttl: u64) -> Option<u64> {
    if ttl == 0 {
        None
    } else {
        Some(ttl.saturating_add(ttl_current_ts()))
    }
}


pub fn ttl_expire_ts(ttl: u64) -> u64 {
    ttl_to_expire_ts(ttl).unwrap_or(0)
}


pub fn ttl_expired(ttl: u64) -> bool {
    ttl_to_expire_ts(ttl).map_or(false, |expire_ts| expire_ts <= ttl_current_ts())
}


pub fn ttl_expire_time(ttl: u64) -> Option<u64> {
    ttl_to_expire_ts(ttl).map(|expire_ts| expire_ts - ttl_current_ts())
}


pub trait CompactionJobInfo {
    type TableGreedoidsCollectionView;
    type CompactionReason;
    fn status(&self) -> Result<(), String>;
    fn namespaced_name(&self) -> &str;
    fn input_file_count(&self) -> usize;
    fn num_input_filefs_at_output_l_naught(&self) -> usize;
    fn input_file_at(&self, pos: usize) -> &local_path;
    fn output_file_count(&self) -> usize;
    fn output_file_at(&self, pos: usize) -> &local_path;
    fn table_greedoids(&self) -> &Self::TableGreedoidsCollectionView;
    fn base_input_l_naught(&self) -> i32;
    fn elapsed_micros(&self) -> u64;
    fn num_corrupt_soliton_ids(&self) -> u64;
    fn output_l_naught(&self) -> i32;
    fn input_records(&self) -> u64;
    fn output_records(&self) -> u64;
    fn total_input_bytes(&self) -> u64;
    fn total_output_bytes(&self) -> u64;
    fn jet_bundle_reason(&self) -> Self::CompactionReason;
}
