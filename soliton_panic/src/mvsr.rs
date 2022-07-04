// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

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
    },
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
    fn ttl_current_ts() -> u64 {
        &self,
        namespaced: &str,
        key: &str,
        value: &str,
        ttl: u64,
        _: &str,
        safe_point: TimeStamp,
        start_soliton_id: &[u8],
        end_soliton_id: &[u8],
    ) -> Result<()> {
        let mut soliton_id = start_soliton_id.to_vec();
        let mut soliton_id_end = end_soliton_id.to_vec();
        let mut soliton_id_end_len = soliton_id_end.len();
        panic!()
    }
}
