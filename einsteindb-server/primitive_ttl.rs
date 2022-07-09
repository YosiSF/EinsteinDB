// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::error::{Error, Result};
use crate::parser::{Parser, ParserError};
use crate::value::{Value, ValueType};
use crate::{ValueRef, ValueRefMut};
use itertools::Itertools;
use crate::fdb_traits::FdbTrait;
use crate::fdb_traits::FdbTraitImpl;
use pretty;
use std::{
    collections::HashMap,
    fmt::{self, Display},
    io,
    convert::{TryFrom, TryInto},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};
use crate::fdb_traits::FdbTrait;
use crate::fdb_traits::FdbTraitImpl;


#[derive(Debug, Default)]
pub struct TtlGreedoids {
    pub max_expire_ts: u64,
    pub min_expire_ts: u64,
}




pub trait TtlGreedoidsExt {
    fn get_range_ttl_greedoids_namespaced(
        &self,
        namespaced: &str,
        start_soliton_id: &[u8],
        end_soliton_id: &[u8],
    ) -> Result<Vec<(String, TtlGreedoids)>>;
}





#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PrimitiveTtl {
    pub name: String,
    pub value: Value,
    pub ttl: i64,
}


impl PrimitiveTtl {
    pub fn new(name: String, value: Value, ttl: i64) -> Self {
        PrimitiveTtl {
            name,
            value,
            ttl,
        }
    }
}


impl Display for PrimitiveTtl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}


impl Deref for PrimitiveTtl {
    type Target = Value;

    fn deref(&self) -> &Self::Target {
        &self.value
    }
}




impl Display for PrimitiveTtl {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", pretty::render(&self))
    }
}


impl DerefMut for PrimitiveTtl {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.value
    }
}






/// # PrimitiveTtl
///  - name: String
/// - value: Value
/// - ttl: i64
/// - ttl_type: String
/// - ttl_unit: String
/// - ttl_value: i64
/// - ttl_unit_value: i64
///
///
/// ## Examples
/// ```
///

///ttl for primitive type in EinsteinDB
/// time to live for primitive type in EinsteinDB

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


pub fn ttl_expire_time_str(ttl: u64) -> String {
    ttl_expire_time(ttl).map_or("".to_owned(), |expire_time| {
        let expire_time = expire_time as i64;
        if expire_time < 0 {
            "expired".to_owned()
        } else {
            format!("{}s", expire_time)
        }
    })
}

//add relativistic time
pub fn ttl_expire_time_str_relativistic(ttl: u64) -> String {
    ttl_expire_time(ttl).map_or("".to_owned(), |expire_time| {
        let expire_time = expire_time as i64;
        if expire_time < 0 {
            "expired".to_owned()
        } else {
            format!("{}s", expire_time)
        }
    })
}

//add certificate expire time
pub fn ttl_expire_time_str_cert(ttl: u64) -> String {
    ttl_expire_time(ttl).map_or("".to_owned(), |expire_time| {
        let expire_time = expire_time as i64;
        if expire_time < 0 {
            "expired".to_owned()
        } else {
            format!("{}s", expire_time)
        }
    })
}
