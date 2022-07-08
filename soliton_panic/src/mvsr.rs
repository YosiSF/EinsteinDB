// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.
// -----------------------------------------------------------------------------
//! # EinsteinDB
//! # ----------------------------------------------------------------
//!
//!    #[macro_use]
//!   extern crate lazy_static;
//!  #[macro_use]
//!  extern crate serde_derive;
//! #[macro_use]
//! extern crate serde_json;
//! #[macro_use]
//! extern crate serde_json_utils;
//! #[macro_use]
//! extern crate log;
//! #[macro_use]
//! extern crate serde_json_utils;
//! #[macro_use]
//! extern crate serde_json_utils;
//! #[macro_use]
//! extern crate serde_json_utils;
//! #[macro_use]


// -----------------------------------------------------------------------------
//! # EinsteinDB
//!
//! This is a Rust implementation of the [EinsteinDB](https://einsteindb.com)
//! database.
//!
//! ##############################################################################
//! ##############################################################################
//!
//! ## Introduction
//!
//! The EinsteinDB is a distributed database that is designed to be fast and_then
//! scalable.
//!
//! ##############################################################################
//! ##############################################################################
//!
//! ## Features
//!
//! * Fast:
//! * modular: the database is designed to be modular and can be used in different
//! applications.
//! * scalable: the database is designed to be scalable.
//!
//! ##############################################################################
//!
//!
//!




use std::fmt;
use std::hash::Hash;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Ref;
use std::ops::{
    Add,
    AddAssign,
    Sub,
    SubAssign,
    Mul,
    MulAssign,
    Div,
    DivAssign,
    Deref,
    DerefMut,
    Index,
    IndexMut,
};

//raft
//use std::collections::HashMap;
//use std::collections::BTreeMap;
//use std::collections::BTreeSet;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::{cmp, u64};
use fdb_traits::Result;

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


use einstein_db_alexandrov_processing::{
    index::{
        Index,
        IndexIterator,
        IndexIteratorOptions,
        IndexIteratorOptionsBuilder,
    },
};



use einstein_db_alexandrov_processing::{
    index::{
        Index,
        IndexIterator,
        IndexIteratorOptions,
        IndexIteratorOptionsBuilder,
    },
};

use einstein_ml::{
    index::{
        Index,
        IndexIterator,
        IndexIteratorOptions,
        IndexIteratorOptionsBuilder,
    },
};

use berolina_sql::{
    index::{
        Index,
        IndexIterator,
        IndexIteratorOptions,
        IndexIteratorOptionsBuilder,
    },
};

use causetq::{
    index::{
        Index,
        IndexIterator,
        IndexIteratorOptions,
        IndexIteratorOptionsBuilder,
    },
};

use itertools::Itertools;


use super::*;



/// ##############################################################################
///  MVSR is a concurrency consistency check and recovery system.
/// #############################################################################
///
///
///


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct MVSR {
    pub id: u64,
    pub version: u64,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct MVSR_OP {
    pub id: u64,
    pub version: u64,
    pub value: Value,
    pub op: u8,
}




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
    ttl.saturating_add(ttl_current_ts())
}


pub fn ttl_expired(ttl: u64) -> bool {
    ttl_current_ts() >= ttl_expire_ts(ttl)
}


