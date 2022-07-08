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
///
///



pub const SLOW_QUORUM: usize = 3; // F + 1
pub const FAST_QUORUM: usize = 3; // F + floor(F + 1 / 2)
pub const REPLICAS_NUM: usize = 5;
pub const LOCALHOST: &str = "127.0.0.1";
pub const VA: &str = "52.23.98.238";
pub const NORCA: &str = "52.53.140.242";
pub const OR: &str = "54.68.85.53";
pub const JP: &str = "18.176.188.121";
pub const EU: &str = "108.128.186.5";
pub const REPLICA_PORT: u16 = 10000;
pub static REPLICA_ADDRESSES: [&str; REPLICAS_NUM] = [VA, JP, NORCA, OR, EU];
pub static REPLICA_PORTS: [u16; REPLICAS_NUM] = [REPLICA_PORT, REPLICA_PORT, REPLICA_PORT, REPLICA_PORT, REPLICA_PORT];

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MVRSInfo {
    pub id: String,
    pub version: u64,
    pub data: Value,
    pub replicas: Vec<String>,
    pub replicas_ports: Vec<u16>,
    pub replicas_addresses: Vec<&'static str>,
    pub replicas_versions: Vec<u64>,
    pub replicas_data: Vec<Value>,
    pub replicas_status: Vec<MVSRStatus>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MVSRStatus {
    pub id: String,
    pub version: u64,
    pub data: Value,
    pub status: MVSRStatusType,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct VioletaBftLogic {
    pub id: String,
    pub version: u64,
    pub data: Value,
    pub replicas: Vec<String>,
    pub replicas_ports: Vec<u16>,

    pub replicas_addresses: Vec<&'static str>,
    pub replicas_versions: Vec<u64>,
    pub replicas_data: Vec<Value>,

    pub replicas_status: Vec<MVSRStatus>,
    pub replicas_status_history: Vec<MVSRStatus>,
    pub replicas_status_history_versions: Vec<u64>,

    pub replicas_status_history_versions_map: HashMap<u64, usize>,



}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct MVSR {
    pub id: u64,
    pub version: u64,
    pub value: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct MvsrOp {
    pub id: u64,
    pub version: u64,
    pub value: Value,
    pub op: u8,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct MvsrOpResult {
    pub id: u64,
    pub version: u64,
    pub value: Value,
    pub op: u8,
    pub result: u8,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct MvsrOpResultWithError {
    pub id: u64,
    pub version: u64,
    pub value: Value,
    pub op: u8,
    pub result: u8,
    pub error: u8,
}

impl VioletaBftLogic {

    pub fn load_from_file(path: &str) -> Result<VioletaBftLogic> {
        let mut file = File::open(path)?;
        let mut contents = String::new();
        file.read_to_string(&mut contents)?;
        let vbft_logic: VioletaBftLogic = serde_json::from_str(&contents)?;
        Ok(vbft_logic)
    }

    pub fn save_to_file(&self, path: &str) -> Result<()> {
        let mut file = File::create(path)?;
        let contents = serde_json::to_string_pretty(self)?;
        file.write_all(contents.as_bytes())?;
        Ok(())
    }


    pub fn new(id: String, version: u64, data: Value, replicas: Vec<String>, replicas_ports: Vec<u16>) -> VioletaBftLogic {
        VioletaBftLogic {
            id,
            version,
            data,
            replicas,
            replicas_ports,
            replicas_addresses: Vec::new(),
            replicas_versions: Vec::new(),
            replicas_data: Vec::new(),
            replicas_status: Vec::new(),
            replicas_status_history: Vec::new(),
            replicas_status_history_versions: Vec::new(),
            replicas_status_history_versions_map: HashMap::new(),
        }
    }

    pub fn init(&mut self) {
        self.replicas_addresses = REPLICA_ADDRESSES.iter().cloned().collect();
        self.replicas_versions = vec![0; self.replicas.len()];
        self.replicas_data = vec![Value::Null; self.replicas.len()];
        self.replicas_status = vec![MVSRStatus {
            id: self.id.clone(),
            version: self.version,
            data: self.data.clone(),
            status: MVSRStatusType::Pending,
        }; self.replicas.len()];
        self.replicas_status_history = vec![MVSRStatus {
            id: self.id.clone(),
            version: self.version,
            data: self.data.clone(),
            status: MVSRStatusType::Pending,
        }; self.replicas.len()];
        self.replicas_status_history_versions = vec![0; self.replicas.len()];
        self.replicas_status_history_versions_map = HashMap::new();
    }

    pub fn get_replica_address(&self, replica_id: &str) -> Option<&'static str> {
        self.replicas_addresses.iter().position(|&x| x == replica_id).map(|i| self.replicas_addresses[i])
    }

    pub fn get_replica_port(&self, replica_id: &str) -> Option<u16> {
        self.replicas_ports.iter().position(|&x| x == replica_id).map(|i| self.replicas_ports[i])
    }

    pub fn get_replica_version(&self, replica_id: &str) -> Option<u64> {
        self.replicas_versions.iter().position(|&x| x == replica_id).map(|i| self.replicas_versions[i])
    }

    pub fn get_replica_data(&self, replica_id: &str) -> Option<Value> {
        self.replicas_data.iter().position(|&x| x == replica_id).map(|i| self.replicas_data[i].clone())
    }

    pub fn get_replica_status(&self, replica_id: &str) -> Option<MVSRStatus> {
        self.replicas_status.iter().position(|&x| x.id == replica_id).map(|i| self.replicas_status[i].clone())
    }

    pub fn get_replica_status_history(&self, replica_id: &str) -> Option<MVSRStatus> {
        self.replicas_status_history.iter().position(|&x| x.id == replica_id).map(|i| self.replicas_status_history[i].clone())
    }


    pub fn get_replica_status_history_version(&self, replica_id: &str, version: u64) -> Option<usize> {
        self.replicas_status_history_versions.iter().position(|&x| x == version).map(|i| self.replicas_status_history_versions[i])
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
#[repr(C)]
pub struct MvsrOpResultWithErrorWithValue {
    pub id: u64,
    pub version: u64,
    pub value: Value,
    pub op: u8,
    pub result: u8,
    pub error: u8,
    pub value_error: u8,
}

use protobuf::{
    Message,
    ProtobufEnum,
};


use std::{
    fmt,
    iter,
    str,
    string,
    vec,
    collections::{
        BTreeMap,
        BTreeSet,
        BinaryHeap,
        HashMap,
        HashSet,
    },
    cmp,
    hash,
    mem,
    ops,
    ptr,
    slice,
    sync::{
        atomic,
        mpsc,
        Arc,
        RwLock,
        Mutex,
    },
    thread,
    time,
    convert::TryFrom,
    convert::TryInto,
    fmt::{
        Debug,
        Display,
        Formatter,
    },
    marker::PhantomData,
    rc::Rc,
    cell::RefCell,
    collections::HashMap,
    collections::HashSet,
    collections::BTreeMap,
    collections::BTreeSet,
    collections::VecDeque,
    collections::HashMap,
    collections::HashSet,
    collections::BTreeMap,
    collections::BTreeSet,
    collections::VecDeque,
    collections::HashMap,
    collections::HashSet,
    collections::BTreeMap,
    collections::BTreeSet,
    collections::VecDeque,
    collections::HashMap,
    collections::HashSet,
    collections::BTreeMap,
    collections::BTreeSet,
    collections::VecDeque,
    collections::HashMap,
    collections::HashSet,
    collections::BTreeMap,
    collections::BTreeSet,
    collections::VecDeque,
    collections::HashMap,
    collections::HashSet,
    collections::BTreeMap,
    collections::BTreeSet,
    collections::VecDeque,
    collections::HashMap,
    collections::HashSet,
    collections::BTreeMap,
    collections::BTreeSet,
    collections::VecDeque,
    collections::HashMap,
    collections::HashSet,
    collections::BTreeMap,
    collections::BTreeSet,
    collections::VecDeque,
    collections::FdbTraitImpl
};




use protobuf::Message as Message_implements;
use protobuf::MessageStatic as MessageStatic_implements;
use protobuf::ProtobufEnum as ProtobufEnum_implements;
use protobuf::ProtobufEnumStatic as ProtobufEnumStatic_implements;
use protobuf::ProtobufError as ProtobufError_implements;



const MVSR_OP_INSERT: u8 = 0;
const MVSR_OP_DELETE: u8 = 1;
const MVSR_OP_UPDATE: u8 = 2;
const MVSR_OP_READ: u8 = 3;

const _PROTOBUF_VERSION_CHECK: () = match protobuf::version() {
    Ok(version) => {
        if version < "3.0.0" {
            panic!("protobuf version too old");
        }
    },
    Err(err) => panic!("protobuf version error: {}", err),
};



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




