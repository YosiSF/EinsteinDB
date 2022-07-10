// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
////////////////////////////////


use std::{
    fmt,
    iter,
    str,
    string,
    vec,
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
    collections::{
        hash_map,
        hash_set,
        BTreeMap,
        BTreeSet,
        BinaryHeap,
        LinkedList,
        VecDeque,
    },
    error::Error as StdError,
    error::ErrorKind as StdErrorKind,
    result::Result as StdResult,
    result::ResultExt as StdResultExt,
    iter::FromIterator,
    option::Option,
    option::Option::Some,
    option::Option::None,
    marker::PhantomData,
    str::FromStr,
    str::FromUtf8Error,
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


use std::sync::{
    atomic::{
        AtomicBool,
        AtomicUsize,
        Ordering::{
            Acquire,
            Relaxed,
            Release,
            SeqCst,
        },
    },
    mpsc::{
        channel,
        Sender,
        Receiver,
        TryRecvError,
    },
    Arc as SyncArc,
    RwLock as SyncRwLock,
    Mutex as SyncMutex,
};


use protobuf::Message as Message_implements;
use protobuf::MessageStatic as MessageStatic_implements;
use protobuf::ProtobufEnum as ProtobufEnum_implements;
use protobuf::ProtobufEnumStatic as ProtobufEnumStatic_implements;
use protobuf::ProtobufError as ProtobufError_implements;
use protobuf::ProtobufErrorStatic as ProtobufErrorStatic_implements;
use protobuf::ProtobufResult as ProtobufResult_implements;


/*
use protobuf::{
    parse_from_bytes,
    Message,
    RepeatedField,
    SingularField,
    ProtobufEnum,
};

use std::error::Error;

use std::io;
use std::string::FromUtf8Error;
use std::str::Utf8Error;


use crate::berolinasql::{Error as BerolinaSqlError, ErrorKind as BerolinaSqlErrorKind};
use crate::berolinasql::{ErrorImpl as BerolinaSqlErrorImpl};


#[derive(Debug)]
pub enum ErrorKind {
    Io(io::Error),
    BerolinaSql(BerolinaSqlError),
    Utf8(Utf8Error),
    FromUtf8(FromUtf8Error),
    Other(String),
}


#[derive(Debug)]
pub struct ErrorImpl {
    pub kind: ErrorKind,

}


#[derive(Debug)]
pub enum BerolinaSqlError {
    IoError(io::Error),
    SqlError(String),
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


    const MVSR_OP_INSERT: u8 = 0;
    const MVSR_OP_DELETE: u8 = 1;
    const MVSR_OP_UPDATE: u8 = 2;
    const MVSR_OP_READ: u8 = 3;*/


const MVSR_OP_INSERT: u8 = 0;
const MVSR_OP_DELETE: u8 = 1;
const MVSR_OP_UPDATE: u8 = 2;
const MVSR_OP_READ: u8 = 3;

const MVSR_RESULT_OK: u8 = 0;
const MVSR_RESULT_ERROR: u8 = 1;
const MVSR_RESULT_NOT_FOUND: u8 = 2;
const MVSR_RESULT_NOT_EXIST: u8 = 3;
const MVSR_RESULT_EXIST: u8 = 4;
const MVSR_RESULT_NOT_EMPTY: u8 = 5;
const MVSR_RESULT_EMPTY: u8 = 6;
const MVSR_RESULT_NOT_LOCKED: u8 = 7;
const MVSR_RESULT_LOCKED: u8 = 8;
const MVSR_RESULT_NOT_UNLOCKED: u8 = 9;
const MVSR_RESULT_UNLOCKED: u8 = 10;


const MVSR_VALUE_ERROR_OK: u8 = 0;
const MVSR_VALUE_ERROR_NOT_FOUND: u8 = 1;
const MVSR_VALUE_ERROR_NOT_EXIST: u8 = 2;
const MVSR_VALUE_ERROR_EXIST: u8 = 3;
const MVSR_VALUE_ERROR_NOT_EMPTY: u8 = 4;




    ////////////////////////////////////////////////////////////////
    ///protobuf::{BuildHasher, hash, HashMap, HashSet, Message, ProtobufEnum, ProtobufError, ProtobufErrorKind, ProtobufResult, ProtobufValue, ProtobufWrapper},
    /// std::{
    ///    fmt,
    ///   iter,
    ///  str,
    /// string,
    /// vec,
    /// cmp,
    /// hash,
    /// mem,
    ///
    ///
    /// ops,
    /// ptr,
    ///
    ///
    ///
    ///
 ///   use protobuf::ProtobufEnum as ProtobufEnum_implements;
  ///  use protobuf::ProtobufEnumStatic as ProtobufEnumStatic_implements;
 ////   use protobuf::ProtobufError as ProtobufError_implements;


/*
    const MVSR_OP_INSERT: u8 = 0;
    const MVSR_OP_DELETE: u8 = 1;
    const MVSR_OP_UPDATE: u8 = 2;
    const MVSR_OP_READ: u8 = 3;

 */
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




/// This macro is used to implement the `ProtobufEnum` trait for enums that
/// represent a field in a protocol buffer message.
/// The `$name` parameter is the name of the enum, and `$value` is the value
/// of the field in the protocol buffer message.
/// The `$variant` parameter is the name of the variant of the enum.
/// The `$variant_value` parameter is the value of the variant.
/// The `$variant_name` parameter is the name of the variant.
/// The `$variant_index` parameter is the index of the variant.
/// The `$variant_count` parameter is the number of variants in the enum.
/// The `$variant_count_minus_one` parameter is the number of variants in the enum minus one.
/// The `$variant_count_minus_two` parameter is the number of variants in the enum minus two.
/// The `$variant_count_minus_three` parameter is the number of variants in the enum minus three.
/// The `$variant_count_minus_four` parameter is the number of variants in the enum minus four.
    #[macro_use]
    einsteindb::lazy_static! {
        pub static ref EINSTEINDB_VERSION: String = {
            let mut version = "0.0.0".to_string();
            if let Ok(mut file) = std::fs::File::open("VERSION") {
                if let Ok(mut contents) = std::io::BufReader::new(file).lines().next() {
                    if let Some(line) = contents {
                        version = line;
                    }
                }
            }
            version
        };
    }

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

    pub id: String,
    pub version: u64,
}

///! ##############################################################################
/// ## MVSR
/// MVSR is a concurrency consistency check and recovery system.
///
/// #############################################################################

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MVSR {
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
pub struct MVSRStatusType {
    pub id: String,
    pub version: u64,
    pub data: Value,
    pub status: MVSRStatusTypeType,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MVSRStatusTypeType {
    OK,
    FAIL,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MVSRStatusTypeTypeOK {
    pub id: String,
    pub version: u64,
    pub data: Value,
    pub status: MVSRStatusTypeType,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MVSRStatusTypeTypeFail {
    pub id: String,
    pub version: u64,
    pub data: Value,
    pub status: MVSRStatusTypeType,
}





pub struct mvsr {
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
    pub fn load_from_file(path: &str) -> soliton_panic::mvsr::VioletaBftLogic {
        std::fs::File::open(path).map_err(|e| format!("{}", e))?;
        let _ = String::new();


        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        struct VioletaBftLogic {
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

        while let Some(line) = file.read_line()? {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let mut parts = line.split_whitespace();
            parts.next().unwrap();
        }

        Ok(VioletaBftLogic {
            id: String::new(),
            version: 0,
            data: Value::Null,
            replicas: Vec::new(),
            replicas_ports: Vec::new(),
            replicas_addresses: Vec::new(),
            replicas_versions: Vec::new(),
            replicas_data: Vec::new(),
            replicas_status: Vec::new(),
            replicas_status_history: Vec::new(),
            replicas_status_history_versions: Vec::new(),
            replicas_status_history_versions_map: HashMap::new(),
        }).expect("TODO: panic message")
    }

    pub fn load_from_file_with_id(path: &str, id: &str) -> soliton_panic::mvsr::VioletaBftLogic {
        std::fs::File::open(path).map_err(|e| format!("{}", e))?;
        let _ = String::new();

        #[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
        struct VioletaBftLogic {
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

        while let Some(line) = file.read_line()? {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let mut parts = line.split_whitespace();
            parts.next().unwrap();
        }

        Ok(VioletaBftLogic {
            id: String::new(),
            version: 0,
            data: Value::Null,
            replicas: Vec::new(),
            replicas_ports: Vec::new(),
            replicas_addresses: Vec::new(),
            replicas_versions: Vec::new(),
            replicas_data: Vec::new(),
            replicas_status: Vec::new(),
            replicas_status_history: Vec::new(),
            replicas_status_history_versions: Vec::new(),
            replicas_status_history_versions_map: HashMap::new(),
        }).expect("TODO: panic message")

    }
}


impl VioletaBftLogic {

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
        self.replicas_ports.iter().position(|&x| x == replica_id.parse().unwrap()).map(|i| self.replicas_ports[i])
    }

    pub fn get_replica_version(&self, replica_id: &str) -> Option<u64> {
        self.replicas_versions.iter().position(|&x| x == replica_id.parse().unwrap()).map(|i| self.replicas_versions[i])
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


    pub fn get_replica_status_history_version(&self, replica_id: &str, version: u64) -> Option<u64> {
        self.replicas_status_history_versions.iter().position(|&x| x == version).map(|i| self.replicas_status_history_versions[i])
    }
}



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


pub fn ttl_expire_ts_str(ttl: u64) -> String {
    if ttl == 0 {
        "never".to_string()
    } else {
        format!("{}", ttl_expire_ts(ttl))
    }
}

///tls config
pub fn tls_config() -> TlsConfig {
    let mut config = TlsConfig::new();
    config.root_store.add_server_trust_anchors(&webpki_roots::TLS_SERVER_ROOTS);
    config
}


pub fn tls_config_with_ca(ca_path: &str) -> TlsConfig {
    let mut config = tls_config();
    config.root_store.add_pem_file(&ca_path).unwrap();
    config
}


pub fn tls_config_with_ca_and_key(ca_path: &str, key_path: &str) -> TlsConfig {
    let mut config = tls_config_with_ca(ca_path);
    config.identity.add_pem_file(&key_path).unwrap();
    config
}

///! tls config with ca and key and certificate
/// # Examples
/// ```
/// use tls_config_with_ca_and_key_and_cert;
/// let config = tls_config_with_ca_and_key_and_cert("ca.pem", "key.pem", "cert.pem");
/// ```
/// # Panics
/// Panics if the certificate file is not found.
/// Panics if the key file is not found.
/// Panics if the CA file is not found.
/// Panics if the certificate file is not a valid PEM file.
/// Panics if the key file is not a valid PEM file.
///
/// # Returns
/// The tls config.
/// # Panics
/// Panics if the CA file is not a valid PEM file.

