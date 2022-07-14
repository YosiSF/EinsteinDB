extern crate core;
extern crate core;

use core::num::flt2dec::decoder;
/// Copyright (c) 2022 Whtcorps Inc and EinsteinDB Project contributors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///    http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
///See the License for the specific language governing permissions and
///limitations under the License.
///
/// # About
///
/// This is a library for the [EinsteinDB](https://einsteindb.com
/// "EinsteinDB: A Scalable, High-Performance, Distributed Database")
/// framework.
/// It is a Rust implementation of the [EinsteinDB](https://einsteindb.com
/// "EinsteinDB: A Scalable, High-Performance, Distributed Database")
///

//macro for unused variables
#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_imports)]
#[allow(dead_code)]
#[allow(unused_must_use)]
#[warn(unused_extern_crates)]
#[warn(unused_import_braces)]
#[warn(unused_qualifications)]

#[warn(unused_variables)]


use std::io;
use std::io::prelude::*;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::io::Read;
use std::io::BufRead;
use std::sync::{Arc, Mutex};

///constants for the library
pub const EINSTEIN_DB_VERSION: u32 = 0x0101;

pub const EINSTEIN_DB_VERSION_STR: &str = "0.1.1";
pub const EINSTEIN_ML_VERSION: u32 = 0x0101;
pub const EINSTEIN_DB_VERSION_STR_LEN: usize = 16;
pub const EINSTEIN_DB_VERSION_STR_LEN_MAX: usize = 16;
pub const EINSTEIN_DB_VERSION_STR_LEN_MIN: usize = 16;



#[derive(Debug)]
pub enum Error {
    /// An error originating from the client library itself.
    /// This is a bug in the library itself and should not occur.
    InternalError(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError2(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError3(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError4(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError5(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError6(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError7(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError8(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError9(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError10(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError11(String),

}


impl Error {
    /// Creates a new `Error` instance.
    /// # Examples
    /// ```
    /// use einsteindb::causetq::sync::new_sync;
    /// use einsteindb::causetq::sync::Sync;
    /// use std::sync::Arc;
    /// use std::sync::Mutex;
    /// let poset = new_sync();
    /// let sync = Sync::new(poset);
    /// ```

    pub fn new() -> Error {
        Error::InternalError(String::new())
    }
}


/// The error type for EinsteinDB.
/// All errors returned from the EinsteinDB library are of this type.
/// This is a catch-all type for errors that are not specific to any
/// particular operation.
///! # Examples
// ```
// !use einsteindb::causetq::sync::new_sync;
// use einsteindb::causetq::sync::Sync;
// use std::sync::Arc;
//
// let poset = new_sync();
// let sync = Sync::new(poset);
//
// ```
// # Examples
// ```
// use einsteindb::causetq::sync::new_sync;
// use einsteindb::causetq::sync::Sync;
// use std::sync::Arc;
//
// let poset = new_sync();
// let sync = Sync::new(poset);
//
// ```
// # Examples
// ```
// use einsteindb::causetq::sync::new_sync;
// use einsteindb::causetq::sync::Sync;
// use std::sync::Arc;
//
//
// let poset = new_sync();
// let sync = Sync::new(poset);
//
//






//  use std::sync::{Arc, Mutex};
// use std::sync::atomic::{AtomicBool, Partitioning};
// // use std::thread;
// // use std::time::Duration

pub struct EinsteinDb {

    pub einstein_db_client_state: Arc<Mutex<EinsteinDbClientState>>,
    pub einstein_db_client_state_str: Arc<Mutex<String>>,


    pub version: u32,
    pub version_str: String,
    pub version_str_len: usize,

    pub einstein_db_state_str: String,
    pub einstein_ml_version: String,
    pub einstein_ml_version_str: String,
    pub einstein_db_version: String,



}


#[derive(Debug)]
pub struct EinsteinDbClientState {
    pub einstein_db_client_state: EinsteinDbClientStateType,
    pub einstein_db_client_state_str: String,
}

#[derive(Debug)]
pub enum EinsteinDbClientStateType {
    EinsteinDbClientStateTypeInit,
    EinsteinDbClientStateTypeInit2,
    EinsteinDbClientStateTypeInit3,
    EinsteinDbClientStateTypeInit4,
    EinsteinDbClientStateTypeInit5,
    EinsteinDbClientStateTypeInit6,
    EinsteinDbClientStateTypeInit7,
    EinsteinDbClientStateTypeInit8,
    EinsteinDbClientStateTypeInit9,
    EinsteinDbClientStateTypeInit10,
    EinsteinDbClientStateTypeInit11,
    EinsteinDbClientStateTypeInit12,
    EinsteinDbClientStateTypeInit13,
    EinsteinDbClientStateTypeInit14,
    EinsteinDbClientStateTypeInit15,
    EinsteinDbClientStateTypeInit16,
    EinsteinDbClientStateTypeInit17,
    EinsteinDbClientStateTypeInit18,
    EinsteinDbClientStateTypeInit19,
    EinsteinDbClientStateTypeInit20,
    EinsteinDbClientStateTypeInit21,
    EinsteinDbClientStateTypeInit22,
    EinsteinDbClientStateTypeInit23,
    EinsteinDbClientStateTypeInit24,
    EinsteinDbClientStateTypeInit25,
    EinsteinDbClientStateTypeInit26,
    EinsteinDbClientStateTypeInit27,
    EinsteinDbClientStateTypeInit28,
    EinsteinDbClientStateTypeInit29,
    EinsteinDbClientStateTypeInit30,
    EinsteinDbClientStateTypeInit31,
    EinsteinDbClientStateTypeInit32,
    EinsteinDbClientStateTypeInit33,
    EinsteinDbClientStateTypeInit34,
    EinsteinDbClientStateTypeInit35,
    EinsteinDbClientStateTypeInit36,
    EinsteinDbClientStateTypeInit37,
    EinsteinDbClientStateTypeInit38,
    EinsteinDbClientStateTypeInit39,
    EinsteinDbClientStateTypeInit40,
    EinsteinDbClientStateTypeInit41,
    EinsteinDbClientStateTypeInit42,
    EinsteinDbClientStateTypeInit43,
    EinsteinDbClientStateTypeInit44,
    EinsteinDbClientStateTypeInit45,
    EinsteinDbClientStateTypeInit46,
    EinsteinDbClientStateTypeInit47,
    EinsteinDbClientStateTypeInit48,
    EinsteinDbClientStateTypeInit49,
    EinsteinDbClientStateTypeInit50,
}


#[derive(Debug)]
pub struct EinsteinDbClientStateInit {
    pub einstein_db_client_state: EinsteinDbClientStateType,
    pub einstein_db_client_state_str: String,
}


impl EinsteinDb {


    pub fn new() -> EinsteinDb {
        EinsteinDb {
            einstein_db_client_state: Arc::new(Mutex::new(EinsteinDbClientState::new())),
            einstein_db_client_state_str: Arc::new(Mutex::new(String::new())),
            version: 0,
            version_str: String::new(),
            version_str_len: 0,
            einstein_db_state_str: String::new(),
            einstein_ml_version: String::new(),
            einstein_ml_version_str: String::new(),
            einstein_db_version: String::new(),
        }
    }



    pub fn get_einstein_db_client_state(&self) -> Arc<Mutex<EinsteinDbClientState>> {
        return self.einstein_db_client_state.clone();
    }

    pub fn get_einstein_db_client_state_str(&self) -> Arc<Mutex<String>> {
        return self.einstein_db_client_state_str.clone();
    }

    pub fn get_einstein_db_client_state_str_len(&self) -> usize {
        return self.einstein_db_client_state_str.lock().unwrap().len();
    }
}



pub struct EinsteinDbClient {
    pub einstein_db_client_state_str: String,
    pub einstein_db_client_state_str_len: usize,
    pub einstein_db_client_state: String,
    pub einstein_db_client_state_len: usize,


}




impl EinsteinDbClient {
    pub fn new() -> EinsteinDbClient {
        EinsteinDbClient {
            einstein_db_client_state_str: String::new(),
            einstein_db_client_state_str_len: 0,
            einstein_db_client_state: String::new(),
            einstein_db_client_state_len: 0,
        }
    }

    pub fn get_einstein_db_client_state_str(&mut self) -> io::Result<()> {
        Ok(())
    }
}


pub struct EinsteinDbServer {
    pub einstein_db_server_state_str: String,
    pub einstein_db_server_state_str_len: usize,
    pub einstein_db_server_state: String,
    pub einstein_db_server_state_len: usize,


}


impl EinsteinDbServer {
    pub fn new() -> EinsteinDbServer {
        EinsteinDbServer {
            einstein_db_server_state_str: String::new(),
            einstein_db_server_state_str_len: 0,
            einstein_db_server_state: String::new(),
            einstein_db_server_state_len: 0,
        }
    }

    pub fn get_einstein_db_server_state_str(&mut self) -> io::Result<()> {
        Ok(())
    }
}





impl EinsteinDbServer {
    pub fn get_einstein_db_server_state_str(&mut self) -> io::Result<()> {
        Ok(())
    }

}


impl EinsteinDbClient {
    pub fn get_einstein_db_client_state_str() -> String {
        let mut einstein_db_client_state_str = String::new();
        let mut einstein_db_client_state_str_len = 0;
        let mut einstein_db_client_state = String::new();
        let mut einstein_db_client_state_len = 0;

        for i in 0..einstein_db_client_state_str_len {
            einstein_db_client_state_str.push(einstein_db_client_state.chars().nth(i).unwrap());
        }

        for i in 0..einstein_db_client_state_len {
            einstein_db_client_state.push(einstein_db_client_state_str.chars().nth(i).unwrap());
        }

        einstein_db_client_state
    }
}




impl EinsteinDbServer {
    pub fn get_einstein_db_server_state_str() -> String {
        let mut einstein_db_server_state_str = String::new();
        let mut einstein_db_server_state_str_len = 0;
        let mut einstein_db_server_state = String::new();
        let mut einstein_db_server_state_len = 0;

        for i in 0..einstein_db_server_state_str_len {
            einstein_db_server_state_str.push(einstein_db_server_state.chars().nth(i).unwrap());
        }

        for i in 0..einstein_db_server_state_len {
            einstein_db_server_state.push(einstein_db_server_state_str.chars().nth(i).unwrap());
        }

        einstein_db_server_state
    }
}


impl EinsteinDbServer {
    pub fn get_einstein_db_server_state_str_len() -> usize {
        let mut einstein_db_server_state_str = String::new();
        let mut einstein_db_server_state_str_len = 0;
        let mut einstein_db_server_state = String::new();
        let mut einstein_db_server_state_len = 0;

        for i in 0..einstein_db_server_state_str_len {
            einstein_db_server_state_str.push(einstein_db_server_state.chars().nth(i).unwrap());
        }

        for i in 0..einstein_db_server_state_len {
            einstein_db_server_state.push(einstein_db_server_state_str.chars().nth(i).unwrap());
        }

        einstein_db_server_state_str_len
    }
}


impl EinsteinDbServer {
    pub fn get_einstein_db_server_state_len() -> usize {
        let mut einstein_db_server_state_str = String::new();
        let mut einstein_db_server_state_str_len = 0;
        let mut einstein_db_server_state = String::new();
        let mut einstein_db_server_state_len = 0;

        for i in 0..einstein_db_server_state_str_len {
            einstein_db_server_state_str.push(einstein_db_server_state.chars().nth(i).unwrap());
        }

        for i in 0..einstein_db_server_state_len {
            einstein_db_server_state.push(einstein_db_server_state_str.chars().nth(i).unwrap());
        }

        einstein_db_server_state_len
    }
}


#[derive(Serialize, Deserialize)]
pub struct AppendLog {
    pub log: String,
    pub log_len: usize,
}


impl AppendLog {
    pub fn new() -> AppendLog {
        AppendLog {
            log: String::new(),
            log_len: 0,
        }
    }

    pub fn get_log(&mut self) -> io::Result<()> {
        Ok(())
    }
}


#[derive(Serialize, Deserialize)]
pub struct AppendEntries {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub entries: Vec<String>,
    pub entries_len: usize,
    pub leader_commit: u64,
}


impl AppendEntries {
    pub fn new() -> AppendEntries {
        AppendEntries {
            term: 0,
            leader_id: 0,
            prev_log_index: 0,
            prev_log_term: 0,
            entries: Vec::new(),
            entries_len: 0,
            leader_commit: 0,
        }
    }

    pub fn get_term(&mut self) -> io::Result<()> {
        Ok(())
    }
}



//import serde
use serde::{Serialize, Deserialize};
use serde_json::{json, Value};
use serde_json::Error;
use serde_json::Result;
use serde_json::Value as JsonValue;
use serde_json::Map as JsonMap;
use serde_json::Number as JsonNumber;
use serde_json::String as JsonString;
use serde_json::Array as JsonArray;
use serde_json::Deserializer;
use serde_json::Serializer;
use serde_json::de::Deserializer as JsonDeserializer;
use serde_json::ser::Serializer as JsonSerializer;


//import einstein_db
use einstein_db::*;
use einstein_db::Error as ErrorImpl;
use einstein_db::ErrorKind as ErrorKindImpl;
use einstein_db::ErrorKind::*;
use einstein_db::ErrorKind::Poset as PosetErrorKind;
use einstein_db::ErrorKind::Poset as PosetErrorKind;

use soliton::*;
use berolinasql::*;
use berolinasql::Error as BerolinASQLError;
use berolinasql::ErrorKind as BerolinASQLErrorKind;
use causal_set::CausalSet;

mod causal_set;
mod config;

mod encoder;
mod event_slice;



/// # About
///     This is a library for the [EinsteinDB](https://einsteindb.com





/// A `Sync` implementation for `AllegroPoset`.
/// This implementation is thread-safe.
/// # Examples
/// ```
/// use einsteindb::causetq::sync::new_sync;
/// use einsteindb::causetq::sync::Sync;
/// use std::sync::Arc;
/// use std::sync::Mutex;
///
/// let poset = new_sync();
/// let sync = Sync::new(poset);
///
/// let mutex = Arc::new(Mutex::new(sync));
/// let mutex2 = Arc::new(Mutex::new(sync));
///
/// let mutex3 = Arc::new(Mutex::new(sync));
///
///
///
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Sync {
    pub poset: Arc<Mutex<AllegroPoset>>,
    pub config: Arc<Mutex<config::Config>>,
    pub db: Arc<Mutex<BerolinASQL>>,
    pub event_slice: Arc<Mutex<event_slice::EventSlice>>,
    pub causal_set: Arc<std::sync::Mutex<causal_set::CausalSet<T>>>,

}


impl Sync {
    /// Creates a new `Sync` instance.

    pub fn new(poset: AllegroPoset) -> Sync {
        let config = config::Config::new();
        let db = BerolinASQL::new();
        let event_slice = event_slice::EventSlice::new();
        let causal_set = causal_set::CausalSet::new();
        Sync {
            poset: Arc::new(Mutex::new(poset)),
            config: Arc::new(Mutex::new(config)),
            db: Arc::new(Mutex::new(db)),
            event_slice: Arc::new(Mutex::new(event_slice)),
            causal_set: Arc::new(Mutex::new(causal_set)),
        }
    }



/// Creates a new `Sync` instance.
///



pub fn new_with_config(poset: AllegroPoset, config: config::Config) -> Sync {
        let db = BerolinASQL::new();
        let event_slice = event_slice::EventSlice::new();
        let causal_set = causal_set::CausalSet::new();
        Sync {
            poset: Arc::new(Mutex::new(poset)),
            config: Arc::new(Mutex::new(config)),
            db: Arc::new(Mutex::new(db)),
            event_slice: Arc::new(Mutex::new(event_slice)),
            causal_set: Arc::new(Mutex::new(causal_set)),
        }
    }
}


pub enum BerolinaSqlError {
    Error(BerolinASQLError),
    ErrorKind(BerolinASQLErrorKind),
    IoError(io::Error),
    JsonError(Error),
    SqlError(String),
    SqlErrorKind(String),
}

#[derive(Debug)]
pub enum BerolinaSqlErrorType {
    Error,
    ErrorKind,
    IoError,
    SqlError,
    SqlErrorKind,
}

#[derive(Debug)]
pub struct BerolinaSqlErrorInfo {

    pub error_type: BerolinaSqlErrorType,
    pub error_msg: String,
    pub error_kind: String,
}



//
// pub fn get_einstein_db_client_state_str_len() -> usize {
//     return self.einstein_db_client_state_str.len();
//
// }
//
// pub fn get_einstein_db_client_state_str() -> String {

impl BerolinaSqlErrorInfoList {
    pub fn new() -> BerolinaSqlErrorInfoList {
        BerolinaSqlErrorInfoList {
            error_info_list: Vec::new(),
        }
    }
}
// #[derive(Deserialize, Serialize, Debug)]
// pub struct BerolinaSqlErrorInfoListSerialized {
//     pub error_info_list: Vec<BerolinaSqlErrorInfoSerialized>,
// }


// impl BerolinaSqlErrorInfoListSerialized {
//     pub fn new() -> BerolinaSqlErrorInfoListSerialized {
//         BerolinaSqlErrorInfoListSerialized {
//             error_info_list: Vec::new(),
//         }
//     }
// }



//serde::{Deserialize, Serialize};



#[derive(Debug)]
pub enum EinsteinDBError {
    IoError(io::Error),
    SqlError(String),
}


#[derive(Debug)]
pub enum EinsteinDBErrorType {
    IoError,
    SqlError,
}

/// The error type for EinsteinDB.
/// All errors returned from the EinsteinDB library are of this type.
/// This is a catch-all type for errors that are not specific to any
/// particular operation.




#[derive(Serialize, Deserialize)]
pub struct AppendEntriesResponse {
    pub term: u64,
    pub success: bool,
}


impl AppendEntriesResponse {
    pub fn new() -> AppendEntriesResponse {
        AppendEntriesResponse {
            term: 0,
            success: false,
        }
    }

    pub fn get_term(&mut self) -> io::Result<()> {
        Ok(())
    }
}


#[derive(Serialize, Deserialize)]
pub struct RequestVote {

    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}


impl RequestVote {
    pub fn new() -> RequestVote {
        RequestVote {
            term: 0,
            candidate_id: 0,
            last_log_index: 0,
            last_log_term: 0,
        }
    }

    pub fn get_term(&mut self) -> io::Result<()> {
        Ok(())
    }
}


#[derive(Serialize, Deserialize)]
pub struct RequestVoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}


impl RequestVoteResponse {
    pub fn new() -> RequestVoteResponse {
        RequestVoteResponse {
            term: 0,
            vote_granted: false,
        }
    }

    pub fn get_term(&mut self) -> io::Result<()> {
        Ok(())
    }
}

///BASE EINSTEINDB is a variation of ACID

pub trait BaseEinsteinDb {
    fn append_log(&mut self, log: String) -> io::Result<()>;
    fn append_entries(&mut self, entries: Vec<String>) -> io::Result<()>;
    fn request_vote(&mut self, vote: RequestVote) -> io::Result<()>;
    fn request_vote_response(&mut self, vote_response: RequestVoteResponse) -> io::Result<()>;
    fn append_entries_response(&mut self, append_entries_response: AppendEntriesResponse) -> io::Result<()>;
}


///Unlike ACID, this section of the library will not have a separate log and state.
/// Instead, the log and state will be stored in the same file.
/// This is because the log and state will be the same for all nodes in the cluster.
///
/// The log will be stored in the following format:
///
/// log_entry_1
/// log_entry_2
/// ...
/// log_entry_n
///
/// we need to compress, encrypt, and hash the log entries.
/// This is a simple way to do it.
///
/// The state will be stored in the following format:
///
/// state_entry_1
/// state_entry_2
/// ...
/// state_entry_n
///
/// then we multiplex the state entries into the log entries.
/// This is a simple way to do it.


pub struct BaseEinsteinDbServer {
    pub log: String,
    pub state: String,
    pub log_len: usize,
    pub state_len: usize,
}


impl BaseEinsteinDbServer {
    pub fn new() -> BaseEinsteinDbServer {
        BaseEinsteinDbServer {
            log: String::new(),
            state: String::new(),
            log_len: 0,
            state_len: 0,
        }
    }

    pub fn get_log_len(&mut self) -> usize {
        self.log_len
    }

    pub fn get_state_len(&mut self) -> usize {
        self.state_len
    }
}



///ACID system for EINSTEINDB
/// ACID is a variation of the VioletaBFT algorithm (a combination of HoneyBadger and Epaxos with interlocking directorate).
/// It is a system that provides a single point of failure-recovery.
/// The system is composed of three components:
/// 1. Log: A log is a collection of entries.
/// 2. State: A state is a collection of entries.
/// 3. Consistency: A consistency is a collection of entries.
///
/// The log is a collection of entries that are stored in a single file.
/// The state is a collection of entries that are stored in a single file.
/// The consistency is a collection of entries that are stored in a single file.
///


#[derive(Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    pub command: String,
}


impl LogEntry {
    pub fn new() -> LogEntry {
        LogEntry {
            term: 0,
            index: 0,
            command: String::new(),
        }
    }

    pub fn get_term(&mut self) -> io::Result<()> {
        Ok(())
    }
}


#[derive(Serialize, Deserialize)]
pub struct StateEntry {
    pub term: u64,
    pub index: u64,
    pub command: String,
}


impl StateEntry {
    pub fn new() -> StateEntry {
        StateEntry {
            term: 0,
            index: 0,
            command: String::new(),
        }
    }

    pub fn get_term(&mut self) -> io::Result<()> {
        Ok(())
    }
}


#[derive(Serialize, Deserialize)]
pub struct ConsistencyEntry {
    pub term: u64,
    pub index: u64,
    pub command: String,
}

///changelog: a collection of log entries for easier dithering on the dedup stream
///without having to store the entire log in memory.



