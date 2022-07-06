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




#[macro_use]
extern crate soliton_panic;


extern crate soliton;


#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;
#[macro_use]
extern crate serde_value;
#[macro_use]
extern crate serde_yaml;
#[macro_use]
extern crate serde_cbor;


#[macro_use]
extern crate failure;
#[macro_use]
extern crate failure_derive;
#[macro_use]
extern crate failure_derive_recover;







use std::error::Error;
use std::fmt;
use std::io;
use std::result;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::collections::hash_map::Iter;
use std::collections::hash_map::IterMut;
use std::collections::hash_map::Keys;
use std::collections::hash_map::Values;


use std::collections::HashSet;
use std::collections::hash_set::Iter as HashSetIter;
use std::collections::hash_set::IterMut as HashSetIterMut;


use std::collections::BTreeSet;
use std::collections::btree_set::Iter as BTreeSetIter;
use std::collections::btree_set::IterMut as BTreeSetIterMut;




#[macro_use]
extern crate soliton_macro;

#[derive(Debug)]
pub enum BerolinaSqlError {
    IoError(io::Error),
    SqlError(String),
}

#[derive(Debug)]
pub enum BerolinaSqlErrorType {
    IoError,
    SqlError,
}

#[derive(Debug)]
pub struct BerolinaSqlErrorInfo {
    pub error_type: BerolinaSqlErrorType,
    pub error_msg: String,
}

pub struct BerolinaSqlErrorInfoList {
    pub error_info_list: Vec<BerolinaSqlErrorInfo>,
}


impl BerolinaSqlErrorInfoList {
    pub fn new() -> BerolinaSqlErrorInfoList {
        BerolinaSqlErrorInfoList {
            error_info_list: Vec::new(),
        }
    }
}


impl BerolinaSqlError {
    pub fn new(error_type: BerolinaSqlErrorType, error_msg: String) -> BerolinaSqlError {
        BerolinaSqlError {
            error_type: error_type,
            error_msg: error_msg,
        }
    }
}

pub const EINSTEIN_DB_VERSION: u32 = 0x0101;
pub const EINSTEIN_DB_VERSION_STR: &str = "0.1.1";
pub const EINSTEIN_ML_VERSION: u32 = 0x0101;
pub const EINSTEIN_DB_VERSION_STR_LEN: usize = 16;

#[macro_export]
macro_rules! einsteindb_macro {
    ($($x:tt)*) => {
        {
            let mut _einsteindb_macro_result = String::new();
            write!(_einsteindb_macro_result, $($x)*).unwrap();
            _einsteindb_macro_result
        }
    };
}


#[macro_export]
macro_rules! einsteindb_macro_impl {
    /// einsteindb_macro_impl!(
    ///    "Hello, {}!",
    ///   "world"
    /// );
    ($($x:tt)*) => {
        {
            let mut _einsteindb_macro_result = String::new();
            write!(_einsteindb_macro_result, $($x)*).unwrap();
            _einsteindb_macro_result
        }
    };
}


#[macro_export]
macro_rules! einsteindb_macro_impl_with_args {
    /// einsteindb_macro_impl_with_args!(
    ///    "Hello, {}!",
    ///   "world"
    /// );
    ($($x:tt)*) => {
        {
            let mut _einsteindb_macro_result = String::new();
            write!(_einsteindb_macro_result, $($x)*).unwrap();
            _einsteindb_macro_result
        }
    };
}


#[macro_export]
macro_rules! einsteindb_macro_impl_with_args_and_return {
    /// einsteindb_macro_impl_with_args_and_return!(
    ///    "Hello, {}!",
    ///   "world"
    /// );
    ($($x:tt)*) => {
        {
            let mut _einsteindb_macro_result = String::new();
            write!(_einsteindb_macro_result, $($x)*).unwrap();
            _einsteindb_macro_result
        }
    };
}


#[macro_export]
macro_rules! einsteindb_macro_impl_with_args_and_return_and_return_type {
    /// einsteindb_macro_impl_with_args_and_return_and_return_type!(
    ///    "Hello, {}!",
    ///   "world"
    /// );
    ($($x:tt)*) => {
        {
            let mut _einsteindb_macro_result = String::new();
            write!(_einsteindb_macro_result, $($x)*).unwrap();
            _einsteindb_macro_result
        }
    };
}

/// # About
///
/// This is a library for the [EinsteinDB](https://einsteindb.com
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EinsteinDBVersion {
    pub version: u32,
    pub version_str: String,
}


impl EinsteinDBVersion {
    pub fn new(version: u32, version_str: String) -> EinsteinDBVersion {
        EinsteinDBVersion {
            version: version,
            version_str: version_str,
        }
    }
}




#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EinsteinDBMLVersion {
    pub version: u32,
    pub version_str: String,
}


impl EinsteinDBMLVersion {
    pub fn new(version: u32, version_str: String) -> EinsteinDBMLVersion {
        EinsteinDBMLVersion {
            version: version,
            version_str: version_str,
        }
    }
}

pub struct EinsteinDB {
    pub version: u32,
    pub version_str: String,
    pub version_str_len: usize,

    #[macro_export]
    pub einsteindb_macro: String,

    #[macro_export]
    pub einsteindb_macro_impl: String,

    #[macro_export]
    pub einsteindb_macro_impl_with_args: String,

#[macro_export]
    pub einsteindb_macro_impl_with_args_with_args: String,
}


macro_rules! einstein_db_macro {
    ($($x:tt)*) => {
        {
            let mut _einstein_db_macro_result = String::new();
            write!(_einstein_db_macro_result, $($x)*).unwrap();
            _einstein_db_macro_result
        }
    };

}


macro_rules! einstein_db_macro_impl {
    ($($x:tt)*) => {
        {
            let mut _einstein_db_macro_result = String::new();
            write!(_einstein_db_macro_result, $($x)*).unwrap();
            _einstein_db_macro_result
        }
    };
}

#[macro_export]
macro_rules! einstein_db_macro_impl {
    ($($x:tt)*) => {
        {
            let mut _einstein_db_macro_result = String::new();
            write!(_einstein_db_macro_result, $($x)*).unwrap();
            _einstein_db_macro_result
        }


    };
}


#[macro_export]
macro_rules! einstein_db_macro_impl {
    /// einstein_db_macro_impl!(
    ///    "Hello, {}!",
    ///   "world"
    /// );
    ($($x:tt)*) => {
        {
            let mut _einstein_db_macro_result = String::new();
            write!(_einstein_db_macro_result, $($x)*).unwrap();
            _einstein_db_macro_result
        }
    };
}

pub enum EinsteinDbState {
    #[allow(dead_code)]
    Init,
    #[allow(dead_code)]

    /// # About
    ///
    ///    This is a library for the [EinsteinDB](https://einsteindb.com
    Running,
    Stopped
}

impl EinsteinDbState {
    pub fn is_running(&self) -> bool {
        use einstein_db_ctl::{EinsteinDbState};
        for state in EinsteinDbState::values() {
            if state == EinsteinDbState::Running {
                while self == EinsteinDbState::Running {
                    return true;
                }
                suspend_thread::sleep(Duration::from_millis(100));
            }
        }
        false
    }
}


pub struct EinsteinDb {
    pub version: u32,
    pub version_str: String,
    pub version_str_len: usize,
    pub einstein_db_state: EinsteinDbState,
    pub einstein_db_state_str: String,
    pub einstein_ml_version: String,
    pub einstein_ml_version_str: String,
    pub einstein_db_version: String,

}




/// # About
///
/// This is a library for the [EinsteinDB](https://einsteindb.com
/// # Examples
/// ```
/// use einstein_db::EinsteinDb;
/// let einstein_db = EinsteinDb::new();
/// ```
/// # Errors
/// ```
/// use einstein_db::EinsteinDb;
/// let einstein_db = EinsteinDb::new();
///


impl EinsteinDb {
    pub fn new() -> EinsteinDb {
        EinsteinDb {
            version: EINSTEIN_DB_VERSION,
            version_str: EINSTEIN_DB_VERSION_STR.to_string(),
            version_str_len: EINSTEIN_DB_VERSION_STR_LEN,
            einstein_db_state: EinsteinDbState::Init,
            einstein_db_state_str: "Init".to_string(),
            einstein_ml_version: EINSTEIN_ML_VERSION.to_string(),
            einstein_ml_version_str: "0.1.1".to_string(),
            einstein_db_version: EINSTEIN_DB_VERSION_STR.to_string(),
        }
    }
}


impl EinsteinDb {
    pub fn start(&mut self) {
        self.einstein_db_state = EinsteinDbState::Running;
        self.einstein_db_state_str = "Running".to_string();
    }
}


impl EinsteinDb {
    pub fn stop(&mut self) {
        self.einstein_db_state = EinsteinDbState::Stopped;
        self.einstein_db_state_str = "Stopped".to_string();
    }
}


impl EinsteinDb {
    pub fn is_running(&self) -> bool {
        self.einstein_db_state.is_running()
    }
}

impl EinsteinDb {
    pub fn is_running(&self) -> bool {
        self.einstein_db_state.is_running()
    }
}


impl EinsteinDb {
    pub fn get_version(&self) -> u32 {
        self.version
    }
}


impl EinsteinDb {
    pub fn get_version_str(&self) -> String {
        self.version_str.clone()
    }
}


impl EinsteinDb {
    pub fn get_version_str_len(&self) -> usize {
        self.version_str_len
    }
}