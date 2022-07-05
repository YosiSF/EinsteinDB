///! Copyright (c) 2022 Whtcorps Inc and EinsteinDB Project contributors
///!
///! Licensed under the Apache License, Version 2.0 (the "License");
///! you may not use this file except in compliance with the License.
///! You may obtain a copy of the License at
///!
///!     http://www.apache.org/licenses/LICENSE-2.0
///!
///! Unless required by applicable law or agreed to in writing, software
///! distributed under the License is distributed on an "AS IS" BASIS,
///! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
///! See the License for the specific language governing permissions and
///! limitations under the License.
///!
///! # About
///!
///! This is a library for the [EinsteinDB](https://einsteindb.com
///! "EinsteinDB: A Scalable, High-Performance, Distributed Database")
///!


/// CHANGELOG:  
/// - [0.1.0]( 
///  - Initial version
/// - [0.1.1](
/// - Add `einsteindb_macro` macro
/// - [0.1.2](
/// - Add `einsteindb_macro_impl` macro
/// 
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

use einstein_db::{EINSTEIN_DB_VERSION, EINSTEIN_DB_VERSION_STR};
use einstein_db::error::{Error, Result};
use causets::{Causets, CausetsMut};
use causetq::{Causetq, CausetqMut};
use causet::{Causet, CausetMut};
use einstein_ml::{Einstein_ML_VERSION, Einstein_ML_VERSION_STR};
use einstein_ml::error::{MLResult, MLResultMut};
use allegro_poset::{Allegro_POSET_VERSION, Allegro_POSET_VERSION_STR};
use einstein_db_alexandrov_poset_processv_processing::{Einstein_DB_ALEXANDROV_POSET_PROCESSV_PROCESSING_VERSION, Einstein_DB_ALEXANDROV_POSET_PROCESSV_PROCESSING_VERSION_STR};
use einstein_db_alexandrov_poset_processv_processing::error::{Einstein_DB_ALEXANDROV_POSET_PROCESSV_PROCESSING_Result, Einstein_DB_ALEXANDROV_POSET_PROCESSV_PROCESSING_ResultMut};

pub const EINSTEIN_DB_VERSION_STR_LEN: usize = 16;


/// # About
///
/// This is a library for the [EinsteinDB](https://einsteindb.com


pub use einstein_db::{EinsteinDB, EinsteinDBMut};
pub use einstein_db::error::{EinsteinDBResult, EinsteinDBResultMut};


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
    pub einstein_db_state: EinsteinDbState,

    pub einstein_ml_version: String,
    pub einstein_ml_version_str: String,
    pub einstein_db_version: String,

}


//! # About
//!
//! This is a library for the [EinsteinDB](https://einsteindb.com
pub fn einstein_db_version() -> String {
    EINSTEIN_DB_VERSION_STR.to_string();
    let replicant_version = format!("{}", EINSTEIN_DB_VERSION);
    replicant_version.to_string();
    let einstein_db_version = EinsteinDB::einstein_db_version();
    einstein_db_version.duration_since(UNIX_EPOCH).unwrap().as_secs();
    let einstein_db_version_str = EinsteinDB::einstein_db_version_str();
    einstein_db_version_str.to_string();
    let einstein_ml_version = Einstein_ML::einstein_ml_version();
    //causet_locale
    let einstein_ml_version_str = Einstein_ML::einstein_ml_version_str();
    einstein_ml_version_str.to_string();
}

pub fn log_einsteindb_info(einstein_db_version: &str, einstein_ml_version: &str, einstein_db_version_str: &str, einstein_ml_version_str: &str) {
    let einstein_db_version = EinsteinDB::einstein_db_version();
    let einstein_db_version_str = EinsteinDB::einstein_db_version_str();
    let einstein_ml_version = Einstein_ML::einstein_ml_version();
    let einstein_ml_version_str = Einstein_ML::einstein_ml_version_str();
    println!("{}", einstein_db_version);
    println!("{}", einstein_db_version_str);
    println!("{}", einstein_ml_version);
    println!("{}", einstein_ml_version_str);

    for state in EinsteinDbState::values() {
        if state == EinsteinDbState::Running {
            #[allow(unused_variables)]
            let einstein_db_version = EinsteinDB::einstein_db_version();
            #[allow(unused_variables)]
            let einstein_db_version_str = EinsteinDB::einstein_db_version_str();
            while state == EinsteinDbState::Running {
                return;
            }
            suspend_thread::sleep(Duration::from_millis(100));
        }
        info!("{}", state);
    }

    //let einstein_db_version = EinsteinDB::einstein_db_version();
    //let einstein_db_version_str = EinsteinDB::einstein_db_version_str();

}



