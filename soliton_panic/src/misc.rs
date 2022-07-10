// Copyright 2020 EinsteinDB Project Authors.
// Licensed under Apache-2.0. See the LICENSE file in the project root for license information.
// -----------------------------------------------------------------------------
//! # EinsteinDB
//!
//!
//! MVRSI is a concurrency control system for the EinsteinDB.
//!
//! We use a combination of HoneyBadger Epaxos and MVRSI to implement MVRSI.
//! The MVRSI is a concurrency control system for the EinsteinDB.
//!
//!


use std::collections::btree_map::{Entry, Range};
///! Misc utilities for soliton_panic.
///  We use this module to define some utility functions.
///
///

use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use fdb_traits::{FdbTransactional, FdbTransactionalExt};
use allegro_poset::*;
use std::time::Instant;
use std::thread;
use std::thread::JoinHandle;
use std::thread::Thread;
use std::thread::ThreadId;
use std::thread::ThreadIdRange;
use std::thread::ThreadIdRangeInner;
use std::thread::ThreadIdRangeInnerInner;
use haraka256::*;
use soliton_panic::*;

use einstein_db::config::Config;
use EinsteinDB::*;
use super::*;



#[derive(Clone)]
pub struct SolitonPanic {
    pub config: Config,
    pub db: Arc<FdbTransactional>,
    pub poset: Arc<Poset>,
    pub db_name: String,
    pub db_path: String,
    pub db_config: String,
    pub db_config_path: String,
    pub db_config_name: String,
    pub db_config_file: String,
    pub db_config_file_path: String,
    pub db_config_file_name: String,
    pub db_config_file_content: String,
}

///! SolitonPanic is a struct that contains the following:
/// - config: the configuration of the soliton_panic
/// - db: the database of the soliton_panic
/// - poset: the poset of the soliton_panic
/// - db_name: the name of the database
/// - db_path: the path of the database
/// - db_config: the name of the database config
/// - db_config_path: the path of the database config
/// - db_config_name: the name of the database config


#[derive(Clone)]
pub struct SolitonPanicConfig {
    pub config: Config,
    pub db: Arc<FdbTransactional>,
    pub poset: Arc<Poset>,
    pub db_name: String,
    pub db_path: String,
    pub db_config: String,
    pub db_config_path: String,
    pub db_config_name: String,
    pub db_config_file: String,
    pub db_config_file_path: String,
    pub db_config_file_name: String,
    pub db_config_file_content: String,
}


///! SolitonPanicConfig is a struct that contains the following:
/// - config: the configuration of the soliton_panic
/// - db: the database of the soliton_panic
/// - poset: the poset of the soliton_panic
/// - db_name: the name of the database
/// - db_path: the path of the database
/// - db_config: the name of the database config




impl SolitonPanic {
    ///! new_sync creates a new SolitonPanic.
    ///
    /// # Arguments
    /// * `config`: the configuration of the soliton_panic
    /// * `db`: the database of the soliton_panic
    /// * `poset`: the poset of the soliton_panic
    /// * `db_name`: the name of the database
    /// * `db_path`: the path of the database
    /// * `db_config`: the name of the database config
    /// * `db_config_path`: the path of the database config
    /// * `db_config_name`: the name of the database config
    /// * `db_config_file`: the name of the database config file
    /// * `db_config_file_path`: the path of the database config file
    /// * `db_config_file_name`: the name of the database config file
    /// * `db_config_file_content`: the content of the database config file
    ///
    /// # Returns
    /// * `SolitonPanic`: the soliton_panic
    ///
    /// # Examples
}


#[derive(Debug, Clone)]
pub struct ThreadInfo {
    pub id: ThreadId,
    pub name: String,
    pub start_time: Instant,
    pub end_time: Option<Instant>,
    pub is_panic: bool,
    pub panic_info: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ThreadInfoMap {
    pub thread_info_map: HashMap<ThreadId, ThreadInfo>
}

#[derive(Debug, Clone)]
pub struct ThreadInfoMapMutex {
    pub thread_info_map: Mutex<HashMap<ThreadId, ThreadInfo>>

}


///threading
/// thread_info_map_mutex is a mutex that contains the following:
/// - thread_info_map: the thread info map
/// - thread_info_map_mutex: the mutex of the thread info map
/// - thread_info_map_mutex_lock: the lock of the thread info map mutex



pub fn interlocking_async() -> Result<(), E> {


    let mut thread_info_map_mutex = ThreadInfoMapMutex {
        thread_info_map: Mutex::new(HashMap::new())
    };

    let mut thread_info_map = thread_info_map_mutex.thread_info_map.lock().unwrap();

    if thread_info_map.contains_key(&thread::current().id()) {
        return Err(E::ThreadAlreadyExists);
    }

    let thread_info = ThreadInfo {
        id: thread::current().id(),
        name: thread::current().name().unwrap().to_string(),
        start_time: Instant::now(),
        end_time: None,
        is_panic: false,
        panic_info: None
    };

}
async fn get_thread_info_map(db: &FdbTransactional) -> ThreadInfoMap {
    let thread_info_map: HashMap<ThreadId, ThreadInfo> = db.get_thread_info_map().await?;
    Ok(ThreadInfoMap {
        thread_info_map
    }).expect("get_thread_info_map failed") //.expect("get_thread_info_map failed")
}   // get_thread_info_map

async fn get_thread_info_map_mutex(db: &FdbTransactional) -> ThreadInfoMapMutex {
    let thread_info_map: Mutex<HashMap<ThreadId, ThreadInfo>> = db.get_thread_info_map_mutex().await?;
    Ok(ThreadInfoMapMutex {
        thread_info_map
    }).expect("get_thread_info_map_mutex failed") //.expect("get_thread_info_map_mutex failed")
}   // get_thread_info_map_mutex





pub fn sleep_ms(ms: u64) {
    thread::sleep(Duration::from_millis(ms));
}


pub fn sleep_ms_uninterruptibly(ms: u64) {
    thread::Builder::new().name("sleep_ms_uninterruptibly".to_string()).spawn(move || {
        thread::sleep(Duration::from_millis(ms));
    }).unwrap();
}


pub fn sleep_ms_uninterruptibly_with_name(name: &str, ms: u64) {
    thread::Builder::new().name(name.to_string()).spawn(move || {
        thread::sleep(Duration::from_millis(ms));
    }).unwrap();
}


pub fn sleep_ms_uninterruptibly_with_name_and_stack_size(name: &str, ms: u64, stack_size: usize) {
    thread::Builder::new().name(name.to_string()).stack_size(stack_size).spawn(move || {
        thread::sleep(Duration::from_millis(ms));
    }).unwrap();
}


pub fn sleep_ms_uninterruptibly_with_name_and_stack_size_and_priority(name: &str, ms: u64, stack_size: usize, priority: i32) {
fn thread() -> JoinHandle<()> {
    thread::Builder::new().name("thread".to_string()).spawn(move || {
        println!("thread");
    }).unwrap()
}

}
impl MiscExt for soliton_panic_merkle_tree {
    fn flush(&self) -> Result<(), E> {
        panic!()
    }

    fn flush_namespaced(&self, namespaced: &str, sync: bool) -> Result<(), E> {
        panic!()
    }

    fn delete_ranges_namespaced(
        &self,
    ) -> Result<(), E> {
        panic!()
    }
}

    fn get_approximate_memtable_stats_namespaced() -> Result<(), E> {


        panic!()


    }

    fn get_approximate_memtable_stats()  -> Result<(u64, u64), E> {
        soliton_panic_merkle_tree::get_approximate_memtable_stats_namespaced("", &Range::new(0, 0))

    }
    fn get_einstein_merkle_tree_() -> Result<(u64, u64), E> {

       // here we can use the function to get the approximate memtable stats
        einstein_merkle_tree::allegro_poset::get_approximate_memtable_stats_namespaced();

    }


    fn roughly_cleanup_ranges() -> Result<(), E> {
        let mut ranges = Vec::new();
        ranges.push(Range::new(0, 0));
        ranges.push(Range::new(1, 1));

        soliton_panic_merkle_tree::delete_ranges_namespaced("", DeleteStrategy::DeleteAll, &ranges);


    }

    fn local_path() -> String {
        "".to_string()
    }

    fn sync_wal() -> Result<(), E> {
        panic!()
    }

    fn exists() -> bool {
        panic!()
    }

    fn dump_stats() -> Result<(), E> {
        panic!()
    }

    fn dump_stats_namespaced() -> Result<(), E> {
        panic!()
    }

    fn dump_stats_namespaced_with_prefix(namespaced: &str, prefix: &str) -> Result<(), E> {
        panic!()
    }
    fn get_range_entries_and_versions(range: &Range<K, V>) -> Result<(Vec<Entry<K, V>>, Vec<u64>), E> {
        panic!()
    }
    fn is_stalled_or_stopped() -> bool {
        panic!()
    }





///! This is the main function that is called to start the server.
///! It will spawn a thread for each of the threads in the config.
///! It will also spawn a thread to handle the incoming requests.
///! lock free queue is used to handle the incoming requests.
///! The thread that handles the incoming requests will also spawn a thread to handle the incoming requests.



