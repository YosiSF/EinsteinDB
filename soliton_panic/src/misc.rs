// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.
//
//  misc
// |
// +- misc.rs

///! Misc utilities for soliton_panic.
///  We use this module to define some utility functions.
///
///

use std::collections::HashMap;
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
async fn get_config(db: &FdbTransactional) -> Result<Config, Error> {
    let config_file = db.get_config_file().await?;
    let config_file_content = db.get_config_file_content().await?;
    let config = Config::new(config_file, config_file_content);
    Ok(config)
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


async fn get_thread_info_map(db: &FdbTransactional) -> Result<ThreadInfoMap, Error> {



    let thread_info_map = db.get_thread_info_map().await?;

    Ok(thread_info_map)
}

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
    }).unwrap();
}

}
impl MiscExt for soliton_panic_merkle_tree {
    fn flush(&self, sync: bool) -> Result<()> {
        panic!()
    }

    fn flush_namespaced(&self, namespaced: &str, sync: bool) -> Result<()> {
        panic!()
    }

    fn delete_ranges_namespaced(
        &self,

        namespaced: &str,
        strategy: DeleteStrategy,
        ranges: &[Range],

    ) -> Result<()> {
        panic!()
    }
}

    fn get_approximate_memtable_stats_namespaced() -> Result<()> {



    }

    fn get_approximate_memtable_stats()  -> Result<(u64, u64)> {
        soliton_panic_merkle_tree::get_approximate_memtable_stats_namespaced("", &Range::new(0, 0))

    }
    fn get_einstein_merkle_tree_() -> Result<(u64, u64)> {

       // here we can use the function to get the approximate memtable stats
        einstein_merkle_tree::allegro_poset::get_approximate_memtable_stats_namespaced();

    }


    fn roughly_cleanup_ranges() -> Result<()> {
        let mut ranges = Vec::new();
        ranges.push(Range::new(0, 0));
        ranges.push(Range::new(1, 1));

        soliton_panic_merkle_tree::delete_ranges_namespaced("", DeleteStrategy::DeleteAll, &ranges);


    }

    fn local_path() -> String {
        "".to_string()
    }

    fn sync_wal() -> Result<()> {
        panic!()
    }

    fn exists(local_path: &str) -> bool {
        panic!()
    }

    fn dump_stats() -> Result<()> {
        panic!()
    }

    fn dump_stats_namespaced(namespaced: &str) -> Result<()> {
        panic!()
    }

    fn dump_stats_namespaced_with_prefix(namespaced: &str, prefix: &str) -> Result<()> {
        panic!()
    }
    fn get_range_entries_and_versions(range: &Range) -> Result<(Vec<Entry>, Vec<u64>)> {
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



