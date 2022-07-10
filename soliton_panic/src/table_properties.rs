// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.
// -----------------------------------------------------------------------------
//! # EinsteinDB
//! # ----------------------------------------------------------------
//!
//!   #[macro_use]
//! extern crate lazy_static;
//! #[macro_use]
//! extern crate serde_derive;
//! #[macro_use]
//! extern crate serde_json;
//! #[macro_use]
//! extern crate serde_json_utils;
//! #[macro_use]
//! extern crate log;
//! #[macro_use]
//! extern crate serde_json_utils;
//!


use std::ffi::CString;

use std::os::raw::c_char;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};

use std::time::Duration;
use std::time::Instant;
use std::{thread, time};
use std::error::Error;


use soliton_panic::*;
use soliton::*;
use causal_set::CausalSet;

pub use soliton_panic::{
    CausetPartitionerRequest,
    CausetPartitionerResult,
    CausetPartitionerContext,
};


use soliton_panic::{
    CausetPartitionerRequest,
    CausetPartitionerResult,
    CausetPartitionerContext,
};


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

#[derive(Debug, Clone)]
pub struct ThreadInfoBrane {
    pub thread_id: usize,
    pub thread_name: String,
    pub thread_receiver: Receiver<()>,
    pub thread_sender: Sender<()>,
    pub thread_is_running: bool,
    pub thread_is_stopped: bool,
    pub thread_is_finished: bool,
    pub thread_is_error: bool,
    pub thread_error: String,
    pub thread_error_message: String,
    pub thread_error_backtrace: String,
    pub thread_error_backtrace_lines: Vec<String>,
    pub thread_error_backtrace_lines_count: usize,
    pub thread_error_backtrace_lines_count_max: usize,
    pub thread_error_backtrace_lines_count_min: usize,
    pub thread_error_backtrace_lines_count_avg: usize,
    pub thread_error_backtrace_lines_count_median: usize,
    pub thread_error_backtrace_lines_count_mode: usize,
    pub thread_error_backtrace_lines_count_stddev: usize,
    pub thread_error_backtrace_lines_count_variance: usize,
    pub thread_error_backtrace_lines_count_skew: usize,
    pub thread_error_backtrace_lines_count_kurtosis: usize,
    pub thread_error_backtrace_lines_count_kurtosis_normalized: usize,

}



///! SolitonPanic is a struct that contains the following:
/// - config: the configuration of the soliton_panic
/// - db: the database of the soliton_panic
/// - poset: the poset of the soliton_panic
/// - db_name: the name of the database
/// - db_path: the path of the database
/// - db_config: the name of the database config
/// - db_config_path: the path of the database config
#[derive(Debug, Clone)]
async fn get_config(db: &FdbTransactional) -> Result<Config, dyn Error> {
    let config_file = db.get_config_file().await?;
    let config_file_content = db.get_config_file_content().await?;
    let config = Config::new(config_file, config_file_content);
    Ok(config)
}


#[derive(Debug, Clone)]
pub struct ThreadInfo {
    pub thread_id: usize,
    pub thread_name: String,
    pub thread_receiver: Receiver<()>,
    pub thread_sender: Sender<()>,
    pub thread_is_running: bool,
    pub thread_is_stopped: bool,
    pub thread_is_finished: bool,
    pub thread_is_error: bool,
    pub thread_error: String,
    pub thread_error_message: String,
    pub thread_error_backtrace: String,
    pub thread_error_backtrace_lines: Vec<String>,
}

// DatabaseTypeName returns the database system name of the column type. If an empty
// string is returned, then the driver type name is not supported.
// Consult your driver documentation for a list of driver data types. Length specifiers
// are not included.
// Common type names include "VARCHAR", "TEXT", "NVARCHAR", "DECIMAL", "BOOL",
// "INT", and "BIGINT".


///! SolitonPanic is a struct that contains the following:
/// - config: the configuration of the soliton_panic
/// - db: the database of the soliton_panic
/// - poset: the poset of the soliton_panic
/// - db_name: the name of the database
/// - db_path: the path of the database
/// - db_config: the name of the database config
/// - db_config_path: the path of the database config
///
#[no_mangle]
#[allow(unused_variables)]
pub extern "C" fn soliton_panic_causet_partitioner_database_type_name() -> *const c_char {
    CString::new("").unwrap().as_ptr()
}


// DatabaseTypeName returns the database system name of the column type. If an empty
// string is returned, then the driver type name is not supported.
// Consult your driver documentation for a list of driver data types. Length specifiers
// are not included.



#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CausetPartitionerRequest<'a> {
    pub prev_user_soliton_id: &'a [u8],
    pub current_user_soliton_id: &'a [u8],
    pub current_output_file_size: u64,
}


#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CausetPartitionerResult {
    NotRequired,
    Required,
}


#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CausetPartitionerContext {
    pub prev_user_soliton_id: Vec<u8>,
    pub current_user_soliton_id: Vec<u8>,
    pub current_output_file_size: u64,
}


#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CausetPartitioner<'a> {
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
    pub thread_info: ThreadInfo,
}


#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CausetPartitionerConfig {
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
    pub thread_info: ThreadInfo,
}


#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_request_new(prev_user_soliton_id: &[u8], current_user_soliton_id: &[u8], current_output_file_size: u64) -> Box<CausetPartitionerRequest> {
    Box::new(CausetPartitionerRequest {
        prev_user_soliton_id,
        current_user_soliton_id,
        current_output_file_size,
    })
}


#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_request_free(request: Box<CausetPartitionerRequest>) {
}

pub struct UserCollectedGreedoids;
impl fdb_traits::UserCollectedGreedoids for UserCollectedGreedoids {
    fn get(&self, _: &[u8]) -> Option<&[u8]> {
        None
    }
    fn approximate_size_and_soliton_ids(&self, _: &[u8], _: &[u8]) -> Option<(usize, usize)> {
        None
    }
}

pub struct TableGreedoidsCollection;
impl fdb_traits::TableGreedoidsCollection for TableGreedoidsCollection {
    type UserCollectedGreedoids = UserCollectedGreedoids;
    fn iter_user_collected_greedoids<F>(&self, _: F)
    where
        F: FnMut(&Self::UserCollectedGreedoids) -> bool,
    {
    }
}

impl fdb_traits::TableGreedoidsExt for soliton_panic_merkle_tree {
    type TableGreedoidsCollection = TableGreedoidsCollection;
    fn table_greedoids_collection(
        &self,
        namespaced: &str,
        table_name: &str,
    ) -> &Self::TableGreedoidsCollection {
        unimplemented!()
    }
}




#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_result_new() -> Box<CausetPartitionerResult> {
    Box::new(CausetPartitionerResult::NotRequired)
}


#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_result_free(result: Box<CausetPartitionerResult>) {
}


// Scan copies the columns in the current row into the values pointed
// at by dest. The number of values in dest must be the same as the
// number of columns in Rows.
//
// Scan converts columns read from the database into the following
// common Go types and special types provided by the sql package:



#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_scan(
    request: Box<CausetPartitionerRequest>,
    result: Box<CausetPartitionerResult>,
) -> Box<CausetPartitionerResult> {
    result
}


#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_context_new() -> Box<CausetPartitionerContext> {
    Box::new(CausetPartitionerContext {
        prev_user_soliton_id: Vec::new(),
        current_user_soliton_id: Vec::new(),
        current_output_file_size: 0,
    })
}



#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_context_free(context: Box<CausetPartitionerContext>) {

    //println!("causet_partitioner_context_free");
    println!("causet_partitioner_context_free");

}

//    *string
//    *[]byte
//    *int, *int8, *int16, *int32, *int64
//    *uint, *uint8, *uint16, *uint32, *uint64
//    *bool
//    *float32, *float64
//    *interface{}
//    *RawBytes
//    *Rows (cursor value)
//    any type implementing Scanner (see Scanner docs)
//
// In the most simple case, if the type of the value from the source
// column is an integer, bool or string type T and dest is of type *T,
// Scan simply assigns the value through the pointer.
//
// Scan also converts between string and numeric types, as long as no
// information would be lost. While Scan stringifies all numbers
// scanned from numeric database columns into *string, scans into
// numeric types are checked for overflow. For example, a float64 with
// value 300 or a string with value "300" can scan into a uint16, but
// not into a uint8, though float64(255) or "255" can scan into a
// uint8. One exception is that scans of some float64 numbers to
// strings may lose information when stringifying. In general, scan
// floating point columns into *float64.
//
// If a dest argument has type *[]byte, Scan saves in that argument a
// copy of the corresponding data. The copy is owned by the caller and
// can be modified and held indefinitely. The copy can be avoided by
// using an argument of type *RawBytes instead; see the documentation
// for RawBytes for restrictions on its use.
//
// If an argument has type *interface{}, Scan copies the value
// provided by the underlying driver without conversion. When scanning
// from a source value of type []byte to *interface{}, a copy of the
// slice is made and the caller owns the result.


#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_scan_column(
    context: Box<CausetPartitionerContext>,
    column_name: &[u8],
    column_value: &[u8],
) -> Box<CausetPartitionerContext> {
    context
}


#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_scan_column_as_string(
    context: Box<CausetPartitionerContext>,
    column_name: &[u8],
    column_value: &[u8],
) -> Box<CausetPartitionerContext> {
    context
}


#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_scan_column_as_int(
    context: Box<CausetPartitionerContext>,
    column_name: &[u8],
    column_value: &[u8],
) -> Box<CausetPartitionerContext> {
    context
}






#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_scan_column_as_bool(
    context: Box<CausetPartitionerContext>,
    column_name: &[u8],
    column_value: &[u8],
) -> Box<CausetPartitionerContext> {
    context
}


#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_scan_column_as_float(
    context: Box<CausetPartitionerContext>,
    column_name: &[u8],
    column_value: &[u8],
) -> Box<CausetPartitionerContext> {
    context
}

// Source values of type time.Time may be scanned into values of type
// *time.Time, *interface{}, *string, or *[]byte. When converting to
// the latter two, time.RFC3339Nano is used.
//
// Source values of type bool may be scanned into types *bool,
// *interface{}, *string, *[]byte, or *RawBytes.
//
// For scanning into *bool, the source may be true, false, 1, 0, or
// string inputs parseable by strconv.ParseBool.
//
// Scan can also convert a cursor returned from a query, such as
// "select cursor(select * from my_table) from dual", into a
// *Rows value that can itself be scanned from. The parent
// select query will close any cursor *Rows if the parent *Rows is closed.
//
// If any of the first arguments implementing Scanner returns an error,
// that error will be wrapped in the returned error
// and the scan will stop. If any argument returns an ErrNoRows, the


#[no_mangle]
#[allow(unused_variables)]
pub fn causet_partitioner_request_scan(request: &CausetPartitionerRequest, dest: &mut [u8]) -> bool {
    let mut dest_iter = dest.iter_mut();
    let mut prev_user_soliton_id_iter = request.prev_user_soliton_id.iter();
    let mut current_user_soliton_id_iter = request.current_user_soliton_id.iter();
    let mut current_output_file_size_iter = request.current_output_file_size.to_be_bytes().iter();
    loop {
        match (prev_user_soliton_id_iter.next(), current_user_soliton_id_iter.next(), current_output_file_size_iter.next()) {
            (Some(prev_user_soliton_id), Some(current_user_soliton_id), Some(current_output_file_size)) => {
                dest_iter.next().map(|dest| *dest = *prev_user_soliton_id);
                dest_iter.next().map(|dest| *dest = *current_user_soliton_id);
                dest_iter.next().map(|dest| *dest = *current_output_file_size);
            }
            _ => break,
        }
    }
    true
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
#[allow(dead_code)]
pub enum CausetScanTablePartition {
    CausetPart0,
    // 0-index-partition
    CausetPart1,
    // 1-index-partition
    CausetPart2,
    // 2-index-partition
    CausetPart3 // 3-index-partition
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
#[allow(dead_code)]
pub enum CausetScanTablePartitionType {
    CausetPartitionType0,
    // 0-index-partition
    CausetPartitionType1,
    // 1-index-partition
    CausetPartitionType2,
    // 2-index-partition
    CausetPartitionType3 // 3-index-partition
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
#[allow(dead_code)]
pub enum CausetScanTablePartitionType0 {
    CausetPartitionType0_0,
    // 0-index-partition
    CausetPartitionType0_1,
    // 1-index-partition
    CausetPartitionType0_2,
    // 2-index-partition
    CausetPartitionType0_3 // 3-index-partition
}




#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
#[allow(dead_code)]
pub enum CausetScanTablePartitionType1 {
    CausetPartitionType1_0,
    // 0-index-partition
    CausetPartitionType1_1,
    // 1-index-partition
    CausetPartitionType1_2,
    // 2-index-partition
    CausetPartitionType1_3 // 3-index-partition
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
#[allow(dead_code)]

pub enum CausetScanTablePartitionType2 {
    CausetPartitionType2_0,
    // 0-index-partition
    CausetPartitionType2_1,
    // 1-index-partition
    CausetPartitionType2_2,
    // 2-index-partition
    CausetPartitionType2_3 // 3-index-partition
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(C)]
#[allow(dead_code)]
pub enum CausetScanTablePartitionType3 {
    CausetPartitionType3_0,
    // 0-index-partition
    CausetPartitionType3_1,
    // 1-index-partition
    CausetPartitionType3_2,
    // 2-index-partition
    CausetPartitionType3_3 // 3-index-partition
}

