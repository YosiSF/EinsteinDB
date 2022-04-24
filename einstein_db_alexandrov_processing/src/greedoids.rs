// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.
// -------------------------------------------------------------------------------------------
use std::ffi::CStr;
use std::os::raw::{c_char, c_int, c_uint, c_void};
use std::ptr;
use std::slice::from_raw_parts;
use std::str;
use std::time::Duration;

use crate::error::{Error, Result};
use causet::causet_locale::{Locale, LocaleError};
use causet::causet_time::{Time, TimeError};
use allegro_poset::{AllegroPoset, PosetError};
use einstein_ml::greedoids::{Greedoids, GreedoidsError};
use causets::AllegroPosetSet;
use berolina_sql::{Sql, SqlError};
use foundationdb::{Fdb, FdbError};
use foundationdb_poset::{FdbPoset, FdbPosetError};
use std::sync::Arc;
use std::thread;
use std::time::Instant;
use std::{thread, time};

#[no_mangle]
pub extern "C" fn greedoids_new(
    locale: *const c_char,
    time_zone: *const c_char,
    time_format: *const c_char,
    time_locale: *const c_char,
    time_zone_locale: *const c_char,
    time_format_locale: *const c_char,
    locale_error: *mut c_int,
    time_error: *mut c_int,
    poset_error: *mut c_int,
    sql_error: *mut c_int,
    fdb_error: *mut c_int,
    fdb_poset_error: *mut c_int,
    greedoids_error: *mut c_int,
) -> *mut Greedoids {
    unsafe { locale_error.as_mut().unwrap() };
    unsafe { time_error.as_mut().unwrap() };
    unsafe { poset_error.as_mut().unwrap() };
    unsafe { sql_error.as_mut().unwrap() };
    unsafe { fdb_error.as_mut().unwrap() };
    unsafe { fdb_poset_error.as_mut().unwrap() };
    unsafe { greedoids_error.as_mut().unwrap() };

    unsafe { CStr::from_ptr(locale).to_str().unwrap() };
    unsafe { CStr::from_ptr(time_zone).to_str().unwrap() };
    unsafe { CStr::from_ptr(time_format).to_str().unwrap() };
    unsafe { CStr::from_ptr(time_locale).to_str().unwrap() };
    unsafe { CStr::from_ptr(time_zone_locale).to_str().unwrap() };
    unsafe { CStr::from_ptr(time_format_locale).to_str().unwrap() };

    let locale = match Locale::new(locale) {
        Ok(locale) => locale,
        Err(error) => {
            unsafe { *locale_error = error.code() };
            return ptr::null_mut();
        }
    };

    let time_zone = match Time::new_time_zone(time_zone) {
        Ok(time_zone) => time_zone,
        Err(error) => {
            unsafe { *time_error = error.code() };
            return ptr::null_mut();
        }
    };

    let time_format = match Time::new_time_format(time_format) {
        Ok(time_format) => time_format,
        Err(error) => {
            unsafe { *time_error = error.code() };
            return ptr::null_mut();
        }
    };

    let time_locale = match Locale::new(time_locale) {
        Ok(time_locale) => time_locale,
        Err(error) => {
            unsafe { *locale_error = error.code() };
            return ptr::null_mut();
        }
    };


#[no_mangle]
pub extern "C" fn greedoids_new_with_fdb(
    locale: *const c_char,
    time_zone: *const c_char,
    time_format: *const c_char,
    time_locale: *const c_char,
    time_zone_locale: *const c_char,
    time_format_locale: *const c_char,
    locale_error: *mut c_int,
    time_error: *mut c_int,
    poset_error: *mut c_int,
    sql_error: *mut c_int,
    fdb_error: *mut c_int,
    fdb_poset_error: *mut c_int,
    greedoids_error: *mut c_int,
) -> *mut Greedoids {
    unsafe { locale_error.as_mut().unwrap() };
    unsafe { time_error.as_mut().unwrap() };
    unsafe { poset_error.as_mut().unwrap() };
    unsafe { sql_error.as_mut().unwrap() };
    unsafe { fdb_error.as_mut().unwrap() };
    unsafe { fdb_poset_error.as_mut().unwrap() };
    unsafe { greedoids_error.as_mut().unwrap() };

    unsafe { CStr::from_ptr(locale).to_str().unwrap() };
    unsafe { CStr::from_ptr(time_zone).to_str().unwrap() };
    unsafe { CStr::from_ptr(time_format).to_str().unwrap() };
    unsafe { CStr::from_ptr(time_locale).to_str().unwrap() };
    unsafe { CStr::from_ptr(time_zone_locale).to_str().unwrap() };
    unsafe { CStr::from_ptr(time_format_locale).to_str().unwrap() };
    }





///! Rust wrapper for the C++ Greedoids library.
/// Causets are Posets and are represented as a vector of vectors of ints.
/// the int is usually the index of the node in the graph.
/// which holds an immutable reference to the graph.
/// the vector_clock inside einstein_db is a vector of vectors of ints.
///
/// The Rust wrapper is a bit more complicated than the C++ wrapper.
///
///
///
/// ![](https://raw.githubusercontent.com/EinsteinDB/EinsteinDB/master/docs/greedoids.png)
///

fn decode_and_own_string(c_str: *const c_char) -> Result<String> {
    let c_str = unsafe { CStr::from_ptr(c_str) };
    let string = c_str.to_str()?;
    Ok(string.to_owned())
}

/// Encodes a `GetReplicaRequest` into a `CString`
/// # Arguments
/// * `request` - The request to encode
/// # Returns
/// A `CString` containing the encoded request
/// # Errors
/// `Error` if the request could not be encoded
/// # Safety
/// The returned `CString` must be freed with `ffi::c_free`
/// # Example
/// ```
/// use einstein_db_alexandrov_processing::greedoids::*;
/// let request = GetReplicaRequest {
///    replica_id: "replica_id".to_string(),
///   replica_type: "replica_type".to_string(),
/// };
/// let c_str = encode_get_replica_request(&request).unwrap();
/// ```
/// # Example
/// ```
/// use einstein_db_alexandrov_processing::greedoids::*;
/// let request = GetReplicaRequest {
///   replica_id: "replica_id".to_string(),
///  replica_type: "replica_type".to_string(),
/// };


pub trait MiskitoMqttp {
    fn zero(&self) -> bool;
    fn visit_mqtt(&self) -> &[u8];
}

///! Rust wrapper for the C++ Greedoids library.
impl MiskitoMqttp for Vec<u8> {
    fn zero(&self) -> bool {
        //println!("{:?}", self);
        //println!("{:?}", self.len());
        //println!("{:?}", self.len() == 0);
        self.len() == 0
    }
    fn visit_mqtt(&self) -> &[u8] {
        self
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct GreedoidsRequest {
    pub request_type: c_int,
    pub request_data: *const c_char,
}


#[repr(C)]
#[derive(Debug)]
pub struct GreedoidsResponse {
    pub response_type: c_int,
    pub response_data: *const c_char,
}
pub trait UserCollectedGreedoids {
    fn get(&self, index: &[u8]) -> Option<&[u8]>;
    fn approximate_size_and_soliton_ids(&self, start: &[u8], end: &[u8]) -> Option<(usize, usize)>;
}

pub trait TableGreedoidsCollection {
    type UserCollectedGreedoids: UserCollectedGreedoids;

    /// Iterator all `UserCollectedGreedoids`, break if `f` returns false.
    fn iter_user_collected_greedoids<F>(&self, f: F)
    where
        F: FnMut(&Self::UserCollectedGreedoids) -> bool;
}

pub trait TableGreedoidsExt {
    type TableGreedoidsCollection: TableGreedoidsCollection;

    /// Collection of tables covering the given range.
    fn table_greedoids_collection(
        &self,
        start: &[u8],
        end: &[u8],
    ) -> Option<&Self::TableGreedoidsCollection>;

}
