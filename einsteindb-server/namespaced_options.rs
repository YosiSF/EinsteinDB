// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// ----------------------------------------------------------------------------
// @author     <> @CavHack @jedisct1 @kamilskurz @rukzuk @tomaslazdik @slushie



use std::fmt::{self, Display, Formatter};
use std::error::Error;
use std::io;
use std::result;
use std::string::FromUtf8Error;
use std::str::Utf8Error;
use std::string::FromUtf8Error;


#[derive(Debug)]
pub struct ResultExt<T, E> {
    pub value: result::Result<T, E>,
}


impl<T, E> ResultExt<T, E> {
    pub fn failure(err: E) -> Self {
        ResultExt {
            value: Err(err),
        }
    }

    pub fn ok(value: T) -> Self {
        ResultExt {
            value: Ok(value),
        }
    }
}


impl<T, E> ResultExt<T, E> {
    pub fn unwrap(self) -> T {
        self.value.unwrap()
    }
}


impl<T, E> ResultExt<T, E> {
    pub fn unwrap_or(self, default: T) -> T {
        self.value.unwrap_or(default)
    }
}


impl<T, E> ResultExt<T, E> {
    pub fn unwrap_or_else(self, f: impl FnOnce(E) -> T) -> T {
        self.value.unwrap_or_else(f)
    }
}





#[macro_export]
macro_rules! einsteindb_macro {
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
}

#[macro_export]
macro_rules! einsteindb_macro_impl {
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
    ($($tokens:tt)*) => {
        $crate::einsteindb_macro_impl!($($tokens)*)
    };
}

use super::*;
use crate::error::{Error, Result};
use crate::parser::{Parser, ParserError};
use crate::value::{Value, ValueType};
use crate::{ValueRef, ValueRefMut};
use itertools::Itertools;
use pretty;
use std::{
    collections::HashMap,
    fmt::{self, Display},
    io,
    convert::{TryFrom, TryInto},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},

};

use einstein_db::{
    einsteindb_macro_impl,
    einsteindb_macro,
};

use causetq::{
    self,
    Txn,
    TxnRead,
    TxnWrite,
    TxnReadWrite,
    TxnReadWriteMut,
    TxnReadWriteMutExt,
    TxnReadExt,
    TxnWriteExt,
   
};

use causets::*;
use einstein_ml::*;


/// Namespaced options.
/// A `NamespacedOptions` is a wrapper around a `HashMap` that allows to store
/// multiple values under the same key.
/// The `NamespacedOptions` is used to store the options for a specific
/// namespace. Using a Unified Key for the same option will result in the
/// value being stored under the same key. Formally, the following is true:
/// ```text
/// let mut options = NamespacedOptions::new();
/// options.set("key", "value");
/// options.set("key", "value2");
/// assert_eq!(options.get("key"), Some("value2")); 
/// ```
/// The `NamespacedOptions` is also used to store the options for the default
/// namespace. This is the namespace that is used when no namespace is specified.
/// The default namespace is always present.






pub (crate) struct NamespacedOptions {
    options: HashMap<String, Value>, //TODO: use a better data structure
}



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AppendLogPanicFdbOptions {

    pub(crate) inner: fdb_lsh_treesoliton_panic_merkle_tree::DBOptions,
}


impl AppendLogPanicFdbOptions {
    pub fn new() -> Self {
        Self {
            inner: fdb_lsh_treesoliton_panic_merkle_tree::DBOptions::new(),
        }
    }
}


impl Default for AppendLogPanicFdbOptions {
    fn default() -> Self {
        Self::new()
    }
}




impl Deref for AppendLogPanicFdbOptions {
    type Target = fdb_lsh_treesoliton_panic_merkle_tree::DBOptions;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}


impl NamespacedOptions {
    /// Creates a new `NamespacedOptions`.
    /// The `NamespacedOptions` is used to store the options for a specific
    /// namespace. Using a Unified Key for the same option will result in the
    /// value being stored under the same key. Formally, the following is true:

    pub fn new() -> Self {
        Self {
            options: HashMap::new(),
        }
    }
    /// Creates a new `NamespacedOptions` instance with the given options.
       /// The `NamespacedOptions` is used to store the options for a specific
    /// namespace. Using a Unified Key for the same option will result in the
    ///

    pub fn with_options(options: HashMap<String, Value>) -> Self {
        Self {
            options,
        }
    }

    /// Sets the value for the given key.
    /// If the key already exists, the value will be overwritten.
    /// If the key does not exist, the key-value pair will be added.
    /// The key is a `String` and the value can be any type that can be
    ///

    pub fn set(&mut self, key: impl Into<String>, value: impl Into<Value>) {
        self.options.insert(key.into(), value.into());
    }

    /// Creates a new `NamespacedOptions` instance with the given options.
    pub fn with_options_arc(options: Arc<Mutex<HashMap<String, Value>>>) -> Self {
        Self {
            options: options.lock().unwrap().clone(),
        }
    }
    /// Creates a new `NamespacedOptions` instance with the given options.
    pub fn with_options_arc_ref(options: Arc<Mutex<HashMap<String, Value>>>) -> Self {
        Self {
            options: options.lock().unwrap().clone(),
        }
    }

    /// Creates a new `NamespacedOptions` instance with the given options.
    pub fn with_options_arc_ref_mut(options: &Arc<Mutex<HashMap<String, Value>>>) -> Self {
        Self {
            options: options.lock().unwrap().clone(),
        }
    }

    /// Creates a new `NamespacedOptions` instance with the given options.
    pub fn with_options_arc_mut(options: Arc<Mutex<HashMap<String, Value>>>) -> Self {
        Self {
            options: options.lock().unwrap().clone(),
        }
    }

}


impl Deref for NamespacedOptions {
    type Target = HashMap<String, Value>;

    fn deref(&self) -> &Self::Target {
        &self.inner.lock().unwrap()
    }
}


impl DerefMut for NamespacedOptions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner.lock().unwrap()
    }
}






impl NamespacedCausets for soliton_panic_merkle_tree {
    type ColumnFamilyOptions = PanicColumnFamilyOptions;

    fn get_options_namespaced(&self, namespaced: &str) -> Result<Self::ColumnFamilyOptions> {
        panic!()
    }
    fn set_options_namespaced(&self, namespaced: &str, options: &[(&str, &str)]) -> Result<()> {
        panic!()
    }
}

pub struct PanicColumnFamilyOptions;

impl ColumnFamilyOptions for PanicColumnFamilyOptions {
    type FoundationDBDBOptions = append_log_panic_fdb
    ;

    fn new() -> Self {
        panic!()
    }
    fn get_max_write_buffer_number(&self) -> u32 {
        panic!()
    }
    fn get_l_naught_zero_slowdown_writes_trigger(&self) -> u32 {
        panic!()
    }
    fn get_l_naught_zero_stop_writes_trigger(&self) -> u32 {
        panic!()
    }
    fn set_l_naught_zero_file_num_jet_bundle_trigger(&mut self, v: i32) {
        panic!()
    }
    fn get_soft_pending_jet_bundle_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_hard_pending_jet_bundle_bytes_limit(&self) -> u64 {
        panic!()
    }
    fn get_block_cache_capacity(&self) -> u64 {
        panic!()
    }
    fn set_block_cache_capacity(&self, capacity: u64) -> std::result::Result<(), String> {
        panic!()
    }
    fn set_foundation_dbdb_options(&mut self, opts: &Self::FoundationDBDBOptions) {
        panic!()
    }
    fn get_target_file_size_base(&self) -> u64 {
        panic!()
    }
    fn set_disable_auto_jet_bundles(&mut self, v: bool) {
        panic!()
    }
    fn get_disable_auto_jet_bundles(&self) -> bool {
        panic!()
    }
    fn get_disable_write_stall(&self) -> bool {
        panic!()
    }
    fn set_causet_partitioner_factory<F: CausetPartitionerFactory>(&mut self, factory: F) {
        panic!()
    }
}


impl Deref for PanicColumnFamilyOptions {
    type Target = append_log_panic_fdb;

    fn deref(&self) -> &Self::Target {
        panic!()
    }
}


impl DerefMut for PanicColumnFamilyOptions {
    fn deref_mut(&mut self) -> &mut Self::Target {
        panic!()
    }
}


impl Deref for append_log_panic_fdb {
    type Target = fdb_lsh_treesoliton_panic_merkle_tree::DBOptions;

    fn deref(&self) -> &Self::Target {
        panic!()
    }
}


impl DerefMut for append_log_panic_fdb {
    fn deref_mut(&mut self) -> &mut Self::Target {
        panic!()
    }
}


