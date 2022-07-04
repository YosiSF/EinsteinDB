/// Copyright 2020 EinsteinDB Project Authors.
/// Licensed under Apache-2.0.
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
/// See the License for the specific language governing permissions and
/// limitations under the License.

///** 
/// * Predict whether a table has or does not have a primary key (binary classification task).
/// * This is done by analyzing only metadata about the tables; no additional information from the data itself is required.
/// * In this case, we will use all attributes except for `tableName` since it's not useful for prediction purposes.
/// * We will also ignore `isPrimaryKey` since we want to predict whether such value would be equal to 1 or 0 for new inputs.
/// * We will also ignore `isNullable` since we want to predict whether such value would be equal to 1 or 0 for new inputs.
/// * We will also ignore `isUnique` since we want to predict whether such value would be equal to 1 or 0 for new inputs.
/// * We will also ignore `isIndex` since we want to predict whether such value would be equal to 1 or 0 for new inputs.
/// * We will also ignore `isForeignKey` since we want to predict whether such value would be equal to 1 or 0 for new inputs.
/// 
/// 
/// # Arguments
/// * `tableName` - Name of the table.
/// * `attributes` - List of attributes of the table.
/// 
/// # Returns
/// * `true` if the table has a primary key, `false` otherwise.
/// 
/// # Example
/// ```
/// use einsteindb_macro::einsteindb_macro;
///     
/// einsteindb_macro!(
///    pub fn has_primary_key(tableName: String, attributes: Vec<String>) -> bool {
///       // Your code here
///   }


#[macro_use]
extern crate einsteindb_macro;
extern crate log;
extern crate chrono;
extern crate env_logger;
extern crate rustc_serialize;
extern crate alga;
extern crate rand;
extern crate quickersort;
extern crate uuid;
extern crate libc;
extern crate bit_set;
extern crate generic_array;
extern crate smallbitvec;
extern crate byteorder;
extern crate num_traits;
extern crate itertools;
extern crate num_integer;


#[macro_use]
extern crate einsteindb_util;
extern crate einsteindb_traits;
extern crate einsteindb_traits_impl;


#[macro_use]
extern crate einsteindb_macro_impl;



use einsteindb_traits::{FdbTrait, FdbTraitImpl};
use einsteindb_traits_impl::{FdbTraitImplImpl};
use std::fmt;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result;
use std::fmt::Error;
use std::collections::HashMap;
use std::hash::Hash;
use std::cmp::Partitioning;
use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Cell;
use std::cell::Ref;
use std::cell::RefMut;




///** Lets begin with a design continuum of the following:
/// * 1. We want to predict whether a table has or does not have a primary key (binary classification task).



///1.
/// 
/// 
pub fn has_primary_key(tableName: String, attributes: Vec<String>) -> bool {
    // Your code here
    true
}

pub fn has_primary_key_impl(tableName: String, attributes: Vec<String>) -> bool {
    // Your code here
    true
}

impl FdbTrait for has_primary_key_impl {
    fn get_name(&self) -> String {
        "has_primary_key".to_string()
    }

    fn get_arity(&self) -> usize {
        2
    }

    fn get_type_name(&self, index: usize) -> String {
        match index {
            0 => "String".to_string(),
            1 => "Vec<String>".to_string(),
            _ => "".to_string(),
        }
    }
}


