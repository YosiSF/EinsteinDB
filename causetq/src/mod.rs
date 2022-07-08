//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


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


//! Builder utilities for making type representations. Currently only includes
//! `FieldTypeBuilder` for building the `FieldType` protobuf message.

mod field_type;



pub use self::field_type::FieldTypeBuilder;

//gremlin queries for causetq

mod ctx;
mod dedup; // a deduping function for gremlin queries
//einsteinml lisp
crate use self::ctx::{Context, ContextBuilder};
crate use self::dedup::dedup;


/// A macro for defining a `Result` with a custom error type.
/// This is similar to the `?` operator in Rust, but it allows you to define a
/// custom error type.
/// This macro is borrowed from the `failure` crate.
/// See [`failure`](https://crates.io/crates/failure) for more information.

#[macro_export]
macro_rules! result {
    ($expr:expr, $err:ty) => (
        match $expr {
            Ok(val) => Ok(val),
            Err(err) => Err(From::from(err)),
        }
    );
    ($expr:expr) => (
        match $expr {
            Ok(val) => Ok(val),
            Err(err) => Err(From::from(err)),
        }
    );
}


/// A macro for defining a `Result` with a custom error type.
/// This is similar to the `?` operator in Rust, but it allows you to define a
///


/// A macro for defining a `Result` with a custom error type.
#[macro_export]
macro_rules! result_err {
    ($expr:expr, $err:ty) => (
        match $expr {
            Ok(val) => Ok(val),
            Err(err) => Err(From::from(err)),
        }
    );
    ($expr:expr) => (
        match $expr {
            Ok(val) => Ok(val),
            Err(err) => Err(From::from(err)),
        }
    );
    ($expr:expr) => (
        match $expr {
            Ok(val) => Ok(val),
            Err(err) => Err(From::from(err)),
        }
    );
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





