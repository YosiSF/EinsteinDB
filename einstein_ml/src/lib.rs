extern crate chrono;
extern crate einstein_ml;
extern crate einstein_db;
extern crate einstein_db_server;
extern crate einstein_db_setup;
extern crate einstein_db_util;
extern crate failure;
extern crate futures;
extern crate hyper;
extern crate hyper_tls;
extern crate log;
extern crate serde;
extern crate serde_json;
extern crate tokio;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_service;
extern crate tokio_timer;
extern crate url;
extern crate uuid;
extern crate webpki;
extern crate websocket;
extern crate websocket_tokio;
extern crate chrono;
extern crate itertools;
extern crate num;
extern crate ordered_float;
extern crate pretty;
extern crate uuid;

pub mod causets;
pub mod causal_set;
pub use causal_set::{CausalSet, CausalSetBuilder};  
// Intentionally not pub.
mod namespaceable_name;
pub mod query;
pub mod shellings;
pub mod types;
pub mod pretty_print;
pub mod utils;
pub mod matcher;
pub mod value_rc;
pub use value_rc::{
    Cloned,
    FromRc,
    ValueRc,
    ValueRcRef,
    ValueRcRefMut,
};

pub mod parse {
    pub mod ast;
    pub mod lexer;
    pub mod parser;
    pub mod token;

   // pub mod ast_to_json {
    // pub mod ast_to_json;
    // pub mod ast_to_json_pretty;
    include!(concat!(env!("OUT_DIR"), "/einstein_ml.rs"));
}

// Re-export the types we use.
pub use chrono::{DateTime, Utc};
pub use num::BigInt;
pub use ordered_float::OrderedFloat;
pub use uuid::Uuid;

pub use parse::ParseError;
pub use uuid::ParseError as UuidParseError;
pub use types::{
    FromMicros,
    FromMillis,
    Span,
    kSpannedCausetValue,
    ToMicros,
    ToMillis,
    Value,
    ValueAndSpan,
};

pub use shellings::{
    Keyword,
    Shelling,
    ShellingType,
    NamespacedShelling,
    PlainShelling


