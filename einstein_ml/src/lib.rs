extern crate chrono;
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
mod isolated_namespace;
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


