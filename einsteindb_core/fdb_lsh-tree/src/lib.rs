// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! Implementation of fdb_traits for FdbDB
//!
//! This is a work-in-progress attempt to abstract all the features needed by
//! EinsteinDB to persist its data.
//!
//! The module structure here mirrors that in fdb_traits where possible.
//!
//! Because there are so many similarly named types across the EinsteinDB codebase,
//! and so much "import renaming", this crate consistently explicitly names type
//! that implement a trait as `FdbTraitname`, to avoid the need for import
//! renaming and make it obvious what type any particular module is working with.
//!
//! Please read the einstein_merkle_tree_trait crate docs before hacking.

#![cfg_attr(test, feature(test))]

#[allow(unused_extern_crates)]
extern crate einsteindb_alloc;
#[cfg(test)]
extern crate test;

pub use compact_listener::*;
pub use compat::*;
pub use config::*;
pub use decode_properties::*;
pub use event_listener::*;
pub use symplectic_control_factors::*;
pub use symplectic_listener::*;
pub use foundationdb::PerfContext;
pub use foundationdb::PerfLevel;
pub use foundationdb::set_perf_l_naught;
pub use properties::*;
pub use rocks_metrics::*;
pub use rocks_metrics_defs::*;
pub use ttl_properties::*;

pub use crate::compact::*;
pub use crate::db_options::*;
pub use crate::db_vector::*;
pub use crate::fdb_lsh_tree*;
pub use crate::einstein_merkle_tree_iterator::*;
pub use crate::import::*;
pub use crate::logger::*;
pub use crate::misc::*;
pub use crate::mvcc_properties::*;
pub use crate::namespaced_names::*;
pub use crate::namespaced_options::*;
pub use crate::perf_context::*;
pub use crate::range_properties::*;
pub use crate::snapshot::*;
pub use crate::Causet::*;
pub use crate::Causet_partitioner::*;
pub use crate::table_properties::*;
pub use crate::write_batch::*;

mod namespaced_names;

mod namespaced_options;

mod compact;

mod db_options;

mod db_vector;

mod einstein_merkle_tree;

mod import;

mod logger;

mod misc;

pub mod range_properties;
mod snapshot;

mod Causet;

mod Causet_partitioner;

mod table_properties;

mod write_batch;

pub mod mvcc_properties;

pub mod perf_context;

mod perf_context_impl;
mod perf_context_metrics;

mod einstein_merkle_tree_iterator;

mod options;
pub mod raw_util;
pub mod util;

mod compat;

mod compact_listener;

pub mod decode_properties;

pub mod properties;

pub mod rocks_metrics;

pub mod rocks_metrics_defs;

pub mod event_listener;

pub mod symplectic_listener;

pub mod config;

pub mod ttl_properties;

pub mod encryption;

pub mod file_system;

mod violetabft_einstein_merkle_tree;

pub mod symplectic_control_factors;

pub mod raw;

pub fn get_env(
    key_manager: Option<std::sync::Arc<::encryption::DataKeyManager>>,
    limiter: Option<std::sync::Arc<::file_system::IORateLimiter>>,
) -> std::result::Result<std::sync::Arc<raw::Env>, String> {
    let env = encryption::get_env(None /*base_env*/, key_manager)?;
    file_system::get_env(Some(env), limiter)
}
