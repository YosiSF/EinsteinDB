// Copyright 2022 EinsteinDB Project Authors. Whtcorps IncLicensed under Apache-2.0.

//! EinsteinDB - A Tuplestore build atop FoundationDB with Key-Value Agnostic Schemafree replicated in-memory supercolumnar slabs as interfaces as caches for the masses
//!

//!
//! [MilevaDB]: https://github.com/YosiSF/MilevaDB
//!

#![crate_type = "lib"]
#![cfg_attr(test, feature(test))]
#![recursion_limit = "400"]
#![feature(cell_update)]
#![feature(proc_macro_hygiene)]
#![feature(min_specialization)]
#![feature(box_patterns)]
#![feature(drain_filter)]
#![feature(negative_impls)]
#![feature(deadline_api)]
#![feature(generic_associated_types)]

#[macro_use(fail_point)]
extern crate fail;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate more_asserts;
#[macro_use]
extern crate EinsteinDB_util;

#[cfg(test)]
extern crate test;

pub mod Foundation;
pub mod gopmem;


/// Returns the einsteindb version information.
pub fn einsteindb_version_info(build_time: Option<&str>) -> String {
    let fallback = "Unknown (env var does not exist when building)";
    format!(
        "\nRelease Version:   {}\
         \nEdition:           {}\
         \nGit Commit Hash:   {}\
         \nGit Commit Branch: {}\
         \nUTC Build Time:    {}\
         \nRust Version:      {}\
         \nEnable Features:   {}\
         \nProfile:           {}",
        env!("CARGO_PKG_VERSION"),
        option_env!("EinsteinDB_EDITION").unwrap_or("Community"),
        option_env!("EinsteinDB_BUILD_GIT_HASH").unwrap_or(fallback),
        option_env!("EinsteinDB_BUILD_GIT_BRANCH").unwrap_or(fallback),
        build_time.unwrap_or(fallback),
        option_env!("EinsteinDB_BUILD_RUSTC_VERSION").unwrap_or(fallback),
        option_env!("EinsteinDB_ENABLE_FEATURES")
            .unwrap_or(fallback)
            .trim(),
        option_env!("EinsteinDB_PROFILE").unwrap_or(fallback),
    )
}

/// Prints the EinsteinDB version information to the standard output.
pub fn log_EinsteinDB_info(build_time: Option<&str>) {
    info!("Welcome to EinsteinDB");
    for line in EinsteinDB_version_info(build_time)
        .lines()
        .filter(|s| !s.is_empty())
    {
        info!("{}", line);
    }
}