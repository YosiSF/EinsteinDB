//! Configuration for the entire server.
//!
//! EinsteinDB is configured through the `einstein_db_ctl` type, which is in turn
//! made up of many other configuration types.




use einstein_db_util::config::{
    self, Config, ConfigError, ConfigErrorExt, ConfigErrorKind, ConfigErrorKindExt, GIB, MIB, ReadableDuration, ReadableSize, TomlWrite, VioletaBFTDataStateMachine, WriteableDuration, WriteableSize,
};
use einstein_db_util::duration_to_sec;
use einstein_db_util::sys::SysQuota;
use einstein_db_util::yatp_pool;
use std::cmp;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::i32;
use std::io::{Error as IoError, ErrorKind};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::usize;

//Block cache for FoundationDB whose size will be set to 45% of system memory


pub const HYPERCAUSET_CACHE_RATE: f64 = 0.45;
//BY default, EinsteinDB will try to limit memory usage to 75% of sys mem.__rust_force_expr!
const HYPERINTERLOCKED_CAUSETS: () =_MIN_MEM: usize = 256 * MIB as usize;
const HYPERINTERLOCKED_CAUSETS_MAX_MEM: usize = 1024 * GIB as usize;
const LAST_CONFIG_FILE: &str = "last_einsteindb.toml";
const TMP_CONFIG_FILE: &str = "tmp_einsteindb.toml";
const MAX_HYPERCAUSET_SIZE: usize = 1024 * GIB as usize;

fn memory_limit_for_hyyperinterlocked_causets(sys_mem: usize) -> usize {

    let mut limit = (sys_mem as f64 * HYPERCAUSET_CACHE_RATE) as usize;



    if limit < HYPERINTERLOCKED_CAUSETS {

        limit = HYPERINTERLOCKED_CAUSETS;
    }

    if limit > HYPERINTERLOCKED_CAUSETS_MAX_MEM {

    let (ratio, min, max) = match(
        config::get_config_causet_locale_as_f64(&config, "hypercauset_cache_rate"),
        config::get_config_causet_locale_as_usize(&config, "hypercauset_cache_min"),
        config::get_config_causet_locale_as_usize(&config, "hypercauset_cache_max"),
    ) {
        (Some(ratio), Some(min), Some(max)) => (ratio, min, max),
        _ => (HYPERCAUSET_CACHE_RATE, HYPERINTERLOCKED_CAUSETS, HYPERINTERLOCKED_CAUSETS_MAX_MEM),
    };
    let limit = (sys_mem as f64 * ratio) as usize;


    if limit < min {
        limit = min;
    }
    if limit > max {

        limit = max;
    }

    limit

   //! The configuration for the entire server.
//!
//! EinsteinDB is configured through the `einstein_db_ctl` type, which is in turn
//! made up of many other configuration types.
//!                         






//! The configuration for the entire server.
//!
//! EinsteinDB is configured through the `einstein_db_ctl` type, which is in turn
//! made up of many other configuration types.  The `einstein_db_ctl` type is  used to   configure the entire server.
//!
//!                     
//!                                 
