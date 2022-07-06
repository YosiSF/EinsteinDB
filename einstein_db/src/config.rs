/// Copyright (c) 2018-2023, WHTCORPS INC. All Rights Reserved.
/// @author WHITFORD LEDER
/// @date 2020-03-23
/// 




// -----------------------------------------------------------------------------
//! # EinsteinDB
//! ----------------------------------------------------------------
//!
//!  ## License
//!
//! Licensed under the Apache License, Version 2.0 (the "License");
//! you may not use this file except in compliance with the License.
//! You may obtain a copy of the License at
//!
//! http://www.apache.org/licenses/LICENSE-2.0
//!
//! Unless required by applicable law or agreed to in writing, software
//! distributed under the License is distributed on an "AS IS" BASIS,
//! WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//! See the License for the specific language governing permission.
//!






// #[derive(Debug, Fail)]
// pub enum Error {
//     #[fail(display = "{}", _0)]
//     Causet(String),
//     #[fail(display = "{}", _0)]
//     CausetQ(String),
//     #[fail(display = "{}", _0)]
//     EinsteinML(String),
//}
use std::cmp;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::i32;
use std::io::Write;
use std::io::{Error as IoError, ErrorKind};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::usize;
use std::vec::Vec;
use std::{fmt, io};


use crate::einsteindb::{Einsteindb, EinsteindbOptions};
use crate::einsteindb::{EinsteindbIterator, EinsteindbIteratorOptions};

use crate::einsteindb::{EinsteindbIteratorItem, EinsteindbIteratorItemOptions};
use crate::einsteindb::{EinsteindbIteratorItemType, EinsteindbIteratorItemTypeOptions};





//Block cache for FoundationDB whose size will be set to 45% of system memory
pub const HYPERCAUSET_CACHE_RATE: f64 = 0.45;   
//BY default, EinsteinDB will try to limit memory usage to 75% of sys mem.__rust_force_expr!
const HYPERINTERLOCKED_CAUSETS: () =_MIN_MEM: usize = 256 * MIB as usize;
const HYPERINTERLOCKED_CAUSETS_MAX_MEM: usize = 1024 * GIB as usize;
const LAST_CONFIG_FILE: &str = "last_einsteindb.toml";
const TMP_CONFIG_FILE: &str = "tmp_einsteindb.toml";
const MAX_HYPERCAUSET_SIZE: usize = 1024 * GIB as usize;
const MAX_HYPERINTERLOCKED_CAUSETS_SIZE: usize = 1024 * GIB as usize;


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperCausetConfig {
    pub cache_rate: f64,
    pub max_cache_size: usize,
    pub max_cache_num: usize,
    pub max_cache_num_per_db: usize,
    pub max_cache_num_per_db_per_thread: usize,
    pub max_cache_num_per_db_per_thread_per_table: usize,
}


fn get_hypercauset_config() -> Result<HyperCausetConfig, ConfigError> {
    let mut config = Config::new();
    config.set_default("cache_rate", HYPERCAUSET_CACHE_RATE);
    config.set_default("max_cache_size", MAX_HYPERCAUSET_SIZE);
    config.set_default("max_cache_num", usize::MAX);
    config.set_default("max_cache_num_per_db", usize::MAX);
    config.set_default("max_cache_num_per_db_per_thread", usize::MAX);

    let mut limit = (sys_mem as f64 * HYPERCAUSET_CACHE_RATE) as usize;


    if limit > MAX_HYPERCAUSET_SIZE {
        limit = MAX_HYPERCAUSET_SIZE;
    }

    config.set_default("max_cache_size", limit);

    let mut hypercauset_config = HyperCausetConfig::default();
    config.try_into(&mut hypercauset_config)?;
    Ok(hypercauset_config)
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HyperInterlockedCausetsConfig {
    pub max_mem: usize,
    pub max_mem_per_db: usize,
    pub max_mem_per_db_per_thread: usize,
    pub max_mem_per_db_per_thread_per_table: usize,

}


fn get_hyperinterlocked_causets_config() -> Result<HyperInterlockedCausetsConfig, ConfigError> {
        limit = MAX_HYPERCAUSET_SIZE;
        max_cache_num = usize::MAX;
        max_cache_num_per_db = usize::MAX;
        max_cache_num_per_db_per_thread = usize::MAX;
        max_cache_num_per_db_per_thread_per_table = usize::MAX;


    let mut config = Config::new();
    config.set_default("max_mem", HYPERINTERLOCKED_CAUSETS);
    config.set_default("max_mem_per_db", HYPERINTERLOCKED_CAUSETS_MAX_MEM);
    config.set_default("max_mem_per_db_per_thread", HYPERINTERLOCKED_CAUSETS_MAX_MEM);
    config.set_default("max_mem_per_db_per_thread_per_table", HYPERINTERLOCKED_CAUSETS_MAX_MEM);

    let mut hyperinterlocked_causets_config = HyperInterlockedCausetsConfig::default();
    config.try_into(&mut hyperinterlocked_causets_config)?;
    Ok(hyperinterlocked_causets_config)
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolverConfig {
    pub max_mem: usize,
    pub cache_rate: f64,
    pub max_cache_size: usize,

    pub max_cache_num: usize,

    async_thread_num: usize,

    pub max_cache_num_per_db: usize,

}


fn get_solver_config() -> Result<SolverConfig, ConfigError> {
    let (ratio, min, max) = match (
        hypercauset_config.cache_rate,
        hypercauset_config.max_cache_size,
        hypercauset_config.max_cache_num,
        config::get_config_value_as_f64(&config, "hypercauset_cache_rate"),
        config::get_config_value_as_usize(&config, "hypercauset_cache_min"),
        config::get_config_value_as_usize(&config, "hypercauset_cache_max"),
    ) {
        (Some(ratio), Some(min), Some(max)) => (ratio, min, max),
        (Some(ratio), Some(min), None) => (ratio, min, min * ratio),
        (Some(ratio), None, Some(max)) => (ratio, max / ratio, max),
        _ => (HYPERCAUSET_CACHE_RATE, HYPERINTERLOCKED_CAUSETS, HYPERINTERLOCKED_CAUSETS_MAX_MEM),
    };
    let limit = (sys_mem as f64 * ratio) as usize;
    if limit < min {
        limit = min;
    }
    if limit > max {
        limit = max;
    }
    let mut config = Config::new();
    config.set_default("max_mem", limit);
    config.set_default("cache_rate", ratio);
    config.set_default("max_cache_size", max);
    config.set_default("max_cache_num", usize::MAX);

    config.set_default("max_cache_num_per_db", usize::MAX);
    config.set_default("async_thread_num", 1);

    let mut solver_config = SolverConfig::default();
    config.try_into(&mut solver_config)?;
}



    
