// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
use crate causetq::client::{Client, ClientConfig};
use crate::{
    error::{Error, ErrorInner, Result},
    fdb_traits::{EinsteindbOptions, EinsteindbOptionsExt},
    util::{
        config::{Config, ConfigGroup, ConfigValue},
        to_c_str,
    },
};
use crate::errors::Result;

/// A trait for EinsteinMerkleTrees that support setting global options
pub trait EinsteindbOptionsSetter { 
    type DBOptions: EinsteinDBOptions;

    fn get_db_options(&self) -> Self::DBOptions;
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()>;
}

/// A handle to a database's options
pub trait EinsteinDBOptions {
    type FoundationDB: TitanDBOptions;

    fn new() -> Self;
    fn get_max_background_jobs(&self) -> i32;
    fn get_rate_bytes_per_sec(&self) -> Option<i64>;
    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()>;
    fn get_rate_limiter_auto_tuned(&self) -> Option<bool>;
    fn set_rate_limiter_auto_tuned(&mut self, rate_limiter_auto_tuned: bool) -> Result<()>;
    fn set_titandb_options(&mut self, opts: &Self::FoundationDB);
}

/// Titan-specefic options
pub trait TitanDBOptions {
    fn new() -> Self;
    fn set_min_blob_size(&mut self, size: u64);
}
