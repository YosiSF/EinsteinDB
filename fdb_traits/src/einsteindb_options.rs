// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;
use std::ops::Deref;
use std::sync::Arc;
use std::time::Duration;

pub use self::config::Config;
pub use self::config::ConfigEntry;
pub use self::config::ConfigValue;
pub use self::config::ConfigValueType;

pub use allegro_poset::poset::poset::PosetConfig;
pub use allegro_poset::poset::poset::PosetConfigEntry;


use super::*;
use crate::soliton::*;


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
pub trait EinsteinOptionsSetter { 
    type DBOptions: EinsteinDBOptions;

    fn get_db_options(&self) -> Self::DBOptions;
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()>;
}

/// A handle to a database's options
pub trait EinsteinDBOptions {
    type FoundationDB: Client;

    fn new() -> Self;
    fn get_max_background_jobs(&self) -> i32;
    fn get_rate_bytes_per_sec(&self) -> Option<i64>;
    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()>;
    fn get_rate_limiter_auto_tuned(&self) -> Option<bool>;
    fn set_rate_limiter_auto_tuned(&mut self, rate_limiter_auto_tuned: bool) -> Result<()>;
    fn set_FoundationDBdb_options(&mut self, opts: &Self::FoundationDB);
}