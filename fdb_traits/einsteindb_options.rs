// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

pub use allegro_poset::poset::poset::PosetConfig;
pub use allegro_poset::poset::poset::PosetConfigEntry;
use std::{
    cmp,
    fmt,
    hash,
    marker::PhantomData,
    mem,
    ptr,
    slice,
};
use std::borrow::Cow;
use std::collections::{
    BTreeMap,
    BTreeSet,
};
use std::collections::HashMap;
use std::fmt::{
    Display,
    Formatter,
    Result as FmtResult,
};
use std::fmt;
use std::iter::FromIterator;
use std::ops::Deref;
use std::ops::Deref;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Arc;
use std::time::Duration;

use crate::{
    error::{Error, ErrorInner, Result},
    fdb_traits::{EinsteindbOptions, EinsteindbOptionsExt},
    util::{
        config::{Config, ConfigGroup, ConfigValue},
        to_c_str,
    },
};
use crate::errors::Result;
use crate::soliton::*;

use super::*;

pub use self::config::Config;
pub use self::config::ConfigEntry;
pub use self::config::ConfigValue;
pub use self::config::ConfigValueType;

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
    fn set_foundation_dbdb_options(&mut self, opts: &Self::FoundationDB);
}


/// A handle to a database's options
pub struct EinsteinDBOptionsImpl {
    pub max_background_jobs: i32,
    pub rate_bytes_per_sec: Option<i64>,
    pub rate_limiter_auto_tuned: Option<bool>,
    pub foundation_dbdb_options: Option<Arc<FoundationDB>>,
}

/// Now we can implement the `EinsteinDBOptions` trait for `EinsteinDBOptionsImpl`.
/// This is where we implement the actual options.

//interlock_guard_mutex_lock_timeout_ms: i32
//interlock_guard_mutex_lock_timeout_ms: i32
//interlock_guard_mutex_lock_timeout_ms: i32


pub struct EinsteinDBOptionsImplBuilder {
    pub max_background_jobs: i32,
    pub rate_bytes_per_sec: Option<i64>,
    pub rate_limiter_auto_tuned: Option<bool>,
    pub foundation_dbdb_options: Option<Arc<FoundationDB>>,
}
