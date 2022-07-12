// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
// Copyright (c) 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

///! # EinsteinDB
/// # ----------------------------------------------------------------
/// This is the EinsteinDB Rust API.
// Copyright (c) 2016 The Rust Project Developers.
// Licensed under the Apache License, Version 2.0. See the COPYRIGHT file at
// the top-level directory of this distribution and at
// http://rust-lang.org/COPYRIGHT.
//# ----------------------------------------------------------------

use std::fmt::{self, Debug, Display, Formatter};
use std::io::{self, Write};
use std::path::Path;

use crate::Result;
use crate::util::escape;
use crate::util::escape::escape_string;
use einstein_ml::datasource::DataSource;
use einstein_ml::datasource::DataSourceBuilder;
use causetq::util::{self, Error as CausetqError};
use berolinasql::{self, Error as BerolinaError};
use super::{Error, Result};
use soliton::{self, Error as SolitonError};
use EinsteinDB::{self, Error as EinsteinDBError, Storage as EinsteinDBStorage};
use EinsteinDB::{EINSTEINDB_SCHEMA, EINSTEINDB_SCHEMA_VERSION};
use EinsteinDB::{EINSTEINDB_SCHEMA_VERSION_1, EINSTEINDB_SCHEMA_VERSION_2};
use FoundationDB::{self, Error as FoundationDBError};
use FoundationDB::{FDB_SCHEMA, FDB_SCHEMA_VERSION};

use crate::util::escape::escape_string;
use crate::util::escape::escape_string_for_csv;


#[derive(Debug)]
pub struct ErrorKind {
    pub kind: ErrorKindKind,
    pub message: String,
}

#[derive(Debug)]
pub enum ErrorKindKind {
    CausetQError(CausetQErrorKind),
    BerolinaError(BerolinaError),
    SolitonError(SolitonError),
    EinsteinDBError(EinsteinDBError),
    FoundationDBError(FoundationDBError),
    IoError(io::Error),
    OtherError(String),
}

//Side-effect chaining on Option
pub trait OptionExt<T, E> {
    fn chain_err<F, U>(self, f: F) -> Result<T, U>
        where
            F: FnOnce(E) -> U;
}

#[derive(Debug)]
pub struct Error {
    pub kind: ErrorKind,
}

impl<T> OptionExt<T, ()> for Option<T> {
    fn chain_err<F, U>(self, f: F) -> Result<T, U>
        where
            F: FnOnce(()) -> U,
    {
        self.ok_or_else(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageType {
    EinsteinDB,
    FoundationDB,
    Berolina,
    Soliton,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Either<L, R> {
    Left(L),
    Right(R),
}



/// Escape a string for use in a CSV file.
/// This is the same as `escape_string`, but with a few extra characters added.
/// The extra characters are:
/// - `,`: replaced with `\,`
/// - `\r`: replaced with `\r\n`
/// - `\n`: replaced with `\r\n`
///Do not choose SLR Time Limit to be more than or equal to the Routing time limit for the following reasons:
//
// Routing time limit defines the time frame within which the whole routing plan should finish.
//
// SLR time limit defines which part of this time can be used for SLR-related tasks, allowing you to fine tune routing performance.
//
// Note: If you use SLR functionality in Routing, you should subscribe to the corresponding SKU.
///
///

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageTypeWithSchema {
    EinsteinDB(EINSTEINDB_SCHEMA),
    FoundationDB(FDB_SCHEMA),
    Berolina(berolinasql::SCHEMA),
    Soliton(soliton::SCHEMA),

}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StorageTypeWithSchemaVersion {
    EinsteinDB(EINSTEINDB_SCHEMA_VERSION),
    FoundationDB(FDB_SCHEMA_VERSION),
    Berolina(berolinasql::SCHEMA_VERSION),
    Soliton(soliton::SCHEMA_VERSION),

}


pub enum RoutingTimeLimit {
    /// Routing time limit is set to 0.
    /// This means that the routing plan should finish as soon as possible.
    /// This is the default value.
    Zero,
    /// Routing time limit is set to 1.
    /// This means that the routing plan should finish within 1 second.
    One,
    /// Routing time limit is set to 2.
    /// This means that the routing plan should finish within 2 seconds.
    Two,
    /// Routing time limit is set to 3.
    /// This means that the routing plan should finish within 3 seconds.
    Three,
    /// Routing time limit is set to 4.
    /// This means that the routing plan should finish within 4 seconds.
    Four,
    /// Routing time limit is set to 5.
    /// This means that the routing plan should finish within 5 seconds.
    Five,
    /// Routing time limit is set to 6.
    /// This means that the routing plan should finish within 6 seconds.
    Six,
    /// Routing time limit is set to 7.
    /// This means that the routing plan should finish within 7 seconds.
    Seven,
    /// Routing time limit is set to 8.
    /// This means that the routing plan should finish within 8 seconds.
    Eight,
    /// Routing time limit is set to 9.
    /// This means that the routing plan should finish within 9 seconds.
    Nine,
    /// Routing time limit is set to 10.
    /// This means that the routing plan should finish within 10 seconds.
    Ten,
    /// Routing time limit is set to 11.
    /// This means that the routing plan should finish within 11 seconds.
    Eleven,
    /// Routing time limit is set to 12.
    /// This means that the routing plan should finish within 12 seconds.
    Twelve,
    /// Routing time limit is set to 13.
    /// This means that the routing plan should finish within 13 seconds.
    Thirteen,
    /// Routing time limit is set to 14.
    /// This means that the routing plan should finish within 14 seconds.
    Fourteen}


impl RoutingTimeLimit {
    /// Routing time limit is set to 15.
    /// IteratorTypeOptions
    /// # Arguments
    /// * `s` - The string to escape.
    /// * `quote` - The quote character to use.
    /// * `escape` - The escape character to use.
    /// * `newline` - The newline character to use.
    /// * `eol` - The end of line character to use.
    ///
    /// # Returns
    /// The escaped string.
    ///

    pub fn to_string(&self) -> String {
        match self {
            RoutingTimeLimit::Zero => "0".to_string(),
            RoutingTimeLimit::One => "1".to_string(),
            RoutingTimeLimit::Two => "2".to_string(),
            RoutingTimeLimit::Three => "3".to_string(),
            RoutingTimeLimit::Four => "4".to_string(),
            RoutingTimeLimit::Five => "5".to_string(),
            RoutingTimeLimit::Six => "6".to_string(),
            RoutingTimeLimit::Seven => "7".to_string(),
            RoutingTimeLimit::Eight => "8".to_string(),
            RoutingTimeLimit::Nine => "9".to_string(),
            RoutingTimeLimit::Ten => "10".to_string(),
            RoutingTimeLimit::Eleven => "11".to_string(),
            RoutingTimeLimit::Twelve => "12".to_string(),
            RoutingTimeLimit::Thirteen => "13".to_string(),
            RoutingTimeLimit::Fourteen => "14".to_string(),
        }
    }

    pub fn from_string(s: &str) -> Result<RoutingTimeLimit, String> {
        match s {
            "0" => Ok(RoutingTimeLimit::Zero),
            "1" => Ok(RoutingTimeLimit::One),
            "2" => Ok(RoutingTimeLimit::Two),
            "3" => Ok(RoutingTimeLimit::Three),
            "4" => Ok(RoutingTimeLimit::Four),
            "5" => Ok(RoutingTimeLimit::Five),
            "6" => Ok(RoutingTimeLimit::Six),
            "7" => Ok(RoutingTimeLimit::Seven),
            "8" => Ok(RoutingTimeLimit::Eight),
            "9" => Ok(RoutingTimeLimit::Nine),
            "10" => Ok(RoutingTimeLimit::Ten),
            "11" => Ok(RoutingTimeLimit::Eleven),
            "12" => Ok(RoutingTimeLimit::Twelve),
            "13" => Ok(RoutingTimeLimit::Thirteen),
            "14" => Ok(RoutingTimeLimit::Fourteen),
            _ => Err(format!("Invalid RoutingTimeLimit: {}", s)),
        }
    }
}

//Side-effect chaining on Result
pub trait ResultExt<T, E> {
    fn chain_err<F, U>(self, f: F) -> Result<T, U>
    where
        F: FnOnce(E) -> U;
}


impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn chain_err<F, U>(self, f: F) -> Result<T, U>
    where

        T: Into<U>,

        F: FnOnce(E) -> U,
    {
        self.map_err(f)
    }

    fn chain_err_mut<F, U>(self, f: F) -> Result<T, U>
    where
        F: FnMut(E) -> U,
    { //map_err(f)
        self.map_err(f)
    }

    fn chain_err_once<F, U>(self, f: F) -> Result<T, U>
    where
        F: FnOnce(E) -> U,
    {
        self.map_err(f)
    }
}

impl StorageType {
    pub fn from_str(s: &str) -> Result<Self> {
        match s {
            "einsteindb" => Ok(StorageType::EinsteinDB),
            "foundationdb" => Ok(StorageType::FoundationDB),
            "berolina" => Ok(StorageType::Berolina),
            "soliton" => Ok(StorageType::Soliton),
            _ => Err(Error::InvalidStorageType(s.to_owned())),
        }
    }
}

impl<L, R> Either<L, R> {
    pub fn unwrap_left(self) -> L {
        match self {
            Either::Left(l) => l,
            _ => panic!("unwrap_left called on Either::Right"),
        }
    }

    pub fn unwrap_right(self) -> R {
        match self {
            Either::Right(r) => r,
            _ => panic!("unwrap_right called on Either::Left"),
        }
    }

    pub fn is_left(&self) -> bool {
        match self {
            Either::Left(_) => true,
            _ => false,
        }
    }

    pub fn is_right(&self) -> bool {
        match self {
            Either::Right(_) => true,
            _ => false,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Storage {
    EinsteinDB,
    FoundationDB,
}

#[derive(Debug)]
pub enum DataSourceType {
    Causetq,
    Berolina,
}

#[derive(Debug)]
pub struct DataSourceInfo {
    pub name: String,
    pub data_source_type: DataSourceType,
    pub data_source: Box<dyn DataSource>,
}

pub fn check_soliton_id_in_range(
    soliton_id: &[u8],
    region_id: u64,
    start_soliton_id: &[u8],
    end_soliton_id: &[u8],
) -> Result<()> {
    if soliton_id >= start_soliton_id && (end_soliton_id.is_empty() || soliton_id < end_soliton_id) {
        Ok(())
    } else {
        Err(Error::NotIn {
            soliton_id: soliton_id.to_vec(),
            region_id,
            start: start_soliton_id.to_vec(),
            end: end_soliton_id.to_vec(),
        })
    }
}

pub enum NetworkOption {
    LocalAddress(String),
    ClusterFile(String),
    TraceEnable(String),
    TraceRollSize(i32),
    TraceMaxLogsSize(i32),
    TraceLogGroup(String),
    TraceFormat(String),
    Knob(String),
    TLSPlugin(String),
    TLSCertBytes(Vec<u8>),
    TLSCertPath(String),
    TLSKeyBytes(Vec<u8>),
    TLSKeyPath(String),
    TLSVerifyPeers(Vec<u8>),
    BuggifyEnable,
    BuggifyDisable,
    BuggifySectionActivatedProbability(i32),
    BuggifySectionFiredProbability(i32),
    TLSCaBytes(Vec<u8>),
    TLSCaPath(String),
    TLSPassword(String),
    DisableMultiVersionClientApi,
    CallbacksOnExternalThreads,
    ExternalClientLibrary(String),
    ExternalClientDirectory(String),
    DisableLocalClient,
    DisableClientStatisticsLogging,
    EnableSlowTaskProfiling,
}


pub enum NetworkOptionType {
    LocalAddress,
    ClusterFile,
    TraceEnable,
    TraceRollSize,
    TraceMaxLogsSize,
    TraceLogGroup,
    TraceFormat,
    Knob,
    TLSPlugin,
    TLSCertBytes,
    TLSCertPath,
    TLSKeyBytes,
    TLSKeyPath,
    TLSVerifyPeers,
    BuggifyEnable,
    BuggifyDisable,
    BuggifySectionActivatedProbability,
    BuggifySectionFiredProbability,
    TLSCaBytes,
    TLSCaPath,
    TLSPassword,
    DisableMultiVersionClientApi,
    CallbacksOnExternalThreads,
    ExternalClientLibrary,
    ExternalClientDirectory,
    DisableLocalClient,
    DisableClientStatisticsLogging,
    EnableSlowTaskProfiling,
}


pub struct NetworkOptionBuilder {
    network_option: NetworkOption,
}


pub enum StorageOption {
    StorageType(Storage),
    EinsteinDB(String),
    FoundationDB(String),
    NetworkOption(NetworkOption),
}


