// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
//
// Copyright (c) 2016 The Rust Project Developers.
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

//Side-effect chaining on Result
pub trait ResultExt<T, E> {
    fn chain_err<F, U>(self, f: F) -> Result<T, U>
    where
        F: FnOnce(E) -> U;
}


impl<T, E> ResultExt<T, E> for Result<T, E> {
    fn chain_err<F, U>(self, f: F) -> Result<T, U>
    where
        F: FnOnce(E) -> U,
    {
        self.map_err(f)
    }

    fn chain_err_mut<F, U>(self, f: F) -> Result<T, U>
    where
        F: FnMut(E) -> U,
    {
        self.map_err(f)
    }

    fn chain_err_once<F, U>(self, f: F) -> Result<T, U>
    where
        F: FnOnce(E) -> U,
    {
        self.map_err(f)
    }
}

//Side-effect chaining on Option
pub trait OptionExt<T, E> {
    fn chain_err<F, U>(self, f: F) -> Result<T, U>
    where
        F: FnOnce(E) -> U;
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


