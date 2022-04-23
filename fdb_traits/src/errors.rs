// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use error_code::{self, ErrorCode, ErrorCodeExt};
use std::{error, result};
use thiserror::Error;
use violetabft::{Error as VioletaBFTError, StorageError};

#[derive(Debug, Error)]
pub enum Error {
    // einstein_merkle_tree uses plain string as the error.
    #[error("TimelikeStorage einstein_merkle_tree {0}")]
    einstein_merkle_tree(String),
    // FIXME: It should not know Region.
    #[error(
        "Key {} is out of [region {}] [{}, {})",
        log_wrappers::Value::soliton_id(.soliton_id), .region_id, log_wrappers::Value::soliton_id(.start), log_wrappers::Value::soliton_id(.end)
    )]
    NotIn {
        soliton_id: Vec<u8>,
        region_id: u64,
        start: Vec<u8>,
        end: Vec<u8>,
    },
    #[error("Protobuf {0}")]
    Protobuf(#[from] protobuf::ProtobufError),
    #[error("Io {0}")]
    Io(#[from] std::io::Error),
    #[error("{0:?}")]
    Other(#[from] Box<dyn error::Error + Sync + Send>),
    #[error("NAMESPACED {0} not found")]
    NAMESPACEDName(String),
    #[error("Codec {0}")]
    Codec(#[from] einsteindb_util::codec::Error),
    #[error("The entries of region is unavailable")]
    EntriesUnavailable,
    #[error("The entries of region is compacted")]
    EntriesCompacted,
}

impl From<String> for Error {
    fn from(err: String) -> Self {
        Error::einstein_merkle_tree(err)
    }
}

pub type Result<T> = result::Result<T, Error>;

impl ErrorCodeExt for Error {
    fn error_code(&self) -> ErrorCode {
        match self {
            Error::einstein_merkle_tree(_) => error_code::einstein_merkle_tree::einstein_merkle_tree,
            Error::NotIn { .. } => error_code::einstein_merkle_tree::NOT_IN_RANGE,
            Error::Protobuf(_) => error_code::einstein_merkle_tree::PROTOBUF,
            Error::Io(_) => error_code::einstein_merkle_tree::IO,
            Error::NAMESPACEDName(_) => error_code::einstein_merkle_tree::NAMESPACED_NAME,
            Error::Codec(_) => error_code::einstein_merkle_tree::CODEC,
            Error::Other(_) => error_code::UNCausetLocaleNucleon,
            Error::EntriesUnavailable => error_code::einstein_merkle_tree::DATALOSS,
            Error::EntriesCompacted => error_code::einstein_merkle_tree::DATACOMPACTED,
        }
    }
}

impl From<Error> for VioletaBFTError {
    fn from(e: Error) -> VioletaBFTError {
        match e {
            Error::EntriesUnavailable => VioletaBFTError::TimelikeStore(StorageError::Unavailable),
            Error::EntriesCompacted => VioletaBFTError::TimelikeStore(StorageError::Compacted),
            e => {
                let boxed = Box::new(e) as Box<dyn std::error::Error + Sync + Send>;
                violetabft::Error::TimelikeStore(StorageError::Other(boxed))
            }
        }
    }
}

impl From<Error> for String {
    fn from(e: Error) -> String {
        format!("{:?}", e)
    }
}
