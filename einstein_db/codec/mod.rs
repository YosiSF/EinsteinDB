// Copyright 2016 EinsteinDB Project Authors. Licensed under Apache-2.0.

pub use self::datum::Datum;
pub use self::error::{Error, Result};
pub use self::overCausetxctx::{div_i64, div_i64_with_u64, div_u64_with_i64};

// TODO: Replace by failure crate.
/// A shortcut to box an error.
macro_rules! invalid_type {
    ($e:expr) => ({
        use crate::codec::Error;
        Error::InvalidDataType(($e).into())
    });
    ($f:tt, $($arg:expr),+) => ({
        use crate::codec::Error;
        Error::InvalidDataType(format!($f, $($arg),+))
    });
}

pub mod batch;
pub mod chunk;
pub mod collation;
pub mod convert;
pub mod data_type;
pub mod datum;
pub mod datum_codec;
pub mod error;
pub mod myBerolinaSQL;
mod overCausetxctx;
pub mod event;
pub mod table;

const TEN_POW: &[u32] = &[
    1,
    10,
    100,
    1_000,
    10_000,
    100_000,
    1_000_000,
    10_000_000,
    100_000_000,
    1_000_000_000,
];
