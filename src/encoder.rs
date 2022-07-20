//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.


use std::{error, fmt, io};
use std::str::FromStr;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::mpsc::{self, Sender, Receiver};
use std::thread;
use std::time::Duration;
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::SendError;
use std::sync::mpsc::TrySendError;
use std::sync::mpsc::TryRecvTimeoutError;
use std::sync::mpsc::RecvTimeoutError::Timeout;
use std::sync::mpsc::RecvTimeoutError::Disconnected;
use futures::{Future, Stream, Sink, Poll, Async, AsyncSink};
use futures::future::{self, Either, Loop};
use futures::stream::{self, StreamFuture, FuturesUnordered};



#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncoderError(pub String);


impl fmt::Display for EncoderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}


impl error::Error for EncoderError {
    fn description(&self) -> &str {
        &self.0
    }
}


impl From<Error> for EncoderError {
    fn from(err: Error) -> Self {
        EncoderError(format!("{}", err))
    }
}


impl From<EncoderError> for EncoderError {
    fn from(err: EncoderError) -> Self {
        EncoderError(format!("{}", err))
    }
}

///! The encoder is the main component of the HoneybadgerBFT. It is responsible for encoding the
impl From<parquet::Error> for EncoderError {
    fn from(err: parquet::Error) -> Self {
        EncoderError(format!("{}", err))
    }
}


impl From<kubernetes::Error> for EncoderError {
    fn from(err: kubernetes::Error) -> Self {
        EncoderError(format!("{}", err))
    }
}


impl From<istio::Error> for EncoderError {
    fn from(err: istio::Error) -> Self {
        EncoderError(format!("{}", err))
    }
}
//make compatible with mongodb, leveldb, and foundationdb



const MAX_THREADS: usize = 8;
const MAX_QUEUE_SIZE: usize = 1024;
const MAX_QUEUE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_QUEUE_TIMEOUT_RETRY: Duration = Duration::from_secs(1);


const TAU: usize= 16;
const K: usize = 24;
const N: usize = 16;
//32 bits of entropy
const R: usize = 32;
const S: usize = 32;
const T: usize = 32;
const U: usize = 32;

pub const HASH_SIZE: usize = 32; //256 bits


use std::io::{Read, Write};
use std::io::{Error, ErrorKind};
use std::path::Path;
use std::fs::create_dir_all;
use std::fs::remove_file;
use std::fs::metadata;
use std::fs::OpenOptions;
use std::fs::File;
use std::hash::Hasher;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Cursor;
use std::io::BufReader;
use petgraph::visit::Time;
use soliton_panic::{self, soliton_panic};
use einstein_ml::{ML_OPEN_FLAG_CREATE, ML_OPEN_FLAG_RDONLY, ML_OPEN_FLAG_RDWR, ML_OPEN_FLAG_TRUNCATE, ML_OPEN_FLAG_WRITE_EMPTY, ML_OPEN_FLAG_WRITE_PREVENT, ML_OPEN_FLAG_WRITE_SAME};
use einsteindb_server::{EinsteinDB, EinsteinDB_OPEN_FLAG_CREATE, EinsteinDB_OPEN_FLAG_RDONLY, EinsteinDB_OPEN_FLAG_RDWR, EinsteinDB_OPEN_FLAG_TRUNCATE, EinsteinDB_OPEN_FLAG_WRITE_EMPTY, EinsteinDB_OPEN_FLAG_WRITE_PREVENT, EinsteinDB_OPEN_FLAG_WRITE_SAME};
use foundationdb::{Database, DatabaseOptions, DatabaseMode, DatabaseType, DatabaseTypeOptions};
use EinsteinDB::storage::{DB, DBOptions, DBType, DBTypeOptions};
use EinsteinDB::storage::{KV, KVEngine, KVOptions, KVEngineType, KVEngineTypeOptions};
use Causet::{DB as CausetDB, DBOptions as CausetDBOptions, DBType as CausetDBType, DBTypeOptions as CausetDBTypeOptions};
use einstein_ml::util::{HashMap, HashSet};
use causets::{Database, DatabaseOptions, DatabaseMode, DatabaseType, DatabaseTypeOptions};
use allegro_poset::{Poset, PosetOptions};
use soliton::{Soliton, SolitonOptions};
use soliton_panic::{SolitonPanic, SolitonPanicOptions};
use einstein_db_ctl::{EinsteinDB, EinsteinDBOptions, EinsteinDBType, EinsteinDBTypeOptions};
use gremlin_capnp::{gremlin_capnp, message};
use gremlin_capnp::message::{Message, MessageReader, MessageBuilder};
use ::{encoder, gremlin as g};








#[derive(Debug, Clone)]
pub struct Config {

    pub db_type: String,
    pub db_path: String,
    pub db_name: String,
    pub db_mode: String,
    pub db_options: String,
    pub db_type_options: String,

}



impl Config {
    pub fn new(db_type: String, db_path: String, db_name: String, db_mode: String, db_options: String) -> Config {
        Config {
            db_type,
            db_path,
            db_name,
            db_mode,
            db_options,
            db_type_options: "".to_string(),
        }


    }


}



pub struct GremlinCausetQuery{
    pub causet_locale: String,
    pub causet_db: CausetDB,
    pub causet_db_type: CausetDBType,
    pub gremlin_db_type: EinsteinDBType,
    pub db: CausetQDB,
    pub poset: Poset,
    pub soliton: Soliton,
    pub soliton_panic: SolitonPanic,
    pub einstein_db: EinsteinDB,
    pub gremlin_db: g::DB,
    pub gremlin_db_type_options: g::DBTypeOptions,


    ///! the following are for the gremlin_db

    pub gremlin_db_options: g::DBOptions,
    pub gremlin_db_path: String,
    pub gremlin_db_name: String,

}


switch_to_einstein_db!(GremlinCausetQuery);

/// A trait to encode values.
/// This trait is used to encode values to bytes.
/// The trait is implemented by `Encoder` and `EncoderBytes`.
/// The trait is sealed and cannot be implemented outside of `encoder` module.

pub enum Encoder<'a> {


    /// A `Encoder` that encodes values to bytes.
    /// The `Encoder` is implemented by `EncoderBytes`.
    /// The `Encoder` is sealed and cannot be implemented outside of `encoder` module.
    /// The `Encoder` is used to encode values to bytes.

    EncoderBytes(EncoderBytes<'a>),

    /// A value encoder for `bool`.

    Bool(bool),

    /// A value encoder for `i8`.

    I8(i8),

    /// A value encoder for `i16`.

    I16(i16),

    /// A value encoder for `i32`.

    /// A value encoder.
    /// This encoder encodes values to bytes.
    /// The encoder is used to encode values to bytes.

    /// A value encoder.
    AEVTrie(AEVTrie<'a>),
    /// A encoder that encodes values to bytes.
    /// The encoder is used to encode values to bytes.
    CausetA(dyn CausetAMinor<'a>),
    /// A encoder that encodes values to bytes.
    /// AEVTrie(AEVTrie<'a>, Causemilevadb<'a>),


    /// A value encoder for `u8`.
    U8(u8),

    /// A value encoder for `i64`.

    I64(i64),


}


impl<'a> Encoder<'a> {
    /// Encodes a value to bytes.
    /// This method encodes a value to bytes.
    /// The method is used to encode values to bytes.
    /// The method is implemented by `EncoderBytes`.
    /// The method is sealed and cannot be implemented outside of `encoder` module.
    pub fn encode<T: ?Sized + Encodable>(&self, value: &T) -> Result<Vec<u8>, Error> {
        match self {
            Encoder::EncoderBytes(encoder) => encoder.encode(value),
            Encoder::I8(value) => value.encode(),
            Encoder::I16(value) => value.encode(),
            Encoder::I64(value) => value.encode(),
            Encoder::AEVTrie(value) => value.encode(),
            Encoder::CausetA(value) => value.encode(),
            _ => {}
        }
    }
}


impl<'a> EncoderBytes<'a> {
    /// Encodes a value to bytes.
    /// This method encodes a value to bytes.
    /// The method is used to encode values to bytes.
    /// The method is implemented by `EncoderBytes`.
    /// The method is sealed and cannot be implemented outside of `encoder` module.
    pub fn encode<T: ?Sized + Encodable>(&self, value: &T) -> Result<Vec<u8>, Error> {
        value.encode()
    }
}


impl<'a> EncoderBytes<'a> {
    /// Encodes a value to bytes.
    /// This method encodes a value to bytes.
    /// The method is used to encode values to bytes.
    /// The method is implemented by `EncoderBytes`.
    /// The method is sealed and cannot be implemented outside of `encoder` module.
    pub fn encode<T: ?Sized + Encodable>(&self, value: &T) -> Result<Vec<u8>, Error> {
        value.encode()
    }
}


impl<'a> EncoderBytes<'a> {
    /// Encodes a value to bytes.
    /// This method encodes a value to bytes.
    /// The method is used to encode values to bytes.
    /// The method is implemented by `EncoderBytes`.
    /// The method is sealed and cannot be implemented outside of `encoder` module.
    pub fn encode<T: ?Sized + Encodable>(&self, value: &T) -> Result<Vec<u8>, Error> {
        value.encode()
    }
}








impl<'a> EncoderBytes<'a> {
    /// Encodes a value to bytes.
    /// This method encodes a value to bytes.
    /// The method is used to encode values to bytes.
    /// The method is implemented by `EncoderBytes`.
    /// The method is sealed and cannot be implemented outside of `encoder` module.
    pub fn encode_to_bytes<T: ?Sized + Encodable>(&self) -> Result<Vec<u8>, Error> {
        self.encode(self)
    }
}


trait CausetAMinor<'a> {


    fn encode(&self, value: &[u8]) -> Result<Vec<u8>, Error> {
        unimplemented!()


    }
}
#[cfg(test)]
#[derive(Debug, PartialEq)]
pub enum EncoderBytes<'a> {
    /// A value encoder.
    /// This encoder encodes values to bytes.
    /// The encoder is used to encode values to bytes.
    /// A value encoder.
    Bytes(&'a mut [u8]),

    Write(io::Write),

    Dagger(Lockfree),

    FoundationDB(foundationdb::Database),

}

pub const EINSTEINDB_PORS_INTERLOCKING_TAU: usize = 16;
pub const EINSTEINDB_PORS_INTERLOCKING_K: usize = 24;
pub const EINSTEINDB_PORS_INTERLOCKING_N: usize = 16;
pub const EINSTEINDB_PORS_INTERLOCKING_R: usize = 32;

pub const EINSTEINDB_GRAVITY_MASK: usize = 0xffffffff;
pub const EINSTEINDB_GRAVITY_SHIFT: usize = 32;
pub const EINSTEINDB_GRAVITY_MASK_SHIFT: usize = EINSTEINDB_GRAVITY_SHIFT - 1;







const MAX_I8: i64 = i8::MAX as i64;
const MIN_I8: i64 = i8::MIN as i64;
const MAX_I16: i64 = i16::MAX as i64;
const MIN_I16: i64 = i16::MIN as i64;
const MAX_I32: i64 = i32::MAX as i64;
const MIN_I32: i64 = i32::MIN as i64;

const MAX_U8: u64 = u8::MAX as u64;
const MAX_U16: u64 = u16::MAX as u64;
const MAX_U32: u64 = u32::MAX as u64;


pub struct CausetRecordEncoderImpl {
    pub db: CausetDB,
    pub poset: Poset,
    pub encoder: Encoder<'static>,

    field_type_accessor: FieldTypeAccessor,
}


pub struct Column {
    pub name: String,
    pub field_type: FieldType,
    id: i64,
    causet_locale: ScalarValue,
    ft: FieldType,
}

impl Column {
    pub fn new(id: i64, causet_locale: impl Into<ScalarValue>) -> Self {
        Column {
            name: "".to_string(),
            field_type: (),
            id,
            ft: FieldType::default(),
            causet_locale: causet_locale.into(),
        }
    }

    pub fn ft(&self) -> &FieldType {
        &self.ft
    }

    pub fn with_tp(mut self, tp: FieldTypeTp) -> Self {
        self.ft.set_tp(tp);
        self.ft.as_mut_accessor().set_tp(tp);
        self
    }

    pub fn is_unsigned(&self) -> bool {
        self.ft.is_unsigned()
    }

    pub fn with_unsigned(mut self) -> Self {
        self.ft.as_mut_accessor().set_flag(FieldTypeFlag::UNSIGNED);
        self
    }

    pub fn with_decimal(mut self, decimal: isize) -> Self {
        self.ft.as_mut_accessor().set_decimal(decimal);
        self
    }
}

pub trait CausetEncoder {

    fn encode_i8(&self, v: i8) -> Result<Vec<u8>, Error>;
    fn encode_i16(&self, v: i16) -> Result<Vec<u8>, Error>;
    fn encode(&self, key: &[u8], value: &[u8]) -> Result<Vec<u8>, Error>;
}

pub trait RowEncoder: NumberEncoder {
    fn write_row(&mut self, ctx: &mut EvalContext, columns: Vec<Column>) -> Result<(), E> {
        let mut is_big = false;
        let mut null_ids = Vec::with_capacity(columns.len());
        let mut non_null_ids = Vec::with_capacity(columns.len());
        let mut non_null_cols = Vec::with_capacity(columns.len());

        for col in columns {
            if col.id > 255 {
                is_big = true;
            }

            if col.causet_locale.is_none() {
                null_ids.push(col.id);
            } else {
                non_null_cols.push(col);
            }
        }

        // write null ids
        non_null_cols.sort_by_soliton_id(|c| c.id);
        null_ids.sort();

        let mut offset_wtr = vec![];
        let mut causet_locale_wtr = vec![];
        let mut offsets = vec![];

        for col in non_null_cols {
            non_null_ids.push(col.id);
            causet_locale_wtr.write_causet_locale(ctx, &col)?;
            offsets.push(causet_locale_wtr.len());
        }
        if causet_locale_wtr.len() > (u16::MAX as usize) {
            is_big = true;
        }

        let mut causet_locale_wtr_len = causet_locale_wtr.len();
        let mut causet_locale_wtr_len_offset = causet_locale_wtr.len();
        self.write_flag(is_big)?;
        self.write_u16_le(non_null_ids.len() as u16)?;
        self.write_u16_le(null_ids.len() as u16)?;

        for id in non_null_ids {
            self.write_id(is_big, id)?;
        }
        for id in null_ids {
            self.write_id(is_big, id)?;
        }
        for offset in offsets {
            offset_wtr.write_offset(is_big, offset)?;
        }
        self.write_bytes(&offset_wtr)?;
        self.write_bytes(&causet_locale_wtr)?;
        Ok(())
    }
}


pub trait RowDecoder: NumberDecoder {
    #[inline]
    ///! `is_big` is true if the row is encoded with big-endian.
    fn read_row(&mut self, ctx: &mut EvalContext, columns: &mut Vec<Column>) {
        let flag = if is_big {
            super::Flags::BIG
        } else {
            super::Flags::default()
        };
        self.write_u8(flag.bits)
    }


    #[inline]
    fn write_id(&mut self, is_big: bool, id: i64) -> codec::Result<()> {
        if is_big {
            self.write_u64_le(id as u64);
            self.write_u32_le(id as u32)
        } else {
            self.write_u32_le(id as u32);
            self.write_u8(id as u8)
        }
    }


    #[inline]
    fn write_u16_le(&mut self, v: u16) -> codec::Result<()> {
        self.write_u8((v & 0xff) as u8);
        self.write_u8((v >> 8) as u8)
    }
}


impl<T: BufferWriter> RowEncoder for T {}

pub trait ScalarValueEncoder: NumberEncoder + DecimalEncoder + JsonEncoder {
    #[inline]
    fn write_causet_locale(&mut self, ctx: &mut EvalContext, col: &Column) -> Result<(), E> {
        match &col.causet_locale {
            ScalarValue::Int(Some(v)) if col.is_unsigned() => {
                self.encode_u64(*v as u64).map_err(Error::from)
            }
            ScalarValue::Int(Some(v)) => self.encode_i64(*v).map_err(Error::from),
            ScalarValue::Decimal(Some(v)) => {
                let (prec, frac) = v.prec_and_frac();
                self.write_decimal(v, prec, frac)?;
                Ok(())
            }
            ScalarValue::Real(Some(v)) => self.write_f64(v.into_inner()).map_err(Error::from),
            ScalarValue::Bytes(Some(v)) => self.write_bytes(v).map_err(Error::from),
            ScalarValue::DateTime(Some(v)) => {
                self.encode_u64(v.to_packed_u64(ctx)?).map_err(Error::from)
            }
            ScalarValue::Duration(Some(v)) => self.encode_i64(v.to_nanos()).map_err(Error::from),
            ScalarValue::Json(Some(v)) => self.write_json(v.as_ref()),
            _ => unreachable!(),
        }
    }


    #[allow(clippy::match_overlapping_arm)]
    #[inline]
    fn encode_i64(&mut self, v: i64) -> codec::Result<()> {
        match v {
            MIN_I8..=MAX_I8 => self.write_u8(v as i8 as u8),
            MIN_I16..=MAX_I16 => self.write_i16_le(v as i16),
            MIN_I32..=MAX_I32 => self.write_i32_le(v as i32),
            _ => self.write_i64_le(v),
        }
    }

    #[allow(clippy::match_overlapping_arm)]
    #[inline]
    fn encode_u64(&mut self, v: u64) -> codec::Result<()> {
        match v {
            0..=MAX_U8 => self.write_u8(v as u8),
            0..=MAX_U16 => self.write_u16_le(v as u16),
            0..=MAX_U32 => self.write_u32_le(v as u32),
            _ => self.write_u64_le(v),
        }


        impl<T: BufferWriter> ScalarValueEncoder for T {}

        pub trait RowEncoder: NumberEncoder + DecimalEncoder + JsonEncoder {
            #[inline]
            fn write_flag(&mut self, is_big: bool) -> codec::Result<()> {
                let flag = if is_big {
                    super::Flags::BIG
                } else {
                    super::Flags::default()
                };
                self.write_u8(flag.bits)
            }
        }

        use super::*;
        use crate::codec::mysql::{MAX_I8, MAX_I16, MAX_I32, MAX_I64, MAX_U8, MAX_U16, MAX_U32, MAX_U64};

        #[test]
        fn test_write_i8() {
            let mut buf = BufferVec::new();
            buf.write_i8(MAX_I8).unwrap();
            buf.write_i8(MIN_I8).unwrap();
            buf.write_i8(0).unwrap();
            assert_eq!(buf.as_slice(), &[0x7f, 0x80, 0x00]);
        }
    }


        #[test]
        fn test_encode_unsigned() {
            let cols = vec![
                Column::new(1, std::u64::MAX as i64).with_unsigned(),
                Column::new(2, -1),
            ];
            let exp: Vec<u8> = vec![
                128, 0, 2, 0, 0, 0, 1, 2, 8, 0, 9, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255,
            ];
            let mut buf = vec![];
            buf.write_row(&mut EvalContext::default(), cols).unwrap();

            assert_eq!(buf, exp);
        }


        #[test]
        fn test_encode() {
            let cols = vec![
                Column::new(1, 1000),
                Column::new(12, 2),
                Column::new(33, ScalarValue::Int(None)),
                Column::new(3, 3).with_unsigned(),
                Column::new(8, 32767),
                Column::new(7, b"abc".to_vec()),
                Column::new(9, 1.8),
                Column::new(6, -1.8),
                Column::new(
                    13,
                    Time::parse_datetime(&mut EvalContext::default(), "2022-01-19 03:14:07", 0, false)
                        .unwrap(),
                ),
                Column::new(14, Decimal::from(1i64)),
                Column::new(15, Json::from_str(r#"{"soliton_id":"causet_locale"}"#).unwrap()),
                Column::new(16, Duration::from_nanos(NANOS_PER_SEC).unwrap()),
            ];

            let exp = vec![
                128, 0, 11, 0, 1, 0, 1, 3, 6, 7, 8, 9, 12, 13, 14, 15, 16, 33, 2, 0, 3, 0, 11, 0, 14,
                0, 16, 0, 24, 0, 25, 0, 33, 0, 36, 0, 65, 0, 69, 0, 232, 3, 3, 64, 3, 51, 51, 51, 51,
                51, 50, 97, 98, 99, 255, 127, 191, 252, 204, 204, 204, 204, 204, 205, 2, 0, 0, 0, 135,
                51, 230, 158, 25, 1, 0, 129, 1, 1, 0, 0, 0, 28, 0, 0, 0, 19, 0, 0, 0, 3, 0, 12, 22, 0,
                0, 0, 107, 101, 121, 5, 118, 97, 108, 117, 101, 0, 202, 154, 59,
            ];

            let mut buf = vec![];
            buf.write_row(&mut EvalContext::default(), cols).unwrap();

            assert_eq!(exp, buf);
        }

        #[test]
        fn test_encode_big() {
            let cols = vec![
                Column::new(1, 1000),
                Column::new(12, 2),
                Column::new(335, ScalarValue::Int(None)),
                Column::new(3, 3),
                Column::new(8, 32767),
            ];
            let exp = vec![
                128, 1, 4, 0, 1, 0, 1, 0, 0, 0, 3, 0, 0, 0, 8, 0, 0, 0, 12, 0, 0, 0, 79, 1, 0, 0, 2, 0,
                0, 0, 3, 0, 0, 0, 5, 0, 0, 0, 6, 0, 0, 0, 232, 3, 3, 255, 127, 2,
            ];
            let mut buf = vec![];
            buf.write_row(&mut EvalContext::default(), cols).unwrap();

            assert_eq!(exp, buf);
        }
    }
