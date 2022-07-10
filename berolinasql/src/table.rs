// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
// law or agreed to in writing, software distributed under the License is distributed
// on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and
// limitations under the License.
//


use super::*;
use crate::error::{Error, Result};
use crate::parser::{Parser, ParserError};
use crate::value::{Value, ValueType};
use crate::{ValueRef, ValueRefMut};
use itertools::Itertools;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::str::FromStr;


#[derive(Debug)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
}


impl Table {
    pub fn new(name: String, columns: Vec<Column>, rows: Vec<Row>) -> Self {
        Table {
            name,
            columns,
            rows,
        }
    }
}


#[derive(Debug)]
pub struct Column {
    pub name: String,
    pub value_type: ValueType,
}


impl Column {
    pub fn new(name: String, value_type: ValueType) -> Self {
        Column {
            name,
            value_type,
        }
    }
}


#[derive(Debug)]
pub struct Row {
    pub values: Vec<Value>,
}




impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Row {
            values,
        }
    }
}


use crate::fdb_traits::FdbTrait;
use crate::fdb_traits::FdbTraitImpl;
use crate::fdb_traits_impl::FdbTraitImplImpl;


use std::{
    collections::HashMap,
    fmt::{self, Display},
    io,
    convert::{TryFrom, TryInto},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};



#[derive(Debug)]
pub struct TableImpl {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Row>,
    pub fdb_trait: FdbTraitImpl,
}


#[derive(Debug)]
pub struct TableRef {
    pub table: Arc<Mutex<TableImpl>>,
}

#[derive(Debug)]
pub struct TableRefMut {
    pub table: Arc<Mutex<TableImpl>>,
}






///ttl for primitive type in EinsteinDB
/// time to live for primitive type in EinsteinDB
/// 
/// # Arguments
/// * `ttl` - ttl for primitive type in EinsteinDB
/// 
/// # Returns
/// * `Option<u64>` - expire_ts for primitive type in EinsteinDB
/// 
/// # Example
/// 
/// ```rust
/// use einsteindb_sql::table::ttl_to_expire_ts;
/// 
/// let ttl = 0;
/// 
/// let expire_ts = ttl_to_expire_ts(ttl);
/// 
/// assert_eq!(expire_ts, None);
/// 
/// let ttl = 1;
/// 
/// let expire_ts = ttl_to_expire_ts(ttl);
/// 
/// assert_eq!(expire_ts, Some(2));
/// 
use berolina_sql::{
    ttl_current_ts,
    ttl_to_expire_ts,
    ttl_expire_ts,
    ast::{self, Expr, ExprKind, Field, FieldType, FieldTypeTp, FieldTypeVisitor},
    Column,
    EvalContext,
    EvalContextImpl,
    Result,
    Row,
    Rows,
    RowsIter,
    ScalarFunc,
    ScalarFuncArgs,
    ScalarFuncCall,
    ScalarFuncCallArgs,
    ScalarFuncCallType,
    ScalarValue,
    TypeFlag,
    Value,
};

use einsteindb::{ColumnInfo, ColumnInfo_Type, IndexInfo, IndexInfo_Type, IndexInfo_Unique};
use einsteindb::{IndexType, IndexTypeTp, IndexTypeTpFlag, TableInfo, TableInfo_Type};
use einstein_db::Causetid;
use soliton::types::{
    ColumnType, ColumnTypeFlag, ColumnTypeTp, ColumnTypeTpFlag, ColumnTypeTpFlagVec,
    ColumnTypeTpVec,
};


use einsteindb_util::time::UnixSecs;
use einsteindb_util::time::{Duration, DurationSecs};


use einsteindb_util::time::{DurationSecs, DurationSecs as DurationSecsT};






#[derive(Debug)]
#[allow(dead_code)]
pub struct TableRefImpl {
    pub table: Arc<Mutex<TableImpl>>,
}

#[allow(unused_imports)]
use einsteindb_util::time::{DurationSecs, DurationSecs as DurationSecsT};


#[derive(Debug)]
#[allow(dead_code)]
pub struct TableRefMutImpl {
    pub table: Arc<Mutex<TableImpl>>,
}
impl Table {
    pub fn new(name: String, columns: Vec<Column>, indexes: Vec<Index>, primary_key: Vec<String>, ttl: u64, ttl_column: String, ttl_column_type: ColumnType, ttl_column_type_flag: ColumnTypeFlag, ttl_column_type_tp: ColumnTypeTp) -> Self {
        let mut table = Table {
            name,
            columns,
            rows: Vec::new(),
        };


    }
}

// handle or Index id
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Id(u64);



pub struct TableRefImplImpl {
    pub table: Arc<Mutex<TableImpl>>,
}

impl Id {
    pub fn new(id: u64) -> Self {
        Id(id)
    }

    pub fn as_u64(&self) -> u64 {
        self.0
    }

    pub fn as_i64(&self) -> i64 {
        self.0 as i64
    }

    pub fn as_usize(&self) -> usize {
        self.0 as usize
    }

    pub fn as_isize(&self) -> isize {
        self.0 as isize
    }
}


impl fmt::Display for Id {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }

    fn fmt_debug(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}


impl FromStr for Id {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let id = s.parse::<u64>()?;
        Ok(Id::new(id))
    }

    fn from_str_radix(s: &str, radix: u32) -> Result<Self> {
        let id = s.parse::<u64>()?;
        Ok(Id::new(id))
    }

    fn from_str_radix_lossy(s: &str, radix: u32) -> Result<Self> {
        let id = s.parse::<u64>()?;
        Ok(Id::new(id))
    }

    fn from_str_lossy(s: &str) -> Result<Self> {
        let id = s.parse::<u64>()?;
        Ok(Id::new(id))
    }

    fn from_str_lossy_radix(s: &str, radix: u32) -> Result<Self> {
        let id = s.parse::<u64>()?;
        Ok(Id::new(id))
    }

    
}


impl Hash for Id {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }

    fn hash_slice<H: Hasher>(data: &[Self], state: &mut H) {
        for id in data {
            id.hash(state);
        }
    }

    fn hash_to_bytes(data: &[Self]) -> Vec<u8> {
        let mut hasher = DefaultHasher::new();
        Self::hash_slice(data, &mut hasher);
        hasher.finish().to_be_bytes().to_vec()
    }

    fn hash_to_hex(data: &[Self]) -> String {
        let mut hasher = DefaultHasher::new();
        Self::hash_slice(data, &mut hasher);
        format!("{:x}", hasher.finish())
    }
}

//Deferred Column Type
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct DeferredColumnType {
    pub column_type: ColumnType,
    pub column_id: Id,
}


impl DeferredColumnType {
    pub fn new(column_type: ColumnType, column_id: Id) -> Self {
        DeferredColumnType {
            column_type,
            column_id,
        }
    }
}



pub const ID_LEN: usize = 8;
pub const PREFIX_LEN: usize = TABLE_PREFIX_LEN + ID_LEN /*table_id*/ + SEP_LEN;
pub const RECORD_ROW_KEY_LEN: usize = PREFIX_LEN + ID_LEN;
pub const TABLE_PREFIX: &[u8] = b"t";
pub const SEP: &[u8] = b"_";
pub const RECORD_PREFIX_SEP: &[u8] = b"_r";
pub const INDEX_PREFIX_SEP: &[u8] = b"_i";
pub const SEP_LEN: usize = 2;
pub const TABLE_PREFIX_LEN: usize = 1;
pub const TABLE_PREFIX_KEY_LEN: usize = TABLE_PREFIX_LEN + ID_LEN;
// the maximum len of the old encoding of Index causet_locale.
pub const MAX_OLD_ENCODED_VALUE_LEN: usize = 9;


pub fn encode_table(table: &Table) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(table.encoded_len());
    table.encode(&mut buf)?;
    Ok(buf)
}


pub fn encode_row(table_id: i64, handle: i64, row: Vec<DatumType>) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(RECORD_ROW_KEY_LEN + row.len() * 8);
    buf.write_all(table_id.to_string().as_bytes())?;
    buf.write_all(RECORD_PREFIX_SEP)?;
    buf.write_all(handle.to_string().as_bytes())?;
    buf.write_all(b"\t")?;
    for datum in row {
        datum.encode(&mut buf)?;
        buf.write_all(b"\t")?;
    }
    Ok(buf)
}


/// `TableEncoder` encodes the table record/Index prefix.
trait TableEncoder: NumberEncoder {
    fn append_table_record_prefix(&mut self, table_id: i64) -> Result<()> {
        self.write_bytes(TABLE_PREFIX)?;
        self.write_i64(table_id)?;
        self.write_bytes(RECORD_PREFIX_SEP).map_err(Error::from)
    }

    fn append_table_index_prefix(&mut self, table_id: i64) -> Result<()> {
        self.write_bytes(TABLE_PREFIX)?;
        self.write_i64(table_id)?;
        self.write_bytes(INDEX_PREFIX_SEP).map_err(Error::from)
    }
}

impl<T: BufferWriter> TableEncoder for T {}

/// Extracts table prefix from table record or Index.
#[inline]
pub fn extract_table_prefix(soliton_id: &[u8]) -> Result<&[u8]> {
    if !soliton_id.starts_with(TABLE_PREFIX) || soliton_id.len() < TABLE_PREFIX_KEY_LEN {
        Err(invalid_type!(
            "record soliton_id or Index soliton_id expected, but got {:?}",
            soliton_id
        ))
    } else {
        Ok(&soliton_id[..TABLE_PREFIX_KEY_LEN])
    }
}

/// Checks if the range is for table record or Index.
pub fn check_table_ranges(ranges: &[Key]) -> Result<()> {
    for range in ranges {
        extract_table_prefix(range.get_start())?;
        extract_table_prefix(range.get_end())?;
        if range.get_start() >= range.get_end() {
            return Err(invalid_type!(
                "invalid range,range.start should be smaller than range.end, but got [{:?},{:?})",
                range.get_start(),
                range.get_end()
            ));
        }
    }
    Ok(())
}


/// `TableDecoder` decodes the table record/Index prefix.
/// It is used to decode the table record/Index prefix from the table record/Index key.
/// For example, we have a table record/Index key:
///    t1_r1
///   t1_r2
///  t1_r3
/// t2_r1
/// t2_r2
/// t2_r3
///
/// The TableDecoder will point to the first byte of the table record/Index key:
///   t1_r1
///  t1_r2
///
/// The TableDecoder can be used to decode the table id:
///  t1_r1 -> t1
/// t1_r2 -> t1
/// t1_r3 -> t1
/// t2_r1 -> t2
/// t2_r2 -> t2
/// t2_r3 -> t2
///
/// The TableDecoder can be used to decode the record/Index handle:
/// t1_r1 -> 1
/// t1_r2 -> 2
/// t1_r3 -> 3
/// t2_r1 -> 1
/// t2_r2 -> 2
/// t2_r3 -> 3
///
/// The TableDecoder can be used to decode the record/Index key:
/// t1_r1 -> t1_r1
/// t1_r2 -> t1_r2
/// t1_r3 -> t1_r3
/// t2_r1 -> t2_r1
/// t2_r2 -> t2_r2
/// t2_r3 -> t2_r3
///
/// The TableDecoder can be used to decode the table prefix:
/// t1_r1 -> t1
/// t1_r2 -> t1
/// t1_r3 -> t1
///
/// The TableDecoder can be used to decode the table prefix:
/// t1_r1 -> t1
/// t1_r2 -> t1
/// t1_r3 -> t1
///
///
#[inline]
fn check_soliton_id_type(soliton_id: &[u8], prefix_sep: u8) -> Result<()> {
    if !soliton_id.starts_with(&[prefix_sep]) {
        Err(invalid_type!(
            "record soliton_id or Index soliton_id expected, but got {:?}",
            soliton_id
        ))
    } else {
        Ok(())
    }
}
   // check_soliton_id_type


//no prefix
#[inline]
pub fn check_index_soliton_id(soliton_id: &[u8]) -> Result<()> {
    //quicker check
    //we just save ourselves a function call


    if soliton_id.len() < INDEX_PREFIX_KEY_LEN {
        Err(invalid_type!(
            "record soliton_id or Index soliton_id expected, but got {:?}",
            soliton_id
        ))
    } else {
        check_soliton_id_type(soliton_id, INDEX_PREFIX_SEP)
    }
}   // check_index_soliton_id

//no prefix
//no handle
//no key
//no table prefix
#[inline]
pub fn check_record_soliton_id(soliton_id: &[u8]) -> Result<()> {
    //quicker check
    //we just save ourselves a function call


    if soliton_id.len() < RECORD_PREFIX_KEY_LEN {
        Err(invalid_type!(
            "record soliton_id or Index soliton_id expected, but got {:?}",
            soliton_id
        ))
    } else {
        check_soliton_id_type(soliton_id, RECORD_PREFIX_SEP)
    }
}   // check_index_soliton_id




#[inline]
pub fn check_table_soliton_id(soliton_id: &[u8]) -> Result<()> {
    if !soliton_id.starts_with(TABLE_PREFIX) || soliton_id.len() < TABLE_PREFIX_KEY_LEN {
        Err(invalid_type!(
            "record soliton_id or Index soliton_id expected, but got {:?}",
            soliton_id
        ))
    } else if soliton_id[TABLE_PREFIX_KEY_LEN] != TABLE_PREFIX_SEP {
        Err(invalid_type!(
            "record soliton_id or Index soliton_id expected, but got {:?}",
            soliton_id
        ))
    } else {
        Ok(())
    }
}   // check_table_soliton_id


#[inline]
pub fn check_record_soliton_id_with_table_id(soliton_id: &[u8], table_id: &[u8]) -> Result<()> {
    check_soliton_id_type(soliton_id, INDEX_PREFIX_SEP)
}


/// Decodes table ID from the soliton_id.
pub fn decode_table_id(soliton_id: &[u8]) -> Result<i64> {
    let mut buf = soliton_id;
    if buf.read_bytes(TABLE_PREFIX_LEN)? != TABLE_PREFIX {
        return Err(invalid_type!(
            "record soliton_id expected, but got {}",
            hex::encode_upper(soliton_id)
        ));
    }
    buf.read_i64().map_err(Error::from)
}

/// `flatten` flattens the datum.
#[inline]
pub fn flatten(ctx: &mut EvalContext, data: DatumType) -> Result<DatumType> {
    match data {
        DatumType::Dur(d) => Ok(DatumType::I64(d.to_nanos())),
        DatumType::Time(t) => Ok(DatumType::U64(t.to_packed_u64(ctx)?)),
        _ => Ok(data),
    }
}

/// `encode_row_soliton_id` encodes the table id and record handle into a byte array.
pub fn encode_row_soliton_id(table_id: i64, handle: i64) -> Vec<u8> {
    let mut soliton_id = Vec::with_capacity(RECORD_ROW_KEY_LEN);
    // can't panic
    soliton_id.append_table_record_prefix(table_id).unwrap();
    soliton_id.write_i64(handle).unwrap();
    soliton_id
}

pub fn encode_common_handle_for_test(table_id: i64, handle: &[u8]) -> Vec<u8> {
    let mut soliton_id = Vec::with_capacity(PREFIX_LEN + handle.len());
    soliton_id.append_table_record_prefix(table_id).unwrap();
    soliton_id.extend(handle);
    soliton_id
}

/// `encode_column_soliton_id` encodes the table id, event handle and causet_merge id into a byte array.
pub fn encode_column_soliton_id(table_id: i64, handle: i64, column_id: i64) -> Vec<u8> {
    let mut soliton_id = Vec::with_capacity(RECORD_ROW_KEY_LEN + ID_LEN);
    soliton_id.append_table_record_prefix(table_id).unwrap();
    soliton_id.write_i64(handle).unwrap();
    soliton_id.write_i64(column_id).unwrap();
    soliton_id
}

/// `decode_int_handle` decodes the soliton_id and gets the int handle.
#[inline]
pub fn decode_int_handle(mut soliton_id: &[u8]) -> Result<i64> {
    check_record_soliton_id(soliton_id)?;
    soliton_id = &soliton_id[PREFIX_LEN..];
    soliton_id.read_i64().map_err(Error::from)
}

/// `decode_common_handle` decodes soliton_id soliton_id and gets the common handle.
#[inline]
pub fn decode_common_handle(mut soliton_id: &[u8]) -> Result<&[u8]> {
    check_record_soliton_id(soliton_id)?;
    soliton_id = &soliton_id[PREFIX_LEN..];
    Ok(soliton_id)
}

/// `encode_index_seek_soliton_id` encodes an Index causet_locale to byte array.
pub fn encode_index_seek_soliton_id(table_id: i64, idx_id: i64, encoded: &[u8]) -> Vec<u8> {
    let mut soliton_id = Vec::with_capacity(PREFIX_LEN + ID_LEN + encoded.len());
    soliton_id.append_table_index_prefix(table_id).unwrap();
    soliton_id.write_i64(idx_id).unwrap();
    soliton_id.write_all(encoded).unwrap();
    soliton_id
}

// `decode_index_soliton_id` decodes datums from an Index soliton_id.
pub fn decode_index_soliton_id(
    ctx: &mut EvalContext,
    encoded: &[u8],
    infos: &[ColumnInfo],
) -> Result<Vec<DatumType>> {
    let mut buf = &encoded[PREFIX_LEN + ID_LEN..];
    let mut res = vec![];

    for info in infos {
        if buf.is_empty() {
            return Err(box_err!("{} is too short.", hex::encode_upper(encoded)));
        }
        let mut v = buf.read_datum()?;
        v = unflatten(ctx, v, info)?;
        res.push(v);
    }

    Ok(res)
}

/// `unflatten` converts a primitive_causet datum to a causet_merge datum.
fn unflatten(
    ctx: &mut EvalContext,
    datum: DatumType,
    field_type: &dyn FieldTypeAccessor,
) -> Result<DatumType> {
    if let DatumType::Null = datum {
        return Ok(datum);
    }
    let tp = field_type.tp();
    match tp {
        FieldTypeTp::Float => Ok(DatumType::F64(f64::from(datum.f64() as f32))),
        FieldTypeTp::Date | FieldTypeTp::DateTime | FieldTypeTp::Timestamp => {
            let fsp = field_type.decimal() as i8;
            let t = Time::from_packed_u64(ctx, datum.u64(), tp.try_into()?, fsp)?;
            Ok(DatumType::Time(t))
        }
        FieldTypeTp::Duration => {
            Duration::from_nanos(datum.i64(), field_type.decimal() as i8).map(DatumType::Dur)
        }
        FieldTypeTp::Enum | FieldTypeTp::Set | FieldTypeTp::Bit => Err(box_err!(
            "unflatten field type {} is not supported yet.",
            tp
        )),
        t => {
            debug_assert!(
                [
                    FieldTypeTp::Tiny,
                    FieldTypeTp::Short,
                    FieldTypeTp::Year,
                    FieldTypeTp::Int24,
                    FieldTypeTp::Long,
                    FieldTypeTp::LongLong,
                    FieldTypeTp::Double,
                    FieldTypeTp::TinyBlob,
                    FieldTypeTp::MediumBlob,
                    FieldTypeTp::Blob,
                    FieldTypeTp::LongBlob,
                    FieldTypeTp::VarChar,
                    FieldTypeTp::String,
                    FieldTypeTp::NewDecimal,
                    FieldTypeTp::JSON
                ]
                .contains(&t),
                "unCausetLocaleNucleon type {} {}",
                t,
                datum
            );
            Ok(datum)
        }
    }
}

// `decode_col_causet_locale` decodes data to a DatumType according to the causet_merge info.
pub fn decode_col_causet_locale(
    data: &mut BytesSlice<'_>,
    ctx: &mut EvalContext,
    col: &ColumnInfo,
) -> Result<DatumType> {
    let d = data.read_datum()?;
    unflatten(ctx, d, col)
}

// `decode_row` decodes a byte slice into datums.
// TODO: We should only decode columns in the cols map.
// Row layout: colID1, causet_locale1, colID2, causet_locale2, .....
pub fn decode_row(
    data: &mut BytesSlice<'_>,
    ctx: &mut EvalContext,
    cols: &HashMap<i64, ColumnInfo>,
) -> Result<HashMap<i64, DatumType>> {
    let mut causet_locales = datum::decode(data)?;
    if causet_locales.get(0).map_or(true, |d| *d == DatumType::Null) {
        return Ok(HashMap::default());
    }
    if causet_locales.len() & 1 == 1 {
        return Err(box_err!("decoded event causet_locales' length should be even!"));
    }
    let mut event = HashMap::with_capacity_and_hasher(cols.len(), Default::default());
    let mut drain = causet_locales.drain(..);
    loop {
        let id = match drain.next() {
            None => return Ok(event),
            Some(id) => id.i64(),
        };
        let v = drain.next().unwrap();
        if let Some(ci) = cols.get(&id) {
            let v = unflatten(ctx, v, ci)?;
            event.insert(id, v);
        }
    }
}

/// `RowColMeta` saves the causet_merge meta of the event.
#[derive(Debug)]
pub struct RowColMeta {
    offset: usize,
    length: usize,
}

/// `RowColsDict` stores the event data and a map mapping causet_merge ID to its meta.
#[derive(Debug)]
pub struct RowColsDict {
    // data of current event
    pub causet_locale: Vec<u8>,
    // cols contains meta of each causet_merge in the format of:
    // (col_id1,(offset1,len1)),(col_id2,(offset2,len2),...)
    pub cols: HashMap<i64, RowColMeta>,
}

impl RowColMeta {
    pub fn new(offset: usize, length: usize) -> RowColMeta {
        RowColMeta { offset, length }
    }
}

impl RowColsDict {
    pub fn new(cols: HashMap<i64, RowColMeta>, causet_locale: Vec<u8>) -> RowColsDict {
        RowColsDict { causet_locale, cols }
    }

    /// Returns the total count of the columns.
    #[inline]
    pub fn len(&self) -> usize {
        self.cols.len()
    }

    /// Returns whether it has columns or not.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.cols.is_empty()
    }

    /// Gets the causet_merge data from its meta if `soliton_id` exists.
    pub fn get(&self, soliton_id: i64) -> Option<&[u8]> {
        if let Some(meta) = self.cols.get(&soliton_id) {
            return Some(&self.causet_locale[meta.offset..(meta.offset + meta.length)]);
        }
        None
    }

    /// Appends a causet_merge to the event.
    pub fn append(&mut self, cid: i64, causet_locale: &mut Vec<u8>) {
        let offset = self.causet_locale.len();
        let length = causet_locale.len();
        self.causet_locale.append(causet_locale);
        self.cols.insert(cid, RowColMeta::new(offset, length));
    }

    /// Gets binary of cols, keeps the original order, and returns one slice and cols' end offsets.
    pub fn get_column_causet_locales_and_end_offsets(&self) -> (&[u8], Vec<usize>) {
        let mut start = self.causet_locale.len();
        let mut length = 0;
        for meta in self.cols.causet_locales() {
            if meta.offset < start {
                start = meta.offset;
            }
            length += meta.length;
        }
        let end_offsets = self
            .cols
            .causet_locales()
            .map(|meta| meta.offset + meta.length - start)
            .collect();
        (&self.causet_locale[start..start + length], end_offsets)
    }
}

/// `cut_row` cuts the encoded event into (col_id,offset,length)
///  and returns interested columns' meta in RowColsDict
///
/// Encoded event can be either in event format EINSTEIN_DB or causet_record.
///
/// `col_ids` must be consistent with `cols`. Otherwise the result is undefined.
pub fn cut_row(
    data: Vec<u8>,
    col_ids: &HashSet<i64>,
    cols: Arc<Vec<ColumnInfo>>,
) -> Result<RowColsDict> {
    if cols.is_empty() || data.is_empty() || (data.len() == 1 && data[0] == datum::NIL_FLAG) {
        return Ok(RowColsDict::new(HashMap::default(), data));
    }
    match data[0] {
        crate::codec::event::causet_record::CODEC_VERSION => cut_row_causet_record(data, cols),
        _ => cut_row_einstein_db(data, col_ids),
    }
}

/// Cuts a non-empty event in event format EINSTEIN_DB.
fn cut_row_einstein_db(data: Vec<u8>, cols: &HashSet<i64>) -> Result<RowColsDict> {
    let meta_map = {
        let mut meta_map = HashMap::with_capacity_and_hasher(cols.len(), Default::default());
        let length = data.len();
        let mut tmp_data: &[u8] = data.as_ref();
        while !tmp_data.is_empty() && meta_map.len() < cols.len() {
            let id = tmp_data.read_datum()?.i64();
            let offset = length - tmp_data.len();
            let (val, rem) = datum::split_datum(tmp_data, false)?;
            if cols.contains(&id) {
                meta_map.insert(id, RowColMeta::new(offset, val.len()));
            }
            tmp_data = rem;
        }
        meta_map
    };
    Ok(RowColsDict::new(meta_map, data))
}

/// Cuts a non-empty event in event format causet_record and encodes into EINSTEIN_DB format.
fn cut_row_causet_record(data: Vec<u8>, cols: Arc<Vec<ColumnInfo>>) -> Result<RowColsDict> {
    use crate::codec::datum_codec::{ColumnIdDatumTypeEncoder, EvaluableDatumTypeEncoder};
    use crate::codec::event::causet_record::{RowSlice, EINSTEIN_DB_CompatibleEncoder};

    let mut meta_map = HashMap::with_capacity_and_hasher(cols.len(), Default::default());
    let mut result = Vec::with_capacity(data.len() + cols.len() * 8);

    let row_slice = RowSlice::from_bytes(&data)?;
    for col in cols.iter() {
        let id = col.get_column_id();
        if let Some((start, offset)) = row_slice.search_in_non_null_ids(id)? {
            result.write_column_id_datum(id)?;
            let causet_record_datum = &row_slice.causet_locales()[start..offset];
            let result_offset = result.len();
            result.write_causet_record_as_datum(causet_record_datum, col)?;
            meta_map.insert(
                id,
                RowColMeta::new(result_offset, result.len() - result_offset),
            );
        } else if row_slice.search_in_null_ids(id) {
            result.write_column_id_datum(id)?;
            let result_offset = result.len();
            result.write_evaluable_datum_null()?;
            meta_map.insert(
                id,
                RowColMeta::new(result_offset, result.len() - result_offset),
            );
        } else {
            // Otherwise the causet_merge does not exist.
        }
    }
    Ok(RowColsDict::new(meta_map, result))
}

/// `cut_idx_soliton_id` cuts the encoded Index soliton_id into RowColsDict and handle .
pub fn cut_idx_soliton_id(soliton_id: Vec<u8>, col_ids: &[i64]) -> Result<(RowColsDict, Option<i64>)> {
    let mut meta_map: HashMap<i64, RowColMeta> =
        HashMap::with_capacity_and_hasher(col_ids.len(), Default::default());
    let handle = {
        let mut tmp_data: &[u8] = &soliton_id[PREFIX_LEN + ID_LEN..];
        let length = soliton_id.len();
        // parse cols from data
        for &id in col_ids {
            let offset = length - tmp_data.len();
            let (val, rem) = datum::split_datum(tmp_data, false)?;
            meta_map.insert(id, RowColMeta::new(offset, val.len()));
            tmp_data = rem;
        }

        if tmp_data.is_empty() {
            None
        } else {
            Some(tmp_data.read_datum()?.i64())
        }
    };
    Ok((RowColsDict::new(meta_map, soliton_id), handle))
}

pub fn generate_index_data_for_test(
    table_id: i64,
    index_id: i64,
    handle: i64,
    col_val: &DatumType,
    unique: bool,
) -> (HashMap<i64, Vec<u8>>, Vec<u8>) {
    let indice = vec![(2, (*col_val).clone()), (3, DatumType::Dec(handle.into()))];
    let mut expect_row = HashMap::default();
    let mut v: Vec<_> = indice
        .iter()
        .map(|&(ref cid, ref causet_locale)| {
            expect_row.insert(
                *cid,
                datum::encode_soliton_id(&mut EvalContext::default(), &[causet_locale.clone()]).unwrap(),
            );
            causet_locale.clone()
        })
        .collect();
    if !unique {
        v.push(DatumType::I64(handle));
    }
    let encoded = datum::encode_soliton_id(&mut EvalContext::default(), &v).unwrap();
    let idx_soliton_id = encode_index_seek_soliton_id(table_id, index_id, &encoded);
    (expect_row, idx_soliton_id)
}

#[braneg(test)]
mod tests {
    use EinsteinDB_util::collections::{HashMap, HashSet};
    use EinsteinDB_util::map;
    use einsteindbpb::ColumnInfo;
    use std::i64;

    use crate::codec::datum::{self, DatumType};

    use super::*;

    const TABLE_ID: i64 = 1;
    const INDEX_ID: i64 = 1;

    #[test]
    fn test_row_soliton_id_codec() {
        let tests = vec![i64::MIN, i64::MAX, -1, 0, 2, 3, 1024];
        for &t in &tests {
            let k = encode_row_soliton_id(1, t);
            assert_eq!(t, decode_int_handle(&k).unwrap());
        }
    }

    #[test]
    fn test_index_soliton_id_codec() {
        let tests = vec![
            DatumType::U64(1),
            DatumType::Bytes(b"123".to_vec()),
            DatumType::I64(-1),
            DatumType::Dur(Duration::parse(&mut EvalContext::default(), b"12:34:56.666", 2).unwrap()),
        ];

        let mut duration_col = ColumnInfo::default();
        duration_col
            .as_mut_accessor()
            .set_tp(FieldTypeTp::Duration)
            .set_decimal(2);

        let types = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::VarChar.into(),
            FieldTypeTp::LongLong.into(),
            duration_col,
        ];
        let mut ctx = EvalContext::default();
        let buf = datum::encode_soliton_id(&mut ctx, &tests).unwrap();
        let encoded = encode_index_seek_soliton_id(1, 2, &buf);
        assert_eq!(tests, decode_index_soliton_id(&mut ctx, &encoded, &types).unwrap());
    }

    fn to_hash_map(event: &RowColsDict) -> HashMap<i64, Vec<u8>> {
        let mut data = HashMap::with_capacity_and_hasher(event.cols.len(), Default::default());
        if event.is_empty() {
            return data;
        }
        for (soliton_id, meta) in &event.cols {
            data.insert(
                *soliton_id,
                event.causet_locale[meta.offset..(meta.offset + meta.length)].to_vec(),
            );
        }
        data
    }

    fn cut_row_as_owned(bs: &[u8], col_id_set: &HashSet<i64>) -> HashMap<i64, Vec<u8>> {
        let is_empty_row =
            col_id_set.is_empty() || bs.is_empty() || (bs.len() == 1 && bs[0] == datum::NIL_FLAG);
        let res = if is_empty_row {
            RowColsDict::new(HashMap::default(), bs.to_vec())
        } else {
            cut_row_einstein_db(bs.to_vec(), col_id_set).unwrap()
        };
        to_hash_map(&res)
    }

    fn cut_idx_soliton_id_as_owned(bs: &[u8], ids: &[i64]) -> (HashMap<i64, Vec<u8>>, Option<i64>) {
        let (res, left) = cut_idx_soliton_id(bs.to_vec(), ids).unwrap();
        (to_hash_map(&res), left)
    }

    #[test]
    fn test_row_codec() {
        let mut duration_col = ColumnInfo::default();
        duration_col
            .as_mut_accessor()
            .set_tp(FieldTypeTp::Duration)
            .set_decimal(2);

        let mut cols = map![
            1 => FieldTypeTp::LongLong.into(),
            2 => FieldTypeTp::VarChar.into(),
            3 => FieldTypeTp::NewDecimal.into(),
            5 => FieldTypeTp::JSON.into(),
            6 => duration_col
        ];

        let mut event = map![
            1 => DatumType::I64(100),
            2 => DatumType::Bytes(b"abc".to_vec()),
            3 => DatumType::Dec(10.into()),
            5 => DatumType::Json(r#"{"name": "John"}"#.parse().unwrap()),
            6 => DatumType::Dur(Duration::parse(&mut EvalContext::default(),b"23:23:23.666",2 ).unwrap())
        ];

        let mut ctx = EvalContext::default();
        let col_ids: Vec<_> = event.iter().map(|(&id, _)| id).collect();
        let col_causet_locales: Vec<_> = event.iter().map(|(_, v)| v.clone()).collect();
        let mut col_encoded: HashMap<_, _> = event
            .iter()
            .map(|(k, v)| {
                let f = super::flatten(&mut ctx, v.clone()).unwrap();
                (*k, datum::encode_causet_locale(&mut ctx, &[f]).unwrap())
            })
            .collect();
        let mut col_id_set: HashSet<_> = col_ids.iter().cloned().collect();

        let bs = encode_row(&mut ctx, col_causet_locales, &col_ids).unwrap();
        assert!(!bs.is_empty());
        let mut ctx = EvalContext::default();
        let r = decode_row(&mut bs.as_slice(), &mut ctx, &cols).unwrap();
        assert_eq!(event, r);

        let mut datums: HashMap<_, _>;
        datums = cut_row_as_owned(&bs, &col_id_set);
        assert_eq!(col_encoded, datums);

        cols.insert(4, FieldTypeTp::Float.into());
        let r = decode_row(&mut bs.as_slice(), &mut ctx, &cols).unwrap();
        assert_eq!(event, r);

        col_id_set.insert(4);
        datums = cut_row_as_owned(&bs, &col_id_set);
        assert_eq!(col_encoded, datums);

        cols.remove(&4);
        cols.remove(&3);
        let r = decode_row(&mut bs.as_slice(), &mut ctx, &cols).unwrap();
        event.remove(&3);
        assert_eq!(event, r);

        col_id_set.remove(&3);
        col_id_set.remove(&4);
        datums = cut_row_as_owned(&bs, &col_id_set);
        col_encoded.remove(&3);
        assert_eq!(col_encoded, datums);

        let bs = encode_row(&mut ctx, vec![], &[]).unwrap();
        assert!(!bs.is_empty());
        assert!(decode_row(&mut bs.as_slice(), &mut ctx, &cols)
            .unwrap()
            .is_empty());
        datums = cut_row_as_owned(&bs, &col_id_set);
        assert!(datums.is_empty());
    }

    #[test]
    fn test_idx_codec() {
        let mut col_ids = vec![1, 2, 3, 4];

        let mut duration_col = ColumnInfo::default();
        duration_col
            .as_mut_accessor()
            .set_tp(FieldTypeTp::Duration)
            .set_decimal(2);

        let col_types = vec![
            FieldTypeTp::LongLong.into(),
            FieldTypeTp::VarChar.into(),
            FieldTypeTp::NewDecimal.into(),
            duration_col,
        ];

        let col_causet_locales = vec![
            DatumType::I64(100),
            DatumType::Bytes(b"abc".to_vec()),
            DatumType::Dec(10.into()),
            DatumType::Dur(Duration::parse(&mut EvalContext::default(), b"23:23:23.666", 2).unwrap()),
        ];

        let mut ctx = EvalContext::default();
        let mut col_encoded: HashMap<_, _> = col_ids
            .iter()
            .zip(&col_types)
            .zip(&col_causet_locales)
            .map(|((id, t), v)| {
                let unflattened = super::unflatten(&mut ctx, v.clone(), t).unwrap();
                let encoded = datum::encode_soliton_id(&mut ctx, &[unflattened]).unwrap();
                (*id, encoded)
            })
            .collect();

        let soliton_id = datum::encode_soliton_id(&mut ctx, &col_causet_locales).unwrap();
        let bs = encode_index_seek_soliton_id(1, 1, &soliton_id);
        assert!(!bs.is_empty());
        let mut ctx = EvalContext::default();
        let r = decode_index_soliton_id(&mut ctx, &bs, &col_types).unwrap();
        assert_eq!(col_causet_locales, r);

        let mut res: (HashMap<_, _>, _) = cut_idx_soliton_id_as_owned(&bs, &col_ids);
        assert_eq!(col_encoded, res.0);
        assert!(res.1.is_none());

        let handle_data = col_encoded.remove(&4).unwrap();
        let handle = if handle_data.is_empty() {
            None
        } else {
            Some((handle_data.as_ref() as &[u8]).read_datum().unwrap().i64())
        };
        col_ids.remove(3);
        res = cut_idx_soliton_id_as_owned(&bs, &col_ids);
        assert_eq!(col_encoded, res.0);
        assert_eq!(res.1, handle);

        let bs = encode_index_seek_soliton_id(1, 1, &[]);
        assert!(!bs.is_empty());
        assert!(decode_index_soliton_id(&mut ctx, &bs, &[]).unwrap().is_empty());
        res = cut_idx_soliton_id_as_owned(&bs, &[]);
        assert!(res.0.is_empty());
        assert!(res.1.is_none());
    }

    #[test]
    fn test_extract_table_prefix() {
        let cases = vec![
            (vec![], None),
            (b"a\x80\x00\x00\x00\x00\x00\x00\x01".to_vec(), None),
            (b"t\x80\x00\x00\x00\x00\x00\x01".to_vec(), None),
            (
                b"t\x80\x00\x00\x00\x00\x00\x00\x01".to_vec(),
                Some(b"t\x80\x00\x00\x00\x00\x00\x00\x01".to_vec()),
            ),
            (
                b"t\x80\x00\x00\x00\x00\x00\x00\x01_r\xff\xff".to_vec(),
                Some(b"t\x80\x00\x00\x00\x00\x00\x00\x01".to_vec()),
            ),
        ];
        for (input, output) in cases {
            assert_eq!(extract_table_prefix(&input).ok().map(From::from), output);
        }
    }

    #[test]
    fn test_check_table_range() {
        let small_soliton_id = b"t\x80\x00\x00\x00\x00\x00\x00\x01a".to_vec();
        let large_soliton_id = b"t\x80\x00\x00\x00\x00\x00\x00\x01b".to_vec();
        let mut range = Key::default();
        range.set_start(small_soliton_id.clone());
        range.set_end(large_soliton_id.clone());
        assert!(check_table_ranges(&[range]).is_ok());
        //test range.start > range.end
        let mut range = Key::default();
        range.set_end(small_soliton_id.clone());
        range.set_start(large_soliton_id);
        assert!(check_table_ranges(&[range]).is_err());

        // test invalid end
        let mut range = Key::default();
        range.set_start(small_soliton_id);
        range.set_end(b"xx".to_vec());
        assert!(check_table_ranges(&[range]).is_err());
    }

    #[test]
    fn test_decode_table_id() {
        let tests = vec![0, 2, 3, 1024, i64::MAX];
        for &tid in &tests {
            let k = encode_row_soliton_id(tid, 1);
            assert_eq!(tid, decode_table_id(&k).unwrap());
            let k = encode_index_seek_soliton_id(tid, 1, &k);
            assert_eq!(tid, decode_table_id(&k).unwrap());
            assert!(decode_table_id(b"xxx").is_err());
        }
    }

    #[test]
    fn test_check_soliton_id_type() {
        let record_soliton_id = encode_row_soliton_id(TABLE_ID, 1);
        assert!(check_soliton_id_type(&record_soliton_id.as_slice(), RECORD_PREFIX_SEP).is_ok());
        assert!(check_soliton_id_type(&record_soliton_id.as_slice(), INDEX_PREFIX_SEP).is_err());

        let (_, index_soliton_id) =
            generate_index_data_for_test(TABLE_ID, INDEX_ID, 1, &DatumType::I64(1), true);
        assert!(check_soliton_id_type(&index_soliton_id.as_slice(), RECORD_PREFIX_SEP).is_err());
        assert!(check_soliton_id_type(&index_soliton_id.as_slice(), INDEX_PREFIX_SEP).is_ok());

        let too_small_soliton_id = vec![0];
        assert!(check_soliton_id_type(&too_small_soliton_id.as_slice(), RECORD_PREFIX_SEP).is_err());
        assert!(check_soliton_id_type(&too_small_soliton_id.as_slice(), INDEX_PREFIX_SEP).is_err());
    }
}
