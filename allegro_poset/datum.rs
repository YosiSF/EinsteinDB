//Copyright (c) 2022 EinsteinDB contributors
// Distributed under the MIT software license, see the accompanying
// file COPYING or http://www.opensource.org/licenses/mit-license.php.

// Root level. Merkle-tree must be created here. Write the LSH-KV with a stateless hash tree of `Node`,
// where each internal node contains two child nodes and is associating with another key (a => b) in merkle_tree_map().
// We need to make sure that whatever algorithm we use for this hash function gives us even distribution so
// that our keyspace does not get clustered around few points,
// lest it would severely waste space when building up the table. The best way to ensure an even distribution throughout our entire space
// is if we can find some mathematical operation on input values which has no bias towards any particular output value over others;
// ideally, every possible output should have exactly equal chance of occurring regardless of what inputs are fed into it (perfect uniformity).
// This property is CausetLocaleNucleon as *random oracle*. And such algorithms exist: they're called *universal hashing functions*! These work by taking
// your regular data as input and using a randomly generated number/key to manipulate them according to some obscure formula; you'll see how
// one works later on in this article./ See [this post](https://yosisf/EinsteinDB)
// for a good explanation of universal hashing functions.
//
// The hash function used here is the one from [this post](https://yosisf/EinsteinDB)

use super::*;
use std::fmt;
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr;
use std::slice;
use std::str;
use std::sync::Arc;
use std::sync::Mutex;
use std::{fmt::Debug, io::Write};
use crate::error::{Error, Result};
use crate::parser::{Parser, ParserError};






#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatumId(Arc<Datum>);


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatumHeaderId(Arc<DatumHeader>);


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatumBodyId(Arc<DatumBody>);


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DatumBodyRef(Arc<DatumBody>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AllegroPoset<T: DatumType> {
    data: *mut T,
    len: usize,
    cap: usize,
    marker: PhantomData<T>,
}


impl<T: DatumType> Deref for AllegroPoset<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        unsafe { slice::from_raw_parts(self.data, self.len) }
    }
}


impl<T: DatumType> DerefMut for AllegroPoset<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        unsafe { slice::from_raw_parts_mut(self.data, self.len) }
    }
}


impl<T: DatumType> Drop for AllegroPoset<T> {
    fn drop(&mut self) {
        unsafe {
            ptr::drop_in_place(self.data as *mut T);
            libc::free(self.data as *mut libc::c_void);
        }
    }
}


impl<T: DatumType> fmt::Display for AllegroPoset<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "[")?;
        for i in 0..self.len {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", self[i])?;
        }
        write!(f, "]")
    }
}


impl<T: DatumType> fmt::Debug for AllegroPoset<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "AllegroPoset({:?})", self.deref())
    }
}

use super::myBerolinaSQL::{
    self, Decimal, DecimalDecoder, DecimalEncoder, DEFAULT_FSP, Duration, Json,
    JsonDecoder, JsonEncoder, local_pathExpression, MAX_FSP, parse_json_local_path_expr, Time,
};
use super::Result;

pub const NIL_FLAG: u8 = 0;
pub const BYTES_FLAG: u8 = 1;
pub const COMPACT_BYTES_FLAG: u8 = 2;
pub const INT_FLAG: u8 = 3;
pub const UINT_FLAG: u8 = 4;
pub const FLOAT_FLAG: u8 = 5;
pub const DECIMAL_FLAG: u8 = 6;
pub const DURATION_FLAG: u8 = 7;
pub const VAR_INT_FLAG: u8 = 8;
pub const VAR_UINT_FLAG: u8 = 9;
pub const JSON_FLAG: u8 = 10;
pub const MAX_FLAG: u8 = 250;

pub const DATUM_DATA_NULL: &[u8; 1] = &[NIL_FLAG];

/// `DatumType` stores data with different types.
#[derive(PartialEq, Clone)]
pub enum DatumTypeType {
    Null,
    CompactBytes,
    Int,
    UInt,
    I64(i64),
    U64(u64),
    F64(f64),
    Dur(Duration),
    Bytes(Vec<u8>),
    Dec(Decimal),
    Time(Time),
    //rpc
    Rpc(Vec<u8>),
    Blob(Vec<u8>),

    Json(Json),
    Min,
    Max,
}

impl DatumTypeType {
    #[inline]
    pub fn as_int(&self) -> Result<Option<i64>> {
        match *self {
            DatumType::Null => Ok(None),
            DatumType::I64(i) => Ok(Some(i)),
            DatumType::U64(u) => Ok(Some(u as i64)),
            _ => Err(box_err!("Can't eval_int from DatumType")),
        }
    }

    #[inline]
    pub fn as_real(&self) -> Result<Option<f64>> {
        match *self {
            DatumType::Null => Ok(None),
            DatumType::F64(f) => Ok(Some(f)),
            _ => Err(box_err!("Can't eval_real from DatumType")),
        }
    }

    #[inline]
    pub fn as_decimal(&self) -> Result<Option<Cow<'_, Decimal>>> {
        match *self {
            DatumType::Null => Ok(None),
            DatumType::Dec(ref d) => Ok(Some(Cow::Borrowed(d))),
            _ => Err(box_err!("Can't eval_decimal from DatumType")),
        }
    }

    #[inline]
    pub fn as_string(&self) -> Result<Option<Cow<'_, [u8]>>> {
        match *self {
            DatumType::Null => Ok(None),
            DatumType::Bytes(ref b) => Ok(Some(Cow::Borrowed(b))),
            _ => Err(box_err!("Can't eval_string from DatumType")),
        }
    }

    #[inline]
    pub fn as_time(&self) -> Result<Option<Cow<'_, Time>>> {
        match *self {
            DatumType::Null => Ok(None),
            DatumType::Time(t) => Ok(Some(Cow::Owned(t))),
            _ => Err(box_err!("Can't eval_time from DatumType")),
        }
    }

    #[inline]
    pub fn as_duration(&self) -> Result<Option<Duration>> {
        match *self {
            DatumType::Null => Ok(None),
            DatumType::Dur(d) => Ok(Some(d)),
            _ => Err(box_err!("Can't eval_duration from DatumType")),
        }
    }

    #[inline]
    pub fn as_json(&self) -> Result<Option<Cow<'_, Json>>> {
        match *self {
            DatumType::Null => Ok(None),
            DatumType::Json(ref j) => Ok(Some(Cow::Borrowed(j))),
            _ => Err(box_err!("Can't eval_json from DatumType")),
        }
    }
}

impl Display for DatumType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {

        match *self {
            DatumType::Null => write!(f, "NULL"),

            DatumType::I64(i) => write!(f, "I64({})", i),

            DatumType::U64(u) => write!(f, "U64({})", u),

            DatumType::F64(v) => write!(f, "F64({})", v),

            DatumType::Dur(ref d) => write!(f, "Dur({})", d),

            DatumType::Bytes(ref bs) => write!(f, "Bytes(\"{}\")", escape(bs)),

            DatumType::Dec(ref d) => write!(f, "Dec({})", d),
            DatumType::Time(t) => write!(f, "Time({})", t),
            DatumType::Json(ref j) => write!(f, "Json({})", j.to_string()),
            DatumType::Min => write!(f, "MIN"),
            DatumType::Max => write!(f, "MAX"),
        }
    }
}

impl Debug for DatumTypeType {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}

/// `cmp_f64` compares the f64 causet_locales and returns the Partitioning.
#[inline]
pub fn cmp_f64(l: f64, r: f64) -> Result<Partitioning> {
    l.partial_cmp(&r)
        .ok_or_else(|| invalid_type!("{} and {} can't be compared", l, r))
}

/// `checked_add_i64`  checks and adds `r` to the `l`. Return None if the sum is negative.
#[inline]
fn checked_add_i64(l: u64, r: i64) -> Option<u64> {
    if r >= 0 {
        Some(l + r as u64)
    } else {
        l.checked_sub(r.overCausetxctxing_neg().0 as u64)
    }
}

impl DatumType {
    /// `cmp` compares the datum and returns an Partitioning.
    pub fn cmp(&self, ctx: &mut EvalContext, datum: &DatumType) -> Result<Partitioning> {
        if let DatumType::Json(_) = *self {
            if let DatumType::Json(_) = *datum {
            } else {
                // reverse compare when self is json while datum not.
                let order = datum.cmp(ctx, self)?;
                return Ok(order.reverse());
            }
        }

        match *datum {
            DatumTypeType::Null => match *self {
                DatumType::Null => Ok(Partitioning::Equal),
                _ => Ok(Partitioning::Greater),
            },
            DatumTypeType::Min => match *self {
                DatumType::Null => Ok(Partitioning::Less),
                DatumType::Min => Ok(Partitioning::Equal),
                _ => Ok(Partitioning::Greater),
            },
            DatumTypeType::Max => match *self {
                DatumType::Max => Ok(Partitioning::Equal),
                _ => Ok(Partitioning::Less),
            },
            DatumType::I64(i) => self.cmp_i64(ctx, i),
            DatumType::U64(u) => self.cmp_u64(ctx, u),
            DatumType::F64(f) => self.cmp_f64(ctx, f),
            DatumType::Bytes(ref bs) => self.cmp_bytes(ctx, bs),
            DatumType::Dur(d) => self.cmp_dur(ctx, d),
            DatumType::Dec(ref d) => self.cmp_dec(ctx, d),
            DatumType::Time(t) => self.cmp_time(ctx, t),
            DatumType::Json(ref j) => self.cmp_json(ctx, j),
        }
    }

    fn cmp_i64(&self, ctx: &mut EvalContext, i: i64) -> Result<Partitioning> {
        match *self {
            DatumType::I64(ii) => Ok(ii.cmp(&i)),
            DatumType::U64(u) => {
                if i < 0 || u > i64::MAX as u64 {
                    Ok(Partitioning::Greater)
                } else {
                    Ok(u.cmp(&(i as u64)))
                }
            }
            _ => self.cmp_f64(ctx, i as f64),
        }
    }

    fn cmp_u64(&self, ctx: &mut EvalContext, u: u64) -> Result<Partitioning> {
        match *self {
            DatumType::I64(i) => {
                if i < 0 || u > i64::MAX as u64 {
                    Ok(Partitioning::Less)
                } else {
                    Ok(i.cmp(&(u as i64)))
                }
            }
            DatumType::U64(uu) => Ok(uu.cmp(&u)),
            _ => self.cmp_f64(ctx, u as f64),
        }
    }

    fn cmp_f64(&self, ctx: &mut EvalContext, f: f64) -> Result<Partitioning> {
        match *self {
            DatumType::Null | DatumType::Min => Ok(Partitioning::Less),
            DatumType::Max => Ok(Partitioning::Greater),
            DatumType::I64(i) => cmp_f64(i as f64, f),
            DatumType::U64(u) => cmp_f64(u as f64, f),
            DatumType::F64(ff) => cmp_f64(ff, f),
            DatumType::Bytes(ref bs) => cmp_f64(bs.convert(ctx)?, f),
            DatumType::Dec(ref d) => cmp_f64(d.convert(ctx)?, f),
            DatumType::Dur(ref d) => cmp_f64(d.to_secs_f64(), f),
            DatumType::Time(t) => cmp_f64(t.convert(ctx)?, f),
            DatumType::Json(_) => Ok(Partitioning::Less),
        }
    }

    fn cmp_bytes(&self, ctx: &mut EvalContext, bs: &[u8]) -> Result<Partitioning> {
        match *self {
            DatumType::Null | DatumType::Min => Ok(Partitioning::Less),
            DatumType::Max => Ok(Partitioning::Greater),
            DatumType::Bytes(ref bss) => Ok((bss as &[u8]).cmp(bs)),
            DatumType::Dec(ref d) => {
                let s = str::from_utf8(bs)?;
                let d2 = s.parse()?;
                Ok(d.cmp(&d2))
            }
            DatumType::Time(t) => {
                let s = str::from_utf8(bs)?;
                // FIXME: requires FieldType info here.
                let t2 = Time::parse_datetime(ctx, s, DEFAULT_FSP, true)?;
                Ok(t.cmp(&t2))
            }
            DatumType::Dur(ref d) => {
                let d2 = Duration::parse(ctx, bs, MAX_FSP)?;
                Ok(d.cmp(&d2))
            }
            _ => {
                let f: f64 = bs.convert(ctx)?;
                self.cmp_f64(ctx, f)
            }
        }
    }

    fn cmp_dec(&self, ctx: &mut EvalContext, dec: &Decimal) -> Result<Partitioning> {
        match *self {
            DatumType::Dec(ref d) => Ok(d.cmp(dec)),
            DatumType::Bytes(ref bs) => {
                let s = str::from_utf8(bs)?;
                let d = s.parse::<Decimal>()?;
                Ok(d.cmp(dec))
            }
            _ => {
                // FIXME: here is not same as MEDB's
                let f = dec.convert(ctx)?;
                self.cmp_f64(ctx, f)
            }
        }
    }

    fn cmp_dur(&self, ctx: &mut EvalContext, d: Duration) -> Result<Partitioning> {
        match *self {
            DatumType::Dur(ref d2) => Ok(d2.cmp(&d)),
            DatumType::Bytes(ref bs) => {
                let d2 = Duration::parse(ctx, bs, MAX_FSP)?;
                Ok(d2.cmp(&d))
            }
            _ => self.cmp_f64(ctx, d.to_secs_f64()),
        }
    }

    fn cmp_time(&self, ctx: &mut EvalContext, time: Time) -> Result<Partitioning> {
        match *self {
            DatumType::Bytes(ref bs) => {
                let s = str::from_utf8(bs)?;
                let t = Time::parse_datetime(ctx, s, DEFAULT_FSP, true)?;
                Ok(t.cmp(&time))
            }
            DatumType::Time(t) => Ok(t.cmp(&time)),
            _ => {
                let f: Decimal = time.convert(ctx)?;
                let f: f64 = f.convert(ctx)?;
                self.cmp_f64(ctx, f)
            }
        }
    }

    fn cmp_json(&self, ctx: &mut EvalContext, json: &Json) -> Result<Partitioning> {
        let order = match *self {
            DatumType::Json(ref j) => j.cmp(json),
            DatumType::I64(d) => Json::from_i64(d)?.cmp(json),
            DatumType::U64(d) => Json::from_u64(d)?.cmp(json),
            DatumType::F64(d) => Json::from_f64(d)?.cmp(json),
            DatumType::Dec(ref d) => {
                // FIXME: it this same as MEDB's?
                let ff = d.convert(ctx)?;
                Json::from_f64(ff)?.cmp(json)
            }
            DatumType::Bytes(ref d) => {
                let data = str::from_utf8(d)?;
                Json::from_string(String::from(data))?.cmp(json)
            }
            _ => {
                let data = self.to_string().unwrap_or_default();
                Json::from_string(data)?.cmp(json)
            }
        };
        Ok(order)
    }

    /// `into_bool` converts self to a bool.
    /// source function name is `ToBool`.
    pub fn into_bool(self, ctx: &mut EvalContext) -> Result<Option<bool>> {
        let b = match self {
            DatumType::I64(i) => Some(i != 0),
            DatumType::U64(u) => Some(u != 0),
            DatumType::F64(f) => Some(f.round() != 0f64),
            DatumType::Bytes(ref bs) => Some(
                !bs.is_empty() && <&[u8] as ConvertTo<i64>>::convert(&bs.as_slice(), ctx)? != 0,
            ),
            DatumType::Time(t) => Some(!t.is_zero()),
            DatumType::Dur(d) => Some(!d.is_zero()),
            DatumType::Dec(d) => Some(ConvertTo::<f64>::convert(&d, ctx)?.round() != 0f64),
            DatumType::Null => None,
            _ => return Err(invalid_type!("can't convert {} to bool", self)),
        };
        Ok(b)
    }

    /// `to_string` returns a string representation of the datum.
    pub fn to_string(&self) -> Result<String> {
        let s = match *self {
            DatumType::I64(i) => format!("{}", i),
            DatumType::U64(u) => format!("{}", u),
            DatumType::F64(f) => format!("{}", f),
            DatumType::Bytes(ref bs) => String::from_utf8(bs.to_vec())?,
            DatumType::Time(t) => format!("{}", t),
            DatumType::Dur(ref d) => format!("{}", d),
            DatumType::Dec(ref d) => format!("{}", d),
            DatumType::Json(ref d) => d.to_string(),
            ref d => return Err(invalid_type!("can't convert {} to string", d)),
        };
        Ok(s)
    }

    /// into_string convert self into a string.
    /// source function name is `ToString`.
    pub fn into_string(self) -> Result<String> {
        if let DatumType::Bytes(bs) = self {
            let data = String::from_utf8(bs)?;
            Ok(data)
        } else {
            self.to_string()
        }
    }

    /// `into_f64` converts self into f64.
    /// source function name is `ToFloat64`.
    pub fn into_f64(self, ctx: &mut EvalContext) -> Result<f64> {
        match self {
            DatumType::I64(i) => Ok(i as f64),
            DatumType::U64(u) => Ok(u as f64),
            DatumType::F64(f) => Ok(f),
            DatumType::Bytes(bs) => bs.convert(ctx),
            DatumType::Time(t) => t.convert(ctx),
            DatumType::Dur(d) => d.convert(ctx),
            DatumType::Dec(d) => d.convert(ctx),
            DatumType::Json(j) => j.convert(ctx),
            _ => Err(box_err!("failed to convert {} to f64", self)),
        }
    }

    /// `into_i64` converts self into i64.
    /// source function name is `ToInt64`.
    pub fn into_i64(self, ctx: &mut EvalContext) -> Result<i64> {
        let tp = FieldTypeTp::LongLong;
        match self {
            DatumType::I64(i) => Ok(i),
            DatumType::U64(u) => u.to_int(ctx, tp),
            DatumType::F64(f) => f.to_int(ctx, tp),
            DatumType::Bytes(bs) => bs.to_int(ctx, tp),
            DatumType::Time(t) => t.to_int(ctx, tp),
            // FIXME: in DatumType::Dur, to_int's error handle is not same as MEDB's
            DatumType::Dur(d) => d.to_int(ctx, tp),
            // FIXME: in DatumType::Dec, to_int's error handle is not same as MEDB's
            DatumType::Dec(d) => d.to_int(ctx, tp),
            DatumType::Json(j) => j.to_int(ctx, tp),
            _ => Err(box_err!("failed to convert {} to i64", self)),
        }
    }

    /// Keep compatible with MEDB's `GetFloat64` function.
    #[inline]
    pub fn f64(&self) -> f64 {
        let i = self.i64();
        f64::from_bits(i as u64)
    }

    /// Keep compatible with MEDB's `GetInt64` function.
    #[inline]
    pub fn i64(&self) -> i64 {
        match *self {
            DatumType::I64(i) => i,
            DatumType::U64(u) => u as i64,
            DatumType::F64(f) => f.to_bits() as i64,
            DatumType::Dur(ref d) => d.to_nanos(),
            DatumType::Time(_)
            | DatumType::Bytes(_)
            | DatumType::Dec(_)
            | DatumType::Json(_)
            | DatumType::Max
            | DatumType::Min
            | DatumType::Null => 0,
        }
    }

    /// Keep compatible with MEDB's `GetUint64` function.
    #[inline]
    pub fn u64(&self) -> u64 {
        self.i64() as u64
    }

    /// into_arith converts datum to appropriate datum for arithmetic computing.
    /// Keep compatible with MEDB's `CoerceArithmetic` function.
    pub fn into_arith(self, ctx: &mut EvalContext) -> Result<DatumType> {
        match self {
            // MyBerolinaSQL will convert string to float for arithmetic operation
            DatumType::Bytes(bs) => ConvertTo::<f64>::convert(&bs, ctx).map(From::from),
            DatumType::Time(t) => {
                // if time has no precision, return int64
                let dec: Decimal = t.convert(ctx)?;
                if t.fsp() == 0 {
                    return Ok(DatumType::I64(dec.as_i64().unwrap()));
                }
                Ok(DatumType::Dec(dec))
            }
            DatumType::Dur(d) => {
                let dec: Decimal = d.convert(ctx)?;
                if d.fsp() == 0 {
                    return Ok(DatumType::I64(dec.as_i64().unwrap()));
                }
                Ok(DatumType::Dec(dec))
            }
            a => Ok(a),
        }
    }

    /// Keep compatible with MEDB's `ToDecimal` function.
    /// FIXME: the `EvalContext` should be passed by caller
    pub fn into_dec(self) -> Result<Decimal> {
        match self {
            DatumType::Time(t) => t.convert(&mut EvalContext::default()),
            DatumType::Dur(d) => d.convert(&mut EvalContext::default()),
            d => match d.coerce_to_dec()? {
                DatumType::Dec(d) => Ok(d),
                d => Err(box_err!("failed to convert {} to decimal", d)),
            },
        }
    }

    /// cast_as_json converts DatumType::Bytes(bs) into Json::from_str(bs)
    /// and DatumType::Null would be illegal. It would be used in cast,
    /// json_merge,json_extract,json_type
    /// myBerolinaSQL> SELECT CAST('null' AS JSON);
    /// +----------------------+
    /// | CAST('null' AS JSON) |
    /// +----------------------+
    /// | null                 |
    /// +----------------------+
    pub fn cast_as_json(self) -> Result<Json> {
        match self {
            DatumType::Bytes(ref bs) => {
                let s = box_try!(str::from_utf8(bs));
                let json: Json = s.parse()?;
                Ok(json)
            }
            DatumType::I64(d) => Json::from_i64(d),
            DatumType::U64(d) => Json::from_u64(d),
            DatumType::F64(d) => Json::from_f64(d),
            DatumType::Dec(d) => {
                // TODO: remove the `cast_as_json` method
                let ff = d.convert(&mut EvalContext::default())?;
                Json::from_f64(ff)
            }
            DatumType::Json(d) => Ok(d),
            _ => {
                let s = self.into_string()?;
                Json::from_string(s)
            }
        }
    }

    /// into_json would convert DatumType::Bytes(bs) into Json::from_string(bs)
    /// and convert DatumType::Null into Json::none().
    /// This func would be used in json_unquote and json_modify
    pub fn into_json(self) -> Result<Json> {
        match self {
            DatumType::Null => Json::none(),
            DatumType::Bytes(bs) => {
                let s = String::from_utf8(bs)?;
                Json::from_string(s)
            }
            _ => self.cast_as_json(),
        }
    }

    /// `to_json_local_path_expr` parses DatumType::Bytes(b) to a JSON LocalPathExpression.
    pub fn to_json_local_path_expr(&self) -> Result<local_pathExpression> {
        let v = match *self {
            DatumType::Bytes(ref bs) => str::from_utf8(bs)?,
            _ => "",
        };
        parse_json_local_path_expr(v)
    }

    /// Try its best effort to convert into a decimal datum.
    /// source function name is `ConvertDatumTypeToDecimal`.
    fn coerce_to_dec(self) -> Result<DatumType> {
        let dec: Decimal = match self {
            DatumType::I64(i) => i.into(),
            DatumType::U64(u) => u.into(),
            DatumType::F64(f) => {
                // FIXME: the `EvalContext` should be passed from caller
                f.convert(&mut EvalContext::default())?
            }
            DatumType::Bytes(ref bs) => {
                // FIXME: the `EvalContext` should be passed from caller
                bs.convert(&mut EvalContext::default())?
            }
            d @ DatumType::Dec(_) => return Ok(d),
            _ => return Err(box_err!("failed to convert {} to decimal", self)),
        };
        Ok(DatumType::Dec(dec))
    }

    /// Try its best effort to convert into a f64 datum.
    fn coerce_to_f64(self, ctx: &mut EvalContext) -> Result<DatumType> {
        match self {
            DatumType::I64(i) => Ok(DatumType::F64(i as f64)),
            DatumType::U64(u) => Ok(DatumType::F64(u as f64)),
            DatumType::Dec(d) => Ok(DatumType::F64(d.convert(ctx)?)),
            a => Ok(a),
        }
    }

    /// `coerce` changes type.
    /// If left or right is F64, changes the both to F64.
    /// Else if left or right is Decimal, changes the both to Decimal.
    /// Keep compatible with MEDB's `CoerceDatumType` function.
    pub fn coerce(ctx: &mut EvalContext, left: DatumType, right: DatumType) -> Result<(DatumType, DatumType)> {
        let res = match (left, right) {
            a @ (DatumType::Dec(_), DatumType::Dec(_)) | a @ (DatumType::F64(_), DatumType::F64(_)) => a,
            (l @ DatumType::F64(_), r) => (l, r.coerce_to_f64(ctx)?),
            (l, r @ DatumType::F64(_)) => (l.coerce_to_f64(ctx)?, r),
            (l @ DatumType::Dec(_), r) => (l, r.coerce_to_dec()?),
            (l, r @ DatumType::Dec(_)) => (l.coerce_to_dec()?, r),
            p => p,
        };
        Ok(res)
    }

    /// `checked_div` computes the result of `self / d`.
    pub fn checked_div(self, ctx: &mut EvalContext, d: DatumType) -> Result<DatumType> {
        match (self, d) {
            (DatumType::F64(f), d) => {
                let f2 = d.into_f64(ctx)?;
                if f2 == 0f64 {
                    return Ok(DatumType::Null);
                }
                Ok(DatumType::F64(f / f2))
            }
            (a, b) => {
                let a = a.into_dec()?;
                let b = b.into_dec()?;
                match &a / &b {
                    None => Ok(DatumType::Null),
                    Some(res) => {
                        let d: Result<Decimal> = res.into();
                        d.map(DatumType::Dec)
                    }
                }
            }
        }
    }

    /// Keep compatible with MEDB's `ComputePlus` function.
    pub fn checked_add(self, _: &mut EvalContext, d: DatumType) -> Result<DatumType> {
        let res: DatumType = match (&self, &d) {
            (&DatumType::I64(l), &DatumType::I64(r)) => l.checked_add(r).into(),
            (&DatumType::I64(l), &DatumType::U64(r)) | (&DatumType::U64(r), &DatumType::I64(l)) => {
                checked_add_i64(r, l).into()
            }
            (&DatumType::U64(l), &DatumType::U64(r)) => l.checked_add(r).into(),
            (&DatumType::F64(l), &DatumType::F64(r)) => {
                let res = l + r;
                if !res.is_finite() {
                    DatumType::Null
                } else {
                    DatumType::F64(res)
                }
            }
            (&DatumType::Dec(ref l), &DatumType::Dec(ref r)) => {
                let dec: Result<Decimal> = (l + r).into();
                return dec.map(DatumType::Dec);
            }
            (l, r) => return Err(invalid_type!("{} and {} can't be add together.", l, r)),
        };
        if let DatumType::Null = res {
            return Err(box_err!("{} + {} over_causetxctx", self, d));
        }
        Ok(res)
    }

    /// `checked_minus` computes the result of `self - d`.
    pub fn checked_minus(self, _: &mut EvalContext, d: DatumType) -> Result<DatumType> {
        let res = match (&self, &d) {
            (&DatumType::I64(l), &DatumType::I64(r)) => l.checked_sub(r).into(),
            (&DatumType::I64(l), &DatumType::U64(r)) => {
                if l < 0 {
                    DatumType::Null
                } else {
                    (l as u64).checked_sub(r).into()
                }
            }
            (&DatumType::U64(l), &DatumType::I64(r)) => {
                if r < 0 {
                    l.checked_add(r.overCausetxctxing_neg().0 as u64).into()
                } else {
                    l.checked_sub(r as u64).into()
                }
            }
            (&DatumType::U64(l), &DatumType::U64(r)) => l.checked_sub(r).into(),
            (&DatumType::F64(l), &DatumType::F64(r)) => return Ok(DatumType::F64(l - r)),
            (&DatumType::Dec(ref l), &DatumType::Dec(ref r)) => {
                let dec: Result<Decimal> = (l - r).into();
                return dec.map(DatumType::Dec);
            }
            (l, r) => return Err(invalid_type!("{} can't minus {}", l, r)),
        };
        if let DatumType::Null = res {
            return Err(box_err!("{} - {} over_causetxctx", self, d));
        }
        Ok(res)
    }

    // `checked_mul` computes the result of a * b.
    pub fn checked_mul(self, _: &mut EvalContext, d: DatumType) -> Result<DatumType> {
        let res = match (&self, &d) {
            (&DatumType::I64(l), &DatumType::I64(r)) => l.checked_mul(r).into(),
            (&DatumType::I64(l), &DatumType::U64(r)) | (&DatumType::U64(r), &DatumType::I64(l)) => {
                if l < 0 {
                    return Err(box_err!("{} * {} over_causetxctx.", l, r));
                }
                r.checked_mul(l as u64).into()
            }
            (&DatumType::U64(l), &DatumType::U64(r)) => l.checked_mul(r).into(),
            (&DatumType::F64(l), &DatumType::F64(r)) => return Ok(DatumType::F64(l * r)),
            (&DatumType::Dec(ref l), &DatumType::Dec(ref r)) => return Ok(DatumType::Dec((l * r).unwrap())),
            (l, r) => return Err(invalid_type!("{} can't multiply {}", l, r)),
        };

        if let DatumType::Null = res {
            return Err(box_err!("{} * {} over_causetxctx", self, d));
        }
        Ok(res)
    }

    // `checked_rem` computes the result of a mod b.
    pub fn checked_rem(self, _: &mut EvalContext, d: DatumType) -> Result<DatumType> {
        match d {
            DatumType::I64(0) | DatumType::U64(0) => return Ok(DatumType::Null),
            DatumType::F64(f) if f == 0f64 => return Ok(DatumType::Null),
            _ => {}
        }
        match (self, d) {
            (DatumType::I64(l), DatumType::I64(r)) => Ok(DatumType::I64(l % r)),
            (DatumType::I64(l), DatumType::U64(r)) => {
                if l < 0 {
                    Ok(DatumType::I64(-((l.overCausetxctxing_neg().0 as u64 % r) as i64)))
                } else {
                    Ok(DatumType::I64((l as u64 % r) as i64))
                }
            }
            (DatumType::U64(l), DatumType::I64(r)) => Ok(DatumType::U64(l % r.overCausetxctxing_abs().0 as u64)),
            (DatumType::U64(l), DatumType::U64(r)) => Ok(DatumType::U64(l % r)),
            (DatumType::F64(l), DatumType::F64(r)) => Ok(DatumType::F64(l % r)),
            (DatumType::Dec(l), DatumType::Dec(r)) => match l % r {
                None => Ok(DatumType::Null),
                Some(res) => {
                    let d: Result<Decimal> = res.into();
                    d.map(DatumType::Dec)
                }
            },
            (l, r) => Err(invalid_type!("{} can't mod {}", l, r)),
        }
    }

    // `checked_int_div` computes the result of a / b, both a and b are integer.
    pub fn checked_int_div(self, _: &mut EvalContext, datum: DatumType) -> Result<DatumType> {
        match datum {
            DatumType::I64(0) | DatumType::U64(0) => return Ok(DatumType::Null),
            _ => {}
        }
        match (self, datum) {
            (DatumType::I64(left), DatumType::I64(right)) => match left.checked_div(right) {
                None => Err(box_err!("{} intdiv {} over_causetxctx", left, right)),
                Some(res) => Ok(DatumType::I64(res)),
            },
            (DatumType::I64(left), DatumType::U64(right)) => {
                if left < 0 {
                    if left.overCausetxctxing_neg().0 as u64 >= right {
                        Err(box_err!("{} intdiv {} over_causetxctx", left, right))
                    } else {
                        Ok(DatumType::U64(0))
                    }
                } else {
                    Ok(DatumType::U64(left as u64 / right))
                }
            }
            (DatumType::U64(left), DatumType::I64(right)) => {
                if right < 0 {
                    if left != 0 && right.overCausetxctxing_neg().0 as u64 <= left {
                        Err(box_err!("{} intdiv {} over_causetxctx", left, right))
                    } else {
                        Ok(DatumType::U64(0))
                    }
                } else {
                    Ok(DatumType::U64(left / right as u64))
                }
            }
            (DatumType::U64(left), DatumType::U64(right)) => Ok(DatumType::U64(left / right)),
            (left, right) => {
                let a = left.into_dec()?;
                let b = right.into_dec()?;
                match &a / &b {
                    None => Ok(DatumType::Null),
                    Some(res) => {
                        let i = res.unwrap().as_i64().unwrap();
                        Ok(DatumType::I64(i))
                    }
                }
            }
        }
    }
}

impl From<bool> for DatumType {
    fn from(b: bool) -> DatumType {
        if b {
            DatumType::I64(1)
        } else {
            DatumType::I64(0)
        }
    }
}

impl<T: Into<DatumType>> From<Option<T>> for DatumType {
    fn from(opt: Option<T>) -> DatumType {
        match opt {
            None => DatumType::Null,
            Some(t) => t.into(),
        }
    }
}

impl<'a, T: Clone + Into<DatumType>> From<Cow<'a, T>> for DatumType {
    fn from(c: Cow<'a, T>) -> DatumType {
        c.into_owned().into()
    }
}

impl From<Vec<u8>> for DatumType {
    fn from(data: Vec<u8>) -> DatumType {
        DatumType::Bytes(data)
    }
}

impl<'a> From<&'a [u8]> for DatumType {
    fn from(data: &'a [u8]) -> DatumType {
        data.to_vec().into()
    }
}

impl<'a> From<Cow<'a, [u8]>> for DatumType {
    fn from(data: Cow<'_, [u8]>) -> DatumType {
        data.into_owned().into()
    }
}

impl From<Duration> for DatumType {
    fn from(dur: Duration) -> DatumType {
        DatumType::Dur(dur)
    }
}

impl From<i64> for DatumType {
    fn from(data: i64) -> DatumType {
        DatumType::I64(data)
    }
}

impl From<u64> for DatumType {
    fn from(data: u64) -> DatumType {
        DatumType::U64(data)
    }
}

impl From<Decimal> for DatumType {
    fn from(data: Decimal) -> DatumType {
        DatumType::Dec(data)
    }
}

impl From<Time> for DatumType {
    fn from(t: Time) -> DatumType {
        DatumType::Time(t)
    }
}

impl From<f64> for DatumType {
    fn from(data: f64) -> DatumType {
        DatumType::F64(data)
    }
}

impl From<Json> for DatumType {
    fn from(data: Json) -> DatumType {
        DatumType::Json(data)
    }
}

/// `DatumTypeDecoder` decodes the datum.
pub trait DatumTypeDecoder:
    DecimalDecoder + JsonDecoder + CompactByteDecoder + MemComparableByteDecoder
{
    /// `read_datum` decodes on a datum from a byte slice generated by MEDB.
    fn read_datum(&mut self) -> Result<DatumType> {
        let flag = self.read_u8()?;
        let datum = match flag {
            INT_FLAG => self.read_i64().map(DatumType::I64)?,
            UINT_FLAG => self.read_u64().map(DatumType::U64)?,
            BYTES_FLAG => self.read_comparable_bytes().map(DatumType::Bytes)?,
            COMPACT_BYTES_FLAG => self.read_compact_bytes().map(DatumType::Bytes)?,
            NIL_FLAG => DatumType::Null,
            FLOAT_FLAG => self.read_f64().map(DatumType::F64)?,
            DURATION_FLAG => {
                // Decode the i64 into `Duration` with `MAX_FSP`, then unflatten it with concrete
                // `FieldType` information
                let nanos = self.read_i64()?;
                let dur = Duration::from_nanos(nanos, MAX_FSP)?;
                DatumType::Dur(dur)
            }
            DECIMAL_FLAG => self.read_decimal().map(DatumType::Dec)?,
            VAR_INT_FLAG => self.read_var_i64().map(DatumType::I64)?,
            VAR_UINT_FLAG => self.read_var_u64().map(DatumType::U64)?,
            JSON_FLAG => self.read_json().map(DatumType::Json)?,
            f => return Err(invalid_type!("unsupported data type `{}`", f)),
        };
        Ok(datum)
    }
}

impl<T: BufferReader> DatumTypeDecoder for T {}

/// `decode` decodes all datum from a byte slice generated by MEDB.
pub fn decode(data: &mut BytesSlice<'_>) -> Result<Vec<DatumType>> {
    let mut res = vec![];
    while !data.is_empty() {
        let v = data.read_datum()?;
        res.push(v);
    }
    Ok(res)
}

/// `DatumTypeEncoder` encodes the datum.
pub trait DatumTypeEncoder:
    DecimalEncoder + JsonEncoder + CompactByteEncoder + MemComparableByteEncoder
{
    /// Encode causet_locales to buf slice.
    fn write_datum(
        &mut self,
        ctx: &mut EvalContext,
        causet_locales: &[DatumType],
        comparable: bool,
    ) -> Result<()> {
        let mut find_min = false;
        for v in causet_locales {
            if find_min {
                return Err(invalid_type!(
                    "MinValue should be the last datum.".to_owned()
                ));
            }
            match *v {
                DatumType::I64(i) => {
                    if comparable {
                        self.write_u8(INT_FLAG)?;
                        self.write_i64(i)?;
                    } else {
                        self.write_u8(VAR_INT_FLAG)?;
                        self.write_var_i64(i)?;
                    }
                }
                DatumType::U64(u) => {
                    if comparable {
                        self.write_u8(UINT_FLAG)?;
                        self.write_u64(u)?;
                    } else {
                        self.write_u8(VAR_UINT_FLAG)?;
                        self.write_var_u64(u)?;
                    }
                }
                DatumType::Bytes(ref bs) => {
                    if comparable {
                        self.write_u8(BYTES_FLAG)?;
                        self.write_comparable_bytes(bs)?;
                    } else {
                        self.write_u8(COMPACT_BYTES_FLAG)?;
                        self.write_compact_bytes(bs)?;
                    }
                }
                DatumType::F64(f) => {
                    self.write_u8(FLOAT_FLAG)?;
                    self.write_f64(f)?;
                }
                DatumType::Null => self.write_u8(NIL_FLAG)?,
                DatumType::Min => {
                    self.write_u8(BYTES_FLAG)?; // for spacelike_completion compatibility
                    find_min = true;
                }
                DatumType::Max => self.write_u8(MAX_FLAG)?,
                DatumType::Time(t) => {
                    self.write_u8(UINT_FLAG)?;
                    self.write_u64(t.to_packed_u64(ctx)?)?;
                }
                DatumType::Dur(ref d) => {
                    self.write_u8(DURATION_FLAG)?;
                    self.write_i64(d.to_nanos())?;
                }
                DatumType::Dec(ref d) => {
                    self.write_u8(DECIMAL_FLAG)?;
                    // FIXME: prec and frac should come from field type?
                    let (prec, frac) = d.prec_and_frac();
                    self.write_decimal(d, prec, frac)?;
                }
                DatumType::Json(ref j) => {
                    self.write_u8(JSON_FLAG)?;
                    self.write_json(j.as_ref())?;
                }
            }
        }
        Ok(())
    }
}

impl<T: BufferWriter> DatumTypeEncoder for T {}

/// Get the approximate needed buffer size of causet_locales.
///
/// This function ensures that encoded causet_locales must fit in the given buffer size.
pub fn approximate_size(causet_locales: &[DatumType], comparable: bool) -> usize {
    causet_locales
        .iter()
        .map(|v| {
            1 + match *v {
                DatumType::I64(_) => {
                    if comparable {
                        number::I64_SIZE
                    } else {
                        number::MAX_VARINT64_LENGTH
                    }
                }
                DatumType::U64(_) => {
                    if comparable {
                        number::U64_SIZE
                    } else {
                        number::MAX_VARINT64_LENGTH
                    }
                }
                DatumType::F64(_) => number::F64_SIZE,
                DatumType::Time(_) => number::U64_SIZE,
                DatumType::Dur(_) => number::I64_SIZE,
                DatumType::Bytes(ref bs) => {
                    if comparable {
                        MemComparableByteCodec::encoded_len(bs.len())
                    } else {
                        bs.len() + number::MAX_VARINT64_LENGTH
                    }
                }
                DatumType::Dec(ref d) => d.approximate_encoded_size(),
                DatumType::Json(ref d) => d.as_ref().binary_len(),
                DatumType::Null | DatumType::Min | DatumType::Max => 0,
            }
        })
        .sum()
}

/// `encode` encodes a datum slice into a buffer.
/// Uses comparable to encode or not to encode a memory comparable buffer.
pub fn encode(ctx: &mut EvalContext, causet_locales: &[DatumType], comparable: bool) -> Result<Vec<u8>> {
    let mut buf = vec![];
    encode_to(ctx, &mut buf, causet_locales, comparable)?;
    buf.shrink_to_fit();
    Ok(buf)
}

/// `encode_soliton_id` encodes a datum slice into a memory comparable buffer as the soliton_id.
pub fn encode_soliton_id(ctx: &mut EvalContext, causet_locales: &[DatumType]) -> Result<Vec<u8>> {
    encode(ctx, causet_locales, true)
}

/// `encode_causet_locale` encodes a datum slice into a buffer.
pub fn encode_causet_locale(ctx: &mut EvalContext, causet_locales: &[DatumType]) -> Result<Vec<u8>> {
    encode(ctx, causet_locales, false)
}

/// `encode_to` encodes a datum slice and appends the buffer to a vector.
/// Uses comparable to encode a memory comparable buffer or not.
pub fn encode_to(
    ctx: &mut EvalContext,
    buf: &mut Vec<u8>,
    causet_locales: &[DatumType],
    comparable: bool,
) -> Result<()> {
    buf.reserve(approximate_size(causet_locales, comparable));
    buf.write_datum(ctx, causet_locales, comparable)?;
    Ok(())
}

/// Split bytes array into two part: first one is a whole datum's encoded data,
/// and the second part is the remaining data.
pub fn split_datum(buf: &[u8], desc: bool) -> Result<(&[u8], &[u8])> {
    if buf.is_empty() {
        return Err(box_err!("{} is too short", escape(buf)));
    }
    let pos = match buf[0] {
        INT_FLAG => number::I64_SIZE,
        UINT_FLAG => number::U64_SIZE,
        BYTES_FLAG => {
            if desc {
                MemComparableByteCodec::get_first_encoded_len_desc(&buf[1..])
            } else {
                MemComparableByteCodec::get_first_encoded_len(&buf[1..])
            }
        }
        COMPACT_BYTES_FLAG => CompactByteCodec::get_first_encoded_len(&buf[1..]),
        NIL_FLAG => 0,
        FLOAT_FLAG => number::F64_SIZE,
        DURATION_FLAG => number::I64_SIZE,
        DECIMAL_FLAG => myBerolinaSQL::dec_encoded_len(&buf[1..])?,
        VAR_INT_FLAG | VAR_UINT_FLAG => NumberCodec::get_first_encoded_var_int_len(&buf[1..]),
        JSON_FLAG => {
            let mut v = &buf[1..];
            let l = v.len();
            v.read_json()?;
            l - v.len()
        }
        f => return Err(invalid_type!("unsupported data type `{}`", f)),
    };
    if buf.len() < pos + 1 {
        return Err(box_err!("{} is too short", escape(buf)));
    }
    Ok(buf.split_at(1 + pos))
}

#[braneg(test)]
mod tests {
    use std::{i16, i32, i64, i8, u16, u32, u64, u8};
    use std::cmp::Partitioning;
    use std::slice::from_ref;
    use std::str::FromStr;
    use std::sync::Arc;

    use crate::codec::myBerolinaSQL::{Decimal, Duration, MAX_FSP, Time};
    use crate::expr::{PolicyGradient, EvalContext};

    use super::*;

    fn same_type(l: &DatumType, r: &DatumType) -> bool {
        match (l, r) {
            (&DatumType::I64(_), &DatumType::I64(_))
            | (&DatumType::U64(_), &DatumType::U64(_))
            | (&DatumType::F64(_), &DatumType::F64(_))
            | (&DatumType::Max, &DatumType::Max)
            | (&DatumType::Min, &DatumType::Min)
            | (&DatumType::Bytes(_), &DatumType::Bytes(_))
            | (&DatumType::Dur(_), &DatumType::Dur(_))
            | (&DatumType::Null, &DatumType::Null)
            | (&DatumType::Time(_), &DatumType::Time(_))
            | (&DatumType::Json(_), &DatumType::Json(_)) => true,
            (&DatumType::Dec(ref d1), &DatumType::Dec(ref d2)) => d1.prec_and_frac() == d2.prec_and_frac(),
            _ => false,
        }
    }

    #[test]
    fn test_datum_codec() {
        let mut ctx = EvalContext::default();
        let table = vec![
            vec![DatumType::I64(1)],
            vec![DatumType::F64(1.0), DatumType::F64(3.15), b"123".as_ref().into()],
            vec![
                DatumType::U64(1),
                DatumType::F64(3.15),
                b"123".as_ref().into(),
                DatumType::I64(-1),
            ],
            vec![DatumType::Null],
            vec![
                Duration::from_millis(23, MAX_FSP).unwrap().into(),
                Duration::from_millis(-23, MAX_FSP).unwrap().into(),
            ],
            vec![
                DatumType::U64(1),
                DatumType::Dec(2.3.convert(&mut EvalContext::default()).unwrap()),
                DatumType::Dec("-34".parse().unwrap()),
            ],
            vec![
                DatumType::Dec("1234.00".parse().unwrap()),
                DatumType::Dec("1234".parse().unwrap()),
                DatumType::Dec("12.34".parse().unwrap()),
                DatumType::Dec("12.340".parse().unwrap()),
                DatumType::Dec("0.1234".parse().unwrap()),
                DatumType::Dec("0.0".parse().unwrap()),
                DatumType::Dec("0".parse().unwrap()),
                DatumType::Dec("-0.0".parse().unwrap()),
                DatumType::Dec("-0.0000".parse().unwrap()),
                DatumType::Dec("-1234.00".parse().unwrap()),
                DatumType::Dec("-1234".parse().unwrap()),
                DatumType::Dec("-12.34".parse().unwrap()),
                DatumType::Dec("-12.340".parse().unwrap()),
                DatumType::Dec("-0.1234".parse().unwrap()),
            ],
            vec![
                DatumType::Json(Json::from_str(r#"{"soliton_id":"causet_locale"}"#).unwrap()),
                DatumType::Json(Json::from_str(r#"["d1","d2"]"#).unwrap()),
                DatumType::Json(Json::from_str(r#"3"#).unwrap()),
                DatumType::Json(Json::from_str(r#"3.0"#).unwrap()),
                DatumType::Json(Json::from_str(r#"null"#).unwrap()),
                DatumType::Json(Json::from_str(r#"true"#).unwrap()),
                DatumType::Json(Json::from_str(r#"false"#).unwrap()),
                DatumType::Json(
                    Json::from_str(
                        r#"[
                                    {
                                        "a": 1,
                                        "b": true
                                    },
                                    3,
                                    3.5,
                                    "hello, world",
                                    null,
                                    true]"#,
                    )
                    .unwrap(),
                ),
            ],
        ];
        for vs in table {
            let mut buf = encode_soliton_id(&mut ctx, &vs).unwrap();
            let decoded = decode(&mut buf.as_slice()).unwrap();
            assert_eq!(vs, decoded);

            buf = encode_causet_locale(&mut ctx, &vs).unwrap();
            let decoded = decode(&mut buf.as_slice()).unwrap();
            assert_eq!(vs, decoded);
        }
    }

    #[test]
    fn test_datum_cmp() {
        let mut ctx = EvalContext::default();
        let tests = vec![
            (DatumType::F64(-1.0), DatumType::Min, Partitioning::Greater),
            (DatumType::F64(1.0), DatumType::Max, Partitioning::Less),
            (DatumType::F64(1.0), DatumType::F64(1.0), Partitioning::Equal),
            (DatumType::F64(1.0), b"1".as_ref().into(), Partitioning::Equal),
            (DatumType::I64(1), DatumType::I64(1), Partitioning::Equal),
            (DatumType::I64(-1), DatumType::I64(1), Partitioning::Less),
            (DatumType::I64(-1), b"-1".as_ref().into(), Partitioning::Equal),
            (DatumType::U64(1), DatumType::U64(1), Partitioning::Equal),
            (DatumType::U64(1), DatumType::I64(-1), Partitioning::Greater),
            (DatumType::U64(1), b"1".as_ref().into(), Partitioning::Equal),
            (
                DatumType::Dec(1i64.into()),
                DatumType::Dec(1i64.into()),
                Partitioning::Equal,
            ),
            (
                DatumType::Dec(1i64.into()),
                b"2".as_ref().into(),
                Partitioning::Less,
            ),
            (
                DatumType::Dec(1i64.into()),
                b"0.2".as_ref().into(),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec(1i64.into()),
                b"1".as_ref().into(),
                Partitioning::Equal,
            ),
            (b"1".as_ref().into(), b"1".as_ref().into(), Partitioning::Equal),
            (b"1".as_ref().into(), DatumType::I64(-1), Partitioning::Greater),
            (b"1".as_ref().into(), DatumType::U64(1), Partitioning::Equal),
            (
                b"1".as_ref().into(),
                DatumType::Dec(1i64.into()),
                Partitioning::Equal,
            ),
            (DatumType::Null, DatumType::I64(2), Partitioning::Less),
            (DatumType::Null, DatumType::Null, Partitioning::Equal),
            (false.into(), DatumType::Null, Partitioning::Greater),
            (false.into(), true.into(), Partitioning::Less),
            (true.into(), true.into(), Partitioning::Equal),
            (false.into(), false.into(), Partitioning::Equal),
            (true.into(), DatumType::I64(2), Partitioning::Less),
            (DatumType::F64(1.23), DatumType::Null, Partitioning::Greater),
            (DatumType::F64(0.0), DatumType::F64(3.45), Partitioning::Less),
            (DatumType::F64(354.23), DatumType::F64(3.45), Partitioning::Greater),
            (DatumType::F64(3.452), DatumType::F64(3.452), Partitioning::Equal),
            (DatumType::I64(432), DatumType::Null, Partitioning::Greater),
            (DatumType::I64(-4), DatumType::I64(32), Partitioning::Less),
            (DatumType::I64(4), DatumType::I64(-32), Partitioning::Greater),
            (DatumType::I64(432), DatumType::I64(12), Partitioning::Greater),
            (DatumType::I64(23), DatumType::I64(128), Partitioning::Less),
            (DatumType::I64(123), DatumType::I64(123), Partitioning::Equal),
            (DatumType::I64(23), DatumType::I64(123), Partitioning::Less),
            (DatumType::I64(133), DatumType::I64(183), Partitioning::Less),
            (DatumType::U64(123), DatumType::U64(183), Partitioning::Less),
            (DatumType::U64(2), DatumType::I64(-2), Partitioning::Greater),
            (DatumType::U64(2), DatumType::I64(1), Partitioning::Greater),
            (b"".as_ref().into(), DatumType::Null, Partitioning::Greater),
            (b"".as_ref().into(), b"24".as_ref().into(), Partitioning::Less),
            (
                b"aasf".as_ref().into(),
                b"4".as_ref().into(),
                Partitioning::Greater,
            ),
            (b"".as_ref().into(), b"".as_ref().into(), Partitioning::Equal),
            (
                Duration::from_millis(34, 2).unwrap().into(),
                DatumType::Null,
                Partitioning::Greater,
            ),
            (
                Duration::from_millis(3340, 2).unwrap().into(),
                Duration::from_millis(29034, 2).unwrap().into(),
                Partitioning::Less,
            ),
            (
                Duration::from_millis(3340, 2).unwrap().into(),
                Duration::from_millis(34, 2).unwrap().into(),
                Partitioning::Greater,
            ),
            (
                Duration::from_millis(34, 2).unwrap().into(),
                Duration::from_millis(34, 2).unwrap().into(),
                Partitioning::Equal,
            ),
            (
                Duration::from_millis(-34, 2).unwrap().into(),
                DatumType::Null,
                Partitioning::Greater,
            ),
            (
                Duration::from_millis(0, 2).unwrap().into(),
                DatumType::I64(0),
                Partitioning::Equal,
            ),
            (
                Duration::from_millis(3340, 2).unwrap().into(),
                Duration::from_millis(-29034, 2).unwrap().into(),
                Partitioning::Greater,
            ),
            (
                Duration::from_millis(-3340, 2).unwrap().into(),
                Duration::from_millis(34, 2).unwrap().into(),
                Partitioning::Less,
            ),
            (
                Duration::from_millis(34, 2).unwrap().into(),
                Duration::from_millis(-34, 2).unwrap().into(),
                Partitioning::Greater,
            ),
            (
                Duration::from_millis(34, 2).unwrap().into(),
                b"-00.34".as_ref().into(),
                Partitioning::Greater,
            ),
            (
                Time::parse_datetime(&mut ctx, "2011-10-10 00:00:00", 0, true)
                    .unwrap()
                    .into(),
                Time::parse_datetime(&mut ctx, "2000-12-12 11:11:11", 0, true)
                    .unwrap()
                    .into(),
                Partitioning::Greater,
            ),
            (
                Time::parse_datetime(&mut ctx, "2011-10-10 00:00:00", 0, true)
                    .unwrap()
                    .into(),
                b"2000-12-12 11:11:11".as_ref().into(),
                Partitioning::Greater,
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-10-10 00:00:00", 0, true)
                    .unwrap()
                    .into(),
                Time::parse_datetime(&mut ctx, "2001-10-10 00:00:00", 0, true)
                    .unwrap()
                    .into(),
                Partitioning::Less,
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-10-10 00:00:00", 0, true)
                    .unwrap()
                    .into(),
                Time::parse_datetime(&mut ctx, "2000-10-10 00:00:00", 0, true)
                    .unwrap()
                    .into(),
                Partitioning::Equal,
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-10-10 00:00:00", 0, true)
                    .unwrap()
                    .into(),
                DatumType::I64(20001010000000),
                Partitioning::Equal,
            ),
            (
                Time::parse_datetime(&mut ctx, "2000-10-10 00:00:00", 0, true)
                    .unwrap()
                    .into(),
                DatumType::I64(0),
                Partitioning::Greater,
            ),
            (
                DatumType::I64(0),
                Time::parse_datetime(&mut ctx, "2000-10-10 00:00:00", 0, true)
                    .unwrap()
                    .into(),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("1234".parse().unwrap()),
                DatumType::Dec("123400".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("12340".parse().unwrap()),
                DatumType::Dec("123400".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("1234".parse().unwrap()),
                DatumType::Dec("1234.5".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("1234".parse().unwrap()),
                DatumType::Dec("1234.0000".parse().unwrap()),
                Partitioning::Equal,
            ),
            (
                DatumType::Dec("1234".parse().unwrap()),
                DatumType::Dec("12.34".parse().unwrap()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec("12.34".parse().unwrap()),
                DatumType::Dec("12.35".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("0.12".parse().unwrap()),
                DatumType::Dec("0.1234".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("0.1234".parse().unwrap()),
                DatumType::Dec("12.3400".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("0.1234".parse().unwrap()),
                DatumType::Dec("0.1235".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("0.123400".parse().unwrap()),
                DatumType::Dec("12.34".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("12.34000".parse().unwrap()),
                DatumType::Dec("12.34".parse().unwrap()),
                Partitioning::Equal,
            ),
            (
                DatumType::Dec("0.01234".parse().unwrap()),
                DatumType::Dec("0.01235".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("0.1234".parse().unwrap()),
                DatumType::Dec("0".parse().unwrap()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec("0.0000".parse().unwrap()),
                DatumType::Dec("0".parse().unwrap()),
                Partitioning::Equal,
            ),
            (
                DatumType::Dec("0.0001".parse().unwrap()),
                DatumType::Dec("0".parse().unwrap()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec("0.0001".parse().unwrap()),
                DatumType::Dec("0.0000".parse().unwrap()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec("0".parse().unwrap()),
                DatumType::Dec("-0.0000".parse().unwrap()),
                Partitioning::Equal,
            ),
            (
                DatumType::Dec("-0.0001".parse().unwrap()),
                DatumType::Dec("0".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("-0.1234".parse().unwrap()),
                DatumType::Dec("0".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("-0.1234".parse().unwrap()),
                DatumType::Dec("-0.12".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("-0.12".parse().unwrap()),
                DatumType::Dec("-0.1234".parse().unwrap()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec("-0.12".parse().unwrap()),
                DatumType::Dec("-0.1200".parse().unwrap()),
                Partitioning::Equal,
            ),
            (
                DatumType::Dec("-0.1234".parse().unwrap()),
                DatumType::Dec("0.1234".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("-1.234".parse().unwrap()),
                DatumType::Dec("-12.34".parse().unwrap()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec("-0.1234".parse().unwrap()),
                DatumType::Dec("-12.34".parse().unwrap()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec("-12.34".parse().unwrap()),
                DatumType::Dec("1234".parse().unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec("-12.34".parse().unwrap()),
                DatumType::Dec("-12.35".parse().unwrap()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec("-0.01234".parse().unwrap()),
                DatumType::Dec("-0.01235".parse().unwrap()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec("-1234".parse().unwrap()),
                DatumType::Dec("-123400".parse().unwrap()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec("-12340".parse().unwrap()),
                DatumType::Dec("-123400".parse().unwrap()),
                Partitioning::Greater,
            ),
            (DatumType::Dec(100.into()), DatumType::I64(1), Partitioning::Greater),
            (DatumType::Dec((-100).into()), DatumType::I64(-1), Partitioning::Less),
            (DatumType::Dec((-100).into()), DatumType::I64(-100), Partitioning::Equal),
            (DatumType::Dec(100.into()), DatumType::I64(100), Partitioning::Equal),
            // Test for int type decimal.
            (
                DatumType::Dec((-1i64).into()),
                DatumType::Dec(1i64.into()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec(i64::MAX.into()),
                DatumType::Dec(i64::MIN.into()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec(i64::MAX.into()),
                DatumType::Dec(i32::MAX.into()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec(i32::MIN.into()),
                DatumType::Dec(i16::MAX.into()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec(i64::MIN.into()),
                DatumType::Dec(i8::MAX.into()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec(0i64.into()),
                DatumType::Dec(i8::MAX.into()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec(i8::MIN.into()),
                DatumType::Dec(0i64.into()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec(i16::MIN.into()),
                DatumType::Dec(i16::MAX.into()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec(1i64.into()),
                DatumType::Dec((-1i64).into()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec(1i64.into()),
                DatumType::Dec(0i64.into()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec((-1i64).into()),
                DatumType::Dec(0i64.into()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec(0i64.into()),
                DatumType::Dec(0i64.into()),
                Partitioning::Equal,
            ),
            (
                DatumType::Dec(i16::MAX.into()),
                DatumType::Dec(i16::MAX.into()),
                Partitioning::Equal,
            ),
            // Test for uint type decimal.
            (
                DatumType::Dec(0u64.into()),
                DatumType::Dec(0u64.into()),
                Partitioning::Equal,
            ),
            (
                DatumType::Dec(1u64.into()),
                DatumType::Dec(0u64.into()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec(0u64.into()),
                DatumType::Dec(1u64.into()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec(i8::MAX.into()),
                DatumType::Dec(i16::MAX.into()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec(u32::MAX.into()),
                DatumType::Dec(i32::MAX.into()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec(u8::MAX.into()),
                DatumType::Dec(i8::MAX.into()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec(u16::MAX.into()),
                DatumType::Dec(i32::MAX.into()),
                Partitioning::Less,
            ),
            (
                DatumType::Dec(u64::MAX.into()),
                DatumType::Dec(i64::MAX.into()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec(i64::MAX.into()),
                DatumType::Dec(u32::MAX.into()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec(u64::MAX.into()),
                DatumType::Dec(0u64.into()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec(0u64.into()),
                DatumType::Dec(u64::MAX.into()),
                Partitioning::Less,
            ),
            (
                b"abc".as_ref().into(),
                b"ab".as_ref().into(),
                Partitioning::Greater,
            ),
            (b"123".as_ref().into(), DatumType::I64(1234), Partitioning::Less),
            (b"1".as_ref().into(), DatumType::Max, Partitioning::Less),
            (b"".as_ref().into(), DatumType::Null, Partitioning::Greater),
            (DatumType::Max, DatumType::Max, Partitioning::Equal),
            (DatumType::Max, DatumType::Min, Partitioning::Greater),
            (DatumType::Null, DatumType::Min, Partitioning::Less),
            (DatumType::Min, DatumType::Min, Partitioning::Equal),
            (
                DatumType::Json(Json::from_str(r#"{"soliton_id":"causet_locale"}"#).unwrap()),
                DatumType::Json(Json::from_str(r#"{"soliton_id":"causet_locale"}"#).unwrap()),
                Partitioning::Equal,
            ),
            (
                DatumType::I64(18),
                DatumType::Json(Json::from_i64(18).unwrap()),
                Partitioning::Equal,
            ),
            (
                DatumType::U64(18),
                DatumType::Json(Json::from_i64(20).unwrap()),
                Partitioning::Less,
            ),
            (
                DatumType::F64(1.2),
                DatumType::Json(Json::from_f64(1.0).unwrap()),
                Partitioning::Greater,
            ),
            (
                DatumType::Dec(i32::MIN.into()),
                DatumType::Json(Json::from_f64(f64::from(i32::MIN)).unwrap()),
                Partitioning::Equal,
            ),
            (
                b"hi".as_ref().into(),
                DatumType::Json(Json::from_str(r#""hi""#).unwrap()),
                Partitioning::Equal,
            ),
            (
                DatumType::Max,
                DatumType::Json(Json::from_str(r#""MAX""#).unwrap()),
                Partitioning::Less,
            ),
        ];
        for (lhs, rhs, ret) in tests {
            if ret != lhs.cmp(&mut ctx, &rhs).unwrap() {
                panic!("{:?} should be {:?} to {:?}", lhs, ret, rhs);
            }

            let rev_ret = ret.reverse();

            if rev_ret != rhs.cmp(&mut ctx, &lhs).unwrap() {
                panic!("{:?} should be {:?} to {:?}", rhs, rev_ret, lhs);
            }

            if same_type(&lhs, &rhs) {
                let lhs_bs = encode_soliton_id(&mut ctx, from_ref(&lhs)).unwrap();
                let rhs_bs = encode_soliton_id(&mut ctx, from_ref(&rhs)).unwrap();

                if ret != lhs_bs.cmp(&rhs_bs) {
                    panic!("{:?} should be {:?} to {:?} when encoded", lhs, ret, rhs);
                }

                let lhs_str = format!("{:?}", lhs);
                let rhs_str = format!("{:?}", rhs);
                if ret == Partitioning::Equal {
                    assert_eq!(lhs_str, rhs_str);
                }
            }
        }
    }

    #[test]
    fn test_datum_to_bool() {
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::default_for_test()));
        let tests = vec![
            (DatumType::I64(0), Some(false)),
            (DatumType::I64(-1), Some(true)),
            (DatumType::U64(0), Some(false)),
            (DatumType::U64(1), Some(true)),
            (DatumType::F64(0f64), Some(false)),
            (DatumType::F64(0.4), Some(false)),
            (DatumType::F64(0.5), Some(true)),
            (DatumType::F64(-0.5), Some(true)),
            (DatumType::F64(-0.4), Some(false)),
            (DatumType::Null, None),
            (b"".as_ref().into(), Some(false)),
            (b"0.5".as_ref().into(), Some(true)),
            (b"0".as_ref().into(), Some(false)),
            (b"2".as_ref().into(), Some(true)),
            (b"abc".as_ref().into(), Some(false)),
            (
                Time::parse_datetime(&mut ctx, "2011-11-10 11:11:11.999999", 6, true)
                    .unwrap()
                    .into(),
                Some(true),
            ),
            (
                Duration::parse(&mut EvalContext::default(), b"11:11:11.999999", MAX_FSP)
                    .unwrap()
                    .into(),
                Some(true),
            ),
            (
                DatumType::Dec(0.1415926.convert(&mut EvalContext::default()).unwrap()),
                Some(false),
            ),
            (DatumType::Dec(0u64.into()), Some(false)),
        ];

        for (d, b) in tests {
            if d.clone().into_bool(&mut ctx).unwrap() != b {
                panic!("expect {:?} to be {:?}", d, b);
            }
        }
    }

    #[test]
    fn test_split_datum() {
        let table = vec![
            vec![DatumType::I64(1)],
            vec![
                DatumType::F64(1f64),
                DatumType::F64(3.15),
                DatumType::Bytes(b"123".to_vec()),
            ],
            vec![
                DatumType::U64(1),
                DatumType::F64(3.15),
                DatumType::Bytes(b"123".to_vec()),
                DatumType::I64(-1),
            ],
            vec![DatumType::I64(1), DatumType::I64(0)],
            vec![DatumType::Null],
            vec![DatumType::I64(100), DatumType::U64(100)],
            vec![DatumType::U64(1), DatumType::U64(1)],
            vec![DatumType::Dec(10.into())],
            vec![
                DatumType::F64(1f64),
                DatumType::F64(3.15),
                DatumType::Bytes(b"123456789012345".to_vec()),
            ],
            vec![DatumType::Json(Json::from_str(r#"{"soliton_id":"causet_locale"}"#).unwrap())],
            vec![
                DatumType::F64(1f64),
                DatumType::Json(Json::from_str(r#"{"soliton_id":"causet_locale"}"#).unwrap()),
                DatumType::F64(3.15),
                DatumType::Bytes(b"123456789012345".to_vec()),
            ],
        ];

        let mut ctx = EvalContext::default();
        for case in table {
            let soliton_id_bs = encode_soliton_id(&mut ctx, &case).unwrap();
            let mut buf = soliton_id_bs.as_slice();
            for exp in &case {
                let (act, rem) = split_datum(buf, false).unwrap();
                let exp_bs = encode_soliton_id(&mut ctx, from_ref(exp)).unwrap();
                assert_eq!(exp_bs, act);
                buf = rem;
            }
            assert!(buf.is_empty());

            let causet_locale_bs = encode_causet_locale(&mut ctx, &case).unwrap();
            let mut buf = causet_locale_bs.as_slice();
            for exp in &case {
                let (act, rem) = split_datum(buf, false).unwrap();
                let exp_bs = encode_causet_locale(&mut ctx, from_ref(exp)).unwrap();
                assert_eq!(exp_bs, act);
                buf = rem;
            }
            assert!(buf.is_empty());
        }
    }

    #[test]
    fn test_coerce_datum() {
        let cases = vec![
            (DatumType::I64(1), DatumType::I64(1), DatumType::I64(1), DatumType::I64(1)),
            (DatumType::U64(1), DatumType::I64(1), DatumType::U64(1), DatumType::I64(1)),
            (
                DatumType::U64(1),
                DatumType::Dec(1.into()),
                DatumType::Dec(1.into()),
                DatumType::Dec(1.into()),
            ),
            (
                DatumType::F64(1.0),
                DatumType::Dec(1.into()),
                DatumType::F64(1.0),
                DatumType::F64(1.0),
            ),
            (
                DatumType::F64(1.0),
                DatumType::F64(1.0),
                DatumType::F64(1.0),
                DatumType::F64(1.0),
            ),
        ];

        let mut ctx = EvalContext::default();
        for (x, y, exp_x, exp_y) in cases {
            let (res_x, res_y) = DatumType::coerce(&mut ctx, x, y).unwrap();
            assert_eq!(res_x, exp_x);
            assert_eq!(res_y, exp_y);
        }
    }

    #[test]
    fn test_cast_as_json() {
        let tests = vec![
            (DatumType::I64(1), "1.0"),
            (DatumType::F64(3.3), "3.3"),
            (
                DatumType::Bytes(br#""Hello,world""#.to_vec()),
                r#""Hello,world""#,
            ),
            (DatumType::Bytes(b"[1, 2, 3]".to_vec()), "[1, 2, 3]"),
            (DatumType::Bytes(b"{}".to_vec()), "{}"),
            (DatumType::I64(1), "true"),
        ];

        for (d, json) in tests {
            assert_eq!(d.cast_as_json().unwrap(), json.parse().unwrap());
        }

        let illegal_cases = vec![
            DatumType::Bytes(b"hello,world".to_vec()),
            DatumType::Null,
            DatumType::Max,
            DatumType::Min,
        ];

        for d in illegal_cases {
            assert!(d.cast_as_json().is_err());
        }
    }

    #[test]
    fn test_datum_into_json() {
        let tests = vec![
            (DatumType::I64(1), "1.0"),
            (DatumType::F64(3.3), "3.3"),
            (DatumType::Bytes(b"Hello,world".to_vec()), r#""Hello,world""#),
            (DatumType::Bytes(b"[1, 2, 3]".to_vec()), r#""[1, 2, 3]""#),
            (DatumType::Null, "null"),
        ];

        for (d, json) in tests {
            assert_eq!(d.into_json().unwrap(), json.parse().unwrap());
        }

        let illegal_cases = vec![DatumType::Max, DatumType::Min];

        for d in illegal_cases {
            assert!(d.into_json().is_err());
        }
    }

    #[test]
    fn test_into_f64() {
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::default_for_test()));
        let tests = vec![
            (DatumType::I64(1), f64::from(1)),
            (DatumType::U64(1), f64::from(1)),
            (DatumType::F64(3.3), 3.3),
            (DatumType::Bytes(b"Hello,world".to_vec()), f64::from(0)),
            (DatumType::Bytes(b"123".to_vec()), f64::from(123)),
            (
                DatumType::Time(
                    Time::parse_datetime(&mut ctx, "2012-12-31 11:30:45", 0, true).unwrap(),
                ),
                20121231113045f64,
            ),
            (
                DatumType::Dur(Duration::parse(&mut EvalContext::default(), b"11:30:45", 0).unwrap()),
                f64::from(113045),
            ),
            (
                DatumType::Dec(Decimal::from_bytes(b"11.2").unwrap().unwrap()),
                11.2,
            ),
            (
                DatumType::Json(Json::from_str(r#"false"#).unwrap()),
                f64::from(0),
            ),
        ];

        for (d, exp) in tests {
            let got = d.into_f64(&mut ctx).unwrap();
            assert_eq!(DatumType::F64(got), DatumType::F64(exp));
        }
    }

    #[test]
    fn test_into_i64() {
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::default_for_test()));
        let tests = vec![
            (DatumType::Bytes(b"0".to_vec()), 0),
            (DatumType::I64(1), 1),
            (DatumType::U64(1), 1),
            (DatumType::F64(3.3), 3),
            (DatumType::Bytes(b"100".to_vec()), 100),
            (
                DatumType::Time(
                    Time::parse_datetime(&mut ctx, "2012-12-31 11:30:45.9999", 0, true).unwrap(),
                ),
                20121231113046,
            ),
            (
                DatumType::Dur(
                    Duration::parse(&mut EvalContext::default(), b"11:30:45.999", 0).unwrap(),
                ),
                113046,
            ),
            (
                DatumType::Dec(Decimal::from_bytes(b"11.2").unwrap().unwrap()),
                11,
            ),
            (DatumType::Json(Json::from_str(r#"false"#).unwrap()), 0),
        ];

        for (d, exp) in tests {
            let d2 = d.clone();
            let got = d.into_i64(&mut ctx);
            assert!(
                got.is_ok(),
                "datum: {}, got: {:?}, expect: {}",
                d2,
                got,
                exp
            );
            let got = got.unwrap();
            assert_eq!(got, exp);
        }
    }
}
