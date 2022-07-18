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

use std::collections::HashMap;
use std::hash::Hash;
use std::iter::FromIterator;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;



//! # Convert
//! Convert between different types of data.
//!
//! ## Example
//! ```
//! use einstein_db::convert::*;
//!
//! let mut a = [1, 2, 3, 4, 5];
//! let mut b = [0; 5];
//!
//! // Convert from [u8; 5] to [i32; 5]
//! convert_from_u8_to_i32(&mut a, &mut b);
//!


/// Convert from [u8; 5] to [i32; 5]
/// ```
/// use einstein_db::convert::*;
/// let mut a = [1, 2, 3, 4, 5];
/// let mut b = [0; 5];
/// convert_from_u8_to_i32(&mut a, &mut b);
/// ```
/// #### ResultExtract
/// ```
/// use einstein_db::convert::*;
/// let mut a = [1, 2, 3, 4, 5];
/// let mut b = [0; 5];
/// convert_from_u8_to_i32(&mut a, &mut b);
/// ```

use std::convert::TryInto;
use crate::poset::*;
use crate::poset::node::*;
use crate::poset::node::node_type::*;

///! Convert from [u8; 5] to [i32; 5] to [u8; 5]
/// This function is used to convert from a byte array to a byte array.
/// We treat causets and tuples as the same type of data.
/// In essence, we treat them as a byte array.
/// If for example, there is a stack of causets, we can convert it to a stack of byte arrays.
/// This is useful when we want to store the stack of causets in a database.
/// We can then later on retrieve the stack of causets from the database and convert it back to a stack of causets.
pub fn with_causet_assertion_in_timelike_convert_from_u8_to_i32(a: &mut [u8], b: &mut [i32]) {
    let mut i = 0;
    for x in a.iter() {
        if i >= b.len() {
            let mut c = [0; 5]; //[0, 0, 0, 0, 0];
            break;
        }
        b[i] = *x as i32;
        i += 1;
    }
}




/// Convert from [i32; 5] to [u8; 5]

pub fn with_causet_assertion_in_timelike_convert_from_i32_to_u8(a: &mut [i32], b: &mut [u8]) {
    let mut i = 0;
    for x in a.iter() {
        if i >= b.len() {
            [0; 5]; //[0, 0, 0, 0, 0];
            break;
        }
        b[i] = *x as u8;
        i += 1;
    }
}


//! ```
//! use einstein_db::convert::*;
//! use einstein_db::error::*;
//! use einstein_db::poset::*;




//! ```
//!     let mut a = [1, 2, 3, 4, 5];
//!    let mut b = [0; 5];
//!   let mut c = [0; 5];
//!
//! // Convert from [u8; 5] to [i32; 5]
//! convert_from_u8_to_i32(&mut a, &mut b);
//!
//! // Convert from [i32; 5] to [u8; 5]
//! convert_from_i32_to_u8(&mut b, &mut c);
//!


pub fn convert_from_u8_to_i32(a: &mut [u8], b: &mut [i32]) -> Result<(), Error> {
    if (a.len() * 4) != b.len() {
        return Err(Error::new(ErrorKind::Other, "convert_from_u8_to_i32: a.len() * 4 != b.len()"));
    }
    let mut i = 0;
    for x in a.iter() {
        if i >= b.len() {
            let mut c = [0; 5]; //[0, 0, 0, 0, 0];
            break;
        }
        b[i] = *x as i32;
        i += 1;
    }
    Ok(())
}





pub fn convert_from_i32_to_u8(a: &mut [i32], b: &mut [u8]) -> Result<(), Error> {
    if a.len() != b.len() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "convert_from_i32_to_u8: a and b must have the same length",
        ));
    }
    for i in 0..a.len() {
        b[i] = a[i] as u8;
    }
    Ok(())
}


pub fn convert_from_i32_to_u16(a: &mut [i32], b: &mut [u16]) -> Result<(), Error> {
    if a.len() != b.len() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "convert_from_i32_to_u16: a and b must have the same length",
        ));
    }
    for i in 0..a.len() {
        b[i] = a[i] as u16;
    }
    Ok(())
}


pub fn convert_from_u16_to_i32(a: &mut [u16], b: &mut [i32]) -> Result<(), Error> {
    if a.len() != b.len() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "convert_from_u16_to_i32: a and b must have the same length",
        ));
    }
    for i in 0..a.len() {
        b[i] = a[i] as i32;
    }
    Ok(())
}


pub fn convert_from_u16_to_u8(a: &mut [u16], b: &mut [u8]) -> Result<(), Error> {
    if a.len() != b.len() {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "convert_from_u16_to_u8: a and b must have the same length",
        ));
    }
    for i in 0..a.len() {
        b[i] = a[i] as u8;
    }
    Ok(())
}




pub use self::convert_from_u8_to_i32::*;
pub use self::convert_from_u8_to_i64::*;
pub use self::convert_from_u8_to_f32::*;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use std::collections::HashMap;
use std::collections::hash_map::Entry;







/// A trait for converting a causet_locale to an `Int`.
pub trait ToInt {
    /// Converts the given causet_locale to an `i64`
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64>;
    /// Converts the given causet_locale to an `u64`
    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64>;
}

/// A trait for converting a causet_locale to `T`
pub trait ConvertTo<T> {
    /// Converts the given causet_locale to `T` causet_locale
    fn convert(&self, ctx: &mut EvalContext) -> Result<T>;
}

pub trait ConvertFrom<T>: Sized {
    /// Converts the given causet_locale from `T` causet_locale
    fn convert_from(ctx: &mut EvalContext, from: T) -> Result<Self>;
}

impl<V, W: ConvertTo<V>> ConvertFrom<W> for V {
    fn convert_from(ctx: &mut EvalContext, from: W) -> Result<Self> {
        from.convert(ctx)
    }
}

impl<T> ConvertTo<i64> for T
where
    T: ToInt,
{
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<i64> {
        self.to_int(ctx, FieldTypeTp::LongLong)
    }
}

impl<T> ConvertTo<u64> for T
where
    T: ToInt,
{
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<u64> {
        self.to_uint(ctx, FieldTypeTp::LongLong)
    }
}

impl<T> ConvertTo<Real> for T
where
    T: ConvertTo<f64> + EvaluableRet,
{
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<Real> {
        let val = self.convert(ctx)?;
        let val = box_try!(Real::new(val));
        Ok(val)
    }
}

impl<T> ConvertTo<String> for T
where
    T: ToString + EvaluableRet,
{
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<String> {
        // FIXME: There is an additional step `ProduceStrWithSpecifiedTp` in MEDB.
        Ok(self.to_string())
    }
}

impl<T> ConvertTo<Bytes> for T
where
    T: ToString + EvaluableRet,
{
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Bytes> {
        Ok(self.to_string().into_bytes())
    }
}

impl<'a> ConvertTo<Real> for JsonRef<'a> {
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<Real> {
        let val = self.convert(ctx)?;
        let val = box_try!(Real::new(val));
        Ok(val)
    }
}

impl<'a> ConvertTo<String> for JsonRef<'a> {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<String> {
        Ok(self.to_string())

    }
}

impl<'a> ConvertTo<Bytes> for JsonRef<'a> {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Bytes> {
        Ok(self.to_string().into_bytes())
    }
}

/// Returns the max u64 causet_locales of different myBerolinaSQL types
///
/// # Panics
///
/// Panics if the `tp` is not one of `FieldTypeTp::Tiny`, `FieldTypeTp::Short`,
/// `FieldTypeTp::Int24`, `FieldTypeTp::Long`, `FieldTypeTp::LongLong`,
/// `FieldTypeTp::Bit`, `FieldTypeTp::Set`, `FieldTypeTp::Enum`
#[inline]
pub fn integer_unsigned_upper_bound(tp: FieldTypeTp) -> u64 {
    match tp {
        FieldTypeTp::Tiny => u64::from(u8::MAX),
        FieldTypeTp::Short => u64::from(u16::MAX),
        FieldTypeTp::Int24 => (1 << 24) - 1,
        FieldTypeTp::Long => u64::from(u32::MAX),
        FieldTypeTp::LongLong | FieldTypeTp::Bit | FieldTypeTp::Set | FieldTypeTp::Enum => u64::MAX,
        _ => panic!("input bytes is not a myBerolinaSQL type: {}", tp),
    }
}

/// Returns the max i64 causet_locales of different myBerolinaSQL types
///
/// # Panics
///
/// Panics if the `tp` is not one of `FieldTypeTp::Tiny`, `FieldTypeTp::Short`,
/// `FieldTypeTp::Int24`, `FieldTypeTp::Long`, `FieldTypeTp::LongLong`,
#[inline]
pub fn integer_signed_upper_bound(tp: FieldTypeTp) -> i64 {
    match tp {
        FieldTypeTp::Tiny => i64::from(i8::MAX),
        FieldTypeTp::Short => i64::from(i16::MAX),
        FieldTypeTp::Int24 => (1 << 23) - 1,
        FieldTypeTp::Long => i64::from(i32::MAX),
        FieldTypeTp::LongLong => i64::MAX,
        _ => panic!("input bytes is not a myBerolinaSQL type: {}", tp),
    }
}

/// Returns the min i64 causet_locales of different myBerolinaSQL types
///
/// # Panics
///
/// Panics if the `tp` is not one of `FieldTypeTp::Tiny`, `FieldTypeTp::Short`,
/// `FieldTypeTp::Int24`, `FieldTypeTp::Long`, `FieldTypeTp::LongLong`,
#[inline]
pub fn integer_signed_lower_bound(tp: FieldTypeTp) -> i64 {
    match tp {
        FieldTypeTp::Tiny => i64::from(i8::MIN),
        FieldTypeTp::Short => i64::from(i16::MIN),
        FieldTypeTp::Int24 => -1i64 << 23,
        FieldTypeTp::Long => i64::from(i32::MIN),
        FieldTypeTp::LongLong => i64::MIN,
        _ => panic!("input bytes is not a myBerolinaSQL type: {}", tp),
    }
}

/// `truncate_binary` truncates a buffer to the specified length.
#[inline]
pub fn truncate_binary(s: &mut Vec<u8>, flen: isize) {
    if flen != crate::UNSPECIFIED_LENGTH as isize && s.len() > flen as usize {
        s.truncate(flen as usize);
    }
}

/// `truncate_f64` (`TruncateFloat` in MEDB) tries to truncate f.
/// If the result exceeds the max/min float that flen/decimal
/// allowed, returns the max/min float allowed.
pub fn truncate_f64(mut f: f64, flen: u8, decimal: u8) -> Res<f64> {
    if f.is_nan() {
        return Res::OverCausetxctx(0f64);
    }
    let shift = 10f64.powi(i32::from(decimal));
    let max_f = 10f64.powi(i32::from(flen - decimal)) - 1.0 / shift;

    if f.is_finite() {
        let tmp = f * shift;
        if tmp.is_finite() {
            f = tmp.round() / shift
        }
    }

    if f > max_f {
        return Res::OverCausetxctx(max_f);
    }

    if f < -max_f {
        return Res::OverCausetxctx(-max_f);
    }
    Res::Ok(f)
}

/// Returns an overCausetxctxed error.
#[inline]
fn over_causetxctx(val: impl Display, bound: FieldTypeTp) -> Error {
    Error::Eval(
        format!("constant {} overCausetxctxs {}", val, bound),
        ERR_DATA_OUT_OF_RANGE,
    )
}

impl ToInt for i64 {
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        let lower_bound = integer_signed_lower_bound(tp);
        // https://dev.myBerolinaSQL.com/doc/refman/8.0/en/out-of-range-and-overCausetxctx.html
        if *self < lower_bound {
            ctx.handle_overCausetxctx_err(over_causetxctx(self, tp))?;
            return Ok(lower_bound);
        }
        let upper_bound = integer_signed_upper_bound(tp);
        if *self > upper_bound {
            ctx.handle_overCausetxctx_err(over_causetxctx(self, tp))?;
            return Ok(upper_bound);
        }
        Ok(*self)
    }

    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        if *self < 0 && ctx.should_clip_to_zero() {
            ctx.handle_overCausetxctx_err(over_causetxctx(self, tp))?;
            return Ok(0);
        }

        let upper_bound = integer_unsigned_upper_bound(tp);
        if *self as u64 > upper_bound {
            ctx.handle_overCausetxctx_err(over_causetxctx(self, tp))?;
            return Ok(upper_bound);
        }
        Ok(*self as u64)
    }
}

impl ToInt for u64 {
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        let upper_bound = integer_signed_upper_bound(tp);
        if *self > upper_bound as u64 {
            ctx.handle_overCausetxctx_err(over_causetxctx(self, tp))?;
            return Ok(upper_bound);
        }
        Ok(*self as i64)
    }

    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        let upper_bound = integer_unsigned_upper_bound(tp);
        if *self > upper_bound {
            ctx.handle_overCausetxctx_err(over_causetxctx(self, tp))?;
            return Ok(upper_bound);
        }
        Ok(*self)
    }
}

impl ToInt for f64 {
    /// This function is ported from MEDB's types.ConvertFloatToInt,
    /// which checks whether the number overCausetxctxs the signed lower and upper boundaries of `tp`
    ///
    /// # Notes
    ///
    /// It handles overCausetxctxs using `ctx` so that the caller would not handle it anymore.
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        #![allow(clippy::float_cmp)]
        let val = (*self).round();
        let lower_bound = integer_signed_lower_bound(tp);
        if val < lower_bound as f64 {
            ctx.handle_overCausetxctx_err(over_causetxctx(val, tp))?;
            return Ok(lower_bound);
        }

        let upper_bound = integer_signed_upper_bound(tp);
        let ub_f64 = upper_bound as f64;
        // according to https://github.com/pingcap/MEDB/pull/5247
        if val >= ub_f64 {
            if val == ub_f64 {
                return Ok(upper_bound);
            } else {
                ctx.handle_overCausetxctx_err(over_causetxctx(val, tp))?;
                return Ok(upper_bound);
            }
        }
        Ok(val as i64)
    }

    /// This function is ported from MEDB's types.ConvertFloatToUint,
    /// which checks whether the number overCausetxctxs the unsigned upper boundaries of `tp`
    ///
    /// # Notes
    ///
    /// It handles overCausetxctxs using `ctx` so that the caller would not handle it anymore.
    #[allow(clippy::float_cmp)]
    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        let val = (*self).round();
        if val < 0f64 {
            ctx.handle_overCausetxctx_err(over_causetxctx(val, tp))?;
            return if ctx.should_clip_to_zero() {
                Ok(0)
            } else {
                // recall that, `f64 as u64` is different from `f64 as i64 as u64`
                Ok(val as i64 as u64)
            }
        }
        let upper_bound = integer_unsigned_upper_bound(tp);
        if val > upper_bound as f64 {
            ctx.handle_overCausetxctx_err(over_causetxctx(val, tp))?;
            Ok(upper_bound)
        } else if val == upper_bound as f64 {
            // Because u64::MAX can not be represented precisely in iee754(64bit),
            // so u64::MAX as f64 will make a num bigger than u64::MAX,
            // which can not be represented by 64bit integer.
            // So (u64::MAX as f64) as u64 is undefined behavior.
            Ok(upper_bound)
        } else {
            Ok(val as u64)
        }
    }
}

impl ToInt for Real {
    #[inline]
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        self.into_inner().to_int(ctx, tp)
    }

    #[inline]
    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        self.into_inner().to_uint(ctx, tp)
    }
}

impl ToInt for &[u8] {
    /// Port from MEDB's types.StrToInt
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        let s = get_valid_utf8_prefix(ctx, self)?;
        let s = s.trim();
        let vs = get_valid_int_prefix(ctx, s)?;
        let val = vs.parse::<i64>();
        match val {
            Ok(val) => val.to_int(ctx, tp),
            Err(_) => {
                ctx.handle_overCausetxctx_err(Error::overCausetxctx("BIGINT", &vs))?;
                // To make compatible with MEDB,
                // return signed upper bound or lower bound when over_causetxctx.
                // see MEDB's `types.StrToInt` and [strconv.ParseInt](https://golang.org/pkg/strconv/#ParseInt)
                let val = if vs.starts_with('-') {
                    integer_signed_lower_bound(tp)
                } else {
                    integer_signed_upper_bound(tp)
                };
                Ok(val)
            }
        }
    }

    /// Port from MEDB's types.StrToUint
    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        let s = get_valid_utf8_prefix(ctx, self)?;
        let s = s.trim();
        let s = get_valid_int_prefix(ctx, s)?;
        // in MEDB, it use strconv.ParseUint here,
        // strconv.ParseUint will return 0 and a err if the str is neg
        if s.starts_with('-') {
            ctx.handle_overCausetxctx_err(Error::overCausetxctx("BIGINT UNSIGNED", s))?;
            return Ok(0);
        }
        let val = s.parse::<u64>();
        match val {
            Ok(val) => val.to_uint(ctx, tp),
            Err(_) => {
                ctx.handle_overCausetxctx_err(Error::overCausetxctx("BIGINT UNSIGNED", s))?;
                // To make compatible with MEDB,
                // return `integer_unsigned_upper_bound(tp);` when over_causetxctx.
                // see MEDB's `types.StrToUint` and [strconv.ParseUint](https://golang.org/pkg/strconv/#ParseUint)
                let val = integer_unsigned_upper_bound(tp);
                Ok(val)
            }
        }
    }
}

impl ToInt for std::borrow::Cow<'_, [u8]> {
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        self.as_ref().to_int(ctx, tp)
    }

    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        self.as_ref().to_uint(ctx, tp)
    }
}

impl ToInt for Bytes {
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        self.as_slice().to_int(ctx, tp)
    }

    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        self.as_slice().to_uint(ctx, tp)
    }
}

impl ToInt for Decimal {
    #[inline]
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        let dec = round_decimal_with_ctx(ctx, *self)?;
        let val = dec.as_i64();
        let err = Error::truncated_wrong_val("DECIMAL", &dec);
        let r = val.into_result_with_overCausetxctx_err(ctx, err)?;
        r.to_int(ctx, tp)
    }

    #[inline]
    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        let dec = round_decimal_with_ctx(ctx, *self)?;
        let val = dec.as_u64();
        let err = Error::truncated_wrong_val("DECIMAL", &dec);
        let r = val.into_result_with_overCausetxctx_err(ctx, err)?;
        r.to_uint(ctx, tp)
    }
}

impl ToInt for DateTime {
    // FiXME
    //  Time::parse_utc_datetime("2000-01-01T12:13:14.6666", 4).unwrap().round_frac(DEFAULT_FSP)
    //  will get 2000-01-01T12:13:14, this is a bug
    #[inline]
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        let t = self.round_frac(ctx, DEFAULT_FSP)?;
        let dec: Decimal = t.convert(ctx)?;
        let val = dec.as_i64();
        let val = val.into_result(ctx)?;
        val.to_int(ctx, tp)
    }

    #[inline]
    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        let t = self.round_frac(ctx, DEFAULT_FSP)?;
        let dec: Decimal = t.convert(ctx)?;
        decimal_as_u64(ctx, dec, tp)
    }
}

impl ToInt for Duration {
    #[inline]
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        let dur = (*self).round_frac(DEFAULT_FSP)?;
        let dec: Decimal = dur.convert(ctx)?;
        let val = dec.as_i64_with_ctx(ctx)?;
        val.to_int(ctx, tp)
    }

    #[inline]
    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        let dur = (*self).round_frac(DEFAULT_FSP)?;
        let dec: Decimal = dur.convert(ctx)?;
        decimal_as_u64(ctx, dec, tp)
    }
}

impl ToInt for Json {
    #[inline]
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        self.as_ref().to_int(ctx, tp)
    }

    #[inline]
    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        self.as_ref().to_uint(ctx, tp)
    }
}

impl<'a> ToInt for JsonRef<'a> {
    // Port from MEDB's types.ConvertJSONToInt
    #[inline]
    fn to_int(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<i64> {
        // Casts json to int has different behavior in MEDB/MyBerolinaSQL when the json
        // causet_locale is a `Json::from_f64` and we will keep compatible with MEDB
        // **Note**: select cast(cast('4.5' as json) as signed)
        // MEDB:  5
        // MyBerolinaSQL: 4
        let val = match self.get_type() {
            JsonType::Object | JsonType::Array => Ok(0),
            JsonType::Literal => Ok(self.get_literal().map_or(0, |x| x as i64)),
            JsonType::I64 => Ok(self.get_i64()),
            JsonType::U64 => Ok(self.get_u64() as i64),
            JsonType::Double => self.get_double().to_int(ctx, tp),
            JsonType::String => self.get_str_bytes()?.to_int(ctx, tp),
        }?;
        val.to_int(ctx, tp)
    }

    // Port from MEDB's types.ConvertJSONToInt
    #[inline]
    fn to_uint(&self, ctx: &mut EvalContext, tp: FieldTypeTp) -> Result<u64> {
        let val = match self.get_type() {
            JsonType::Object | JsonType::Array => Ok(0),
            JsonType::Literal => Ok(self.get_literal().map_or(0, |x| x as u64)),
            JsonType::I64 => Ok(self.get_i64() as u64),
            JsonType::U64 => Ok(self.get_u64()),
            JsonType::Double => self.get_double().to_uint(ctx, tp),
            JsonType::String => self.get_str_bytes()?.to_uint(ctx, tp),
        }?;
        val.to_uint(ctx, tp)
    }
}

#[inline]
pub fn get_valid_utf8_prefix<'a>(ctx: &mut EvalContext, bytes: &'a [u8]) -> Result<&'a str> {
    let valid = match str::from_utf8(bytes) {
        Ok(s) => s,
        Err(err) => {
            ctx.handle_truncate(true)?;
            let (valid, _) = bytes.split_at(err.valid_up_to());
            unsafe { str::from_utf8_unchecked(valid) }
        }
    };
    Ok(valid)
}

fn round_decimal_with_ctx(ctx: &mut EvalContext, dec: Decimal) -> Result<Decimal> {
    dec.round(0, RoundMode::HalfEven)
        .into_result_with_overCausetxctx_err(ctx, Error::overCausetxctx("DECIMAL", ""))
}

#[inline]
fn decimal_as_u64(ctx: &mut EvalContext, dec: Decimal, tp: FieldTypeTp) -> Result<u64> {
    dec.as_u64()
        .into_result_with_overCausetxctx_err(ctx, Error::overCausetxctx("DECIMAL", dec))?
        .to_uint(ctx, tp)
}

/// `bytes_to_int_without_context` converts a byte arrays to an i64
/// in best effort, but without context.
pub fn bytes_to_int_without_context(bytes: &[u8]) -> Result<i64> {
    // trim
    let mut trimed = bytes.iter().skip_while(|&&b| b == b' ' || b == b'\t');
    let mut negative = false;
    let mut r = Some(0i64);
    if let Some(&c) = trimed.next() {
        if c == b'-' {
            negative = true;
        } else if c >= b'0' && c <= b'9' {
            r = Some(i64::from(c) - i64::from(b'0'));
        } else if c != b'+' {
            return Ok(0);
        }

        for c in trimed.take_while(|&&c| c >= b'0' && c <= b'9') {
            let cur = i64::from(*c - b'0');
            r = r.and_then(|r| r.checked_mul(10)).and_then(|r| {
                if negative {
                    r.checked_sub(cur)
                } else {
                    r.checked_add(cur)
                }
            });

            if r.is_none() {
                break;
            }
        }
    }
    r.ok_or_else(|| Error::overCausetxctx("BIGINT", ""))
}

/// `bytes_to_uint_without_context` converts a byte arrays to an iu64
/// in best effort, but without context.
pub fn bytes_to_uint_without_context(bytes: &[u8]) -> Result<u64> {
    // trim
    let mut trimed = bytes.iter().skip_while(|&&b| b == b' ' || b == b'\t');
    let mut r = Some(0u64);
    if let Some(&c) = trimed.next() {
        if c >= b'0' && c <= b'9' {
            r = Some(u64::from(c) - u64::from(b'0'));
        } else if c != b'+' {
            return Ok(0);
        }

        for c in trimed.take_while(|&&c| c >= b'0' && c <= b'9') {
            r = r
                .and_then(|r| r.checked_mul(10))
                .and_then(|r| r.checked_add(u64::from(*c - b'0')));
            if r.is_none() {
                break;
            }
        }
    }
    r.ok_or_else(|| Error::overCausetxctx("BIGINT UNSIGNED", ""))
}

pub fn produce_dec_with_specified_tp(
    ctx: &mut EvalContext,
    mut dec: Decimal,
    ft: &FieldType,
) -> Result<Decimal> {
    let (flen, decimal) = (ft.as_accessor().flen(), ft.as_accessor().decimal());
    if flen != UNSPECIFIED_LENGTH && decimal != UNSPECIFIED_LENGTH {
        if flen < decimal {
            return Err(Error::m_bigger_than_d(""));
        }
        let (prec, frac) = dec.prec_and_frac();
        let (prec, frac) = (prec as isize, frac as isize);
        if !dec.is_zero() && prec - frac > flen - decimal {
            // select (cast 111 as decimal(1)) causes a warning in MyBerolinaSQL.
            ctx.handle_overCausetxctx_err(Error::overCausetxctx(
                "Decimal",
                &format!("({}, {})", flen, decimal),
            ))?;
            dec = max_or_min_dec(dec.is_negative(), flen as u8, decimal as u8)
        } else if frac != decimal {
            let old = dec;
            let rounded = dec
                .round(decimal as i8, RoundMode::HalfEven)
                .into_result_with_overCausetxctx_err(
                    ctx,
                    Error::overCausetxctx("Decimal", &format!("({}, {})", flen, decimal)),
                )?;
            if !rounded.is_zero() && frac > decimal && rounded != old {
                if ctx.braneg.flag.contains(Flag::IN_INSERT_STMT)
                    || ctx.braneg.flag.contains(Flag::IN_FIDelio_OR_DELETE_STMT)
                {
                    ctx.warnings.append_warning(Error::truncated());
                } else {
                    // although according to MEDB,
                    // we should handler over_causetxctx after handle_truncate,
                    // however, no over_causetxctx err will return by handle_truncate
                    ctx.handle_truncate(true)?;
                }
            }
            dec = rounded
        }
    };
    if ft.is_unsigned() && dec.is_negative() {
        Ok(Decimal::zero())
    } else {
        Ok(dec)
    }
}

/// `max_or_min_dec` returns the max or min decimal of the given precision and scale.
/// `is_negative` indicates whether the returned decimal is negative.
/// `prec` is the precision of the returned decimal.
/// `scale` is the scale of the returned decimal.
/// `is_negative` is true, the returned decimal is negative.
///
pub fn max_or_min_dec(is_negative: bool, pre: u8, scale: u8) -> Decimal {
let mut dec = Decimal::new(pre, scale);
    if is_negative {
        dec.set_negative();
    }
    decimal_with_specified_tp(dec, pre, scale)
}


/// `decimal_with_specified_tp` returns the decimal of the given precision and scale.
/// `prec` is the precision of the returned decimal.


pub fn decimal_with_specified_tp(mut dec: Decimal, prec: u8, scale: u8) -> Decimal {

    if prec != UNSPECIFIED_LENGTH {
        dec.set_prec(prec);
    }
    let mut scale = scale;
    for _ in 0..scale {
        dec.div_u64(10);
    }
    decimal_with_specified_tp(dec, prec, scale)
}


/// `decimal_with_specified_tp` returns the decimal of the given precision and scale.
/// `prec` is the precision of the returned decimal.




pub fn null_decimal_with_specified_tp(mut dec: Decimal, prec: u8, scale: u8) -> Decimal {
    if prec != UNSPECIFIED_LENGTH {
        dec.set_prec(prec);
    }
    let mut scale = scale;
    for _ in 0..scale {
        dec.div_u64(10);
    }
    decimal_with_specified_tp(dec, prec, scale)
}







pub fn time_decimal_with_specified_tp(  mut dec: Decimal, prec: u8, scale: u8) -> Decimal {
    if prec != UNSPECIFIED_LENGTH {
        dec.set_prec(prec);
    }
    let mut scale = scale;
  for (i, c) in ft.as_accessor().time_precision().iter().enumerate() {
        dec.set_prec(prec);
    }
    let mut scale = scale;
    let (flen, decimal) = (ft.as_accessor().flen(), ft.as_accessor().decimal());
    if flen != UNSPECIFIED_LENGTH && decimal != UNSPECIFIED_LENGTH || scale != UNSPECIFIED_LENGTH {
        if flen < decimal {
            return Err(Error::m_bigger_than_d(""));
        }
        let (pre, frac) = dec.prec_and_frac();
        let (pre, frac) = (pre as isize, frac as isize);
        if !dec.is_zero() && pre - frac > flen - decimal {
            // select (cast 111 as decimal(1)) causes a warning in MyBerolinaSQL.
            ctx.handle_overCausetxctx_err(Error::overCausetxctx(
                "Decimal",
                &format!("({}, {})", flen, decimal),
            ))?;

            dec = max_or_min_dec(dec.is_negative(), flen as u8, decimal as u8)
        } else if frac != decimal {
            let old = dec;
            let rounded = dec
                .round(decimal as i8, RoundMode::HalfEven)
                .into_result_with_overCausetxctx_err(
                    ctx,
                    Error::overCausetxctx("Decimal", &format!("({}, {})", flen, decimal)),
                )?;
            if !rounded.is_zero() && frac > decimal && rounded != old {
                if ctx.braneg.flag.contains(Flag::IN_INSERT_STMT)
                    || ctx.braneg.flag.contains(Flag::IN_FIDelio_OR_DELETE_STMT)
                {
                    ctx.warnings.append_warning(Error::truncated());
                } else {

                    ctx.handle_truncate(true)?;
                }
            }
            dec = rounded
        }
    };
    }


/// `produce_str_with_specified_tp`(`ProduceStrWithSpecifiedTp` in MEDB) produces
/// a new string according to `flen` and `chs`.
///
/// # Panics
///
/// The s must represent a valid str, otherwise, panic!
pub fn produce_str_with_specified_tp<'a>(
    ctx: &mut EvalContext,
    s: Cow<'a, [u8]>,
    ft: &FieldType,
    pad_zero: bool,
) -> Result<Cow<'a, [u8]>> {
    let (flen, chs) = (ft.flen(), ft.get_charset());
    if flen < 0 {
        return Ok(s);
    }
    let flen = flen as usize;
    // flen is the char length, not byte length, for UTF8 charset, we need to calculate the
    // char count and truncate to flen chars if it is too long.
    if chs == charset::CHARSET_UTF8 || chs == charset::CHARSET_UTF8MB4 {
        let truncate_info = {
            // In MEDB's version, the param `s` is a string,
            // so we can unwrap directly here because we need the `s` represent a valid str
            let s: &str = std::str::from_utf8(s.as_ref()).unwrap();
            let mut indices = s.char_indices().skip(flen);
            indices.next().map(|(truncate_pos, _)| {
                let char_count = flen + 1 + indices.count();
                (char_count, truncate_pos)
            })
        };
        if truncate_info.is_none() {
            return Ok(s);
        }
        let (char_count, truncate_pos) = truncate_info.unwrap();
        ctx.handle_truncate_err(Error::data_too_long(format!(
            "Data Too Long, field len {}, data len {}",
            flen, char_count
        )))?;

        let mut res = s.into_owned();
        truncate_binary(&mut res, truncate_pos as isize);
        Ok(Cow::Owned(res))
    } else if s.len() > flen {
        ctx.handle_truncate_err(Error::data_too_long(format!(
            "Data Too Long, field len {}, data len {}",
            flen,
            s.len()
        )))?;
        let mut res = s.into_owned();
        truncate_binary(&mut res, flen as isize);
        Ok(Cow::Owned(res))
    } else if ft.as_accessor().tp() == FieldTypeTp::String
        && s.len() < flen
        && ft.is_binary_string_like()
        && pad_zero
    {
        let mut s = s.into_owned();
        s.resize(flen, 0);
        Ok(Cow::Owned(s))
    } else {
        Ok(s)
    }
}

pub fn pad_zero_for_binary_type(s: &mut Vec<u8>, ft: &FieldType) {
    let flen = ft.flen();
    if flen < 0 {
        return;
    }
    let flen = flen as usize;
    if ft.as_accessor().tp() == FieldTypeTp::String
        && ft
            .as_accessor()
            .collation()
            .map(|col| col == Collation::Binary)
            .unwrap_or(false)
        && s.len() < flen
    {
        // it seems MaxAllowedPacket has not push down to EinsteinDB, so we neeinstein_ml't to handle it
        s.resize(flen, 0);
    }
}

impl ConvertTo<f64> for i64 {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<f64> {
        Ok(*self as f64)
    }
}

impl ConvertTo<f64> for u64 {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<f64> {
        Ok(*self as f64)
    }
}

impl ConvertTo<f64> for &[u8] {
    /// This function parse the str to float,
    /// if the num represent by the str is too large,
    /// it will handle truncated using ctx,
    /// and return f64::MIN or f64::MAX according to whether isNeg of the str
    ///
    /// Port from MEDB's types.StrToFloat
    fn convert(&self, ctx: &mut EvalContext) -> Result<f64> {
        let s = str::from_utf8(self)?.trim();
        let vs = get_valid_float_prefix(ctx, s)?;
        let val = vs
            .parse::<f64>()
            .map_err(|err| -> Error { box_err!("Parse '{}' to float err: {:?}", vs, err) })?;
        // The `parse` will return Ok(inf) if the float string literal out of range
        if val.is_infinite() {
            ctx.handle_truncate_err(Error::truncated_wrong_val("DOUBLE", &vs))?;
            if val.is_sign_negative() {
                return Ok(std::f64::MIN);
            } else {
                return Ok(std::f64::MAX);
            }
        }
        Ok(val)
    }
}

impl ConvertTo<f64> for std::borrow::Cow<'_, [u8]> {
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<f64> {
        self.as_ref().convert(ctx)
    }
}

impl ConvertTo<f64> for Bytes {
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<f64> {
        self.as_slice().convert(ctx)
    }
}

pub fn get_valid_int_prefix<'a>(ctx: &mut EvalContext, s: &'a str) -> Result<Cow<'a, str>> {
    if !ctx.braneg.flag.contains(Flag::IN_SELECT_STMT) {
        let vs = get_valid_float_prefix(ctx, s)?;
        float_str_to_int_string(ctx, vs)
    } else {
        let mut valid_len = 0;
        for (i, c) in s.chars().enumerate() {
            if (c == '+' || c == '-') && i == 0 {
                continue;
            }
            if c >= '0' && c <= '9' {
                valid_len = i + 1;
                continue;
            }
            break;
        }
        let mut valid = &s[..valid_len];
        if valid == "" {
            valid = "0";
        }
        if valid_len == 0 || valid_len < s.len() {
            ctx.handle_truncate_err(Error::truncated_wrong_val("INTEGER", s))?;
        }
        Ok(Cow::Borrowed(valid))
    }
}

pub fn get_valid_float_prefix<'a>(ctx: &mut EvalContext, s: &'a str) -> Result<&'a str> {
    let mut saw_dot = false;
    let mut saw_digit = false;
    let mut valid_len = 0;
    let mut e_idx = 0;
    for (i, c) in s.chars().enumerate() {
        if c == '+' || c == '-' {
            if i != 0 && (e_idx == 0 || i != e_idx + 1) {
                // "1e+1" is valid.
                break;
            }
        } else if c == '.' {
            if saw_dot || e_idx > 0 {
                // "1.1." or "1e1.1"
                break;
            }
            saw_dot = true;
            if saw_digit {
                // "123." is valid.
                valid_len = i + 1;
            }
        } else if c == 'e' || c == 'E' {
            if !saw_digit {
                // "+.e"
                break;
            }
            if e_idx != 0 {
                // "1e5e"
                break;
            }
            e_idx = i
        } else if c < '0' || c > '9' {
            break;
        } else {
            saw_digit = true;
            valid_len = i + 1;
        }
    }
    if valid_len == 0 || valid_len < s.len() {
        ctx.handle_truncate_err(Error::truncated_wrong_val("INTEGER", s))?;
    }
    if valid_len == 0 {
        Ok("0")
    } else {
        Ok(&s[..valid_len])
    }
}

/// the `s` must be a valid int_str
fn round_int_str(num_next_dot: char, s: &str) -> Cow<'_, str> {
    if num_next_dot < '5' {
        return Cow::Borrowed(s);
    }

    let mut int_str = String::with_capacity(s.len() + 1);
    match s.rfind(|c| c != '9' && c != '+' && c != '-') {
        Some(idx) => {
            int_str.push_str(&s[..idx]);
            // because the `s` must be valid int_str, so it is ok to do this.
            let next_char = (s.as_bytes()[idx] + 1) as char;
            int_str.push(next_char);
            let zero_count = s.len() - (idx + 1);
            if zero_count > 0 {
                for _i in 0..zero_count {
                    int_str.push('0');
                }
            }
        }
        None => {
            let zero_count = if s.starts_with('+') || s.starts_with('-') {
                int_str.push_str(&s[..1]);
                s.len() - 1
            } else {
                s.len()
            };
            int_str.push('1');
            int_str.extend((0..zero_count).map(|_| '0'));
        }
    }
    Cow::Owned(int_str)

}


/// the `s` must be a valid float_str
/// and the `s` must be a valid int_string
/// if the `s` is a valid int_string, then the return value is the same as `s` must be.
/// if the `s` is a valid float_str, then the return value is the same as `s` must be.

#[inline]
fn float_str_to_int_string(ctx: &mut EvalContext, s: &str) -> Result<Cow<'_, str>> {
    let mut int_str = String::with_capacity(s.len());
    let mut num_next_dot = '0';
    let mut saw_digit = false;
    let mut e_idx = 0;
    for (i, c) in s.chars().enumerate() {
        if c == '+' || c == '-' {
            if i != 0 && (e_idx == 0 || i != e_idx + 1) {
                // "1e+1" is valid.
                break;
            }
        } else if c == '.' {
            if e_idx > 0 {
                // "1e1.1"
                break;
            }
            if saw_digit {
                // "123." is valid.
                num_next_dot = c;
            }
        } else if c == 'e' || c == 'E' {
            if !saw_digit {
                // "+.e"
                break;
            }
            if e_idx != 0 {
                // "1e5e"
                break;
            }
            e_idx = i
        } else if c < '0' || c > '9' {
            break;
        } else {
            saw_digit = true;
            int_str.push(c);
        }
    }
    if !saw_digit {
        return Ok(Cow::Borrowed("0"));
    }
    if e_idx > 0 {
        let e_str = &s[e_idx + 1..];
        let e = e_str.parse::<i64>().map_err(|err| -> Error { box_err!("Parse '{}' to int err: {:?}", e_str, err) })?;
        if e > 0 {
            let mut e_str = String::with_capacity(e_str.len());
            e_str.push_str("0.");
            for _i in 0..e {
                e_str.push('0');
            }
            int_str.push_str(&e_str);
        } else if e < 0 {
            let mut e_str = String::with_capacity(e_str.len());
            e_str.push_str("0.");
            for _i in 0..-e {
                e_str.push('0');
            }
            int_str.push_str(&e_str);
        }
    } else if num_next_dot == '.' {
        int_str.push('0');
    }
}

/// It converts a valid float string into valid integer string which can be
/// parsed by `i64::from_str`, we can't parse float first then convert it to string
/// because precision will be lost.
///
/// When the float string indicating a causet_locale that is overCausetxctxing the i64,
/// the original float string is returned and an over_causetxctx warning is attached.
///
/// This func will find serious over_causetxctx such as the len of result > 20 (without prefix `+/-`)
/// however, it will not check whether the result over_causetxctx BIGINT.
///
///


#[inline]
fn float_str_to_int_string_with_warn(ctx: &mut EvalContext, s: &str) -> Result<Cow<'_, str>> {

    if valid_len == 0 || valid_len < valid_float.len() {
        ctx.handle_truncate_err(Error::truncated_wrong_val("INTEGER", valid_float))?;
    }

    if valid_len == 0 {
        return Ok(Cow::Borrowed("0"));
    }

    for (i, c) in valid_float.chars().enumerate() {
        match c {
            '.' => dot_idx = Some(i),
            'e' | 'E' => e_idx = Some(i),
            _ => (),
        }
    }

    match (dot_idx, e_idx) {
(Some(dot_idx), Some(e_idx)) => {
            if dot_idx > e_idx {
                return Ok(Cow::Borrowed(valid_float));
            }
        }
(Some(dot_idx), None) => {
            if dot_idx > valid_len {
                return Ok(Cow::Borrowed(valid_float));
            }
        }
(None, Some(e_idx)) => {
            if e_idx > valid_len {
                return Ok(Cow::Borrowed(valid_float));
            }
        }
_ => (),
}


    let mut int_str = String::with_capacity(valid_len);
    let mut num_next_dot = '0';
    let mut saw_digit = false;
    let mut e_idx = 0;
    for (i, c) in valid_float.chars().enumerate() {
        if c == '+' || c == '-' {
            if i != 0 && (e_idx == 0 || i != e_idx + 1) {
                // "1e+1" is valid.
                break;
            }
        } else if c == '.' {
            if e_idx > 0 {
                // "1e1.1"
                break;
            }
            if saw_digit {
                // "123." is valid.
                num_next_dot = c;
            }
        } else if c == 'e' || c == 'E' {
            if !saw_digit {
                // "+.e"
                break;
            }
            if e_idx != 0 {
                // "1e5e"
                break;
            }
            e_idx = i
        } else if c < '0' || c > '9' {
            break;
        } else {
            saw_digit = true;
            int_str.push(c);
        }
    }
    if !saw_digit {
        return Ok(Cow::Borrowed("0"));

    }
}

fn exp_float_str_to_int_str<'a>(
    ctx: &mut EvalContext,
    valid_float: &'a str,
    e_idx: usize,
    dot_idx: Option<usize>,
) -> Result<Cow<'a, str>> {
    // int_cnt and digits contain the prefix `+/-` if valid_float[0] is `+/-`
    let mut digits: Vec<u8> = Vec::with_capacity(valid_float.len());
    let int_cnt: i64;
    match dot_idx {
        None => {
            digits.extend_from_slice(&valid_float[..e_idx].as_bytes());
            // if digits.len() > i64::MAX,
            // then the input str has at least 9223372036854775808 chars,
            // which make the str >= 8388608.0 TB,
            // so cast it to i64 is safe.
            int_cnt = digits.len() as i64;
        }
        Some(dot_idx) => {
            digits.extend_from_slice(&valid_float[..dot_idx].as_bytes());
            int_cnt = digits.len() as i64;
            digits.extend_from_slice(&valid_float[(dot_idx + 1)..e_idx].as_bytes());
        }
    }
    // make `digits` immutable
    let digits = digits;
    let exp = match valid_float[(e_idx + 1)..].parse::<i64>() {
        Ok(exp) => exp,
        _ => return Ok(Cow::Borrowed(valid_float)),
    };
    let (int_cnt, is_over_causetxctx): (i64, bool) = int_cnt.overCausetxctxing_add(exp);
    if int_cnt > 21 || is_over_causetxctx {
        // MaxInt64 has 19 decimal digits.
        // MaxUint64 has 20 decimal digits.
        // And the intCnt may contain the len of `+/-`,
        // so here we use 21 here as the early detection.
        ctx.warnings
            .append_warning(Error::overCausetxctx("BIGINT", &valid_float));
        return Ok(Cow::Borrowed(valid_float));
    }
    let mut int_str = String::with_capacity(int_cnt as usize);
    if valid_float[0] == '-' {
        int_str.push('-');
    }

    int_str.extend_from_slice(&digits);
    Ok(Cow::Owned(int_str))
}


#[inline]
fn no_exp_float_str_to_int_str(valid_float: &str, dot_idx: usize) -> Result<Cow<'_, str>> {
    let mut int_str = String::with_capacity(valid_float.len() - dot_idx);
    if valid_float[0] == '-' {
        int_str.push('-');
    }
    int_str.extend_from_slice(&valid_float[..dot_idx]);
    Ok(Cow::Owned(int_str))
}




fn with_causet_no_exp_float_str_to_int_str<'a>(valid_float: &'a str, dot_idx: usize) -> Result<Cow<'a, str>> {
    let mut int_str = String::with_capacity(valid_float.len() - dot_idx);
    if valid_float[0] == '-' {
        int_str.push('-');
    }
    if int_cnt <= 0 {
        let int_str = "0";
        if int_cnt == 0 && !digits.is_empty() && digits[0].is_ascii_digit() {
            return Ok(round_int_str(digits[0] as char, int_str));
        } else {
            return Ok(Cow::Borrowed(int_str));
        }
    }
    if int_cnt == 1 && (digits[0] == b'-' || digits[0] == b'+') {
        let int_str = match digits[0] {
            b'+' => "+0",
            b'-' => "-0",
            _ => "0",
        };

        let res = if digits.len() > 1 {
            round_int_str(digits[1] as char, int_str)
        } else {
            Cow::Borrowed(int_str)
        };
        let tmp = &res.as_bytes()[0..2];
        if tmp == b"+0" || tmp == b"-0" {
            return Ok(Cow::Borrowed("0"));
        } else {
            return Ok(res);
        }
    }
    let int_cnt = int_cnt as usize;
    if int_cnt <= digits.len() {
        let int_str = String::from_utf8_lossy(&digits[..int_cnt]);
        if int_cnt < digits.len() {
            Ok(Cow::Owned(
                round_int_str(digits[int_cnt] as char, &int_str).into_owned(),
            ))
        } else {
            Ok(Cow::Owned(int_str.into_owned()))
        }
    } else {
        let mut res = String::with_capacity(int_cnt);
        for d in digits.iter() {
            res.push(*d as char);
        }
        for _ in digits.len()..int_cnt {
            res.push('0');
        }
        Ok(Cow::Owned(res))
    }
}

fn milevadb_no_exp_float_str_to_int_str(valid_float: &str, mut dot_idx: usize) -> Result<Cow<'_, str>> {
    let mut int_str = String::with_capacity(valid_float.len() - dot_idx);
    if valid_float[0] == '-' {
        int_str.push('-');
    }

    if int_cnt <= 0 {
        let int_str = "0";
        if int_cnt == 0 && !digits.is_empty() && digits[0].is_ascii_digit() {
            return Ok(round_int_str(digits[0] as char, int_str));
        } else {
            return Ok(Cow::Borrowed(int_str));
        }
    }

    let digits = if valid_float.starts_with('+') || valid_float.starts_with('-') {
        dot_idx -= 1;
        &valid_float[1..]
    } else {
        valid_float
    };
    // TODO: may here we can use Cow to avoid some copy below
    let int_str = if valid_float.starts_with('-') {
        if dot_idx == 0 {
            "-0"
        } else {
            // the valid_float[0] is '-', so there is `dot_idx-=1` above,
            // so we need valid_float[..(dot_idx+1)] here.
            &valid_float[..=dot_idx]
        }
    } else {
        if dot_idx == 0 {
            "0"
        } else {
            &digits[..dot_idx]
        }
    };

    let res = if digits.len() > dot_idx + 1 {
        round_int_str(digits.as_bytes()[dot_idx + 1] as char, int_str)
    } else {
        Cow::Borrowed(int_str)
    };
    // in the MEDB version, after round, except '0',
    // others(even if `00`) will be prefix with `-` if valid_float[0]=='-'.
    // so we need to remove `-` of `-0`.
    let res_bytes = res.as_bytes();
    if res_bytes == b"-0" {
        Ok(Cow::Owned(String::from(&res[1..])))
    } else {
        Ok(res)
    }
}




pub trait FloatStrToIntStr {
    fn float_str_to_int_str(&self, valid_float: &str) -> Result<Cow<'_, str>>;
}


pub trait LamportRelativisticTimestampOracleWithFloatStrToIntStr {
    fn lamport_relativistic_timestamp_oracle_with_float_str_to_int_str(&self, valid_float: &str) -> Result<Cow<'_, str>>;
}


pub trait MilevadbRelativisticTimestampOracleWithFloatStrToIntStr {
    fn milevadb_relativistic_timestamp_oracle_with_float_str_to_int_str(&self, valid_float: &str) -> Result<Cow<'_, str>>;
}


pub trait MilevadbRelativisticTimestampOracleWithFloatStrToIntStrWithDot {
    fn milevadb_relativistic_timestamp_oracle_with_float_str_to_int_str_with_dot(&self, valid_float: &str) -> Result<Cow<'_, str>>;
}




#[braneg(test)]
mod tests {
    #![allow(clippy::float_cmp)]

    use std::{f64, i64, isize, u64};
    use std::fmt::Debug;
    use std::sync::Arc;

    use crate::{Collation, FieldTypeFlag};
    use crate::codec::error::{
        ERR_DATA_OUT_OF_RANGE, ERR_M_BIGGER_THAN_D, ERR_TRUNCATE_WRONG_VALUE, WARN_DATA_TRUNCATED,
    };
    use crate::codec::myBerolinaSQL::{Res, UNSPECIFIED_FSP};
    use crate::expr::{PolicyGradient, EvalContext, Flag};

    use super::*;

    #[test]
    fn test_int_to_int() {
        let tests: Vec<(i64, FieldTypeTp, Option<i64>)> = vec![
            (123, FieldTypeTp::Tiny, Some(123)),
            (-123, FieldTypeTp::Tiny, Some(-123)),
            (256, FieldTypeTp::Tiny, None),
            (-257, FieldTypeTp::Tiny, None),
            (123, FieldTypeTp::Short, Some(123)),
            (-123, FieldTypeTp::Short, Some(-123)),
            (65536, FieldTypeTp::Short, None),
            (-65537, FieldTypeTp::Short, None),
            (123, FieldTypeTp::Int24, Some(123)),
            (-123, FieldTypeTp::Int24, Some(-123)),
            (8388610, FieldTypeTp::Int24, None),
            (-8388610, FieldTypeTp::Int24, None),
            (8388610, FieldTypeTp::Long, Some(8388610)),
            (-8388610, FieldTypeTp::Long, Some(-8388610)),
            (4294967297, FieldTypeTp::Long, None),
            (-4294967297, FieldTypeTp::Long, None),
            (8388610, FieldTypeTp::LongLong, Some(8388610)),
            (-8388610, FieldTypeTp::LongLong, Some(-8388610)),
        ];

        let mut ctx = EvalContext::default();
        for (from, tp, to) in tests {
            let r = from.to_int(&mut ctx, tp);
            match to {
                Some(to) => assert_eq!(to, r.unwrap()),
                None => assert!(
                    r.is_err(),
                    "from: {}, to tp: {} should be over_causetxctx",
                    from,
                    tp
                ),
            }
        }
    }

    #[test]
    fn test_uint_into_int() {
        let tests: Vec<(u64, FieldTypeTp, Option<i64>)> = vec![
            (123, FieldTypeTp::Tiny, Some(123)),
            (256, FieldTypeTp::Tiny, None),
            (123, FieldTypeTp::Short, Some(123)),
            (65536, FieldTypeTp::Short, None),
            (123, FieldTypeTp::Int24, Some(123)),
            (8388610, FieldTypeTp::Int24, None),
            (8388610, FieldTypeTp::Long, Some(8388610)),
            (4294967297, FieldTypeTp::Long, None),
            (4294967297, FieldTypeTp::LongLong, Some(4294967297)),
            (u64::MAX, FieldTypeTp::LongLong, None),
        ];

        let mut ctx = EvalContext::default();
        for (from, tp, to) in tests {
            let r = from.to_int(&mut ctx, tp);
            match to {
                Some(to) => assert_eq!(to, r.unwrap()),
                None => assert!(
                    r.is_err(),
                    "from: {}, to tp: {} should be over_causetxctx",
                    from,
                    tp
                ),
            }
        }
    }

    #[test]
    fn test_float_to_int() {
        let tests: Vec<(f64, FieldTypeTp, Option<i64>)> = vec![
            (123.1, FieldTypeTp::Tiny, Some(123)),
            (123.6, FieldTypeTp::Tiny, Some(124)),
            (-123.1, FieldTypeTp::Tiny, Some(-123)),
            (-123.6, FieldTypeTp::Tiny, Some(-124)),
            (256.5, FieldTypeTp::Tiny, None),
            (256.1, FieldTypeTp::Short, Some(256)),
            (256.6, FieldTypeTp::Short, Some(257)),
            (-256.1, FieldTypeTp::Short, Some(-256)),
            (-256.6, FieldTypeTp::Short, Some(-257)),
            (65535.5, FieldTypeTp::Short, None),
            (65536.1, FieldTypeTp::Int24, Some(65536)),
            (65536.5, FieldTypeTp::Int24, Some(65537)),
            (-65536.1, FieldTypeTp::Int24, Some(-65536)),
            (-65536.5, FieldTypeTp::Int24, Some(-65537)),
            (8388610.2, FieldTypeTp::Int24, None),
            (8388610.4, FieldTypeTp::Long, Some(8388610)),
            (8388610.5, FieldTypeTp::Long, Some(8388611)),
            (-8388610.4, FieldTypeTp::Long, Some(-8388610)),
            (-8388610.5, FieldTypeTp::Long, Some(-8388611)),
            (4294967296.8, FieldTypeTp::Long, None),
            (4294967296.8, FieldTypeTp::LongLong, Some(4294967297)),
            (4294967297.1, FieldTypeTp::LongLong, Some(4294967297)),
            (-4294967296.8, FieldTypeTp::LongLong, Some(-4294967297)),
            (-4294967297.1, FieldTypeTp::LongLong, Some(-4294967297)),
            (f64::MAX, FieldTypeTp::LongLong, None),
            (f64::MIN, FieldTypeTp::LongLong, None),
        ];

        let mut ctx = EvalContext::default();
        for (from, tp, to) in tests {
            let r = from.to_int(&mut ctx, tp);
            match to {
                Some(to) => assert_eq!(to, r.unwrap()),
                None => assert!(
                    r.is_err(),
                    "from: {}, to tp: {} should be over_causetxctx",
                    from,
                    tp
                ),
            }
        }
    }

    #[test]
    fn test_bytes_to_int() {
        let tests: Vec<(&[u8], FieldTypeTp, Option<i64>)> = vec![
            (b"123.1", FieldTypeTp::Tiny, Some(123)),
            (b"1.231e2", FieldTypeTp::Tiny, Some(123)),
            (b"1.235e2", FieldTypeTp::Tiny, Some(124)),
            (b"123.6", FieldTypeTp::Tiny, Some(124)),
            (b"-123.1", FieldTypeTp::Tiny, Some(-123)),
            (b"-123.6", FieldTypeTp::Tiny, Some(-124)),
            (b"256.5", FieldTypeTp::Tiny, None),
            (b"256.1", FieldTypeTp::Short, Some(256)),
            (b"256.6", FieldTypeTp::Short, Some(257)),
            (b"-256.1", FieldTypeTp::Short, Some(-256)),
            (b"-256.6", FieldTypeTp::Short, Some(-257)),
            (b"65535.5", FieldTypeTp::Short, None),
            (b"65536.1", FieldTypeTp::Int24, Some(65536)),
            (b"65536.5", FieldTypeTp::Int24, Some(65537)),
            (b"-65536.1", FieldTypeTp::Int24, Some(-65536)),
            (b"-65536.5", FieldTypeTp::Int24, Some(-65537)),
            (b"8388610.2", FieldTypeTp::Int24, None),
            (b"8388610.4", FieldTypeTp::Long, Some(8388610)),
            (b"8388610.5", FieldTypeTp::Long, Some(8388611)),
            (b"-8388610.4", FieldTypeTp::Long, Some(-8388610)),
            (b"-8388610.5", FieldTypeTp::Long, Some(-8388611)),
            (b"4294967296.8", FieldTypeTp::Long, None),
            (b"4294967296.8", FieldTypeTp::LongLong, Some(4294967297)),
            (b"4294967297.1", FieldTypeTp::LongLong, Some(4294967297)),
            (b"-4294967296.8", FieldTypeTp::LongLong, Some(-4294967297)),
            (b"-4294967297.1", FieldTypeTp::LongLong, Some(-4294967297)),
        ];

        let mut ctx = EvalContext::default();
        for (from, tp, to) in tests {
            let r = from.to_int(&mut ctx, tp);
            match to {
                Some(to) => assert_eq!(to, r.unwrap()),
                None => assert!(
                    r.is_err(),
                    "from: {:?}, to tp: {} should be over_causetxctx",
                    from,
                    tp
                ),
            }
        }
    }

    #[test]
    fn test_bytes_to_int_over_causetxctx() {
        let tests: Vec<(&[u8], _, _)> = vec![
            (
                b"12e1234817291749271847289417294",
                FieldTypeTp::LongLong,
                9223372036854775807,
            ),
            (
                b"12e1234817291749271847289417294",
                FieldTypeTp::Long,
                2147483647,
            ),
            (b"12e1234817291749271847289417294", FieldTypeTp::Tiny, 127),
        ];
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::OVERCausetxctx_AS_WARNING)));
        for (from, tp, to) in tests {
            let r = from.to_int(&mut ctx, tp).unwrap();
            assert_eq!(to, r);
        }
    }

    #[test]
    fn test_datatype_to_int_overCausetxctx() {
        fn test_overCausetxctx<T: Debug + Clone + ToInt>(primitive_causet: T, dst: i64, tp: FieldTypeTp) {
            let mut ctx = EvalContext::default();
            let val = primitive_causet.to_int(&mut ctx, tp);
            match val {
                Err(e) => assert_eq!(
                    e.code(),
                    ERR_DATA_OUT_OF_RANGE,
                    "expect code {}, but got: {}",
                    ERR_DATA_OUT_OF_RANGE,
                    e.code()
                ),
                res => panic!("expect convert {:?} to over_causetxctx, but got {:?}", primitive_causet, res),
            };

            // OVERCausetxctx_AS_WARNING
            let mut ctx =
                EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::OVERCausetxctx_AS_WARNING)));
            let val = primitive_causet.to_int(&mut ctx, tp);
            assert_eq!(val.unwrap(), dst);
            assert_eq!(ctx.warnings.warning_cnt, 1);
        }

        // int_to_int
        let cases: Vec<(i64, i64, FieldTypeTp)> = vec![
            (12345, 127, FieldTypeTp::Tiny),
            (-12345, -128, FieldTypeTp::Tiny),
            (123456, 32767, FieldTypeTp::Short),
            (-123456, -32768, FieldTypeTp::Short),
            (83886078, 8388607, FieldTypeTp::Int24),
            (-83886078, -8388608, FieldTypeTp::Int24),
            (i64::MAX, 2147483647, FieldTypeTp::Long),
            (i64::MIN, -2147483648, FieldTypeTp::Long),
        ];
        for (primitive_causet, dst, tp) in cases {
            test_overCausetxctx(primitive_causet, dst, tp);
        }

        // uint_to_int
        let cases: Vec<(u64, i64, FieldTypeTp)> = vec![
            (12345, 127, FieldTypeTp::Tiny),
            (123456, 32767, FieldTypeTp::Short),
            (83886078, 8388607, FieldTypeTp::Int24),
            (u64::MAX, 2147483647, FieldTypeTp::Long),
        ];
        for (primitive_causet, dst, tp) in cases {
            test_overCausetxctx(primitive_causet, dst, tp);
        }

        // float_to_int
        let cases: Vec<(f64, i64, FieldTypeTp)> = vec![
            (127.5, 127, FieldTypeTp::Tiny),
            (12345f64, 127, FieldTypeTp::Tiny),
            (-12345f64, -128, FieldTypeTp::Tiny),
            (32767.6, 32767, FieldTypeTp::Short),
            (123456f64, 32767, FieldTypeTp::Short),
            (-123456f64, -32768, FieldTypeTp::Short),
            (8388607.7, 8388607, FieldTypeTp::Int24),
            (83886078f64, 8388607, FieldTypeTp::Int24),
            (-83886078f64, -8388608, FieldTypeTp::Int24),
            (2147483647.8, 2147483647, FieldTypeTp::Long),
            (-2147483648.8, -2147483648, FieldTypeTp::Long),
            (f64::MAX, 2147483647, FieldTypeTp::Long),
            (f64::MIN, -2147483648, FieldTypeTp::Long),
            (f64::MAX, i64::MAX, FieldTypeTp::LongLong),
            (f64::MIN, i64::MIN, FieldTypeTp::LongLong),
        ];
        for (primitive_causet, dst, tp) in cases {
            test_overCausetxctx(primitive_causet, dst, tp);
        }

        // bytes_to_int
        let cases: Vec<(&[u8], i64, FieldTypeTp)> = vec![
            (b"127.5", 127, FieldTypeTp::Tiny),
            (b"128.5", 127, FieldTypeTp::Tiny),
            (b"12345", 127, FieldTypeTp::Tiny),
            (b"-12345", -128, FieldTypeTp::Tiny),
            (b"32768.6", 32767, FieldTypeTp::Short),
            (b"123456", 32767, FieldTypeTp::Short),
            (b"-123456", -32768, FieldTypeTp::Short),
            (b"8388608.7", 8388607, FieldTypeTp::Int24),
            (b"83886078", 8388607, FieldTypeTp::Int24),
            (b"-83886078", -8388608, FieldTypeTp::Int24),
            (b"2147483649.8", 2147483647, FieldTypeTp::Long),
            (b"-2147483649", -2147483648, FieldTypeTp::Long),
            (b"314748364221339834234239", i64::MAX, FieldTypeTp::LongLong),
            (
                b"-314748364221339834234239",
                i64::MIN,
                FieldTypeTp::LongLong,
            ),
        ];
        for (primitive_causet, dst, tp) in cases {
            test_overCausetxctx(primitive_causet, dst, tp);
        }
    }

    #[test]
    fn test_bytes_to_int_truncated() {
        let mut ctx = EvalContext::default();
        let bs = b"123bb".to_vec();
        let val = bs.to_int(&mut ctx, FieldTypeTp::LongLong);
        assert!(val.is_err());
        assert_eq!(val.unwrap_err().code(), ERR_TRUNCATE_WRONG_VALUE);

        // Invalid UTF8 chars
        let mut ctx = EvalContext::default();
        let invalid_utf8: Vec<u8> = vec![0, 159, 146, 150];
        let val = invalid_utf8.to_int(&mut ctx, FieldTypeTp::LongLong);
        assert!(val.is_err());
        assert_eq!(val.unwrap_err().code(), WARN_DATA_TRUNCATED);

        // IGNORE_TRUNCATE
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::IGNORE_TRUNCATE)));
        let val = bs.to_int(&mut ctx, FieldTypeTp::LongLong);
        assert_eq!(val.unwrap(), 123i64);
        assert_eq!(ctx.warnings.warning_cnt, 0);

        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::IGNORE_TRUNCATE)));
        let invalid_utf8 = vec![b'1', b'2', b'3', 0, 159, 146, 150];
        let val = invalid_utf8.to_int(&mut ctx, FieldTypeTp::LongLong);
        assert_eq!(val.unwrap(), 123i64);
        assert_eq!(ctx.warnings.warning_cnt, 0);

        // TRUNCATE_AS_WARNING
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::TRUNCATE_AS_WARNING)));
        let val = bs.to_int(&mut ctx, FieldTypeTp::LongLong);
        assert_eq!(val.unwrap(), 123i64);
        assert_eq!(ctx.warnings.warning_cnt, 1);

        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::TRUNCATE_AS_WARNING)));
        let val = invalid_utf8.to_int(&mut ctx, FieldTypeTp::LongLong);
        assert_eq!(val.unwrap(), 123i64);
        // note:
        // warning 1: vec!['1' as u8, '2' as u8, '3' as u8, 0, 159, 146, 150] -> utf8
        // warning 2: vec!['1' as u8, '2' as u8, '3' as u8, 0] -> float
        assert_eq!(
            ctx.warnings.warning_cnt, 2,
            "unexpected warning: {:?}",
            ctx.warnings.warnings
        );
    }

    #[test]
    fn test_bytes_to_int_without_context() {
        let tests: Vec<(&'static [u8], i64)> = vec![
            (b"0", 0),
            (b" 23a", 23),
            (b"\t 23a", 23),
            (b"\r23a", 0),
            (b"1", 1),
            (b"2.1", 2),
            (b"23e10", 23),
            (b"ab", 0),
            (b"4a", 4),
            (b"+1024", 1024),
            (b"-231", -231),
            (b"", 0),
            (b"9223372036854775807", i64::MAX),
            (b"-9223372036854775808", i64::MIN),
        ];

        for (bs, n) in tests {
            let t = super::bytes_to_int_without_context(bs).unwrap();
            if t != n {
                panic!("expect convert {:?} to {}, but got {}", bs, n, t);
            }
        }

        let invalid_cases: Vec<&'static [u8]> =
            vec![b"9223372036854775809", b"-9223372036854775810"];
        for bs in invalid_cases {
            match super::bytes_to_int_without_context(bs) {
                Err(e) => assert!(e.is_overCausetxctx()),
                res => panic!("expect convert {:?} to over_causetxctx, but got {:?}", bs, res),
            };
        }
    }

    #[test]
    fn test_cast_to_int() {
        let test_cases = vec![
            ("{}", 0),
            ("[]", 0),
            ("3", 3),
            ("-3", -3),
            ("4.1", 4),
            ("4.5", 5),
            ("true", 1),
            ("false", 0),
            ("null", 0),
            (r#""hello""#, 0),
            (r#""1234""#, 1234),
        ];

        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::default_for_test()));
        for (jstr, exp) in test_cases {
            let json: Json = jstr.parse().unwrap();
            let get = json.to_int(&mut ctx, FieldTypeTp::LongLong).unwrap();
            assert_eq!(get, exp, "json.as_i64 get: {}, exp: {}", get, exp);
        }
    }

    #[test]
    fn test_int_to_uint() {
        let tests: Vec<(i64, FieldTypeTp, Option<u64>)> = vec![
            (123, FieldTypeTp::Tiny, Some(123)),
            (256, FieldTypeTp::Tiny, None),
            (123, FieldTypeTp::Short, Some(123)),
            (65536, FieldTypeTp::Short, None),
            (123, FieldTypeTp::Int24, Some(123)),
            (16777216, FieldTypeTp::Int24, None),
            (16777216, FieldTypeTp::Long, Some(16777216)),
            (4294967297, FieldTypeTp::Long, None),
            (8388610, FieldTypeTp::LongLong, Some(8388610)),
            (-1, FieldTypeTp::LongLong, Some(u64::MAX)),
        ];

        let mut ctx = EvalContext::default();
        for (from, tp, to) in tests {
            let r = from.to_uint(&mut ctx, tp);
            match to {
                Some(to) => assert_eq!(to, r.unwrap()),
                None => assert!(
                    r.is_err(),
                    "from: {}, to tp: {} should be over_causetxctx",
                    from,
                    tp
                ),
            }
        }

        // SHOULD_CLIP_TO_ZERO
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::IN_INSERT_STMT)));
        let r = (-12345 as i64).to_uint(&mut ctx, FieldTypeTp::LongLong);
        assert!(r.is_err());

        // SHOULD_CLIP_TO_ZERO | OVERCausetxctx_AS_WARNING
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(
            Flag::IN_INSERT_STMT | Flag::OVERCausetxctx_AS_WARNING,
        )));
        let r = (-12345 as i64)
            .to_uint(&mut ctx, FieldTypeTp::LongLong)
            .unwrap();
        assert_eq!(r, 0);
    }

    #[test]
    fn test_uint_into_uint() {
        let tests: Vec<(u64, FieldTypeTp, Option<u64>)> = vec![
            (123, FieldTypeTp::Tiny, Some(123)),
            (256, FieldTypeTp::Tiny, None),
            (123, FieldTypeTp::Short, Some(123)),
            (65536, FieldTypeTp::Short, None),
            (123, FieldTypeTp::Int24, Some(123)),
            (16777216, FieldTypeTp::Int24, None),
            (8388610, FieldTypeTp::Long, Some(8388610)),
            (4294967297, FieldTypeTp::Long, None),
            (4294967297, FieldTypeTp::LongLong, Some(4294967297)),
            (u64::MAX, FieldTypeTp::LongLong, Some(u64::MAX)),
        ];

        let mut ctx = EvalContext::default();
        for (from, tp, to) in tests {
            let r = from.to_uint(&mut ctx, tp);
            match to {
                Some(to) => assert_eq!(to, r.unwrap()),
                None => assert!(
                    r.is_err(),
                    "from: {}, to tp: {} should be over_causetxctx",
                    from,
                    tp
                ),
            }
        }
    }

    #[test]
    fn test_float_to_uint() {
        let tests: Vec<(f64, FieldTypeTp, Option<u64>)> = vec![
            (123.1, FieldTypeTp::Tiny, Some(123)),
            (123.6, FieldTypeTp::Tiny, Some(124)),
            (256.5, FieldTypeTp::Tiny, None),
            (256.1, FieldTypeTp::Short, Some(256)),
            (256.6, FieldTypeTp::Short, Some(257)),
            (65535.5, FieldTypeTp::Short, None),
            (65536.1, FieldTypeTp::Int24, Some(65536)),
            (65536.5, FieldTypeTp::Int24, Some(65537)),
            (16777215.4, FieldTypeTp::Int24, Some(16777215)),
            (16777216.1, FieldTypeTp::Int24, None),
            (8388610.4, FieldTypeTp::Long, Some(8388610)),
            (8388610.5, FieldTypeTp::Long, Some(8388611)),
            (4294967296.8, FieldTypeTp::Long, None),
            (4294967296.8, FieldTypeTp::LongLong, Some(4294967297)),
            (4294967297.1, FieldTypeTp::LongLong, Some(4294967297)),
            (-4294967297.1, FieldTypeTp::LongLong, None),
            (f64::MAX, FieldTypeTp::LongLong, None),
            (f64::MIN, FieldTypeTp::LongLong, None),
        ];

        let mut ctx = EvalContext::default();
        for (from, tp, to) in tests {
            let r = from.to_uint(&mut ctx, tp);
            match to {
                Some(to) => assert_eq!(to, r.unwrap()),
                None => assert!(
                    r.is_err(),
                    "from: {}, to tp: {} should be over_causetxctx",
                    from,
                    tp
                ),
            }
        }
    }

    #[test]
    fn test_bytes_to_uint() {
        let tests: Vec<(&[u8], FieldTypeTp, Option<u64>)> = vec![
            (b"123.1", FieldTypeTp::Tiny, Some(123)),
            (b"1.231e2", FieldTypeTp::Tiny, Some(123)),
            (b"1.235e2", FieldTypeTp::Tiny, Some(124)),
            (b"123.6", FieldTypeTp::Tiny, Some(124)),
            (b"256.5", FieldTypeTp::Tiny, None),
            (b"256.1", FieldTypeTp::Short, Some(256)),
            (b"256.6", FieldTypeTp::Short, Some(257)),
            (b"65535.5", FieldTypeTp::Short, None),
            (b"65536.1", FieldTypeTp::Int24, Some(65536)),
            (b"65536.5", FieldTypeTp::Int24, Some(65537)),
            (b"18388610.2", FieldTypeTp::Int24, None),
            (b"8388610.4", FieldTypeTp::Long, Some(8388610)),
            (b"8388610.5", FieldTypeTp::Long, Some(8388611)),
            (b"4294967296.8", FieldTypeTp::Long, None),
            (b"4294967296.8", FieldTypeTp::LongLong, Some(4294967297)),
            (b"4294967297.1", FieldTypeTp::LongLong, Some(4294967297)),
        ];

        let mut ctx = EvalContext::default();
        for (from, tp, to) in tests {
            let r = from.to_uint(&mut ctx, tp);
            match to {
                Some(to) => assert_eq!(to, r.unwrap()),
                None => assert!(
                    r.is_err(),
                    "from: {:?}, to tp: {} should be over_causetxctx",
                    from,
                    tp
                ),
            }
        }
    }


    ///Get Causetid and Solitonid with Lightlike_hash_join_test.sql
    ///
    /// This function is used to test the `get_causetid_and_solitonid` function.
    ///
    /// # Arguments
    ///
    /// * `ctx`: The context of the test.
    /// * `sql`: The sql to be executed.
    /// * `causetid`: The expected causetid.
    /// * `solitonid`: The expected solitonid.clone()
    /// * `is_ok`: Whether the function should be ok or not.
    /// If the function is not ok, the function will panic.
    #[allow(dead_code)]
       fn get_causetid_and_solitonid_test(
        ctx: &EvalContext,
        sql: &str,
        causetid: u64,
        solitonid: u64,
        is_ok: bool,
    ) {
        let sql_parser = SqlParser::new();
        let mut parser_result = sql_parser.parse(sql);
        let mut ctx = EvalContext::default();
        let mut exec_result = parser_result
            .execute(
                &mut ctx,
                &[
                    (
                        "a".to_string(),
                        Datum::Bytes(b"a".to_vec()),
                    ),
                    (
                        "b".to_string(),
                        Datum::Bytes(b"b".to_vec()),
                    ),
                    (
                        "c".to_string(),
                        Datum::Bytes(b"c".to_vec()),
                    ),
                    (
                        "d".to_string(),
                        Datum::Bytes(b"d".to_vec()),
                    ),
                    (
                        "e".to_string(),
                        Datum::Bytes(b"e".to_vec()),
                    ),
                    (
                        "f".to_string(),
                        Datum::Bytes(b"f".to_vec()),
                    ),
                    (
                        "g".to_string(),
                        Datum::Bytes(b"g".to_vec()),
                    ),
                    (
                        "h".to_string(),
                        Datum::Bytes(b"h".to_vec()),
                    ),
                    (
                        "i".to_string(),
                        Datum::Bytes(b"i".to_vec()),
                    ),
                    (
                        "j".to_string(),
                        Datum::Bytes(b"j".to_vec()),
                    ),
                    (
                        "k".to_string(),
                          Datum::Bytes(b"k".to_vec()),
                    ),
                ],

            if is_ok {
                Ok(Some(Datum::Bytes(b"a".to_vec())))
            } else {
                panic!("sql: {} should be failed", sql);
            },
        );

        let mut row = exec_result.unwrap().unwrap();
        let mut causetid = causetid;
        let mut solitonid = solitonid;
        let mut causetid_and_solitonid = get_causetid_and_solitonid(&mut row);

        if is_ok {
            assert_eq!(causetid, causetid_and_solitonid.0);
            assert_eq!(solitonid, causetid_and_solitonid.1);
        } else {
            assert!(causetid_and_solitonid.0 == 0 && causetid_and_solitonid.1 == 0);
        }
    }





                #[test]
    fn test_bytes_to_uint_without_context() {
        let tests: Vec<(&'static [u8], u64)> = vec![
            (b"0", 0),
            (b" 23a", 23),
            (b"\t 23a", 23),
            (b"\r23a", 0),
            (b"1", 1),
            (b"2.1", 2),
            (b"23e10", 23),
            (b"ab", 0),
            (b"4a", 4),
            (b"+1024", 1024),
            (b"231", 231),
            (b"18446744073709551615", u64::MAX),
        ];

        for (bs, n) in tests {
            let t = super::bytes_to_uint_without_context(bs).unwrap();
            if t != n {
                panic!("expect convert {:?} to {}, but got {}", bs, n, t);
            }
        }

        let invalid_cases: Vec<&'static [u8]> = vec![b"18446744073709551616"];
        for bs in invalid_cases {
            match super::bytes_to_uint_without_context(bs) {
                Err(e) => assert!(e.is_overCausetxctx()),
                res => panic!("expect convert {:?} to over_causetxctx, but got {:?}", bs, res),
            };
        }
    }

    #[test]
    fn test_datatype_to_uint_over_causetxctx() {
        fn test_over_causetxctx<T: Debug + Clone + ToInt>(primitive_causet: T, dst: u64, tp: FieldTypeTp) {
            let mut ctx = EvalContext::default();
            let val = primitive_causet.to_uint(&mut ctx, tp);
            match val {
                Err(e) => assert_eq!(
                    e.code(),
                    ERR_DATA_OUT_OF_RANGE,
                    "expect code {}, but got: {}",
                    ERR_DATA_OUT_OF_RANGE,
                    e.code()
                ),
                res => panic!("expect convert {:?} to over_causetxctx, but got {:?}", primitive_causet, res),
            };


            let mut ctx =
                EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::OVERCausetxctx_AS_WARNING)));
            let val = primitive_causet.to_uint(&mut ctx, tp);
            assert_eq!(val.unwrap(), dst, "{:?} => {}", primitive_causet, dst);
            assert_eq!(ctx.warnings.warning_cnt, 1);
        }

        // int_to_uint
        let cases: Vec<(i64, u64, FieldTypeTp)> = vec![
            (12345, 255, FieldTypeTp::Tiny),
            (-1, 255, FieldTypeTp::Tiny),
            (123456, 65535, FieldTypeTp::Short),
            (-1, 65535, FieldTypeTp::Short),
            (16777216, 16777215, FieldTypeTp::Int24),
            (i64::MAX, 4294967295, FieldTypeTp::Long),
            (i64::MIN, u64::from(u32::MAX), FieldTypeTp::Long),
        ];
        for (primitive_causet, dst, tp) in cases {
            test_over_causetxctx(primitive_causet, dst, tp);
        }

        // uint_to_uint
        let cases: Vec<(u64, u64, FieldTypeTp)> = vec![
            (12345, 255, FieldTypeTp::Tiny),
            (123456, 65535, FieldTypeTp::Short),
            (16777216, 16777215, FieldTypeTp::Int24),
            (u64::MAX, 4294967295, FieldTypeTp::Long),
        ];
        for (primitive_causet, dst, tp) in cases {
            test_over_causetxctx(primitive_causet, dst, tp);
        }

        // float_to_uint
        let cases: Vec<(f64, u64, FieldTypeTp)> = vec![
            (255.5, 255, FieldTypeTp::Tiny),
            (12345f64, 255, FieldTypeTp::Tiny),
            (65535.6, 65535, FieldTypeTp::Short),
            (123456f64, 65535, FieldTypeTp::Short),
            (16777215.7, 16777215, FieldTypeTp::Int24),
            (83886078f64, 16777215, FieldTypeTp::Int24),
            (4294967296.8, 4294967295, FieldTypeTp::Long),
            (f64::MAX, 4294967295, FieldTypeTp::Long),
            (f64::MAX, u64::MAX, FieldTypeTp::LongLong),
        ];
        for (primitive_causet, dst, tp) in cases {
            test_over_causetxctx(primitive_causet, dst, tp);
        }

        // bytes_to_uint
        let cases: Vec<(&[u8], u64, FieldTypeTp)> = vec![
            (b"255.5", 255, FieldTypeTp::Tiny),
            (b"12345", 255, FieldTypeTp::Tiny),
            (b"65535.6", 65535, FieldTypeTp::Short),
            (b"123456", 65535, FieldTypeTp::Short),
            (b"16777215.7", 16777215, FieldTypeTp::Int24),
            (b"183886078", 16777215, FieldTypeTp::Int24),
            (b"4294967295.5", 4294967295, FieldTypeTp::Long),
            (b"314748364221339834234239", u64::MAX, FieldTypeTp::LongLong),
        ];
        for (primitive_causet, dst, tp) in cases {
            test_over_causetxctx(primitive_causet, dst, tp);
        }
    }

    #[test]
    fn test_bytes_to_uint_truncated() {
        let mut ctx = EvalContext::default();
        let bs = b"123bb".to_vec();
        let val = bs.to_uint(&mut ctx, FieldTypeTp::LongLong);
        match val {
            Err(e) => assert_eq!(
                e.code(),
                ERR_TRUNCATE_WRONG_VALUE,
                "expect data truncated, but got {:?}",
                e
            ),
            res => panic!("expect convert {:?} to truncated, but got {:?}", bs, res),
        };

        // IGNORE_TRUNCATE
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::IGNORE_TRUNCATE)));
        let val = bs.to_uint(&mut ctx, FieldTypeTp::LongLong);
        assert_eq!(val.unwrap(), 123);

        // TRUNCATE_AS_WARNING
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::TRUNCATE_AS_WARNING)));
        let val = bs.to_uint(&mut ctx, FieldTypeTp::LongLong);
        assert_eq!(val.unwrap(), 123);
        assert_eq!(ctx.warnings.warnings.len(), 1);
    }

    #[test]
    fn test_cast_to_uint() {
        let test_cases = vec![
            ("{}", 0u64),
            ("[]", 0u64),
            ("3", 3u64),
            ("4.1", 4u64),
            ("4.5", 5u64),
            ("true", 1u64),
            ("false", 0u64),
            ("null", 0u64),
            (r#""hello""#, 0u64),
            (r#""1234""#, 1234u64),
        ];

        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::default_for_test()));
        for (jstr, exp) in test_cases {
            let json: Json = jstr.parse().unwrap();
            let get = json.to_uint(&mut ctx, FieldTypeTp::LongLong).unwrap();
            assert_eq!(get, exp, "json.as_u64 get: {}, exp: {}", get, exp);
        }
    }

    #[test]
    fn test_bytes_to_f64() {
        let tests: Vec<(&'static [u8], Option<f64>)> = vec![
            (b"", None),
            (b" 23", Some(23.0)),
            (b"-1", Some(-1.0)),
            (b"1.11", Some(1.11)),
            (b"1.11.00", None),
            (b"xx", None),
            (b"0x00", None),
            (b"11.xx", None),
            (b"xx.11", None),
        ];

        let mut ctx = EvalContext::default();
        for (i, (v, expect)) in tests.iter().enumerate() {
            let ff: Result<f64> = v.convert(&mut ctx);
            match expect {
                Some(val) => {
                    assert_eq!(ff.unwrap(), *val);
                }
                None => {
                    assert!(
                        ff.is_err(),
                        "Index: {}, {:?} should not be converted, but got: {:?}",
                        i,
                        v,
                        ff
                    );
                }
            }
        }

        // test over_causetxctx
        let mut ctx = EvalContext::default();
        let val: Result<f64> = f64::INFINITY.to_string().as_bytes().convert(&mut ctx);
        assert!(val.is_err());

        let mut ctx = EvalContext::default();
        let val: Result<f64> = f64::NEG_INFINITY.to_string().as_bytes().convert(&mut ctx);
        assert!(val.is_err());

        // TRUNCATE_AS_WARNING
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::TRUNCATE_AS_WARNING)));
        let val: f64 = (0..309)
            .map(|_| '9')
            .collect::<String>()
            .as_bytes()
            .convert(&mut ctx)
            .unwrap();
        assert_eq!(val, f64::MAX);
        assert_eq!(ctx.warnings.warning_cnt, 1);
        assert_eq!(
            ctx.warnings.warnings[0].get_code(),
            ERR_TRUNCATE_WRONG_VALUE
        );

        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::TRUNCATE_AS_WARNING)));
        let val: f64 = (0..310)
            .map(|i| if i == 0 { '-' } else { '9' })
            .collect::<String>()
            .as_bytes()
            .convert(&mut ctx)
            .unwrap();
        assert_eq!(val, f64::MIN);
        assert_eq!(ctx.warnings.warning_cnt, 1);
        assert_eq!(
            ctx.warnings.warnings[0].get_code(),
            ERR_TRUNCATE_WRONG_VALUE
        );

        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::TRUNCATE_AS_WARNING)));
        let val: Result<f64> = b"".to_vec().convert(&mut ctx);
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), 0.0);
        assert_eq!(ctx.warnings.warnings.len(), 1);

        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::TRUNCATE_AS_WARNING)));
        let val: Result<f64> = b"1.1a".to_vec().convert(&mut ctx);
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), 1.1);
        assert_eq!(ctx.warnings.warnings.len(), 1);

        // IGNORE_TRUNCATE
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(Flag::IGNORE_TRUNCATE)));
        let val: Result<f64> = b"1.2a".to_vec().convert(&mut ctx);
        assert!(val.is_ok());
        assert_eq!(val.unwrap(), 1.2);
        assert_eq!(ctx.warnings.warnings.len(), 0);
    }

    #[test]
    fn test_get_valid_float_prefix() {
        let cases = vec![
            ("-100", "-100"),
            ("1abc", "1"),
            ("-1-1", "-1"),
            ("+1+1", "+1"),
            ("123..34", "123."),
            ("123.23E-10", "123.23E-10"),
            ("1.1e1.3", "1.1e1"),
            ("11e1.3", "11e1"),
            ("1.1e-13a", "1.1e-13"),
            ("1.", "1."),
            (".1", ".1"),
            ("", "0"),
            ("123e+", "123"),
            ("123.e", "123."),
            ("1-1-", "1"),
            ("11-1-", "11"),
            ("-1-1-", "-1"),
        ];

        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::default_for_test()));
        for (i, o) in cases {
            assert_eq!(super::get_valid_float_prefix(&mut ctx, i).unwrap(), o);
        }
    }

    #[test]
    fn test_round_int_str() {
        let cases = vec![
            ("123", '1', "123"),
            ("123", '4', "123"),
            ("123", '5', "124"),
            ("123", '6', "124"),
            ("999", '6', "1000"),
            ("998", '6', "999"),
            ("989", '6', "990"),
            ("989898979", '6', "989898980"),
            ("989898999", '6', "989899000"),
            ("+989898999", '6', "+989899000"),
            ("-989898999", '6', "-989899000"),
        ];

        for (s, n, expect) in cases {
            let got = round_int_str(n, s);
            assert_eq!(
                got, expect,
                "round int str: {}, {}, expect: {}, got: {}",
                s, n, expect, got
            )
        }
    }

    #[test]
    fn test_invalid_get_valid_int_prefix() {
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::default_for_test()));
        let cases = vec!["1e21", "1e9223372036854775807"];

        // Firstly, make sure no error returns, instead a valid float string is returned
        for i in cases {
            let o = super::get_valid_int_prefix(&mut ctx, i);
            assert_eq!(o.unwrap(), i);
        }

        // Secondly, make sure warnings are attached when the float string cannot be casted to a valid int string
        let warnings = ctx.take_warnings().warnings;
        assert_eq!(warnings.len(), 2);
        for warning in warnings {
            assert_eq!(warning.get_code(), ERR_DATA_OUT_OF_RANGE);
        }
    }

    #[test]
    fn test_valid_get_valid_int_prefix() {
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::default_for_test()));
        let cases = vec![
            ("+0.0", "0"),
            ("+000.0", "000"),
            ("-0.0", "0"),
            ("-000.0", "-000"),
            (".1", "0"),
            (".0", "0"),
            (".5", "1"),
            ("+.5", "1"),
            ("-.5", "-1"),
            (".5e0", "1"),
            ("+.5e0", "+1"),
            ("-.5e0", "-1"),
            ("6.01e-1", "1"),
            ("123", "123"),
            ("255.5", "256"),
            ("123e1", "1230"),
            ("123.1e2", "12310"),
            ("1.231e2", "123"),
            ("1.236e2", "124"),
            ("123.45e5", "12345000"),
            ("123.55e5", "12355000"),
            ("123.45678e5", "12345678"),
            ("123.456789e5", "12345679"),
            ("123.456784e5", "12345678"),
            ("123.456999e5", "12345700"),
            ("-123.45678e5", "-12345678"),
            ("+123.45678e5", "+12345678"),
            ("9e20", "900000000000000000000"),
        ];

        for (i, e) in cases {
            let o = super::get_valid_int_prefix(&mut ctx, i);
            assert_eq!(o.unwrap(), *e, "{}, {}", i, e);
        }
        assert_eq!(ctx.take_warnings().warnings.len(), 0);

        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::from_flag(
            Flag::IN_SELECT_STMT | Flag::IGNORE_TRUNCATE | Flag::OVERCausetxctx_AS_WARNING,
        )));
        let cases = vec![
            ("+0.0", "+0"),
            ("100", "100"),
            ("+100", "+100"),
            ("-100", "-100"),
            ("9e20", "9"),
            ("+9e20", "+9"),
            ("-9e20", "-9"),
            ("-900e20", "-900"),
        ];

        for (i, e) in cases {
            let o = super::get_valid_int_prefix(&mut ctx, i);
            assert_eq!(o.unwrap(), *e, "{}, {}", i, e);
        }
        assert_eq!(ctx.take_warnings().warnings.len(), 0);
    }

    #[test]
    fn test_truncate_binary() {
        let s = b"123456789".to_vec();
        let mut s1 = s.clone();
        truncate_binary(&mut s1, crate::def::UNSPECIFIED_LENGTH);
        assert_eq!(s1, s);
        let mut s2 = s.clone();
        truncate_binary(&mut s2, isize::MAX);
        assert_eq!(s2, s);
        let mut s3 = s;
        truncate_binary(&mut s3, 0);
        assert!(s3.is_empty());
        // TODO port tests from MEDB(MEDB haven't implemented now)
    }

    #[test]
    fn test_truncate_f64() {
        let cases = vec![
            (100.114, 10, 2, Res::Ok(100.11)),
            (100.115, 10, 2, Res::Ok(100.12)),
            (100.1156, 10, 3, Res::Ok(100.116)),
            (100.1156, 3, 1, Res::OverCausetxctx(99.9)),
            (1.36, 10, 2, Res::Ok(1.36)),
            (f64::NAN, 10, 1, Res::OverCausetxctx(0f64)),
        ];

        for (f, flen, decimal, exp) in cases {
            let res = truncate_f64(f, flen, decimal);
            assert_eq!(res, exp);
        }
    }

    #[test]
    fn test_produce_str_with_specified_tp() {
        let cases = vec![
            // branch 1
            ("世界，中国", 1, charset::CHARSET_UTF8),
            ("世界，中国", 2, charset::CHARSET_UTF8),
            ("世界，中国", 3, charset::CHARSET_UTF8),
            ("世界，中国", 4, charset::CHARSET_UTF8),
            ("世界，中国", 5, charset::CHARSET_UTF8),
            ("世界，中国", 6, charset::CHARSET_UTF8),
            // branch 2
            ("世界，中国", 1, charset::CHARSET_ASCII),
            ("世界，中国", 2, charset::CHARSET_ASCII),
            ("世界，中国", 3, charset::CHARSET_ASCII),
            ("世界，中国", 4, charset::CHARSET_ASCII),
            ("世界，中国", 5, charset::CHARSET_ASCII),
            ("世界，中国", 6, charset::CHARSET_ASCII),
        ];

        let braneg = PolicyGradient::from_flag(Flag::TRUNCATE_AS_WARNING);
        let mut ctx = EvalContext::new(Arc::new(braneg));
        let mut ft = FieldType::default();

        for (s, char_num, cs) in cases {
            ft.set_charset(cs.to_string());
            ft.set_flen(char_num);
            let bs = s.as_bytes();
            let r = produce_str_with_specified_tp(&mut ctx, Cow::Borrowed(bs), &ft, false);
            assert!(r.is_ok(), "{}, {}, {}", s, char_num, cs);
            let p = r.unwrap();

            if cs == charset::CHARSET_UTF8MB4 || cs == charset::CHARSET_UTF8 {
                let ns: String = s.chars().take(char_num as usize).collect();
                assert_eq!(p.as_ref(), ns.as_bytes(), "{}, {}, {}", s, char_num, cs);
            } else {
                assert_eq!(
                    p.as_ref(),
                    &bs[..(char_num as usize)],
                    "{}, {}, {}",
                    s,
                    char_num,
                    cs
                );
            }
        }

        let cases = vec![
            // branch 3
            ("世界，中国", 20, charset::CHARSET_ASCII),
            ("世界，中国", 30, charset::CHARSET_ASCII),
            ("世界，中国", 50, charset::CHARSET_ASCII),
        ];

        use crate::FieldTypeAccessor;

        let braneg = PolicyGradient::from_flag(Flag::TRUNCATE_AS_WARNING);
        let mut ctx = EvalContext::new(Arc::new(braneg));
        let mut ft = FieldType::default();
        let fta = ft.as_mut_accessor();
        fta.set_tp(FieldTypeTp::String);
        fta.set_collation(Collation::Binary);

        for (s, char_num, cs) in cases {
            ft.set_charset(cs.to_string());
            ft.set_flen(char_num);
            let bs = s.as_bytes();
            let r = produce_str_with_specified_tp(&mut ctx, Cow::Borrowed(bs), &ft, true);
            assert!(r.is_ok(), "{}, {}, {}", s, char_num, cs);

            let p = r.unwrap();
            assert_eq!(p.len(), char_num as usize, "{}, {}, {}", s, char_num, cs);
        }
    }

    #[test]
    fn test_produce_dec_with_specified_tp() {
        use std::str::FromStr;

        let cases = vec![
            // branch 1
            (
                Decimal::from_str("11.1").unwrap(),
                2,
                2,
                max_or_min_dec(false, 2u8, 2u8),
            ),
            (
                Decimal::from_str("-111.1").unwrap(),
                2,
                2,
                max_or_min_dec(true, 2u8, 2u8),
            ),
            // branch 2
            (
                Decimal::from_str("-1111.1").unwrap(),
                5,
                1,
                Decimal::from_str("-1111.1").unwrap(),
            ),
            (
                Decimal::from_str("-111.111").unwrap(),
                5,
                2,
                Decimal::from_str("-111.11").unwrap(),
            ),
        ];

        let braneg = PolicyGradient::from_flag(Flag::TRUNCATE_AS_WARNING | Flag::OVERCausetxctx_AS_WARNING);
        let mut ctx = EvalContext::new(Arc::new(braneg));
        let mut ft = FieldType::default();

        for (dec, flen, decimal, want) in cases {
            ft.set_flen(flen);
            ft.set_decimal(decimal);
            let nd = produce_dec_with_specified_tp(&mut ctx, dec, &ft);
            assert!(nd.is_ok());
            let nd = nd.unwrap();
            assert_eq!(nd, want, "{}, {}, {}, {}, {}", dec, nd, want, flen, decimal);
        }
    }

    #[test]
    fn test_produce_dec_with_specified_tp_2() {
        let ul = isize::from(UNSPECIFIED_FSP);
        let cs = vec![
            // (
            // origin,
            // (origin_flen, origin_decimal), (res_flen, res_decimal), is_unsigned,
            // expect, warning_err_code,
            // ((InInsertStmt || InFIDelioStmt || InDeleteStmt), overCausetxctx_as_warning, truncate_as_warning)
            // )
            //
            // The origin_flen, origin_decimal field is to
            // let the programmer clearly know what the flen and decimal of the decimal is.

            // res_flen and res_decimal isn't UNSPECIFIED_FSP
            // flen < decimal
            (
                Decimal::zero(),
                (1, 0),
                (1, 2),
                false,
                Err(Error::m_bigger_than_d("")),
                None,
                (false, false, false),
            ),
            (
                Decimal::from(0),
                (1, 0),
                (1, 2),
                false,
                Err(Error::m_bigger_than_d("")),
                None,
                (false, false, false),
            ),
            // origin not zero, but res's int part len < origin's int part
            (
                Decimal::from(1024),
                (4, 0),
                (3, 0),
                false,
                Ok(Decimal::from(999)),
                Some(ERR_DATA_OUT_OF_RANGE),
                (false, true, false),
            ),
            (
                Decimal::from(-1024),
                (4, 0),
                (3, 0),
                false,
                Ok(Decimal::from(-999)),
                Some(ERR_DATA_OUT_OF_RANGE),
                (false, true, false),
            ),
            (
                Decimal::from_f64(10240.01).unwrap(),
                (7, 2),
                (5, 1),
                false,
                Ok(Decimal::from_f64(9999.9).unwrap()),
                Some(ERR_DATA_OUT_OF_RANGE),
                (false, true, false),
            ),
            (
                Decimal::from_f64(-10240.01).unwrap(),
                (7, 2),
                (5, 1),
                false,
                Ok(Decimal::from_f64(-9999.9).unwrap()),
                Some(ERR_DATA_OUT_OF_RANGE),
                (false, true, false),
            ),
            // origin_decimal < res_decimal
            (
                Decimal::from_f64(10.1234).unwrap(),
                (6, 4),
                (7, 5),
                false,
                Ok(Decimal::from_f64(10.12340).unwrap()),
                None,
                (false, false, false),
            ),
            (
                Decimal::from_f64(10.1234).unwrap(),
                (6, 4),
                (7, 5),
                false,
                Ok(Decimal::from_f64(10.12340).unwrap()),
                None,
                (true, false, false),
            ),
            (
                Decimal::from_f64(-10.1234).unwrap(),
                (6, 4),
                (7, 5),
                false,
                Ok(Decimal::from_f64(-10.12340).unwrap()),
                None,
                (false, false, false),
            ),
            (
                Decimal::from_f64(-10.1234).unwrap(),
                (6, 4),
                (7, 5),
                false,
                Ok(Decimal::from_f64(-10.12340).unwrap()),
                None,
                (true, false, false),
            ),
            (
                Decimal::from_f64(10.1234).unwrap(),
                (6, 4),
                (7, 5),
                true,
                Ok(Decimal::from_f64(10.12340).unwrap()),
                None,
                (false, false, false),
            ),
            (
                Decimal::from_f64(10.1234).unwrap(),
                (6, 4),
                (7, 5),
                true,
                Ok(Decimal::from_f64(10.12340).unwrap()),
                None,
                (true, false, false),
            ),
            (
                Decimal::from_f64(-10.1234).unwrap(),
                (6, 4),
                (7, 5),
                true,
                Ok(Decimal::zero()),
                None,
                (false, false, false),
            ),
            (
                Decimal::from_f64(-10.1234).unwrap(),
                (6, 4),
                (7, 5),
                true,
                Ok(Decimal::zero()),
                None,
                (true, false, false),
            ),
            // origin_decimal > res_decimal
            (
                Decimal::from_f64(10.1234).unwrap(),
                (6, 4),
                (5, 3),
                false,
                Ok(Decimal::from_f64(10.123).unwrap()),
                Some(WARN_DATA_TRUNCATED),
                (false, false, true),
            ),
            (
                Decimal::from_f64(10.1234).unwrap(),
                (6, 4),
                (5, 3),
                false,
                Ok(Decimal::from_f64(10.123).unwrap()),
                Some(WARN_DATA_TRUNCATED),
                (true, false, false),
            ),
            (
                Decimal::from_f64(-10.1234).unwrap(),
                (6, 4),
                (5, 3),
                false,
                Ok(Decimal::from_f64(-10.123).unwrap()),
                Some(WARN_DATA_TRUNCATED),
                (false, false, true),
            ),
            (
                Decimal::from_f64(-10.1234).unwrap(),
                (6, 4),
                (5, 3),
                false,
                Ok(Decimal::from_f64(-10.123).unwrap()),
                Some(WARN_DATA_TRUNCATED),
                (true, false, false),
            ),
            (
                Decimal::from_f64(10.1234).unwrap(),
                (6, 4),
                (5, 3),
                true,
                Ok(Decimal::from_f64(10.123).unwrap()),
                Some(WARN_DATA_TRUNCATED),
                (false, false, true),
            ),
            (
                Decimal::from_f64(10.1234).unwrap(),
                (6, 4),
                (5, 3),
                true,
                Ok(Decimal::from_f64(10.123).unwrap()),
                Some(WARN_DATA_TRUNCATED),
                (true, false, false),
            ),
            (
                Decimal::from_f64(-10.1234).unwrap(),
                (6, 4),
                (5, 3),
                true,
                Ok(Decimal::zero()),
                Some(WARN_DATA_TRUNCATED),
                (false, false, true),
            ),
            (
                Decimal::from_f64(-10.1234).unwrap(),
                (6, 4),
                (5, 3),
                true,
                Ok(Decimal::zero()),
                Some(WARN_DATA_TRUNCATED),
                (true, false, false),
            ),
            // if after round, the dec is zero, then there is no err or warning
            (
                Decimal::from_f64(0.00001).unwrap(),
                (5, 5),
                (4, 4),
                false,
                Ok(Decimal::zero()),
                None,
                (false, false, false),
            ),
            (
                Decimal::from_f64(0.00001).unwrap(),
                (5, 5),
                (4, 4),
                false,
                Ok(Decimal::zero()),
                None,
                (true, false, false),
            ),
            (
                Decimal::from_f64(-0.00001).unwrap(),
                (5, 5),
                (4, 4),
                false,
                Ok(Decimal::zero()),
                None,
                (false, false, false),
            ),
            (
                Decimal::from_f64(-0.00001).unwrap(),
                (5, 5),
                (4, 4),
                false,
                Ok(Decimal::zero()),
                None,
                (true, false, false),
            ),
            (
                Decimal::from_f64(0.00001).unwrap(),
                (5, 5),
                (4, 4),
                true,
                Ok(Decimal::zero()),
                None,
                (false, false, false),
            ),
            (
                Decimal::from_f64(0.00001).unwrap(),
                (5, 5),
                (4, 4),
                true,
                Ok(Decimal::zero()),
                None,
                (true, false, false),
            ),
            (
                Decimal::from_f64(-0.00001).unwrap(),
                (5, 5),
                (4, 4),
                true,
                Ok(Decimal::zero()),
                None,
                (false, false, false),
            ),
            (
                Decimal::from_f64(-0.00001).unwrap(),
                (5, 5),
                (4, 4),
                true,
                Ok(Decimal::zero()),
                None,
                (true, false, false),
            ),
            // TODO: add test case for Decimal::round failure

            // zero
            // FIXME:
            //  according to Decimal::prec_and_frac,
            //  the decimals' prec(the number of all digits) and frac(the number of digit after number point) are
            //  Decimal::zero()'s is (1, 0)
            //  Decimal::from_bytes(b"00.00")'s is (2, 2)
            //  Decimal::from_bytes(b"000.00")'s is (2, 2)
            //  Decimal::from_bytes(b"000.00")'s is (2, 2)
            //  Decimal::from_bytes(b"00.000")'s is (3, 3)
            //  Decimal::from_bytes(b"00.0000")'s is (4, 4)
            //  Decimal::from_bytes(b"00.00000")'s is (5, 5)
            //  This may be a bug.
            //  However, the case below are based on these expect.
            (
                Decimal::from_bytes(b"0.00").unwrap().unwrap(),
                (2, 2),
                (ul, ul),
                false,
                Ok(Decimal::zero()),
                None,
                (false, false, false),
            ),
            (
                Decimal::zero(),
                (1, 0),
                (0, 0),
                false,
                Ok(Decimal::zero()),
                None,
                (false, false, false),
            ),
            (
                Decimal::from_bytes(b"0.0000").unwrap().unwrap(),
                (4, 4),
                (4, 1),
                false,
                Ok(Decimal::zero()),
                None,
                (false, false, false),
            ),
        ];

        for (
            input,
            (origin_flen, origin_decimal),
            (res_flen, res_decimal),
            is_unsigned,
            expect,
            warning_err_code,
            (in_dml, overCausetxctx_as_warning, truncate_as_warning),
        ) in cs
        {
            // check origin_flen and origin_decimal
            let (f, d) = input.prec_and_frac();
            let log = format!(
                    "input: {}, origin_flen: {}, origin_decimal: {}, actual flen: {}, actual decimal: {}",
                    input, origin_flen, origin_decimal, f, d
                );
            assert_eq!(f, origin_flen, "{}", log);
            assert_eq!(d, origin_decimal, "{}", log);

            // run test case
            let ctx_in_dml_flag = vec![Flag::IN_INSERT_STMT, Flag::IN_FIDelio_OR_DELETE_STMT];
            for in_dml_flag in ctx_in_dml_flag {
                // make ctx
                let mut flag: Flag = Flag::empty();
                if overCausetxctx_as_warning {
                    flag |= Flag::OVERCausetxctx_AS_WARNING;
                }
                if truncate_as_warning {
                    flag |= Flag::TRUNCATE_AS_WARNING;
                }
                if in_dml {
                    flag |= in_dml_flag;
                }
                let braneg = Arc::new(PolicyGradient::from_flag(flag));
                let mut ctx = EvalContext::new(braneg);

                // make field_type
                let mut rft = FieldType::default();
                let fta = rft.as_mut_accessor();
                fta.set_flen(res_flen);
                fta.set_decimal(res_decimal);
                if is_unsigned {
                    fta.set_flag(FieldTypeFlag::UNSIGNED);
                }

                // call produce_dec_with_specified_tp
                let r = produce_dec_with_specified_tp(&mut ctx, input, &rft);

                // make log
                let rs = r.as_ref().map(|x| x.to_string());
                let expect_str = expect.as_ref().map(|x| x.to_string());
                let log = format!(
                    "input: {}, origin_flen: {}, origin_decimal: {}, \
                     res_flen: {}, res_decimal: {}, is_unsigned: {}, \
                     in_dml: {}, in_dml_flag(if in_dml is false, it will take no effect): {:?}, \
                     expect: {:?}, expect: {:?}",
                    input,
                    origin_flen,
                    origin_decimal,
                    res_flen,
                    res_decimal,
                    is_unsigned,
                    in_dml,
                    in_dml_flag,
                    expect_str,
                    rs
                );

                // check result
                match &expect {
                    Ok(d) => {
                        assert!(r.is_ok(), "{}", log);
                        assert_eq!(&r.unwrap(), d, "{}", log);
                    }
                    Err(Error::Eval(_, _)) => {
                        if let Error::Eval(_, d) = r.err().unwrap() {
                            assert_eq!(d, ERR_M_BIGGER_THAN_D, "{}", log);
                        } else {
                            unreachable!("{}", log);
                        }
                    }
                    _ => unreachable!("{}", log),
                }

                // check warning
                match warning_err_code {
                    Some(code) => {
                        assert_eq!(ctx.warnings.warning_cnt, 1, "{}", log);
                        assert_eq!(ctx.warnings.warnings[0].get_code(), code, "{}", log);
                    }
                    None => assert_eq!(ctx.warnings.warning_cnt, 0, "{}", log),
                }
            }
        }
    }
}


