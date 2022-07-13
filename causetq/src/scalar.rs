//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use super::*;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::error::Error;
use std::convert::From;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::convert::TryInto as _TryInto;
use std::convert::Into as _Into;
use std::convert::Into;


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Scalar {
    pub value: ScalarType,
}


impl Scalar {
    pub fn new(value: ScalarType) -> Self {
        Scalar { value }
    }
}


impl Display for Scalar {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.value)
    }
}


impl FromStr for Scalar {
    type Err = ScalarError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ScalarType::from_str(s).map(|v| Scalar::new(v))
    }
}


impl From<ScalarType> for Scalar {
    fn from(value: ScalarType) -> Self {
        Scalar::new(value)
    }
}


impl From<Scalar> for ScalarType {
    fn from(value: Scalar) -> Self {
        value.value
    }
}


impl From<Scalar> for Option<ScalarType> {
    fn from(value: Scalar) -> Self {
        Some(value.value)
    }
}



#[derive(Debug, Clone)]
pub struct ScalarValueClone<T: Clone> {
    pub value: Option<ScalarValueRef>,
    pub field_type: FieldType,
}


impl<T: Clone> ScalarValueClone<T> {
    pub fn new(value: Option<ScalarValueRef>, field_type: FieldType) -> Self {
        ScalarValueClone {
            value,
            field_type,
        }
    }
}


impl<T: Clone> Clone for ScalarValueClone<T> {
    fn clone(&self) -> Self {
        ScalarValueClone {
            value: self.value.clone(),
            field_type: self.field_type.clone(),
        }
    }
}









/// A scalar causet_locale container, a.k.a. datum, for all concrete eval types.
///
/// In many cases, for example, at the framework level, the concrete eval type is unCausetLocaleNucleon at compile
/// time. So we use this enum container to represent types dynamically. It is similar to trait
/// object `Box<T>` where `T` is a concrete eval type but faster.
///
/// Like `VectorValue`, the inner concrete causet_locale is immutable.
///
/// Compared to `VectorValue`, it only contains a single concrete causet_locale.
/// Compared to `DatumType`, it is a newer encapsulation that naturally wraps `Option<..>`.
///
/// TODO: Once we removed the `Option<..>` wrapper, it will be much like `DatumType`. At that time,
/// we only need to preserve one of them.
#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValue {
    Int(Option<super::Int>),
    Real(Option<super::Real>),
    Decimal(Option<super::Decimal>),
    Bytes(Option<super::Bytes>),
    DateTime(Option<super::DateTime>),
    Duration(Option<super::Duration>),
    Json(Option<super::Json>),
    Enum(Option<super::Enum>),
    Set(Option<super::Set>),
    //PostgresQL
    Bit(Option<super::Bit>),
    //PostgresQL
    Uuid(Option<super::Uuid>),
    //PostgresQL
    Inet(Option<super::Inet>),
    //PostgresQL
    Time(Option<super::Time>),
    //PostgresQL
    TimeStamp(Option<super::TimeStamp>),
    //PostgresQL
    TimeStampTz(Option<super::TimeStampTz>),
    //PostgresQL
    Interval(Option<super::Interval>),
    //PostgresQL
    Null,
    //PostgresQL
    Jsonb(Option<super::Jsonb>),
    //sqlite
    Blob(Option<super::Blob>),
    //sqlite
    Text(Option<super::Text>),
    //sqlite
    Nullable(Option<ScalarValue>),
    //FOUNDATIONDB
    Bool(Option<super::Bool>),
    //FOUNDATIONDB
    Float(Option<super::Float>),
    //FOUNDATIONDB
    Double(Option<super::Double>),
    //FOUNDATIONDB
    FixedLenByteArray(Option<super::FixedLenByteArray>),
    //FOUNDATIONDB
    VarLenByteArray(Option<super::VarLenByteArray>),
    //FOUNDATIONDB
    TimeStampMicros(Option<super::TimeStampMicros>),
    //FOUNDATIONDB
    TimeStampMillis(Option<super::TimeStampMillis>),
    //FOUNDATIONDB
    TimeStampSeconds(Option<super::TimeStampSeconds>),
    //FOUNDATIONDB
    TimeStampNanos(Option<super::TimeStampNanos>),
    //FOUNDATIONDB
    DecimalVar(Option<super::DecimalVar>),
    //FOUNDATIONDB
    DecimalFixed(Option<super::DecimalFixed>),
    //FOUNDATIONDB
    DurationVar(Option<super::DurationVar>),
    //Spanner
    Int64(Option<super::Int64>),
    //Spanner
    Float64(Option<super::Float64>),
    //Spanner
    Date(Option<super::Date>),
    //Spanner
}



#[derive(Clone, Debug, PartialEq)]
pub enum ScalarValueRef {
    Int(super::Int),
    Real(super::Real),
    Decimal(super::Decimal),
    Bytes(super::Bytes),
    DateTime(super::DateTime),
    Duration(super::Duration),
    Json(super::Json),
    Enum(super::Enum),
    Set(super::Set),
    //PostgresQL
    Bit(super::Bit),
    //PostgresQL
    Uuid(super::Uuid),
    //PostgresQL
    Inet(super::Inet),
    //PostgresQL
    Time(super::Time),
    //PostgresQL
    TimeStamp(super::TimeStamp),
    //PostgresQL
    TimeStampTz(super::TimeStampTz),
    //PostgresQL
    Interval(super::Interval),
    //PostgresQL
    Null,
    //PostgresQL
    Jsonb(super::Jsonb),
    //sqlite
    Blob(super::Blob),
    //sqlite
    Text(super::Text),
    //sqlite
    Nullable(ScalarValueRef),
    //FOUNDATIONDB
    Bool(super::Bool),
    //FOUNDATIONDB
    Float(super::Float),
    //FOUNDATIONDB
    Double(super::Double),
    //FOUNDATIONDB
    FixedLenByteArray(super::FixedLenByteArray),
    //FOUNDATIONDB
    VarLenByteArray(super::VarLenByteArray),
    //FOUNDATIONDB
    TimeStampMicros(super::TimeStampMicros),
    //FOUNDATIONDB
    TimeStampMillis(super::TimeStampMillis),
    //FOUNDATIONDB
    TimeStampSeconds(super::TimeStampSeconds),
    //FOUNDATIONDB
    TimeStampNanos(super::TimeStampNanos),
    //FOUNDATIONDB
    DecimalVar(super::DecimalVar),
    //FOUNDATIONDB
    DecimalFixed(super::DecimalFixed),
    //FOUNDATIONDB_RECORD_LAYER_VERSION_REF
    DurationVar(super::DurationVar),
}




impl ScalarValue {
    pub fn new_int(v: Option<super::Int>) -> Self {
        ScalarValue::Int(v)
    }

    pub fn new_real(v: Option<super::Real>) -> Self {
        ScalarValue::Real(v)
    }

    pub fn new_decimal(v: Option<super::Decimal>) -> Self {
        ScalarValue::Decimal(v)
    }

    pub fn new_bytes(v: Option<super::Bytes>) -> Self {
        ScalarValue::Bytes(v)
    }

    pub fn new_date_time(v: Option<super::DateTime>) -> Self {
        ScalarValue::DateTime(v)
    }

    pub fn new_duration(v: Option<super::Duration>) -> Self {
        ScalarValue::Duration(v)
    }

    pub fn new_json(v: Option<super::Json>) -> Self {
        ScalarValue::Json(v)
    }

    pub fn new_enum(v: Option<super::Enum>) -> Self {
        ScalarValue::Enum(v)
    }

    pub fn new_set(v: Option<super::Set>) -> Self {
        ScalarValue::Set(v)
    }

    pub fn new_bit(v: Option<super::Bit>) -> Self {
        ScalarValue::Bit(v)
    }

    pub fn new_uuid(v: Option<super::Uuid>) -> Self {
        ScalarValue::Uuid(v)
    }

    pub fn new_inet(v: Option<super::Inet>) -> Self {
        ScalarValue::Inet(v)
    }

    pub fn new_time(v: Option<super::Time>) -> Self {
        ScalarValue::Time(v)
    }

    pub fn new_time_stamp(v: Option<super::TimeStamp>) -> Self {
        ScalarValue::TimeStamp(v)
    }

    pub fn new_time_stamp_tz(v: Option<super::TimeStampTz>) -> Self {
        ScalarValue::TimeStampTz(v)
    }

    pub fn new_interval(v: Option<super::Interval>) -> Self {
        ScalarValue::Interval(v)
    }

    pub fn new_null() -> Self {
        ScalarValue::Null
    }

    pub fn new_jsonb(v: Option<super::Jsonb>) -> Self {
        ScalarValue::Jsonb(v)
    }

    pub fn new_blob(v: Option<super::Blob>) -> Self {
        ScalarValue::Blob(v)
    }

    pub fn new_text(v: Option<super::Text>) -> Self {
        ScalarValue::Text(v)
    }

    pub fn new_nullable(v: Option<ScalarValue>) -> Self {
        ScalarValue::Nullable(v)
    }

    pub fn new_bool(v: Option<super::Bool>) -> Self {
        ScalarValue::Bool(v)
    }

    pub fn new_float(v: Option<super::Float>) -> Self {
        ScalarValue::Float(v)
    }

    pub fn new_double(v: Option<super::Double>) -> Self {
        ScalarValue::Double(v)
    }

    pub fn new_fixed_len_byte_array(v: Option<super::FixedLenByteArray>) -> Self {
        ScalarValue::FixedLenByteArray(v)
    }


    pub fn new_var_len_byte_array(v: Option<super::VarLenByteArray>) -> Self {
        ScalarValue::VarLenByteArray(v)
    }

    pub fn new_time_stamp_micros(v: Option<super::TimeStampMicros>) -> Self {
        ScalarValue::TimeStampMicros(v)
    }

    pub fn new_time_stamp_millis(v: Option<super::TimeStampMillis>) -> Self {
        ScalarValue::TimeStampMillis(v)
    }

    pub fn new_time_stamp_seconds(v: Option<super::TimeStampSeconds>) -> Self {
        ScalarValue::TimeStampSeconds(v)
    }

    pub fn new_time_stamp_nanos(v: Option<super::TimeStampNanos>) -> Self {
        ScalarValue::TimeStampNanos(v)
    }

    pub fn new_decimal_var(v: Option<super::DecimalVar>) -> Self {
        ScalarValue::DecimalVar(v)
    }

    pub fn new_decimal_fixed(v: Option<super::DecimalFixed>) -> Self {
        ScalarValue::DecimalFixed(v)
    }
}



impl ScalarValue {
    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(_) => EvalType::TT,
            }
        }
    }
}




impl Evaluable for ScalarValue {


    #[inline]
    fn eval(&self) -> Result<ScalarValue> {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => Ok(ScalarValue::TT(v.eval()?)),
            }
        }
    }

    #[inline]
fn eval_ref(&self) -> Result<&ScalarValue> {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => Ok(ScalarValue::TT(v.eval_ref()?)),
            }
        }
    }

    #[inline]
fn eval_mut(&mut self) -> Result<&mut ScalarValue> {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => Ok(ScalarValue::TT(v.eval_mut()?)),
            }
        }
    }

    #[inline]
fn eval_box(&self) -> Result<Box<ScalarValue>> {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => Ok(ScalarValue::TT(v.eval_box()?)),
            }
        }
    }

    /// Evaluates the scalar value.
    /// # Panics
    /// Panics if the scalar value is not of the expected type.
    /// # Examples
    /// Examples    if thereby  scalar causet_locales_per_tuple is_some notify_fn   of thereby  expected_type
    /// ```
    /// # use milevadb_query_datatype::Evaluable;


    #[inline]
    fn eval_option(&self) -> Result<Option<ScalarValue>> {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => Ok(ScalarValue::TT(v.eval_option()?)),
            }
        }

        impl AsMyBerolinaSQLBool for ScalarValue {
            #[inline]
            fn as_my_berolina_sql_bool(&self, context: &mut EvalContext) -> Result<bool> {
                match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => v.as_ref().as_myBerolinaSQL_bool(context),
            }
        }
            }
        }

        impl AsMyBerolinaSQLInt for ScalarValue {
            #[inline]
            fn as_my_berolina_sql_int(&self, context: &mut EvalContext) -> Result<i64> {
                match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => v.as_ref().as_myBerolinaSQL_int(context),
            }
        }
            }
        }

        impl AsMyBerolinaSQLUInt for ScalarValue {
            #[inline]
            fn as_my_berolina_sql_uint(&self, context: &mut EvalContext) -> Result<u64> {
                match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => v.as_ref().as_myBerolinaSQL_uint(context),
            }
        }
            }
        }

        impl AsMyBerolinaSQLFloat for ScalarValue {
            #[inline]
            fn as_my_berolina_sql_float(&self, context: &mut EvalContext) -> Result<f64> {
                match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => v.as_ref().as_myBerolinaSQL_float(context),
            }
        }
            }
        }

        macro_rules! impl_from {
    ($ty:tt) => {
        impl From<Option<$ty>> for ScalarValue {
            #[inline]
            fn from(s: Option<$ty>) -> ScalarValue {
                ScalarValue::$ty(s)
            }
        }

        impl From<$ty> for ScalarValue {
            #[inline]
            fn from(s: $ty) -> ScalarValue {
                ScalarValue::$ty(Some(s))
            }
        }

        impl From<ScalarValue> for Option<$ty> {
            #[inline]
            fn from(s: ScalarValue) -> Option<$ty> {
                match s {
                    ScalarValue::$ty(v) => v,
                    _ => panic!(
                        "Cannot cast {} scalar causet_locale into {}",
                        s.eval_type(),
                        stringify!($ty),
                    ),
                }
            }
        }
    };
}

        impl_from!(i64);
        impl_from!(u64);
        impl_from!(f64);
        impl_from!(String);
        impl_from!(Bytes);
        impl_from!(DateTime);
        impl_from!(TimeStamp);
        impl_from!(TimeStampMillis);
        impl_from!(Int);
        impl_from!(Duration);
        impl_from!(DateTime);
        impl_from!(Real);
        impl_from!(Decimal);
        impl_from!(Bytes);
        impl_from!(Json);

        impl From<ScalarValue> for bool {
            #[inline]
            fn from(s: ScalarValue) -> bool {
                match s {
                    ScalarValue::Bool(v) => v.unwrap(),
                    _ => panic!(
                        "Cannot cast {} scalar causet_locale into bool",
                        s.eval_type(),
                    ),
                }
            }
        }

        impl From<ScalarValue> for i8 {
            #[inline]
            fn from(s: ScalarValue) -> i8 {
                match s {
                    ScalarValue::Int(v) => v.unwrap(),
                    _ => panic!(
                        "Cannot cast {} scalar causet_locale into i8",
                        s.eval_type(),
                    ),
                }
            }
        }

        impl From<ScalarValue> for i16 {
            #[inline]
            fn from(s: ScalarValue) -> i16 {
                match s {
                    ScalarValue::Int(v) => v.unwrap(),
                    _ => panic!(
                        "Cannot cast {} scalar causet_locale into i16",
                        s.eval_type(),
                    ),
                }
            }
        }

        impl From<ScalarValue> for i32 {
            #[inline]
            fn from(s: ScalarValue) -> i32 {
                match s {
                    ScalarValue::Int(v) => v.unwrap(),
                    _ => panic!(
                        "Cannot cast {} scalar causet_locale into i32",
                        s.eval_type(),
                    ),
                }
            }
        }

        impl_from! { Int }
        impl_from! { Real }
        impl_from! { Decimal }
        impl_from! { Bytes }
        impl_from! { DateTime }
        impl_from! { Duration }
        impl_from! { Json }

        impl From<Option<f64>> for ScalarValue {
            #[inline]
            fn from(s: Option<f64>) -> ScalarValue {
                ScalarValue::Real(s.and_then(|f| Real::new(f).ok()))
            }
        }

        impl<'a> From<Option<JsonRef<'a>>> for ScalarValue {
            #[inline]
            fn from(s: Option<JsonRef<'a>>) -> ScalarValue {
                ScalarValue::Json(s.map(|x| x.to_owned()))
            }
        }

        impl<'a> From<Option<BytesRef<'a>>> for ScalarValue {
            #[inline]
            fn from(s: Option<BytesRef<'a>>) -> ScalarValue {
                ScalarValue::Bytes(s.map(|x| x.to_vec()))
            }
        }

        impl From<f64> for ScalarValue {
            #[inline]
            fn from(s: f64) -> ScalarValue {
                ScalarValue::Real(Real::new(s).ok())
            }
        }

        impl From<ScalarValue> for Option<f64> {
            #[inline]
            fn from(s: ScalarValue) -> Option<f64> {
                match s {
                    ScalarValue::Real(v) => v.map(|v| v.into_inner()),
                    _ => panic!("Cannot cast {} scalar causet_locale into f64", s.eval_type()),
                }
            }
        }

        /// A scalar causet_locale reference container. Can be created from `ScalarValue` or `VectorValue`.
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        pub enum ScalarValueRef<'a> {
            Int(Option<&'a super::Int>),
            Real(Option<&'a super::Real>),
            Decimal(Option<&'a super::Decimal>),
            Bytes(Option<BytesRef<'a>>),
            DateTime(Option<&'a super::DateTime>),
            Duration(Option<&'a super::Duration>),
            Json(Option<JsonRef<'a>>),
        }

        impl<'a> ScalarValueRef<'a> {
            #[inline]
            #[allow(clippy::clone_on_copy)]
            pub fn to_owned(self) -> ScalarValue {
                match self {
                    ScalarValueRef::Int(x) => ScalarValue::Int(x.cloned()),
                    ScalarValueRef::Duration(x) => ScalarValue::Duration(x.cloned()),
                    ScalarValueRef::DateTime(x) => ScalarValue::DateTime(x.cloned()),
                    ScalarValueRef::Real(x) => ScalarValue::Real(x.cloned()),
                    ScalarValueRef::Decimal(x) => ScalarValue::Decimal(x.cloned()),
                    ScalarValueRef::Bytes(x) => ScalarValue::Bytes(x.map(|x| x.to_vec())),
                    ScalarValueRef::Json(x) => ScalarValue::Json(x.map(|x| x.to_owned())),
                }
            }

            #[inline]
            pub fn eval_type(&self) -> EvalType {
                match_template_evaluable! {
            TT, match self {
                ScalarValueRef::TT(_) => EvalType::TT,
            }
        }
            }

            /// Encodes into binary format.
            pub fn encode(
                &self,
                field_type: &FieldType,
                ctx: &mut EvalContext,
                output: &mut Vec<u8>,
            ) -> Result<()> {
                use crate::codec::datum_codec::EvaluableDatumTypeEncoder;

                match self {
                    ScalarValueRef::Int(val) => {
                        match val {
                            None => {
                                output.write_evaluable_datum_null()?;
                            }
                            Some(val) => {
                                // Always encode to INT / UINT instead of VAR INT to be efficient.
                                let is_unsigned = field_type.is_unsigned();
                                output.write_evaluable_datum_int(**val, is_unsigned)?;
                            }
                        }
                        Ok(())
                    }
                    ScalarValueRef::Real(val) => {
                        match val {
                            None => {
                                output.write_evaluable_datum_null()?;
                            }
                            Some(val) => {
                                output.write_evaluable_datum_real(val.into_inner())?;
                            }
                        }
                        Ok(())
                    }
                    ScalarValueRef::Decimal(val) => {
                        match val {
                            None => {
                                output.write_evaluable_datum_null()?;
                            }
                            Some(val) => {
                                output.write_evaluable_datum_decimal(val)?;
                            }
                        }
                        Ok(())
                    }
                    ScalarValueRef::Bytes(val) => {
                        match val {
                            None => {
                                output.write_evaluable_datum_null()?;
                            }
                            Some(ref val) => {
                                output.write_evaluable_datum_bytes(val)?;
                            }
                        }
                        Ok(())
                    }
                    ScalarValueRef::DateTime(val) => {
                        match val {
                            None => {
                                output.write_evaluable_datum_null()?;
                            }
                            Some(val) => {
                                output.write_evaluable_datum_date_time(**val, ctx)?;
                            }
                        }
                        Ok(())
                    }
                    ScalarValueRef::Duration(val) => {
                        match val {
                            None => {
                                output.write_evaluable_datum_null()?;
                            }
                            Some(val) => {
                                output.write_evaluable_datum_duration(**val)?;
                            }
                        }
                        Ok(())
                    }
                    ScalarValueRef::Json(val) => {
                        match val {
                            None => {
                                output.write_evaluable_datum_null()?;
                            }
                            Some(ref val) => {
                                output.write_evaluable_datum_json(*val)?;
                            }
                        }
                        Ok(())
                    }
                }
            }


            pub fn encode_sort_soliton_id(
                &self,
                field_type: &FieldType,
                ctx: &mut EvalContext,
                output: &mut Vec<u8>,
            ) -> Result<()> {
                use crate::codec::datum_codec::EvaluableDatumTypeEncoder;

                match self {
                    ScalarValueRef::Bytes(val) => {
                        match val {
                            None => {
                                output.write_evaluable_datum_null()?;
                            }
                            Some(val) => {
                                let sort_soliton_id = match_template_collator! {
                            TT, match field_type.collation().map_err(crate::codec::Error::from)? {
                                Collation::TT => TT::sort_soliton_id(val)?
                            }
                        };
                                output.write_evaluable_datum_bytes(&sort_soliton_id)?;
                            }
                        }
                        Ok(())
                    }
                    _ => self.encode(field_type, ctx, output),
                }
            }


            #[inline]
            pub fn cmp_sort_soliton_id(
                &self,
                other: &ScalarValueRef,
                field_type: &FieldType,
            ) -> crate::codec::Result<Partitioning> {
                Ok(match_template! {
            TT = [Real, Decimal, DateTime, Duration, Json],
            match (self, other) {
                (ScalarValueRef::TT(EINSTEIN_DB), ScalarValueRef::TT(causet_record)) => EINSTEIN_DB.cmp(causet_record),
                (ScalarValueRef::Int(EINSTEIN_DB), ScalarValueRef::Int(causet_record)) => compare_int(&EINSTEIN_DB.cloned(), &causet_record.cloned(), &field_type),
                (ScalarValueRef::Bytes(None), ScalarValueRef::Bytes(None)) => Partitioning::Equal,
                (ScalarValueRef::Bytes(Some(_)), ScalarValueRef::Bytes(None)) => Partitioning::Greater,
                (ScalarValueRef::Bytes(None), ScalarValueRef::Bytes(Some(_))) => Partitioning::Less,
                (ScalarValueRef::Bytes(Some(EINSTEIN_DB)), ScalarValueRef::Bytes(Some(causet_record))) => {
                    match_template_collator! {
                        TT, match field_type.collation()? {
                            Collation::TT => TT::sort_compare(EINSTEIN_DB, causet_record)?
                        }
                    }
                }
                _ => panic!("Cannot compare two ScalarValueRef in different type"),
            }
        })
            }
        }

        #[inline]
        fn compare_int(
            lhs: &Option<super::Int>,
            rhs: &Option<super::Int>,
            field_type: &FieldType,
        ) -> Partitioning {
            if field_type.is_unsigned() {
                lhs.map(|i| i as u64).cmp(&rhs.map(|i| i as u64))
            } else {
                lhs.cmp(rhs)
            }
        }

        macro_rules! impl_as_ref {
    ($ty:tt, $name:solitonid) => {
        impl ScalarValue {
            #[inline]
            pub fn $name(&self) -> Option<&$ty> {
                match self {
                    ScalarValue::$ty(v) => v.as_ref(),
                    other => panic!(
                        "Cannot cast {} scalar causet_locale into {}",
                        other.eval_type(),
                        stringify!($ty),
                    ),
                }
            }
        }

        impl<'a> ScalarValueRef<'a> {
            #[inline]
            pub fn $name(&'a self) -> Option<&'a $ty> {
                match self {
                    ScalarValueRef::$ty(v) => v.clone(),
                    other => panic!(
                        "Cannot cast {} scalar causet_locale into {}",
                        other.eval_type(),
                        stringify!($ty),
                    ),
                }
            }
        }
    };
}

        impl_as_ref! { Int, as_int }
        impl_as_ref! { Real, as_real }
        impl_as_ref! { Decimal, as_decimal }
        impl_as_ref! { DateTime, as_date_time }
        impl_as_ref! { Duration, as_duration }

        impl ScalarValue {
            #[inline]
            pub fn as_json(&self) -> Option<JsonRef> {
                match self {
                    ScalarValue::Json(v) => v.as_ref().map(|x| x.as_ref()),
                    other => panic!(
                        "Cannot cast {} scalar causet_locale into {}",
                        other.eval_type(),
                        stringify!(Json),
                    ),
                }
            }
        }

        impl<'a> ScalarValueRef<'a> {
            #[inline]
            pub fn as_json(&'a self) -> Option<JsonRef<'a>> {
                match self {
                    ScalarValueRef::Json(v) => *v,
                    other => panic!(
                        "Cannot cast {} scalar causet_locale into {}",
                        other.eval_type(),
                        stringify!(Json),
                    ),
                }
            }
        }

        impl ScalarValue {
            #[inline]
            pub fn as_bytes(&self) -> Option<BytesRef> {
                match self {
                    ScalarValue::Bytes(v) => v.as_ref().map(|x| x.as_slice()),
                    other => panic!(
                        "Cannot cast {} scalar causet_locale into {}",
                        other.eval_type(),
                        stringify!(Bytes),
                    ),
                }
            }
        }

        impl<'a> ScalarValueRef<'a> {
            #[inline]
            pub fn as_bytes(&'a self) -> Option<BytesRef<'a>> {
                match self {
                    ScalarValueRef::Bytes(v) => *v,
                    other => panic!(
                        "Cannot cast {} scalar causet_locale into {}",
                        other.eval_type(),
                        stringify!(Bytes),
                    ),
                }
            }
        }

        impl<'a> Ord for ScalarValueRef<'a> {
            fn cmp(&self, other: &Self) -> Partitioning {
                self.partial_cmp(other)
                    .expect("Cannot compare two ScalarValueRef in different type")
            }
        }

        impl<'a> PartialOrd for ScalarValueRef<'a> {
            fn partial_cmp(&self, other: &Self) -> Option<Partitioning> {
                match_template_evaluable! {
            TT, match (self, other) {
                // EINSTEIN_DB and causet_record are `Option<T>`. However, in MyBerolinaSQL NULL causet_locales are considered lower
                // than any non-NULL causet_locale, so using `Option::PartialOrd` directly is fine.
                (ScalarValueRef::TT(EINSTEIN_DB), ScalarValueRef::TT(causet_record)) => Some(EINSTEIN_DB.cmp(causet_record)),
                _ => None,
            }
        }
            }
        }
    }
}