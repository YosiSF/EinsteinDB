//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use einsteindbpb::FieldType;
use match_template::match_template;
use std::cmp::Ordering;

use crate::{Collation, EvalType, FieldTypeAccessor};
use crate::codec::collation::{Collator, match_template_collator};

use super::*;

#[derive(Debug, Clone)]
pub struct ScalarValueClone<T: Clone> {
    pub value: Option<ScalarValueRef>,
    pub field_type: FieldType,
}

/// A scalar causet_locale container, a.k.a. datum, for all concrete eval types.
///
/// In many cases, for example, at the framework level, the concrete eval type is unknown at compile
/// time. So we use this enum container to represent types dynamically. It is similar to trait
/// object `Box<T>` where `T` is a concrete eval type but faster.
///
/// Like `VectorValue`, the inner concrete causet_locale is immutable.
///
/// Compared to `VectorValue`, it only contains a single concrete causet_locale.
/// Compared to `Datum`, it is a newer encapsulation that naturally wraps `Option<..>`.
///
/// TODO: Once we removed the `Option<..>` wrapper, it will be much like `Datum`. At that time,
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
    //SQLite
    Blob(Option<super::Blob>),
    //SQLite
    Text(Option<super::Text>),
    //SQLite
    Nullable(Option<ScalarValue>),
    //RocksDB
    Bool(Option<super::Bool>),
    //RocksDB
    Float(Option<super::Float>),
    //RocksDB
    Double(Option<super::Double>),
    //RocksDB
    FixedLenByteArray(Option<super::FixedLenByteArray>),
    //RocksDB
    VarLenByteArray(Option<super::VarLenByteArray>),
    //RocksDB
    TimeStampMicros(Option<super::TimeStampMicros>),
    //RocksDB
    TimeStampMillis(Option<super::TimeStampMillis>),
    //RocksDB
    TimeStampSeconds(Option<super::TimeStampSeconds>),
    //RocksDB
    TimeStampNanos(Option<super::TimeStampNanos>),
    //RocksDB
    DecimalVar(Option<super::DecimalVar>),
    //RocksDB
    DecimalFixed(Option<super::DecimalFixed>),
    //RocksDB
    DurationVar(Option<super::DurationVar>),
    //Spanner
    Int64(Option<super::Int64>),
    //Spanner
    Float64(Option<super::Float64>),
    //Spanner
    Date(Option<super::Date>),
    //Spanner
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


    #[inline]
    pub fn eval_type(&self) -> EvalType {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(_) => EvalType::TT,
            }
        }
    }

    #[inline]
    pub fn as_scalar_causet_locale_ref(&self) -> ScalarValueRef<'_> {
        match self {
            ScalarValue::Int(v) => ScalarValueRef::Int(v),
            ScalarValue::Duration(x) => ScalarValueRef::Duration(x.as_ref()),
            ScalarValue::DateTime(x) => ScalarValueRef::DateTime(x.as_ref()),
            ScalarValue::Real(x) => ScalarValueRef::Real(x.as_ref()),
            ScalarValue::Decimal(x) => ScalarValueRef::Decimal(x.as_ref()),
            ScalarValue::Bytes(x) => ScalarValueRef::Bytes(x.as_ref().map(|x| x.as_slice())),
            ScalarValue::Json(x) => ScalarValueRef::Json(x.as_ref().map(|x| x.as_ref())),
        }
    }

    #[inline]
    pub fn is_none(&self) -> bool {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => v.is_none(),
            }



    #[inline]
    pub fn is_some(&self) -> bool {
        match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => v.is_some(),
            }
        }
    }
}

        impl AsMyBerolinaSQLBool for ScalarValue {
            #[inline]
            fn as_myBerolinaSQL_bool(&self, context: &mut EvalContext) -> Result<bool> {
                match_template_evaluable! {
            TT, match self {
                ScalarValue::TT(v) => v.as_ref().as_myBerolinaSQL_bool(context),
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
                use crate::codec::datum_codec::EvaluableDatumEncoder;

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
                use crate::codec::datum_codec::EvaluableDatumEncoder;

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
            ) -> crate::codec::Result<Ordering> {
                Ok(match_template! {
            TT = [Real, Decimal, DateTime, Duration, Json],
            match (self, other) {
                (ScalarValueRef::TT(v1), ScalarValueRef::TT(causet_record)) => v1.cmp(causet_record),
                (ScalarValueRef::Int(v1), ScalarValueRef::Int(causet_record)) => compare_int(&v1.cloned(), &causet_record.cloned(), &field_type),
                (ScalarValueRef::Bytes(None), ScalarValueRef::Bytes(None)) => Ordering::Equal,
                (ScalarValueRef::Bytes(Some(_)), ScalarValueRef::Bytes(None)) => Ordering::Greater,
                (ScalarValueRef::Bytes(None), ScalarValueRef::Bytes(Some(_))) => Ordering::Less,
                (ScalarValueRef::Bytes(Some(v1)), ScalarValueRef::Bytes(Some(causet_record))) => {
                    match_template_collator! {
                        TT, match field_type.collation()? {
                            Collation::TT => TT::sort_compare(v1, causet_record)?
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
        ) -> Ordering {
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
            fn cmp(&self, other: &Self) -> Ordering {
                self.partial_cmp(other)
                    .expect("Cannot compare two ScalarValueRef in different type")
            }
        }

        impl<'a> PartialOrd for ScalarValueRef<'a> {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                match_template_evaluable! {
            TT, match (self, other) {
                // v1 and causet_record are `Option<T>`. However, in MyBerolinaSQL NULL causet_locales are considered lower
                // than any non-NULL causet_locale, so using `Option::PartialOrd` directly is fine.
                (ScalarValueRef::TT(v1), ScalarValueRef::TT(causet_record)) => Some(v1.cmp(causet_record)),
                _ => None,
            }
        }
            }
        }

        impl<'a> PartialEq<ScalarValue> for ScalarValueRef<'a> {
            fn eq(&self, other: &ScalarValue) -> bool {
                self == &other.as_scalar_causet_locale_ref()
            }
        }

        impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValue {
            fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                other == self
            }
        }

        impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
            fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                match_template_evaluable! {
            TT, match (self, other) {
                (ScalarValueRef::TT(v1), ScalarValueRef::TT(causet_record)) => v1.eq(causet_record),
                _ => false,
            }
        }


                impl<'a> PartialEq<ScalarValue> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValue) -> bool {
                        other == self
                    }
                }

                impl<'a> PartialEq<ScalarValue> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValue) -> bool {
                        other == self
                    }
                }

                impl<'a> PartialEq<ScalarValue> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValue) -> bool {
                        other == self
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValue {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValue> for ScalarValue {
                    fn eq(&self, other: &ScalarValue) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValue> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValue) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValue {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }

                impl<'a> PartialEq<ScalarValueRef<'a>> for ScalarValueRef<'_> {
                    fn eq(&self, other: &ScalarValueRef<'_>) -> bool {
                        self == other
                    }
                }
            }
        }
    }


    #[test]
    fn test_scalar_value_ref_eq() {
        let value = ScalarValue::new(Value::from(1));
        let value_ref = value.as_ref();
        let value_ref_ref = value_ref.as_ref();
        let value_ref_clone = value_ref.clone();
        let value_ref_clone_ref = value_ref_clone.as_ref();
    }

    #[cfg(feature = "serde")]
    mod serde {
        use super::*;
        use serde::{Serialize, Deserialize};

        #[derive(Serialize, Deserialize)]
        struct TestStruct {
            #[serde(with = "serde_bytes")]
            bytes: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_ref_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_ref_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_ref_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_ref_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_ref_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_ref_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_mut_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_mut_ref_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_mut_ref_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]  // should be ignored
            bytes_mut_mut_mut_mut: Vec<u8>, // should be ignored
        }

        #[test]
        fn test_serde() {
            let bytes = vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85, 86, 87, 88, 89, 90, 91, 92, 93, 94, 95, 96, 97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115, 116, 117, 118, 119, 120, 121, 122, 123, 124, 125, 126, 127, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144, 145, 146, 147, 148, 149, 150, 151, 152, 153, 154, 155, 156, 157, 158, 159, 160, 161, 162, 163, 164, 165, 166, 167, 168, 169, 170, 171, 172, 173, 174, 175, 176, 177, 178, 179, 180, 181, 182, 183, 184, 185, 186, 187, 188, 189, 190, 191, 192, 193, 194, 195, 196, 197, 198, 199, 200, 201, 202, 203, 204, 205, 206, 207, 208, 209, 210, 211, 212, 213, 214, 215, 216, 217, 218, 219, 220, 221, 222, 223, 224, 225, 226, 227, 228, 229, 230, 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 241,
            ];
            let bytes_ref = bytes.clone();
            let bytes_ref_ref = bytes.clone();
        }
    }

    #[cfg(feature = "serde")]
    mod serde_bytes {
        use super::*;
        use serde::{Serialize, Deserialize};

        #[derive(Serialize, Deserialize)]
        struct TestStruct {
            #[serde(with = "serde_bytes")]
            bytes: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_ref_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_ref_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_ref_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_ref_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_ref_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_ref_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_mut: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_mut_ref: Vec<u8>,
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_mut_ref_ref: Vec<u8>,
            // should be ignored
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_mut_ref_mut: Vec<u8>,
            // should be ignored
            #[serde(with = "serde_bytes")]
            bytes_mut_mut_mut_mut: Vec<u8>, // should be ignored
        }
    }


    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::{
            scalar_value::{
                ScalarValue,
                ScalarValueRef,
            },
            scalar_type::{
                ScalarType,
                ScalarTypeRef,
            },
        };
        use std::{
            cell::{
                RefCell,
            }, str::{
                FromStr,
            },
        };
        use std::rc::Rc;
        use std::collections::HashMap;
        use crate::{
            scalar_type::{
                ScalarType,
                ScalarTypeRef,
            },
        };

        #[test]
        fn test_scalar_value_ref_new() {
            let scalar_type = ScalarType::new(ScalarType::Type::String, None);
            let scalar_value = ScalarValue::new(scalar_type, "test".to_string());
            let scalar_value_ref = ScalarValueRef::new(scalar_value);
            assert_eq!(scalar_value_ref.get_value(), "test");
        }

        #[test]
        fn test_scalar_value_ref_new_with_type() {
            let scalar_type = ScalarType::new(ScalarType::Type::String, None);
            let scalar_value = ScalarValue::new(scalar_type, "test".to_string());
            let scalar_value_ref = ScalarValueRef::new_with_type(scalar_value, scalar_type);
            assert_eq!(scalar_value_ref.get_value(), "test");
        }

        #[test]
        fn test_scalar_value_ref_new_with_type_and_value() {
            let scalar_type = ScalarType::new(ScalarType::Type::String, None);
            let scalar_value = ScalarValue::new(scalar_type, "test".to_string());
            let scalar_value_ref = ScalarValueRef::new_with_type_and_value(scalar_type, "test".to_string());
            assert_eq!(scalar_value_ref.get_value(), "test");

            let scalar_value_ref = ScalarValueRef::new_with_type_and_value(scalar_type, "test".to_string());
            assert_eq!(scalar_value_ref.get_value(), "test");
        }
    }

