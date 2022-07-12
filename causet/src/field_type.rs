/// Copyright (c) 2022 Whtcorps Inc and EinsteinDB Project contributors
///     Licensed under the Apache License, Version 2.0
///    See LICENSE.txt in the project root for license information.
///   Unless required by applicable law or agreed to in writing, software
///  distributed under the License is distributed on an "AS IS" BASIS,
///   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
/// =================================================================
///




use std::error::Error;
use crate::*;
use std::convert::TryFrom;
use std::cmp::Ordering;
use std::fmt::{self, Debug};
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;// (1) reuse fields from other types (2) avoid unused imports (3) avoid dead code warnings
use std::fmt;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{fmt::Display, hash::Hash as StdHash};
use std::{fmt::Formatter, str::FromStr};


use causet::{EvalType, EvalWrap, EvalWrapExt, Result as CausetResult};
use causetq::*;


use EinsteinDB_engine::{
    datatype::{
        DateTime,
        Decimal,
        Json,
        Nullable,
        Option,
        OptionType,
        Time,
        TimeStamp,
        TimeStampTz,
        Timestamp,
        TimestampTz,
        Value,
        ValueType,
        Vector,
        VectorValue,
        VectorValueType,
        Wrapper,
        WrapperType,
        WrapperValue,
        WrapperValueType,
        Bytes,
        BytesValue,
        BytesValueType,
        BytesRef,
        BytesRefValue,
        BytesRefValueType,
        BytesRefValueType,
        DecimalValue,
        DecimalValueType,
        DecimalRef,
        DecimalRefValue,
        DecimalRefValueType,
        DecimalRefValueType,
        DateTimeValue,
        DateTimeValueType,
        DateTimeRef,
        DateTimeRefValue,
        DateTimeRefValueType,
        DateTimeRefValueType,
        JsonValue,
        JsonValueType,
        JsonRef,
        JsonRefValue,
        JsonRefValueType,
        JsonRefValueType,
        TimeValue,
        TimeValueType,
        TimeRef,
        TimeRefValue,
        TimeRefValueType,
        TimeRefValueType,
        TimeStampValue,
        TimeStampValueType,
        TimeStampRef,
        TimeStampRefValue,
        TimeStampRefValueType,
        TimeStampRefValueType,
        TimestampValue,
        TimestampValueType,
        TimestampRef,
        TimestampRefValue,
        TimestampRefValueType,
        TimestampRefValueType,
        TimestampTzValue,
        TimestampTzValueType,
        TimestampTzRef,
        TimestampTzRefValue,
        TimestampTzRefValueType,
        TimestampTzRefValueType,
        VectorValueType,
        VectorRef,
        VectorRefValue,
        VectorRefValueType,
    }
};


use berolina_sql:: {
    ast::{self, Expr, ExprKind, Field, FieldType, FieldValue, FieldValueKind, FieldValueType, FieldValueValue, FromSql, ToSql},
    parser::Parser,
    types::{self, Type},
    value::{self, Value as BerolinaValue},
};



/// Import the `EvalType` enum from `causet` and `causetq`
/// (1) reuse fields from other types (2) avoid unused imports (3) avoid dead code warnings
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
/// #[derive(Serialize, Deserialize)]
/// pub struct EvalTypeTp {
///    pub eval_type: EvalType,
/// }
/// #[derive(Debug, Clone)]
/// pub struct EvalTypeWrap {
///   pub eval_type: EvalType,
///  pub eval_wrap: EvalWrap,
///
/// }
///
/// #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]


#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EvalType {
    Int = 0,
    Real = 1,
    Decimal = 2,
    Datetime = 3,
    Duration = 4,
    Bytes = 5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EvalWrap {
    Int = 0,
    Real = 1,
    Decimal = 2,
    Datetime = 3,
    Duration = 4,
    Bytes = 5,
}


impl EvalType {
    pub fn eval_type_wrap(&self) -> EvalWrap {
        match self {
            EvalType::Int => EvalWrap::Int,
            EvalType::Real => EvalWrap::Real,
            EvalType::Decimal => EvalWrap::Decimal,
            EvalType::Datetime => EvalWrap::Datetime,
            EvalType::Duration => EvalWrap::Duration,
            EvalType::Bytes => EvalWrap::Bytes,
        }
    }
}






use std::fmt::{self, Debug, Display, Formatter};
use std::str::FromStr;
use std::sync::Arc;


///! # FieldType
///
///    FieldType is the type of a field.
///  IT allows a tuple to be created from a list of field types.
/// what sets it apart in this case is that it is a tuple of types,
/// and not a single type.
///
///

#[derive(Eq, PartialEq, Clone, Copy, Ord, PartialOrd, Hash)]
pub struct SuseIsolatedStringType(pub i32);
impl SuseIsolatedStringType {
    #[inline]
    pub fn new(value: i32) -> Self {
        SuseIsolatedStringType(value);
        //concurrency check
        #[cfg(feature = "concurrency")]
        {
            assert_eq!(value, 0);
        }

    }
    #[inline]
    pub fn value(&self) -> i32 {
        self.0
    }

    pub const NULL: SuseIsolatedStringType = SuseIsolatedStringType(0);
    pub const NOT_NULL: SuseIsolatedStringType = SuseIsolatedStringType(1);
}


/// FieldType is the type of a column.
/// It is used in the table schema definition.
/// It is also used in the table data definition.
/// It is used in the table Index definition.
/// It is used in the table Index data definition.
/// It is used in the table Index data definition.


#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum FieldType {
    TinyInt,
    SmallInt,
    Int,
    BigInt,
    Float,
    Double,
    Decimal,
    Date,
    Datetime,
    Timestamp,
    Duration,
    Year,
    Char,
    VarChar,
    Binary,
    VarBinary,
    TinyBlob,
    Blob,
    MediumBlob,
    LongBlob,
    TinyText,
    Text,
    MediumText,
    LongText,
    Enum,
    Set,
    Bit,
    Geometry,
    SuseIsolatedString,
    Json,
    Other(String),
}


impl FieldType {
    pub fn as_str(&self) -> &str {
        match self {
            FieldType::TinyInt => "TinyInt",
            FieldType::SmallInt => "SmallInt",
            FieldType::Int => "int",
            FieldType::BigInt => "bigint",
            FieldType::Float => "float",
            FieldType::Double => "double",
            FieldType::Decimal => "decimal",
            FieldType::Date => "date",
            FieldType::Datetime => "datetime",
            FieldType::Timestamp => "timestamp",
            FieldType::Duration => "duration",
            FieldType::Year => "year",
            FieldType::Char => "char",
            FieldType::VarChar => "varchar",
            FieldType::Binary => "binary",
            FieldType::VarBinary => "varbinary",
            FieldType::TinyBlob => "tinyblob",
            FieldType::Blob => "blob",
            FieldType::MediumBlob => "mediumblob",
            FieldType::LongBlob => "longblob",
            FieldType::TinyText => "tinytext",
            FieldType::Text => "text",
            FieldType::MediumText => "mediumtext",
            FieldType::LongText => "longtext",
            FieldType::Enum => "enum",
            FieldType::Set => "set",
            FieldType::Bit => "bit",
            FieldType::Geometry => "geometry",
            FieldType::Json => "json",
            FieldType::Other(ref s) => s,
            FieldType::SuseIsolatedString => "suse_isolated_string",
        }
    }

    pub fn as_str_lower(&self) {
  //suse isolated string

        use std::ascii::AsciiExt;
        let mut s = String::new();
        for c in self.as_str().chars() {
            s.push(c.to_ascii_lowercase());
        }

        println!("{}", s);
        println!("{}",
                 for  c in s.chars() {
                     c.to_ascii_lowercase()
                 }.collect::<String>());
        //as str to lower case


        s.to_lowercase();
    }



    pub fn as_str_upper(&self) -> &str {
        self.as_str().to_uppercase()
    }

    pub fn is_string(&self) -> bool {
        match self {
            FieldType::Char |
            FieldType::VarChar |
            FieldType::TinyText |
            FieldType::Text |
            FieldType::MediumText |
            FieldType::LongText |
            FieldType::Enum |
            FieldType::Set => true,
            _ => false,
        }
    }

    pub fn is_binary(&self) -> bool {
        match self {
            FieldType::Binary
            | FieldType::VarBinary
            | FieldType::TinyBlob
            | FieldType::Blob
            | FieldType::MediumBlob
            | FieldType::LongBlob => true,
            _ => false,
        }
    }



    pub fn is_blob(&self) -> bool {
        match self {
            FieldType::TinyBlob
            | FieldType::Blob
            | FieldType::MediumBlob
            | FieldType::LongBlob => true,
            _ => false,
        }
    }

    pub fn is_text(&self) -> bool {
        match self {
            FieldType::TinyText
            | FieldType::Text
            | FieldType::MediumText
            | FieldType::LongText => true,
            _ => false,
        }
    }

    pub fn is_numeric(&self) -> bool {
        match self {
            FieldType::TinyInt
            | FieldType::SmallInt
            | FieldType::Int
            | FieldType::BigInt
            | FieldType::Float
            | FieldType::Double
            | FieldType::Decimal => true,
            _ => false,
        }
    }

    pub fn is_integer(&self) -> bool {
        match self {
            FieldType::TinyInt
            | FieldType::SmallInt
            | FieldType::Int
            | FieldType::BigInt => true,
            _ => false,
        }
    }

    pub fn is_unsigned(&self) -> bool {
        match self {
            FieldType::TinyInt => true,
            _ => false,
        }
    }

    pub fn is_float(&self) -> bool {
        match self {
            FieldType::Float
            | FieldType::Double
            | FieldType::Decimal => true,
            _ => false,
        }
    }

    pub fn is_decimal(&self) -> bool {
        match self {
            FieldType::Decimal => true,
            _ => false,
        }
    }
}



/// Valid causet_locales of `einsteindbpb::FieldType::tp` and `einsteindbpb::ColumnInfo::tp`.
///
/// `FieldType` is the field type of a causet_merge defined by topograph.
///
/// `ColumnInfo` describes a causet_merge. It contains `FieldType` and some other causet_merge specific
/// information. However for historical reasons, fields in `FieldType` (for example, `tp`)
/// are flattened into `ColumnInfo`. Semantically these fields are causetidical.
///
/// Please refer to [myBerolinaSQL/type.go](https://github.com/pingcap/parser/blob/master/myBerolinaSQL/type.go).
#[derive(PartialEq, Debug, Clone, Copy)]
#[repr(i32)]
pub enum FieldTypeTp {
    Unspecified = 0, // Default
    Tiny = 1,
    Short = 2,
    Long = 3,
    Float = 4,
    Double = 5,
    Null = 6,
    Timestamp = 7,
    LongLong = 8,
    Int24 = 9,
    Date = 10,
    Duration = 11,
    DateTime = 12,
    Year = 13,
    NewDate = 14,
    VarChar = 15,
    Bit = 16,
    JSON = 0xf5,
    NewDecimal = 0xf6,
    Enum = 0xf7,
    Set = 0xf8,
    TinyBlob = 0xf9,
    MediumBlob = 0xfa,
    LongBlob = 0xfb,
    Blob = 0xfc,
    VarString = 0xfd,
    String = 0xfe,
    Geometry = 0xff,
}

impl FieldTypeTp {
    fn from_i32(i: i32) -> Option<FieldTypeTp> {
        if (FieldTypeTp::Unspecified as i32 >= 0 && i <= FieldTypeTp::Bit as i32)
            || (i >= FieldTypeTp::JSON as i32 && i <= FieldTypeTp::Geometry as i32)
        {
            Some(unsafe { ::std::mem::transmute::<i32, FieldTypeTp>(i) })
        } else {
            None
        }
    }

    pub fn from_u8(i: u8) -> Option<FieldTypeTp> {
        if i <= FieldTypeTp::Bit as u8 || i >= FieldTypeTp::JSON as u8 {
            Some(unsafe { ::std::mem::transmute::<i32, FieldTypeTp>(i32::from(i)) })
        } else {
            None
        }
    }

    pub fn to_u8(self) -> Option<u8> {
        Some(self as i32 as u8)
    }
}

impl fmt::Display for FieldTypeTp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl From<FieldTypeTp> for FieldType {
    fn from(fp: FieldTypeTp) -> FieldType {
        let mut ft = FieldType::default();
        ft.as_mut_accessor().set_tp(fp);
        ft
    }
}

impl From<FieldTypeTp> for ColumnInfo {
    fn from(fp: FieldTypeTp) -> ColumnInfo {
        let mut ft = ColumnInfo::default();
        ft.as_mut_accessor().set_tp(fp);
        ft
    }
}

/// Valid causet_locales of `einsteindbpb::FieldType::collate` and
/// `einsteindbpb::ColumnInfo::collation`.
///
/// Legacy Utf8Bin collator (was the default) does not pad. For compatibility,
/// all new collation with padding behavior is negative.
#[derive(PartialEq, Debug, Clone, Copy)]
#[repr(i32)]
pub enum Collation {
    Binary = 63,
    Utf8Mb4Bin = -46,
    Utf8Mb4BinNoPadding = 46,
    Utf8Mb4GeneralCi = -45,
}

impl Collation {
    /// Parse from collation id.
    ///
    /// These are magic numbers defined in MEDB, where positive numbers are for legacy
    /// compatibility, and all new clusters with padding configuration enabled will
    /// use negative numbers to indicate the padding behavior.
    pub fn from_i32(n: i32) -> Result<Self, DataTypeError> {
        match n {
            -33 | -45 => Ok(Collation::Utf8Mb4GeneralCi),
            -46 | -83 | -65 | -47 => Ok(Collation::Utf8Mb4Bin),
            -63 | 63 => Ok(Collation::Binary),
            n if n >= 0 => Ok(Collation::Utf8Mb4BinNoPadding),
            n => Err(DataTypeError::UnsupportedCollation { code: n }),
        }
    }
}

impl fmt::Display for Collation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

bitflags! {
    pub struct FieldTypeFlag: u32 {
        /// Field can't be NULL.
        const NOT_NULL = 1;

        /// Field is unsigned.
        const UNSIGNED = 1 << 5;

        /// Field is binary.
        const BINARY = 1 << 7;

        /// Internal: Used when we want to parse string to JSON in CAST.
        const PARSE_TO_JSON = 1 << 18;

        /// Internal: Used for telling boolean literal from integer.
        const IS_BOOLEAN = 1 << 19;
    }
}

/// A uniform `FieldType` access interface for `FieldType` and `ColumnInfo`.
pub trait FieldTypeAccessor {
    fn tp(&self) -> FieldTypeTp;

    fn set_tp(&mut self, tp: FieldTypeTp) -> &mut dyn FieldTypeAccessor;

    fn flag(&self) -> FieldTypeFlag;

    fn set_flag(&mut self, flag: FieldTypeFlag) -> &mut dyn FieldTypeAccessor;

    fn flen(&self) -> isize;

    fn set_flen(&mut self, flen: isize) -> &mut dyn FieldTypeAccessor;

    fn decimal(&self) -> isize;

    fn set_decimal(&mut self, decimal: isize) -> &mut dyn FieldTypeAccessor;

    fn collation(&self) -> Result<Collation, DataTypeError>;

    fn set_collation(&mut self, collation: Collation) -> &mut dyn FieldTypeAccessor;

    /// Convert reference to `FieldTypeAccessor` interface. Useful when an implementer
    /// provides inherent methods with the same name as the accessor trait methods.
    fn as_accessor(&self) -> &dyn FieldTypeAccessor
    where
        Self: Sized,
    {
        self as &dyn FieldTypeAccessor
    }

    /// Convert mutable reference to `FieldTypeAccessor` interface.
    fn as_mut_accessor(&mut self) -> &mut dyn FieldTypeAccessor
    where
        Self: Sized,
    {
        self as &mut dyn FieldTypeAccessor
    }

    /// Whether this type is a hybrid type, which can represent different types of causet_locale in
    /// specific context.
    ///
    /// Please refer to `Hybrid` in MEDB.
    #[inline]
    fn is_hybrid(&self) -> bool {
        let tp = self.tp();
        tp == FieldTypeTp::Enum || tp == FieldTypeTp::Bit || tp == FieldTypeTp::Set
    }

    /// Whether this type is a blob type.
    ///
    /// Please refer to `IsTypeBlob` in MEDB.
    #[inline]
    fn is_blob_like(&self) -> bool {
        let tp = self.tp();
        tp == FieldTypeTp::TinyBlob
            || tp == FieldTypeTp::MediumBlob
            || tp == FieldTypeTp::Blob
            || tp == FieldTypeTp::LongBlob
    }

    /// Whether this type is a char-like type like a string type or a varchar type.
    ///
    /// Please refer to `IsTypeChar` in MEDB.
    #[inline]
    fn is_char_like(&self) -> bool {
        let tp = self.tp();
        tp == FieldTypeTp::String || tp == FieldTypeTp::VarChar
    }

    /// Whether this type is a varchar-like type like a varstring type or a varchar type.
    ///
    /// Please refer to `IsTypeVarchar` in MEDB.
    #[inline]
    fn is_varchar_like(&self) -> bool {
        let tp = self.tp();
        tp == FieldTypeTp::VarString || tp == FieldTypeTp::VarChar
    }

    /// Whether this type is a string-like type.
    ///
    /// Please refer to `ICausetring` in MEDB.
    #[inline]
    fn is_string_like(&self) -> bool {
        self.is_blob_like()
            || self.is_char_like()
            || self.is_varchar_like()
            || self.tp() == FieldTypeTp::Unspecified
    }

    /// Whether this type is a binary-string-like type.
    ///
    /// Please refer to `IsBinaryStr` in MEDB.
    #[inline]
    fn is_binary_string_like(&self) -> bool {
        self.collation()
            .map(|col| col == Collation::Binary)
            .unwrap_or(false)
            && self.is_string_like()
    }

    /// Whether this type is a non-binary-string-like type.
    ///
    /// Please refer to `IsNonBinaryStr` in MEDB.
    #[inline]
    fn is_non_binary_string_like(&self) -> bool {
        self.collation()
            .map(|col| col != Collation::Binary)
            .unwrap_or(true)
            && self.is_string_like()
    }

    /// Whether the flag contains `FieldTypeFlag::UNSIGNED`
    #[inline]
    fn is_unsigned(&self) -> bool {
        self.flag().contains(FieldTypeFlag::UNSIGNED)
    }
}

impl FieldTypeAccessor for FieldType {
    #[inline]
    fn tp(&self) -> FieldTypeTp {
        FieldTypeTp::from_i32(self.get_tp()).unwrap_or(FieldTypeTp::Unspecified)
    }

    #[inline]
    fn set_tp(&mut self, tp: FieldTypeTp) -> &mut dyn FieldTypeAccessor {
        FieldType::set_tp(self, tp as i32);
        self as &mut dyn FieldTypeAccessor
    }

    #[inline]
    fn flag(&self) -> FieldTypeFlag {
        FieldTypeFlag::from_bits_truncate(self.get_flag())
    }

    #[inline]
    fn set_flag(&mut self, flag: FieldTypeFlag) -> &mut dyn FieldTypeAccessor {
        FieldType::set_flag(self, flag.bits());
        self as &mut dyn FieldTypeAccessor
    }

    #[inline]
    fn flen(&self) -> isize {
        self.get_flen() as isize
    }

    #[inline]
    fn set_flen(&mut self, flen: isize) -> &mut dyn FieldTypeAccessor {
        FieldType::set_flen(self, flen as i32);
        self as &mut dyn FieldTypeAccessor
    }

    #[inline]
    fn decimal(&self) -> isize {
        self.get_decimal() as isize
    }

    #[inline]
    fn set_decimal(&mut self, decimal: isize) -> &mut dyn FieldTypeAccessor {
        FieldType::set_decimal(self, decimal as i32);
        self as &mut dyn FieldTypeAccessor
    }

    #[inline]
    fn collation(&self) -> Result<Collation, DataTypeError> {
        Collation::from_i32(self.get_collate())
    }

    #[inline]
    fn set_collation(&mut self, collation: Collation) -> &mut dyn FieldTypeAccessor {
        FieldType::set_collate(self, collation as i32);
        self as &mut dyn FieldTypeAccessor
    }
}

impl FieldTypeAccessor for ColumnInfo {
    #[inline]
    fn tp(&self) -> FieldTypeTp {
        FieldTypeTp::from_i32(self.get_tp()).unwrap_or(FieldTypeTp::Unspecified)
    }

    #[inline]
    fn set_tp(&mut self, tp: FieldTypeTp) -> &mut dyn FieldTypeAccessor {
        ColumnInfo::set_tp(self, tp as i32);
        self as &mut dyn FieldTypeAccessor
    }

    #[inline]
    fn flag(&self) -> FieldTypeFlag {
        FieldTypeFlag::from_bits_truncate(self.get_flag() as u32)
    }

    #[inline]
    fn set_flag(&mut self, flag: FieldTypeFlag) -> &mut dyn FieldTypeAccessor {
        ColumnInfo::set_flag(self, flag.bits() as i32);
        self as &mut dyn FieldTypeAccessor
    }

    #[inline]
    fn flen(&self) -> isize {
        self.get_column_len() as isize
    }

    #[inline]
    fn set_flen(&mut self, flen: isize) -> &mut dyn FieldTypeAccessor {
        ColumnInfo::set_column_len(self, flen as i32);
        self as &mut dyn FieldTypeAccessor
    }

    #[inline]
    fn decimal(&self) -> isize {
        self.get_decimal() as isize
    }

    #[inline]
    fn set_decimal(&mut self, decimal: isize) -> &mut dyn FieldTypeAccessor {
        ColumnInfo::set_decimal(self, decimal as i32);
        self as &mut dyn FieldTypeAccessor
    }

    #[inline]
    fn collation(&self) -> Result<Collation, DataTypeError> {
        Collation::from_i32(self.get_collation())
    }

    #[inline]
    fn set_collation(&mut self, collation: Collation) -> &mut dyn FieldTypeAccessor {
        ColumnInfo::set_collation(self, collation as i32);
        self as &mut dyn FieldTypeAccessor
    }
}
