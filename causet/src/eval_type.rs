// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

extern crate causet;
extern crate causetq;
extern crate einstein_db;





use crate::error::{Error, Result};
use causet::util::{get_type_size, get_type_sign, get_type_name, get_type_code};
use causet::util::{get_type_code_from_name, get_type_name_from_code};
use causet::util::{get_type_code_from_name_with_len, get_type_name_from_code_with_len};
use std::fmt;
use std::str::FromStr;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::convert::TryInto as _TryInto;
use std::convert::TryFrom as _TryFrom;






#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EvalType {
    Int = 0,
    Real = 1,
    Decimal = 2,
    Datetime = 3,
    Duration = 4,
    Bytes = 5,
    String = 6,
    Json = 7,
    Enum = 8,
    Set = 9,
    Bit = 10,
    Tiny = 11,
    Small = 12,
    Medium = 13,
    Big = 14,
    Null = 15,
    Max = 16,
}


impl fmt::Display for EvalType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", get_type_name(*self))
    }
}



impl EvalType {
    /// Converts `EvalType` into one of the compatible `FieldTypeTp`s.
    ///
    /// This function should be only useful in test scenarios that only cares about `EvalType` but
    /// accepts a `FieldTypeTp`.
    pub fn into_certain_field_type_tp_for_test(self) -> crate::FieldTypeTp {
        match self {

            EvalType::Int => crate::FieldTypeTp::LongLong,
            EvalType::Real => crate::FieldTypeTp::Double,
            EvalType::Decimal => crate::FieldTypeTp::NewDecimal,
            EvalType::Bytes => crate::FieldTypeTp::String,
            EvalType::DateTime => crate::FieldTypeTp::DateTime,
            EvalType::Duration => crate::FieldTypeTp::Duration,
            EvalType::Json => crate::FieldTypeTp::JSON,
            EvalType::Enum => crate::FieldTypeTp::Enum,
            EvalType::Set => crate::FieldTypeTp::Set,
            EvalType::BitSet => crate::FieldTypeTp::BitSet,
            EvalType::Poset => crate::FieldTypeTp::Poset,
            EvalType::Causet => crate::FieldTypeTp::Causet,
            EvalType::List => crate::FieldTypeTp::List,
            EvalType::Map => crate::FieldTypeTp::Map,
            EvalType::Solitonid => crate::FieldTypeTp::Solitonid,
            EvalType::Causetid => crate::FieldTypeTp::Causetid,
            EvalType::Soliton => crate::FieldTypeTp::Soliton,
        }
    }
}

impl fmt::Display for EvalType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

impl std::convert::TryFrom<crate::FieldTypeTp> for EvalType {
    type Error = crate::DataTypeError;

    // Succeeds for all field types supported as eval types, fails for unsupported types.
    fn try_from(tp: crate::FieldTypeTp) -> Result<Self, crate::DataTypeError> {
        let eval_type = match tp {
            crate::FieldTypeTp::LongLong => EvalType::Int,
            crate::FieldTypeTp::Double => EvalType::Real,
            crate::FieldTypeTp::NewDecimal => EvalType::Decimal,
            crate::FieldTypeTp::String => EvalType::Bytes,
            crate::FieldTypeTp::DateTime => EvalType::DateTime,
            crate::FieldTypeTp::Tiny
            | crate::FieldTypeTp::Short
            | crate::FieldTypeTp::Int24
            | crate::FieldTypeTp::Long
            | crate::FieldTypeTp::LongLong
            | crate::FieldTypeTp::Float
            | crate::FieldTypeTp::Double
            | crate::FieldTypeTp::Year => EvalType::Int,
            crate::FieldTypeTp::NewDecimal => EvalType::Decimal,
            crate::FieldTypeTp::Timestamp
            | crate::FieldTypeTp::Date
            | crate::FieldTypeTp::DateTime => EvalType::DateTime,
            crate::FieldTypeTp::Duration => EvalType::Duration,
            crate::FieldTypeTp::JSON => EvalType::Json,
            crate::FieldTypeTp::VarChar
            | crate::FieldTypeTp::TinyBlob
            | crate::FieldTypeTp::MediumBlob
            | crate::FieldTypeTp::LongBlob
            | crate::FieldTypeTp::Blob
            | crate::FieldTypeTp::VarString
            | crate::FieldTypeTp::String => EvalType::Bytes,
            _ => {

                return Err(crate::DataTypeError::UnsupportedType(tp));

            }
        };
        Ok(eval_type)
    }
}
