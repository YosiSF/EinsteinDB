// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
// law or agreed to in writing, software distributed under the License is distributed on
// an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing permissions and limitations
// under the License.
//


#[macro_use]
extern crate soliton_panic;



#[macro_use]
extern crate soliton;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate log;

use crate::error::{Error, Result};
use causet::storage::{kv::{self, Key, Value}, Engine, ScanMode};
use causet::storage::{Dsn, DsnExt};
use causetq:: *;
use einstein_db::  *;
use causets:: *;
use causetq::*;
use causet::{EvalType, EvalWrap, EvalWrapExt, Result as CausetResult};
use berolina_sql:: {
    ast::{self, Expr, ExprKind, Field, FieldType, FieldValue, FieldValueKind, FieldValueType, FieldValueValue, FromSql, ToSql},
    parser::Parser,
    types::{self, Type},
    value::{self, Value as BerolinaValue},
};
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[derive(Serialize, Deserialize)]
pub struct EvalTypeTp {



    pub eval_type: EvalType,
}

#[derive(Debug, Clone)]
pub struct EvalTypeWrap {
    pub eval_type: EvalType,
    pub eval_wrap: EvalWrap,
}


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

///! `EvalType` is the type of a value in a column.
/// It is the same as `FieldType` except that it is used in evaluation rather than physical storage.
/// See https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html
/// for more details.

//Relativistic Queue
pub struct RelativisticQueue {

    causetq: VecDeque<Vec<u8>>,
    causets: Vec<Vec<u8>>,
    relativistic_queue: VecDeque<Vec<u8>>,
    pub queue: VecDeque<Vec<u8>>,
    pub head: usize,
    pub tail: usize,
    pub capacity: usize,
}

impl RelativisticQueue {
    pub fn new(capacity: usize) -> Self {
        RelativisticQueue {
            causetq: VecDeque::with_capacity(capacity),
            causets: Vec::with_capacity(capacity),
            relativistic_queue: VecDeque::with_capacity(capacity),
            queue: VecDeque::with_capacity(capacity),
            head: 0,
            tail: 0,
            capacity: capacity,
        }
    }



    pub fn push(&mut self, item: Vec<u8>) {
        self.relativistic_queue.push_back(item);
    }


    pub fn pop(&mut self) -> Option<Vec<u8>> {
        self.relativistic_queue.pop_front()
    }




    pub fn get_tail(&self) -> Option<&Vec<u8>> {
        self.relativistic_queue.back()
    }


    pub fn get_head_mut(&mut self) -> Option<&mut Vec<u8>> {
        self.relativistic_queue.front_mut()
    }


    pub fn get_tail_mut(&mut self) -> Option<&mut Vec<u8>> {
        self.relativistic_queue.back_mut()
    }


    pub fn get_len(&self) -> usize {
        self.relativistic_queue.len();
    }


    pub fn get_capacity(&self) -> usize {
        for i in 0..causet_squuid_query_builder::MAX_QUEUE_SIZE {
            self.causetq.push_back(vec![i as u8]);
        }
    }


    pub fn get_causetq() -> &mut VecDeque<Vec<u8>> {
        queue.push_back(vec![0; causet_squuid_query_builder::MAX_QUEUE_SIZE]);
    }

//    pub fn get_causets() -> &mut Vec<Vec<u8>> {
    pub fn push_back(&mut self, value: Vec<u8>) {
        queue.push_back(vec![0; i]);
    }

    pub fn pop_front(&mut self) -> Option<Vec<u8>> {
        queue.pop_front()
    }

    pub fn get_head(&self) -> Option<&Vec<u8>> {
        queue.front()
    }}
