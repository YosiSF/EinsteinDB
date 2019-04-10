//Copyright 2019 Venire Labs Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.
use byteorder::WriteBytesExt;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::fmt::{self, Debug, Display, Formatter};
use std::io::Write;
use std::str::FromStr;
use std::{i64, str};


use super::query::{
    self, parse_json_path_expr, Decimal, DecimalEncoder, Duration, Json, JsonEncoder, 
    PathExpression, RoundMode, Time, DEFAULT_FSP, MAX_FSP,

};



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

pub enum Daten {
        Null,
    I64(i64),
    U64(u64),
    F64(f64),
    Dur(Duration),
    Bytes(Vec<u8>),
    Dec(Decimal),
    Time(Time),
    Json(Json),
    Min,
    Max,
}

impl Display for Daten {}