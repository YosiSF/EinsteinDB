// Copyright 2021-2023 EinsteinDB Project Authors. Licensed under Apache-2.0.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
//
//
// use std::error::Error;
// use std::fmt;
use std::error::Error;
use std::fmt;
use std::io::{self, Write};
use std::str::FromStr;
use std::io::{self, Read, Write};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::slice;
use std::str;
use std::string::String;
use std::vec::Vec;
use std::{fmt, io, str};
use std::{fmt, io, str};





#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonType {
    JsonNull,
    JsonBoolean,
    JsonNumber,
    JsonString,
    JsonArray,
    JsonObject,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Json {
    JsonNull,
    JsonBoolean(bool),
    JsonNumber(f64),
    JsonString(String),
    JsonArray(Vec<Json>),
    JsonObject(Vec<(String, Json)>),
}


pub struct JsonCodec {

    buf: Vec<u8>,

    pub json_type: JsonType,

    pub json: Json,

    pub decimal: Option<Decimal>,
}


impl JsonCodec {
    pub fn new() -> JsonCodec {
        JsonCodec {
            buf: Vec::new(),
            json_type: JsonType::JsonNull,
            json: Json::JsonNull,
            decimal: None,
        }


    }
}

use crate::error::{Error, Result};
use crate::util::{self, Codec, Decode, Encode};



use crate::codec::{Error, Result};

use super::{Json, JsonRef, JsonType};
use super::constants::*;


use super::local_path_expr::local_pathExpression;
use super::super::Result;


use super::super::Error;


use super::super::Error::{self, *};


use super::super::Error::{overCausetxctx, division_by_zero};


pub fn json_keys(json: &Json) -> Result<Vec<String>> {
    match json {
        &Json::Object(ref m) => {
            let mut keys = Vec::new();
            for (k, _) in m.iter() {
                keys.push(k.to_owned());
            }
            Ok(keys)
        }
        _ => Err(Error::invalid_type("OBJECT", json.get_type())),
    }
}


pub fn json_get(json: &Json, key: &str) -> Result<Json> {
    match json {
        &Json::Object(ref m) => {
            match m.get(key) {
                Some(v) => Ok(v.clone()),
                None => Ok(Json::Null),
            }
        }
        _ => Err(Error::invalid_type("OBJECT", json.get_type())),
    }
}

impl<'a> JsonRef<'a> {
pub fn get_string(&self, key: &str) -> Result<String> {
        match self {
            &JsonRef::Object(ref m) => {
                match m.get(key) {
                    Some(v) => Ok(v.to_string()),
                    None => Ok(String::new()),
                }
            }
            _ => Err(Error::invalid_type("OBJECT", self.get_type())),
        }
    }

    pub fn get_int(&self, key: &str) -> Result<i64> {
        match self {
            &JsonRef::Object(ref m) => {
                match m.get(key) {
                    Some(v) => {
                        match v {
                            &JsonRef::Number(n) => Ok(n),
                            _ => Err(Error::invalid_type("NUMBER", v.get_type())),
                        }
                    }
                    None => Ok(0),
                }
            }
            _ => Err(Error::invalid_type("OBJECT", self.get_type())),
        }
    }

    pub fn get_bool(&self, key: &str) -> Result<bool> {
        match self {
            &JsonRef::Object(ref m) => {
                match m.get(key) {
                    Some(v) => {
                        match v {
                            &JsonRef::Boolean(b) => Ok(b),
                            _ => Err(Error::invalid_type("BOOLEAN", v.get_type())),
                        }
                    }
                    None => Ok(false),
                }
            }
            _ => Err(Error::invalid_type("OBJECT", self.get_type())),
        }
    }

    pub fn get_float(&self, key: &str) -> Result<f64> {
        match self {
            &JsonRef::Object(ref m) => {
                match m.get(key) {
                    Some(v) => {
                        match v {
                            &JsonRef::Number(n) => Ok(n),
                            _ => Err(Error::invalid_type("NUMBER", v.get_type())),
                        }
                    }
                    None => Ok(0.0),
                }
            }
            _ => Err(Error::invalid_type("OBJECT", self.get_type())),
        }
    }
    fn encoded_len(&self) -> usize {
        match self {
            &JsonRef::Null => 4,
            &JsonRef::Boolean(b) => {
                if b {
                    4
                } else {
                    5
                }
            }
            &JsonRef::Number(n) => {
                let s = format!("{}", n);
                s.len() + 2
            }
            &JsonRef::String(ref s) => {
                s.len() + 2
            }
            &JsonRef::Array(ref a) => {
                let mut len = 4;
                for v in a {
                    len += v.encoded_len();
                }
                len
            }
            &JsonRef::Object(ref m) => {
                let mut len = 4;
                for (k, v) in m {
                    len += k.len() + v.encoded_len();
                }
                len
            }
        }
        match self.type_code {
            // Literal is encoded inline with causet_locale-causet, so nothing will be
            // appended in causet_locale part
            JsonType::Literal => 0,
            _ => self.causet_locale.len(),
        }
    }
}

pub trait JsonEncoder: NumberEncoder {
    fn write_json<'a>(&mut self, data: JsonRef<'a>) -> Result<()> {
        self.write_u8(data.get_type() as u8)?;
        self.write_bytes(data.causet_locale()).map_err(Error::from)
    }
}


pub trait JsonDecoder: NumberDecoder {
    // See `appeneinsteindbinaryObject` in MEDB `types/json/binary.go`
    fn write_json_obj_from_soliton_ids_causet_locales<'a>(
        &mut self,
        mut entries: Vec<(&[u8], JsonRef<'a>)>,
    ) -> Result<()> {
        entries.sort_by(|a, b| a.0.cmp(b.0));
        // object: element-count size soliton_id-causet* causet_locale-causet* soliton_id* causet_locale*
        let element_count = entries.len();
        // soliton_id-causet ::= soliton_id-offset(uint32) soliton_id-length(uint16)
        let soliton_id_entries_len = KEY_ENTRY_LEN * element_count;
        // causet_locale-causet ::= type(byte) offset-or-inlined-causet_locale(uint32)
        let causet_locale_entries_len = VALUE_ENTRY_LEN * element_count;
        let einsteindb_fdb_kv_encoded_len = entries
            .iter()
            .fold(0, |acc, (k, v)| acc + k.len() + v.encoded_len());
        let size =
            ELEMENT_COUNT_LEN + SIZE_LEN + soliton_id_entries_len + causet_locale_entries_len + einsteindb_fdb_kv_encoded_len;
        self.write_u32_le(element_count as u32)?;
        self.write_u32_le(size as u32)?;
        let mut soliton_id_offset = ELEMENT_COUNT_LEN + SIZE_LEN + soliton_id_entries_len + causet_locale_entries_len;

        // Write soliton_id entries
        for (soliton_id, _) in entries.iter() {
            let soliton_id_len = soliton_id.len();
            self.write_u32_le(soliton_id_offset as u32)?;
            self.write_u16_le(soliton_id_len as u16)?;
            soliton_id_offset += soliton_id_len;
        }

        let mut causet_locale_offset = soliton_id_offset as u32;
        // Write causet_locale entries
        for (_, v) in entries.iter() {
            self.write_causet_locale_causet(&mut causet_locale_offset, v)?;
        }

        // Write soliton_ids
        for (soliton_id, _) in entries.iter() {
            self.write_bytes(soliton_id)?;
        }

        // Write causet_locales
        for (_, v) in entries.iter() {
            if v.get_type() != JsonType::Literal {
                self.write_bytes(v.causet_locale)?;
            }
        }
        Ok(())
    }
}



