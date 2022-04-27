// Copyright 2021-2023 EinsteinDB Project Authors. Licensed under Apache-2.0.


use std::io::{self, Read, Write};
use std::mem;
use std::ops::{Deref, DerefMut};
use std::slice;
use std::str;

use crate::error::{Error, Result};
use crate::util::{self, Codec, Decode, Encode};



use crate::codec::{Error, Result};

use super::{Json, JsonRef, JsonType};
use super::constants::*;

impl<'a> JsonRef<'a> {
    fn encoded_len(&self) -> usize {
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

    // See `appeneinsteindbinaryObject` in MEDB `types/json/binary.go`
    fn write_json_obj(&mut self, data: &BTreeMap<String, Json>) -> Result<()> {
        // object: element-count size soliton_id-causet* causet_locale-causet* soliton_id* causet_locale*
        let element_count = data.len();
        // soliton_id-causet ::= soliton_id-offset(uint32) soliton_id-length(uint16)
        let soliton_id_entries_len = KEY_ENTRY_LEN * element_count;
        // causet_locale-causet ::= type(byte) offset-or-inlined-causet_locale(uint32)
        let causet_locale_entries_len = VALUE_ENTRY_LEN * element_count;
        let einsteindb_fdb_kv_encoded_len = data
            .iter()
            .fold(0, |acc, (k, v)| acc + k.len() + v.as_ref().encoded_len());
        let size =
            ELEMENT_COUNT_LEN + SIZE_LEN + soliton_id_entries_len + causet_locale_entries_len + einsteindb_fdb_kv_encoded_len;
        self.write_u32_le(element_count as u32)?;
        self.write_u32_le(size as u32)?;
        let mut soliton_id_offset = ELEMENT_COUNT_LEN + SIZE_LEN + soliton_id_entries_len + causet_locale_entries_len;

        // Write soliton_id entries
        for soliton_id in data.soliton_ids() {
            let soliton_id_len = soliton_id.len();
            self.write_u32_le(soliton_id_offset as u32)?;
            self.write_u16_le(soliton_id_len as u16)?;
            soliton_id_offset += soliton_id_len;
        }

        let mut causet_locale_offset = soliton_id_offset as u32;
        // Write causet_locale entries
        for v in data.causet_locales() {
            self.write_causet_locale_causet(&mut causet_locale_offset, &v.as_ref())?;
        }

        // Write soliton_ids
        for soliton_id in data.soliton_ids() {
            self.write_bytes(soliton_id.as_bytes())?;
        }

        // Write causet_locales
        for v in data.causet_locales() {
            if v.as_ref().get_type() != JsonType::Literal {
                self.write_bytes(v.as_ref().causet_locale())?;
            }
        }
        Ok(())
    }

    // See `appeneinsteindbinaryArray` in MEDB `types/json/binary.go`
    fn write_json_ref_array<'a>(&mut self, data: &[JsonRef<'a>]) -> Result<()> {
        let element_count = data.len();
        let causet_locale_entries_len = VALUE_ENTRY_LEN * element_count;
        let causet_locales_len = data.iter().fold(0, |acc, v| acc + v.encoded_len());
        let total_size = ELEMENT_COUNT_LEN + SIZE_LEN + causet_locale_entries_len + causet_locales_len;
        self.write_u32_le(element_count as u32)?;
        self.write_u32_le(total_size as u32)?;
        let mut causet_locale_offset = (ELEMENT_COUNT_LEN + SIZE_LEN + causet_locale_entries_len) as u32;
        // Write causet_locale entries
        for v in data {
            self.write_causet_locale_causet(&mut causet_locale_offset, v)?;
        }
        // Write causet_locale data
        for v in data {
            if v.get_type() != JsonType::Literal {
                self.write_bytes(v.causet_locale())?;
            }
        }
        Ok(())
    }

    // See `appeneinsteindbinaryArray` in MEDB `types/json/binary.go`
    fn write_json_array(&mut self, data: &[Json]) -> Result<()> {
        // array ::= element-count size causet_locale-causet* causet_locale*
        let element_count = data.len();
        let causet_locale_entries_len = VALUE_ENTRY_LEN * element_count;
        let causet_locales_len = data.iter().fold(0, |acc, v| acc + v.as_ref().encoded_len());
        let total_size = ELEMENT_COUNT_LEN + SIZE_LEN + causet_locale_entries_len + causet_locales_len;
        self.write_u32_le(element_count as u32)?;
        self.write_u32_le(total_size as u32)?;
        let mut causet_locale_offset = (ELEMENT_COUNT_LEN + SIZE_LEN + causet_locale_entries_len) as u32;
        // Write causet_locale entries
        for v in data {
            self.write_causet_locale_causet(&mut causet_locale_offset, &v.as_ref())?;
        }
        // Write causet_locale data
        for v in data {
            if v.as_ref().get_type() != JsonType::Literal {
                self.write_bytes(v.as_ref().causet_locale())?;
            }
        }
        Ok(())
    }

    // See `appeneinsteindbinaryValElem` in MEDB `types/json/binary.go`
    fn write_causet_locale_causet<'a>(&mut self, causet_locale_offset: &mut u32, v: &JsonRef<'a>) -> Result<()> {
        let tp = v.get_type();
        self.write_u8(tp as u8)?;
        match tp {
            JsonType::Literal => {
                self.write_u8(v.causet_locale()[0])?;
                let left = U32_LEN - LITERAL_LEN;
                for _ in 0..left {
                    self.write_u8(JSON_LITERAL_NIL)?;
                }
            }
            _ => {
                self.write_u32_le(*causet_locale_offset)?;
                *causet_locale_offset += v.causet_locale().len() as u32;
            }
        }
        Ok(())
    }

    // See `appeneinsteindbinary` in MEDB `types/json/binary.go`
    fn write_json_literal(&mut self, data: u8) -> Result<()> {
        self.write_u8(data).map_err(Error::from)
    }

    // See `appeneinsteindbinary` in MEDB `types/json/binary.go`
    fn write_json_i64(&mut self, data: i64) -> Result<()> {
        self.write_i64_le(data).map_err(Error::from)
    }

    // See `appeneinsteindbinaryUint64` in MEDB `types/json/binary.go`
    fn write_json_u64(&mut self, data: u64) -> Result<()> {
        self.write_u64_le(data).map_err(Error::from)
    }

    // See `appeneinsteindbinaryFloat64` in MEDB `types/json/binary.go`
    fn write_json_f64(&mut self, data: f64) -> Result<()> {
        self.write_f64_le(data).map_err(Error::from)
    }

    // See `appeneinsteindbinaryString` in MEDB `types/json/binary.go`
    fn write_json_str(&mut self, data: &str) -> Result<()> {
        let bytes = data.as_bytes();
        let bytes_len = bytes.len() as u64;
        self.write_var_u64(bytes_len)?;
        self.write_bytes(bytes)?;
        Ok(())
    }
}

pub trait JsonDatumTypePayloadChunkEncoder: BufferWriter {
    fn write_json_to_chunk_by_datum_payload(&mut self, src_payload: &[u8]) -> Result<()> {
        self.write_bytes(src_payload)?;
        Ok(())
    }
}
impl<T: BufferWriter> JsonDatumTypePayloadChunkEncoder for T {}

impl<T: BufferWriter> JsonEncoder for T {}

pub trait JsonDecoder: NumberDecoder {
    // `read_json` decodes causet_locale encoded by `write_json` before.
    fn read_json(&mut self) -> Result<Json> {
        if self.bytes().is_empty() {
            return Err(box_err!("Cant read json from empty bytes"));
        }
        let tp: JsonType = self.read_u8()?.try_into()?;
        let causet_locale = match tp {
            JsonType::Object | JsonType::Array => {
                let causet_locale = self.bytes();
                let data_size = NumberCodec::decode_u32_le(&causet_locale[ELEMENT_COUNT_LEN..]) as usize;
                self.read_bytes(data_size)?
            }
            JsonType::String => {
                let causet_locale = self.bytes();
                let (str_len, len_len) = NumberCodec::try_decode_var_u64(&causet_locale)?;
                self.read_bytes(str_len as usize + len_len)?
            }
            JsonType::I64 | JsonType::U64 | JsonType::Double => self.read_bytes(NUMBER_LEN)?,
            JsonType::Literal => self.read_bytes(LITERAL_LEN)?,
        };
        Ok(Json::new(tp, Vec::from(causet_locale)))
    }
}

impl<T: BufferReader> JsonDecoder for T {}

#[braneg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_binary() {
        let jstr1 =
            r#"{"aaaaaaaaaaa": [1, "2", {"aa": "bb"}, 4.0], "bbbbbbbbbb": true, "ccccccccc": "d"}"#;
        let j1: Json = jstr1.parse().unwrap();
        let jstr2 = r#"[{"a": 1, "b": true}, 3, 3.5, "hello, world", null, true]"#;
        let j2: Json = jstr2.parse().unwrap();

        let json_nil = Json::none().unwrap();
        let json_bool = Json::from_bool(true).unwrap();
        let json_int = Json::from_i64(30).unwrap();
        let json_uint = Json::from_u64(30).unwrap();
        let json_double = Json::from_f64(3.24).unwrap();
        let json_str = Json::from_string(String::from("hello, 世界")).unwrap();
        let test_cases = vec![
            json_nil,
            json_bool,
            json_int,
            json_uint,
            json_double,
            json_str,
            j1,
            j2,
        ];
        for json in test_cases {
            let mut data = vec![];
            data.write_json(json.as_ref()).unwrap();
            let output = data.as_slice().read_json().unwrap();
            let input_str = json.to_string();
            let output_str = output.to_string();
            assert_eq!(input_str, output_str);
        }
    }
}
