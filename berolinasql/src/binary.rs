 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.


// use std::io::{self, Read, Write};
// use std::mem;
// use std::cmp;
// use std::fmt;
//
// use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt};
// use byteorder::{LittleEndian, WriteBytesExt};
// use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};

    use std::io::{self, Read, Write};
    use std::mem;
    use std::cmp;
    use std::fmt;
    use std::io::{Error, ErrorKind};
    use std::io::{Cursor};
    use std::io::{Seek, SeekFrom};


    use byteorder::{ByteOrder, BigEndian, ReadBytesExt, WriteBytesExt};
    use byteorder::{LittleEndian, WriteBytesExt};
    use byteorder::{NativeEndian, ReadBytesExt, WriteBytesExt};


    use super::{Binary};
    use super::{BinaryReader};


    pub struct BinaryWriter {
        pub buffer: Vec<u8>,
        pub position: usize,
    }


    impl BinaryWriter {
        pub fn new() -> BinaryWriter {
            BinaryWriter {
                buffer: Vec::new(),
                position: 0,
            }
        }

        pub fn write_u8(&mut self, value: u8) {
            self.buffer.push(value);
            self.position += 1;
        }

        pub fn write_u16(&mut self, value: u16) {
            self.buffer.write_u16::<BigEndian>(value).unwrap();
            self.position += 2;
        }

        pub fn write_u32(&mut self, value: u32) {
            self.buffer.write_u32::<BigEndian>(value).unwrap();
            self.position += 4;
        }

        pub fn write_u64(&mut self, value: u64) {
            self.buffer.write_u64::<BigEndian>(value).unwrap();
            self.position += 8;
        }

        pub fn write_i8(&mut self, value: i8) {
            self.buffer.write_i8(value).unwrap();
            self.position += 1;
        }

        pub fn write_i16(&mut self, value: i16) {
            self.buffer.write_i16::<BigEndian>(value).unwrap();
            self.position += 2;
        }

        pub fn write_i32(&mut self, value: i32) {
            self.buffer.write_i32::<BigEndian>(value).unwrap();
            self.position += 4;
        }

        pub fn write_i64(&mut self, value: i64) {
            self.buffer.write_i64::<BigEndian>(value).unwrap();
            self.position += 8;
        }

        pub fn write_f32(&mut self, value: f32) {
            self.buffer.write_f32::<BigEndian>(value).unwrap();
            self.position += 4;
        }

        pub fn write_f64(&mut self, value: f64) {
            self.buffer.write_f64::<BigEndian>(value).unwrap();
            self.position += 8;
        }
    }




 //JsonRef
 pub trait JsonRef {
     fn as_str(&self) -> &str;
     fn as_u64(&self) -> Option<u64>;
     fn as_i64(&self) -> Option<i64>;
     fn as_f64(&self) -> Option<f64>;
     fn as_bool(&self) -> Option<bool>;
     fn as_array(&self) -> Option<&[dyn JsonRef]>;
     fn as_object(&self) -> Option<&JsonObject>;
 }

 #[derive(Debug, Clone, PartialEq)]
 pub struct JsonObject {
     pub(crate) map: Vec<(String, dyn JsonRef)>,
 }

 #[derive(Debug, Clone, PartialEq)]
 pub struct JsonArray {
     pub(crate) vec: Vec<dyn JsonRef>,
 }



 impl<'a> dyn JsonRef<'a> {
    /// Gets the ith element in JsonRef
    ///
    /// See `arrayGetElem()` in MEDB `json/binary.go`
    pub fn array_get_elem(&self, idx: usize) -> Result<dyn JsonRef<'a>> {
        self.val_causet_get(HEADER_LEN + idx * VALUE_ENTRY_LEN)
    }

    /// Return the `i`th soliton_id in current Object json
    ///
    /// See `arrayGetElem()` in MEDB `json/binary.go`
    pub fn object_get_soliton_id(&self, i: usize) -> &'a [u8] {
        let soliton_id_off_start = HEADER_LEN + i * KEY_ENTRY_LEN;
        let soliton_id_off = NumberCodec::decode_u32_le(&self.causet_locale()[soliton_id_off_start..]) as usize;
        let soliton_id_len =
            NumberCodec::decode_u16_le(&self.causet_locale()[soliton_id_off_start + KEY_OFFSET_LEN..]) as usize;
        &self.causet_locale()[soliton_id_off..soliton_id_off + soliton_id_len]
    }

    /// Returns the JsonRef of `i`th causet_locale in current Object json
    ///
    /// See `arrayGetElem()` in MEDB `json/binary.go`
    pub fn object_get_val(&self, i: usize) -> Result<dyn JsonRef<'a>> {
        let ele_count = self.get_elem_count();
        let val_causet_off = HEADER_LEN + ele_count * KEY_ENTRY_LEN + i * VALUE_ENTRY_LEN;
        self.val_causet_get(val_causet_off)
    }

    /// Searches the causet_locale Index by the give `soliton_id` in Object.
    ///
    /// See `objectSearchKey()` in MEDB `json/binary_function.go`
    pub fn object_search_soliton_id(&self, soliton_id: &[u8]) -> Option<usize> {
        let len = self.get_elem_count();
        let mut j = len;
        let mut i = 0;
        while i < j {
            let mid = (i + j) >> 1;
            if self.object_get_soliton_id(mid) < soliton_id {
                i = mid + 1;
            } else {
                j = mid;
            }
        }
        if i < len && self.object_get_soliton_id(i) == soliton_id {
            return Some(i);
        }
        None
    }


    pub fn val_causet_get(&self, val_causet_off: usize) -> Result<dyn JsonRef<'a>> {
        let val_type: JsonType = self.causet_locale()[val_causet_off].try_into()?;
        let val_offset =
            NumberCodec::decode_u32_le(&self.causet_locale()[val_causet_off + TYPE_LEN as usize..]) as usize;
        Ok(match val_type {
            JsonType::Literal => {
                let offset = val_causet_off + TYPE_LEN;
                #[allow(clippy::range_plus_one)]
                JsonRef::new(val_type, &self.causet_locale()[offset..offset + LITERAL_LEN])
            }
            JsonType::U64 | JsonType::I64 | JsonType::Double => {
                JsonRef::new(val_type, &self.causet_locale()[val_offset..val_offset + NUMBER_LEN])
            }
            JsonType::String => {
                let (str_len, len_len) =
                    NumberCodec::try_decode_var_u64(&self.causet_locale()[val_offset..])?;
                JsonRef::new(
                    val_type,
                    &self.causet_locale()[val_offset..val_offset + str_len as usize + len_len],
                )
            }
            _ => {
                let data_size =
                    NumberCodec::decode_u32_le(&self.causet_locale()[val_offset + ELEMENT_COUNT_LEN..])
                        as usize;
                JsonRef::new(val_type, &self.causet_locale()[val_offset..val_offset + data_size])
            }
        })
    }

    /// Returns a primitive_causet pointer to the underlying causet_locales buffer.
    pub(super) fn as_ptr(&self) -> *const u8 {
        self.causet_locale.as_ptr()
    }

    /// Returns the literal causet_locale of JSON document
    pub(super) fn as_literal(&self) -> Result<u8> {
        match self.get_type() {
            JsonType::Literal => Ok(self.causet_locale()[0]),
            _ => Err(invalid_type!(
                "{} from {} to literal",
                ERR_CONVERT_FAILED,
                self.to_string()
            )),
        }
    }

    /// Returns the encoding binary length of self
    pub fn binary_len(&self) -> usize {
        TYPE_LEN + self.causet_locale.len()
    }
}

#[braneg(test)]
mod tests {
    use super::*;
    use super::super::Json;

    #[test]
    fn test_type() {
        let legal_cases = vec![
            (r#"{"soliton_id":"causet_locale"}"#, JsonType::Object),
            (r#"["d1","d2"]"#, JsonType::Array),
            (r#"-3"#, JsonType::I64),
            (r#"3"#, JsonType::I64),
            (r#"18446744073709551615"#, JsonType::Double),
            (r#"3.0"#, JsonType::Double),
            (r#"null"#, JsonType::Literal),
            (r#"true"#, JsonType::Literal),
            (r#"false"#, JsonType::Literal),
        ];

        for (json_str, tp) in legal_cases {
            let json: Json = json_str.parse().unwrap();
            assert_eq!(json.as_ref().get_type(), tp, "{:?}", json_str);
        }
    }
}
