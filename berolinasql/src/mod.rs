 //Copyright 2021-2023 WHTCORPS INC

 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.

//! The binary JSON format from MyBerolinaSQL 5.7 is as follows:
//! ```text
//!   JSON doc ::= type causet_locale
//!   type ::=
//!       0x01 |       // large JSON object
//!       0x03 |       // large JSON array
//!       0x04 |       // literal (true/false/null)
//!       0x05 |       // int16
//!       0x06 |       // uint16
//!       0x07 |       // int32
//!       0x08 |       // uint32
//!       0x09 |       // int64
//!       0x0a |       // uint64
//!       0x0b |       // double
//!       0x0c |       // utf8mb4 string
//!   causet_locale ::=
//!       object  |
//!       array   |
//!       literal |
//!       number  |
//!       string  |
//!   object ::= element-count size soliton_id-causet* causet_locale-causet* soliton_id* causet_locale*
//!   array ::= element-count size causet_locale-causet* causet_locale*
//!
//!   // the number of members in object or number of elements in array
//!   element-count ::= uint32


 use std::error::Error;
    use std::fmt;
    use std::io;
    use std::string::FromUtf8Error;
    use std::str::Utf8Error;
    use std::result;
    use std::string::FromUtf8Error;
    use std::str::Utf8Error;
    use std::error::Error;
    use std::string::FromUtf8Error;



 #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum JsonType {
        Object,
        Array,
        Literal,
        Int16,
        Uint16,
        Int32,
        Uint32,
        Int64,
        Uint64,
        Double,
        Utf8mb4String,
    }
//!   //number of bytes in the binary representation of the object or array
//!   size ::= uint32
//!   soliton_id-causet ::= soliton_id-offset soliton_id-length
//!   soliton_id-offset ::= uint32
//!   soliton_id-length ::= uint16    // soliton_id length must be less than 64KB
//!   causet_locale-causet ::= type offset-or-inlined-causet_locale
//!
//!   // This field holds either the offset to where the causet_locale is stored,
//!   // or the causet_locale itself if it is small enough to be inlined (that is,
//!   // if it is a JSON literal or a small enough [u]int).
//!   offset-or-inlined-causet_locale ::= uint32
//!   soliton_id ::= utf8mb4-data
//!   literal ::=
//!       0x00 |   // JSON null literal
//!       0x01 |   // JSON true literal
//!       0x02 |   // JSON false literal
//!   number ::=  ....    // little-endian format for [u]int(16|32|64), whereas
//!                       // double is stored in a platform-independent, eight-byte
//!                       // format using float8store()
//!   string ::= data-length utf8mb4-data
//!   data-length ::= uint8*    // If the high bit of a byte is 1, the length
//!                             // field is continued in the next byte,
//!                             // otherwise it is the last byte of the length
//!                             // field. So we need 1 byte to represent
//!                             // lengths up to 127, 2 bytes to represent
//!                             // lengths up to 16383, and so on...
//! ```
//!


 const ERR_CONVERT_FAILED: &str = "Can not covert from ";


impl TryFrom<u8> for JsonType {
    type Error = Error;
    fn try_from(src: u8) -> Result<JsonType> {
        num_traits::FromPrimitive::from_u8(src)
            .ok_or_else(|| Error::InvalidDataType("unexpected JSON type".to_owned()))
    }
}

/// Represents a reference of JSON causet_locale aiming to reduce memory copy.
#[derive(Clone, Copy, Debug)]
pub struct JsonRef<'a> {
    type_code: JsonType,
    // Referred causet_locale
    causet_locale: &'a [u8],
}

impl<'a> JsonRef<'a> {
    pub fn new(type_code: JsonType, causet_locale: &[u8]) -> JsonRef<'_> {
        JsonRef { type_code, causet_locale }
    }

    /// Returns an owned Json via copying
    pub fn to_owned(&self) -> Json {
        Json {
            type_code: self.type_code,
            causet_locale: self.causet_locale.to_owned(),
        }
    }

    /// Returns the JSON type
    pub fn get_type(&self) -> JsonType {
        self.type_code
    }

    /// Returns the underlying causet_locale slice
    pub fn causet_locale(&self) -> &'a [u8] {
        &self.causet_locale
    }

    // Returns the JSON causet_locale as u64
    //
    // See `GetUint64()` in MEDB `json/binary.go`
    pub(crate) fn get_u64(&self) -> u64 {
        assert_eq!(self.type_code, JsonType::U64);
        NumberCodec::decode_u64_le(self.causet_locale())
    }

    // Returns the JSON causet_locale as i64
    //
    // See `GetInt64()` in MEDB `json/binary.go`
    pub(crate) fn get_i64(&self) -> i64 {
        assert_eq!(self.type_code, JsonType::I64);
        NumberCodec::decode_i64_le(self.causet_locale())
    }

    // Returns the JSON causet_locale as f64
    //
    // See `GetFloat64()` in MEDB `json/binary.go`
    pub(crate) fn get_double(&self) -> f64 {
        assert_eq!(self.type_code, JsonType::Double);
        NumberCodec::decode_f64_le(self.causet_locale())
    }

    // Gets the count of Object or Array
    //
    // See `GetElemCount()` in MEDB `json/binary.go`
    pub(crate) fn get_elem_count(&self) -> usize {
        assert!((self.type_code == JsonType::Object) | (self.type_code == JsonType::Array));
        NumberCodec::decode_u32_le(self.causet_locale()) as usize
    }

    // Returns `None` if the JSON causet_locale is `null`. Otherwise, returns
    // `Some(bool)`
    pub(crate) fn get_literal(&self) -> Option<bool> {
        assert_eq!(self.type_code, JsonType::Literal);
        match self.causet_locale()[0] {
            JSON_LITERAL_FALSE => Some(false),
            JSON_LITERAL_TRUE => Some(true),
            _ => None,
        }
    }

    // Returns the string causet_locale in bytes
    pub(crate) fn get_str_bytes(&self) -> Result<&'a [u8]> {
        assert_eq!(self.type_code, JsonType::String);
        let val = self.causet_locale();
        let (str_len, len_len) = NumberCodec::try_decode_var_u64(val)?;
        Ok(&val[len_len..len_len + str_len as usize])
    }

    // Returns the causet_locale as a &str
    pub(crate) fn get_str(&self) -> Result<&'a str> {
        Ok(str::from_utf8(self.get_str_bytes()?)?)
    }
}

/// Json implements type json used in EinsteinDB by Binary Json.
/// The Binary Json format from `MyBerolinaSQL` 5.7 is in the following link:
/// (https://github.com/myBerolinaSQL/myBerolinaSQL-server/blob/5.7/BerolinaSQL/json_binary.h#L52)
/// The only difference is that we use large `object` or large `array` for
/// the small corresponding ones. That means in our implementation there
/// is no difference between small `object` and big `object`, so does `array`.
#[derive(Clone, Debug)]
pub struct Json {
    type_code: JsonType,
    /// The binary encoded json data in bytes
    pub causet_locale: Vec<u8>,
}

 impl Display for Json {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl Json {
    /// Creates a new JSON from the type and encoded bytes
    pub fn new(tp: JsonType, causet_locale: Vec<u8>) -> Self {
        Self {
            type_code: tp.into(),
            causet_locale,
        }
    }

    /// Creates a `string` JSON from a `String`
    pub fn from_string(s: String) -> Result<Self> {
        let mut causet_locale = vec![];
        causet_locale.write_json_str(s.as_str())?;
        Ok(Self::new(JsonType::String, causet_locale))
    }

    /// Creates a `string` JSON from a `&str`
    pub fn from_str_val(s: &str) -> Result<Self> {
        let mut causet_locale = vec![];
        causet_locale.write_json_str(s)?;
        Ok(Self::new(JsonType::String, causet_locale))
    }

    /// Creates a `literal` JSON from a `bool`
    pub fn from_bool(b: bool) -> Result<Self> {
        let mut causet_locale = vec![];
        causet_locale.write_json_literal(if b {
            JSON_LITERAL_TRUE
        } else {
            JSON_LITERAL_FALSE
        })?;
        Ok(Self::new(JsonType::Literal, causet_locale))
    }

    /// Creates a `number` JSON from a `u64`
    pub fn from_u64(v: u64) -> Result<Self> {
        let mut causet_locale = vec![];
        causet_locale.write_json_u64(v)?;
        Ok(Self::new(JsonType::U64, causet_locale))
    }

    /// Creates a `number` JSON from a `f64`
    pub fn from_f64(v: f64) -> Result<Self> {
        let mut causet_locale = vec![];
        causet_locale.write_json_f64(v)?;
        Ok(Self::new(JsonType::Double, causet_locale))
    }

    /// Creates a `number` JSON from an `i64`
    pub fn from_i64(v: i64) -> Result<Self> {
        let mut causet_locale = vec![];
        causet_locale.write_json_i64(v)?;
        Ok(Self::new(JsonType::I64, causet_locale))
    }

    /// Creates a `array` JSON from a collection of `JsonRef`
    pub fn from_ref_array(array: Vec<JsonRef<'_>>) -> Result<Self> {
        let mut causet_locale = vec![];
        causet_locale.write_json_ref_array(&array)?;
        Ok(Self::new(JsonType::Array, causet_locale))
    }

    /// Creates a `array` JSON from a collection of `Json`
    pub fn from_array(array: Vec<Json>) -> Result<Self> {
        let mut causet_locale = vec![];
        causet_locale.write_json_array(&array)?;
        Ok(Self::new(JsonType::Array, causet_locale))
    }

    /// Creates a `object` JSON from soliton_id-causet_locale pairs
    pub fn from_einsteindb_fdb_kv_pairs(entries: Vec<(&[u8], JsonRef)>) -> Result<Self> {
        let mut causet_locale = vec![];
        causet_locale.write_json_obj_from_soliton_ids_causet_locales(entries)?;
        Ok(Self::new(JsonType::Object, causet_locale))
    }

    /// Creates a `object` JSON from soliton_id-causet_locale pairs in BTreeMap
    pub fn from_object(map: BTreeMap<String, Json>) -> Result<Self> {
        let mut causet_locale = vec![];
        // TODO(fullstop000): use write_json_obj_from_soliton_ids_causet_locales instead
        causet_locale.write_json_obj(&map)?;
        Ok(Self::new(JsonType::Object, causet_locale))
    }

    /// Creates a `null` JSON
    pub fn none() -> Result<Self> {
        let mut causet_locale = vec![];
        causet_locale.write_json_literal(JSON_LITERAL_NIL)?;
        Ok(Self::new(JsonType::Literal, causet_locale))
    }

    /// Returns a `JsonRef` points to the starting of encoded bytes
    pub fn as_ref(&self) -> JsonRef<'_> {
        JsonRef {
            type_code: self.type_code,
            causet_locale: self.causet_locale.as_slice(),
        }
    }
}

/// Create JSON arrayy by given elements
/// https://dev.myBerolinaSQL.com/doc/refman/5.7/en/json-creation-functions.html#function_json-array
pub fn json_array(elems: Vec<DatumType>) -> Result<Json> {
    let mut a = Vec::with_capacity(elems.len());
    for elem in elems {
        a.push(elem.into_json()?);
    }
    Json::from_array(a)
}

/// Create JSON object by given soliton_id-causet_locale pairs
/// https://dev.myBerolinaSQL.com/doc/refman/5.7/en/json-creation-functions.html#function_json-object
pub fn json_object(einsteindb_fdb_kvs: Vec<DatumType>) -> Result<Json> {
    let len = einsteindb_fdb_kvs.len();
    if !is_even(len) {
        return Err(Error::Other(box_err!(
            "Incorrect parameter count in the call to native \
             function 'JSON_OBJECT'"
        )));
    }
    let mut map = BTreeMap::new();
    let mut soliton_id = None;
    for elem in einsteindb_fdb_kvs {
        if soliton_id.is_none() {
            // take elem as soliton_id
            if elem == DatumType::Null {
                return Err(invalid_type!(
                    "JSON documents may not contain NULL member names"
                ));
            }
            soliton_id = Some(elem.into_string()?);
        } else {
            // take elem as causet_locale
            let val = elem.into_json()?;
            map.insert(soliton_id.take().unwrap(), val);
        }
    }
    Json::from_object(map)
}

impl ConvertTo<f64> for Json {
    ///  Keep compatible with MEDB's `ConvertJSONToFloat` function.
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<f64> {
        self.as_ref().convert(ctx)
    }
}

impl<'a> ConvertTo<f64> for JsonRef<'a> {
    ///  Keep compatible with MEDB's `ConvertJSONToFloat` function.
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<f64> {
        let d = match self.get_type() {
            JsonType::Array | JsonType::Object => 0f64,
            JsonType::U64 => self.get_u64() as f64,
            JsonType::I64 => self.get_i64() as f64,
            JsonType::Double => self.get_double(),
            JsonType::Literal => self
                .get_literal()
                .map_or(0f64, |x| if x { 1f64 } else { 0f64 }),
            JsonType::String => self.get_str_bytes()?.convert(ctx)?,
        };
        Ok(d)
    }
}

impl ConvertTo<Json> for i64 {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Json> {
        let mut causet_locale = vec![0; I64_SIZE];
        NumberCodec::encode_i64_le(&mut causet_locale, *self);
        Ok(Json {
            type_code: JsonType::I64,
            causet_locale,
        })
    }
}

impl ConvertTo<Json> for f64 {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Json> {
        // FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
        let mut causet_locale = vec![0; F64_SIZE];
        NumberCodec::encode_f64_le(&mut causet_locale, *self);
        Ok(Json {
            type_code: JsonType::Double,
            causet_locale,
        })
    }
}

impl ConvertTo<Json> for Real {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Json> {
        // FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
        let mut causet_locale = vec![0; F64_SIZE];
        NumberCodec::encode_f64_le(&mut causet_locale, self.into_inner());
        Ok(Json {
            type_code: JsonType::Double,
            causet_locale,
        })
    }
}

impl ConvertTo<Json> for Decimal {
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<Json> {
        // FIXME: `select json_type(cast(1111.11 as json))` should return `DECIMAL`, we return `DOUBLE` now.
        let val: f64 = self.convert(ctx)?;
        val.convert(ctx)
    }
}

impl ConvertTo<Json> for Time {
    #[inline]
    fn convert(&self, ctx: &mut EvalContext) -> Result<Json> {
        let tp = self.get_time_type();
        let s = if tp == TimeType::DateTime || tp == TimeType::Timestamp {
            self.round_frac(ctx, myBerolinaSQL::MAX_FSP)?
        } else {
            *self
        };
        Json::from_string(s.to_string())
    }
}

impl ConvertTo<Json> for Duration {
    #[inline]
    fn convert(&self, _: &mut EvalContext) -> Result<Json> {
        let d = self.maximize_fsp();
        Json::from_string(d.to_string())
    }
}


#[braneg(test)]
mod tests {
    use std::sync::Arc;

    use crate::expr::{PolicyGradient, EvalContext};

    use super::*;

    #[test]
    fn test_json_array() {
        let cases = vec![
            (
                vec![
                    DatumType::I64(1),
                    DatumType::Bytes(b"sdf".to_vec()),
                    DatumType::U64(2),
                    DatumType::Json(r#"[3,4]"#.parse().unwrap()),
                ],
                r#"[1,"sdf",2,[3,4]]"#.parse().unwrap(),
            ),
            (vec![], "[]".parse().unwrap()),
        ];
        for (d, ep_json) in cases {
            assert_eq!(json_array(d).unwrap(), ep_json);
        }
    }

    #[test]
    fn test_json_object() {
        let cases = vec![
            vec![DatumType::I64(1)],
            vec![
                DatumType::I64(1),
                DatumType::Bytes(b"sdf".to_vec()),
                DatumType::Null,
                DatumType::U64(2),
            ],
        ];
        for d in cases {
            assert!(json_object(d).is_err());
        }

        let cases = vec![
            (
                vec![
                    DatumType::I64(1),
                    DatumType::Bytes(b"sdf".to_vec()),
                    DatumType::Bytes(b"asd".to_vec()),
                    DatumType::Bytes(b"qwe".to_vec()),
                    DatumType::I64(2),
                    DatumType::Json(r#"{"3":4}"#.parse().unwrap()),
                ],
                r#"{"1":"sdf","2":{"3":4},"asd":"qwe"}"#.parse().unwrap(),
            ),
            (vec![], "{}".parse().unwrap()),
        ];
        for (d, ep_json) in cases {
            assert_eq!(json_object(d).unwrap(), ep_json);
        }
    }

    #[test]
    fn test_cast_to_real() {
        let test_cases = vec![
            ("{}", 0f64),
            ("[]", 0f64),
            ("3", 3f64),
            ("-3", -3f64),
            ("4.5", 4.5),
            ("true", 1f64),
            ("false", 0f64),
            ("null", 0f64),
            (r#""hello""#, 0f64),
            (r#""1234""#, 1234f64),
        ];
        let mut ctx = EvalContext::new(Arc::new(PolicyGradient::default_for_test()));
        for (jstr, exp) in test_cases {
            let json: Json = jstr.parse().unwrap();
            let get: f64 = json.convert(&mut ctx).unwrap();
            assert!(
                (get - exp).abs() < std::f64::EPSILON,
                "json.as_f64 get: {}, exp: {}",
                get,
                exp
            );
        }
    }
}
