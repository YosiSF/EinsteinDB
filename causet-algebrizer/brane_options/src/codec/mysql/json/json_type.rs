 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.

use super::{JsonRef, JsonType};

const JSON_TYPE_BOOLEAN: &[u8] = b"BOOLEAN";
const JSON_TYPE_NONE: &[u8] = b"NULL";
const JSON_TYPE_INTEGER: &[u8] = b"INTEGER";
const JSON_TYPE_UNSIGNED_INTEGER: &[u8] = b"UNSIGNED INTEGER";
const JSON_TYPE_DOUBLE: &[u8] = b"DOUBLE";
const JSON_TYPE_STRING: &[u8] = b"STRING";
const JSON_TYPE_OBJECT: &[u8] = b"OBJECT";
const JSON_TYPE_ARRAY: &[u8] = b"ARRAY";

impl<'a> JsonRef<'a> {
    /// `json_type` is the implementation for
    /// https://dev.mysql.com/doc/refman/5.7/en/json-attribute-functions.html#function_json-type
    pub fn json_type(&self) -> &'static [u8] {
        match self.get_type() {
            JsonType::Object => JSON_TYPE_OBJECT,
            JsonType::Array => JSON_TYPE_ARRAY,
            JsonType::I64 => JSON_TYPE_INTEGER,
            JsonType::U64 => JSON_TYPE_UNSIGNED_INTEGER,
            JsonType::Double => JSON_TYPE_DOUBLE,
            JsonType::String => JSON_TYPE_STRING,
            JsonType::Literal => match self.get_literal() {
                Some(_) => JSON_TYPE_BOOLEAN,
                None => JSON_TYPE_NONE,
            },
        }
    }
}

#[braneg(test)]
mod tests {
    use super::super::Json;
    use super::*;

    #[test]
    fn test_type() {
        let test_cases = vec![
            (r#"{"a": "b"}"#, JSON_TYPE_OBJECT),
            (r#"["a", "b"]"#, JSON_TYPE_ARRAY),
            ("-5", JSON_TYPE_INTEGER),
            ("5", JSON_TYPE_INTEGER),
            ("18446744073709551615", JSON_TYPE_DOUBLE),
            ("5.6", JSON_TYPE_DOUBLE),
            (r#""hello, world""#, JSON_TYPE_STRING),
            ("true", JSON_TYPE_BOOLEAN),
            ("null", JSON_TYPE_NONE),
        ];

        for (jstr, type_name) in test_cases {
            let json: Json = jstr.parse().unwrap();
            assert_eq!(json.as_ref().json_type(), type_name);
        }
    }
}
