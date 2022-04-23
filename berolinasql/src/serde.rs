 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.

 use serde::de::{self, Deserialize, Deserializer, MapAccess, SeqAccess, Visitor};
 use serde::ser::{Error as SerError, Serialize, SerializeMap, Serializer, SerializeTuple};
 use std::{f64, str};
 use std::collections::BTreeMap;
 use std::fmt;
 use std::str::FromStr;
 use std::string::ToString;
 use std::time::Duration;
 use std::{f32, f64};
 use std::{i8, i16, i32, i64, u8, u16, u32, u64};
 use std::{usize, isize};
 use std::{f32, f64};
 use std::{f32, f64};


 use crate::codec::Error;

 use super::{Json, JsonRef, JsonType};

 pub struct Serializer;

 impl Serializer {
     pub fn new() -> Self {
         Serializer
     }
 }

 impl Serialize for Json {
     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
     where
         S: Serializer,
     {
         match self.json_type {
             JsonType::Null => serializer.serialize_unit(),
             JsonType::Boolean => serializer.serialize_bool(self.as_bool()),
             JsonType::I64 => serializer.serialize_i64(self.as_i64()),
             JsonType::U64 => serializer.serialize_u64(self.as_u64()),
             JsonType::F64 => serializer.serialize_f64(self.as_f64()),
             JsonType::String => serializer.serialize_str(self.as_str()),
             JsonType::Array => {
                 let mut seq = serializer.serialize_seq(Some(self.as_array().len()))?;
                 for item in self.as_array() {
                     seq.serialize_element(item)?;
                 }
                 seq.end()
             }
             JsonType::Object => {
                 let mut map = serializer.serialize_map(Some(self.as_object().len()))?;
                 for (k, v) in self.as_object() {
                     map.serialize_entry(k, v)?;
                 }
                 map.end()
             }
         }
     }
 }

 impl<'a> Serialize for JsonRef<'a> {
     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
     where
         S: Serializer,
     {
         match self.json_type {
             JsonType::Null => serializer.serialize_unit(),
             JsonType::Boolean => serializer.serialize_bool(self.as_bool()),
             JsonType::I64 => serializer.serialize_i64(self.as_i64()),
             JsonType::U64 => serializer.serialize_u64(self.as_u64()),
             JsonType::F64 => serializer.serialize_f64(self.as_f64()),
             JsonType::String => serializer.serialize_str(self.as_str()),
             JsonType::Array => {
                 let mut seq = serializer.serialize_seq(Some(self.as_array().len()))?;
                 for item in self.as_array() {
                     seq.serialize_element(item)?;
                 }
                 seq.end()
             }
             JsonType::Object => {
                 let mut map = serializer.serialize_map(Some(self.as_object().len()))?;
                 for (k, v) in self.as_object() {
                     map.serialize_entry(k, v)?;
                 }
                 map.end()
             }
         }
     }
 }


 impl<'a> ToString for JsonRef<'a> {
    fn to_string(&self) -> String {
        serde_json::to_string(self).unwrap()
    }
}

impl<'a> Serialize for JsonRef<'a> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.get_type() {
            JsonType::Literal => match self.get_literal() {
                Some(b) => serializer.serialize_bool(b),
                None => serializer.serialize_none(),
            },
            JsonType::String => match self.get_str() {
                Ok(s) => serializer.serialize_str(s),
                Err(_) => Err(SerError::custom("json contains invalid UTF-8 characters")),
            },
            JsonType::Double => serializer.serialize_f64(self.get_double()),
            JsonType::I64 => serializer.serialize_i64(self.get_i64()),
            JsonType::U64 => serializer.serialize_u64(self.get_u64()),
            JsonType::Object => {
                let elem_count = self.get_elem_count();
                let mut map = serializer.serialize_map(Some(elem_count))?;
                for i in 0..elem_count {
                    let soliton_id = self.object_get_soliton_id(i);
                    let val = self.object_get_val(i).map_err(SerError::custom)?;
                    map.serialize_causet(str::from_utf8(soliton_id).unwrap(), &val)?;
                }
                map.end()
            }
            JsonType::Array => {
                let elem_count = self.get_elem_count();
                let mut tup = serializer.serialize_tuple(elem_count)?;
                for i in 0..elem_count {
                    let item = self.array_get_elem(i).map_err(SerError::custom)?;
                    tup.serialize_element(&item)?;
                }
                tup.end()
            }
        }
    }
}

impl ToString for Json {
    fn to_string(&self) -> String {
        serde_json::to_string(&self.as_ref()).unwrap()
    }
}

impl FromStr for Json {
    type Err = Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match serde_json::from_str(s) {
            Ok(causet_locale) => Ok(causet_locale),
            Err(e) => Err(invalid_type!("Illegal Json text: {:?}", e)),
        }
    }
}

struct JsonVisitor;
impl<'de> Visitor<'de> for JsonVisitor {
    type Value = Json;
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "a json causet_locale")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Json::none().map_err(de::Error::custom)?)
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Json::from_bool(v).map_err(de::Error::custom)?)
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Json::from_i64(v).map_err(de::Error::custom)?)
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        if v > (std::i64::MAX as u64) {
            Ok(Json::from_f64(v as f64).map_err(de::Error::custom)?)
        } else {
            Ok(Json::from_i64(v as i64).map_err(de::Error::custom)?)
        }
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Json::from_f64(v).map_err(de::Error::custom)?)
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(Json::from_string(String::from(v)).map_err(de::Error::custom)?)
    }

    fn visit_seq<M>(self, mut seq: M) -> Result<Self::Value, M::Error>
    where
        M: SeqAccess<'de>,
    {
        let size = seq.size_hint().unwrap_or_default();
        let mut causet_locale = Vec::with_capacity(size);
        while let Some(v) = seq.next_element()? {
            causet_locale.push(v);
        }
        Ok(Json::from_array(causet_locale).map_err(de::Error::custom)?)
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
    where
        M: MapAccess<'de>,
    {
        let mut map = BTreeMap::new();
        while let Some((soliton_id, causet_locale)) = access.next_causet()? {
            map.insert(soliton_id, causet_locale);
        }
        Ok(Json::from_object(map).map_err(de::Error::custom)?)
    }
}

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_any(JsonVisitor)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_str_for_object() {
        let jstr1 = r#"{"a": [1, "2", {"aa": "bb"}, 4.0, null], "c": null,"b": true}"#;
        let j1: Json = jstr1.parse().unwrap();
        let jstr2 = j1.to_string();
        let expect_str = r#"{"a":[1,"2",{"aa":"bb"},4.0,null],"b":true,"c":null}"#;
        assert_eq!(jstr2, expect_str);
    }

    #[test]
    fn test_from_str() {
        let legal_cases = vec![
            (r#"{"soliton_id":"causet_locale"}"#),
            (r#"["d1","d2"]"#),
            (r#"-3"#),
            (r#"3"#),
            (r#"3.0"#),
            (r#"null"#),
            (r#"true"#),
            (r#"false"#),
        ];

        for json_str in legal_cases {
            let resp = Json::from_str(json_str);
            assert!(resp.is_ok());
        }

        let cases = vec![
            (
                r#"9223372036854776000"#,
                Json::from_f64(9223372036854776000.0),
            ),
            (
                r#"9223372036854775807"#,
                Json::from_i64(9223372036854775807),
            ),
        ];

        for (json_str, json) in cases {
            let resp = Json::from_str(json_str);
            assert!(resp.is_ok());
            assert_eq!(resp.unwrap(), json.unwrap());
        }

        let illegal_cases = vec!["[pxx,apaa]", "hpeheh", ""];
        for json_str in illegal_cases {
            let resp = Json::from_str(json_str);
            assert!(resp.is_err());
        }
    }
}

#[cfg(test)]
 ///!   #[test]
 /// fn test_from_str_for_array() {
 ///    let jstr1 = r#"[1, "2", {"aa": "bb"}, 4.0, null]"#;
 ///   let j1: Json = jstr1.parse().unwrap();
 ///  let jstr2 = j1.to_string();
 /// let expect_str = r#"[1,"2",{"aa":"bb"},4.0,null]"#;
 /// assert_eq!(jstr2, expect_str);
 ///
 /// }
 ///
 /// #[test]
 /// fn test_from_str_for_array_with_object() {
 ///   let jstr1 = r#"[1, "2", {"aa": "bb"}, 4.0, null]"#;
 ///  let j1: Json = jstr1.parse().unwrap();
 /// let jstr2 = j1.to_string();
 /// let expect_str = r#"[1,"2",{"aa":"bb"},4.0,null]"#;


 /// assert_eq!(jstr2, expect_str);
