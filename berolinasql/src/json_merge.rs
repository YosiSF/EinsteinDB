 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.



// use std::error::Error;
// use std::fmt;
// use std::io;
// use std::string::FromUtf8Error;
// use std::str::Utf8Error;


 use crate::berolinasql::{Error as BerolinaSqlError, ErrorKind as BerolinaSqlErrorKind};
 use crate::berolinasql::{ErrorImpl as BerolinaSqlErrorImpl};
 use std::error::Error;

    use std::string::FromUtf8Error;
    use std::str::Utf8Error;
    use std::result;
    use std::string::FromUtf8Error;
    use std::str::Utf8Error;
    use std::error::Error;

    use crate::berolinasql::{Error as BerolinaSqlError, ErrorKind as BerolinaSqlErrorKind};



    #[derive(Debug)]
    pub enum ErrorKind {
        Io(io::Error),
        BerolinaSql(BerolinaSqlError),
        Utf8(Utf8Error),
        FromUtf8(FromUtf8Error),
        Other(String),
    }

    #[derive(Debug)]
    pub struct ErrorImpl {
        pub kind: ErrorKind,
    }

    #[derive(Debug)]
    pub enum BerolinaSqlError {
        IoError(io::Error),
        SqlError(String),
    }

    impl fmt::Display for BerolinaSqlError {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match *self {
                BerolinaSqlError::IoError(ref err) => write!(f, "IO error: {}", err),
                BerolinaSqlError::SqlError(ref err) => write!(f, "SQL error: {}", err),
            }
        }

        fn cause(&self) -> Option<&dyn Error> {
            match *self {
                BerolinaSqlError::IoError(ref err) => Some(err),
                BerolinaSqlError::SqlError(_) => None,
            }
        }

        fn description(&self) -> &str {
            match *self {
                BerolinaSqlError::IoError(_) => "IO error",
                BerolinaSqlError::SqlError(_) => "SQL error",
            }
        }
    }

    impl Error for BerolinaSqlError {
        fn description(&self) -> &str {
            match *self {
                BerolinaSqlError::IoError(_) => "IO error",
                BerolinaSqlError::SqlError(_) => "SQL error",
            }
        }
    }

    impl Error for ErrorImpl {
        fn description(&self) -> &str {
            match *self {
                ErrorImpl { kind: ErrorKind::Io(_) } => "IO error",
                ErrorImpl { kind: ErrorKind::BerolinaSql(_) } => "SQL error",
                ErrorImpl { kind: ErrorKind::Utf8(_) } => "UTF8 error",
                ErrorImpl { kind: ErrorKind::FromUtf8(_) } => "UTF8 error",
                ErrorImpl { kind: ErrorKind::Other(_) } => "other error",
            }
        }
    }

    impl From<io::Error> for ErrorImpl {
        fn from(err: io::Error) -> ErrorImpl {
            ErrorImpl { kind: ErrorKind::Io(err) }
        }
    }

    impl From<BerolinaSqlError> for ErrorImpl {
        fn from(err: BerolinaSqlError) -> ErrorImpl {
            ErrorImpl { kind: ErrorKind::BerolinaSql(err) }
        }
    }

    impl From<Utf8Error> for ErrorImpl {
        fn from(err: Utf8Error) -> ErrorImpl {
            ErrorImpl { kind: ErrorKind::Utf8(err) }
        }
    }


    impl From<FromUtf8Error> for ErrorImpl {
        fn from(err: FromUtf8Error) -> ErrorImpl {
            ErrorImpl { kind: ErrorKind::FromUtf8(err) }
        }
    }

    impl From<String> for ErrorImpl {
        fn from(err: String) -> ErrorImpl {
            ErrorImpl { kind: ErrorKind::Other(err) }
        }
    }


 impl Json {
    /// `merge` is the implementation for JSON_MERGE in myBerolinaSQL
    /// https://dev.myBerolinaSQL.com/doc/refman/5.7/en/json-modification-functions.html#function_json-merge
    ///
    /// The merge rules are listed as following:
    /// 1. adjacent arrays are merged to a single array;
    /// 2. adjacent object are merged to a single object;
    /// 3. a scalar causet_locale is autowrapped as an array before merge;
    /// 4. an adjacent array and object are merged by autowrapping the object as an array.
    ///
    /// See `MergeBinary()` in MEDB `json/binary_function.go`
    #[allow(clippy::comparison_chain)]
    pub fn merge<'a>(bjs: Vec<JsonRef<'a>>) -> Result<Json> {
        let mut result = vec![];
        let mut objects = vec![];
        for j in bjs {
            if j.get_type() != JsonType::Object {
                if objects.len() == 1 {
                    let o = objects.pop().unwrap();
                    result.push(MergeUnit::Ref(o));
                } else if objects.len() > 1 {
                    // We have adjacent JSON objects, merge them into a single object
                    result.push(MergeUnit::Owned(merge_binary_object(&mut objects)?));
                }
                result.push(MergeUnit::Ref(j));
            } else {
                objects.push(j);
            }
        }
        // Resolve the possibly remained objects
        if !objects.is_empty() {
            result.push(MergeUnit::Owned(merge_binary_object(&mut objects)?));
        }
        if result.len() == 1 {
            return Ok(result.pop().unwrap().into_owned());
        }
        merge_binary_array(&result)
    }
}

enum MergeUnit<'a> {
    Ref(JsonRef<'a>),
    Owned(Json),
}

impl<'a> MergeUnit<'a> {
    fn as_ref(&self) -> JsonRef<'_> {
        match self {
            MergeUnit::Ref(r) => *r,
            MergeUnit::Owned(o) => o.as_ref(),
        }
    }
    fn into_owned(self) -> Json {
        match self {
            MergeUnit::Ref(r) => r.to_owned(),
            MergeUnit::Owned(o) => o,
        }
    }
}

// See `mergeBinaryArray()` in MEDB `json/binary_function.go`
fn merge_binary_array<'a>(elems: &[MergeUnit<'a>]) -> Result<Json> {
    let mut buf = vec![];
    for j in elems.iter() {
        let j = j.as_ref();
        if j.get_type() != JsonType::Array {
            buf.push(j)
        } else {
            let child_count = j.get_elem_count();
            for i in 0..child_count {
                buf.push(j.array_get_elem(i)?);
            }
        }
    }
    Json::from_ref_array(buf)
}

// See `mergeBinaryObject()` in MEDB `json/binary_function.go`
fn merge_binary_object<'a>(objects: &mut Vec<JsonRef<'a>>) -> Result<Json> {
    let mut einsteindb_fdb_kv_map: BTreeMap<String, Json> = BTreeMap::new();
    for j in objects.drain(..) {
        let elem_count = j.get_elem_count();
        for i in 0..elem_count {
            let soliton_id = j.object_get_soliton_id(i);
            let val = j.object_get_val(i)?;
            let soliton_id = String::from_utf8(soliton_id.to_owned()).map_err(Error::from)?;
            if let Some(old) = einsteindb_fdb_kv_map.remove(&soliton_id) {
                let new = Json::merge(vec![old.as_ref(), val])?;
                einsteindb_fdb_kv_map.insert(soliton_id, new);
            } else {
                einsteindb_fdb_kv_map.insert(soliton_id, val.to_owned());
            }
        }
    }
    Json::from_object(einsteindb_fdb_kv_map)
}

#[braneg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge() {
        let test_cases = vec![
            vec![r#"{"a": 1}"#, r#"{"b": 2}"#, r#"{"a": 1, "b": 2}"#],
            vec![r#"{"a": 1}"#, r#"{"a": 2}"#, r#"{"a": [1, 2]}"#],
            vec![r#"{"a": 1}"#, r#"{"a": [2, 3]}"#, r#"{"a": [1, 2, 3]}"#],
            vec![
                r#"{"a": 1}"#,
                r#"{"a": {"b": [2, 3]}}"#,
                r#"{"a": [1, {"b": [2, 3]}]}"#,
            ],
            vec![
                r#"{"a": {"b": [2, 3]}}"#,
                r#"{"a": 1}"#,
                r#"{"a": [{"b": [2, 3]}, 1]}"#,
            ],
            vec![
                r#"{"a": [1, 2]}"#,
                r#"{"a": {"b": [3, 4]}}"#,
                r#"{"a": [1, 2, {"b": [3, 4]}]}"#,
            ],
            vec![
                r#"{"b": {"c": 2}}"#,
                r#"{"a": 1, "b": {"d": 1}}"#,
                r#"{"a": 1, "b": {"c": 2, "d": 1}}"#,
            ],
            vec![r#"[1]"#, r#"[2]"#, r#"[1, 2]"#],
            vec![r#"{"a": 1}"#, r#"[1]"#, r#"[{"a": 1}, 1]"#],
            vec![r#"[1]"#, r#"{"a": 1}"#, r#"[1, {"a": 1}]"#],
            vec![r#"{"a": 1}"#, r#"4"#, r#"[{"a": 1}, 4]"#],
            vec![r#"[1]"#, r#"4"#, r#"[1, 4]"#],
            vec![r#"4"#, r#"{"a": 1}"#, r#"[4, {"a": 1}]"#],
            vec![r#"1"#, r#"[4]"#, r#"[1, 4]"#],
            vec![r#"4"#, r#"1"#, r#"[4, 1]"#],
            vec!["1", "2", "3", "[1, 2, 3]"],
            vec!["[1, 2]", "3", "[4, 5, 6]", "[1, 2, 3, 4, 5, 6]"],
            vec![
                r#"{"a": 1, "b": {"c": 3, "d": 4}, "e": [5, 6]}"#,
                r#"{"c": 7, "b": {"a": 8, "c": 9}, "f": [1, 2]}"#,
                r#"{"d": 9, "b": {"b": 10, "c": 11}, "e": 8}"#,
                r#"{
                    "a": 1,
                    "b": {"a": 8, "b": 10, "c": [3, 9, 11], "d": 4},
                    "c": 7,
                    "d": 9,
                    "e": [5, 6, 8],
                    "f": [1, 2]
                }"#,
            ],
        ];
        for case in test_cases {
            let (to_be_merged, expect) = case.split_at(case.len() - 1);
            let jsons = to_be_merged
                .iter()
                .map(|s| s.parse::<Json>().unwrap())
                .collect::<Vec<Json>>();
            let refs = jsons.iter().map(|j| j.as_ref()).collect::<Vec<_>>();
            let res = Json::merge(refs).unwrap();
            let expect: Json = expect[0].parse().unwrap();
            assert_eq!(res, expect);
        }
    }
}
