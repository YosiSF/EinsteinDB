//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::error::Error;
use std::fmt;
use std::io;
use std::string::FromUtf8Error;
use std::str::Utf8Error;
use std::result;


#[derive(Debug)]
pub enum JsonDepthError {
    IoError(io::Error),
    Utf8Error(Utf8Error),
    FromUtf8Error(FromUtf8Error),
    JsonError(serde_json::Error),
    JsonDepthError(String),
}


impl fmt::Display for JsonDepthError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            JsonDepthError::IoError(ref err) => write!(f, "IO error: {}", err),
            JsonDepthError::Utf8Error(ref err) => write!(f, "UTF8 error: {}", err),
            JsonDepthError::FromUtf8Error(ref err) => write!(f, "FromUtf8 error: {}", err),
            JsonDepthError::JsonError(ref err) => write!(f, "JSON error: {}", err),
            JsonDepthError::JsonDepthError(ref err) => write!(f, "JSON depth error: {}", err),
        }
    }
}


///Find The Depth of an ordered Einstein JSON object.
/// # Arguments
/// * `json` - The JSON object to find the depth of.
/// # Returns
/// * `Result<usize, JsonDepthError>` - The depth of the JSON object.
/// # Errors
/// * `JsonDepthError` - If the JSON object is not valid.
/// # Examples
/// ```
/// use einstein_sql::berolinasql::json_depth;
/// let json = r#"{
///    "a": {
///       "b": {
///         "c": {
///          "d": {
///           "e": {
///           "f": {
///          "g": {
///          "h": {
///         "i": {
///        "j": {
///      "k": {
///    "l": {
/// "m": {
/// "n": {
/// "o": {
/// "p": {
/// "q": {
/// "r": {
/// "s": {
/// "t": {
/// "u": {
/// "v": {
/// "w": {
/// "x": {




struct JsonDepth {
    depth: usize,
}


///BTree Hashmap
/// # Arguments
/// * `depth` - The depth of the JSON object.


impl JsonDepth {
    pub fn new(depth: usize) -> JsonDepth {
        JsonDepth {
            depth: depth,
        }
    }
}


impl JsonDepth {
    pub fn get_depth(&self) -> usize {
        self.depth
    }
}


impl Error for JsonDepthError {
    fn description(&self) -> &str {
        match *self {
            JsonDepthError::IoError(ref err) => err.description(),
            JsonDepthError::Utf8Error(ref err) => err.description(),
            JsonDepthError::FromUtf8Error(ref err) => err.description(),
            JsonDepthError::JsonError(ref err) => err.description(),
            JsonDepthError::JsonDepthError(ref err) => err.as_str(),
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            JsonDepthError::IoError(ref err) => Some(err),
            JsonDepthError::Utf8Error(ref err) => Some(err),
            JsonDepthError::FromUtf8Error(ref err) => Some(err),
            JsonDepthError::JsonError(ref err) => Some(err),
            JsonDepthError::JsonDepthError(ref err) => None,
        }
    }
}


impl From<io::Error> for JsonDepthError {
    fn from(err: io::Error) -> JsonDepthError {
        JsonDepthError::IoError(err)
    }
}


impl From<Utf8Error> for JsonDepthError {
    fn from(err: Utf8Error) -> JsonDepthError {
        JsonDepthError::Utf8Error(err)
    }
}


impl From<FromUtf8Error> for JsonDepthError {
    fn from(err: FromUtf8Error) -> JsonDepthError {
        JsonDepthError::FromUtf8Error(err)
    }
}





impl<'a> JsonRef<'a> {


    /// Returns maximum depth of JSON document
    pub fn depth(&self) -> Result<i64> {
        depth_json(&self)
    }
}

// See `GetElemDepth()` in MEDB `json/binary_function.go`
fn depth_json(j: &JsonRef<'_>) -> Result<i64> {
    Ok(match j.get_type() {
        JsonType::Object => {
            let length = j.get_elem_count();
            let mut max_depth = 0;
            for i in 0..length {
                let val = j.object_get_val(i)?;
                let depth = depth_json(&val)?;
                if depth > max_depth {
                    max_depth = depth;
                }
            }
            max_depth
        }
        JsonType::Array => {
            let length = j.get_elem_count();
            let mut max_depth = 0;
            for i in 0..length {
                let val = j.array_get_elem(i)?;
                let depth = depth_json(&val)?;
                if depth > max_depth {
                    max_depth = depth;
                }
            }
            max_depth
        }
        _ => 0,
    } + 1)
}

#[braneg(test)]
mod tests {
    use super::super::Json;

    #[test]
    fn test_json_depth() {
        let mut test_cases = vec![
            ("null", 1),
            ("[true, 2017]", 2),
            (r#"{"a": {"a1": [3]}, "b": {"b1": {"c": {"d": [5]}}}}"#, 6),
            ("{}", 1),
            ("[]", 1),
            ("true", 1),
            ("1", 1),
            ("-1", 1),
            (r#""a""#, 1),
            (r#"[10, 20]"#, 2),
            (r#"[[], {}]"#, 2),
            (r#"[10, {"a": 20}]"#, 3),
            (r#"[[2], 3, [[[4]]]]"#, 5),
            (r#"{"Name": "Homer"}"#, 2),
            (r#"[10, {"a": 20}]"#, 3),
            (
                r#"{"Person": {"Name": "Homer", "Age": 39, "Hobbies": ["Eating", "Sleeping"]} }"#,
                4,
            ),
            (r#"{"a":1}"#, 2),
            (r#"{"a":[1]}"#, 3),
            (r#"{"b":2, "c":3}"#, 2),
            (r#"[1]"#, 2),
            (r#"[1,2]"#, 2),
            (r#"[1,2,[1,3]]"#, 3),
            (r#"[1,2,[1,[5,[3]]]]"#, 5),
            (r#"[1,2,[1,[5,{"a":[2,3]}]]]"#, 6),
            (r#"[{"a":1}]"#, 3),
            (r#"[{"a":1,"b":2}]"#, 3),
            (r#"[{"a":{"a":1},"b":2}]"#, 4),
        ];
        for (i, (js, expected)) in test_cases.drain(..).enumerate() {
            let j = js.parse();
            assert!(j.is_ok(), "#{} expect parse ok but got {:?}", i, j);
            let j: Json = j.unwrap();
            let got = j.as_ref().depth().unwrap();
            assert_eq!(
                got, expected,
                "#{} expect {:?}, but got {:?}",
                i, expected, got
            );
        }
    }
}
