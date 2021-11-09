//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use super::super::Result;
use super::{JsonRef, JsonType};

impl<'a> JsonRef<'a> {
    /// Returns maximum depth of JSON document
    pub fn depth(&self) -> Result<i64> {
        depth_json(&self)
    }
}

// See `GetElemDepth()` in MilevaDB `json/binary_function.go`
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
