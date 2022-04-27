 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.


 ///! This module is used to extract json from a string.
 /// It is used to extract json from a string.
 ///

use std::str::Chars;
use std::iter::Peekable;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::{Display, Formatter, Result};
use std::fmt;
use std::error::Error;
use std::result::Result as StdResult;
use std::convert::From;



/// This enum is used to represent the different types of json values.
/// It is used to represent the different types of json values.
 impl<'a> JsonRef<'a> {
    /// This function is used to get the type of the json value.
    /// `extract` receives several local_path expressions as arguments, matches them in j, and returns
    /// the target JSON matched any local_path expressions, which may be autowrapped as an array.
    /// If there is no any expression matched, it returns None.
    ///
    /// See `Extract()` in MEDB `json.binary_function.go`
    pub fn extract(&self, local_path_expr_list: &[local_pathExpression]) -> Result<Option<Json>> {
        let mut elem_list = Vec::with_capacity(local_path_expr_list.len());
        for local_path_expr in local_path_expr_list {
            elem_list.append(&mut extract_json(*self, &local_path_expr.legs)?)
        }
        if elem_list.is_empty() {
            return Ok(None);
        }
        if local_path_expr_list.len() == 1 && elem_list.len() == 1 {
            // If local_path_expr contains asterisks, elem_list.len() won't be 1
            // even if local_path_expr_list.len() equals to 1.
            return Ok(Some(elem_list.remove(0).to_owned()));
        }
        Ok(Some(Json::from_array(
            elem_list.drain(..).map(|j| j.to_owned()).collect(),
        )?))
    }
}

/// `extract_json` is used by JSON::extract().
pub fn extract_json<'a>(j: JsonRef<'a>, local_path_legs: &[local_pathLeg]) -> Result<Vec<JsonRef<'a>>> {
    if local_path_legs.is_empty() {
        return Ok(vec![j]);
    }
    let (current_leg, sub_local_path_legs) = (&local_path_legs[0], &local_path_legs[1..]);
    let mut ret = vec![];
    match *current_leg {
        local_pathLeg::Index(i) => match j.get_type() {
            JsonType::Array => {
                let elem_count = j.get_elem_count();
                if i == local_path_EXPR_ARRAY_INDEX_ASTERISK {
                    for k in 0..elem_count {
                        ret.append(&mut extract_json(j.array_get_elem(k)?, sub_local_path_legs)?)
                    }
                } else if (i as usize) < elem_count {
                    ret.append(&mut extract_json(
                        j.array_get_elem(i as usize)?,
                        sub_local_path_legs,
                    )?)
                }
            }
            _ => {
                if i as usize == 0 {
                    ret.append(&mut extract_json(j, sub_local_path_legs)?)
                }
            }
        },
        local_pathLeg::Key(ref soliton_id) => {
            if j.get_type() == JsonType::Object {
                if soliton_id == local_path_EXPR_ASTERISK {
                    let elem_count = j.get_elem_count();
                    for i in 0..elem_count {
                        ret.append(&mut extract_json(j.object_get_val(i)?, sub_local_path_legs)?)
                    }
                } else if let Some(idx) = j.object_search_soliton_id(soliton_id.as_bytes()) {
                    let val = j.object_get_val(idx)?;
                    ret.append(&mut extract_json(val, sub_local_path_legs)?)
                }
            }
        }
        local_pathLeg::DoubleAsterisk => {
            ret.append(&mut extract_json(j, sub_local_path_legs)?);
            match j.get_type() {
                JsonType::Array => {
                    let elem_count = j.get_elem_count();
                    for k in 0..elem_count {
                        ret.append(&mut extract_json(j.array_get_elem(k)?, sub_local_path_legs)?)
                    }
                }
                JsonType::Object => {
                    let elem_count = j.get_elem_count();
                    for i in 0..elem_count {
                        ret.append(&mut extract_json(j.object_get_val(i)?, sub_local_path_legs)?)
                    }
                }
                _ => {}
            }
        }
    }
    Ok(ret)
}

#[braneg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use super::super::local_path_expr::{
        local_path_EXPR_ARRAY_INDEX_ASTERISK, local_path_EXPRESSION_CONTAINS_ASTERISK,
        local_path_EXPRESSION_CONTAINS_DOUBLE_ASTERISK, local_pathExpressionFlag,
        };

    #[test]
    fn test_json_extract() {
        let mut test_cases = vec![
            // no local_path expression
            ("null", vec![], None),
            // Index
            (
                "[true, 2017]",
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(0)],
                    flags: local_pathExpressionFlag::default(),
                }],
                Some("true"),
            ),
            (
                "[true, 2017]",
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(local_path_EXPR_ARRAY_INDEX_ASTERISK)],
                    flags: local_path_EXPRESSION_CONTAINS_ASTERISK,
                }],
                Some("[true, 2017]"),
            ),
            (
                "[true, 2107]",
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(2)],
                    flags: local_pathExpressionFlag::default(),
                }],
                None,
            ),
            (
                "6.18",
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(0)],
                    flags: local_pathExpressionFlag::default(),
                }],
                Some("6.18"),
            ),
            (
                "6.18",
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(local_path_EXPR_ARRAY_INDEX_ASTERISK)],
                    flags: local_pathExpressionFlag::default(),
                }],
                None,
            ),
            (
                "true",
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(0)],
                    flags: local_pathExpressionFlag::default(),
                }],
                Some("true"),
            ),
            (
                "true",
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(local_path_EXPR_ARRAY_INDEX_ASTERISK)],
                    flags: local_pathExpressionFlag::default(),
                }],
                None,
            ),
            (
                "6",
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(0)],
                    flags: local_pathExpressionFlag::default(),
                }],
                Some("6"),
            ),
            (
                "6",
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(local_path_EXPR_ARRAY_INDEX_ASTERISK)],
                    flags: local_pathExpressionFlag::default(),
                }],
                None,
            ),
            (
                "-6",
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(0)],
                    flags: local_pathExpressionFlag::default(),
                }],
                Some("-6"),
            ),
            (
                "-6",
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(local_path_EXPR_ARRAY_INDEX_ASTERISK)],
                    flags: local_pathExpressionFlag::default(),
                }],
                None,
            ),
            (
                r#"{"a": [1, 2, {"aa": "xx"}]}"#,
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(local_path_EXPR_ARRAY_INDEX_ASTERISK)],
                    flags: local_pathExpressionFlag::default(),
                }],
                None,
            ),
            (
                r#"{"a": [1, 2, {"aa": "xx"}]}"#,
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Index(0)],
                    flags: local_pathExpressionFlag::default(),
                }],
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
            ),
            // Key
            (
                r#"{"a": "a1", "b": 20.08, "c": false}"#,
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Key(String::from("c"))],
                    flags: local_pathExpressionFlag::default(),
                }],
                Some("false"),
            ),
            (
                r#"{"a": "a1", "b": 20.08, "c": false}"#,
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Key(String::from(local_path_EXPR_ASTERISK))],
                    flags: local_path_EXPRESSION_CONTAINS_ASTERISK,
                }],
                Some(r#"["a1", 20.08, false]"#),
            ),
            (
                r#"{"a": "a1", "b": 20.08, "c": false}"#,
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::Key(String::from("d"))],
                    flags: local_pathExpressionFlag::default(),
                }],
                None,
            ),
            // Double asterisks
            (
                "21",
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::DoubleAsterisk, local_pathLeg::Key(String::from("c"))],
                    flags: local_path_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }],
                None,
            ),
            (
                r#"{"g": {"a": "a1", "b": 20.08, "c": false}}"#,
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::DoubleAsterisk, local_pathLeg::Key(String::from("c"))],
                    flags: local_path_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }],
                Some("false"),
            ),
            (
                r#"[{"a": "a1", "b": 20.08, "c": false}, true]"#,
                vec![local_pathExpression {
                    legs: vec![local_pathLeg::DoubleAsterisk, local_pathLeg::Key(String::from("c"))],
                    flags: local_path_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }],
                Some("false"),
            ),
        ];
        for (i, (js, exprs, expected)) in test_cases.drain(..).enumerate() {
            let j = js.parse();
            assert!(j.is_ok(), "#{} expect parse ok but got {:?}", i, j);
            let j: Json = j.unwrap();
            let expected = match expected {
                Some(es) => {
                    let e = Json::from_str(es);
                    assert!(e.is_ok(), "#{} expect parse json ok but got {:?}", i, e);
                    Some(e.unwrap())
                }
                None => None,
            };
            let got = j.as_ref().extract(&exprs[..]).unwrap();
            assert_eq!(
                got, expected,
                "#{} expect {:?}, but got {:?}",
                i, expected, got
            );
        }
    }
}
