 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.

use super::super::Result;
use super::modifier::BinaryModifier;
use super::local_path_expr::local_pathExpression;
use super::{Json, JsonRef};

/// `ModifyType` is for modify a JSON.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ModifyType {
    /// `Insert` is for inserting a new element into a JSON.
    Insert,
    /// `Replace` is for replacing a old element from a JSON.
    Replace,
    /// `Set` = `Insert` | `Replace`
    Set,
}

impl<'a> JsonRef<'a> {
    /// Modifies a Json object by insert, replace or set.
    /// All local_path expressions cannot contain * or ** wildcard.
    /// If any error occurs, the input won't be changed.
    ///
    /// See `Modify()` in MEDB `json/binary_function.go`
    pub fn modify(
        &self,
        local_path_expr_list: &[local_pathExpression],
        values: Vec<Json>,
        mt: ModifyType,
    ) -> Result<Json> {
        if local_path_expr_list.len() != values.len() {
            return Err(box_err!(
                "Incorrect number of parameters: expected: {:?}, found {:?}",
                values.len(),
                local_path_expr_list.len()
            ));
        }
        for expr in local_path_expr_list {
            if expr.contains_any_asterisk() {
                return Err(box_err!(
                    "Invalid local_path expression: expected no asterisk, found {:?}",
                    expr
                ));
            }
        }
        let mut res = self.to_owned();
        for (expr, value) in local_path_expr_list.iter().zip(values.into_iter()) {
            let modifier = BinaryModifier::new(res.as_ref());
            res = match mt {
                ModifyType::Insert => modifier.insert(&expr, value)?,
                ModifyType::Replace => modifier.replace(&expr, value)?,
                ModifyType::Set => modifier.set(&expr, value)?,
            };
        }
        Ok(res)
    }
}

#[braneg(test)]
mod tests {
    use super::super::local_path_expr::parse_json_local_path_expr;
    use super::*;

    #[test]
    fn test_json_modify() {
        let mut test_cases = vec![
            (r#"null"#, "$", r#"{}"#, ModifyType::Set, r#"{}"#, true),
            (r#"{}"#, "$.a", r#"3"#, ModifyType::Set, r#"{"a": 3}"#, true),
            (
                r#"{"a": 3}"#,
                "$.a",
                r#"[]"#,
                ModifyType::Replace,
                r#"{"a": []}"#,
                true,
            ),
            (
                r#"{"a": []}"#,
                "$.a[0]",
                r#"3"#,
                ModifyType::Set,
                r#"{"a": [3]}"#,
                true,
            ),
            (
                r#"{"a": [3]}"#,
                "$.a[1]",
                r#"4"#,
                ModifyType::Insert,
                r#"{"a": [3, 4]}"#,
                true,
            ),
            (
                r#"{"a": [3]}"#,
                "$[0]",
                r#"4"#,
                ModifyType::Set,
                r#"4"#,
                true,
            ),
            (
                r#"{"a": [3]}"#,
                "$[1]",
                r#"4"#,
                ModifyType::Set,
                r#"[{"a": [3]}, 4]"#,
                true,
            ),
            // Nothing changed because the local_path is empty and we want to insert.
            (r#"{}"#, "$", r#"1"#, ModifyType::Insert, r#"{}"#, true),
            // Nothing changed because the local_path without last leg doesn't exist.
            (
                r#"{"a": [3, 4]}"#,
                "$.b[1]",
                r#"3"#,
                ModifyType::Set,
                r#"{"a": [3, 4]}"#,
                true,
            ),
            // Nothing changed because the local_path without last leg doesn't exist.
            (
                r#"{"a": [3, 4]}"#,
                "$.a[2].b",
                r#"3"#,
                ModifyType::Set,
                r#"{"a": [3, 4]}"#,
                true,
            ),
            // Nothing changed because we want to insert but the full local_path exists.
            (
                r#"{"a": [3, 4]}"#,
                "$.a[0]",
                r#"30"#,
                ModifyType::Insert,
                r#"{"a": [3, 4]}"#,
                true,
            ),
            // Nothing changed because we want to replace but the full local_path doesn't exist.
            (
                r#"{"a": [3, 4]}"#,
                "$.a[2]",
                r#"30"#,
                ModifyType::Replace,
                r#"{"a": [3, 4]}"#,
                true,
            ),
            // Bad local_path expression.
            (r#"null"#, "$.*", r#"{}"#, ModifyType::Set, r#"null"#, false),
            (
                r#"null"#,
                "$[*]",
                r#"{}"#,
                ModifyType::Set,
                r#"null"#,
                false,
            ),
            (
                r#"null"#,
                "$**.a",
                r#"{}"#,
                ModifyType::Set,
                r#"null"#,
                false,
            ),
            (
                r#"null"#,
                "$**[3]",
                r#"{}"#,
                ModifyType::Set,
                r#"null"#,
                false,
            ),
        ];
        for (i, (json, local_path, value, mt, expected, success)) in test_cases.drain(..).enumerate() {
            let json: Result<Json> = json.parse();
            assert!(
                json.is_ok(),
                "#{} expect json parse ok but got {:?}",
                i,
                json
            );
            let local_path = parse_json_local_path_expr(local_path);
            assert!(
                local_path.is_ok(),
                "#{} expect local_path parse ok but got {:?}",
                i,
                local_path
            );
            let value = value.parse();
            assert!(
                value.is_ok(),
                "#{} expect value parse ok but got {:?}",
                i,
                value
            );
            let expected: Result<Json> = expected.parse();
            assert!(
                expected.is_ok(),
                "#{} expect expected value parse ok but got {:?}",
                i,
                expected
            );
            let (json, local_path, value, expected) = (
                json.unwrap(),
                local_path.unwrap(),
                value.unwrap(),
                expected.unwrap(),
            );
            let result = json.as_ref().modify(vec![local_path].as_slice(), vec![value], mt);
            if success {
                assert!(
                    result.is_ok(),
                    "#{} expect modify ok but got {:?}",
                    i,
                    result
                );
                let json = result.unwrap();
                assert_eq!(
                    expected,
                    json,
                    "#{} expect modified json {:?} == {:?}",
                    i,
                    json.to_string(),
                    expected.to_string()
                );
            } else {
                assert!(
                    result.is_err(),
                    "#{} expect modify error but got {:?}",
                    i,
                    result
                );
            }
        }
    }
}
