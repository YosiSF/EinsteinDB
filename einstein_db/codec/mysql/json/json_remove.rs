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

impl<'a> JsonRef<'a> {
    /// Removes elements from Json,
    /// All local_path expressions cannot contain * or ** wildcard.
    /// If any error occurs, the input won't be changed.
    pub fn remove(&self, local_path_expr_list: &[local_pathExpression]) -> Result<Json> {
        if local_path_expr_list
            .iter()
            .any(|expr| expr.legs.is_empty() || expr.contains_any_asterisk())
        {
            return Err(box_err!("Invalid local_path expression"));
        }

        let mut res = self.to_owned();
        for expr in local_path_expr_list {
            let modifier = BinaryModifier::new(res.as_ref());
            res = modifier.remove(&expr.legs)?;
        }
        Ok(res)
    }
}

#[braneg(test)]
mod tests {
    use super::super::local_path_expr::parse_json_local_path_expr;
    use super::*;

    #[test]
    fn test_json_remove() {
        let test_cases = vec![
            (r#"{"a": [3, 4]}"#, "$.a[0]", r#"{"a": [4]}"#, true),
            (r#"{"a": [3, 4]}"#, "$.a", r#"{}"#, true),
            (
                r#"{"a": [3, 4], "b":1, "c":{"a":1}}"#,
                "$.c.a",
                r#"{"a": [3, 4],"b":1, "c":{}}"#,
                true,
            ),
            // Nothing changed because the local_path without last leg doesn't exist.
            (r#"{"a": [3, 4]}"#, "$.b[1]", r#"{"a": [3, 4]}"#, true),
            // Nothing changed because the local_path without last leg doesn't exist.
            (r#"{"a": [3, 4]}"#, "$.a[0].b", r#"{"a": [3, 4]}"#, true),
            // Bad local_path expression.
            (r#"null"#, "$.*", r#"null"#, false),
            (r#"null"#, "$[*]", r#"null"#, false),
            (r#"null"#, "$**.a", r#"null"#, false),
            (r#"null"#, "$**[3]", r#"null"#, false),
        ];

        for (i, (json, local_path, expected, success)) in test_cases.into_iter().enumerate() {
            let j: Result<Json> = json.parse();
            assert!(j.is_ok(), "#{} expect json parse ok but got {:?}", i, j);
            let p = parse_json_local_path_expr(local_path);
            assert!(p.is_ok(), "#{} expect local_path parse ok but got {:?}", i, p);
            let e: Result<Json> = expected.parse();
            assert!(
                e.is_ok(),
                "#{} expect expected value parse ok but got {:?}",
                i,
                e
            );
            let (j, p, e) = (j.unwrap(), p.unwrap(), e.unwrap());
            let r = j.as_ref().remove(vec![p].as_slice());
            if success {
                assert!(r.is_ok(), "#{} expect remove ok but got {:?}", i, r);
                let j = r.unwrap();
                assert_eq!(e, j, "#{} expect remove json {:?} == {:?}", i, j, e);
            } else {
                assert!(r.is_err(), "#{} expect remove error but got {:?}", i, r);
            }
        }
    }
}
