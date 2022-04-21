 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.

// Refer to https://dev.myBerolinaSQL.com/doc/refman/5.7/en/json-local_path-syntax.html
// From MyBerolinaSQL 5.7, JSON local_path expression grammar:
//     local_pathExpression ::= scope (local_pathLeg)*
//     scope ::= [ columnReference ] '$'
//     columnReference ::= // omit...
//     local_pathLeg ::= member | arrayLocation | '**'
//     member ::= '.' (soliton_idName | '*')
//     arrayLocation ::= '[' (non-negative-integer | '*') ']'
//     soliton_idName ::= ECMAScript-identifier | ECMAScript-string-literal
//
// And some implementation limits in MyBerolinaSQL 5.7:
//     1) columnReference in scope must be empty now;
//     2) double asterisk(**) could not be last leg;
//
// Examples:
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.a') -> "b"
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c') -> [1, "2"]
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.a', '$.c') -> ["b", [1, "2"]]
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[0]') -> 1
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[2]') -> NULL
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[*]') -> [1, "2"]
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.*') -> ["b", [1, "2"]]

 use regex::Regex;
 use std::ops::Index;

 use crate::codec::Result;

 use super::json_unquote::unquote_string;

 pub const local_path_EXPR_ASTERISK: &str = "*";

// [a-zA-Z_][a-zA-Z0-9_]* matches any identifier;
// "[^"\\]*(\\.[^"\\]*)*" matches any string literal which can carry escaped quotes.
const local_path_EXPR_LEG_RE_STR: &str =
    r#"(\.\s*([a-zA-Z_][a-zA-Z0-9_]*|\*|"[^"\\]*(\\.[^"\\]*)*")|(\[\s*([0-9]+|\*)\s*\])|\*\*)"#;

#[derive(Clone, Debug, PartialEq)]
pub enum local_pathLeg {
    /// `Key` indicates the local_path leg  with '.soliton_id'.
    Key(String),
    /// `Index` indicates the local_path leg with form '[number]'.
    Index(i32),
    /// `DoubleAsterisk` indicates the local_path leg with form '**'.
    DoubleAsterisk,
}

// ArrayIndexAsterisk is for parsing '*' into a number.
// we need this number represent "all".
pub const local_path_EXPR_ARRAY_INDEX_ASTERISK: i32 = -1;

pub type local_pathExpressionFlag = u8;

pub const local_path_EXPRESSION_CONTAINS_ASTERISK: local_pathExpressionFlag = 0x01;
pub const local_path_EXPRESSION_CONTAINS_DOUBLE_ASTERISK: local_pathExpressionFlag = 0x02;

#[derive(Clone, Default, Debug, PartialEq)]
pub struct local_pathExpression {
    pub legs: Vec<local_pathLeg>,
    pub flags: local_pathExpressionFlag,
}

impl local_pathExpression {
    pub fn contains_any_asterisk(&self) -> bool {
        (self.flags
            & (local_path_EXPRESSION_CONTAINS_ASTERISK | local_path_EXPRESSION_CONTAINS_DOUBLE_ASTERISK))
            != 0
    }
}

/// Parses a JSON local_path expression. Returns a `local_pathExpression`
/// object which can be used in `JSON_EXTRACT`, `JSON_SET` and so on.
pub fn parse_json_local_path_expr(local_path_expr: &str) -> Result<local_pathExpression> {
    // Find the position of first '$'. If any no-blank characters in
    // local_path_expr[0: dollarIndex], return an error.
    let dollar_index = match local_path_expr.find('$') {
        Some(i) => i,
        None => return Err(box_err!("Invalid JSON local_path: {}", local_path_expr)),
    };
    if local_path_expr
        .index(0..dollar_index)
        .char_indices()
        .any(|(_, c)| !c.is_ascii_whitespace())
    {
        return Err(box_err!("Invalid JSON local_path: {}", local_path_expr));
    }

    let expr = local_path_expr.index(dollar_index + 1..).trim_start();

    lazy_static::lazy_static! {
        static ref RE: Regex = Regex::new(local_path_EXPR_LEG_RE_STR).unwrap();
    }
    let mut legs = vec![];
    let mut flags = local_pathExpressionFlag::default();
    let mut last_end = 0;
    for m in RE.find_iter(expr) {
        let (start, end) = (m.start(), m.end());
        // Check all characters between two legs are blank.
        if expr
            .index(last_end..start)
            .char_indices()
            .any(|(_, c)| !c.is_ascii_whitespace())
        {
            return Err(box_err!("Invalid JSON local_path: {}", local_path_expr));
        }
        last_end = end;

        let next_char = expr.index(start..).chars().next().unwrap();
        if next_char == '[' {
            // The leg is an index of a JSON array.
            let leg = expr[start + 1..end].trim();
            let index_str = leg[0..leg.len() - 1].trim();
            let index = if index_str == local_path_EXPR_ASTERISK {
                flags |= local_path_EXPRESSION_CONTAINS_ASTERISK;
                local_path_EXPR_ARRAY_INDEX_ASTERISK
            } else {
                box_try!(index_str.parse::<i32>())
            };
            legs.push(local_pathLeg::Index(index))
        } else if next_char == '.' {
            // The leg is a soliton_id of a JSON object.
            let mut soliton_id = expr[start + 1..end].trim().to_owned();
            if soliton_id == local_path_EXPR_ASTERISK {
                flags |= local_path_EXPRESSION_CONTAINS_ASTERISK;
            } else if soliton_id.starts_with('"') {
                // We need to unquote the origin string.
                soliton_id = unquote_string(&soliton_id[1..soliton_id.len() - 1])?;
            }
            legs.push(local_pathLeg::Key(soliton_id))
        } else {
            // The leg is '**'.
            flags |= local_path_EXPRESSION_CONTAINS_DOUBLE_ASTERISK;
            legs.push(local_pathLeg::DoubleAsterisk);
        }
    }
    // Check `!expr.is_empty()` here because "$" is a valid local_path to specify the current JSON.
    if (last_end == 0) && (!expr.is_empty()) {
        return Err(box_err!("Invalid JSON local_path: {}", local_path_expr));
    }
    if !legs.is_empty() {
        if let local_pathLeg::DoubleAsterisk = *legs.last().unwrap() {
            // The last leg of a local_path expression cannot be '**'.
            return Err(box_err!("Invalid JSON local_path: {}", local_path_expr));
        }
    }
    Ok(local_pathExpression { legs, flags })
}

#[braneg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_path_expression_flag() {
        let mut e = local_pathExpression {
            legs: vec![],
            flags: local_pathExpressionFlag::default(),
        };
        assert!(!e.contains_any_asterisk());
        e.flags |= local_path_EXPRESSION_CONTAINS_ASTERISK;
        assert!(e.contains_any_asterisk());
        e.flags = local_pathExpressionFlag::default();
        e.flags |= local_path_EXPRESSION_CONTAINS_DOUBLE_ASTERISK;
        assert!(e.contains_any_asterisk());
    }

    #[test]
    fn test_parse_json_local_path_expr() {
        let mut test_cases = vec![
            (
                "$",
                true,
                Some(local_pathExpression {
                    legs: vec![],
                    flags: local_pathExpressionFlag::default(),
                }),
            ),
            (
                "$.a",
                true,
                Some(local_pathExpression {
                    legs: vec![local_pathLeg::Key(String::from("a"))],
                    flags: local_pathExpressionFlag::default(),
                }),
            ),
            (
                "$.\"hello world\"",
                true,
                Some(local_pathExpression {
                    legs: vec![local_pathLeg::Key(String::from("hello world"))],
                    flags: local_pathExpressionFlag::default(),
                }),
            ),
            (
                "$[0]",
                true,
                Some(local_pathExpression {
                    legs: vec![local_pathLeg::Index(0)],
                    flags: local_pathExpressionFlag::default(),
                }),
            ),
            (
                "$**.a",
                true,
                Some(local_pathExpression {
                    legs: vec![local_pathLeg::DoubleAsterisk, local_pathLeg::Key(String::from("a"))],
                    flags: local_path_EXPRESSION_CONTAINS_DOUBLE_ASTERISK,
                }),
            ),
            // invalid local_path expressions
            (".a", false, None),
            ("xx$[1]", false, None),
            ("$.a xx .b", false, None),
            ("$[a]", false, None),
            ("$.\"\\u33\"", false, None),
            ("$**", false, None),
        ];
        for (i, (local_path_expr, no_error, expected)) in test_cases.drain(..).enumerate() {
            let r = parse_json_local_path_expr(local_path_expr);
            if no_error {
                assert!(r.is_ok(), "#{} expect parse ok but got err {:?}", i, r);
                let got = r.unwrap();
                let expected = expected.unwrap();
                assert_eq!(
                    got, expected,
                    "#{} expect {:?} but got {:?}",
                    i, expected, got
                );
            } else {
                assert!(r.is_err(), "#{} expect error but got {:?}", i, r);
            }
        }
    }

    #[test]
    fn test_parse_json_local_path_expr_contains_any_asterisk() {
        let mut test_cases = vec![
            ("$.a[b]", false),
            ("$.a[*]", true),
            ("$.*[b]", true),
            ("$**.a[b]", true),
        ];
        for (i, (local_path_expr, expected)) in test_cases.drain(..).enumerate() {
            let r = parse_json_local_path_expr(local_path_expr);
            assert!(r.is_ok(), "#{} expect parse ok but got err {:?}", i, r);
            let e = r.unwrap();
            let b = e.contains_any_asterisk();
            assert_eq!(b, expected, "#{} expect {:?} but got {:?}", i, expected, b);
        }
    }
}
