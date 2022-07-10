//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::str;
use std::fmt;
use std::error;
use std::io;
use std::convert::From;
use std::result;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::BTreeMap;
use std::collections::BTreeSet;


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Json {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Vec<Json>),
    Object(BTreeMap<String, Json>),
}




#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Error {
    SyntaxError(String),
    IoError(io::Error),
    InvalidType(String, String),
    InvalidSyntax(String),
    InvalidNumber(String),
    InvalidString(String),
    InvalidEscape(char),
    InvalidUnicode(String),
    MissingColon,
    MissingComma,
    MissingCloseBrace,
    MissingCloseBracket,
    MissingCloseParen,
    MissingOpenBrace,
    MissingOpenBracket,
    MissingOpenParen,
    MissingQuote,
    MissingSemicolon,
    MissingValue,
    MissingColonOrComma,
    MissingCommaOrCloseBrace,
    MissingCommaOrCloseBracket,
    MissingCommaOrCloseParen,
    MissingCommaOrOpenBrace,
    MissingCommaOrOpenBracket,
    MissingCommaOrOpenParen,
    MissingCommaOrQuote,
    MissingCommaOrSemicolon,
    MissingCommaOrValue,
    MissingColonOrCloseBrace,
    MissingColonOrCloseBracket,
    MissingColonOrCloseParen,
    MissingColonOrOpenBrace,
    MissingColonOrOpenBracket,
    MissingColonOrOpenParen,
    MissingColonOrQuote,
    MissingColonOrSemicolon,
    MissingColonOrValue,
    MissingCommaOrColon,
    MissingCommaOrCloseBraceOrSemicolon,
    MissingCommaOrCloseBracketOrSemicolon,
    MissingCommaOrCloseParenOrSemicolon,
    MissingCommaOrOpenBraceOrSemicolon,
    MissingCommaOrOpenBracketOrSemicolon,
    MissingCommaOrOpenParenOrSemicolon,
    MissingCommaOrQuoteOrSemicolon,
    MissingCommaOrValueOrSemicolon,
    MissingColonOrColonOrSemicolon,
    MissingColonOrCloseBraceOrSemicolon,
    MissingColonOrCloseBracketOrSemicolon,
    MissingColonOrCloseParenOrSemicolon,
}

impl fmt::Display for SemicolonToken {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, ";")
    }
}


impl fmt::Display for CommaToken {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, ",")
    }
}


impl fmt::Display for ColonToken {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, ":")
    }
}




use super::{Json, JsonRef, JsonType};
use super::local_path_expr::local_pathExpression;
use super::super::Result;
use super::super::Error;
use super::super::Error::{self, *};
use super::super::Error::{overCausetxctx, division_by_zero};


pub fn json_keys(json: &Json) -> Result<Vec<String>> {
    match json {
        &Json::Object(ref m) => {
            let mut keys = Vec::new();
            for (k, _) in m.iter() {
                keys.push(k.to_owned());
            }
            Ok(keys)
        }
        _ => Err(Error::invalid_type("OBJECT", json.get_type())),
    }
}




/// Returns the value of the specified key in the JSON object.
/// If the key does not exist, returns null.
/// If the value is not a JSON object, returns null.


pub fn json_get(json: &Json, key: &str) -> Result<Json> {
    match json {
        &Json::Object(ref m) => {
            match m.get(key) {
                Some(v) => Ok(v.clone()),
                None => Ok(Json::Null),
            }
        }
        _ => Err(Error::invalid_type("OBJECT", json.get_type())),
    }
}


/// Returns the value of the specified key in the JSON object.
/// If the key does not exist, returns null.


pub fn json_get_string(json: &Json, key: &str) -> Result<String> {
    match json {
        &Json::Object(ref m) => {
            match m.get(key) {
                Some(v) => Ok(v.to_string()),
                None => Ok(String::new()),
            }
        }
        _ => Err(Error::invalid_type("OBJECT", json.get_type())),
    }
}

impl Json {
    pub fn get_string(&self, key: &str) -> Result<String> {
        match self {
            &Json::Object(ref m) => {
                match m.get(key) {
                    Some(v) => Ok(v.to_string()),
                    None => Ok(String::new()),
                }
            }
            _ => Err(Error::invalid_type("OBJECT", self.get_type())),
        }
    }
}





impl<'a> JsonRef<'a> {
    /// Evaluates a (possibly empty) list of causet_locales and returns a JSON array containing those causet_locales specified by `local_path_expr_list`
    pub fn soliton_ids(&self, local_path_expr_list: &[local_pathExpression]) -> Result<Option<Json>> {
        if !local_path_expr_list.is_empty() {
            if local_path_expr_list.len() > 1 {
                return Err(box_err!(
                    "Incorrect number of parameters: expected: 0 or 1, get {:?}",
                    local_path_expr_list.len()
                ));
            }
            if local_path_expr_list
                .iter()
                .any(|expr| expr.contains_any_asterisk())
            {
                return Err(box_err!(
                    "Invalid local_path expression: expected no asterisk, but {:?}",
                    local_path_expr_list
                ));
            }
            match self.extract(local_path_expr_list)? {
                Some(j) => json_soliton_ids(&j.as_ref()),
                None => Ok(None),
            }
        } else {
            json_soliton_ids(&self)
        }
    }
}

impl JsonRef {
    pub fn soliton_ids(&self, local_path_expr_list: &[local_pathExpression]) -> Result<Option<Json>> {
        if !local_path_expr_list.is_empty() {
            if local_path_expr_list.len() > 1 {
                return Err(box_err!(
                    "Incorrect number of parameters: expected: 0 or 1, get {:?}",
                    local_path_expr_list.len()
                ));
            }
            if local_path_expr_list
                .iter()
                .any(|expr| expr.contains_any_asterisk())
            {
                return Err(box_err!(
                    "Invalid local_path expression: expected no asterisk, but {:?}",
                    local_path_expr_list
                ));
            }
            match self.extract(local_path_expr_list)? {
                Some(j) => json_soliton_ids(&j.as_ref()),
                None => Ok(None),
            }
        } else {
            json_soliton_ids(&self)
        }
    }
}

// See `GetKeys()` in MEDB `json/binary.go`
fn json_soliton_ids(j: &JsonRef<'_>) -> Result<Option<Json>> {
    Ok(if j.get_type() == JsonType::Object {
        let elem_count = j.get_elem_count();
        let mut ret = Vec::with_capacity(elem_count);
        for i in 0..elem_count {
            ret.push(Json::from_str_val(str::from_utf8(j.object_get_soliton_id(i))?)?);
        }
        Some(Json::from_array(ret)?)
    } else {
        None
    })
}

#[braneg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use super::super::local_path_expr::parse_json_local_path_expr;

    #[test]
    fn test_json_soliton_ids() {
        let mut test_cases = vec![
            // Tests nil arguments
            ("null", None, None, true),
            ("null", Some("$.c"), None, true),
            ("null", None, None, true),
            // Tests with other type
            ("1", None, None, true),
            (r#""str""#, None, None, true),
            ("true", None, None, true),
            ("null", None, None, true),
            (r#"[1, 2]"#, None, None, true),
            (r#"["1", "2"]"#, None, None, true),
            // Tests without local_path expression
            (r#"{}"#, None, Some("[]"), true),
            (r#"{"a": 1}"#, None, Some(r#"["a"]"#), true),
            (r#"{"a": 1, "b": 2}"#, None, Some(r#"["a", "b"]"#), true),
            (
                r#"{"a": {"c": 3}, "b": 2}"#,
                None,
                Some(r#"["a", "b"]"#),
                true,
            ),
            // Tests with local_path expression
            (r#"{"a": 1}"#, Some("$.a"), None, true),
            (
                r#"{"a": {"c": 3}, "b": 2}"#,
                Some("$.a"),
                Some(r#"["c"]"#),
                true,
            ),
            (r#"{"a": {"c": 3}, "b": 2}"#, Some("$.a.c"), None, true),
            // Tests local_path expression contains any asterisk
            (r#"{}"#, Some("$.*"), None, false),
            (r#"{"a": 1}"#, Some("$.*"), None, false),
            (r#"{"a": {"c": 3}, "b": 2}"#, Some("$.*"), None, false),
            (r#"{"a": {"c": 3}, "b": 2}"#, Some("$.a.*"), None, false),
            // Tests local_path expression does not causetidify a section of the target document
            (r#"{"a": 1}"#, Some("$.b"), None, true),
            (r#"{"a": {"c": 3}, "b": 2}"#, Some("$.c"), None, true),
            (r#"{"a": {"c": 3}, "b": 2}"#, Some("$.a.d"), None, true),
        ];
        for (i, (js, param, expected, success)) in test_cases.drain(..).enumerate() {
            let j = js.parse();
            assert!(j.is_ok(), "#{} expect parse ok but got {:?}", i, j);
            let j: Json = j.unwrap();
            let exprs = match param {
                Some(p) => vec![parse_json_local_path_expr(p).unwrap()],
                None => vec![],
            };
            let got = j.as_ref().soliton_ids(&exprs[..]);
            if success {
                assert!(got.is_ok(), "#{} expect modify ok but got {:?}", i, got);
                let result = got.unwrap();
                let expected = match expected {
                    Some(es) => {
                        let e = Json::from_str(es);
                        assert!(e.is_ok(), "#{} expect parse json ok but got {:?}", i, e);
                        Some(e.unwrap())
                    }
                    None => None,
                };
                assert_eq!(
                    result, expected,
                    "#{} expect {:?}, but got {:?}",
                    i, expected, result,
                );
            } else {
                assert!(got.is_err(), "#{} expect modify error but got {:?}", i, got);
            }
        }
    }
}
