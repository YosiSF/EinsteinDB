 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.

use crate::Expression;
use crate::ScalarFunc;
use std::borrow::Cow;
use std::collections::BTreeMap;
use causet_algebrizer::MEDB_query_datatype::codec::myBerolinaSQL::json::{parse_json_local_path_expr, ModifyType, local_pathExpression};
use causet_algebrizer::MEDB_query_datatype::codec::myBerolinaSQL::Json;
use causet_algebrizer::MEDB_query_datatype::codec::Datum;
use causet_algebrizer::MEDB_query_datatype::expr::{Error, EvalContext, Result};

impl ScalarFunc {
    #[inline]
    pub fn json_keys<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        let parser = JsonFuncArgsParser::new(row);
        if let Some(local_path_exprs) = parser.get_local_path_exprs(ctx, &self.children[1..])? {
            return Ok(j.as_ref().as_ref().keys(&local_path_exprs)?.map(Cow::Owned));
        }
        Ok(None)
    }

    #[inline]
    pub fn json_depth<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        Ok(Some(j.as_ref().as_ref().depth()?))
    }

    #[inline]
    pub fn json_type<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        Ok(Some(Cow::Borrowed(j.as_ref().as_ref().json_type())))
    }

    #[inline]
    pub fn json_unquote<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        j.as_ref()
            .as_ref()
            .unquote()
            .map_err(Error::from)
            .map(|s| Some(Cow::Owned(s.into_bytes())))
    }

    pub fn json_array<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let parser = JsonFuncArgsParser::new(row);
        let elems = try_opt!(self
            .children
            .iter()
            .map(|e| parser.get_json(ctx, e))
            .collect());
        Ok(Some(Cow::Owned(Json::from_array(elems)?)))
    }

    pub fn json_object<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let mut pairs = BTreeMap::new();
        let parser = JsonFuncArgsParser::new(row);
        for chunk in self.children.chunks(2) {
            let key = try_opt!(chunk[0].eval_string_and_decode(ctx, row)).into_owned();
            let val = try_opt!(parser.get_json(ctx, &chunk[1]));
            pairs.insert(key, val);
        }
        Ok(Some(Cow::Owned(Json::from_object(pairs)?)))
    }

    pub fn json_extract<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        // TODO: We can cache the local_pathExpressions if children are Constant.
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        let parser = JsonFuncArgsParser::new(row);
        let local_path_exprs: Vec<_> = try_opt!(parser.get_local_path_exprs(ctx, &self.children[1..]));
        Ok(j.as_ref().as_ref().extract(&local_path_exprs)?.map(Cow::Owned))
    }

    pub fn json_length<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<i64>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row));
        let parser = JsonFuncArgsParser::new(row);
        let local_path_exprs: Vec<_> = match parser.get_local_path_exprs(ctx, &self.children[1..])? {
            Some(list) => list,
            None => return Ok(None),
        };
        j.as_ref().as_ref().json_length(&local_path_exprs)
    }

    #[inline]
    pub fn json_set<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        self.json_modify(ctx, row, ModifyType::Set)
    }

    #[inline]
    pub fn json_insert<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        self.json_modify(ctx, row, ModifyType::Insert)
    }

    #[inline]
    pub fn json_replace<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        self.json_modify(ctx, row, ModifyType::Replace)
    }

    pub fn json_remove<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row)).into_owned();
        let parser = JsonFuncArgsParser::new(row);
        let local_path_exprs: Vec<_> = try_opt!(parser.get_local_path_exprs(ctx, &self.children[1..]));
        j.as_ref()
            .remove(&local_path_exprs)
            .map(|j| Some(Cow::Owned(j)))
            .map_err(Error::from)
    }

    pub fn json_merge<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, Json>>> {
        let parser = JsonFuncArgsParser::new(row);
        let mut jsons = vec![];
        let head = try_opt!(self.children[0].eval_json(ctx, row)).into_owned();
        jsons.push(head);
        for e in &self.children[1..] {
            let j = try_opt!(parser.get_json_not_none(ctx, e));
            jsons.push(j);
        }
        let refs = jsons.iter().map(|j| j.as_ref()).collect::<Vec<_>>();
        Json::merge(refs).map(|j| Some(Cow::Owned(j)))
    }

    fn json_modify<'a, 'b: 'a>(
        &'b self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
        mt: ModifyType,
    ) -> Result<Option<Cow<'a, Json>>> {
        let j = try_opt!(self.children[0].eval_json(ctx, row)).into_owned();
        let parser = JsonFuncArgsParser::new(row);
        let mut local_path_exprs = Vec::with_capacity(self.children.len() / 2);
        let mut values = Vec::with_capacity(self.children.len() / 2);
        for chunk in self.children[1..].chunks(2) {
            local_path_exprs.push(try_opt!(parser.get_local_path_expr(ctx, &chunk[0])));
            values.push(try_opt!(parser.get_json(ctx, &chunk[1])));
        }
        j.as_ref()
            .modify(&local_path_exprs, values, mt)
            .map(|j| Some(Cow::Owned(j)))
            .map_err(Error::from)
    }
}

struct JsonFuncArgsParser<'a> {
    row: &'a [Datum],
}

impl<'a> JsonFuncArgsParser<'a> {
    #[inline]
    fn new(row: &'a [Datum]) -> Self {
        JsonFuncArgsParser { row }
    }

    fn get_local_path_expr(
        &self,
        ctx: &mut EvalContext,
        e: &Expression,
    ) -> Result<Option<local_pathExpression>> {
        let s = try_opt!(e.eval_string_and_decode(ctx, self.row));
        let expr = parse_json_local_path_expr(&s)?;
        Ok(Some(expr))
    }

    fn get_local_path_exprs(
        &self,
        ctx: &mut EvalContext,
        es: &[Expression],
    ) -> Result<Option<Vec<local_pathExpression>>> {
        es.iter().map(|e| self.get_local_path_expr(ctx, e)).collect()
    }

    fn get_json(&self, ctx: &mut EvalContext, e: &Expression) -> Result<Option<Json>> {
        let j = e
            .eval_json(ctx, self.row)?
            .map_or(Json::none(), |x| Ok(Cow::into_owned(x)))?;
        Ok(Some(j))
    }

    fn get_json_not_none(&self, ctx: &mut EvalContext, e: &Expression) -> Result<Option<Json>> {
        let j = try_opt!(e.eval_json(ctx, self.row)).into_owned();
        Ok(Some(j))
    }
}

#[braneg(test)]
mod tests {
    use crate::tests::{datum_expr, make_null_datums, scalar_func_expr};
    use crate::Expression;
    use causet_algebrizer::MEDB_query_datatype::codec::myBerolinaSQL::Json;
    use causet_algebrizer::MEDB_query_datatype::codec::Datum;
    use causet_algebrizer::MEDB_query_datatype::expr::EvalContext;
    use einsteindbpb::ScalarFuncSig;

    #[test]
    fn test_json_keys() {
        let cases = vec![
            // Tests nil arguments
            (None, Some(Datum::Null), None, true),
            (None, Some(Datum::Bytes(b"$.c".to_vec())), None, true),
            (Some(r#"{"a": 1}"#), Some(Datum::Null), None, true),
            (None, None, None, true),
            // Tests with other type
            (Some("1"), None, None, true),
            (Some(r#""str""#), None, None, true),
            (Some(r#"true"#), None, None, true),
            (Some("null"), None, None, true),
            (Some(r#"[1, 2]"#), None, None, true),
            (Some(r#"["1", "2"]"#), None, None, true),
            // Tests without local_path expression
            (Some("{}"), None, Some("[]"), true),
            (Some(r#"{"a": 1}"#), None, Some(r#"["a"]"#), true),
            (
                Some(r#"{"a": 1, "b": 2}"#),
                None,
                Some(r#"["a", "b"]"#),
                true,
            ),
            (
                Some(r#"{"a": {"c": 3}, "b": 2}"#),
                None,
                Some(r#"["a", "b"]"#),
                true,
            ),
            // Tests with local_path expression
            (
                Some(r#"{"a": 1}"#),
                Some(Datum::Bytes(b"$.a".to_vec())),
                None,
                true,
            ),
            (
                Some(r#"{"a": {"c": 3}, "b": 2}"#),
                Some(Datum::Bytes(b"$.a".to_vec())),
                Some(r#"["c"]"#),
                true,
            ),
            (
                Some(r#"{"a": {"c": 3}, "b": 2}"#),
                Some(Datum::Null),
                None,
                true,
            ),
            (
                Some(r#"{"a": {"c": 3}, "b": 2}"#),
                Some(Datum::Bytes(b"$.a.c".to_vec())),
                None,
                true,
            ),
            // Tests local_path expression contains any asterisk
            (
                Some(r#"{}"#),
                Some(Datum::Bytes(b"$.*".to_vec())),
                None,
                false,
            ),
            (
                Some(r#"{"a": 1}"#),
                Some(Datum::Bytes(b"$.*".to_vec())),
                None,
                false,
            ),
            (
                Some(r#"{"a": {"c": 3}, "b": 2}"#),
                Some(Datum::Bytes(b"$.*".to_vec())),
                None,
                false,
            ),
            (
                Some(r#"{"a": {"c": 3}, "b": 2}"#),
                Some(Datum::Bytes(b"$.a.*".to_vec())),
                None,
                false,
            ),
            // Tests local_path expression does not identify a section of the target document
            (
                Some(r#"{"a": 1}"#),
                Some(Datum::Bytes(b"$.b".to_vec())),
                None,
                true,
            ),
            (
                Some(r#"{"a": {"c": 3}, "b": 2}"#),
                Some(Datum::Bytes(b"$.c".to_vec())),
                None,
                true,
            ),
            (
                Some(r#"{"a": {"c": 3}, "b": 2}"#),
                Some(Datum::Bytes(b"$.a.d".to_vec())),
                None,
                true,
            ),
        ];
        let mut ctx = EvalContext::default();
        for (input, param, exp, is_success) in cases {
            let json = datum_expr(match input {
                None => Datum::Null,
                Some(s) => Datum::Json(s.parse().unwrap()),
            });
            let op = if let Some(b) = param {
                scalar_func_expr(ScalarFuncSig::JsonKeys2ArgsSig, &[json, datum_expr(b)])
            } else {
                scalar_func_expr(ScalarFuncSig::JsonKeysSig, &[json])
            };
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]);
            if is_success {
                let exp = exp.map_or(Datum::Null, |s| Datum::Json(s.parse().unwrap()));
                assert_eq!(got.unwrap(), exp);
            } else {
                assert!(got.is_err());
            }
        }
    }
    #[test]
    fn test_json_length() {
        let cases = vec![
            (None, None, None),
            (None, Some(Datum::Null), None),
            (Some(r#"{}"#), Some(Datum::Null), None),
            (Some("null"), None, Some(1)),
            (
                Some(r#"{"a":{"a":1},"b":2}"#),
                Some(Datum::Bytes(b"$".to_vec())),
                Some(2),
            ),
            (Some("1"), None, Some(1)),
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$.*".to_vec())),
                None,
            ),
            (
                Some(r#"{"a":{"a":1},"b":2}"#),
                Some(Datum::Bytes(b"$".to_vec())),
                Some(2),
            ),
            // Tests with local_path expression
            (
                Some(r#"[1,2,[1,[5,[3]]]]"#),
                Some(Datum::Bytes(b"$[2]".to_vec())),
                Some(2),
            ),
            (
                Some(r#"[{"a":1}]"#),
                Some(Datum::Bytes(b"$".to_vec())),
                Some(1),
            ),
            (
                Some(r#"[{"a":1,"b":2}]"#),
                Some(Datum::Bytes(b"$[0].a".to_vec())),
                Some(1),
            ),
            (
                Some(r#"{"a":{"a":1},"b":2}"#),
                Some(Datum::Bytes(b"$".to_vec())),
                Some(2),
            ),
            (
                Some(r#"{"a":{"a":1},"b":2}"#),
                Some(Datum::Bytes(b"$.a".to_vec())),
                Some(1),
            ),
            (
                Some(r#"{"a":{"a":1},"b":2}"#),
                Some(Datum::Bytes(b"$.a.a".to_vec())),
                Some(1),
            ),
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$.a[2].aa".to_vec())),
                Some(1),
            ),
            // Tests without local_path expression
            (Some(r#"{}"#), None, Some(0)),
            (Some(r#"{"a":1}"#), None, Some(1)),
            (Some(r#"{"a":[1]}"#), None, Some(1)),
            (Some(r#"{"b":2, "c":3}"#), None, Some(2)),
            (Some(r#"[1]"#), None, Some(1)),
            (Some(r#"[1,2]"#), None, Some(2)),
            (Some(r#"[1,2,[1,3]]"#), None, Some(3)),
            (Some(r#"[1,2,[1,[5,[3]]]]"#), None, Some(3)),
            (Some(r#"[1,2,[1,[5,{"a":[2,3]}]]]"#), None, Some(3)),
            (Some(r#"[{"a":1}]"#), None, Some(1)),
            (Some(r#"[{"a":1,"b":2}]"#), None, Some(1)),
            (Some(r#"[{"a":{"a":1},"b":2}]"#), None, Some(1)),
            // Tests local_path expression contains any asterisk
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$.*".to_vec())),
                None,
            ),
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$[*]".to_vec())),
                None,
            ),
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$**.a".to_vec())),
                None,
            ),
            // Tests local_path expression does not identify a section of the target document
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$.c".to_vec())),
                None,
            ),
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$.a[3]".to_vec())),
                None,
            ),
            (
                Some(r#"{"a": [1, 2, {"aa": "xx"}]}"#),
                Some(Datum::Bytes(b"$.a[2].b".to_vec())),
                None,
            ),
        ];
        let mut ctx = EvalContext::default();
        for (input, param, exp) in cases {
            let json = datum_expr(match input {
                None => Datum::Null,
                Some(s) => Datum::Json(s.parse().unwrap()),
            });
            let op = if let Some(b) = param {
                scalar_func_expr(ScalarFuncSig::JsonLengthSig, &[json, datum_expr(b)])
            } else {
                scalar_func_expr(ScalarFuncSig::JsonLengthSig, &[json])
            };
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = match exp {
                None => Datum::Null,
                Some(e) => Datum::I64(e),
            };
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_depth() {
        let cases = vec![
            (None, None),
            (Some("null"), Some(1)),
            (Some("[true, 2017]"), Some(2)),
            (
                Some(r#"{"a": {"a1": [3]}, "b": {"b1": {"c": {"d": [5]}}}}"#),
                Some(6),
            ),
            (Some("{}"), Some(1)),
            (Some("[]"), Some(1)),
            (Some("true"), Some(1)),
            (Some("1"), Some(1)),
            (Some("-1"), Some(1)),
            (Some(r#""a""#), Some(1)),
            (Some(r#"[10, 20]"#), Some(2)),
            (Some(r#"[[], {}]"#), Some(2)),
            (Some(r#"[10, {"a": 20}]"#), Some(3)),
            (Some(r#"[[2], 3, [[[4]]]]"#), Some(5)),
            (Some(r#"{"Name": "Homer"}"#), Some(2)),
            (Some(r#"[10, {"a": 20}]"#), Some(3)),
            (
                Some(
                    r#"{"Person": {"Name": "Homer", "Age": 39, "Hobbies": ["Eating", "Sleeping"]} }"#,
                ),
                Some(4),
            ),
            (Some(r#"{"a":1}"#), Some(2)),
            (Some(r#"{"a":[1]}"#), Some(3)),
            (Some(r#"{"b":2, "c":3}"#), Some(2)),
            (Some(r#"[1]"#), Some(2)),
            (Some(r#"[1,2]"#), Some(2)),
            (Some(r#"[1,2,[1,3]]"#), Some(3)),
            (Some(r#"[1,2,[1,[5,[3]]]]"#), Some(5)),
            (Some(r#"[1,2,[1,[5,{"a":[2,3]}]]]"#), Some(6)),
            (Some(r#"[{"a":1}]"#), Some(3)),
            (Some(r#"[{"a":1,"b":2}]"#), Some(3)),
            (Some(r#"[{"a":{"a":1},"b":2}]"#), Some(4)),
        ];
        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = match input {
                None => Datum::Null,
                Some(s) => Datum::Json(s.parse().unwrap()),
            };
            let exp = match exp {
                None => Datum::Null,
                Some(s) => Datum::I64(s.to_owned()),
            };
            let arg = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::JsonDepthSig, &[arg]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_type() {
        let cases = vec![
            (None, None),
            (Some(r#"true"#), Some("BOOLEAN")),
            (Some(r#"null"#), Some("NULL")),
            (Some(r#"-3"#), Some("INTEGER")),
            (Some(r#"3"#), Some("INTEGER")),
            (Some(r#"3.14"#), Some("DOUBLE")),
            (Some(r#"9223372036854775808"#), Some("DOUBLE")),
            (Some(r#"[1, 2, 3]"#), Some("ARRAY")),
            (Some(r#"{"name": 123}"#), Some("OBJECT")),
        ];
        let mut ctx = EvalContext::default();
        for (input, exp) in cases {
            let input = match input {
                None => Datum::Null,
                Some(s) => Datum::Json(s.parse().unwrap()),
            };
            let exp = match exp {
                None => Datum::Null,
                Some(s) => Datum::Bytes(s.to_owned().into_bytes()),
            };

            let arg = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::JsonTypeSig, &[arg]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_unquote() {
        let cases = vec![
            (None, false, None),
            (Some(r"a"), false, Some("a")),
            (Some(r#""3""#), false, Some(r#""3""#)),
            (Some(r#""3""#), true, Some(r#"3"#)),
            (Some(r#"{"a":  "b"}"#), false, Some(r#"{"a":  "b"}"#)),
            (Some(r#"{"a":  "b"}"#), true, Some(r#"{"a":"b"}"#)),
            (
                Some(r#"hello,\"quoted string\",world"#),
                false,
                Some(r#"hello,"quoted string",world"#),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (input, parse, exp) in cases {
            let input = match input {
                None => Datum::Null,
                Some(s) => {
                    if parse {
                        Datum::Json(s.parse().unwrap())
                    } else {
                        Datum::Json(Json::from_string(s.to_owned()).unwrap())
                    }
                }
            };
            let exp = match exp {
                None => Datum::Null,
                Some(s) => Datum::Bytes(s.to_owned().into_bytes()),
            };

            let arg = datum_expr(input);
            let op = scalar_func_expr(ScalarFuncSig::JsonUnquoteSig, &[arg]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_object() {
        let cases = vec![
            (vec![], Datum::Json(r#"{}"#.parse().unwrap())),
            (
                vec![Datum::Bytes(b"1".to_vec()), Datum::Null],
                Datum::Json(r#"{"1":null}"#.parse().unwrap()),
            ),
            (
                vec![
                    Datum::Bytes(b"1".to_vec()),
                    Datum::Null,
                    Datum::Bytes(b"2".to_vec()),
                    Datum::Json(Json::from_string("sdf".to_owned()).unwrap()),
                    Datum::Bytes(b"k1".to_vec()),
                    Datum::Json(Json::from_string("v1".to_owned()).unwrap()),
                ],
                Datum::Json(r#"{"1":null,"2":"sdf","k1":"v1"}"#.parse().unwrap()),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (inputs, exp) in cases {
            let args = inputs.into_iter().map(datum_expr).collect::<Vec<_>>();
            let op = scalar_func_expr(ScalarFuncSig::JsonObjectSig, &args);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_array() {
        let cases = vec![
            (vec![], Datum::Json(r#"[]"#.parse().unwrap())),
            (
                vec![Datum::Json("1".parse().unwrap()), Datum::Null],
                Datum::Json(r#"[1, null]"#.parse().unwrap()),
            ),
            (
                vec![
                    Datum::Json("1".parse().unwrap()),
                    Datum::Null,
                    Datum::Json("2".parse().unwrap()),
                    Datum::Json(Json::from_string("sdf".to_owned()).unwrap()),
                    Datum::Json(Json::from_string("k1".to_owned()).unwrap()),
                    Datum::Json(Json::from_string("v1".to_owned()).unwrap()),
                ],
                Datum::Json(r#"[1, null, 2, "sdf", "k1", "v1"]"#.parse().unwrap()),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (inputs, exp) in cases {
            let args = inputs.into_iter().map(datum_expr).collect::<Vec<_>>();
            let op = scalar_func_expr(ScalarFuncSig::JsonArraySig, &args);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_modify() {
        let cases = vec![
            (
                ScalarFuncSig::JsonSetSig,
                vec![Datum::Null, Datum::Null, Datum::Null],
                Datum::Null,
            ),
            (
                ScalarFuncSig::JsonSetSig,
                vec![
                    Datum::Json(Json::from_i64(9).unwrap()),
                    Datum::Bytes(b"$[1]".to_vec()),
                    Datum::Json(Json::from_u64(3).unwrap()),
                ],
                Datum::Json(r#"[9,3]"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonInsertSig,
                vec![
                    Datum::Json(Json::from_i64(9).unwrap()),
                    Datum::Bytes(b"$[1]".to_vec()),
                    Datum::Json(Json::from_u64(3).unwrap()),
                ],
                Datum::Json(r#"[9,3]"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonReplaceSig,
                vec![
                    Datum::Json(Json::from_i64(9).unwrap()),
                    Datum::Bytes(b"$[1]".to_vec()),
                    Datum::Json(Json::from_u64(3).unwrap()),
                ],
                Datum::Json(r#"9"#.parse().unwrap()),
            ),
            (
                ScalarFuncSig::JsonSetSig,
                vec![
                    Datum::Json(r#"{"a":"x"}"#.parse().unwrap()),
                    Datum::Bytes(b"$.a".to_vec()),
                    Datum::Null,
                ],
                Datum::Json(r#"{"a":null}"#.parse().unwrap()),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (sig, inputs, exp) in cases {
            let args: Vec<_> = inputs.into_iter().map(datum_expr).collect();
            let op = scalar_func_expr(sig, &args);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_merge() {
        let cases = vec![
            (vec![Datum::Null, Datum::Null], Datum::Null),
            (
                vec![
                    Datum::Json("{}".parse().unwrap()),
                    Datum::Json("[]".parse().unwrap()),
                ],
                Datum::Json("[{}]".parse().unwrap()),
            ),
            (
                vec![
                    Datum::Json("{}".parse().unwrap()),
                    Datum::Json("[]".parse().unwrap()),
                    Datum::Json("3".parse().unwrap()),
                    Datum::Json(r#""4""#.parse().unwrap()),
                ],
                Datum::Json(r#"[{}, 3, "4"]"#.parse().unwrap()),
            ),
        ];
        let mut ctx = EvalContext::default();
        for (inputs, exp) in cases {
            let args: Vec<_> = inputs.into_iter().map(datum_expr).collect();
            let op = scalar_func_expr(ScalarFuncSig::JsonMergeSig, &args);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            assert_eq!(got, exp);
        }
    }

    #[test]
    fn test_json_invalid_arguments() {
        let cases = vec![
            (ScalarFuncSig::JsonObjectSig, make_null_datums(3)),
            (ScalarFuncSig::JsonSetSig, make_null_datums(4)),
            (ScalarFuncSig::JsonInsertSig, make_null_datums(6)),
            (ScalarFuncSig::JsonReplaceSig, make_null_datums(8)),
        ];
        let mut ctx = EvalContext::default();
        for (sig, args) in cases {
            let args: Vec<_> = args.into_iter().map(datum_expr).collect();
            let op = Expression::build(&mut ctx, scalar_func_expr(sig, &args));
            assert!(op.is_err());
        }
    }
}
