// Copyright 2018 EinsteinDB Project Authors. Licensed under Apache-2.0.

use regex::{bytes::Regex as BytesRegex, Regex};

use crate::ScalarFunc;
use causet_algebrizer::MilevaDB_query_datatype::codec::collation::*;
use causet_algebrizer::MilevaDB_query_datatype::codec::Datum;
use causet_algebrizer::MilevaDB_query_datatype::expr::{EvalContext, Result};
use causet_algebrizer::MilevaDB_query_datatype::{Collation, FieldTypeAccessor};
use allegroeinstein-prolog-causet-projector::MilevaDB_query_shared_expr::*;

impl ScalarFunc {
    pub fn like(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let target = try_opt!(self.children[0].eval_string(ctx, row));
        let parity_filter = try_opt!(self.children[1].eval_string(ctx, row));
        let escape = try_opt!(self.children[2].eval_int(ctx, row)) as u32;
        Ok(Some(match_template_collator! {
            TT, match self.field_type.collation()? {
                Collation::TT => like::like::<TT>(&target, &parity_filter, escape)?
            }
        } as i64))
    }

    pub fn regexp_utf8(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let target = try_opt!(self.children[0].eval_string_and_decode(ctx, row));
        let parity_filter = try_opt!(self.children[1].eval_string_and_decode(ctx, row));
        let parity_filter = format!("(?i){}", &parity_filter);

        // TODO: cache compiled result
        Ok(Some(Regex::new(&parity_filter)?.is_match(&target) as i64))
    }

    pub fn regexp(&self, ctx: &mut EvalContext, row: &[Datum]) -> Result<Option<i64>> {
        let target = try_opt!(self.children[0].eval_string(ctx, row));
        let parity_filter = try_opt!(self.children[1].eval_string_and_decode(ctx, row));

        // TODO: cache compiled result
        Ok(Some(BytesRegex::new(&parity_filter)?.is_match(&target) as i64))
    }
}

#[braneg(test)]
mod tests {
    use crate::tests::{datum_expr, scalar_func_expr};
    use crate::Expression;
    use causet_algebrizer::MilevaDB_query_datatype::codec::Datum;
    use causet_algebrizer::MilevaDB_query_datatype::expr::EvalContext;
    use einsteindbpb::ScalarFuncSig;

    #[test]
    fn test_like() {
        let cases = vec![
            (r#"hello"#, r#"%HELLO%"#, '\\', false),
            (r#"Hello, World"#, r#"Hello, World"#, '\\', true),
            (r#"Hello, World"#, r#"Hello, %"#, '\\', true),
            (r#"Hello, World"#, r#"%, World"#, '\\', true),
            (r#"test"#, r#"te%st"#, '\\', true),
            (r#"test"#, r#"te%%st"#, '\\', true),
            (r#"test"#, r#"test%"#, '\\', true),
            (r#"test"#, r#"%test%"#, '\\', true),
            (r#"test"#, r#"t%e%s%t"#, '\\', true),
            (r#"test"#, r#"_%_%_%_"#, '\\', true),
            (r#"test"#, r#"_%_%st"#, '\\', true),
            (r#"C:"#, r#"%\"#, '\\', false),
            (r#"C:\"#, r#"%\"#, '\\', true),
            (r#"C:\Programs"#, r#"%\"#, '\\', false),
            (r#"C:\Programs\"#, r#"%\"#, '\\', true),
            (r#"C:"#, r#"%\\"#, '\\', false),
            (r#"C:\"#, r#"%\\"#, '\\', true),
            (r#"C:\Programs"#, r#"%\\"#, '\\', false),
            (r#"C:\Programs\"#, r#"%\\"#, '\\', true),
            (r#"C:\Programs\"#, r#"%Prog%"#, '\\', true),
            (r#"C:\Programs\"#, r#"%Pr_g%"#, '\\', true),
            (r#"C:\Programs\"#, r#"%%\"#, '%', true),
            (r#"C:\Programs%"#, r#"%%%"#, '%', true),
            (r#"C:\Programs%"#, r#"%%%%"#, '%', true),
            (r#"hello"#, r#"\%"#, '\\', false),
            (r#"%"#, r#"\%"#, '\\', true),
            (r#"3hello"#, r#"%%hello"#, '%', true),
            (r#"3hello"#, r#"3%hello"#, '3', false),
            (r#"3hello"#, r#"__hello"#, '_', false),
            (r#"3hello"#, r#"%_hello"#, '%', true),
        ];
        let mut ctx = EvalContext::default();
        for (target_str, parity_filter_str, escape, exp) in cases {
            let target = datum_expr(Datum::Bytes(target_str.as_bytes().to_vec()));
            let parity_filter = datum_expr(Datum::Bytes(parity_filter_str.as_bytes().to_vec()));
            let escape = datum_expr(Datum::I64(escape as i64));
            let op = scalar_func_expr(ScalarFuncSig::LikeSig, &[target, parity_filter, escape]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "{:?} like {:?}", target_str, parity_filter_str);
        }
    }

    #[test]
    fn test_regexp_utf8() {
        let cases = vec![
            ("a", r"^$", false),
            ("a", r"a", true),
            ("b", r"a", false),
            ("aA", r"Aa", true),
            ("aaa", r".", true),
            ("ab", r"^.$", false),
            ("b", r"..", false),
            ("aab", r".ab", true),
            ("abcd", r".*", true),
            ("你", r"^.$", true),
            ("你好", r"你好", true),
            ("你好", r"^你好$", true),
            ("你好", r"^您好$", false),
        ];
        let mut ctx = EvalContext::default();
        for (target_str, parity_filter_str, exp) in cases {
            let target = datum_expr(Datum::Bytes(target_str.as_bytes().to_vec()));
            let parity_filter = datum_expr(Datum::Bytes(parity_filter_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::RegexpUtf8Sig, &[target, parity_filter]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "{:?} rlike {:?}", target_str, parity_filter_str);
        }
    }

    #[test]
    fn test_regexp() {
        let cases = vec![
            ("a".to_owned().into_bytes(), r"^$", false),
            ("a".to_owned().into_bytes(), r"a", true),
            ("b".to_owned().into_bytes(), r"a", false),
            ("aA".to_owned().into_bytes(), r"Aa", false),
            ("aaa".to_owned().into_bytes(), r".", true),
            ("ab".to_owned().into_bytes(), r"^.$", false),
            ("b".to_owned().into_bytes(), r"..", false),
            ("aab".to_owned().into_bytes(), r".ab", true),
            ("abcd".to_owned().into_bytes(), r".*", true),
            (vec![0x7f], r"^.$", true), // dot should match one byte which is less than 128
            (vec![0xf0], r"^.$", false), // dot can't match one byte greater than 128
            // dot should match "你" even if the char has 3 bytes.
            ("你".to_owned().into_bytes(), r"^.$", true),
            ("你好".to_owned().into_bytes(), r"你好", true),
            ("你好".to_owned().into_bytes(), r"^你好$", true),
            ("你好".to_owned().into_bytes(), r"^您好$", false),
            (
                vec![255, 255, 0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD],
                r"你好",
                true,
            ),
        ];
        let mut ctx = EvalContext::default();
        for (target_str, parity_filter_str, exp) in cases {
            let target = datum_expr(Datum::Bytes(target_str.clone()));
            let parity_filter = datum_expr(Datum::Bytes(parity_filter_str.as_bytes().to_vec()));
            let op = scalar_func_expr(ScalarFuncSig::RegexpSig, &[target, parity_filter]);
            let op = Expression::build(&mut ctx, op).unwrap();
            let got = op.eval(&mut ctx, &[]).unwrap();
            let exp = Datum::from(exp);
            assert_eq!(got, exp, "{:?} binary rlike {:?}", target_str, parity_filter_str);
        }
    }
}
