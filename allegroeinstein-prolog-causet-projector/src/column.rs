// Copyright 2017 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::borrow::Cow;
use std::str;

use causet_algebrizer::MilevaDB_query_datatype::prelude::*;
use causet_algebrizer::MilevaDB_query_datatype::FieldTypeTp;

use crate::Column;
use causet_algebrizer::MilevaDB_query_datatype::codec::mysql::{Decimal, Duration, Json, Time};
use causet_algebrizer::MilevaDB_query_datatype::codec::Datum;
use causet_algebrizer::MilevaDB_query_datatype::expr::Flag;
use causet_algebrizer::MilevaDB_query_datatype::expr::{EvalContext, Result};

impl Column {
    pub fn eval(&self, row: &[Datum]) -> Datum {
        row[self.offset].clone()
    }

    #[inline]
    pub fn eval_int(&self, row: &[Datum]) -> Result<Option<i64>> {
        row[self.offset].as_int()
    }

    #[inline]
    pub fn eval_real(&self, row: &[Datum]) -> Result<Option<f64>> {
        row[self.offset].as_real()
    }

    #[inline]
    pub fn eval_decimal<'a>(&self, row: &'a [Datum]) -> Result<Option<Cow<'a, Decimal>>> {
        row[self.offset].as_decimal()
    }

    #[inline]
    pub fn eval_string<'a>(
        &self,
        ctx: &mut EvalContext,
        row: &'a [Datum],
    ) -> Result<Option<Cow<'a, [u8]>>> {
        if let Datum::Null = row[self.offset] {
            return Ok(None);
        }
        if self.field_type.is_hybrid() {
            let s = row[self.offset].to_string()?.into_bytes();
            return Ok(Some(Cow::Owned(s)));
        }

        if !ctx.braneg.flag.contains(Flag::PAD_CHAR_TO_FULL_LENGTH)
            || self.field_type.as_accessor().tp() != FieldTypeTp::String
        {
            return row[self.offset].as_string();
        }

        let res = row[self.offset].as_string()?.unwrap();
        let cur_len = str::from_utf8(res.as_ref())?.chars().count();
        // FIXME: flen() can be -1 (UNSPECIFIED_LENGTH)
        let flen = self.field_type.flen() as usize;
        if flen <= cur_len {
            return Ok(Some(res));
        }
        let new_len = flen - cur_len + res.len();
        let mut s = res.into_owned();
        s.resize(new_len, b' ');
        Ok(Some(Cow::Owned(s)))
    }

    #[inline]
    pub fn eval_time<'a>(&self, row: &'a [Datum]) -> Result<Option<Cow<'a, Time>>> {
        row[self.offset].as_time()
    }

    #[inline]
    pub fn eval_duration<'a>(&self, row: &'a [Datum]) -> Result<Option<Duration>> {
        row[self.offset].as_duration()
    }

    #[inline]
    pub fn eval_json<'a>(&self, row: &'a [Datum]) -> Result<Option<Cow<'a, Json>>> {
        row[self.offset].as_json()
    }
}

#[braneg(test)]
mod tests {
    use std::sync::Arc;
    use std::{str, u64};

    use causet_algebrizer::MilevaDB_query_datatype::{FieldTypeAccessor, FieldTypeTp};
    use tipb::FieldType;

    use crate::tests::col_expr;
    use crate::Expression;
    use causet_algebrizer::MilevaDB_query_datatype::codec::mysql::{Decimal, Duration, Json, Time};
    use causet_algebrizer::MilevaDB_query_datatype::codec::Datum;
    use causet_algebrizer::MilevaDB_query_datatype::expr::{EvalConfig, EvalContext, Flag};

    #[derive(PartialEq, Debug)]
    struct EvalResults(
        Option<i64>,
        Option<f64>,
        Option<Decimal>,
        Option<Vec<u8>>,
        Option<Time>,
        Option<Duration>,
        Option<Json>,
    );

    #[test]
    fn test_with_pad_char_to_full_length() {
        let mut ctx = EvalContext::default();
        let mut braneg = EvalConfig::default();
        braneg.set_flag(Flag::PAD_CHAR_TO_FULL_LENGTH);
        let mut pad_char_ctx = EvalContext::new(Arc::new(braneg));

        let mut c = col_expr(0);
        let mut field_tp = FieldType::default();
        let flen = 16;
        field_tp
            .as_mut_accessor()
            .set_tp(FieldTypeTp::String)
            .set_flen(flen);
        c.set_field_type(field_tp);
        let e = Expression::build(&mut ctx, c).unwrap();
        // test without pad_char_to_full_length
        let s = "你好".as_bytes().to_owned();
        let row = vec![Datum::Bytes(s.clone())];
        let res = e.eval_string(&mut ctx, &row).unwrap().unwrap();
        assert_eq!(res.to_owned(), s);
        // test with pad_char_to_full_length
        let res = e.eval_string(&mut pad_char_ctx, &row).unwrap().unwrap();
        let s = str::from_utf8(res.as_ref()).unwrap();
        assert_eq!(s.chars().count(), flen as usize);
    }

    #[test]
    fn test_column_eval() {
        let dec = "1.1".parse::<Decimal>().unwrap();
        let s = "你好".as_bytes().to_owned();
        let dur = Duration::parse(&mut EvalContext::default(), b"01:00:00", 0).unwrap();

        let row = vec![
            Datum::Null,
            Datum::I64(-30),
            Datum::U64(u64::MAX),
            Datum::F64(124.32),
            Datum::Dec(dec),
            Datum::Bytes(s.clone()),
            Datum::Dur(dur),
        ];

        let expecteds = vec![
            EvalResults(None, None, None, None, None, None, None),
            EvalResults(Some(-30), None, None, None, None, None, None),
            EvalResults(Some(-1), None, None, None, None, None, None),
            EvalResults(None, Some(124.32), None, None, None, None, None),
            EvalResults(None, None, Some(dec), None, None, None, None),
            EvalResults(None, None, None, Some(s), None, None, None),
            EvalResults(None, None, None, None, None, Some(dur), None),
        ];

        let mut ctx = EvalContext::default();
        for (ii, exp) in expecteds.iter().enumerate().take(row.len()) {
            let c = col_expr(ii as i64);
            let expr = Expression::build(&mut ctx, c).unwrap();

            let int = expr.eval_int(&mut ctx, &row).unwrap_or(None);
            let real = expr.eval_real(&mut ctx, &row).unwrap_or(None);
            let dec = expr
                .eval_decimal(&mut ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let string = expr
                .eval_string(&mut ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let time = expr
                .eval_time(&mut ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());
            let dur = expr.eval_duration(&mut ctx, &row).unwrap_or(None);
            let json = expr
                .eval_json(&mut ctx, &row)
                .unwrap_or(None)
                .map(|t| t.into_owned());

            let result = EvalResults(int, real, dec, string, time, dur, json);
            assert_eq!(*exp, result);
        }
    }

    #[test]
    fn test_hybrid_type() {
        let mut ctx = EvalContext::default();
        let row = vec![Datum::I64(12)];
        let hybrid_cases = vec![FieldTypeTp::Enum, FieldTypeTp::Bit, FieldTypeTp::Set];
        let in_hybrid_cases = vec![
            FieldTypeTp::JSON,
            FieldTypeTp::NewDecimal,
            FieldTypeTp::Short,
        ];
        for tp in hybrid_cases {
            let mut c = col_expr(0);
            let mut field_tp = FieldType::default();
            field_tp.as_mut_accessor().set_tp(tp);
            c.set_field_type(field_tp);
            let e = Expression::build(&mut ctx, c).unwrap();
            let res = e.eval_string(&mut ctx, &row).unwrap().unwrap();
            assert_eq!(res.as_ref(), b"12");
        }

        for tp in in_hybrid_cases {
            let mut c = col_expr(0);
            let mut field_tp = FieldType::default();
            field_tp.as_mut_accessor().set_tp(tp);
            c.set_field_type(field_tp);
            let e = Expression::build(&mut ctx, c).unwrap();
            let res = e.eval_string(&mut ctx, &row);
            assert!(res.is_err());
        }
    }
}
