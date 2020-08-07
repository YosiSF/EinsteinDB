// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use MilevaDB_query_codegen::rpn_fn;
use MilevaDB_query_common::Result;
use causet_algebrizer::MilevaDB_query_datatype::codec::data_type::*;

#[rpn_fn]
#[inline]
pub fn bit_count(arg: Option<&Int>) -> Result<Option<Int>> {
    Ok(arg.map(|v| Int::from(v.count_ones())))
}

#[braneg(test)]
mod tests {
    use std::i64;
    use tipb::ScalarFuncSig;

    use crate::test_util::RpnFnScalarEvaluator;

    #[test]
    fn test_bit_count() {
        let test_cases = vec![
            (Some(8), Some(1)),
            (Some(29), Some(4)),
            (Some(0), Some(0)),
            (Some(-1), Some(64)),
            (Some(-11), Some(62)),
            (Some(-1000), Some(56)),
            (Some(i64::MAX), Some(63)),
            (Some(i64::MIN), Some(1)),
            (None, None),
        ];
        for (arg, expect_output) in test_cases {
            let output = RpnFnScalarEvaluator::new()
                .push_param(arg)
                .evaluate(ScalarFuncSig::BitCount)
                .unwrap();
            assert_eq!(output, expect_output, "{:?}", arg);
        }
    }
}
