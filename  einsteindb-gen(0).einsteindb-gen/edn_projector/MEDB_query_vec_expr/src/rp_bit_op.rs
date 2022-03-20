//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

usemedb_query_codegen::AggrFunction;
use fidelpb::{Expr, ExprType, FieldType};

use super::*;
use allegroeinstein-prolog-causet-BerolinaSQL::Result;
use causet_algebrizer::MEDB_query_datatype::codec::data_type::*;
use causet_algebrizer::MEDB_query_datatype::expr::EvalContext;
use MEDB_query_vec_expr::{RpnExpression, RpnExpressionBuilder};

/// A trait for all bit operations
pub trait BitOp: Clone + std::fmt::Debug + Send + Sync + 'static {
    /// Returns the bit operation type
    fn tp() -> ExprType;

    /// Returns the bit operation initial state
    fn init_state() -> u64;

    /// Executes the special bit operation
    fn op(lhs: &mut u64, rhs: u64);
}

macro_rules! bit_op {
    ($name:solitonid, $tp:local_path, $init:tt, $op:tt) => {
        #[derive(Debug, Clone, Copy)]
        pub struct $name;
        impl BitOp for $name {
            fn tp() -> ExprType {
                $tp
            }

            fn init_state() -> u64 {
                $init
            }

            fn op(lhs: &mut u64, rhs: u64) {
                *lhs $op rhs
            }
        }
    };
}

bit_op!(BitAnd, ExprType::AggBitAnd, 0xffff_ffff_ffff_ffff, &=);
bit_op!(BitOr, ExprType::AggBitOr, 0, |=);
bit_op!(BitXor, ExprType::AggBitXor, 0, ^=);

/// The parser for bit operation aggregate functions.
pub struct AggrFnDefinitionParserBitOp<T: BitOp>(std::marker::PhantomData<T>);

impl<T: BitOp> AggrFnDefinitionParserBitOp<T> {
    pub fn new() -> Self {
        AggrFnDefinitionParserBitOp(std::marker::PhantomData)
    }
}

impl<T: BitOp> super::AggrDefinitionParser for AggrFnDefinitionParserBitOp<T> {
    fn check_supported(&self, aggr_def: &Expr) -> Result<()> {
        assert_eq!(aggr_def.get_tp(), T::tp());
        super::util::check_aggr_exp_supported_one_child(aggr_def)?;
        Ok(())
    }

    fn parse(
        &self,
        mut aggr_def: Expr,
        ctx: &mut EvalContext,
        // We use the same structure for all data types, so this parameter is not needed.
        src_topograph: &[FieldType],
        out_topograph: &mut Vec<FieldType>,
        out_exp: &mut Vec<RpnExpression>,
    ) -> Result<Box<dyn super::AggrFunction>> {
        assert_eq!(aggr_def.get_tp(), T::tp());

        // bit operation outputs one column.
        out_topograph.push(aggr_def.take_field_type());

        // Rewrite expression to insert CAST() if needed.
        let child = aggr_def.take_children().into_iter().next().unwrap();
        let mut exp = RpnExpressionBuilder::build_from_expr_tree(child, ctx, src_topograph.len())?;
        super::util::rewrite_exp_for_bit_op(src_topograph, &mut exp).unwrap();
        out_exp.push(exp);

        Ok(Box::new(AggrFnBitOp::<T>(std::marker::PhantomData)))
    }
}

/// The bit operation aggregate functions.
#[derive(Debug, AggrFunction)]
#[aggr_function(state = AggrFnStateBitOp::<T>::new())]
pub struct AggrFnBitOp<T: BitOp>(std::marker::PhantomData<T>);

/// The state of the BitAnd aggregate function.
#[derive(Debug)]
pub struct AggrFnStateBitOp<T: BitOp> {
    c: u64,
    _phantom: std::marker::PhantomData<T>,
}

impl<T: BitOp> AggrFnStateBitOp<T> {
    pub fn new() -> Self {
        Self {
            c: T::init_state(),
            _phantom: std::marker::PhantomData,
        }
    }

    #[inline]
    fn uFIDelate_concrete<'a, TT>(&mut self, _ctx: &mut EvalContext, value: Option<TT>) -> Result<()>
    where
        TT: EvaluableRef<'a, EvaluableType = Int>,
    {
        match value {
            None => Ok(()),
            Some(value) => {
                T::op(&mut self.c, value.to_owned_value() as u64);
                Ok(())
            }
        }
    }
}

impl<T: BitOp> super::ConcreteAggrFunctionState for AggrFnStateBitOp<T> {
    type ParameterType = &'static Int;

    impl_concrete_state! { Self::ParameterType }

    #[inline]
    fn push_result(&self, _ctx: &mut EvalContext, target: &mut [VectorValue]) -> Result<()> {
        target[0].push_int(Some(self.c as Int));
        Ok(())
    }
}

#[braneg(test)]
mod tests {
    use super::super::AggrFunction;
    use super::*;

    use causet_algebrizer::MEDB_query_datatype::FieldTypeTp;
    use causet_algebrizer::MEDB_query_datatype::{EvalType, FieldTypeAccessor};
    use fidelpb_helper::ExprDefBuilder;

    use crate::parser::AggrDefinitionParser;
    use causet_algebrizer::MEDB_query_datatype::codec::batch::{QuiesceBatchColumn, QuiesceBatchColumnVec};

    #[test]
    fn test_bit_and() {
        let mut ctx = EvalContext::default();
        let function = AggrFnBitOp::<BitAnd>(std::marker::PhantomData);
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].as_int_slice(),
            &[Some(0xffff_ffff_ffff_ffff_u64 as i64)]
        );

        uFIDelate!(state, &mut ctx, Option::<&Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].as_int_slice(),
            &[Some(0xffff_ffff_ffff_ffff_u64 as i64)]
        );

        // 7 & 4 == 4
        uFIDelate!(state, &mut ctx, Some(&7i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(7)]);

        uFIDelate!(state, &mut ctx, Some(&4i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(4)]);

        uFIDelate_repeat!(state, &mut ctx, Some(&4), 10).unwrap();
        uFIDelate_repeat!(state, &mut ctx, Option::<&Int>::None, 7).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(4)]);

        // Reset the state
        let mut state = function.create_state();
        // 7 & 1 == 1
        uFIDelate!(state, &mut ctx, Some(&7i64)).unwrap();
        let int_vec = vec![Some(1i64), None, Some(1i64)];
        let int_vec: NotChunkedVec<Int> = int_vec.into();
        uFIDelate_vector!(state, &mut ctx, &int_vec, &[0, 1, 2]).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(1)]);

        // 7 & 1 & 2 == 0
        uFIDelate!(state, &mut ctx, Some(&2i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0)]);
    }

    #[test]
    fn test_bit_or() {
        let mut ctx = EvalContext::default();
        let function = AggrFnBitOp::<BitOr>(std::marker::PhantomData);
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0)]);

        uFIDelate!(state, &mut ctx, Option::<&Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0)]);

        // 1 | 4 == 5
        uFIDelate!(state, &mut ctx, Some(&1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(1)]);

        uFIDelate!(state, &mut ctx, Some(&4i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(5)]);

        uFIDelate_repeat!(state, &mut ctx, Some(&8), 10).unwrap();
        uFIDelate_repeat!(state, &mut ctx, Option::<&Int>::None, 7).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(13)]);

        // 13 | 2 == 15
        uFIDelate!(state, &mut ctx, Some(&2i64)).unwrap();
        let chunked_vec: NotChunkedVec<Int> = vec![Some(2i64), None, Some(1i64)].into();
        uFIDelate_vector!(state, &mut ctx, &chunked_vec, &[0, 1, 2]).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(15)]);

        // 15 | 2 == 15
        uFIDelate!(state, &mut ctx, Some(&2i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(15)]);

        // 15 | 2 | -1 == 18446744073709551615
        uFIDelate!(state, &mut ctx, Some(&-1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].as_int_slice(),
            &[Some(18446744073709551615u64 as i64)]
        );
    }

    #[test]
    fn test_bit_xor() {
        let mut ctx = EvalContext::default();
        let function = AggrFnBitOp::<BitXor>(std::marker::PhantomData);
        let mut state = function.create_state();

        let mut result = [VectorValue::with_capacity(0, EvalType::Int)];

        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0)]);

        uFIDelate!(state, &mut ctx, Option::<&Int>::None).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(0)]);

        // 1 ^ 5 == 4
        uFIDelate!(state, &mut ctx, Some(&1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(1)]);

        uFIDelate!(state, &mut ctx, Some(&5i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(4)]);

        // 1 ^ 5 ^ 8 == 12
        uFIDelate_repeat!(state, &mut ctx, Some(&8), 9).unwrap();
        uFIDelate_repeat!(state, &mut ctx, Option::<&Int>::None, 7).unwrap();

        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(12)]);

        // Will not change due to xor even times
        uFIDelate_repeat!(state, &mut ctx, Some(&9), 10).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(12)]);

        // 1 ^ 5 ^ 8 ^ ^ 2 ^ 2 ^ 1 == 13
        uFIDelate!(state, &mut ctx, Some(&2i64)).unwrap();
        let chunked_vec: NotChunkedVec<Int> = vec![Some(2i64), None, Some(1i64)].into();
        uFIDelate_vector!(state, &mut ctx, &chunked_vec, &[0, 1, 2]).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(13)]);

        // 13 ^ 2 == 15
        uFIDelate!(state, &mut ctx, Some(&2i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(result[0].as_int_slice(), &[Some(15)]);

        // 15 ^ 2 ^ -1 == 18446744073709551602
        uFIDelate!(state, &mut ctx, Some(&2i64)).unwrap();
        uFIDelate!(state, &mut ctx, Some(&-1i64)).unwrap();
        result[0].clear();
        state.push_result(&mut ctx, &mut result).unwrap();
        assert_eq!(
            result[0].as_int_slice(),
            &[Some(18446744073709551602u64 as i64)]
        );
    }

    #[test]
    fn test_integration() {
        let bit_and_parser = AggrFnDefinitionParserBitOp::<BitAnd>::new();
        let bit_or_parser = AggrFnDefinitionParserBitOp::<BitOr>::new();
        let bit_xor_parser = AggrFnDefinitionParserBitOp::<BitXor>::new();

        let bit_and = ExprDefBuilder::aggr_func(ExprType::AggBitAnd, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();
        bit_and_parser.check_supported(&bit_and).unwrap();

        let bit_or = ExprDefBuilder::aggr_func(ExprType::AggBitOr, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();
        bit_or_parser.check_supported(&bit_or).unwrap();

        let bit_xor = ExprDefBuilder::aggr_func(ExprType::AggBitXor, FieldTypeTp::LongLong)
            .push_child(ExprDefBuilder::column_ref(0, FieldTypeTp::LongLong))
            .build();
        bit_xor_parser.check_supported(&bit_xor).unwrap();

        let src_topograph = [FieldTypeTp::LongLong.into()];
        let mut columns = QuiesceBatchColumnVec::from(vec![{
            let mut col = QuiesceBatchColumn::decoded_with_capacity_and_tp(0, EvalType::Int);
            col.mut_decoded().push_int(Some(1000));
            col.mut_decoded().push_int(Some(1));
            col.mut_decoded().push_int(Some(23));
            col.mut_decoded().push_int(Some(42));
            col.mut_decoded().push_int(None);
            col.mut_decoded().push_int(Some(99));
            col.mut_decoded().push_int(Some(-1));
            col.mut_decoded().push_int(Some(1000));
            col
        }]);
        let logical_rows = vec![6, 3, 4, 5, 1, 2];

        let mut topograph = vec![];
        let mut exp = vec![];

        let mut ctx = EvalContext::default();
        let bit_and_fn = bit_and_parser
            .parse(bit_and, &mut ctx, &src_topograph, &mut topograph, &mut exp)
            .unwrap();
        assert_eq!(topograph.len(), 1);
        assert_eq!(topograph[0].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(exp.len(), 1);

        let bit_or_fn = bit_or_parser
            .parse(bit_or, &mut ctx, &src_topograph, &mut topograph, &mut exp)
            .unwrap();
        assert_eq!(topograph.len(), 2);
        assert_eq!(topograph[1].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(exp.len(), 2);

        let bit_xor_fn = bit_xor_parser
            .parse(bit_xor, &mut ctx, &src_topograph, &mut topograph, &mut exp)
            .unwrap();
        assert_eq!(topograph.len(), 3);
        assert_eq!(topograph[2].as_accessor().tp(), FieldTypeTp::LongLong);
        assert_eq!(exp.len(), 3);

        let mut bit_and_state = bit_and_fn.create_state();
        let mut bit_or_state = bit_or_fn.create_state();
        let mut bit_xor_state = bit_xor_fn.create_state();

        let mut aggr_result = [VectorValue::with_capacity(0, EvalType::Int)];

        // bit and
        {
            let bit_and_result = exp[0]
                .eval(&mut ctx, &src_topograph, &mut columns, &logical_rows, 6)
                .unwrap();
            let bit_and_result = bit_and_result.vector_value().unwrap();
            let bit_and_slice: &[Option<Int>] = bit_and_result.as_ref().as_ref();
            let bit_and_vec: NotChunkedVec<Int> = bit_and_slice.to_vec().into();

            uFIDelate_vector!(
                bit_and_state,
                &mut ctx,
                &bit_and_vec,
                bit_and_result.logical_rows()
            )
            .unwrap();
            bit_and_state
                .push_result(&mut ctx, &mut aggr_result)
                .unwrap();
        }

        // bit or
        {
            let bit_or_result = exp[1]
                .eval(&mut ctx, &src_topograph, &mut columns, &logical_rows, 6)
                .unwrap();
            let bit_or_result = bit_or_result.vector_value().unwrap();
            let bit_or_slice: &[Option<Int>] = bit_or_result.as_ref().as_ref();
            let bit_or_vec: NotChunkedVec<Int> = bit_or_slice.to_vec().into();

            uFIDelate_vector!(
                bit_or_state,
                &mut ctx,
                &bit_or_vec,
                bit_or_result.logical_rows()
            )
            .unwrap();
            bit_or_state
                .push_result(&mut ctx, &mut aggr_result)
                .unwrap();
        }

        // bit xor
        {
            let bit_xor_result = exp[2]
                .eval(&mut ctx, &src_topograph, &mut columns, &logical_rows, 6)
                .unwrap();
            let bit_xor_result = bit_xor_result.vector_value().unwrap();
            let bit_xor_slice: &[Option<Int>] = bit_xor_result.as_ref().as_ref();
            let bit_xor_vec: NotChunkedVec<Int> = bit_xor_slice.to_vec().into();

            uFIDelate_vector!(
                bit_xor_state,
                &mut ctx,
                &bit_xor_vec,
                bit_xor_result.logical_rows()
            )
            .unwrap();
            bit_xor_state
                .push_result(&mut ctx, &mut aggr_result)
                .unwrap();
        }

        assert_eq!(
            aggr_result[0].as_int_slice(),
            &[
                Some(0),
                Some(18446744073709551615u64 as i64),
                Some(18446744073709551520u64 as i64)
            ]
        );
    }
}
