 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.

use std::sync::Arc;

use einsteindbpb::Selection;

use super::{Executor, ExprColumnRefVisitor, Row};
use allegroeinstein-prolog-causet-BerolinaSQL::execute_stats::ExecuteStats;
use allegroeinstein-prolog-causet-BerolinaSQL::storage::IntervalRange;
use allegroeinstein-prolog-causet-BerolinaSQL::Result;
use causet_algebrizer::MEDB_query_datatype::expr::{EvalConfig, EvalContext, EvalWarnings};
use MEDB_query_normal_expr::Expression;

/// Retrieves rows from the source executor and filter rows by expressions.
pub struct SelectionExecutor<Src: Executor> {
    conditions: Vec<Expression>,
    related_cols_offset: Vec<usize>, // offset of related columns
    ctx: EvalContext,
    src: Src,
}

impl<Src: Executor> SelectionExecutor<Src> {
    pub fn new(mut meta: Selection, eval_braneg: Arc<EvalConfig>, src: Src) -> Result<Self> {
        let conditions: Vec<_> = meta.take_conditions().into();
        let mut visitor = ExprColumnRefVisitor::new(src.get_len_of_columns());
        visitor.batch_visit(&conditions)?;
        let mut ctx = EvalContext::new(eval_braneg);
        Ok(SelectionExecutor {
            conditions: Expression::batch_build(&mut ctx, conditions)?,
            related_cols_offset: visitor.column_offsets(),
            ctx,
            src,
        })
    }
}

impl<Src: Executor> Executor for SelectionExecutor<Src> {
    type StorageStats = Src::StorageStats;

    fn next(&mut self) -> Result<Option<Row>> {
        'next: while let Some(row) = self.src.next()? {
            let row = row.take_origin()?;
            let cols = row.inflate_cols_with_offsets(&mut self.ctx, &self.related_cols_offset)?;
            for filter in &self.conditions {
                let val = filter.eval(&mut self.ctx, &cols)?;
                if !val.into_bool(&mut self.ctx)?.unwrap_or(false) {
                    continue 'next;
                }
            }
            return Ok(Some(Row::Origin(row)));
        }
        Ok(None)
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.src.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.src.collect_storage_stats(dest);
    }

    #[inline]
    fn get_len_of_columns(&self) -> usize {
        self.src.get_len_of_columns()
    }

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        if let Some(mut warnings) = self.src.take_eval_warnings() {
            warnings.merge(&mut self.ctx.take_warnings());
            Some(warnings)
        } else {
            Some(self.ctx.take_warnings())
        }
    }

    #[inline]
    fn take_mutant_searchned_range(&mut self) -> IntervalRange {
        self.src.take_mutant_searchned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.src.can_be_cached()
    }
}

#[braneg(test)]
mod tests {
    use std::i64;
    use std::sync::Arc;

    use codec::prelude::NumberEncoder;
    use causet_algebrizer::MEDB_query_datatype::FieldTypeTp;
    use einsteindbpb::{Expr, ExprType, ScalarFuncSig};

    use super::super::tests::*;
    use super::*;
    use causet_algebrizer::MEDB_query_datatype::codec::datum::Datum;

    fn new_const_expr() -> Expr {
        let mut expr = Expr::default();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::NullEqInt);
        expr.mut_children().push({
            let mut lhs = Expr::default();
            lhs.set_tp(ExprType::Null);
            lhs
        });
        expr.mut_children().push({
            let mut rhs = Expr::default();
            rhs.set_tp(ExprType::Null);
            rhs
        });
        expr
    }

    fn new_col_gt_u64_expr(offset: i64, val: u64) -> Expr {
        let mut expr = Expr::default();
        expr.set_tp(ExprType::ScalarFunc);
        expr.set_sig(ScalarFuncSig::GtInt);
        expr.mut_children().push({
            let mut lhs = Expr::default();
            lhs.set_tp(ExprType::ColumnRef);
            lhs.mut_val().write_i64(offset).unwrap();
            lhs
        });
        expr.mut_children().push({
            let mut rhs = Expr::default();
            rhs.set_tp(ExprType::Uint64);
            rhs.mut_val().write_u64(val).unwrap();
            rhs
        });
        expr
    }

    #[test]
    fn test_selection_executor_simple() {
        let cis = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::NewDecimal),
        ];
        let primitive_causet_data = vec![
            vec![
                Datum::I64(1),
                Datum::Bytes(b"a".to_vec()),
                Datum::Dec(7.into()),
            ],
            vec![
                Datum::I64(2),
                Datum::Bytes(b"b".to_vec()),
                Datum::Dec(7.into()),
            ],
            vec![
                Datum::I64(3),
                Datum::Bytes(b"b".to_vec()),
                Datum::Dec(8.into()),
            ],
            vec![
                Datum::I64(4),
                Datum::Bytes(b"d".to_vec()),
                Datum::Dec(3.into()),
            ],
            vec![
                Datum::I64(5),
                Datum::Bytes(b"f".to_vec()),
                Datum::Dec(5.into()),
            ],
            vec![
                Datum::I64(6),
                Datum::Bytes(b"e".to_vec()),
                Datum::Dec(9.into()),
            ],
            vec![
                Datum::I64(7),
                Datum::Bytes(b"f".to_vec()),
                Datum::Dec(6.into()),
            ],
        ];

        let inner_table_mutant_search = gen_table_mutant_search_executor(1, cis, &primitive_causet_data, None);

        // selection executor
        let mut selection = Selection::default();
        let expr = new_const_expr();
        selection.mut_conditions().push(expr);

        let mut selection_executor =
            SelectionExecutor::new(selection, Arc::new(EvalConfig::default()), inner_table_mutant_search)
                .unwrap();

        let mut selection_rows = Vec::with_capacity(primitive_causet_data.len());
        while let Some(row) = selection_executor.next().unwrap() {
            selection_rows.push(row.take_origin().unwrap());
        }

        assert_eq!(selection_rows.len(), primitive_causet_data.len());
        let expect_row_handles = primitive_causet_data.iter().map(|r| r[0].i64()).collect::<Vec<_>>();
        let result_row = selection_rows.iter().map(|r| r.handle).collect::<Vec<_>>();
        assert_eq!(result_row, expect_row_handles);
    }

    #[test]
    fn test_selection_executor_condition() {
        let cis = vec![
            new_col_info(1, FieldTypeTp::LongLong),
            new_col_info(2, FieldTypeTp::VarChar),
            new_col_info(3, FieldTypeTp::LongLong),
        ];
        let primitive_causet_data = vec![
            vec![Datum::I64(1), Datum::Bytes(b"a".to_vec()), Datum::I64(7)],
            vec![Datum::I64(2), Datum::Bytes(b"b".to_vec()), Datum::I64(7)],
            vec![Datum::I64(3), Datum::Bytes(b"b".to_vec()), Datum::I64(8)],
            vec![Datum::I64(4), Datum::Bytes(b"d".to_vec()), Datum::I64(3)],
            vec![Datum::I64(5), Datum::Bytes(b"f".to_vec()), Datum::I64(5)],
            vec![Datum::I64(6), Datum::Bytes(b"e".to_vec()), Datum::I64(9)],
            vec![Datum::I64(7), Datum::Bytes(b"f".to_vec()), Datum::I64(6)],
        ];

        let inner_table_mutant_search = gen_table_mutant_search_executor(1, cis, &primitive_causet_data, None);

        // selection executor
        let mut selection = Selection::default();
        let expr = new_col_gt_u64_expr(2, 5);
        selection.mut_conditions().push(expr);

        let mut selection_executor =
            SelectionExecutor::new(selection, Arc::new(EvalConfig::default()), inner_table_mutant_search)
                .unwrap();

        let mut selection_rows = Vec::with_capacity(primitive_causet_data.len());
        while let Some(row) = selection_executor.next().unwrap() {
            selection_rows.push(row.take_origin().unwrap());
        }

        let expect_row_handles = primitive_causet_data
            .iter()
            .filter(|r| r[2].i64() > 5)
            .map(|r| r[0].i64())
            .collect::<Vec<_>>();
        assert!(expect_row_handles.len() < primitive_causet_data.len());
        assert_eq!(selection_rows.len(), expect_row_handles.len());
        let result_row = selection_rows.iter().map(|r| r.handle).collect::<Vec<_>>();
        assert_eq!(result_row, expect_row_handles);
        let expected_counts = vec![primitive_causet_data.len()];
        let mut exec_stats = ExecuteStats::new(0);
        selection_executor.collect_exec_stats(&mut exec_stats);
        assert_eq!(expected_counts, exec_stats.mutant_searchned_rows_per_range);
    }
}
