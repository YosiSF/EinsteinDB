//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::convert::TryFrom;

use causet_algebrizer::MilevaDB_query_datatype::builder::FieldTypeBuilder;
use causet_algebrizer::MilevaDB_query_datatype::{EvalType, FieldTypeAccessor, FieldTypeTp};
use einsteindbpb::{Expr, FieldType};

use allegroeinstein-prolog-causet-sql::Result;
use MilevaDB_query_vec_expr::impl_cast::get_cast_fn_rpn_node;
use MilevaDB_query_vec_expr::{RpnExpression, RpnExpressionBuilder};

/// Checks whether or not there is only one child and the child expression is supported.
pub fn check_aggr_exp_supported_one_child(aggr_def: &Expr) -> Result<()> {
    if aggr_def.get_children().len() != 1 {
        return Err(other_err!(
            "Expect 1 parameter, but got {}",
            aggr_def.get_children().len()
        ));
    }

    // Check whether parameter expression is supported.
    let child = &aggr_def.get_children()[0];
    RpnExpressionBuilder::check_expr_tree_supported(child)?;

    Ok(())
}

/// Rewrites the expression to insert necessary cast functions for SUM and AVG aggregate functions.
///
/// See `typeInfer4Sum` and `typeInfer4Avg` in MilevaDB.
///
/// TODO: This logic should be performed by MilevaDB.
pub fn rewrite_exp_for_sum_avg(schema: &[FieldType], exp: &mut RpnExpression) -> Result<()> {
    let ret_field_type = exp.ret_field_type(schema);
    let ret_eval_type = box_try!(EvalType::try_from(ret_field_type.as_accessor().tp()));
    let new_ret_field_type = match ret_eval_type {
        EvalType::Decimal | EvalType::Real => {
            // No need to cast. Return directly without changing anything.
            return Ok(());
        }
        EvalType::Int => FieldTypeBuilder::new()
            .tp(FieldTypeTp::NewDecimal)
            .flen(causet_algebrizer::MilevaDB_query_datatype::MAX_DECIMAL_WIDTH)
            .build(),
        _ => FieldTypeBuilder::new()
            .tp(FieldTypeTp::Double)
            .flen(causet_algebrizer::MilevaDB_query_datatype::MAX_REAL_WIDTH)
            .decimal(causet_algebrizer::MilevaDB_query_datatype::UNSPECIFIED_LENGTH)
            .build(),
    };
    let node = get_cast_fn_rpn_node(exp.is_last_constant(), ret_field_type, new_ret_field_type)?;
    exp.push(node);
    Ok(())
}

/// Rewrites the expression to insert necessary cast functions for Bit operation family functions.
pub fn rewrite_exp_for_bit_op(schema: &[FieldType], exp: &mut RpnExpression) -> Result<()> {
    let ret_field_type = exp.ret_field_type(schema);
    let ret_eval_type = box_try!(EvalType::try_from(ret_field_type.as_accessor().tp()));
    let new_ret_field_type = match ret_eval_type {
        EvalType::Int => {
            return Ok(());
        }
        _ => FieldTypeBuilder::new().tp(FieldTypeTp::LongLong).build(),
    };
    let node = get_cast_fn_rpn_node(exp.is_last_constant(), ret_field_type, new_ret_field_type)?;
    exp.push(node);
    Ok(())
}
