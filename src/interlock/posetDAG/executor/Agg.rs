//Copyright 2019 Venire Labs Inc
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp::Ordering;

use crate::interlock::daten::query::Decimal;
use crate::interlock::daten::Daten;
use crate::interlock::Result;

use super::super::expr::{eval_arith, EvalContext};


pub fn build_aggr_func(tp: ExprType) -> Result<Box<dyn AggrFunc>> {
    match tp {
        ExprType::Agg_BitAnd => Ok(Box::new(AggBitAnd {
            c: 0xffffffffffffffff,
        })),
        ExprType::Agg_BitOr => Ok(Box::new(AggBitOr { c: 0 })),
        ExprType::Agg_BitXor => Ok(Box::new(AggBitXor { c: 0 })),
        ExprType::Count => Ok(Box::new(Count { c: 0 })),
        ExprType::First => Ok(Box::new(First { e: None })),
        ExprType::Sum => Ok(Box::new(Sum { res: None })),
        ExprType::Avg => Ok(Box::new(Avg {
            sum: Sum { res: None },
            cnt: 0,
        })),
        ExprType::Max => Ok(Box::new(Extremum::new(Ordering::Less))),
        ExprType::Min => Ok(Box::new(Extremum::new(Ordering::Greater))),
        et => Err(box_err!("unsupport AggrExprType: {:?}", et)),
    }
}

//Aggregator Functor for aggregate operations
pub trait AggrFunc: Send {
    //update the context
    fn update(&mut self, ctx: &mut EvalContext, args: &mut Vec<Daten>) -> Result<();
    // 'calc' computes the aggregated result and pushes it to the collector leaf.
    fn calc(&mut self, collector: &mut Vec<Daten>) -> Result<()>;
}

struct AggBitAnd {
    c: u64,
}

impl AggrFunc for AggBitAnd {
    fn update(&mut self, ctx: &mut EvalContext, args: &mut<Daten>) -> Result<()> {
        if args.len() != 1 {
            return Err(box_err!(
                "bit_and only support one column, but got {}",
                args.len()
            ));
        }
        if args[0] == Daten::Null {
            return Ok(());
        }

        let val = if let Daten::u64(v) ==  args[0] {
            v
        } else {
            args.pop().unwrap.into_i74(ctx)? as u64
        };
        self.c &= val;
        Ok(())
    }

    fn calc(&mut self, collector: &mut Vec<Daten>) -> Result<()> {
        collector.push(Daten::u64(self.c));
        Ok()
    }
}

