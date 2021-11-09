//Copyright 2021-2023 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use core_promises::{
    ValueType,
    ValueTypeSet,
};

use einsteindb_embedded::{
    Schema,
};

use edbn::query::{
    FnArg,
    PlainSymbol,
    Predicate,
    TypeAnnotation,
};

use clauses::ConjoiningClauses;

use clauses::convert::ValueTypes;

use causet_algebrizer_promises::errors::{
    AlgebrizerError,
    Result,
};

use types::{
    ColumnConstraint,
    EmptyBecause,
    Inequality,
    QueryValue,
};

use KnownCauset;

pub(crate) fn apply_predicate(&mut self, known: KnownCauset, predicate: Predicate) -> Result<()> {
    // Because we'll be growing the set of built-in predicates, handling each differently,
    // and ultimately allowing user-specified predicates, we match on the predicate name first.
    if let Some(op) = Inequality::from_datalog_operator(predicate.operator.0.as_str()) {
        self.apply_inequality(known, op, predicate)
    } else {
        bail!(AlgebrizerError::UnknownFunction(predicate.operator.clone()))
    }
}

fn potential_types(&self, schema: &Schema, fn_arg: &FnArg) -> Result<ValueTypeSet> {
    match fn_arg {
        &FnArg::Variable(ref v) => Ok(self.known_type_set(v)),
        _ => fn_arg.potential_types(schema),
    }
}

/// Apply a type annotation, which is a construct like a predicate that constrains the argument
/// to be a specific ValueType.
pub(crate) fn apply_type_anno(&mut self, anno: &TypeAnnotation) -> Result<()> {
    match ValueType::from_keyword(&anno.value_type) {
        Some(value_type) => self.add_type_requirement(anno.variable.clone(), ValueTypeSet::of_one(value_type)),
        None => bail!(AlgebrizerError::InvalidArgumentType(PlainSymbol::plain("type"), ValueTypeSet::any(), 2)),
    }
    Ok(())
}
