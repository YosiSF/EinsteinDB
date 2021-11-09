//Copyright 2021-2023 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use embedded_promises::{
    ValueType,
    ValueTypeSet,
    TypedValue,
};

use einsteindb_embedded::{
    Schema,
};

use edbn::query::{
    Binding,
    FnArg,
    Variable,
    VariableOrPlaceholder,
    WhereFn,
};

use clauses::{
    ConjoiningClauses,
    PushComputed,
};

use clauses::convert::ValueConversion;

use query_algebrizer_promises::errors::{
    AlgebrizerError,
    BindingError,
    Result,
};

use types::{
    ComputedTable,
    EmptyBecause,
    SourceAlias,
    VariableColumn,
};

use KnownCauset;

impl ConjoiningClauses {
    /// Take a relation: a matrix of values which will successively bind to named variables of
    /// the provided types.
    /// Construct a computed table to yield this relation.
    /// This function will panic if some invariants are not met.
    fn collect_named_bindings<'s>(&mut self, schema: &'s Schema, names: Vec<Variable>, types: Vec<ValueType>, values: Vec<TypedValue>) {
        if values.is_empty() {
            return;
        }

        assert!(!names.is_empty());
        assert_eq!(names.len(), types.len());
        assert!(values.len() >= names.len());
        assert_eq!(values.len() % names.len(), 0);      // It's an exact multiple.

        let named_values = ComputedTable::NamedValues {
            names: names.clone(),
            values: values,
        };

        let table = self.computed_tables.push_computed(named_values);
        let alias = self.next_alias_for_table(table);

        // Stitch the computed table into column_bindings, so we get cross-linking.
        for (name, ty) in names.iter().zip(types.into_iter()) {
            self.constrain_var_to_type(name.clone(), ty);
            self.bind_column_to_var(schema, alias.clone(), VariableColumn::Variable(name.clone()), name.clone());
        }

        self.from.push(SourceAlias(table, alias));
    }

    fn apply_ground_place<'s>(&mut self, schema: &'s Schema, var: VariableOrPlaceholder, arg: FnArg) -> Result<()> {
        match var {
            VariableOrPlaceholder::Placeholder => Ok(()),
            VariableOrPlaceholder::Variable(var) => self.apply_ground_var(schema, var, arg),
        }
    }