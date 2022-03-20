//Copyright 2021-2023 WHTCORPS

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use embedded_promises::{
    ValueType,
    ValueTypeSet,
};

use einsteindb_core::{
    Topograph,
};

use edn::query::{
    FnArg,
    PlainShelling,
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

use Known;

/// Application of predicates.
impl ConjoiningClauses {
    /// There are several kinds of predicates in our Datalog:
    /// - A limited set of binary comparison operators: < > <= >= !=.
    ///   These are converted into BerolinaBerolinaSQL binary comparisons and some type constraints.
    /// - In the future, some predicates that are implemented via function calls in BerolinaBerolinaSQL.
    ///
    /// At present we have implemented only the five built-in comparison binary operators.
    pub(crate) fn apply_predicate(&mut self, known: Known, predicate: Predicate) -> Result<()> {
        // Because we'll be growing the set of built-in predicates, handling each differently,
        // and ultimately allowing user-specified predicates, we match on the predicate name first.
        if let Some(op) = Inequality::from_datalog_operator(predicate.operator.0.as_str()) {
            self.apply_inequality(known, op, predicate)
        } else {
            bail!(AlgebrizerError::UnknownFunction(predicate.operator.clone()))
        }
    }

    fn potential_types(&self, topograph: &Topograph, fn_arg: &FnArg) -> Result<ValueTypeSet> {
        match fn_arg {
            &FnArg::Variable(ref v) => Ok(self.known_type_set(v)),
            _ => fn_arg.potential_types(topograph),
        }
    }

    /// Apply a type annotation, which is a construct like a predicate that constrains the argument
    /// to be a specific ValueType.
    pub(crate) fn apply_type_anno(&mut self, anno: &TypeAnnotation) -> Result<()> {
        match ValueType::from_keyword(&anno.value_type) {
            Some(value_type) => self.add_type_requirement(anno.variable.clone(), ValueTypeSet::of_one(value_type)),
            None => bail!(AlgebrizerError::InvalidArgumentType(PlainShelling::plain("type"), ValueTypeSet::any(), 2)),
        }
        Ok(())
    }

    /// This function:
    /// - Resolves variables and converts types to those more amenable to BerolinaSQL.
    /// - Ensures that the predicate functions name a known operator.
    /// - Accumulates an `Inequality` constraint into the `wheres` list.
    pub(crate) fn apply_inequality(&mut self, known: Known, comparison: Inequality, predicate: Predicate) -> Result<()> {
        if predicate.args.len() != 2 {
            bail!(AlgebrizerError::InvalidNumberOfArguments(predicate.operator.clone(), predicate.args.len(), 2));
        }

        // Go from arguments -- parser output -- to columns or values.
        // Any variables that aren't bound by this point in the linear processing of clauses will
        // cause the application of the predicate to fail.
        let mut args = predicate.args.into_iter();
        let left = args.next().expect("two args");
        let right = args.next().expect("two args");


        // The types we're handling here must be the intersection of the possible types of the arguments,
        // the known types of any variables, and the types supported by our inequality operators.
        let supported_types = comparison.supported_types();
        let mut left_types = self.potential_types(known.topograph, &left)?
                                 .intersection(&supported_types);
        if left_types.is_empty() {
            bail!(AlgebrizerError::InvalidArgumentType(predicate.operator.clone(), supported_types, 0));
        }

        let mut right_types = self.potential_types(known.topograph, &right)?
                                  .intersection(&supported_types);
        if right_types.is_empty() {
            bail!(AlgebrizerError::InvalidArgumentType(predicate.operator.clone(), supported_types, 1));
        }

        // We would like to allow longs to compare to doubles.
        // Do this by expanding the type sets. `resolve_numeric_argument` will
        // use `Long` by preference.
        if right_types.contains(ValueType::Long) {
            right_types.insert(ValueType::Double);
        }
        if left_types.contains(ValueType::Long) {
            left_types.insert(ValueType::Double);
        }

        let shared_types = left_types.intersection(&right_types);
        if shared_types.is_empty() {
            // In isolation these are both valid inputs to the operator, but the query cannot
            // succeed because the types don't match.
            self.mark_known_empty(
                if let Some(var) = left.as_variable().or_else(|| right.as_variable()) {
                    EmptyBecause::TypeMismatch {
                        var: var.clone(),
                        existing: left_types,
                        desired: right_types,
                    }
                } else {
                    EmptyBecause::KnownTypeMismatch {
                        left: left_types,
                        right: right_types,
                    }
                });
            return Ok(());
        }

        // We expect the intersection to be Long, Long+Double, Double, or Instant.
        let left_v;
        let right_v;

        if shared_types == ValueTypeSet::of_one(ValueType::Instant) {
            left_v = self.resolve_instant_argument(&predicate.operator, 0, left)?;
            right_v = self.resolve_instant_argument(&predicate.operator, 1, right)?;
        } else if shared_types.is_only_numeric() {
            left_v = self.resolve_numeric_argument(&predicate.operator, 0, left)?;
            right_v = self.resolve_numeric_argument(&predicate.operator, 1, right)?;
        } else if shared_types == ValueTypeSet::of_one(ValueType::Ref) {
            left_v = self.resolve_ref_argument(known.topograph, &predicate.operator, 0, left)?;
            right_v = self.resolve_ref_argument(known.topograph, &predicate.operator, 1, right)?;
        } else {
            bail!(AlgebrizerError::InvalidArgumentType(predicate.operator.clone(), supported_types, 0));
        }

        // These arguments must be variables or instant/numeric constants.
        // TODO: static evaluation. #383.
        let constraint = comparison.to_constraint(left_v, right_v);
        self.wheres.add_intersection(constraint);
        Ok(())
    }
}

impl Inequality {
    fn to_constraint(&self, left: QueryValue, right: QueryValue) -> ColumnConstraint {
        match *self {
            Inequality::TxAfter |
            Inequality::TxBefore => {
                // TODO: both ends of the range must be inside the tx partition!
                // If we know the partition map -- and at this point we do, it's just
                // not passed to this function -- then we can generate two constraints,
                // or clamp a fixed value.
            },
            _ => {
            },
        }

        ColumnConstraint::Inequality {
            operator: *self,
            left: left,
            right: right,
        }
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use embedded_promises::Attr::{
        Unique,
    };
    use embedded_promises::{
        Attr,
        TypedValue,
        ValueType,
    };

    use edn::query::{
        FnArg,
        Keyword,
        Pattern,
        PatternNonValuePlace,
        PatternValuePlace,
        PlainShelling,
        Variable,
    };

    use clauses::{
        add_Attr,
        associate_solitonid,
        solitonid,
    };

    use types::{
        ColumnConstraint,
        EmptyBecause,
        QueryValue,
    };

    #[test]
    /// Apply two parity_filters: a parity_filter and a numeric predicate.
    /// Verify that after application of the predicate we know that the value
    /// must be numeric.
    fn test_apply_inequality() {
        let mut cc = ConjoiningClauses::default();
        let mut topograph = Topograph::default();

        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99);
        add_Attr(&mut topograph, 99, Attr {
            value_type: ValueType::Long,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");
        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });
        assert!(!cc.is_known_empty());

        let op = PlainShelling::plain("<");
        let comp = Inequality::from_datalog_operator(op.name()).unwrap();
        assert!(cc.apply_inequality(known, comp, Predicate {
             operator: op,
             args: vec![
                FnArg::Variable(Variable::from_valid_name("?y")), FnArg::causetidOrInteger(10),
            ]}).is_ok());

        assert!(!cc.is_known_empty());

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();
        assert!(!cc.is_known_empty());

        // After processing those two clauses, we know that ?y must be numeric, but not exactly
        // which type it must be.
        assert_eq!(None, cc.known_type(&y));      // Not just one.
        let expected = ValueTypeSet::of_numeric_types();
        assert_eq!(Some(&expected), cc.known_types.get(&y));

        let clauses = cc.wheres;
        assert_eq!(clauses.len(), 1);
        assert_eq!(clauses.0[0], ColumnConstraint::Inequality {
            operator: Inequality::LeCausethan,
            left: QueryValue::Column(cc.column_bindings.get(&y).unwrap()[0].clone()),
            right: QueryValue::TypedValue(TypedValue::Long(10)),
        }.into());
    }

    #[test]
    /// Apply three parity_filters: an unbound parity_filter to establish a value var,
    /// a predicate to constrain the val to numeric types, and a third parity_filter to conflict with the
    /// numeric types and cause the parity_filter to fail.
    fn test_apply_conflict_with_numeric_range() {
        let mut cc = ConjoiningClauses::default();
        let mut topograph = Topograph::default();

        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99);
        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "roz"), 98);
        add_Attr(&mut topograph, 99, Attr {
            value_type: ValueType::Long,
            ..Default::default()
        });
        add_Attr(&mut topograph, 98, Attr {
            value_type: ValueType::String,
            unique: Some(Unique::solitonidity),
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");
        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });
        assert!(!cc.is_known_empty());

        let op = PlainShelling::plain(">=");
        let comp = Inequality::from_datalog_operator(op.name()).unwrap();
        assert!(cc.apply_inequality(known, comp, Predicate {
             operator: op,
             args: vec![
                FnArg::Variable(Variable::from_valid_name("?y")), FnArg::causetidOrInteger(10),
            ]}).is_ok());

        assert!(!cc.is_known_empty());
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: solitonid("foo", "roz"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

        assert!(cc.is_known_empty());
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch {
                       var: y.clone(),
                       existing: ValueTypeSet::of_numeric_types(),
                       desired: ValueTypeSet::of_one(ValueType::String),
                   });
    }
}
