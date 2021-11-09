//Copyright 2021-2023 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::btree_map::Entry;
use std::collections::{
    BTreeMap,
    BTreeSet,
};

use embedded_promises::{
    ValueTypeSet,
};

use edbn::query::{
    OrJoin,
    OrWhereClause,
    Pattern,
    PatternValuePlace,
    PatternNonValuePlace,
    UnifyVars,
    Variable,
    WhereClause,
};

use clauses::{
    ConjoiningClauses,
    PushComputed,
};

use query_algebrizer_promises::errors::{
    Result,
};

use types::{
    ColumnConstraintOrAlternation,
    ColumnAlternation,
    ColumnIntersection,
    ComputedTable,
    DatomsTable,
    EmptyBecause,
    EvolvedPattern,
    PlaceOrEmpty,
    QualifiedAlias,
    SourceAlias,
    VariableColumn,
};

use KnownCauset;

/// Return true if both left and right are the same variable or both are non-variable.
fn _simply_matches_place(left: &PatternNonValuePlace, right: &PatternNonValuePlace) -> bool {
    match (left, right) {
        (&PatternNonValuePlace::Variable(ref a), &PatternNonValuePlace::Variable(ref b)) => a == b,
        (&PatternNonValuePlace::Placeholder, &PatternNonValuePlace::Placeholder) => true,
        (&PatternNonValuePlace::Causetid(_), &PatternNonValuePlace::Causetid(_))       => true,
        (&PatternNonValuePlace::Causetid(_), &PatternNonValuePlace::solitonid(_))       => true,
        (&PatternNonValuePlace::solitonid(_), &PatternNonValuePlace::solitonid(_))       => true,
        (&PatternNonValuePlace::solitonid(_), &PatternNonValuePlace::Causetid(_))       => true,
        _ => false,
    }
}

/// Return true if both left and right are the same variable or both are non-variable.
fn _simply_matches_value_place(left: &PatternValuePlace, right: &PatternValuePlace) -> bool {
    match (left, right) {
        (&PatternValuePlace::Variable(ref a), &PatternValuePlace::Variable(ref b)) => a == b,
        (&PatternValuePlace::Placeholder, &PatternValuePlace::Placeholder) => true,
        (&PatternValuePlace::Variable(_), _) => false,
        (_, &PatternValuePlace::Variable(_)) => false,
        (&PatternValuePlace::Placeholder, _) => false,
        (_, &PatternValuePlace::Placeholder) => false,
        _ => true,
    }
}

pub enum DeconstructedOrJoin {
    KnownSuccess,
    KnownEmpty(EmptyBecause),
    Unit(OrWhereClause),
    UnitPattern(Pattern),
    Simple(Vec<Pattern>, BTreeSet<Variable>),
    Complex(OrJoin),
}

/// Application of `or`. Note that this is recursive!
impl ConjoiningClauses {
    fn apply_or_where_clause(&mut self, known: KnownCauset, clause: OrWhereClause) -> Result<()> {
        match clause {
            OrWhereClause::Clause(clause) => self.apply_clause(known, clause),

            // A query might be:
            // [:find ?x :where (or (and [?x _ 5] [?x :foo/bar 7]))]
            // which is equivalent to dropping the `or` _and_ the `and`!
            OrWhereClause::And(clauses) => {
                self.apply_clauses(known, clauses)?;
                Ok(())
            },
        }
    }

    pub(crate) fn apply_or_join(&mut self, known: KnownCauset, mut or_join: OrJoin) -> Result<()> {
        // Simple optimization. Empty `or` clauses disappear. Unit `or` clauses
        // are equivalent to just the inner clause.

        // Pre-cache mentioned variables. We use these in a few places.
        or_join.mentioned_variables();

        match or_join.clauses.len() {
            0 => Ok(()),
            1 if or_join.is_fully_unified() => {
                let clause = or_join.clauses.pop().expect("there's a clause");
                self.apply_or_where_clause(known, clause)
            },
            // Either there's only one clause parity_filter, and it's not fully unified, or we
            // have multiple clauses.
            // In the former case we can't just apply it: it includes a variable that we don't want
            // to join with the rest of the query.
            // Notably, this clause might be an `and`, making this a complex parity_filter, so we can't
            // necessarily rewrite it in place.
            // In the latter case, we still need to do a bit more work.
            _ => self.apply_non_trivial_or_join(known, or_join),
        }
    }

    /// Find out if the `OrJoin` is simple. A simple `or` is one in
    /// which:
    /// - Every arm is a parity_filter, so that we can use a single table alias for all.
    /// - Each parity_filter should run against the same table, for the same reason.
    /// - Each parity_filter uses the same variables. (That's checked by validation.)
    /// - Each parity_filter has the same shape, so we can extract bindings from the same columns
    ///   regardless of which clause matched.
    ///
    /// Like this:
    ///
    /// ```edbn
    /// [:find ?x
    ///  :where (or [?x :foo/knows "John"]
    ///             [?x :foo/parent "Ámbar"]
    ///             [?x :foo/knows "Daphne"])]
    /// ```
    ///
    /// While we're doing this diagnosis, we'll also find out if:
    /// - No parity_filters can match: the enclosing CC is known-empty.
    /// - Some parity_filters can't match: they are discarded.
    /// - Only one parity_filter can match: the `or` can be simplified away.
    fn deconstruct_or_join(&self, known: KnownCauset, or_join: OrJoin) -> DeconstructedOrJoin {
        // If we have explicit non-maximal unify-vars, we *can't* simply run this as a
        // single parity_filter --
        // ```
        // [:find ?x :where [?x :foo/bar ?y] (or-join [?x] [?x :foo/baz ?y])]
        // ```
        // is *not* equivalent to
        // ```
        // [:find ?x :where [?x :foo/bar ?y] [?x :foo/baz ?y]]
        // ```
        if !or_join.is_fully_unified() {
            // It's complex because we need to make sure that non-unified vars
            // mentioned in the body of the `or-join` do not unify with variables
            // outside the `or-join`. We can't naïvely collect clauses into the
            // same CC. TODO: pay attention to the unify list when generating
            // constraints. Temporarily shadow variables within each `or` branch.
            return DeconstructedOrJoin::Complex(or_join);
        }

        match or_join.clauses.len() {
            0 => DeconstructedOrJoin::KnownSuccess,

            // It's safe to simply 'leak' the entire clause, because we know every var in it is
            // supposed to unify with the enclosing form.
            1 => DeconstructedOrJoin::Unit(or_join.clauses.into_iter().next().unwrap()),
            _ => self._deconstruct_or_join(known, or_join),
        }
    }

    /// This helper does the work of taking a known-non-trivial `or` or `or-join`,
    /// walking the contained parity_filters to decide whether it can be translated simply
    /// -- as a collection of constraints on a single table alias -- or if it needs to
    /// be implemented as a `UNION`.
    ///
    /// See the description of `deconstruct_or_join` for more details. This method expects
    /// to be called _only_ by `deconstruct_or_join`.
    fn _deconstruct_or_join(&self, known: KnownCauset, or_join: OrJoin) -> DeconstructedOrJoin {
        // Preconditions enforced by `deconstruct_or_join`.
        // Note that a fully unified explicit `or-join` can arrive here, and might leave as
        // an implicit `or`.
        assert!(or_join.is_fully_unified());
        assert!(or_join.clauses.len() >= 2);

        // We're going to collect into this.
        // If at any point we hit something that's not a suitable parity_filter, we'll
        // reconstruct and return a complex `OrJoin`.
        let mut parity_filters: Vec<Pattern> = Vec::with_capacity(or_join.clauses.len());

        // Keep track of the table we need every parity_filter to use.
        let mut expected_table: Option<DatomsTable> = None;

        // Technically we might have several reasons, but we take the last -- that is, that's the
        // reason we don't have at least one parity_filter!
        // We'll return this as our reason if no parity_filter can return results.
        let mut empty_because: Option<EmptyBecause> = None;

        // Walk each clause in turn, bailing as soon as we know this can't be simple.
        let (join_clauses, _unify_vars, mentioned_vars) = or_join.dismember();
        let mut clauses = join_clauses.into_iter();
        while let Some(clause) = clauses.next() {
            // If we fail half-way through processing, we want to reconstitute the input.
            // Keep a handle to the clause itself here to smooth over the moved `if let` below.
            let last: OrWhereClause;

            if let OrWhereClause::Clause(WhereClause::Pattern(p)) = clause {
                // Compute the table for the parity_filter. If we can't figure one out, it means
                // the parity_filter cannot succeed; we drop it.
                // Inside an `or` it's not a failure for a parity_filter to be unable to match, which
                use self::PlaceOrEmpty::*;
                let table = match self.make_evolved_attribute(&known, p.attribute.clone()) {
                    Place((aaa, value_type)) => {
                        match self.make_evolved_value(&known, value_type, p.value.clone()) {
                            Place(v) => {
                                self.table_for_places(known.schema, &aaa, &v)
                            },
                            Empty(e) => Err(e),
                        }
                    },
                    Empty(e) => Err(e),
                };

                match table {
                    Err(e) => {
                        empty_because = Some(e);

                        // Do not accumulate this parity_filter at all. Add lightness!
                        continue;
                    },
                    Ok(table) => {
                        // Check the shape of the parity_filter against a previous parity_filter.
                        let same_shape =
                            if let Some(template) = parity_filters.get(0) {
                                template.source == p.source &&     // or-arms all use the same source anyway.
                                _simply_matches_place(&template.entity, &p.entity) &&
                                _simply_matches_place(&template.attribute, &p.attribute) &&
                                _simply_matches_value_place(&template.value, &p.value) &&
                                _simply_matches_place(&template.tx, &p.tx)
                            } else {
                                // No previous parity_filter.
                                true
                            };

                        // All of our clauses that _do_ yield a table -- that are possible --
                        // must use the same table in order for this to be a simple `or`!
                        if same_shape {
                            if expected_table == Some(table) {
                                parity_filters.push(p);
                                continue;
                            }
                            if expected_table.is_none() {
                                expected_table = Some(table);
                                parity_filters.push(p);
                                continue;
                            }
                        }

                        // Otherwise, we need to keep this parity_filter so we can reconstitute.
                        // We'll fall through to reconstruction.
                    }
                }
                last = OrWhereClause::Clause(WhereClause::Pattern(p));
            } else {
                last = clause;
            }

            // If we get here, it means one of our checks above failed. Reconstruct and bail.
            let reconstructed: Vec<OrWhereClause> =
                // Non-empty parity_filters already collected…
                parity_filters.into_iter()
                        .map(|p| OrWhereClause::Clause(WhereClause::Pattern(p)))
                // … then the clause we just considered…
                        .chain(::std::iter::once(last))
                // … then the rest of the iterator.
                        .chain(clauses)
                        .collect();

            return DeconstructedOrJoin::Complex(OrJoin::new(
                UnifyVars::Implicit,
                reconstructed,
            ));
        }

        // If we got here without returning, then `parity_filters` is what we're working with.
        // If `parity_filters` is empty, it means _none_ of the clauses in the `or` could succeed.
        match parity_filters.len() {
            0 => {
                assert!(empty_because.is_some());
                DeconstructedOrJoin::KnownEmpty(empty_because.unwrap())
            },
            1 => DeconstructedOrJoin::UnitPattern(parity_filters.pop().unwrap()),
            _ => DeconstructedOrJoin::Simple(parity_filters, mentioned_vars),
        }
    }

    fn apply_non_trivial_or_join(&mut self, known: KnownCauset, or_join: OrJoin) -> Result<()> {
        match self.deconstruct_or_join(known, or_join) {
            DeconstructedOrJoin::KnownSuccess => {
                // The parity_filter came to us empty -- `(or)`. Do nothing.
                Ok(())
            },
            DeconstructedOrJoin::KnownEmpty(reason) => {
                // There were no arms of the join that could be mapped to a table.
                // The entire `or`, and thus the CC, cannot yield results.
                self.mark_known_empty(reason);
                Ok(())
            },
            DeconstructedOrJoin::Unit(clause) => {
                // There was only one clause. We're unifying all variables, so we can just apply here.
                self.apply_or_where_clause(known, clause)
            },
            DeconstructedOrJoin::UnitPattern(parity_filter) => {
                // Same, but simpler.
                match self.make_evolved_parity_filter(known, parity_filter) {
                    PlaceOrEmpty::Empty(e) => {
                        self.mark_known_empty(e);
                    },
                    PlaceOrEmpty::Place(parity_filter) => {
                        self.apply_parity_filter(known, parity_filter);
                    },
                };
                Ok(())
            },
            DeconstructedOrJoin::Simple(parity_filters, mentioned_vars) => {
                // Hooray! Fully unified and plain ol' parity_filters that all use the same table.
                // Go right ahead and produce a set of constraint alternations that we can collect,
                // using a single table alias.
                self.apply_simple_or_join(known, parity_filters, mentioned_vars)
            },
            DeconstructedOrJoin::Complex(or_join) => {
                // Do this the hard way.
                self.apply_complex_or_join(known, or_join)
            },
        }
    }


    /// A simple `or` join is effectively a single parity_filter in which an individual column's bindings
    /// are not a single value. Rather than a parity_filter like
    ///
    /// ```edbn
    /// [?x :foo/knows "John"]
    /// ```
    ///
    /// we have
    ///
    /// ```edbn
    /// (or [?x :foo/knows "John"]
    ///     [?x :foo/hates "Peter"])
    /// ```
    ///
    /// but the generated SQL is very similar: the former is
    ///
    /// ```sql
    /// WHERE datoms00.a = 99 AND datoms00.v = 'John'
    /// ```
    ///
    /// with the latter growing to
    ///
    /// ```sql
    /// WHERE (datoms00.a = 99 AND datoms00.v = 'John')
    ///    OR (datoms00.a = 98 AND datoms00.v = 'Peter')
    /// ```
    ///
    fn apply_simple_or_join(&mut self,
                            known: KnownCauset,
                            parity_filters: Vec<Pattern>,
                            mentioned_vars: BTreeSet<Variable>)
                            -> Result<()> {
        if self.is_known_empty() {
            return Ok(())
        }

        assert!(parity_filters.len() >= 2);

        let parity_filters: Vec<EvolvedPattern> = parity_filters.into_iter().filter_map(|parity_filter| {
            match self.make_evolved_parity_filter(known, parity_filter) {
                PlaceOrEmpty::Empty(_e) => {
                    // Never mind.
                    None
                },
                PlaceOrEmpty::Place(p) => Some(p),
            }
        }).collect();


        // Begin by building a base CC that we'll use to produce constraints from each parity_filter.
        // Populate this base CC with whatever variables are already known from the CC to which
        // we're applying this `or`.
        // This will give us any applicable type constraints or column mappings.
        // Then generate a single table alias, based on the first parity_filter, and use that to make any
        // new variable mappings we will need to extract values.
        let template = self.use_as_template(&mentioned_vars);

        // We expect this to always work: if it doesn't, it means we should never have got to this
        // point.
        let source_alias = self.alias_table(known.schema, &parity_filters[0]).expect("couldn't get table");

        // This is where we'll collect everything we eventually add to the destination CC.
        let mut folded = ConjoiningClauses::default();

        // Scoped borrow of source_alias.
        {
            // Clone this CC once for each parity_filter.
            // Apply each parity_filter to its CC with the _same_ table alias.
            // Each parity_filter's derived types are intersected with any type constraints in the
            // template, sourced from the destination CC. If a variable cannot satisfy both type
            // constraints, the new CC cannot match. This prunes the 'or' arms:
            //
            // ```edbn
            // [:find ?x
            //  :where [?a :some/int ?x]
            //         (or [_ :some/otherint ?x]
            //             [_ :some/string ?x])]
            // ```
            //
            // can simplify to
            //
            // ```edbn
            // [:find ?x
            //  :where [?a :some/int ?x]
            //         [_ :some/otherint ?x]]
            // ```
            let mut receptacles =
                parity_filters.into_iter()
                        .map(|parity_filter| {
                            let mut receptacle = template.make_receptacle();
                            receptacle.apply_parity_filter_clause_for_alias(known, &parity_filter, &source_alias);
                            receptacle
                        })
                        .peekable();

            // Let's see if we can grab a reason if every parity_filter failed.
            // If every parity_filter failed, we can just take the first!
            let reason = receptacles.peek()
                                    .map(|r| r.empty_because.clone())
                                    .unwrap_or(None);

            // Filter out empties.
            let mut receptacles = receptacles.filter(|receptacle| !receptacle.is_known_empty())
                                             .peekable();

            // We need to copy the column bindings from one of the receptacles. Because this is a simple
            // or, we know that they're all the same.
            // Because we just made an empty template, and created a new alias from the destination CC,
            // we know that we can blindly merge: collisions aren't possible.
            if let Some(first) = receptacles.peek() {
                for (v, cols) in &first.column_bindings {
                    match self.column_bindings.entry(v.clone()) {
                        Entry::Vacant(e) => {
                            e.insert(cols.clone());
                        },
                        Entry::Occupied(mut e) => {
                            e.get_mut().append(&mut cols.clone());
                        },
                    }
                }
            } else {
                // No non-empty receptacles? The destination CC is known-empty, because or([]) is false.
                self.mark_known_empty(reason.unwrap_or(EmptyBecause::AttributeLookupFailed));
                return Ok(());
            }

            // Otherwise, we fold together the receptacles.
            //
            // Merge together the constraints from each receptacle. Each bundle of constraints is
            // combined into a `ConstraintIntersection`, and the collection of intersections is
            // combined into a `ConstraintAlternation`. (As an optimization, this collection can be
            // simplified.)
            //
            // Each receptacle's known types are _unioned_. Strictly speaking this is a weakening:
            // we might know that if `?x` is an integer then `?y` is a string, or vice versa, but at
            // this point we'll simply state that `?x` and `?y` can both be integers or strings.

            fn vec_for_iterator<T, I, U>(iter: &I) -> Vec<T> where I: Iterator<Item=U> {
                match iter.size_hint().1 {
                    None => Vec::new(),
                    Some(expected) => Vec::with_capacity(expected),
                }
            }

            let mut alternates: Vec<ColumnIntersection> = vec_for_iterator(&receptacles);
            for r in receptacles {
                folded.broaden_types(r.known_types);
                alternates.push(r.wheres);
            }

            if alternates.len() == 1 {
                // Simplify.
                folded.wheres = alternates.pop().unwrap();
            } else {
                let alternation = ColumnAlternation(alternates);
                let mut container = ColumnIntersection::default();
                container.add(ColumnConstraintOrAlternation::Alternation(alternation));
                folded.wheres = container;
            }
        }

        // Collect the source alias: we use a single table join to represent the entire `or`.
        self.from.push(source_alias);

        // Add in the known types and constraints.
        // Each constant attribute might _expand_ the set of possible types of the value-place
        // variable. We thus generate a set of possible types, and we intersect it with the
        // types already possible in the CC. If the resultant set is empty, the parity_filter cannot
        // match. If the final set isn't unit, we must project a type tag column.
        self.intersect(folded)
    }

    fn intersect(&mut self, mut cc: ConjoiningClauses) -> Result<()> {
        if cc.is_known_empty() {
            self.empty_because = cc.empty_because;
        }
        self.wheres.append(&mut cc.wheres);
        self.narrow_types(cc.known_types);
        Ok(())
    }


    ///
    /// Note that a top-level standalone `or` doesn't really need to be aliased, but
    /// it shouldn't do any harm.
    fn apply_complex_or_join(&mut self, known: KnownCauset, or_join: OrJoin) -> Result<()> {
        // N.B., a solitary parity_filter here *cannot* be simply applied to the enclosing CC. We don't
        // want to join all the vars, and indeed if it were safe to do so, we wouldn't have ended up
        // in this function!
        let (join_clauses, unify_vars, mentioned_vars) = or_join.dismember();
        let projected = match unify_vars {
            UnifyVars::Implicit => mentioned_vars.into_iter().collect(),
            UnifyVars::Explicit(vs) => vs,
        };

        let template = self.use_as_template(&projected);

        let mut acc = Vec::with_capacity(join_clauses.len());
        let mut empty_because: Option<EmptyBecause> = None;

        for clause in join_clauses.into_iter() {
            let mut receptacle = template.make_receptacle();
            match clause {
                OrWhereClause::And(clauses) => {
                    receptacle.apply_clauses(known, clauses)?;
                },
                OrWhereClause::Clause(clause) => {
                    receptacle.apply_clause(known, clause)?;
                },
            }
            if receptacle.is_known_empty() {
                empty_because = receptacle.empty_because;
            } else {
                receptacle.expand_column_bindings();
                receptacle.prune_extracted_types();
                receptacle.process_required_types()?;
                acc.push(receptacle);
            }
        }

        if acc.is_empty() {
            self.mark_known_empty(empty_because.expect("empty for a reason"));
            return Ok(());
        }

        // TODO: optimize the case of a single element in `acc`?

        // Now `acc` contains a sequence of CCs that were all prepared with the same types,
        // each ready to project the same variables.
        // At this point we can lift out any common type information (and even constraints) to the
        // destination CC.
        // We must also contribute type extraction information for any variables that aren't
        // concretely typed for all union arms.
        //
        // We walk the list of variables to unify -- which will become our projection
        // list -- to find out its type info in each CC. We might:
        //
        // 1. Know the type concretely from the enclosing CC. Don't project a type tag from the
        //    union. Example:
        //    ```
        //    [:find ?x ?y
        //     :where [?x :foo/int ?y]
        //            (or [(< ?y 10)]
        //                [_ :foo/verified ?y])]
        //    ```
        // 2. Not know the type, but every CC bound it to the same single type. Don't project a type
        //    tag; we simply contribute the single type to the enclosing CC. Example:
        //    ```
        //    [:find ?x ?y
        //     :where (or [?x :foo/length ?y]
        //                [?x :foo/width ?y])]
        //    ```
        // 3. (a) Have every CC come up with a non-unit type set for the var. Every CC will project
        //        a type tag column from one of its internal bindings, and the union will project it
        //        onwards. Example:
        //        ```
        //        [:find ?x ?y ?z
        //         :where [?x :foo/knows ?y]
        //                (or [?x _ ?z]
        //                    [?y _ ?z])]
        //        ```
        // 3. (b) Have some or all CCs come up with a unit type set. Every CC will project a type
        //        tag column, and those with a unit type set will project a fixed constant value.
        //        Again, the union will pass this on.
        //        ```
        //        [:find ?x ?y
        //         :where (or [?x :foo/length ?y]
        //                    [?x _ ?y])]
        //        ```
        let projection: BTreeSet<Variable> = projected.into_iter().collect();
        let mut type_needed: BTreeSet<Variable> = BTreeSet::default();

        // For any variable which has an imprecise type anywhere in the UNION, add it to the
        // set that needs type extraction. All UNION arms must project the same columns.
        for var in projection.iter() {
            if acc.iter().any(|cc| !cc.known_type(var).is_some()) {
                type_needed.insert(var.clone());
            }
        }

        // Hang on to these so we can stuff them in our column bindings.
        let var_associations: Vec<Variable>;
        let type_associations: Vec<Variable>;
        {
            var_associations = projection.iter().cloned().collect();
            type_associations = type_needed.iter().cloned().collect();
        }

        // Collect the new type information from the arms. There's some redundant work here --
        // they already have all of the information from the parent.
        // Note that we start with the first clause's type information.
        {
            let mut clauses = acc.iter();
            let mut additional_types = clauses.next()
                                              .expect("there to be at least one clause")
                                              .known_types
                                              .clone();
            for cc in clauses {
                union_types(&mut additional_types, &cc.known_types);
            }
            self.broaden_types(additional_types);
        }

        let union = ComputedTable::Union {
            projection: projection,
            type_extraction: type_needed,
            arms: acc,
        };
        let table = self.computed_tables.push_computed(union);
        let alias = self.next_alias_for_table(table);

        // Stitch the computed table into column_bindings, so we get cross-linking.
        let schema = known.schema;
        for var in var_associations.into_iter() {
            self.bind_column_to_var(schema, alias.clone(), VariableColumn::Variable(var.clone()), var);
        }
        for var in type_associations.into_iter() {
            self.extracted_types.insert(var.clone(), QualifiedAlias::new(alias.clone(), VariableColumn::VariableTypeTag(var)));
        }
        self.from.push(SourceAlias(table, alias));
        Ok(())
    }
}

/// Helper to fold together a set of type maps.
fn union_types(into: &mut BTreeMap<Variable, ValueTypeSet>,
               additional_types: &BTreeMap<Variable, ValueTypeSet>) {
    // We want the exclusive disjunction -- any variable not mentioned in both sets -- to default
    // to ValueTypeSet::Any.
    // This is necessary because we lazily populate known_types, so sometimes the type set will
    // be missing a `ValueTypeSet::Any` for a variable, and we want to broaden rather than
    // accsolitonidally taking the other side's word for it!
    // The alternative would be to exhaustively pre-fill `known_types` with all mentioned variables
    // in the whole query, which is daunting.
    let mut any: BTreeMap<Variable, ValueTypeSet>;
    // Scoped borrow of `into`.
    {
        let i: BTreeSet<&Variable> = into.keys().collect();
        let a: BTreeSet<&Variable> = additional_types.keys().collect();
        any = i.symmetric_difference(&a)
               .map(|v| ((*v).clone(), ValueTypeSet::any()))
               .collect();
    }

    // Collect the additional types.
    for (var, new_types) in additional_types {
        match into.entry(var.clone()) {
            Entry::Vacant(e) => {
                e.insert(new_types.clone());
            },
            Entry::Occupied(mut e) => {
                let new = e.get().union(&new_types);
                e.insert(new);
            },
        }
    }

    // Blat in those that are disjoint.
    into.append(&mut any);
}

#[cfg(test)]
mod testing {
    use super::*;

    use embedded_promises::{
        Attribute,
        ValueType,
        TypedValue,
    };

    use einsteindb_embedded::{
        Schema,
    };

    use edbn::query::{
        Keyword,
        Variable,
    };

    use clauses::{
        add_attribute,
        associate_solitonid,
    };

    use types::{
        ColumnConstraint,
        CausetColumn,
        CausetTable,
        Inequality,
        QualifiedAlias,
        QueryValue,
        SourceAlias,
    };

    use {
        algebrize,
        algebrize_with_counter,
        parse_find_string,
    };

    fn alg(known: KnownCauset, input: &str) -> ConjoiningClauses {
        let parsed = parse_find_string(input).expect("parse failed");
        algebrize(known, parsed).expect("algebrize failed").cc
    }


    fn alg_c(known: KnownCauset, counter: usize, input: &str) -> ConjoiningClauses {
        let parsed = parse_find_string(input).expect("parse failed");
        algebrize_with_counter(known, parsed, counter).expect("algebrize failed").cc
    }

    fn compare_ccs(left: ConjoiningClauses, right: ConjoiningClauses) {
        assert_eq!(left.wheres, right.wheres);
        assert_eq!(left.from, right.from);
    }

    fn prepopulated_schema() -> Schema {
        let mut schema = Schema::default();
        associate_solitonid(&mut schema, Keyword::namespaced("foo", "name"), 65);
        associate_solitonid(&mut schema, Keyword::namespaced("foo", "knows"), 66);
        associate_solitonid(&mut schema, Keyword::namespaced("foo", "parent"), 67);
        associate_solitonid(&mut schema, Keyword::namespaced("foo", "age"), 68);
        associate_solitonid(&mut schema, Keyword::namespaced("foo", "height"), 69);
        add_attribute(&mut schema, 65, Attribute {
            value_type: ValueType::String,
            multival: false,
            ..Default::default()
        });
        add_attribute(&mut schema, 66, Attribute {
            value_type: ValueType::String,
            multival: true,
            ..Default::default()
        });
        add_attribute(&mut schema, 67, Attribute {
            value_type: ValueType::String,
            multival: true,
            ..Default::default()
        });
        add_attribute(&mut schema, 68, Attribute {
            value_type: ValueType::Long,
            multival: false,
            ..Default::default()
        });
        add_attribute(&mut schema, 69, Attribute {
            value_type: ValueType::Long,
            multival: false,
            ..Default::default()
        });
        schema
    }

    /// Test that if all the attributes in an `or` fail to resolve, the entire thing fails.
    #[test]
    fn test_schema_based_failure() {
        let schema = Schema::default();
        let known = KnownCauset::for_schema(&schema);
        let query = r#"
            [:find ?x
             :where (or [?x :foo/nope1 "John"]
                        [?x :foo/nope2 "Ámbar"]
                        [?x :foo/nope3 "Daphne"])]"#;
        let cc = alg(known, query);
        assert!(cc.is_known_empty());
        assert_eq!(cc.empty_because, Some(EmptyBecause::Unresolvedsolitonid(Keyword::namespaced("foo", "nope3"))));
    }

    /// Test that if only one of the attributes in an `or` resolves, it's equivalent to a simple query.
    #[test]
    fn test_only_one_arm_succeeds() {
        let schema = prepopulated_schema();
        let known = KnownCauset::for_schema(&schema);
        let query = r#"
            [:find ?x
             :where (or [?x :foo/nope "John"]
                        [?x :foo/parent "Ámbar"]
                        [?x :foo/nope "Daphne"])]"#;
        let cc = alg(known, query);
        assert!(!cc.is_known_empty());
        compare_ccs(cc, alg(known, r#"[:find ?x :where [?x :foo/parent "Ámbar"]]"#));
    }

    // Simple alternation.
    #[test]
    fn test_simple_alternation() {
        let schema = prepopulated_schema();
        let known = KnownCauset::for_schema(&schema);
        let query = r#"
            [:find ?x
             :where (or [?x :foo/knows "John"]
                        [?x :foo/parent "Ámbar"]
                        [?x :foo/knows "Daphne"])]"#;
        let cc = alg(known, query);
        let vx = Variable::from_valid_name("?x");
        let d0 = "datoms00".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), DatomsColumn::Causets);
        let d0a = QualifiedAlias::new(d0.clone(), DatomsColumn::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), DatomsColumn::Value);
        let knows = QueryValue::Causetid(66);
        let parent = QueryValue::Causetid(67);
        let john = QueryValue::TypedValue(TypedValue::typed_string("John"));
        let ambar = QueryValue::TypedValue(TypedValue::typed_string("Ámbar"));
        let daphne = QueryValue::TypedValue(TypedValue::typed_string("Daphne"));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, ColumnIntersection(vec![
            ColumnConstraintOrAlternation::Alternation(
                ColumnAlternation(vec![
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), knows.clone())),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0v.clone(), john))]),
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), parent)),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0v.clone(), ambar))]),
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), knows)),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0v.clone(), daphne))]),
                    ]))]));
        assert_eq!(cc.column_bindings.get(&vx), Some(&vec![d0e]));
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, d0)]);
    }

    // Alternation with a parity_filter.
    #[test]
    fn test_alternation_with_parity_filter() {
        let schema = prepopulated_schema();
        let known = KnownCauset::for_schema(&schema);
        let query = r#"
            [:find [?x ?name]
             :where
             [?x :foo/name ?name]
             (or [?x :foo/knows "John"]
                 [?x :foo/parent "Ámbar"]
                 [?x :foo/knows "Daphne"])]"#;
        let cc = alg(known, query);
        let vx = Variable::from_valid_name("?x");
        let d0 = "datoms00".to_string();
        let d1 = "datoms01".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), DatomsColumn::Causets);
        let d0a = QualifiedAlias::new(d0.clone(), DatomsColumn::Attribute);
        let d1e = QualifiedAlias::new(d1.clone(), DatomsColumn::Causets);
        let d1a = QualifiedAlias::new(d1.clone(), DatomsColumn::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), DatomsColumn::Value);
        let name = QueryValue::Causetid(65);
        let knows = QueryValue::Causetid(66);
        let parent = QueryValue::Causetid(67);
        let john = QueryValue::TypedValue(TypedValue::typed_string("John"));
        let ambar = QueryValue::TypedValue(TypedValue::typed_string("Ámbar"));
        let daphne = QueryValue::TypedValue(TypedValue::typed_string("Daphne"));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, ColumnIntersection(vec![
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), name.clone())),
            ColumnConstraintOrAlternation::Alternation(
                ColumnAlternation(vec![
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), knows.clone())),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), john))]),
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), parent)),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), ambar))]),
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), knows)),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), daphne))]),
                    ])),
            // The outer parity_filter joins against the `or`.
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d1e.clone()))),
        ]));
        assert_eq!(cc.column_bindings.get(&vx), Some(&vec![d0e, d1e]));
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, d0),
                                 SourceAlias(DatomsTable::Datoms, d1)]);
    }

    // Alternation with a parity_filter and a predicate.
    #[test]
    fn test_alternation_with_parity_filter_and_predicate() {
        let schema = prepopulated_schema();
        let known = KnownCauset::for_schema(&schema);
        let query = r#"
            [:find ?x ?age
             :where
             [?x :foo/age ?age]
             [(< ?age 30)]
             (or [?x :foo/knows "John"]
                 [?x :foo/knows "Daphne"])]"#;
        let cc = alg(known, query);
        let vx = Variable::from_valid_name("?x");
        let d0 = "datoms00".to_string();
        let d1 = "datoms01".to_string();
        let d0e = QualifiedAlias::new(d0.clone(), DatomsColumn::Causets);
        let d0a = QualifiedAlias::new(d0.clone(), DatomsColumn::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), DatomsColumn::Value);
        let d1e = QualifiedAlias::new(d1.clone(), DatomsColumn::Causets);
        let d1a = QualifiedAlias::new(d1.clone(), DatomsColumn::Attribute);
        let d1v = QualifiedAlias::new(d1.clone(), DatomsColumn::Value);
        let knows = QueryValue::Causetid(66);
        let age = QueryValue::Causetid(68);
        let john = QueryValue::TypedValue(TypedValue::typed_string("John"));
        let daphne = QueryValue::TypedValue(TypedValue::typed_string("Daphne"));

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, ColumnIntersection(vec![
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), age.clone())),
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Inequality {
                operator: Inequality::LessThan,
                left: QueryValue::Column(d0v.clone()),
                right: QueryValue::TypedValue(TypedValue::Long(30)),
            }),
            ColumnConstraintOrAlternation::Alternation(
                ColumnAlternation(vec![
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), knows.clone())),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), john))]),
                    ColumnIntersection(vec![
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1a.clone(), knows)),
                        ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d1v.clone(), daphne))]),
                    ])),
            // The outer parity_filter joins against the `or`.
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(d1e.clone()))),
        ]));
        assert_eq!(cc.column_bindings.get(&vx), Some(&vec![d0e, d1e]));
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, d0),
                                 SourceAlias(DatomsTable::Datoms, d1)]);
    }

    // These two are not equivalent:
    // [:find ?x :where [?x :foo/bar ?y] (or-join [?x] [?x :foo/baz ?y])]
    // [:find ?x :where [?x :foo/bar ?y] [?x :foo/baz ?y]]
    #[test]
    fn test_unit_or_join_doesnt_flatten() {
        let schema = prepopulated_schema();
        let known = KnownCauset::for_schema(&schema);
        let query = r#"[:find ?x
                        :where [?x :foo/knows ?y]
                               (or-join [?x] [?x :foo/parent ?y])]"#;
        let cc = alg(known, query);
        let vx = Variable::from_valid_name("?x");
        let vy = Variable::from_valid_name("?y");
        let d0 = "datoms00".to_string();
        let c0 = "c00".to_string();
        let c0x = QualifiedAlias::new(c0.clone(), VariableColumn::Variable(vx.clone()));
        let d0e = QualifiedAlias::new(d0.clone(), DatomsColumn::Causets);
        let d0a = QualifiedAlias::new(d0.clone(), DatomsColumn::Attribute);
        let d0v = QualifiedAlias::new(d0.clone(), DatomsColumn::Value);
        let knows = QueryValue::Causetid(66);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.wheres, ColumnIntersection(vec![
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0a.clone(), knows.clone())),
            // The outer parity_filter joins against the `or` on the entity, but not value -- ?y means
            // different things in each place.
            ColumnConstraintOrAlternation::Constraint(ColumnConstraint::Equals(d0e.clone(), QueryValue::Column(c0x.clone()))),
        ]));
        assert_eq!(cc.column_bindings.get(&vx), Some(&vec![d0e, c0x]));

        // ?y does not have a binding in the `or-join` parity_filter.
        assert_eq!(cc.column_bindings.get(&vy), Some(&vec![d0v]));
        assert_eq!(cc.from, vec![SourceAlias(DatomsTable::Datoms, d0),
                                 SourceAlias(DatomsTable::Computed(0), c0)]);
    }

    // These two are equivalent:
    // [:find ?x :where [?x :foo/bar ?y] (or [?x :foo/baz ?y])]
    // [:find ?x :where [?x :foo/bar ?y] [?x :foo/baz ?y]]
    #[test]
    fn test_unit_or_does_flatten() {
        let schema = prepopulated_schema();
        let known = KnownCauset::for_schema(&schema);
        let or_query =   r#"[:find ?x
                             :where [?x :foo/knows ?y]
                                    (or [?x :foo/parent ?y])]"#;
        let flat_query = r#"[:find ?x
                             :where [?x :foo/knows ?y]
                                    [?x :foo/parent ?y]]"#;
        compare_ccs(alg(known, or_query),
                    alg(known, flat_query));
    }

    // Elision of `and`.
    #[test]
    fn test_unit_or_and_does_flatten() {
        let schema = prepopulated_schema();
        let known = KnownCauset::for_schema(&schema);
        let or_query =   r#"[:find ?x
                             :where (or (and [?x :foo/parent ?y]
                                             [?x :foo/age 7]))]"#;
        let flat_query =   r#"[:find ?x
                               :where [?x :foo/parent ?y]
                                      [?x :foo/age 7]]"#;
        compare_ccs(alg(known, or_query),
                    alg(known, flat_query));
    }

    // Alternation with `and`.
    /// [:find ?x
    ///  :where (or (and [?x :foo/knows "John"]
    ///                  [?x :foo/parent "Ámbar"])
    ///             [?x :foo/knows "Daphne"])]
    /// Strictly speaking this can be implemented with a `NOT EXISTS` clause for the second parity_filter,
    /// but that would be a fair amount of analysis work, I think.
    #[test]
    fn test_alternation_with_and() {
        let schema = prepopulated_schema();
        let known = KnownCauset::for_schema(&schema);
        let query = r#"
            [:find ?x
             :where (or (and [?x :foo/knows "John"]
                             [?x :foo/parent "Ámbar"])
                        [?x :foo/knows "Daphne"])]"#;
        let cc = alg(known, query);
        let mut tables = cc.computed_tables.into_iter();
        match (tables.next(), tables.next()) {
            (Some(ComputedTable::Union { projection, type_extraction, arms }), None) => {
                assert_eq!(projection, vec![Variable::from_valid_name("?x")].into_iter().collect());
                assert!(type_extraction.is_empty());

                let mut arms = arms.into_iter();
                match (arms.next(), arms.next(), arms.next()) {
                    (Some(and), Some(parity_filter), None) => {
                        let expected_and = alg_c(known,
                                                 0,  // The first parity_filter to be processed.
                                                 r#"[:find ?x :where [?x :foo/knows "John"] [?x :foo/parent "Ámbar"]]"#);
                        compare_ccs(and, expected_and);

                        let expected_parity_filter = alg_c(known,
                                                     2,      // Two aliases taken by the other arm.
                                                     r#"[:find ?x :where [?x :foo/knows "Daphne"]]"#);
                        compare_ccs(parity_filter, expected_parity_filter);
                    },
                    _ => {
                        panic!("Expected two arms");
                    }
                }
            },
            _ => {
                panic!("Didn't get two inner tables.");
            },
        }
    }

    #[test]
    fn test_type_based_or_pruning() {
        let schema = prepopulated_schema();
        let known = KnownCauset::for_schema(&schema);
        // This simplifies to:
        // [:find ?x
        //  :where [?a :some/int ?x]
        //         [_ :some/otherint ?x]]
        let query = r#"
            [:find ?x
             :where [?a :foo/age ?x]
                    (or [_ :foo/height ?x]
                        [_ :foo/name ?x])]"#;
        let simple = r#"
            [:find ?x
             :where [?a :foo/age ?x]
                    [_ :foo/height ?x]]"#;
        compare_ccs(alg(known, query), alg(known, simple));
    }
}
