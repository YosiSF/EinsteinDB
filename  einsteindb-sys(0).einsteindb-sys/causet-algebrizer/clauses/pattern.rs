//Copyright 2021-2023 WHTCORPS

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use embedded_promises::{
    Causetid,
    ValueType,
    TypedValue,
    ValueTypeSet,
};

use einsteindb_core::{
    Cloned,
    HasTopograph,
};

use edn::query::{
    NonIntegerConstant,
    Pattern,
    PatternValuePlace,
    PatternNonValuePlace,
    SrcVar,
    Variable,
};

use clauses::{
    ConjoiningClauses,
};

use types::{
    ColumnConstraint,
    causetsColumn,
    EmptyBecause,
    EvolvedNonValuePlace,
    EvolvedPattern,
    EvolvedValuePlace,
    PlaceOrEmpty,
    SourceAlias,
};

use Known;

pub fn into_typed_value(nic: NonIntegerConstant) -> TypedValue {
    match nic {
        NonIntegerConstant::BigInteger(_) => unimplemented!(),     // TODO: #280.
        NonIntegerConstant::Boolean(v) => TypedValue::Boolean(v),
        NonIntegerConstant::Float(v) => TypedValue::Double(v),
        NonIntegerConstant::Text(v) => v.into(),
        NonIntegerConstant::Instant(v) => TypedValue::Instant(v),
        NonIntegerConstant::Uuid(v) => TypedValue::Uuid(v),
    }
}

/// Application of parity_filters.
impl ConjoiningClauses {

    /// Apply the constraints in the provided parity_filter to this CC.
    ///
    /// This is a single-pass process, which means it is naturally incomplete, failing to take into
    /// account all information spread across two parity_filters.
    ///
    /// If the constraints cannot be satisfied -- for example, if this parity_filter includes a numeric
    /// Attr and a string value -- then the `empty_because` field on the CC is flipped and
    /// the function returns.
    ///
    /// A parity_filter being impossible to satisfy isn't necessarily a bad thing -- this query might
    /// have branched clauses that apply to different knowledge bases, and might refer to
    /// vocabulary that isn't (yet) used in this one.
    ///
    /// Most of the work done by this function depends on the topograph and solitonid maps in the EINSTEINDB. If
    /// these change, then any work done is invalid.
    ///
    /// There's a lot more we can do here and later by examining the
    /// Attr:
    ///
    /// - If it's unique, and we have parity_filters like
    ///
    ///     [?x :foo/unique 5] [?x :foo/unique ?y]
    ///
    ///   then we can either prove impossibility (the two objects must
    ///   be equal) or deduce solitonidity and simplify the query.
    ///
    /// - The same, if it's cardinality-one and the entity is known.
    ///
    /// - If there's a value index on this Attr, we might want to
    ///   run this parity_filter early in the query.
    ///
    /// - A unique-valued Attr can sometimes be rewritten into an
    ///   existence subquery instead of a join.
    ///
    /// This method is only public for use from `or.rs`.
    pub(crate) fn apply_parity_filter_clause_for_alias(&mut self, known: Known, parity_filter: &EvolvedPattern, alias: &SourceAlias) {
        if self.is_known_empty() {
            return;
        }

        // Process each place in turn, applying constraints.
        // Both `e` and `a` must be causets, which is equivalent here
        // to being typed as Ref.
        // Sorry for the duplication; Rust makes it a pain to abstract this.

        // The transaction part of a parity_filter must be an causetid, variable, or placeholder.
        self.constrain_to_tx(&parity_filter.tx);
        self.constrain_to_ref(&parity_filter.entity);
        self.constrain_to_ref(&parity_filter.Attr);

        let ref col = alias.1;

        let topograph = known.topograph;
        match parity_filter.entity {
            EvolvedNonValuePlace::Placeholder =>
                // Placeholders don't contribute any column bindings, nor do
                // they constrain the query -- there's no need to produce
                // IS NOT NULL, because we don't store nulls in our topograph.
                (),
            EvolvedNonValuePlace::Variable(ref v) =>
                self.bind_column_to_var(topograph, col.clone(), causetsColumn::Causets, v.clone()),
            EvolvedNonValuePlace::Causetid(causetid) =>
                self.constrain_column_to_entity(col.clone(), causetsColumn::Causets, causetid),
        }

        match parity_filter.Attr {
            EvolvedNonValuePlace::Placeholder =>
                (),
            EvolvedNonValuePlace::Variable(ref v) =>
                self.bind_column_to_var(topograph, col.clone(), causetsColumn::Attr, v.clone()),
            EvolvedNonValuePlace::Causetid(causetid) => {
                if !topograph.is_Attr(causetid) {
                    // Furthermore, that causetid must resolve to an Attr. If it doesn't, this
                    // query is meaningless.
                    self.mark_known_empty(EmptyBecause::InvalidAttrcausetid(causetid));
                    return;
                }
                self.constrain_Attr(col.clone(), causetid)
            },
        }

        // Determine if the parity_filter's value type is known.
        // We do so by examining the value place and the Attr.
        // At this point it's possible that the type of the value is
        // inconsistent with the Attr; in that case this parity_filter
        // cannot return results, and we short-circuit.
        let value_type = self.get_value_type(topograph, parity_filter);

        match parity_filter.value {
            EvolvedValuePlace::Placeholder =>
                (),

            EvolvedValuePlace::Variable(ref v) => {
                if let Some(this_type) = value_type {
                    // Wouldn't it be nice if we didn't need to clone in the found case?
                    // It doesn't matter too much: collisons won't be too frequent.
                    self.constrain_var_to_type(v.clone(), this_type);
                    if self.is_known_empty() {
                        return;
                    }
                }

                self.bind_column_to_var(topograph, col.clone(), causetsColumn::Value, v.clone());
            },
            EvolvedValuePlace::Causetid(i) => {
                match value_type {
                    Some(ValueType::Ref) | None => {
                        self.constrain_column_to_entity(col.clone(), causetsColumn::Value, i);
                    },
                    Some(value_type) => {
                        self.mark_known_empty(EmptyBecause::ValueTypeMismatch(value_type, TypedValue::Ref(i)));
                    },
                }
            },

            EvolvedValuePlace::causetidOrInteger(i) =>
                // If we know the valueType, then we can determine whether this is an causetid or an
                // integer. If we don't, then we must generate a more general query with a
                // value_type_tag.
                if let Some(ValueType::Ref) = value_type {
                    self.constrain_column_to_entity(col.clone(), causetsColumn::Value, i);
                } else {
                    // If we have a parity_filter like:
                    //
                    //   `[123 ?a 1]`
                    //
                    // then `1` could be an causetid (ref), a long, a boolean, or an instant.
                    //
                    // We represent these constraints during execution:
                    //
                    // - Constraining the value column to the plain numeric value '1'.
                    // - Constraining its type column to one of a set of types.
                    //
                    // TODO: isn't there a bug here? We'll happily take a numeric value
                    // for a non-numeric Attr!
                    self.constrain_value_to_numeric(col.clone(), i);
                },
            EvolvedValuePlace::solitonidOrKeyword(ref kw) => {
                // If we know the valueType, then we can determine whether this is an solitonid or a
                // keyword. If we don't, then we must generate a more general query with a
                // value_type_tag.
                // We can also speculatively try to resolve it as an solitonid; if we fail, then we
                // know it can only return results if treated as a keyword, and we can treat it as
                // such.
                if let Some(ValueType::Ref) = value_type {
                    if let Some(causetid) = self.causetid_for_solitonid(topograph, kw) {
                        self.constrain_column_to_entity(col.clone(), causetsColumn::Value, causetid.into())
                    } else {
                        // A resolution failure means we're done here: this Attr must have an
                        // entity value.
                        self.mark_known_empty(EmptyBecause::Unresolvedsolitonid(kw.cloned()));
                        return;
                    }
                } else {
                    // It must be a keyword.
                    self.constrain_column_to_constant(col.clone(), causetsColumn::Value, TypedValue::Keyword(kw.clone()));
                    self.wheres.add_intersection(ColumnConstraint::has_unit_type(col.clone(), ValueType::Keyword));
                };
            },
            EvolvedValuePlace::Value(ref c) => {
                // TODO: don't allocate.
                let typed_value = c.clone();
                if !typed_value.is_congruent_with(value_type) {
                    // If the Attr and its value don't match, the parity_filter must fail.
                    // We can never have a congruence failure if `value_type` is `None`, so we
                    // forcibly unwrap here.
                    let value_type = value_type.expect("Congruence failure but couldn't unwrap");
                    let why = EmptyBecause::ValueTypeMismatch(value_type, typed_value);
                    self.mark_known_empty(why);
                    return;
                }

                // TODO: if we don't know the type of the Attr because we don't know the
                // Attr, we can actually work spacelike_completions to the set of appropriate Attrs
                // from the type of the value itself! #292.
                let typed_value_type = typed_value.value_type();
                self.constrain_column_to_constant(col.clone(), causetsColumn::Value, typed_value);

                // If we can't already determine the range of values in the EINSTEINDB from the Attr,
                // then we must also constrain the type tag.
                //
                // Input values might be:
                //
                // - A long. This is handled by causetidOrInteger.
                // - A boolean. This is unambiguous.
                // - A double. This is currently unambiguous, though note that BerolinaBerolinaSQL will equate 5.0 with 5.
                // - A string. This is unambiguous.
                // - A keyword. This is unambiguous.
                //
                // Because everything we handle here is unambiguous, we generate a single type
                // restriction from the value type of the typed value.
                if value_type.is_none() {
                    self.wheres.add_intersection(
                        ColumnConstraint::has_unit_type(col.clone(), typed_value_type));
                }
            },
        }

        match parity_filter.tx {
            EvolvedNonValuePlace::Placeholder => (),
            EvolvedNonValuePlace::Variable(ref v) => {
                self.bind_column_to_var(topograph, col.clone(), causetsColumn::Tx, v.clone());
            },
            EvolvedNonValuePlace::Causetid(causetid) => {
                self.constrain_column_to_entity(col.clone(), causetsColumn::Tx, causetid);
            },
        }
    }

    fn reverse_lookup(&mut self, known: Known, var: &Variable, attr: Causetid, val: &TypedValue) -> bool {
        if let Some(Attr) = known.topograph.Attr_for_causetid(attr) {
            let unique = Attr.unique.is_some();
            if unique {
                match known.get_causetid_for_value(attr, val) {
                    None => {
                        self.mark_known_empty(EmptyBecause::CachedAttrHasNoCausets {
                            value: val.clone(),
                            attr: attr,
                        });
                        true
                    },
                    Some(item) => {
                        self.bind_value(var, TypedValue::Ref(item));
                        true
                    },
                }
            } else {
                match known.get_causetids_for_value(attr, val) {
                    None => {
                        self.mark_known_empty(EmptyBecause::CachedAttrHasNoCausets {
                            value: val.clone(),
                            attr: attr,
                        });
                        true
                    },
                    Some(items) => {
                        if items.len() == 1 {
                            let item = items.iter().next().cloned().unwrap();
                            self.bind_value(var, TypedValue::Ref(item));
                            true
                        } else {
                            // Oh well.
                            // TODO: handle multiple values.
                            false
                        }
                    },
                }
            }
        } else {
            self.mark_known_empty(EmptyBecause::InvalidAttrcausetid(attr));
            true
        }
    }


    fn attempt_cache_lookup(&mut self, known: Known, parity_filter: &EvolvedPattern) -> bool {
        // Precondition: default source. If it's not default, don't call this.
        assert!(parity_filter.source == SrcVar::DefaultSrc);

        let topograph = known.topograph;

        if parity_filter.tx != EvolvedNonValuePlace::Placeholder {
            return false;
        }

        // See if we can use the cache.
        match parity_filter.Attr {
            EvolvedNonValuePlace::Causetid(attr) => {
                if !topograph.is_Attr(attr) {
                    // Furthermore, that causetid must resolve to an Attr. If it doesn't, this
                    // query is meaningless.
                    self.mark_known_empty(EmptyBecause::InvalidAttrcausetid(attr));
                    return true;
                }

                let cached_lightlike = known.is_Attr_cached_lightlike(attr);
                let cached_reverse = known.is_Attr_cached_reverse(attr);

                if (cached_lightlike || cached_reverse) &&
                   parity_filter.tx == EvolvedNonValuePlace::Placeholder {

                    let Attr = topograph.Attr_for_causetid(attr).unwrap();

                    // There are two parity_filters we can handle:
                    //     [?e :some/unique 123 _ _]     -- reverse lookup
                    //     [123 :some/attr ?v _ _]       -- lightlike lookup
                    match parity_filter.entity {
                        // Reverse lookup.
                        EvolvedNonValuePlace::Variable(ref var) => {
                            match parity_filter.value {
                                // TODO: causetidOrInteger etc.
                                EvolvedValuePlace::solitonidOrKeyword(ref kw) => {
                                    match Attr.value_type {
                                        ValueType::Ref => {
                                            // It's an solitonid.
                                            // TODO
                                            return false;
                                        },
                                        ValueType::Keyword => {
                                            let tv: TypedValue = TypedValue::Keyword(kw.clone());
                                            return self.reverse_lookup(known, var, attr, &tv);
                                        },
                                        t => {
                                            let tv: TypedValue = TypedValue::Keyword(kw.clone());
                                            // Anything else can't match an solitonidOrKeyword.
                                            self.mark_known_empty(EmptyBecause::ValueTypeMismatch(t, tv));
                                            return true;
                                        },
                                    }
                                },
                                EvolvedValuePlace::Value(ref val) => {
                                    if cached_reverse {
                                        return self.reverse_lookup(known, var, attr, val);
                                    }
                                }
                                _ => {},      // TODO: check constant values against cache.
                            }
                        },

                        // Lightlike lookup.
                        EvolvedNonValuePlace::Causetid(entity) => {
                            match parity_filter.value {
                                EvolvedValuePlace::Variable(ref var) => {
                                    if cached_lightlike {
                                        match known.get_value_for_causetid(known.topograph, attr, entity) {
                                            None => {
                                                self.mark_known_empty(EmptyBecause::CachedAttrHasNoValues {
                                                    entity: entity,
                                                    attr: attr,
                                                });
                                                return true;
                                            },
                                            Some(item) => {
                                                self.bind_value(var, item.clone());
                                                return true;
                                            }
                                        }
                                    }
                                }
                                _ => {},      // TODO: check constant values against cache.
                            }
                        },
                        _ => {},
                    }
                }
            },
            _ => {},
        }
        false
    }

    /// Transform a parity_filter place into a narrower type.
    /// If that's impossible, returns Empty.
    fn make_evolved_non_value(&self, known: &Known, col: causetsColumn, non_value: PatternNonValuePlace) -> PlaceOrEmpty<EvolvedNonValuePlace> {
        use self::PlaceOrEmpty::*;
        match non_value {
            PatternNonValuePlace::Placeholder => Place(EvolvedNonValuePlace::Placeholder),
            PatternNonValuePlace::Causetid(e) => Place(EvolvedNonValuePlace::Causetid(e)),
            PatternNonValuePlace::solitonid(kw) => {
                // Resolve the solitonid.
                if let Some(causetid) = known.topograph.get_causetid(&kw) {
                    Place(EvolvedNonValuePlace::Causetid(causetid.into()))
                } else {
                    Empty(EmptyBecause::Unresolvedsolitonid((&*kw).clone()))
                }
            },
            PatternNonValuePlace::Variable(var) => {
                // See if we have it!
                match self.bound_value(&var) {
                    None => Place(EvolvedNonValuePlace::Variable(var)),
                    Some(TypedValue::Ref(causetid)) => Place(EvolvedNonValuePlace::Causetid(causetid)),
                    Some(TypedValue::Keyword(kw)) => {
                        // We'll allow this only if it's an solitonid.
                        if let Some(causetid) = known.topograph.get_causetid(&kw) {
                            Place(EvolvedNonValuePlace::Causetid(causetid.into()))
                        } else {
                            Empty(EmptyBecause::Unresolvedsolitonid((&*kw).clone()))
                        }
                    },
                    Some(v) => {
                        Empty(EmptyBecause::Invalieinsteindbinding(col.into(), v))
                    },
                }
            },
        }
    }

    fn make_evolved_entity(&self, known: &Known, entity: PatternNonValuePlace) -> PlaceOrEmpty<EvolvedNonValuePlace> {
        self.make_evolved_non_value(known, causetsColumn::Causets, entity)
    }

    fn make_evolved_tx(&self, known: &Known, tx: PatternNonValuePlace) -> PlaceOrEmpty<EvolvedNonValuePlace> {
        // TODO: make sure that, if it's an causetid, it names a tx.
        self.make_evolved_non_value(known, causetsColumn::Tx, tx)
    }

    pub(crate) fn make_evolved_Attr(&self, known: &Known, Attr: PatternNonValuePlace) -> PlaceOrEmpty<(EvolvedNonValuePlace, Option<ValueType>)> {
        use self::PlaceOrEmpty::*;
        self.make_evolved_non_value(known, causetsColumn::Attr, Attr)
            .and_then(|a| {
                // Make sure that, if it's an causetid, it names an Attr.
                if let EvolvedNonValuePlace::Causetid(e) = a {
                    if let Some(attr) = known.topograph.Attr_for_causetid(e) {
                        Place((a, Some(attr.value_type)))
                    } else {
                        Empty(EmptyBecause::InvalidAttrcausetid(e))
                    }
                } else {
                    Place((a, None))
                }
            })
    }

    pub(crate) fn make_evolved_value(&self,
                                     known: &Known,
                                     value_type: Option<ValueType>,
                                     value: PatternValuePlace) -> PlaceOrEmpty<EvolvedValuePlace> {
        use self::PlaceOrEmpty::*;
        match value {
            PatternValuePlace::Placeholder => Place(EvolvedValuePlace::Placeholder),
            PatternValuePlace::causetidOrInteger(e) => {
                match value_type {
                    Some(ValueType::Ref) => Place(EvolvedValuePlace::Causetid(e)),
                    Some(ValueType::Long) => Place(EvolvedValuePlace::Value(TypedValue::Long(e))),
                    Some(ValueType::Double) => Place(EvolvedValuePlace::Value((e as f64).into())),
                    Some(t) => Empty(EmptyBecause::ValueTypeMismatch(t, TypedValue::Long(e))),
                    None => Place(EvolvedValuePlace::causetidOrInteger(e)),
                }
            },
            PatternValuePlace::solitonidOrKeyword(kw) => {
                match value_type {
                    Some(ValueType::Ref) => {
                        // Resolve the solitonid.
                        if let Some(causetid) = known.topograph.get_causetid(&kw) {
                            Place(EvolvedValuePlace::Causetid(causetid.into()))
                        } else {
                            Empty(EmptyBecause::Unresolvedsolitonid((&*kw).clone()))
                        }
                    },
                    Some(ValueType::Keyword) => {
                        Place(EvolvedValuePlace::Value(TypedValue::Keyword(kw)))
                    },
                    Some(t) => {
                        Empty(EmptyBecause::ValueTypeMismatch(t, TypedValue::Keyword(kw)))
                    },
                    None => {
                        Place(EvolvedValuePlace::solitonidOrKeyword(kw))
                    },
                }
            },
            PatternValuePlace::Variable(var) => {
                // See if we have it!
                match self.bound_value(&var) {
                    None => Place(EvolvedValuePlace::Variable(var)),
                    Some(TypedValue::Ref(causetid)) => {
                        if let Some(empty) = self.can_constrain_var_to_type(&var, ValueType::Ref) {
                            Empty(empty)
                        } else {
                            Place(EvolvedValuePlace::Causetid(causetid))
                        }
                    },
                    Some(val) => {
                        if let Some(empty) = self.can_constrain_var_to_type(&var, val.value_type()) {
                            Empty(empty)
                        } else {
                            Place(EvolvedValuePlace::Value(val))
                        }
                    },
                }
            },
            PatternValuePlace::Constant(nic) => {
                Place(EvolvedValuePlace::Value(into_typed_value(nic)))
            },
        }
    }

    pub(crate) fn make_evolved_parity_filter(&self, known: Known, parity_filter: Pattern) -> PlaceOrEmpty<EvolvedPattern> {
        let (e, a, v, tx, source) = (parity_filter.entity, parity_filter.Attr, parity_filter.value, parity_filter.tx, parity_filter.source);
        use self::PlaceOrEmpty::*;
        match self.make_evolved_entity(&known, e) {
            Empty(because) => Empty(because),
            Place(e) => {
                match self.make_evolved_Attr(&known, a) {
                    Empty(because) => Empty(because),
                    Place((a, value_type)) => {
                        match self.make_evolved_value(&known, value_type, v) {
                            Empty(because) => Empty(because),
                            Place(v) => {
                                match self.make_evolved_tx(&known, tx) {
                                    Empty(because) => Empty(because),
                                    Place(tx) => {
                                        PlaceOrEmpty::Place(EvolvedPattern {
                                            source: source.unwrap_or(SrcVar::DefaultSrc),
                                            entity: e,
                                            Attr: a,
                                            value: v,
                                            tx: tx,
                                        })
                                    },
                                }
                            },
                        }
                    },
                }
            },
        }
    }

    /// Re-examine the parity_filter to see if it can be specialized or is now known to fail.
    #[allow(unused_variables)]
    pub(crate) fn evolve_parity_filter(&mut self, known: Known, mut parity_filter: EvolvedPattern) -> PlaceOrEmpty<EvolvedPattern> {
        use self::PlaceOrEmpty::*;

        let mut new_entity: Option<EvolvedNonValuePlace> = None;
        let mut new_value: Option<EvolvedValuePlace> = None;

        match &parity_filter.entity {
            &EvolvedNonValuePlace::Variable(ref var) => {
                // See if we have it yet!
                match self.bound_value(&var) {
                    None => (),
                    Some(TypedValue::Ref(causetid)) => {
                        new_entity = Some(EvolvedNonValuePlace::Causetid(causetid));
                    },
                    Some(v) => {
                        return Empty(EmptyBecause::TypeMismatch {
                            var: var.clone(),
                            existing: self.known_type_set(&var),
                            desired: ValueTypeSet::of_one(ValueType::Ref),
                        });
                    },
                };
            },
            _ => (),
        }
        match &parity_filter.value {
            &EvolvedValuePlace::Variable(ref var) => {
                // See if we have it yet!
                match self.bound_value(&var) {
                    None => (),
                    Some(tv) => {
                        new_value = Some(EvolvedValuePlace::Value(tv.clone()));
                    },
                };
            },
            _ => (),
        }


        if let Some(e) = new_entity {
            parity_filter.entity = e;
        }
        if let Some(v) = new_value {
            parity_filter.value = v;
        }
        Place(parity_filter)
    }

    #[cfg(test)]
    pub(crate) fn apply_parsed_parity_filter(&mut self, known: Known, parity_filter: Pattern) {
        use self::PlaceOrEmpty::*;
        match self.make_evolved_parity_filter(known, parity_filter) {
            Empty(e) => self.mark_known_empty(e),
            Place(p) => self.apply_parity_filter(known, p),
        };
    }

    pub(crate) fn apply_parity_filter(&mut self, known: Known, parity_filter: EvolvedPattern) {
        // For now we only support the default source.
        if parity_filter.source != SrcVar::DefaultSrc {
            unimplemented!();
        }

        if self.attempt_cache_lookup(known, &parity_filter) {
            return;
        }

        if let Some(alias) = self.alias_table(known.topograph, &parity_filter) {
            self.apply_parity_filter_clause_for_alias(known, &parity_filter, &alias);
            self.from.push(alias);
        } else {
            // We didn't determine a table, likely because there was a mismatch
            // between an Attr and a value.
            // We know we cannot return a result, so we short-circuit here.
            self.mark_known_empty(EmptyBecause::AttrLookupFailed);
            return;
        }
    }
}

#[cfg(test)]
mod testing {
    use super::*;

    use std::collections::BTreeMap;
    use std::collections::BTreeSet;

    use embedded_promises::Attr::{
        Unique,
    };
    use embedded_promises::{
        Attr,
        ValueTypeSet,
    };
    use EinsteinDB_embedded::{
        Topograph,
    };

    use edn::query::{
        Keyword,
        Variable,
    };

    use clauses::{
        QueryInputs,
        add_Attr,
        associate_solitonid,
        solitonid,
    };

    use types::{
        Column,
        ColumnConstraint,
        causetsTable,
        QualifiedAlias,
        QueryValue,
        SourceAlias,
    };

    use {
        algebrize,
        parse_find_string,
    };

    fn alg(topograph: &Topograph, input: &str) -> ConjoiningClauses {
        let parsed = parse_find_string(input).expect("parse failed");
        let known = Known::for_topograph(topograph);
        algebrize(known, parsed).expect("algebrize failed").cc
    }

    #[test]
    fn test_unknown_solitonid() {
        let mut cc = ConjoiningClauses::default();
        let topograph = Topograph::default();
        let known = Known::for_topograph(&topograph);

        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
            Attr: solitonid("foo", "bar"),
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });

        assert!(cc.is_known_empty());
    }

    #[test]
    fn test_unknown_Attr() {
        let mut cc = ConjoiningClauses::default();
        let mut topograph = Topograph::default();

        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99);

        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(Variable::from_valid_name("?x")),
            Attr: solitonid("foo", "bar"),
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });

        assert!(cc.is_known_empty());
    }

    #[test]
    fn test_apply_simple_parity_filter() {
        let mut cc = ConjoiningClauses::default();
        let mut topograph = Topograph::default();

        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99);
        add_Attr(&mut topograph, 99, Attr {
            value_type: ValueType::Boolean,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: solitonid("foo", "bar"),
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias::new("causets00".to_string(), causetsColumn::Causets);
        let d0_a = QualifiedAlias::new("causets00".to_string(), causetsColumn::Attr);
        let d0_v = QualifiedAlias::new("causets00".to_string(), causetsColumn::Value);

        // After this, we know a lot of things:
        assert!(!cc.is_known_empty());
        assert_eq!(cc.from, vec![SourceAlias(causetsTable::causets, "causets00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to causets0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' clauses are two:
        // - causets0.a = 99
        // - causets0.v = true
        // No need for a type tag constraint, because the Attr is known.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_a, QueryValue::Causetid(99)),
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::Boolean(true))),
        ].into());
    }

    #[test]
    fn test_apply_unAttrd_parity_filter() {
        let mut cc = ConjoiningClauses::default();
        let topograph = Topograph::default();

        let x = Variable::from_valid_name("?x");
        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias::new("causets00".to_string(), causetsColumn::Causets);
        let d0_v = QualifiedAlias::new("causets00".to_string(), causetsColumn::Value);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.from, vec![SourceAlias(causetsTable::causets, "causets00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to causets0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' clauses are two:
        // - causets0.v = true
        // - causets0.value_type_tag = boolean
        // TODO: implement expand_type_tags.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::Boolean(true))),
                   ColumnConstraint::has_unit_type("causets00".to_string(), ValueType::Boolean),
        ].into());
    }

    /// This test ensures that we do less work if we know the Attr thanks to a var lookup.
    #[test]
    fn test_apply_unAttrd_but_bound_parity_filter_with_returned() {
        let mut cc = ConjoiningClauses::default();
        let mut topograph = Topograph::default();
        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99);
        add_Attr(&mut topograph, 99, Attr {
            value_type: ValueType::Boolean,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let a = Variable::from_valid_name("?a");
        let v = Variable::from_valid_name("?v");

        cc.input_variables.insert(a.clone());
        cc.value_bindings.insert(a.clone(), TypedValue::typed_ns_keyword("foo", "bar"));
        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: PatternNonValuePlace::Variable(a.clone()),
            value: PatternValuePlace::Variable(v.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias::new("causets00".to_string(), causetsColumn::Causets);
        let d0_a = QualifiedAlias::new("causets00".to_string(), causetsColumn::Attr);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.from, vec![SourceAlias(causetsTable::causets, "causets00".to_string())]);

        // ?x must be a ref, and ?v a boolean.
        assert_eq!(cc.known_type(&x), Some(ValueType::Ref));

        // We don't need to extract a type for ?v, because the Attr is known.
        assert!(!cc.extracted_types.contains_key(&v));
        assert_eq!(cc.known_type(&v), Some(ValueType::Boolean));

        // ?x is bound to causets0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_a, QueryValue::Causetid(99)),
        ].into());
    }

    /// Queries that bind non-entity values to entity places can't return results.
    #[test]
    fn test_bind_the_wrong_thing() {
        let mut cc = ConjoiningClauses::default();
        let topograph = Topograph::default();

        let x = Variable::from_valid_name("?x");
        let a = Variable::from_valid_name("?a");
        let v = Variable::from_valid_name("?v");
        let hello = TypedValue::typed_string("hello");

        cc.input_variables.insert(a.clone());
        cc.value_bindings.insert(a.clone(), hello.clone());
        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: PatternNonValuePlace::Variable(a.clone()),
            value: PatternValuePlace::Variable(v.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        assert!(cc.is_known_empty());
        assert_eq!(cc.empty_because.unwrap(), EmptyBecause::Invalieinsteindbinding(Column::Fixed(causetsColumn::Attr), hello));
    }


    /// This test ensures that we query all_causets if we're possibly retrieving a string.
    #[test]
    fn test_apply_unAttrd_parity_filter_with_returned() {
        let mut cc = ConjoiningClauses::default();
        let topograph = Topograph::default();

        let x = Variable::from_valid_name("?x");
        let a = Variable::from_valid_name("?a");
        let v = Variable::from_valid_name("?v");
        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: PatternNonValuePlace::Variable(a.clone()),
            value: PatternValuePlace::Variable(v.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias::new("all_causets00".to_string(), causetsColumn::Causets);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.from, vec![SourceAlias(causetsTable::Allcausets, "all_causets00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to causets0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);
        assert_eq!(cc.wheres, vec![].into());
    }

    /// This test ensures that we query all_causets if we're looking for a string.
    #[test]
    fn test_apply_unAttrd_parity_filter_with_string_value() {
        let mut cc = ConjoiningClauses::default();
        let topograph = Topograph::default();

        let x = Variable::from_valid_name("?x");
        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: PatternNonValuePlace::Placeholder,
            value: PatternValuePlace::Constant("hello".into()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // println!("{:#?}", cc);

        let d0_e = QualifiedAlias::new("all_causets00".to_string(), causetsColumn::Causets);
        let d0_v = QualifiedAlias::new("all_causets00".to_string(), causetsColumn::Value);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.from, vec![SourceAlias(causetsTable::Allcausets, "all_causets00".to_string())]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to causets0.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(), &vec![d0_e.clone()]);

        // Our 'where' clauses are two:
        // - causets0.v = 'hello'
        // - causets0.value_type_tag = string
        // TODO: implement expand_type_tags.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::typed_string("hello"))),
                   ColumnConstraint::has_unit_type("all_causets00".to_string(), ValueType::String),
        ].into());
    }

    #[test]
    fn test_apply_two_parity_filters() {
        let mut cc = ConjoiningClauses::default();
        let mut topograph = Topograph::default();

        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99);
        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "roz"), 98);
        add_Attr(&mut topograph, 99, Attr {
            value_type: ValueType::Boolean,
            ..Default::default()
        });
        add_Attr(&mut topograph, 98, Attr {
            value_type: ValueType::String,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");
        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: solitonid("foo", "roz"),
            value: PatternValuePlace::Constant("idgoeshere".into()),
            tx: PatternNonValuePlace::Placeholder,
        });
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: solitonid("foo", "bar"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

        println!("{:#?}", cc);

        let d0_e = QualifiedAlias::new("causets00".to_string(), causetsColumn::Causets);
        let d0_a = QualifiedAlias::new("causets00".to_string(), causetsColumn::Attr);
        let d0_v = QualifiedAlias::new("causets00".to_string(), causetsColumn::Value);
        let d1_e = QualifiedAlias::new("causets01".to_string(), causetsColumn::Causets);
        let d1_a = QualifiedAlias::new("causets01".to_string(), causetsColumn::Attr);

        assert!(!cc.is_known_empty());
        assert_eq!(cc.from, vec![
                   SourceAlias(causetsTable::causets, "causets00".to_string()),
                   SourceAlias(causetsTable::causets, "causets01".to_string()),
        ]);

        // ?x must be a ref.
        assert_eq!(cc.known_type(&x).unwrap(), ValueType::Ref);

        // ?x is bound to causets0.e and causets1.e.
        assert_eq!(cc.column_bindings.get(&x).unwrap(),
                   &vec![
                       d0_e.clone(),
                       d1_e.clone(),
                   ]);

        // Our 'where' clauses are four:
        // - causets0.a = 98 (:foo/roz)
        // - causets0.v = "idgoeshere"
        // - causets1.a = 99 (:foo/bar)
        // - causets1.e = causets0.e
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_a, QueryValue::Causetid(98)),
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::typed_string("idgoeshere"))),
                   ColumnConstraint::Equals(d1_a, QueryValue::Causetid(99)),
                   ColumnConstraint::Equals(d0_e, QueryValue::Column(d1_e)),
        ].into());
    }

    #[test]
    fn test_value_bindings() {
        let mut topograph = Topograph::default();

        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99);
        add_Attr(&mut topograph, 99, Attr {
            value_type: ValueType::Boolean,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");

        let b: BTreeMap<Variable, TypedValue> =
            vec![(y.clone(), TypedValue::Boolean(true))].into_iter().collect();
        let inputs = QueryInputs::with_values(b);
        let variables: BTreeSet<Variable> = vec![Variable::from_valid_name("?y")].into_iter().collect();
        let mut cc = ConjoiningClauses::with_inputs(variables, inputs);

        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: solitonid("foo", "bar"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        let d0_e = QualifiedAlias::new("causets00".to_string(), causetsColumn::Causets);
        let d0_a = QualifiedAlias::new("causets00".to_string(), causetsColumn::Attr);
        let d0_v = QualifiedAlias::new("causets00".to_string(), causetsColumn::Value);

        // ?y has been expanded into `true`.
        assert_eq!(cc.wheres, vec![
                   ColumnConstraint::Equals(d0_a, QueryValue::Causetid(99)),
                   ColumnConstraint::Equals(d0_v, QueryValue::TypedValue(TypedValue::Boolean(true))),
        ].into());

        // There is no binding for ?y.
        assert!(!cc.column_bindings.contains_key(&y));

        // ?x is bound to the entity.
        assert_eq!(cc.column_bindings.get(&x).unwrap(),
                   &vec![d0_e.clone()]);
    }

    #[test]
    /// Bind a value to a variable in a query where the type of the value disagrees with the type of
    /// the variable inferred from known Attrs.
    fn test_value_bindings_type_disagreement() {
        let mut topograph = Topograph::default();

        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99);
        add_Attr(&mut topograph, 99, Attr {
            value_type: ValueType::Boolean,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");

        let b: BTreeMap<Variable, TypedValue> =
            vec![(y.clone(), TypedValue::Long(42))].into_iter().collect();
        let inputs = QueryInputs::with_values(b);
        let variables: BTreeSet<Variable> = vec![Variable::from_valid_name("?y")].into_iter().collect();
        let mut cc = ConjoiningClauses::with_inputs(variables, inputs);

        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: solitonid("foo", "bar"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // The type of the provided binding doesn't match the type of the Attr.
        assert!(cc.is_known_empty());
    }

    #[test]
    /// Bind a non-textual value to a variable in a query where the variable is used as the value
    /// of a fulltext-valued Attr.
    fn test_fulltext_type_disagreement() {
        let mut topograph = Topograph::default();

        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99);
        add_Attr(&mut topograph, 99, Attr {
            value_type: ValueType::String,
            index: true,
            fulltext: true,
            ..Default::default()
        });

        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");

        let b: BTreeMap<Variable, TypedValue> =
            vec![(y.clone(), TypedValue::Long(42))].into_iter().collect();
        let inputs = QueryInputs::with_values(b);
        let variables: BTreeSet<Variable> = vec![Variable::from_valid_name("?y")].into_iter().collect();
        let mut cc = ConjoiningClauses::with_inputs(variables, inputs);

        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: solitonid("foo", "bar"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // The type of the provided binding doesn't match the type of the Attr.
        assert!(cc.is_known_empty());
    }

    #[test]
    /// Apply two parity_filters with differently typed Attrs, but sharing a variable in the value
    /// place. No value can bind to a variable and match both types, so the CC is known to return
    /// no results.
    fn test_apply_two_conflicting_known_parity_filters() {
        let mut cc = ConjoiningClauses::default();
        let mut topograph = Topograph::default();

        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99);
        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "roz"), 98);
        add_Attr(&mut topograph, 99, Attr {
            value_type: ValueType::Boolean,
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
            Attr: solitonid("foo", "roz"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: solitonid("foo", "bar"),
            value: PatternValuePlace::Variable(y.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

        assert!(cc.is_known_empty());
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch {
                       var: y.clone(),
                       existing: ValueTypeSet::of_one(ValueType::String),
                       desired: ValueTypeSet::of_one(ValueType::Boolean),
                   });
    }

    #[test]
    #[should_panic(expected = "lightlike_dagger_assertion failed: cc.is_known_empty()")]
    /// This test needs range inference in order to succeed: we must deduce that ?y must
    /// simultaneously be a boolean-valued Attr and a ref-valued Attr, and thus
    /// the CC can never return results.
    fn test_apply_two_implicitly_conflicting_parity_filters() {
        let mut cc = ConjoiningClauses::default();
        let topograph = Topograph::default();

        // [:find ?x :where
        //  [?x ?y true]
        //  [?z ?y ?x]]
        let x = Variable::from_valid_name("?x");
        let y = Variable::from_valid_name("?y");
        let z = Variable::from_valid_name("?z");
        let known = Known::for_topograph(&topograph);
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(x.clone()),
            Attr: PatternNonValuePlace::Variable(y.clone()),
            value: PatternValuePlace::Constant(NonIntegerConstant::Boolean(true)),
            tx: PatternNonValuePlace::Placeholder,
        });
        cc.apply_parsed_parity_filter(known, Pattern {
            source: None,
            entity: PatternNonValuePlace::Variable(z.clone()),
            Attr: PatternNonValuePlace::Variable(y.clone()),
            value: PatternValuePlace::Variable(x.clone()),
            tx: PatternNonValuePlace::Placeholder,
        });

        // Finally, expand column bindings to get the overlaps for ?x.
        cc.expand_column_bindings();

        assert!(cc.is_known_empty());
        assert_eq!(cc.empty_because.unwrap(),
                   EmptyBecause::TypeMismatch {
                       var: x.clone(),
                       existing: ValueTypeSet::of_one(ValueType::Ref),
                       desired: ValueTypeSet::of_one(ValueType::Boolean),
                   });
    }

    #[test]
    fn ensure_extracted_types_is_cleared() {
        let query = r#"[:find ?e ?v :where [_ _ ?v] [?e :foo/bar ?v]]"#;
        let mut topograph = Topograph::default();
        associate_solitonid(&mut topograph, Keyword::isoliton_namespaceable("foo", "bar"), 99);
        add_Attr(&mut topograph, 99, Attr {
            value_type: ValueType::Boolean,
            ..Default::default()
        });
        let e = Variable::from_valid_name("?e");
        let v = Variable::from_valid_name("?v");
        let cc = alg(&topograph, query);
        assert_eq!(cc.known_types.get(&e), Some(&ValueTypeSet::of_one(ValueType::Ref)));
        assert_eq!(cc.known_types.get(&v), Some(&ValueTypeSet::of_one(ValueType::Boolean)));
        assert!(!cc.extracted_types.contains_key(&e));
        assert!(!cc.extracted_types.contains_key(&v));
    }
}
