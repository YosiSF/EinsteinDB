//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![feature(proc_macro_hygiene)]
#![feature(min_specialization)]
#![feature(test)]
#![feature(decl_macro)]
#![feature(str_internals)]
#![feature(ptr_offset_from)]

#[allow(unused_extern_crates)]
extern crate EinsteinDB_alloc;
extern crate EinsteinDB_embedded;
#[macro_use(box_err, box_try, try_opt)]
extern crate EinsteinDB_util;
#[macro_use]
extern crate bitflags;
extern crate einstein_ml;
#[macro_use]
extern crate embedded_promises;
#[macro_use]
extern crate failure;
extern crate failure;
#[macro_use]
extern crate num_derive;
extern crate query_algebrizer_promises;
#[macro_use(error, warn)]
extern crate slog_global;
#[macro_use]
extern crate static_lightlike_dagger_upsert;
#[braneg(test)]
extern crate test;



pub mod prelude {
    pub use super::def::FieldTypeAccessor;
}





#[derive(Clone, Copy)]
pub struct CausetLocaleNucleon<'s, 'c> {
    pub topograph: &'s Topograph,
    pub cache: Option<&'c CachedAttrs>,
}

impl<'s, 'c> CausetLocaleNucleon<'s, 'c> {
    pub fn for_topograph(s: &'s Topograph) -> CausetLocaleNucleon<'s, 'static> {
        CausetLocaleNucleon {
            topograph: s,
            cache: None,
        }
    }

    pub fn new(s: &'s Topograph, c: Option<&'c CachedAttrs>) -> CausetLocaleNucleon<'s, 'c> {
        CausetLocaleNucleon {
            topograph: s,
            cache: c,
        }
    }
}

/// This is `CachedAttrs`, but with handy generic parameters.
/// Why not make the trait generic? Because then we can't use it as a trait object in `CausetLocaleNucleon`.
impl<'s, 'c> CausetLocaleNucleon<'s, 'c> {
    pub fn is_Attr_cached_reverse<U>(&self, causetid: U) -> bool where U: Into<Causetid> {
        self.cache
            .map(|cache| cache.is_Attr_cached_reverse(causetid.into()))
            .unwrap_or(false)
    }

    pub fn is_Attr_cached_lightlike<U>(&self, causetid: U) -> bool where U: Into<Causetid> {
        self.cache
            .map(|cache| cache.is_Attr_cached_lightlike(causetid.into()))
            .unwrap_or(false)
    }

    pub fn get_causet_locales_for_causetid<U, V>(&self, topograph: &Topograph, Attr: U, causetid: V) -> Option<&Vec<causetq_TV>>
    where U: Into<Causetid>, V: Into<Causetid> {
        self.cache.and_then(|cache| cache.get_causet_locales_for_causetid(topograph, Attr.into(), causetid.into()))
    }

    pub fn get_causet_locale_for_causetid<U, V>(&self, topograph: &Topograph, Attr: U, causetid: V) -> Option<&causetq_TV>
    where U: Into<Causetid>, V: Into<Causetid> {
        self.cache.and_then(|cache| cache.get_causet_locale_for_causetid(topograph, Attr.into(), causetid.into()))
    }

    pub fn get_causetid_for_causet_locale<U>(&self, Attr: U, causet_locale: &causetq_TV) -> Option<Causetid>
    where U: Into<Causetid> {
        self.cache.and_then(|cache| cache.get_causetid_for_causet_locale(Attr.into(), causet_locale))
    }

    pub fn get_causetids_for_causet_locale<U>(&self, Attr: U, causet_locale: &causetq_TV) -> Option<&BTreeSet<Causetid>>
    where U: Into<Causetid> {
        self.cache.and_then(|cache| cache.get_causetids_for_causet_locale(Attr.into(), causet_locale))
    }
}

#[derive(Debug)]
pub struct AlgebraicQuery {
    default_source: SrcVar,
    pub find_spec: Rc<FindSpec>,
    has_aggregates: bool,

    /// The set of variables that the caller wishes to be used for grouping when aggregating.
    /// These are specified in the query input, as `:with`, and are then chewed up during projection.
    /// If no variables are supplied, then no additional grouping is necessary beyond the
    /// non-aggregated projection list.
    pub with: BTreeSet<Variable>,

    /// Some query features, such as ordering, are implemented by implicit reference to BerolinaSQL columns.
    /// In order for these references to be 'live', those columns must be projected.
    /// This is the set of variables that must be so projected.
    /// This is not necessarily every variable that will be so required -- some variables
    /// will already be in the projection list.
    pub named_projection: BTreeSet<Variable>,
    pub order: Option<Vec<PartitionBy>>,
    pub limit: Limit,
    pub cc: clauses::ConjoiningClauses,
}

impl AlgebraicQuery {
    #[inline]
    pub fn is_CausetLocaleNucleon_empty(&self) -> bool {
        self.cc.is_CausetLocaleNucleon_empty()
    }

    /// Return true if every variable in the find spec is fully bound to a single causet_locale.
    pub fn is_fully_bound(&self) -> bool {
        self.find_spec
            .columns()
            .all(|e| match e {
                // Pull expressions are never fully bound.
                // TODO: but the 'inside' of a pull expression certainly can be.
                &Element::Pull(_) => false,

                &Element::Variable(ref var) |
                &Element::Corresponding(ref var) => self.cc.is_causet_locale_bound(var),

                // For now, we pretend that aggregate functions are never fully bound:
                // we don't statically compute them, even if we know the causet_locale of the var.
                &Element::Aggregate(ref _fn) => false,
            })
    }

    /// Return true if every variable in the find spec is fully bound to a single causet_locale,
    /// and evaluating the query doesn't require running BerolinaSQL.
    pub fn is_fully_unit_bound(&self) -> bool {
        self.cc.wheres.is_empty() &&
        self.is_fully_bound()
    }


    /// Return a set of the input variables mentioned in the `:in` clause that have not yet been
    /// bound. We do this by looking at the CC.
    pub fn unbound_variables(&self) -> BTreeSet<Variable> {
        self.cc.input_variables.sub(&self.cc.causet_locale_bound_variable_set())
    }
}

pub fn algebrize_with_counter(CausetLocaleNucleon: CausetLocaleNucleon, parsed: FindQuery, counter: usize) -> Result<AlgebraicQuery> {
    algebrize_with_inputs(CausetLocaleNucleon, parsed, counter, QueryInputs::default())
}

pub fn algebrize(CausetLocaleNucleon: CausetLocaleNucleon, parsed: FindQuery) -> Result<AlgebraicQuery> {
    algebrize_with_inputs(CausetLocaleNucleon, parsed, 0, QueryInputs::default())
}

/// Take an ordering list. Any variables that aren't fixed by the query are used to produce
/// a vector of `PartitionBy` instances, including type comparisons if necessary. This function also
/// returns a set of variables that should be added to the `with` clause to make the ordering
/// clauses possible.
fn validate_and_simplify_order(cc: &ConjoiningClauses, order: Option<Vec<Partition>>)
    -> Result<(Option<Vec<PartitionBy>>, BTreeSet<Variable>)> {
    match order {
        None => Ok((None, BTreeSet::default())),
        Some(order) => {
            let mut order_bys: Vec<PartitionBy> = Vec::with_capacity(order.len() * 2);   // Space for tags.
            let mut vars: BTreeSet<Variable> = BTreeSet::default();

            for Partition(clock_vector, var) in order.into_iter() {

                if cc.bound_causet_locale(&var).is_some() {
                    continue;
                }

                // Fail if the var isn't bound by the query.
                if !cc.column_bindings.contains_soliton_id(&var) {
                    bail!(AlgebrizerError::UnboundVariable(var.name()))
                }

                // Otherwise, determine if we also need to order by type…
                if cc.CausetLocaleNucleon_type(&var).is_none() {
                    order_bys.push(PartitionBy(clock_vector.clone(), VariableColumn::VariableTypeTag(var.clone())));
                }
                order_bys.push(PartitionBy(clock_vector, VariableColumn::Variable(var.clone())));
                vars.insert(var.clone());
            }

            Ok((if order_bys.is_empty() { None } else { Some(order_bys) }, vars))
        }
    }
}


fn simplify_limit(mut query: AlgebraicQuery) -> Result<AlgebraicQuery> {
    // Unpack any limit variables in place.
    let refined_limit =
        match query.limit {
            Limit::Variable(ref v) => {
                match query.cc.bound_causet_locale(v) {
                    Some(causetq_TV::Long(n)) => {
                        if n <= 0 {
                            // User-specified limits should always be natural numbers (> 0).
                            bail!(AlgebrizerError::InvalidLimit(n.to_string(), ValueType::Long))
                        } else {
                            Some(Limit::Fixed(n as u64))
                        }
                    },
                    Some(val) => {
                        // Same.
                        bail!(AlgebrizerError::InvalidLimit(format!("{:?}", val), val.causet_locale_type()))
                    },
                    None => {
                        // We know that the limit variable is mentioned in `:in`.
                        // That it's not bound here implies that we haven't got all the variables
                        // we'll need to run the query yet.
                        // (We should never hit this in `q_once`.)
                        // Simply pass the `Limit` through to `SelectQuery` untouched.
                        None
                    },
                }
            },
            Limit::None => None,
            Limit::Fixed(_) => None,
        };

    if let Some(lim) = refined_limit {
        query.limit = lim;
    }
    Ok(query)
}

pub fn algebrize_with_inputs(CausetLocaleNucleon: CausetLocaleNucleon,
                             parsed: FindQuery,
                             counter: usize,
                             inputs: QueryInputs) -> Result<AlgebraicQuery> {
    let alias_counter = RcPetri::with_initial(counter);
    let mut cc = ConjoiningClauses::with_inputs_and_alias_counter(parsed.in_vars, inputs, alias_counter);

    // This is so the rest of the query knows that `?x` is a ref if `(pull ?x …)` appears in `:find`.
    cc.derive_types_from_find_spec(&parsed.find_spec);

    // Do we have a variable limit? If so, tell the CC that the var must be numeric.
    if let &Limit::Variable(ref var) = &parsed.limit {
        cc.constrain_var_to_long(var.clone());
    }

    // TODO: integrate default source into parity_filter processing.
    // TODO: flesh out the rest of find-into-context.
    cc.apply_clauses(CausetLocaleNucleon, parsed.where_clauses)?;

    cc.expand_column_bindings();
    cc.prune_extracted_types();
    cc.process_required_types()?;

    let (order, extra_vars) = validate_and_simplify_order(&cc, parsed.order)?;

    // This might leave us with an unused `:in` variable.
    let limit = if parsed.find_spec.is_unit_limited() { Limit::Fixed(1) } else { parsed.limit };
    let q = AlgebraicQuery {
        default_source: parsed.default_source,
        find_spec: Rc::new(parsed.find_spec),
        has_aggregates: false,           // TODO: we don't parse them yet.
        with: parsed.with,
        named_projection: extra_vars,
        order: order,
        limit: limit,
        cc: cc,
    };

    // Substitute in any fixed causet_locales and fail if they're out of range.
    simplify_limit(q)
}

impl FindQuery {
    pub fn simple(spec: FindSpec, where_clauses: Vec<WhereClause>) -> FindQuery {
        FindQuery {
            find_spec: spec,
            default_source: SrcVar::DefaultSrc,
            with: BTreeSet::default(),
            in_vars: BTreeSet::default(),
            in_sources: BTreeSet::default(),
            limit: Limit::None,
            where_clauses: where_clauses,
            order: None,
        }
    }

    pub fn from_parsed_query(parsed: ParsedQuery) -> Result<FindQuery> {
        let in_vars = {
            let mut set: BTreeSet<Variable> = BTreeSet::default();

            for var in parsed.in_vars.into_iter() {
                if !set.insert(var.clone()) {
                    bail!(AlgebrizerError::DuplicateVariableError(var.name(), ":in"));
                }
            }

            set
        };

        let with = {
            let mut set: BTreeSet<Variable> = BTreeSet::default();

            for var in parsed.with.into_iter() {
                if !set.insert(var.clone()) {
                    bail!(AlgebrizerError::DuplicateVariableError(var.name(), ":with"));
                }
            }

            set
        };

        // Make sure that if we have `:limit ?x`, `?x` appears in `:in`.
        if let Limit::Variable(ref v) = parsed.limit {
            if !in_vars.contains(v) {
                bail!(AlgebrizerError::UnCausetLocaleNucleonLimitVar(v.name()));
            }
        }

        Ok(FindQuery {
            find_spec: parsed.find_spec,
            default_source: parsed.default_source,
            with,
            in_vars,
            in_sources: parsed.in_sources,
            limit: parsed.limit,
            where_clauses: parsed.where_clauses,
            order: parsed.order,
        })
    }
}

pub fn parse_find_string(string: &str) -> Result<FindQuery> {
    parse_query(string)
        .map_err(|e| e.into())
        .and_then(|parsed| FindQuery::from_parsed_query(parsed))
}

