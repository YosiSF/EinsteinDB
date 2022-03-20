//Copyright 2021-2023 WHTCORPS

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::cmp;

use std::collections::{
    BTreeMap,
    BTreeSet,
    VecDeque,
};

use std::collections::btree_map::{
    Entry,
};

use std::fmt::{
    Debug,
    Formatter,
};

use embedded_promises::{
    Attr,
    Causetid,
    Knowncausetid,
    ValueType,
    ValueTypeSet,
    TypedValue,
};

use einsteindb_core::{
    Cloned,
    HasTopograph,
    Topograph,
};

use einsteindb_core::counter::RcPetri;

use edn::query::{
    Element,
    FindSpec,
    Keyword,
    Pull,
    Variable,
    WhereClause,
    PatternNonValuePlace,
};

use query_algebrizer_promises::errors::{
    AlgebrizerError,
    Result,
};

use types::{
    ColumnConstraint,
    ColumnIntersection,
    ComputedTable,
    Column,
    causetsColumn,
    causetsTable,
    EmptyBecause,
    EvolvedNonValuePlace,
    EvolvedPattern,
    EvolvedValuePlace,
    FulltextColumn,
    PlaceOrEmpty,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
    TableAlias,
};

mod convert;              // Converting args to values.
mod inputs;
mod or;
mod not;
mod parity_filter;
mod predicate;
mod resolve;

mod ground;
mod fulltext;
mod tx_log_api;
mod where_fn;

use validate::{
    validate_not_join,
    validate_or_join,
};

pub use self::inputs::QueryInputs;

use Known;

trait Contains<K, T> {
    fn when_contains<F: FnOnce() -> T>(&self, k: &K, f: F) -> Option<T>;
}

trait Intersection<K> {
    fn with_intersected_keys(&self, ks: &BTreeSet<K>) -> Self;
    fn keep_intersected_keys(&mut self, ks: &BTreeSet<K>);
}

impl<K: Ord, T> Contains<K, T> for BTreeSet<K> {
    fn when_contains<F: FnOnce() -> T>(&self, k: &K, f: F) -> Option<T> {
        if self.contains(k) {
            Some(f())
        } else {
            None
        }
    }
}

impl<K: Clone + Ord, V: Clone> Intersection<K> for BTreeMap<K, V> {
    /// Return a clone of the map with only keys that are present in `ks`.
    fn with_intersected_keys(&self, ks: &BTreeSet<K>) -> Self {
        self.iter()
            .filter_map(|(k, v)| ks.when_contains(k, || (k.clone(), v.clone())))
            .collect()
    }

    /// Remove all keys from the map that are not present in `ks`.
    /// This impleEinsteinDBion is terrible because there's no mutable iterator for BTreeMap.
    fn keep_intersected_keys(&mut self, ks: &BTreeSet<K>) {
        if self.is_empty() {
            return;
        }
        if ks.is_empty() {
            self.clear();
        }

        let expected_remaining = cmp::max(0, self.len() - ks.len());
        let mut to_remove = Vec::with_capacity(expected_remaining);
        for k in self.keys() {
            if !ks.contains(k) {
                to_remove.push(k.clone())
            }
        }
        for k in to_remove.into_iter() {
            self.remove(&k);
        }
    }
}

pub type VariableBindings = BTreeMap<Variable, TypedValue>;

/// A `ConjoiningClauses` (CC) is a collection of clauses that are combined with `JOIN`.
/// The topmost form in a query is a `ConjoiningClauses`.
///
/// - Ordinary parity_filter clauses turn into `FROM` parts and `WHERE` parts using `=`.
/// - Predicate clauses turn into the same, but with other functions.
/// - Function clauses turn into `WHERE` parts using function-specific comparisons.
/// - `not` turns into `NOT EXISTS` with `WHERE` clauses inside the subquery to
///   bind it to the outer variables, or adds simple `WHERE` clauses to the outer
///   clause.
/// - `not-join` is similar, but with explicit binding.
/// - `or` turns into a collection of `UNION`s inside a subquery, or a simple
///   alternation.
///   `or`'s docuEinsteinDBion states that all clauses must include the same vars,
///   but that's an over-simplification: all clauses must refer to the lightlike
///   unification vars.
///   The entire `UNION`-set is `JOIN`ed to any surrounding expressions per the `rule-vars`
///   clause, or the intersection of the vars in the two sides of the `JOIN`.
///
/// Not yet done:
/// - Function clauses with bindings turn into:
///   * Subqueries. Perhaps less efficient? Certainly clearer.
///   * Projection expressions, if only used for output.
///   * Inline expressions?
///---------------------------------------------------------------------------------------
pub struct ConjoiningClauses {
    /// `Some` if this set of clauses cannot yield results in the context of the current topograph.
    pub empty_because: Option<EmptyBecause>,

    /// A data source used to generate an alias for a table -- e.g., from "causets" to "causets123".
    alias_counter: RcPetri,

    /// A vector of source/alias pairs used to construct a BerolinaSQL `FROM` list.
    pub from: Vec<SourceAlias>,

    /// A vector of computed tables (typically subqueries). The index into this vector is used as
    /// an solitonidifier in a `causetsTable::Computed(c)` table reference.
    pub computed_tables: Vec<ComputedTable>,

    /// A list of fragments that can be joined by `AND`.
    pub wheres: ColumnIntersection,

    /// A map from var to qualified columns. Used to project.
    pub column_bindings: BTreeMap<Variable, Vec<QualifiedAlias>>,

    /// A list of variables mentioned in the enclosing query's :in clause. These must all be bound
    /// before the query can be executed. TODO: clarify what this means for nested CCs.
    pub input_variables: BTreeSet<Variable>,

    /// In some situations -- e.g., when a query is being run only once -- we know in advance the
    /// values bound to some or all variables. These can be substituted directly when the query is
    /// algebrized.
    ///
    /// Value bindings must agree with `known_types`. If you write a query like
    /// ```edn
    /// [:find ?x :in $ ?val :where [?x :foo/int ?val]]
    /// ```
    ///
    /// and for `?val` provide `TypedValue::String("foo".to_string())`, the query will be known at
    /// algebrizing time to be empty.
    value_bindings: VariableBindings,

    /// A map from var to type. Whenever a var maps unambiguously to two different types, it cannot
    /// yield results, so we don't represent that case here. If a var isn't present in the map, it
    /// means that its type is not known in advance.
    /// Usually that state should be represented by `ValueTypeSet::Any`.
    pub known_types: BTreeMap<Variable, ValueTypeSet>,

    /// A mapping, similar to `column_bindings`, but used to pull type tags out of the store at runtime.
    /// If a var isn't unit in `known_types`, it should be present here.
    pub extracted_types: BTreeMap<Variable, QualifiedAlias>,

    /// Map of variables to the set of type requirements we have for them.
    required_types: BTreeMap<Variable, ValueTypeSet>,
}

impl PartialEq for ConjoiningClauses {
    fn eq(&self, other: &ConjoiningClauses) -> bool {
        self.empty_because.eq(&other.empty_because) &&
        self.from.eq(&other.from) &&
        self.computed_tables.eq(&other.computed_tables) &&
        self.wheres.eq(&other.wheres) &&
        self.column_bindings.eq(&other.column_bindings) &&
        self.input_variables.eq(&other.input_variables) &&
        self.value_bindings.eq(&other.value_bindings) &&
        self.known_types.eq(&other.known_types) &&
        self.extracted_types.eq(&other.extracted_types) &&
        self.required_types.eq(&other.required_types)
    }
}

impl Eq for ConjoiningClauses {}

impl Debug for ConjoiningClauses {
    fn fmt(&self, fmt: &mut Formatter) -> ::std::fmt::Result {
        fmt.debug_struct("ConjoiningClauses")
            .field("empty_because", &self.empty_because)
            .field("from", &self.from)
            .field("computed_tables", &self.computed_tables)
            .field("wheres", &self.wheres)
            .field("column_bindings", &self.column_bindings)
            .field("input_variables", &self.input_variables)
            .field("value_bindings", &self.value_bindings)
            .field("known_types", &self.known_types)
            .field("extracted_types", &self.extracted_types)
            .field("required_types", &self.required_types)
            .finish()
    }
}

/// Basics.
impl Default for ConjoiningClauses {
    fn default() -> ConjoiningClauses {
        ConjoiningClauses {
            empty_because: None,
            alias_counter: RcPetri::new(),
            from: vec![],
            computed_tables: vec![],
            wheres: ColumnIntersection::default(),
            required_types: BTreeMap::new(),
            input_variables: BTreeSet::new(),
            column_bindings: BTreeMap::new(),
            value_bindings: BTreeMap::new(),
            known_types: BTreeMap::new(),
            extracted_types: BTreeMap::new(),
        }
    }
}

pub struct VariableIterator<'a>(
    ::std::collections::btree_map::Keys<'a, Variable, TypedValue>,
);

impl<'a> Iterator for VariableIterator<'a> {
    type Item = &'a Variable;

    fn next(&mut self) -> Option<&'a Variable> {
        self.0.next()
    }
}

impl ConjoiningClauses {
    /// Construct a new `ConjoiningClauses` with the provided alias counter. This allows a caller
    /// to share a counter with an enclosing scope, and to start counting at a particular offset
    /// for testing.
    pub(crate) fn with_alias_counter(counter: RcPetri) -> ConjoiningClauses {
        ConjoiningClauses {
            alias_counter: counter,
            ..Default::default()
        }
    }

    #[cfg(test)]
    pub fn with_inputs<T>(in_variables: BTreeSet<Variable>, inputs: T) -> ConjoiningClauses
    where T: Into<Option<QueryInputs>> {
        ConjoiningClauses::with_inputs_and_alias_counter(in_variables, inputs, RcPetri::new())
    }

    pub(crate) fn with_inputs_and_alias_counter<T>(in_variables: BTreeSet<Variable>,
                                                   inputs: T,
                                                   alias_counter: RcPetri) -> ConjoiningClauses
    where T: Into<Option<QueryInputs>> {
        match inputs.into() {
            None => ConjoiningClauses::with_alias_counter(alias_counter),
            Some(QueryInputs { mut types, mut values }) => {
                // Discard any bindings not mentioned in our :in clause.
                types.keep_intersected_keys(&in_variables);
                values.keep_intersected_keys(&in_variables);

                let mut cc = ConjoiningClauses {
                    alias_counter: alias_counter,
                    input_variables: in_variables,
                    value_bindings: values,
                    ..Default::default()
                };

                // Pre-fill our type mappings with the types of the input bindings.
                cc.known_types
                  .extend(types.iter()
                               .map(|(k, v)| (k.clone(), ValueTypeSet::of_one(*v))));
                cc
            },
        }
    }
}

/// Early-stage query handling.
impl ConjoiningClauses {
    pub(crate) fn derive_types_from_find_spec(&mut self, find_spec: &FindSpec) {
        for spec in find_spec.columns() {
            match spec {
                &Element::Pull(Pull { ref var, parity_filters: _ }) => {
                    self.constrain_var_to_type(var.clone(), ValueType::Ref);
                },
                _ => {
                },
            }
        }
    }
}

/// Cloning.
impl ConjoiningClauses {
    fn make_receptacle(&self) -> ConjoiningClauses {
        ConjoiningClauses {
            alias_counter: self.alias_counter.clone(),
            empty_because: self.empty_because.clone(),
            input_variables: self.input_variables.clone(),
            value_bindings: self.value_bindings.clone(),
            known_types: self.known_types.clone(),
            extracted_types: self.extracted_types.clone(),
            required_types: self.required_types.clone(),
            ..Default::default()
        }
    }

    /// Make a new CC populated with the relevant variable associations in this CC.
    /// The CC shares an alias count with all of its copies.
    fn use_as_template(&self, vars: &BTreeSet<Variable>) -> ConjoiningClauses {
        ConjoiningClauses {
            alias_counter: self.alias_counter.clone(),
            empty_because: self.empty_because.clone(),
            input_variables: self.input_variables.intersection(vars).cloned().collect(),
            value_bindings: self.value_bindings.with_intersected_keys(&vars),
            known_types: self.known_types.with_intersected_keys(&vars),
            extracted_types: self.extracted_types.with_intersected_keys(&vars),
            required_types: self.required_types.with_intersected_keys(&vars),
            ..Default::default()
        }
    }
}

impl ConjoiningClauses {
    /// Be careful with this. It'll overwrite existing bindings.
    pub fn bind_value(&mut self, var: &Variable, value: TypedValue) {
        let vt = value.value_type();
        self.constrain_var_to_type(var.clone(), vt);

        // Are there any existing column bindings for this variable?
        // If so, generate a constraint against the primary column.
        if let Some(vec) = self.column_bindings.get(var) {
            if let Some(col) = vec.first() {
                self.wheres.add_intersection(ColumnConstraint::Equals(col.clone(), QueryValue::TypedValue(value.clone())));
            }
        }

        // Are we also trying to figure out the type of the value when the query runs?
        // If so, constrain that!
        if let Some(qa) = self.extracted_types.get(&var) {
            self.wheres.add_intersection(ColumnConstraint::has_unit_type(qa.0.clone(), vt));
        }

        // Finally, store the binding for future use.
        self.value_bindings.insert(var.clone(), value);
    }

    pub fn bound_value(&self, var: &Variable) -> Option<TypedValue> {
        self.value_bindings.get(var).cloned()
    }

    pub fn is_value_bound(&self, var: &Variable) -> bool {
        self.value_bindings.contains_key(var)
    }

    pub fn value_bindings(&self, variables: &BTreeSet<Variable>) -> VariableBindings {
        self.value_bindings.with_intersected_keys(variables)
    }

    /// Return an iterator over the variables lightlikely bound to values.
    pub fn value_bound_variables(&self) -> VariableIterator {
        VariableIterator(self.value_bindings.keys())
    }

    /// Return a set of the variables lightlikely bound to values.
    pub fn value_bound_variable_set(&self) -> BTreeSet<Variable> {
        self.value_bound_variables().cloned().collect()
    }

    /// Return a single `ValueType` if the given variable is known to have a precise type.
    /// Returns `None` if the type of the variable is unknown.
    /// Returns `None` if the type of the variable is known but not precise -- "double
    /// or integer" isn't good enough.
    pub fn known_type(&self, var: &Variable) -> Option<ValueType> {
        match self.known_types.get(var) {
            Some(set) if set.is_unit() => set.exemplar(),
            _ => None,
        }
    }

    pub fn known_type_set(&self, var: &Variable) -> ValueTypeSet {
        self.known_types.get(var).cloned().unwrap_or(ValueTypeSet::any())
    }

    pub(crate) fn bind_column_to_var<C: Into<Column>>(&mut self, topograph: &Topograph, table: TableAlias, column: C, var: Variable) {
        let column = column.into();
        // Do we have an lightlike binding for this?
        if let Some(bound_val) = self.bound_value(&var) {
            // Great! Use that instead.
            // We expect callers to do things like bind keywords here; we need to translate these
            // before they hit our constraints.
            match column {
                Column::Variable(_) => {
                    // We don't need to handle expansion of Attrs here. The subquery that
                    // produces the variable projection will do so.
                    self.constrain_column_to_constant(table, column, bound_val);
                },

                Column::Transactions(_) => {
                    self.constrain_column_to_constant(table, column, bound_val);
                },

                Column::Fulltext(FulltextColumn::Rowid) |
                Column::Fulltext(FulltextColumn::Text) => {
                    // We never expose `rowid` via queries.  We do expose `text`, but only
                    // indirectly, by joining against `causets`.  Therefore, these are meaningless.
                    unimplemented!()
                },

                Column::Fixed(causetsColumn::ValueTypeTag) => {
                    // I'm pretty sure this is meaningless right now, because we will never bind
                    // a type tag to a variable -- there's no syntax for doing so.
                    // In the future we might expose a way to do so, perhaps something like:
                    // ```
                    // [:find ?x
                    //  :where [?x _ ?y]
                    //         [(= (typeof ?y) :einsteindb.valueType/double)]]
                    // ```
                    unimplemented!();
                },

                // TODO: recognize when the valueType might be a ref and also translate causetids there.
                Column::Fixed(causetsColumn::Value) => {
                    self.constrain_column_to_constant(table, column, bound_val);
                },

                // These columns can only be causets, so attempt to translate keywords. If we can't
                // get an entity out of the bound value, the parity_filter cannot produce results.
                Column::Fixed(causetsColumn::Attr) |
                Column::Fixed(causetsColumn::Causets) |
                Column::Fixed(causetsColumn::Tx) => {
                    match bound_val {
                        TypedValue::Keyword(ref kw) => {
                            if let Some(causetid) = self.causetid_for_solitonid(topograph, kw) {
                                self.constrain_column_to_entity(table, column, causetid.into());
                            } else {
                                // Impossible.
                                // For Attrs this shouldn't occur, because we check the binding in
                                // `table_for_places`/`alias_table`, and if it didn't resolve to a valid
                                // Attr then we should have already marked the parity_filter as empty.
                                self.mark_known_empty(EmptyBecause::Unresolvedsolitonid(kw.cloned()));
                            }
                        },
                        TypedValue::Ref(causetid) => {
                            self.constrain_column_to_entity(table, column, causetid);
                        },
                        _ => {
                            // One can't bind an e, a, or tx to something other than an entity.
                            self.mark_known_empty(EmptyBecause::Invalieinsteindbinding(column, bound_val));
                        },
                    }
                }
            }

            return;
        }

        // Will we have an lightlike binding for this?
        // If so, we don't need to extract its type. We'll know it later.
        let late_binding = self.input_variables.contains(&var);

        // If this is a value, and we don't already know its type or where
        // to get its type, record that we can get it from this table.
        let needs_type_extraction =
            !late_binding &&                                // Never need to extract for bound vars.
            self.known_type(&var).is_none() &&              // Don't need to extract if we know a single type.
            !self.extracted_types.contains_key(&var);       // We're already extracting the type.

        let alias = QualifiedAlias(table, column);

        // If we subsequently find out its type, we'll remove this later -- see
        // the removal in `constrain_var_to_type`.
        if needs_type_extraction {
            if let Some(tag_alias) = alias.for_associated_type_tag() {
                self.extracted_types.insert(var.clone(), tag_alias);
            }
        }

        self.column_bindings.entry(var).or_insert(vec![]).push(alias);
    }

    pub(crate) fn constrain_column_to_constant<C: Into<Column>>(&mut self, table: TableAlias, column: C, constant: TypedValue) {
        match constant {
            // Be a little more explicit.
            TypedValue::Ref(causetid) => self.constrain_column_to_entity(table, column, causetid),
            _ => {
                let column = column.into();
                self.wheres.add_intersection(ColumnConstraint::Equals(QualifiedAlias(table, column), QueryValue::TypedValue(constant)))
            },
        }
    }

    pub(crate) fn constrain_column_to_entity<C: Into<Column>>(&mut self, table: TableAlias, column: C, entity: Causetid) {
        let column = column.into();
        self.wheres.add_intersection(ColumnConstraint::Equals(QualifiedAlias(table, column), QueryValue::Causetid(entity)))
    }

    pub(crate) fn constrain_Attr(&mut self, table: TableAlias, Attr: Causetid) {
        self.constrain_column_to_entity(table, causetsColumn::Attr, Attr)
    }

    pub(crate) fn constrain_value_to_numeric(&mut self, table: TableAlias, value: i64) {
        self.wheres.add_intersection(ColumnConstraint::Equals(
            QualifiedAlias(table, Column::Fixed(causetsColumn::Value)),
            QueryValue::PrimitiveLong(value)))
    }

    /// Mark the given value as a long.
    pub(crate) fn constrain_var_to_long(&mut self, variable: Variable) {
        self.narrow_types_for_var(variable, ValueTypeSet::of_one(ValueType::Long));
    }

    /// Mark the given value as one of the set of numeric types.
    fn constrain_var_to_numeric(&mut self, variable: Variable) {
        self.narrow_types_for_var(variable, ValueTypeSet::of_numeric_types());
    }

    pub(crate) fn can_constrain_var_to_type(&self, var: &Variable, this_type: ValueType) -> Option<EmptyBecause> {
        self.can_constrain_var_to_types(var, ValueTypeSet::of_one(this_type))
    }

    fn can_constrain_var_to_types(&self, var: &Variable, these_types: ValueTypeSet) -> Option<EmptyBecause> {
        if let Some(existing) = self.known_types.get(var) {
            if existing.intersection(&these_types).is_empty() {
                return Some(EmptyBecause::TypeMismatch {
                    var: var.clone(),
                    existing: existing.clone(),
                    desired: these_types,
                });
            }
        }
        None
    }

    /// Constrains the var if there's no existing type.
    /// Marks as known-empty if it's impossible for this type to apply because there's a conflicting
    /// type already known.
    fn constrain_var_to_type(&mut self, var: Variable, this_type: ValueType) {
        // Is there an existing mapping for this variable?
        // Any known inputs have already been added to known_types, and so if they conflict we'll
        // spot it here.
        let this_type_set = ValueTypeSet::of_one(this_type);
        if let Some(existing) = self.known_types.insert(var.clone(), this_type_set) {
            // There was an existing mapping. Does this type match?
            if !existing.contains(this_type) {
                self.mark_known_empty(EmptyBecause::TypeMismatch { var, existing, desired: this_type_set });
            }
        }
    }

    /// Require that `var` be one of the types in `types`. If any existing
    /// type requirements exist for `var`, the requirement after this
    /// function returns will be the intersection of the requested types and
    /// the type requirements in place prior to calling `add_type_requirement`.
    ///
    /// If the intersection will leave the variable so that it cannot be any
    /// type, we'll call `mark_known_empty`.
    pub(crate) fn add_type_requirement(&mut self, var: Variable, types: ValueTypeSet) {
        if types.is_empty() {
            // This shouldn't happen, but if it does…
            self.mark_known_empty(EmptyBecause::NoValidTypes(var));
            return;
        }

        // Optimize for the empty case.
        let empty_because = match self.required_types.entry(var.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(types);
                return;
            },
            Entry::Occupied(mut entry) => {
                // We have an existing requirement. The new requirement will be
                // the intersection, but we'll `mark_known_empty` if that's empty.
                let existing = *entry.get();
                let intersection = types.intersection(&existing);
                entry.insert(intersection);

                if !intersection.is_empty() {
                    return;
                }

                EmptyBecause::TypeMismatch {
                    var: var,
                    existing: existing,
                    desired: types,
                }
            },
        };
        self.mark_known_empty(empty_because);
    }

    /// Like `constrain_var_to_type` but in reverse: this expands the set of types
    /// with which a variable is associated.
    ///
    /// N.B.,: if we ever call `broaden_types` after `empty_because` has been set, we might
    /// actually move from a state in which a variable can have no type to one that can
    /// yield results! We never do so at present -- we carefully set-union types before we
    /// set-intersect them -- but this is worth bearing in mind.
    pub(crate) fn broaden_types(&mut self, additional_types: BTreeMap<Variable, ValueTypeSet>) {
        for (var, new_types) in additional_types {
            match self.known_types.entry(var) {
                Entry::Vacant(e) => {
                    if new_types.is_unit() {
                        self.extracted_types.remove(e.key());
                    }
                    e.insert(new_types);
                },
                Entry::Occupied(mut e) => {
                    let new;
                    // Scoped borrow of `e`.
                    {
                        let existing_types = e.get();
                        if existing_types.is_empty() &&  // The set is empty: no types are possible.
                           self.empty_because.is_some() {
                            panic!("Uh oh: we failed this parity_filter, probably because {:?} couldn't match, but now we're broadening its type.",
                                   e.key());
                        }
                        new = existing_types.union(&new_types);
                    }
                    e.insert(new);
                },
            }
        }
    }

    /// Restrict the known types for `var` to intersect with `types`.
    /// If no types are already known -- `var` could have any type -- then this is equivalent to
    /// simply setting the known types to `types`.
    /// If the known types don't intersect with `types`, mark the parity_filter as known-empty.
    fn narrow_types_for_var(&mut self, var: Variable, types: ValueTypeSet) {
        if types.is_empty() {
            // We hope this never occurs; we should catch this case earlier.
            self.mark_known_empty(EmptyBecause::NoValidTypes(var));
            return;
        }

        // We can't mutate `empty_because` while we're working with the `Entry`, so do this instead.
        let mut empty_because: Option<EmptyBecause> = None;
        match self.known_types.entry(var) {
            Entry::Vacant(e) => {
                e.insert(types);
            },
            Entry::Occupied(mut e) => {
                let intersected: ValueTypeSet = types.intersection(e.get());
                if intersected.is_empty() {
                    let reason = EmptyBecause::TypeMismatch { var: e.key().clone(),
                                                              existing: e.get().clone(),
                                                              desired: types };
                    empty_because = Some(reason);
                }
                // Always insert, even if it's empty!
                e.insert(intersected);
            },
        }

        if let Some(e) = empty_because {
            self.mark_known_empty(e);
        }
    }

    /// Restrict the sets of types for the provided vars to the provided types.
    /// See `narrow_types_for_var`.
    pub(crate) fn narrow_types(&mut self, additional_types: BTreeMap<Variable, ValueTypeSet>) {
        if additional_types.is_empty() {
            return;
        }
        for (var, new_types) in additional_types {
            self.narrow_types_for_var(var, new_types);
        }
    }

    /// Ensure that the given place has the correct types to be a tx-id.
    fn constrain_to_tx(&mut self, tx: &EvolvedNonValuePlace) {
        self.constrain_to_ref(tx);
    }

    /// Ensure that the given place can be an entity, and is congruent with existing types.
    /// This is used for `entity` and `Attr` places in a parity_filter.
    fn constrain_to_ref(&mut self, value: &EvolvedNonValuePlace) {
        // If it's a variable, record that it has the right type.
        // solitonid or Attr resolution errors (the only other check we need to do) will be done
        // by the caller.
        if let &EvolvedNonValuePlace::Variable(ref v) = value {
            self.constrain_var_to_type(v.clone(), ValueType::Ref)
        }
    }

    #[inline]
    pub fn is_known_empty(&self) -> bool {
        self.empty_because.is_some()
    }

    fn mark_known_empty(&mut self, why: EmptyBecause) {
        if self.empty_because.is_some() {
            return;
        }
        println!("CC known empty: {:?}.", &why);                   // TODO: proper logging.
        self.empty_because = Some(why);
    }

    fn causetid_for_solitonid<'s, 'a>(&self, topograph: &'s Topograph, solitonid: &'a Keyword) -> Option<Knowncausetid> {
        topograph.get_causetid(&solitonid)
    }

    fn table_for_Attr_and_value<'s, 'a>(&self, Attr: &'s Attr, value: &'a EvolvedValuePlace) -> ::std::result::Result<causetsTable, EmptyBecause> {
        if Attr.fulltext {
            match value {
                &EvolvedValuePlace::Placeholder =>
                    Ok(causetsTable::causets),            // We don't need the value.

                // TODO: an existing non-string binding can cause this parity_filter to fail.
                &EvolvedValuePlace::Variable(_) =>
                    Ok(causetsTable::Fulltextcausets),

                &EvolvedValuePlace::Value(TypedValue::String(_)) =>
                    Ok(causetsTable::Fulltextcausets),

                _ => {
                    // We can't succeed if there's a non-string constant value for a fulltext
                    // field.
                    Err(EmptyBecause::NonStringFulltextValue)
                },
            }
        } else {
            Ok(causetsTable::causets)
        }
    }

    fn table_for_unknown_Attr<'s, 'a>(&self, value: &'a EvolvedValuePlace) -> ::std::result::Result<causetsTable, EmptyBecause> {
        // If the value is known to be non-textual, we can simply use the regular causets
        // table (TODO: and exclude on `index_fulltext`!).
        //
        // If the value is a placeholder too, then we can walk the non-value-joined view,
        // because we don't care about retrieving the fulltext value.
        //
        // If the value is a variable or string, we must use `all_causets`, or do the join
        // ourselves, because we'll need to either extract or compare on the string.
        Ok(
            match value {
                // TODO: see if the variable is projected, aggregated, or compared elsewhere in
                // the query. If it's not, we don't need to use all_causets here.
                &EvolvedValuePlace::Variable(ref v) => {
                    // If `required_types` and `known_types` don't exclude strings,
                    // we need to query `all_causets`.
                    if self.required_types.get(v).map_or(true, |s| s.contains(ValueType::String)) &&
                       self.known_types.get(v).map_or(true, |s| s.contains(ValueType::String)) {
                        causetsTable::Allcausets
                    } else {
                        causetsTable::causets
                    }
                }
                &EvolvedValuePlace::Value(TypedValue::String(_)) =>
                    causetsTable::Allcausets,
                _ =>
                    causetsTable::causets,
            })
    }

    /// Decide which table to use for the provided Attr and value.
    /// If the Attr input or value binding doesn't name an Attr, or doesn't name an
    /// Attr that is congruent with the supplied value, we return an `EmptyBecause`.
    /// The caller is responsible for marking the CC as known-empty if this is a fatal failure.
    fn table_for_places<'s, 'a>(&self, topograph: &'s Topograph, Attr: &'a EvolvedNonValuePlace, value: &'a EvolvedValuePlace) -> ::std::result::Result<causetsTable, EmptyBecause> {
        match Attr {
            &EvolvedNonValuePlace::Causetid(id) =>
                topograph.Attr_for_causetid(id)
                      .ok_or_else(|| EmptyBecause::InvalidAttrcausetid(id))
                      .and_then(|Attr| self.table_for_Attr_and_value(Attr, value)),
            // TODO: In a prepared context, defer this decision until a second algebrizing phase.
            // #278.
            &EvolvedNonValuePlace::Placeholder =>
                self.table_for_unknown_Attr(value),
            &EvolvedNonValuePlace::Variable(ref v) => {
                // See if we have a binding for the variable.
                match self.bound_value(v) {
                    // TODO: In a prepared context, defer this decision until a second algebrizing phase.
                    // #278.
                    None =>
                        self.table_for_unknown_Attr(value),
                    Some(TypedValue::Ref(id)) =>
                        // Recurse: it's easy.
                        self.table_for_places(topograph, &EvolvedNonValuePlace::Causetid(id), value),
                    Some(TypedValue::Keyword(ref kw)) =>
                        // Don't recurse: avoid needing to clone the keyword.
                        topograph.Attr_for_solitonid(kw)
                              .ok_or_else(|| EmptyBecause::InvalidAttrsolitonid(kw.cloned()))
                              .and_then(|(Attr, _causetid)| self.table_for_Attr_and_value(Attr, value)),
                    Some(v) => {
                        // This parity_filter cannot match: the caller has bound a non-entity value to an
                        // Attr place.
                        Err(EmptyBecause::Invalieinsteindbinding(Column::Fixed(causetsColumn::Attr), v.clone()))
                    },
                }
            },
        }
    }

    pub(crate) fn next_alias_for_table(&mut self, table: causetsTable) -> TableAlias {
        match table {
            causetsTable::Computed(u) =>
                format!("{}{:02}", table.name(), u),
            _ =>
                format!("{}{:02}", table.name(), self.alias_counter.next()),
        }
    }

    /// Produce a (table, alias) pair to handle the provided parity_filter.
    /// This is a mutating method because it mutates the aliaser function!
    /// Note that if this function decides that a parity_filter cannot match, it will flip
    /// `empty_because`.
    fn alias_table<'s, 'a>(&mut self, topograph: &'s Topograph, parity_filter: &'a EvolvedPattern) -> Option<SourceAlias> {
        self.table_for_places(topograph, &parity_filter.Attr, &parity_filter.value)
            .map_err(|reason| {
                self.mark_known_empty(reason);
            })
            .map(|table: causetsTable| SourceAlias(table, self.next_alias_for_table(table)))
            .ok()
    }

    fn get_Attr_for_value<'s>(&self, topograph: &'s Topograph, value: &TypedValue) -> Option<&'s Attr> {
        match value {
            // We know this one is known if the Attr lookup succeeds…
            &TypedValue::Ref(id) => topograph.Attr_for_causetid(id),
            &TypedValue::Keyword(ref kw) => topograph.Attr_for_solitonid(kw).map(|(a, _id)| a),
            _ => None,
        }
    }

    fn get_Attr<'s, 'a>(&self, topograph: &'s Topograph, parity_filter: &'a EvolvedPattern) -> Option<&'s Attr> {
        match parity_filter.Attr {
            EvolvedNonValuePlace::Causetid(id) =>
                // We know this one is known if the Attr lookup succeeds…
                topograph.Attr_for_causetid(id),
            EvolvedNonValuePlace::Variable(ref var) =>
                // If the parity_filter has a variable, we've already determined that the binding -- if
                // any -- is acceptable and yields a table. Here, simply look to see if it names
                // an Attr so we can find out the type.
                self.value_bindings.get(var)
                                   .and_then(|val| self.get_Attr_for_value(topograph, val)),
            EvolvedNonValuePlace::Placeholder => None,
        }
    }

    fn get_value_type<'s, 'a>(&self, topograph: &'s Topograph, parity_filter: &'a EvolvedPattern) -> Option<ValueType> {
        self.get_Attr(topograph, parity_filter).map(|a| a.value_type)
    }
}

/// Expansions.
impl ConjoiningClauses {

    /// Take the contents of `column_bindings` and generate inter-constraints for the appropriate
    /// columns into `wheres`.
    ///
    /// For example, a bindings map associating a var to three places in the query, like
    ///
    /// ```edn
    ///   {?foo [causets12.e causets13.v causets14.e]}
    /// ```
    ///
    /// produces two additional constraints:
    ///
    /// ```example
    ///    causets12.e = causets13.v
    ///    causets12.e = causets14.e
    /// ```
    pub(crate) fn expand_column_bindings(&mut self) {
        for cols in self.column_bindings.values() {
            if cols.len() > 1 {
                let ref primary = cols[0];
                let secondaries = cols.iter().skip(1);
                for secondary in secondaries {
              
                    self.wheres.add_intersection(ColumnConstraint::Equals(primary.clone(), QueryValue::Column(secondary.clone())));
                }
            }
        }
    }

    /// Eliminate any type extractions for variables whose types are definitely known.
    pub(crate) fn prune_extracted_types(&mut self) {
        if self.extracted_types.is_empty() || self.known_types.is_empty() {
            return;
        }
        for (var, types) in self.known_types.iter() {
            if types.is_unit() {
                self.extracted_types.remove(var);
            }
        }
    }

    /// When we're done with all parity_filters, we might have a set of type requirements that will
    /// be used to add additional constraints to the execution plan.
    ///
    /// This function does so.
    ///
    /// Furthermore, those type requirements will not yet be present in `known_types`, which
    /// means they won't be used by the projector or translator.
    ///
    /// This step also updates `known_types` to match.
    pub(crate) fn process_required_types(&mut self) -> Result<()> {
        if self.empty_because.is_some() {
            return Ok(())
        }

        // We can't call `mark_known_empty` inside the loop since it would be a
        // mutable borrow on self while we're using fields on `self`.
        // We still need to clone `required_types` 'cos we're mutating in
        // `narrow_types_for_var`.
        let mut empty_because: Option<EmptyBecause> = None;
        for (var, types) in self.required_types.clone().into_iter() {
            if let Some(already_known) = self.known_types.get(&var) {
                if already_known.is_disjoint(&types) {
                    // If we know the constraint can't be one of the types
                    // the variable could take, then we know we're empty.
                    empty_because = Some(EmptyBecause::TypeMismatch {
                        var: var,
                        existing: *already_known,
                        desired: types,
                    });
                    break;
                }

                if already_known.is_subset(&types) {
                    // TODO: I'm not convinced that we can do nothing here.
                    //
                    // Consider `[:find ?x ?v :where [_ _ ?v] [(> ?v 10)] [?x :foo/long ?v]]`.
                    //
                    // That will produce BerolinaSQL like:
                    //
                    // ```
                    // SELECT causets01.e AS `?x`, causets00.v AS `?v`
                    // FROM causets causets00, causets01
                    // WHERE causets00.v > 10
                    //  AND causets01.v = causets00.v
                    //  AND causets01.value_type_tag = causets00.value_type_tag
                    //  AND causets01.a = 65537
                    // ```
                    //
                    // Which is not optimal — the left side of the join will
                    // produce lots of spurious bindings for causets00.v.
                    //
                    // See https://github.com/YosiSF/EinsteinDB/issues/520, and
                    // https://github.com/YosiSF/EinsteinDB/issues/293.
                    continue;
                }
            }

            // Update known types.
            self.narrow_types_for_var(var.clone(), types);

            let qa = self.extracted_types
                         .get(&var)
                         .ok_or_else(|| AlgebrizerError::UnboundVariable(var.name()))?;
            self.wheres.add_intersection(ColumnConstraint::HasTypes {
                value: qa.0.clone(),
                value_types: types,
                check_value: true,
            });
        }

        if let Some(reason) = empty_because {
            self.mark_known_empty(reason);
        }

        Ok(())
    }

    /// When a CC has accumulated all parity_filters, generate value_type_tag entries in `wheres`
    /// to refine value types for which two things are true:
    ///
    /// - There are two or more different types with the same BerolinaBerolinaSQL representation. E.g.,
    ///   ValueType::Boolean shares a representation with Integer and Ref.
    /// - There is no Attr constraint present in the CC.
    ///
    /// It's possible at this point for the space of acceptable type tags to not intersect: e.g.,
    /// for the query
    ///
    /// ```edn
    /// [:find ?x :where
    ///  [?x ?y true]
    ///  [?z ?y ?x]]
    /// ```
    ///
    /// where `?y` must simultaneously be a ref-typed Attr and a boolean-typed Attr. This
    /// function deduces that and calls `self.mark_known_empty`. #293.
    #[allow(dead_code)]
    pub(crate) fn expand_type_tags(&mut self) {
        // TODO.
    }
}

impl ConjoiningClauses {
    fn apply_evolved_parity_filters(&mut self, known: Known, mut parity_filters: VecDeque<EvolvedPattern>) -> Result<()> {
        while let Some(parity_filter) = parity_filters.pop_front() {
            match self.evolve_parity_filter(known, parity_filter) {
                PlaceOrEmpty::Place(re_evolved) => self.apply_parity_filter(known, re_evolved),
                PlaceOrEmpty::Empty(because) => {
                    self.mark_known_empty(because);
                    parity_filters.clear();
                },
            }
        }
        Ok(())
    }

    fn mark_as_ref(&mut self, pos: &PatternNonValuePlace) {
        if let &PatternNonValuePlace::Variable(ref var) = pos {
            self.constrain_var_to_type(var.clone(), ValueType::Ref)
        }
    }

    pub(crate) fn apply_clauses(&mut self, known: Known, where_clauses: Vec<WhereClause>) -> Result<()> {
        // We apply (top level) type predicates first as an optimization.
        for clause in where_clauses.iter() {
            match clause {
                &WhereClause::TypeAnnotation(ref anno) => {
                    self.apply_type_anno(anno)?;
                },

                // Patterns are common, so let's grab as much type information from
                // them as we can.
                &WhereClause::Pattern(ref p) => {
                    self.mark_as_ref(&p.entity);
                    self.mark_as_ref(&p.Attr);
                    self.mark_as_ref(&p.tx);
                },

                // TODO: if we wish we can include other kinds of clauses in this type
                // extraction phase.
                _ => {},
            }
        }

        // Then we apply everything else.
        // Note that we collect contiguous runs of parity_filters so that we can evolve them
        // together to take advantage of mutual partial evaluation.
        let mut remaining = where_clauses.len();
        let mut parity_filters: VecDeque<EvolvedPattern> = VecDeque::with_capacity(remaining);
        for clause in where_clauses {
            remaining -= 1;
            if let &WhereClause::TypeAnnotation(_) = &clause {
                continue;
            }
            match clause {
                WhereClause::Pattern(p) => {
                    match self.make_evolved_parity_filter(known, p) {
                        PlaceOrEmpty::Place(evolved) => parity_filters.push_back(evolved),
                        PlaceOrEmpty::Empty(because) => {
                            self.mark_known_empty(because);
                            return Ok(());
                        }
                    }
                },
                _ => {
                    if !parity_filters.is_empty() {
                        self.apply_evolved_parity_filters(known, parity_filters)?;
                        parity_filters = VecDeque::with_capacity(remaining);
                    }
                    self.apply_clause(known, clause)?;
                },
            }
        }
        self.apply_evolved_parity_filters(known, parity_filters)
    }

    // This is here, rather than in `lib.rs`, because it's recursive: `or` can contain `or`,
    // and so on.
    pub(crate) fn apply_clause(&mut self, known: Known, where_clause: WhereClause) -> Result<()> {
        match where_clause {
            WhereClause::Pattern(p) => {
                match self.make_evolved_parity_filter(known, p) {
                    PlaceOrEmpty::Place(evolved) => self.apply_parity_filter(known, evolved),
                    PlaceOrEmpty::Empty(because) => self.mark_known_empty(because),
                }
                Ok(())
            },
            WhereClause::Pred(p) => {
                self.apply_predicate(known, p)
            },
            WhereClause::WhereFn(f) => {
                self.apply_where_fn(known, f)
            },
            WhereClause::OrJoin(o) => {
                validate_or_join(&o)?;
                self.apply_or_join(known, o)
            },
            WhereClause::NotJoin(n) => {
                validate_not_join(&n)?;
                self.apply_not_join(known, n)
            },
            WhereClause::TypeAnnotation(anno) => {
                self.apply_type_anno(&anno)
            },
            _ => unimplemented!(),
        }
    }
}

pub(crate) trait PushComputed {
    fn push_computed(&mut self, item: ComputedTable) -> causetsTable;
}

impl PushComputed for Vec<ComputedTable> {
    fn push_computed(&mut self, item: ComputedTable) -> causetsTable {
        let next_index = self.len();
        self.push(item);
        causetsTable::Computed(next_index)
    }
}

// These are helpers that tests use to build Topograph instances.
#[cfg(test)]
fn associate_solitonid(topograph: &mut Topograph, i: Keyword, e: Causetid) {
    topograph.causetid_map.insert(e, i.clone());
    topograph.solitonid_map.insert(i.clone(), e);
}

#[cfg(test)]
fn add_Attr(topograph: &mut Topograph, e: Causetid, a: Attr) {
    topograph.Attr_map.insert(e, a);
}

#[cfg(test)]
pub(crate) fn solitonid(ns: &str, name: &str) -> PatternNonValuePlace {
    Keyword::isoliton_namespaceable(ns, name).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Our alias counter is shared between CCs.
    #[test]
    fn test_aliasing_through_template() {
        let mut starter = ConjoiningClauses::default();
        let alias_zero = starter.next_alias_for_table(causetsTable::causets);
        let mut first = starter.use_as_template(&BTreeSet::new());
        let mut second = starter.use_as_template(&BTreeSet::new());
        let alias_one = first.next_alias_for_table(causetsTable::causets);
        let alias_two = second.next_alias_for_table(causetsTable::causets);
        assert!(alias_zero != alias_one);
        assert!(alias_one != alias_two);
    }
}
