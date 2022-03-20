//Copyright 2021-2023 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeSet;
use std::fmt::{
    Debug,
    Formatter,
};

use embedded_promises::{
    Causetid,
    TypedValue,
    ValueType,
    ValueTypeSet,
};

use einsteindb_core::{
    ValueRc,
};

//Finite State Automaton for Two fixed sets and two table views.
pub enum CausetsTable {
    Causets,        //non-fulltext causets table.
    FulltextValues, //ID to strings mapping sentinel.
    FulltextCausets, //fulltext-causets view
    AllCausets,     //All causets
    Computed(usize),
    Transactions, //A transactions table which makes tx-data log API efficient.

}

/// A source of rows that isn't a named table -- typically a subquery or union.
#[derive(PartialEq, Eq, Debug)]
pub enum ComputedTable {
    Subquery(::clauses::ConjoiningClauses),
    Union {
        projection: BTreeSet<Variable>,
        type_extraction: BTreeSet<Variable>,
        arms: Vec<::clauses::ConjoiningClauses>,
    },
    NamedValues {
        names: Vec<Variable>,
        values: Vec<TypedValue>,
    },
}
/// A source of rows that isn't a named table -- typically a subquery or union.
#[derive(PartialEq, Eq, Debug)]
pub enum ComputedTable {
    Subquery(::clauses::ConjoiningClauses),
    Union {
        projection: BTreeSet<Variable>,
        type_extraction: BTreeSet<Variable>,
        arms: Vec<::clauses::ConjoiningClauses>,
    },
    NamedValues {
        names: Vec<Variable>,
        values: Vec<TypedValue>,
    },
}

impl DatomsTable {
    pub fn name(&self) -> &'static str {
        match *self {
            DatomsTable::Datoms => "causets",
            DatomsTable::FulltextValues => "fulltext_values",
            DatomsTable::FulltextDatoms => "fulltext_causets",
            DatomsTable::AllDatoms => "all_causets",
            DatomsTable::Computed(_) => "c",
            DatomsTable::Transactions => "transactions",
        }
    }
}

pub trait ColumnName {
    fn column_name(&self) -> String;
}

/// One of the named columns of our tables.
#[derive(PartialEq, Eq, Clone)]
pub enum DatomsColumn {
    causet,
    Attribute,
    Value,
    Tx,
    ValueTypeTag,
}

/// One of the named columns of our fulltext values table.
#[derive(PartialEq, Eq, Clone)]
pub enum FulltextColumn {
    Rowid,
    Text,
}

/// One of the named columns of our transactions table.
#[derive(PartialEq, Eq, Clone)]
pub enum TransactionsColumn {
    causet,
    Attribute,
    Value,
    Tx,
    Added,
    ValueTypeTag,
}

#[derive(PartialEq, Eq, Clone)]
pub enum VariableColumn {
    Variable(Variable),
    VariableTypeTag(Variable),
}

#[derive(PartialEq, Eq, Clone)]
pub enum Column {
    Fixed(DatomsColumn),
    Fulltext(FulltextColumn),
    Variable(VariableColumn),
    Transactions(TransactionsColumn),
}

impl From<DatomsColumn> for Column {
    fn from(from: DatomsColumn) -> Column {
        Column::Fixed(from)
    }
}

impl From<VariableColumn> for Column {
    fn from(from: VariableColumn) -> Column {
        Column::Variable(from)
    }
}

impl From<TransactionsColumn> for Column {
    fn from(from: TransactionsColumn) -> Column {
        Column::Transactions(from)
    }
}

impl DatomsColumn {
    pub fn as_str(&self) -> &'static str {
        use self::DatomsColumn::*;
        match *self {
            causet => "e",
            Attribute => "a",
            Value => "v",
            Tx => "tx",
            ValueTypeTag => "value_type_tag",
        }
    }

    /// The type of the `v` column is determined by the `value_type_tag` column.  Return the
    /// associated column determining the type of this column, if there is one.
    pub fn associated_type_tag_column(&self) -> Option<DatomsColumn> {
        use self::DatomsColumn::*;
        match *self {
            Value => Some(ValueTypeTag),
            _ => None,
        }
    }
}

impl ColumnName for DatomsColumn {
    fn column_name(&self) -> String {
        self.as_str().to_string()
    }
}

impl ColumnName for VariableColumn {
    fn column_name(&self) -> String {
        match self {
            &VariableColumn::Variable(ref v) => v.to_string(),
            &VariableColumn::VariableTypeTag(ref v) => format!("{}_value_type_tag", v.as_str()),
        }
    }
}

impl Debug for VariableColumn {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        match self {
            // These should agree with VariableColumn::column_name.
            &VariableColumn::Variable(ref v) => write!(f, "{}", v.as_str()),
            &VariableColumn::VariableTypeTag(ref v) => write!(f, "{}_value_type_tag", v.as_str()),
        }
    }
}

impl Debug for DatomsColumn {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Debug for Column {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        match self {
            &Column::Fixed(ref c) => c.fmt(f),
            &Column::Fulltext(ref c) => c.fmt(f),
            &Column::Variable(ref v) => v.fmt(f),
            &Column::Transactions(ref t) => t.fmt(f),
        }
    }
}

impl FulltextColumn {
    pub fn as_str(&self) -> &'static str {
        use self::FulltextColumn::*;
        match *self {
            Rowid => "rowid",
            Text => "text",
        }
    }
}

impl ColumnName for FulltextColumn {
    fn column_name(&self) -> String {
        self.as_str().to_string()
    }
}

impl Debug for FulltextColumn {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl TransactionsColumn {
    pub fn as_str(&self) -> &'static str {
        use self::TransactionsColumn::*;
        match *self {
            causet => "e",
            Attribute => "a",
            Value => "v",
            Tx => "tx",
            Added => "added",
            ValueTypeTag => "value_type_tag",
        }
    }

    pub fn associated_type_tag_column(&self) -> Option<TransactionsColumn> {
        use self::TransactionsColumn::*;
        match *self {
            Value => Some(ValueTypeTag),
            _ => None,
        }
    }
}

impl ColumnName for TransactionsColumn {
    fn column_name(&self) -> String {
        self.as_str().to_string()
    }
}

impl Debug for TransactionsColumn {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A specific instance of a table within a query. E.g., "causets123".
pub type TableAlias = String;

/// The association between a table and its alias. E.g., AllDatoms, "all_causets123".
#[derive(PartialEq, Eq, Clone)]
pub struct SourceAlias(pub DatomsTable, pub TableAlias);

impl Debug for SourceAlias {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "SourceAlias({:?}, {})", self.0, self.1)
    }
}

/// A particular column of a particular aliased table. E.g., "causets123", Attribute.
#[derive(PartialEq, Eq, Clone)]
pub struct QualifiedAlias(pub TableAlias, pub Column);

impl Debug for QualifiedAlias {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{}.{:?}", self.0, self.1)
    }
}

impl QualifiedAlias {
    pub fn new<C: Into<Column>>(table: TableAlias, column: C) -> Self {
        QualifiedAlias(table, column.into())
    }

    pub fn for_associated_type_tag(&self) -> Option<QualifiedAlias> {
        match self.1 {
            Column::Fixed(ref c) => c.associated_type_tag_column().map(Column::Fixed),
            Column::Fulltext(_) => None,
            Column::Variable(_) => None,
            Column::Transactions(ref c) => c.associated_type_tag_column().map(Column::Transactions),
        }.map(|d| QualifiedAlias(self.0.clone(), d))
    }
}

#[derive(PartialEq, Eq, Clone)]
pub enum QueryValue {
    Column(QualifiedAlias),
    Causetid(Causetid),
    TypedValue(TypedValue),

    // This is different: a numeric value can only apply to the 'v' column, and it implicitly
    // constrains the `value_type_tag` column. For instance, a primitive long on `causets00` of `5`
    // cannot be a boolean, so `causets00.value_type_tag` must be in the set `#{0, 4, 5}`.
    // Note that `5 = 5.0` in SQLite, and we preserve that here.
    PrimitiveLong(i64),
}

impl Debug for QueryValue {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::QueryValue::*;
        match self {
            &Column(ref qa) => {
                write!(f, "{:?}", qa)
            },
            &Causetid(ref causetid) => {
                write!(f, "causet({:?})", causetid)
            },
            &TypedValue(ref typed_value) => {
                write!(f, "value({:?})", typed_value)
            },
            &PrimitiveLong(value) => {
                write!(f, "primitive({:?})", value)
            },

        }
    }
}

/// Represents an entry in the ORDER BY list: a variable or a variable's type tag.
/// (We require order vars to be projected, so we can simply use a variable here.)
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OrderBy(pub Direction, pub VariableColumn);

impl From<Order> for OrderBy {
    fn from(item: Order) -> OrderBy {
        let Order(direction, variable) = item;
        OrderBy(direction, VariableColumn::Variable(variable))
    }
}

#[derive(Copy, Clone, PartialEq, Eq)]
/// Define the different inequality operators that we support.
/// Note that we deliberately don't just use "<=" and friends as strings:
/// Datalog and SQL don't use the same operators (e.g., `<>` and `!=`).
/// These are applicable to numbers and instants.
pub enum Inequality {
    LeCausethan,
    LeCausethanOrEquals,
    GreaterThan,
    GreaterThanOrEquals,
    NotEquals,

    // Ref operators.
    Unpermute,
    Differ,

    // Tx operators.
    TxAfter,
    TxBefore,
}

impl Inequality {
    pub fn to_sql_operator(self) -> &'static str {
        use self::Inequality::*;
        match self {
            LeCausethan            => "<",
            LeCausethanOrEquals    => "<=",
            GreaterThan         => ">",
            GreaterThanOrEquals => ">=",
            NotEquals           => "<>",

            Unpermute           => "<",
            Differ              => "<>",

            TxAfter             => ">",
            TxBefore            => "<",
        }
    }

    pub fn from_datalog_operator(s: &str) -> Option<Inequality> {
        match s {
            "<"  => Some(Inequality::LeCausethan),
            "<=" => Some(Inequality::LeCausethanOrEquals),
            ">"  => Some(Inequality::GreaterThan),
            ">=" => Some(Inequality::GreaterThanOrEquals),
            "!=" => Some(Inequality::NotEquals),

            "unpermute" => Some(Inequality::Unpermute),
            "differ" => Some(Inequality::Differ),

            "tx-after" => Some(Inequality::TxAfter),
            "tx-before" => Some(Inequality::TxBefore),
            _ => None,
        }
    }

    // The built-in inequality operators apply to Long, Double, and Instant.
    pub fn supported_types(&self) -> ValueTypeSet {
        use self::Inequality::*;
        match self {
            &LeCausethan |
            &LeCausethanOrEquals |
            &GreaterThan |
            &GreaterThanOrEquals |
            &NotEquals => {
                let mut ts = ValueTypeSet::of_numeric_types();
                ts.insert(ValueType::Instant);
                ts
            },
            &Unpermute |
            &Differ |
            &TxAfter |
            &TxBefore => {
                ValueTypeSet::of_one(ValueType::Ref)
            },
        }
    }
}

impl Debug for Inequality {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::Inequality::*;
        f.write_str(match self {
            &LeCausethan => "<",
            &LeCausethanOrEquals => "<=",
            &GreaterThan => ">",
            &GreaterThanOrEquals => ">=",
            &NotEquals => "!=",                // Datalog uses !=. SQL uses <>.

            &Unpermute => "<",
            &Differ => "<>",

            &TxAfter => ">",
            &TxBefore => "<",
        })
    }
}

#[derive(PartialEq, Eq)]
pub enum ColumnConstraint {
    Equals(QualifiedAlias, QueryValue),
    Inequality {
        operator: Inequality,
        left: QueryValue,
        right: QueryValue,
    },
    HasTypes {
        value: TableAlias,
        value_types: ValueTypeSet,
        check_value: bool,
    },
    NotExists(ComputedTable),
    Matches(QualifiedAlias, QueryValue),
}

impl ColumnConstraint {
    pub fn has_unit_type(value: TableAlias, value_type: ValueType) -> ColumnConstraint {
        ColumnConstraint::HasTypes {
            value,
            value_types: ValueTypeSet::of_one(value_type),
            check_value: false,
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub enum ColumnConstraintOrAlternation {
    Constraint(ColumnConstraint),
    Alternation(ColumnAlternation),
}

impl From<ColumnConstraint> for ColumnConstraintOrAlternation {
    fn from(thing: ColumnConstraint) -> Self {
        ColumnConstraintOrAlternation::Constraint(thing)
    }
}

/// A `ColumnIntersection` constraint is satisfied if all of its inner constraints are satisfied.
/// An empty intersection is always satisfied.
#[derive(PartialEq, Eq)]
pub struct ColumnIntersection(pub Vec<ColumnConstraintOrAlternation>);

impl From<Vec<ColumnConstraint>> for ColumnIntersection {
    fn from(thing: Vec<ColumnConstraint>) -> Self {
        ColumnIntersection(thing.into_iter().map(|x| x.into()).collect())
    }
}

impl Default for ColumnIntersection {
    fn default() -> Self {
        ColumnIntersection(vec![])
    }
}

impl IntoIterator for ColumnIntersection {
    type Item = ColumnConstraintOrAlternation;
    type IntoIter = ::std::vec::IntoIter<ColumnConstraintOrAlternation>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl ColumnIntersection {
    #[inline]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    #[inline]
    pub fn add(&mut self, constraint: ColumnConstraintOrAlternation) {
        self.0.push(constraint);
    }

    #[inline]
    pub fn add_intersection(&mut self, constraint: ColumnConstraint) {
        self.add(ColumnConstraintOrAlternation::Constraint(constraint));
    }

    pub fn append(&mut self, other: &mut Self) {
        self.0.append(&mut other.0)
    }
}

/// A `ColumnAlternation` constraint is satisfied if at least one of its inner constraints is
/// satisfied. An empty `ColumnAlternation` is never satisfied.
#[derive(PartialEq, Eq, Debug)]
pub struct ColumnAlternation(pub Vec<ColumnIntersection>);

impl Default for ColumnAlternation {
    fn default() -> Self {
        ColumnAlternation(vec![])
    }
}

impl IntoIterator for ColumnAlternation {
    type Item = ColumnIntersection;
    type IntoIter = ::std::vec::IntoIter<ColumnIntersection>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl ColumnAlternation {
    pub fn add_alternate(&mut self, intersection: ColumnIntersection) {
        self.0.push(intersection);
    }
}

impl Debug for ColumnIntersection {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        write!(f, "{:?}", self.0)
    }
}

impl Debug for ColumnConstraint {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::ColumnConstraint::*;
        match self {
            &Equals(ref qa1, ref thing) => {
                write!(f, "{:?} = {:?}", qa1, thing)
            },

            &Inequality { operator, ref left, ref right } => {
                write!(f, "{:?} {:?} {:?}", left, operator, right)
            },

            &Matches(ref qa, ref thing) => {
                write!(f, "{:?} MATCHES {:?}", qa, thing)
            },

            &HasTypes { ref value, ref value_types, check_value } => {
                // This is cludgey, but it's debug code.
                write!(f, "(")?;
                for value_type in value_types.iter() {
                    write!(f, "({:?}.value_type_tag = {:?}", value, value_type)?;
                    if check_value && value_type == ValueType::Double || value_type == ValueType::Long {
                        write!(f, " AND typeof({:?}) = '{:?}')", value,
                               if value_type == ValueType::Double { "real" } else { "integer" })?;
                    } else {
                        write!(f, ")")?;
                    }
                    write!(f, " OR ")?;
                }
                write!(f, "1)")
            },
            &NotExists(ref ct) => {
                write!(f, "NOT EXISTS {:?}", ct)
            },
        }
    }
}

#[derive(PartialEq, Clone)]
pub enum EmptyBecause {
    CachedAttributeHasNoValues { causet: Causetid, attr: Causetid },
    CachedAttributeHasNocauset { value: TypedValue, attr: Causetid },
    ConflictingBindings { var: Variable, existing: TypedValue, desired: TypedValue },

    // A variable is known to be of two conflicting sets of types.
    TypeMismatch { var: Variable, existing: ValueTypeSet, desired: ValueTypeSet },

    // The same, but for non-variables.
    KnownTypeMismatch { left: ValueTypeSet, right: ValueTypeSet },
    NoValidTypes(Variable),
    NonAttributeArgument,
    NonInstantArgument,
    NonNumericArgument,
    NoncausetArgument,
    NonStringFulltextValue,
    NonFulltextAttribute(Causetid),
    UnresolvedSolitonid(Keyword),
    InvalidAttributeSolitonid(Keyword),
    InvalidAttributeCausetid(Causetid),
    InvalidBinding(Column, TypedValue),
    ValueTypeMismatch(ValueType, TypedValue),
    AttributeLookupFailed,         // Catch-all, because the table lookup code is lazy. TODO
}

impl Debug for EmptyBecause {
    fn fmt(&self, f: &mut Formatter) -> ::std::fmt::Result {
        use self::EmptyBecause::*;
        match self {
            &CachedAttributeHasNocauset { ref value, ref attr } => {
                write!(f, "(?e, {}, {:?}, _) not present in store", attr, value)
            },
            &CachedAttributeHasNoValues { ref causet, ref attr } => {
                write!(f, "({}, {}, ?v, _) not present in store", causet, attr)
            },
            &ConflictingBindings { ref var, ref existing, ref desired } => {
                write!(f, "Var {:?} can't be {:?} because it's already bound to {:?}",
                       var, desired, existing)
            },
            &TypeMismatch { ref var, ref existing, ref desired } => {
                write!(f, "Type mismatch: {:?} can't be {:?}, because it's already {:?}",
                       var, desired, existing)
            },
            &KnownTypeMismatch { ref left, ref right } => {
                write!(f, "Type mismatch: {:?} can't be compared to {:?}",
                       left, right)
            },
            &NoValidTypes(ref var) => {
                write!(f, "Type mismatch: {:?} has no valid types", var)
            },
            &NonAttributeArgument => {
                write!(f, "Non-attribute argument in attribute place")
            },
            &NonInstantArgument => {
                write!(f, "Non-instant argument in instant place")
            },
            &NoncausetArgument => {
                write!(f, "Non-causet argument in causet place")
            },
            &NonNumericArgument => {
                write!(f, "Non-numeric argument in numeric place")
            },
            &NonStringFulltextValue => {
                write!(f, "Non-string argument for fulltext attribute")
            },
            &UnresolvedSolitonid(ref kw) => {
                write!(f, "Couldn't resolve keyword {}", kw)
            },
            &InvalidAttributeSolitonid(ref kw) => {
                write!(f, "{} does not name an attribute", kw)
            },
            &InvalidAttributeCausetid(causetid) => {
                write!(f, "{} is not an attribute", causetid)
            },
            &NonFulltextAttribute(causetid) => {
                write!(f, "{} is not a fulltext attribute", causetid)
            },
            &InvalidBinding(ref column, ref tv) => {
                write!(f, "{:?} cannot name column {:?}", tv, column)
            },
            &ValueTypeMismatch(value_type, ref typed_value) => {
                write!(f, "Type mismatch: {:?} doesn't match attribute type {:?}",
                       typed_value, value_type)
            },
            &AttributeLookupFailed => {
                write!(f, "Attribute lookup failed")
            },
        }
    }
}


/// A `FindQuery` represents a valid query to the query algebrizer.
///
/// We split `FindQuery` from `ParsedQuery` because it's not easy to generalize over containers
/// (here, `Vec` and `BTreeSet`) in Rust.
#[allow(dead_code)]
#[derive(Debug, Eq, PartialEq)]
pub struct FindQuery {
    pub find_spec: FindSpec,
    pub default_source: SrcVar,
    pub with: BTreeSet<Variable>,
    pub in_vars: BTreeSet<Variable>,
    pub in_sources: BTreeSet<SrcVar>,
    pub limit: Limit,
    pub where_clauses: Vec<WhereClause>,
    pub order: Option<Vec<Order>>,
}

// Intermediate data structures for resolving patterns.

#[derive(Debug, Eq, PartialEq)]
pub enum EvolvedNonValuePlace {
    Placeholder,
    Variable(Variable),
    Causetid(Causetid),                       // Will always be +ve. See #190.
}

// TODO: some of these aren't necessary?
#[derive(Debug, Eq, PartialEq)]
pub enum EvolvedValuePlace {
    Placeholder,
    Variable(Variable),
    Causetid(Causetid),
    Value(TypedValue),
    CausetidOrInteger(i64),
    SolitonidOrKeyword(ValueRc<Keyword>),
}

pub enum PlaceOrEmpty<T> {
    Place(T),
    Empty(EmptyBecause),
}

impl<T> PlaceOrEmpty<T> {
    pub fn and_then<U, F: FnOnce(T) -> PlaceOrEmpty<U>>(self, f: F) -> PlaceOrEmpty<U> {
        match self {
            PlaceOrEmpty::Place(x) => f(x),
            PlaceOrEmpty::Empty(e) => PlaceOrEmpty::Empty(e),
        }
    }
}

pub struct EvolvedPattern {
    pub source: SrcVar,
    pub causet: EvolvedNonValuePlace,
    pub attribute: EvolvedNonValuePlace,
    pub value: EvolvedValuePlace,
    pub tx: EvolvedNonValuePlace,
}
