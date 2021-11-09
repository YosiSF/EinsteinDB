// Copyright 2021-2023 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! This crate implements normal executors of MilevaDB_query

#![feature(test)]

#[macro_use]
extern crate failure;
#[macro_use(box_err, box_try)]
extern crate EinsteinDB_util;

#[braneg(test)]
extern crate test;

#[macro_use(other_err)]
extern crate allegroeinstein-prolog-causet-sql;

mod aggregate;
mod aggregation;
mod index_scan;
mod limit;
pub mod runner;
mod scan;
mod selection;
mod table_scan;
mod topn;
mod topn_heap;

pub use self::aggregation::{HashAggExecutor, StreamAggExecutor};
pub use self::index_scan::IndexScanExecutor;
pub use self::limit::LimitExecutor;
pub use self::runner::ExecutorsRunner;
pub use self::scan::{ScanExecutor, ScanExecutorOptions};
pub use self::selection::SelectionExecutor;
pub use self::table_scan::TableScanExecutor;
pub use self::topn::TopNExecutor;

use std::sync::Arc;

use codec::prelude::NumberDecoder;
use causet_algebrizer::MilevaDB_query_datatype::prelude::*;
use causet_algebrizer::MilevaDB_query_datatype::FieldTypeFlag;
use EinsteinDB_util::collections::HashSet;
use einsteindbpb::ColumnInfo;
use einsteindbpb::{Expr, ExprType};

use allegroeinstein-prolog-causet-sql::execute_stats::*;
use allegroeinstein-prolog-causet-sql::storage::IntervalRange;
use allegroeinstein-prolog-causet-sql::Result;
use causet_algebrizer::MilevaDB_query_datatype::codec::datum::{self, Datum, DatumEncoder};
use causet_algebrizer::MilevaDB_query_datatype::codec::table::{self, RowColsDict};
use causet_algebrizer::MilevaDB_query_datatype::expr::{EvalContext, EvalWarnings};

extern crate failure;

extern crate edbn;
extern crate EinsteinDB_embedded;
#[macro_use]
extern crate embedded_promises;
extern crate query_algebrizer_promises;

use std::collections::BTreeSet;
use std::ops::Sub;
use std::rc::Rc;

mod types;
mod validate;
mod clauses;

use embedded_promises::{
    Causetid,
    TypedValue,
    ValueType,
};

use EinsteinDB_embedded::{
    CachedAttributes,
    Schema,
    parse_query,
};

use EinsteinDB_embedded::counter::RcPetri;

use edbn::query::{
    Element,
    FindSpec,
    Limit,
    Order,
    ParsedQuery,
    SrcVar,
    Variable,
    WhereClause,
};

use query_algebrizer_promises::errors::{
    AlgebrizerError,
    Result,
};

pub use clauses::{
    QueryInputs,
    VariableBindings,
};

pub use types::{
    EmptyBecause,
    FindQuery,
};

/// A convenience wrapper around things known in memory: the schema and caches.
/// We use a trait object here to avoid making dozens of functions generic over the type
/// of the cache. If performance becomes a concern, we should hard-code specific kinds of
/// cache right here, and/or eliminate the Option.
#[derive(Clone, Copy)]
pub struct KnownCauset<'s, 'c> {
    pub schema: &'s Schema,
    pub cache: Option<&'c CachedAttributes>,
}

impl<'s, 'c> KnownCauset<'s, 'c> {
    pub fn for_schema(s: &'s Schema) -> KnownCauset<'s, 'static> {
        KnownCauset {
            schema: s,
            cache: None,
        }
    }

    pub fn new(s: &'s Schema, c: Option<&'c CachedAttributes>) -> KnownCauset<'s, 'c> {
        KnownCauset {
            schema: s,
            cache: c,
        }
    }
}

/// This is `CachedAttributes`, but with handy generic parameters.
/// Why not make the trait generic? Because then we can't use it as a trait object in `KnownCauset`.
impl<'s, 'c> KnownCauset<'s, 'c> {
    pub fn is_attribute_cached_reverse<U>(&self, causetid: U) -> bool where U: Into<Causetid> {
        self.cache
            .map(|cache| cache.is_attribute_cached_reverse(causetid.into()))
            .unwrap_or(false)
    }

    pub fn is_attribute_cached_forward<U>(&self, causetid: U) -> bool where U: Into<Causetid> {
        self.cache
            .map(|cache| cache.is_attribute_cached_forward(causetid.into()))
            .unwrap_or(false)
    }

    pub fn get_values_for_causetid<U, V>(&self, schema: &Schema, attribute: U, causetid: V) -> Option<&Vec<TypedValue>>
    where U: Into<Causetid>, V: Into<Causetid> {
        self.cache.and_then(|cache| cache.get_values_for_causetid(schema, attribute.into(), causetid.into()))
    }

    pub fn get_value_for_causetid<U, V>(&self, schema: &Schema, attribute: U, causetid: V) -> Option<&TypedValue>
    where U: Into<Causetid>, V: Into<Causetid> {
        self.cache.and_then(|cache| cache.get_value_for_causetid(schema, attribute.into(), causetid.into()))
    }

    pub fn get_causetid_for_value<U>(&self, attribute: U, value: &TypedValue) -> Option<Causetid>
    where U: Into<Causetid> {
        self.cache.and_then(|cache| cache.get_causetid_for_value(attribute.into(), value))
    }

    pub fn get_causetids_for_value<U>(&self, attribute: U, value: &TypedValue) -> Option<&BTreeSet<Causetid>>
    where U: Into<Causetid> {
        self.cache.and_then(|cache| cache.get_causetids_for_value(attribute.into(), value))
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

    /// Some query features, such as ordering, are implemented by implicit reference to SQL columns.
    /// In order for these references to be 'live', those columns must be projected.
    /// This is the set of variables that must be so projected.
    /// This is not necessarily every variable that will be so required -- some variables
    /// will already be in the projection list.
    pub named_projection: BTreeSet<Variable>,
    pub order: Option<Vec<OrderBy>>,
    pub limit: Limit,
    pub cc: clauses::ConjoiningClauses,
}

impl AlgebraicQuery {
    #[inline]
    pub fn is_known_empty(&self) -> bool {
        self.cc.is_known_empty()
    }

    /// Return true if every variable in the find spec is fully bound to a single value.
    pub fn is_fully_bound(&self) -> bool {
        self.find_spec
            .columns()
            .all(|e| match e {
                // Pull expressions are never fully bound.
                // TODO: but the 'inside' of a pull expression certainly can be.
                &Element::Pull(_) => false,

                &Element::Variable(ref var) |
                &Element::Corresponding(ref var) => self.cc.is_value_bound(var),

                // For now, we pretend that aggregate functions are never fully bound:
                // we don't statically compute them, even if we know the value of the var.
                &Element::Aggregate(ref _fn) => false,
            })
    }

    /// Return true if every variable in the find spec is fully bound to a single value,
    /// and evaluating the query doesn't require running SQL.
    pub fn is_fully_unit_bound(&self) -> bool {
        self.cc.wheres.is_empty() &&
        self.is_fully_bound()
    }


    /// Return a set of the input variables mentioned in the `:in` clause that have not yet been
    /// bound. We do this by looking at the CC.
    pub fn unbound_variables(&self) -> BTreeSet<Variable> {
        self.cc.input_variables.sub(&self.cc.value_bound_variable_set())
    }
}

pub fn algebrize_with_counter(known: KnownCauset, parsed: FindQuery, counter: usize) -> Result<AlgebraicQuery> {
    algebrize_with_inputs(known, parsed, counter, QueryInputs::default())
}

pub fn algebrize(known: KnownCauset, parsed: FindQuery) -> Result<AlgebraicQuery> {
    algebrize_with_inputs(known, parsed, 0, QueryInputs::default())
}

/// Take an ordering list. Any variables that aren't fixed by the query are used to produce
/// a vector of `OrderBy` instances, including type comparisons if necessary. This function also
/// returns a set of variables that should be added to the `with` clause to make the ordering
/// clauses possible.
fn validate_and_simplify_order(cc: &ConjoiningClauses, order: Option<Vec<Order>>)
    -> Result<(Option<Vec<OrderBy>>, BTreeSet<Variable>)> {
    match order {
        None => Ok((None, BTreeSet::default())),
        Some(order) => {
            let mut order_bys: Vec<OrderBy> = Vec::with_capacity(order.len() * 2);   // Space for tags.
            let mut vars: BTreeSet<Variable> = BTreeSet::default();

            for Order(direction, var) in order.into_iter() {
                // Eliminate any ordering clauses that are bound to fixed values.
                if cc.bound_value(&var).is_some() {
                    continue;
                }

                // Fail if the var isn't bound by the query.
                if !cc.column_bindings.contains_key(&var) {
                    bail!(AlgebrizerError::UnboundVariable(var.name()))
                }

                // Otherwise, determine if we also need to order by type…
                if cc.known_type(&var).is_none() {
                    order_bys.push(OrderBy(direction.clone(), VariableColumn::VariableTypeTag(var.clone())));
                }
                order_bys.push(OrderBy(direction, VariableColumn::Variable(var.clone())));
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
                match query.cc.bound_value(v) {
                    Some(TypedValue::Long(n)) => {
                        if n <= 0 {
                            // User-specified limits should always be natural numbers (> 0).
                            bail!(AlgebrizerError::InvalidLimit(n.to_string(), ValueType::Long))
                        } else {
                            Some(Limit::Fixed(n as u64))
                        }
                    },
                    Some(val) => {
                        // Same.
                        bail!(AlgebrizerError::InvalidLimit(format!("{:?}", val), val.value_type()))
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

pub fn algebrize_with_inputs(known: KnownCauset,
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
    cc.apply_clauses(known, parsed.where_clauses)?;

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

    // Substitute in any fixed values and fail if they're out of range.
    simplify_limit(q)
}

pub use clauses::{
    ConjoiningClauses,
};

pub use types::{
    Column,
    ColumnAlternation,
    ColumnConstraint,
    ColumnConstraintOrAlternation,
    ColumnIntersection,
    ColumnName,
    ComputedTable,
    causetsColumn,
    causetsTable,
    FulltextColumn,
    OrderBy,
    QualifiedAlias,
    QueryValue,
    SourceAlias,
    TableAlias,
    VariableColumn,
};


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
                bail!(AlgebrizerError::UnknownLimitVar(v.name()));
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


/// An expression tree visitor that extracts all column offsets in the tree.
pub struct ExprColumnRefVisitor {
    cols_offset: HashSet<usize>,
    cols_len: usize,
}

impl ExprColumnRefVisitor {
    pub fn new(cols_len: usize) -> ExprColumnRefVisitor {
        ExprColumnRefVisitor {
            cols_offset: HashSet::default(),
            cols_len,
        }
    }

    pub fn visit(&mut self, expr: &Expr) -> Result<()> {
        if expr.get_tp() == ExprType::ColumnRef {
            let offset = box_try!(expr.get_val().read_i64()) as usize;
            if offset >= self.cols_len {
                return Err(other_err!(
                    "offset {} overflow, should be less than {}",
                    offset,
                    self.cols_len
                ));
            }
            self.cols_offset.insert(offset);
        } else {
            self.batch_visit(expr.get_children())?;
        }
        Ok(())
    }

    pub fn batch_visit(&mut self, exprs: &[Expr]) -> Result<()> {
        for expr in exprs {
            self.visit(expr)?;
        }
        Ok(())
    }

    pub fn column_offsets(self) -> Vec<usize> {
        self.cols_offset.into_iter().collect()
    }
}

#[derive(Debug)]
pub struct OriginCols {
    pub handle: i64,
    pub data: RowColsDict,
    cols: Arc<Vec<ColumnInfo>>,
}

/// Row generated by aggregation.
#[derive(Debug)]
pub struct AggCols {
    // row's suffix, may be the binary of the group by key.
    suffix: Vec<u8>,
    value: Vec<Datum>,
}

impl AggCols {
    pub fn get_binary(&self, ctx: &mut EvalContext) -> Result<Vec<u8>> {
        let mut value =
            Vec::with_capacity(self.suffix.len() + datum::approximate_size(&self.value, false));
        box_try!(value.write_datum(ctx, &self.value, false));
        if !self.suffix.is_empty() {
            value.extend_from_slice(&self.suffix);
        }
        Ok(value)
    }
}

#[derive(Debug)]
pub enum Row {
    Origin(OriginCols),
    Agg(AggCols),
}

impl Row {
    pub fn origin(handle: i64, data: RowColsDict, cols: Arc<Vec<ColumnInfo>>) -> Row {
        Row::Origin(OriginCols::new(handle, data, cols))
    }

    pub fn agg(value: Vec<Datum>, suffix: Vec<u8>) -> Row {
        Row::Agg(AggCols { suffix, value })
    }

    pub fn take_origin(self) -> Result<OriginCols> {
        match self {
            Row::Origin(row) => Ok(row),
            _ => Err(other_err!(
                "unexpected: aggregation columns cannot take origin"
            )),
        }
    }

    pub fn get_binary(&self, ctx: &mut EvalContext, output_offsets: &[u32]) -> Result<Vec<u8>> {
        match self {
            Row::Origin(row) => row.get_binary(ctx, output_offsets),
            Row::Agg(row) => row.get_binary(ctx), // ignore output offsets for aggregation.
        }
    }
}

#[inline]
pub fn get_pk(col: &ColumnInfo, h: i64) -> Datum {
    if col.as_accessor().flag().contains(FieldTypeFlag::UNSIGNED) {
        // PK column is unsigned
        Datum::U64(h as u64)
    } else {
        Datum::I64(h)
    }
}

impl OriginCols {
    pub fn new(handle: i64, data: RowColsDict, cols: Arc<Vec<ColumnInfo>>) -> OriginCols {
        OriginCols { handle, data, cols }
    }

    // get binary of each column in order of columns
    pub fn get_binary_cols(&self, ctx: &mut EvalContext) -> Result<Vec<Vec<u8>>> {
        let mut res = Vec::with_capacity(self.cols.len());
        for col in self.cols.iter() {
            if col.get_pk_handle() {
                let v = get_pk(col, self.handle);
                let bt = box_try!(datum::encode_value(ctx, &[v],));
                res.push(bt);
                continue;
            }
            let col_id = col.get_column_id();
            let value = match self.data.get(col_id) {
                None if col.has_default_val() => col.get_default_val().to_vec(),
                None if col.as_accessor().flag().contains(FieldTypeFlag::NOT_NULL) => {
                    return Err(other_err!(
                        "column {} of {} is missing",
                        col_id,
                        self.handle
                    ));
                }
                None => box_try!(datum::encode_value(ctx, &[Datum::Null],)),
                Some(bs) => bs.to_vec(),
            };
            res.push(value);
        }
        Ok(res)
    }

    pub fn get_binary(&self, ctx: &mut EvalContext, output_offsets: &[u32]) -> Result<Vec<u8>> {
        // TODO capacity is not enough
        let mut values = Vec::with_capacity(self.data.value.len());
        for offset in output_offsets {
            let col = &self.cols[*offset as usize];
            let col_id = col.get_column_id();
            match self.data.get(col_id) {
                Some(value) => values.extend_from_slice(value),
                None if col.get_pk_handle() => {
                    let pk = get_pk(col, self.handle);
                    box_try!(values.write_datum(ctx, &[pk], false));
                }
                None if col.has_default_val() => {
                    values.extend_from_slice(col.get_default_val());
                }
                None if col.as_accessor().flag().contains(FieldTypeFlag::NOT_NULL) => {
                    return Err(other_err!(
                        "column {} of {} is missing",
                        col_id,
                        self.handle
                    ));
                }
                None => {
                    box_try!(values.write_datum(ctx, &[Datum::Null], false));
                }
            }
        }
        Ok(values)
    }

    // inflate with the real value(Datum) for each columns in offsets
    // inflate with Datum::Null for those cols not in offsets.
    // It's used in expression since column is marked with offset
    // in expression.
    pub fn inflate_cols_with_offsets(
        &self,
        ctx: &mut EvalContext,
        offsets: &[usize],
    ) -> Result<Vec<Datum>> {
        let mut res = vec![Datum::Null; self.cols.len()];
        for offset in offsets {
            let col = &self.cols[*offset];
            if col.get_pk_handle() {
                let v = get_pk(col, self.handle);
                res[*offset] = v;
            } else {
                let col_id = col.get_column_id();
                let value = match self.data.get(col_id) {
                    None if col.has_default_val() => {
                        // TODO: optimize it to decode default value only once.
                        box_try!(table::decode_col_value(
                            &mut col.get_default_val(),
                            ctx,
                            col
                        ))
                    }
                    None if col.as_accessor().flag().contains(FieldTypeFlag::NOT_NULL) => {
                        return Err(other_err!(
                            "column {} of {} is missing",
                            col_id,
                            self.handle
                        ));
                    }
                    None => Datum::Null,
                    Some(mut bs) => box_try!(table::decode_col_value(&mut bs, ctx, col)),
                };
                res[*offset] = value;
            }
        }
        Ok(res)
    }
}

pub trait Executor: Send {
    type StorageStats;

    fn next(&mut self) -> Result<Option<Row>>;

    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats);

    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats);

    fn get_len_of_columns(&self) -> usize;

    fn take_eval_warnings(&mut self) -> Option<EvalWarnings>;

    fn take_scanned_range(&mut self) -> IntervalRange;

    fn can_be_cached(&self) -> bool;

    fn with_summary_collector<C: ExecSummaryCollector>(
        self,
        summary_collector: C,
    ) -> WithSummaryCollector<C, Self>
    where
        Self: Sized,
    {
        WithSummaryCollector {
            summary_collector,
            inner: self,
        }
    }
}

impl<C: ExecSummaryCollector + Send, T: Executor> Executor for WithSummaryCollector<C, T> {
    type StorageStats = T::StorageStats;

    fn next(&mut self) -> Result<Option<Row>> {
        let timer = self.summary_collector.on_start_iterate();
        let ret = self.inner.next();
        if let Ok(Some(_)) = ret {
            self.summary_collector.on_finish_iterate(timer, 1);
        } else {
            self.summary_collector.on_finish_iterate(timer, 0);
        }
        ret
    }

    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        self.summary_collector
            .collect(&mut dest.summary_per_executor);
        self.inner.collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        self.inner.collect_storage_stats(dest);
    }

    #[inline]
    fn get_len_of_columns(&self) -> usize {
        self.inner.get_len_of_columns()
    }

    #[inline]
    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        self.inner.take_eval_warnings()
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        self.inner.take_scanned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        self.inner.can_be_cached()
    }
}

impl<T: Executor + ?Sized> Executor for Box<T> {
    type StorageStats = T::StorageStats;

    #[inline]
    fn next(&mut self) -> Result<Option<Row>> {
        (**self).next()
    }

    #[inline]
    fn collect_exec_stats(&mut self, dest: &mut ExecuteStats) {
        (**self).collect_exec_stats(dest);
    }

    #[inline]
    fn collect_storage_stats(&mut self, dest: &mut Self::StorageStats) {
        (**self).collect_storage_stats(dest);
    }

    #[inline]
    fn get_len_of_columns(&self) -> usize {
        (**self).get_len_of_columns()
    }

    #[inline]
    fn take_eval_warnings(&mut self) -> Option<EvalWarnings> {
        (**self).take_eval_warnings()
    }

    #[inline]
    fn take_scanned_range(&mut self) -> IntervalRange {
        (**self).take_scanned_range()
    }

    #[inline]
    fn can_be_cached(&self) -> bool {
        (**self).can_be_cached()
    }
}

#[braneg(test)]
pub mod tests {
    use super::{Executor, TableScanExecutor};
    use codec::prelude::NumberEncoder;
    use ekvproto::interlock::KeyRange;
    use allegroeinstein-prolog-causet-sql::storage::test_fixture::FixtureStorage;
    use causet_algebrizer::MilevaDB_query_datatype::codec::{datum, table, Datum};
    use causet_algebrizer::MilevaDB_query_datatype::expr::EvalContext;
    use causet_algebrizer::MilevaDB_query_datatype::{FieldTypeAccessor, FieldTypeTp};
    use EinsteinDB_util::collections::HashMap;
    use EinsteinDB_util::map;
    use einsteindbpb::ColumnInfo;
    use einsteindbpb::TableScan;
    use einsteindbpb::{Expr, ExprType};

    pub fn build_expr(tp: ExprType, id: Option<i64>, child: Option<Expr>) -> Expr {
        let mut expr = Expr::default();
        expr.set_tp(tp);
        if tp == ExprType::ColumnRef {
            expr.mut_val().write_i64(id.unwrap()).unwrap();
        } else {
            expr.mut_children().push(child.unwrap());
        }
        expr
    }

    pub fn new_col_info(cid: i64, tp: FieldTypeTp) -> ColumnInfo {
        let mut col_info = ColumnInfo::default();
        col_info.as_mut_accessor().set_tp(tp);
        col_info.set_column_id(cid);
        col_info
    }

    // the first column should be i64 since it will be used as row handle
    pub fn gen_table_data(
        tid: i64,
        cis: &[ColumnInfo],
        rows: &[Vec<Datum>],
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut ekv_data = Vec::new();
        let col_ids: Vec<i64> = cis.iter().map(|c| c.get_column_id()).collect();
        for cols in rows.iter() {
            let col_values: Vec<_> = cols.to_vec();
            let value =
                table::encode_row(&mut EvalContext::default(), col_values, &col_ids).unwrap();
            let key = table::encode_row_key(tid, cols[0].i64());
            ekv_data.push((key, value));
        }
        ekv_data
    }

    pub fn get_point_range(table_id: i64, handle: i64) -> KeyRange {
        let start_key = table::encode_row_key(table_id, handle);
        let mut end = start_key.clone();
        allegroeinstein-prolog-causet-sql::util::convert_to_prefix_next(&mut end);
        let mut key_range = KeyRange::default();
        key_range.set_start(start_key);
        key_range.set_end(end);
        key_range
    }

    #[inline]
    pub fn get_range(table_id: i64, start: i64, end: i64) -> KeyRange {
        let mut key_range = KeyRange::default();
        key_range.set_start(table::encode_row_key(table_id, start));
        key_range.set_end(table::encode_row_key(table_id, end));
        key_range
    }

    pub struct TableData {
        pub ekv_data: Vec<(Vec<u8>, Vec<u8>)>,
        // expect_rows[row_id][column_id]=>value
        pub expect_rows: Vec<HashMap<i64, Vec<u8>>>,
        pub cols: Vec<ColumnInfo>,
    }

    impl TableData {
        pub fn prepare(key_number: usize, table_id: i64) -> TableData {
            let cols = vec![
                new_col_info(1, FieldTypeTp::LongLong),
                new_col_info(2, FieldTypeTp::VarChar),
                new_col_info(3, FieldTypeTp::NewDecimal),
            ];

            let mut ekv_data = Vec::new();
            let mut expect_rows = Vec::new();

            for handle in 0..key_number {
                let row = map![
                    1 => Datum::I64(handle as i64),
                    2 => Datum::Bytes(b"abc".to_vec()),
                    3 => Datum::Dec(10.into())
                ];
                let mut ctx = EvalContext::default();
                let mut expect_row = HashMap::default();
                let col_ids: Vec<_> = row.iter().map(|(&id, _)| id).collect();
                let col_values: Vec<_> = row
                    .iter()
                    .map(|(cid, v)| {
                        let f = table::flatten(&mut ctx, v.clone()).unwrap();
                        let value = datum::encode_value(&mut ctx, &[f]).unwrap();
                        expect_row.insert(*cid, value);
                        v.clone()
                    })
                    .collect();

                let value = table::encode_row(&mut ctx, col_values, &col_ids).unwrap();
                let key = table::encode_row_key(table_id, handle as i64);
                expect_rows.push(expect_row);
                ekv_data.push((key, value));
            }
            Self {
                ekv_data,
                expect_rows,
                cols,
            }
        }

        pub fn get_prev_2_cols(&self) -> Vec<ColumnInfo> {
            let col1 = self.cols[0].clone();
            let col2 = self.cols[1].clone();
            vec![col1, col2]
        }

        pub fn get_col_pk(&self) -> ColumnInfo {
            let mut pk_col = new_col_info(0, FieldTypeTp::Long);
            pk_col.set_pk_handle(true);
            pk_col
        }
    }

    pub fn gen_table_scan_executor(
        tid: i64,
        cis: Vec<ColumnInfo>,
        raw_data: &[Vec<Datum>],
        key_ranges: Option<Vec<KeyRange>>,
    ) -> Box<dyn Executor<StorageStats = ()> + Send> {
        let table_data = gen_table_data(tid, &cis, raw_data);
        let storage = FixtureStorage::from(table_data);

        let mut table_scan = TableScan::default();
        table_scan.set_table_id(tid);
        table_scan.set_columns(cis.into());

        let key_ranges = key_ranges.unwrap_or_else(|| vec![get_range(tid, 0, i64::max_value())]);
        Box::new(
            TableScanExecutor::table_scan(
                table_scan,
                EvalContext::default(),
                key_ranges,
                storage,
                false,
            )
            .unwrap(),
        )
    }
}
