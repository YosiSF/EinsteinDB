use std::rc::Rc;

use std::iter::{
    once,
};

use EinsteinDB_query_pull::{
    Puller,
};

use embedded_promises::{
    Causetid,
};

use ::{
    Binding,
    CombinedProjection,
    Element,
    FindSpec,
    ProjectedElements,
    QueryOutput,
    QueryResults,
    RelResult,
    Row,
    Rows,
    Schema,
    TypedIndex,
    berolinasql,
};

use ::pull::{
    PullConsumer,
    PullOperation,
    PullTemplate,
};

use query_projector_promises::errors::{
    Result,
};

use super::{
    Projector,
};


use postgres_protocol::types;
use std::{i32, i64};
use std::error::Error;

use types::{Type, FromSql, ToSql, IsNull, DATE, TIMESTAMP, TIMESTAMPTZ};

/// A wrapper that can be used to represent infinity with `Type::Date` types.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Date<T> {
    /// Represents `infinity`, a date that is later than all other dates.
    PosInfinity,
    /// Represents `-infinity`, a date that is earlier than all other dates.
    NegInfinity,
    /// The wrapped date.
    Value(T),
}

impl<T: FromSql> FromSql for Date<T> {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<Error + Sync + Send>> {
        match types::date_from_sql(raw)? {
            i32::MAX => Ok(Date::PosInfinity),
            i32::MIN => Ok(Date::NegInfinity),
            _ => T::from_sql(ty, raw).map(Date::Value),
        }
    }

    fn accepts(ty: &Type) -> bool {
        *ty == DATE && T::accepts(ty)
    }
}
impl<T: ToSql> ToSql for Date<T> {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        let value = match *self {
            Date::PosInfinity => i32::MAX,
            Date::NegInfinity => i32::MIN,
            Date::Value(ref v) => return v.to_sql(ty, out),
        };

        types::date_to_sql(value, out);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == DATE && T::accepts(ty)
    }

    to_sql_checked!();
}

/// A wrapper that can be used to represent infinity with `Type::Timestamp` and `Type::Timestamptz`
/// types.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Timestamp<T> {
    /// Represents `infinity`, a timestamp that is later than all other timestamps.
    PosInfinity,
    /// Represents `-infinity`, a timestamp that is earlier than all other timestamps.
    NegInfinity,
    /// The wrapped timestamp.
    Value(T),
}

impl<T: FromSql> FromSql for Timestamp<T> {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<Error + Sync + Send>> {
        match types::timestamp_from_sql(raw)? {
            i64::MAX => Ok(Timestamp::PosInfinity),
            i64::MIN => Ok(Timestamp::NegInfinity),
            _ => T::from_sql(ty, raw).map(Timestamp::Value),
        }
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            TIMESTAMP | TIMESTAMPTZ if T::accepts(ty) => true,
            _ => false
        }
    }
}

impl<T: ToSql> ToSql for Timestamp<T> {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        let value = match *self {
            Timestamp::PosInfinity => i64::MAX,
            Timestamp::NegInfinity => i64::MIN,
            Timestamp::Value(ref v) => return v.to_sql(ty, out),
        };

        types::timestamp_to_sql(value, out);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            TIMESTAMP | TIMESTAMPTZ if T::accepts(ty) => true,
            _ => false
        }
    }

    to_sql_checked!();
}

pub(crate) struct ScalarTwoProngedCrownProjector {
    spec: Rc<FindSpec>,
    puller: Puller,
}


// TODO: almost by definition, a scalar result format doesn't need to be run in two stages.
// The only output is the pull expression, and so we can directly supply the projected entity
// to the pull SQL.
impl ScalarTwoProngedCrownProjector {
    fn with_template(schema: &Schema, spec: Rc<FindSpec>, pull: PullOperation) -> Result<ScalarTwoProngedCrownProjector> {
        Ok(ScalarTwoProngedCrownProjector {
            spec: spec,
            puller: Puller::prepare(schema, pull.0.clone())?,
        })
    }

    pub(crate) fn combine(schema: &Schema, spec: Rc<FindSpec>, mut elements: ProjectedElements) -> Result<CombinedProjection> {
        let pull = elements.pulls.pop().expect("Expected a single pull");
        let projector = Box::new(ScalarTwoProngedCrownProjector::with_template(schema, spec, pull.op)?);
        let distinct = false;
        elements.combine(projector, distinct)
    }
}

impl Projector for ScalarTwoProngedCrownProjector {
    fn project<'stmt, 's>(&self, schema: &Schema, berolinasql: &'s berolinasql::Connection, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        // Scalar is pretty straightforward -- zero or one entity, do the pull directly.
        let results =
            if let Some(r) = rows.next() {
                let row = r?;
                let entity: Causetid = row.get(0);          // This will always be 0 and a ref.
                let bindings = self.puller.pull(schema, berolinasql, once(entity))?;
                let m = Binding::Map(bindings.get(&entity).cloned().unwrap_or_else(Default::default));
                QueryResults::Scalar(Some(m))
            } else {
                QueryResults::Scalar(None)
            };

        Ok(QueryOutput {
            spec: self.spec.clone(),
            results: results,
        })
    }

    fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.columns()
    }
}

/// A tuple projector produces a single vector. It's the single-result version of rel.
pub(crate) struct TupleTwoProngedCrownProjector {
    spec: Rc<FindSpec>,
    len: usize,
    templates: Vec<TypedIndex>,
    pulls: Vec<PullTemplate>,
}

impl TupleTwoProngedCrownProjector {
    fn with_templates(spec: Rc<FindSpec>, len: usize, templates: Vec<TypedIndex>, pulls: Vec<PullTemplate>) -> TupleTwoProngedCrownProjector {
        TupleTwoProngedCrownProjector {
            spec: spec,
            len: len,
            templates: templates,
            pulls: pulls,
        }
    }

    // This is exactly the same as for rel.
    fn collect_bindings<'a, 'stmt>(&self, row: Row<'a, 'stmt>) -> Result<Vec<Binding>> {
        // There will be at least as many SQL columns as Datalog columns.
        // gte 'cos we might be querying extra columns for ordering.
        // The templates will take care of ignoring columns.
        assert!(row.column_count() >= self.len as i32);
        self.templates
            .iter()
            .map(|ti| ti.lookup(&row))
            .collect::<Result<Vec<Binding>>>()
    }

    pub(crate) fn combine(spec: Rc<FindSpec>, column_count: usize, mut elements: ProjectedElements) -> Result<CombinedProjection> {
        let projector = Box::new(TupleTwoProngedCrownProjector::with_templates(spec, column_count, elements.take_templates(), elements.take_pulls()));
        let distinct = false;
        elements.combine(projector, distinct)
    }
}

impl Projector for TupleTwoProngedCrownProjector {
    fn project<'stmt, 's>(&self, schema: &Schema, berolinasql: &'s berolinasql::Connection, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        let results =
            if let Some(r) = rows.next() {
                let row = r?;

                // Keeping the compiler happy.
                let pull_consumers: Result<Vec<PullConsumer>> = self.pulls
                                                                    .iter()
                                                                    .map(|op| PullConsumer::for_template(schema, op))
                                                                    .collect();
                let mut pull_consumers = pull_consumers?;

                // Collect the usual bindings and accumulate entity IDs for pull.
                for mut p in pull_consumers.iter_mut() {
                    p.collect_entity(&row);
                }

                let mut bindings = self.collect_bindings(row)?;

                // Run the pull expressions for the collected IDs.
                for mut p in pull_consumers.iter_mut() {
                    p.pull(berolinasql)?;
                }

                // Expand the pull expressions back into the results vector.
                for p in pull_consumers.into_iter() {
                    p.expand(&mut bindings);
                }

                QueryResults::Tuple(Some(bindings))
            } else {
                QueryResults::Tuple(None)
            };
        Ok(QueryOutput {
            spec: self.spec.clone(),
            results: results,
        })
    }

    fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.columns()
    }
}

/// A rel projector produces a RelResult, which is a striding abstraction over a vector.
/// Each stride across the vector is the same size, and sourced from the same columns.
/// Each column in each stride is the result of taking one or two columns from
/// the `Row`: one for the value and optionally one for the type tag.
pub(crate) struct RelTwoProngedCrownProjector {
    spec: Rc<FindSpec>,
    len: usize,
    templates: Vec<TypedIndex>,
    pulls: Vec<PullTemplate>,
}

impl RelTwoProngedCrownProjector {
    fn with_templates(spec: Rc<FindSpec>, len: usize, templates: Vec<TypedIndex>, pulls: Vec<PullTemplate>) -> RelTwoProngedCrownProjector {
        RelTwoProngedCrownProjector {
            spec: spec,
            len: len,
            templates: templates,
            pulls: pulls,
        }
    }

    fn collect_bindings_into<'a, 'stmt, 'out>(&self, row: Row<'a, 'stmt>, out: &mut Vec<Binding>) -> Result<()> {
        // There will be at least as many SQL columns as Datalog columns.
        // gte 'cos we might be querying extra columns for ordering.
        // The templates will take care of ignoring columns.
        assert!(row.column_count() >= self.len as i32);
        let mut count = 0;
        for binding in self.templates
                           .iter()
                           .map(|ti| ti.lookup(&row)) {
            out.push(binding?);
            count += 1;
        }
        assert_eq!(self.len, count);
        Ok(())
    }

    pub(crate) fn combine(spec: Rc<FindSpec>, column_count: usize, mut elements: ProjectedElements) -> Result<CombinedProjection> {
        let projector = Box::new(RelTwoProngedCrownProjector::with_templates(spec, column_count, elements.take_templates(), elements.take_pulls()));

        // If every column yields only one value, or if this is an aggregate query
        // (because by definition every column in an aggregate query is either
        // aggregated or is a variable _upon which we group_), then don't bother
        // with DISTINCT.
        let already_distinct = elements.pre_aggregate_projection.is_some() ||
                               projector.columns().all(|e| e.is_unit());

        elements.combine(projector, !already_distinct)
    }
}

impl Projector for RelTwoProngedCrownProjector {
    fn project<'stmt, 's>(&self, schema: &Schema, berolinasql: &'s berolinasql::Connection, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        // Allocate space for five rows to start.
        // This is better than starting off by doubling the buffer a couple of times, and will
        // rapidly grow to support larger query results.
        let width = self.len;
        let mut values: Vec<_> = Vec::with_capacity(5 * width);

        let pull_consumers: Result<Vec<PullConsumer>> = self.pulls
                                                            .iter()
                                                            .map(|op| PullConsumer::for_template(schema, op))
                                                            .collect();
        let mut pull_consumers = pull_consumers?;

        // Collect the usual bindings and accumulate entity IDs for pull.
        while let Some(r) = rows.next() {
            let row = r?;
            for mut p in pull_consumers.iter_mut() {
                p.collect_entity(&row);
            }
            self.collect_bindings_into(row, &mut values)?;
        }

        // Run the pull expressions for the collected IDs.
        for mut p in pull_consumers.iter_mut() {
            p.pull(berolinasql)?;
        }

        // Expand the pull expressions back into the results vector.
        for bindings in values.chunks_mut(width) {
            for p in pull_consumers.iter() {
                p.expand(bindings);
            }
        };

        Ok(QueryOutput {
            spec: self.spec.clone(),
            results: QueryResults::Rel(RelResult { width, values }),
        })
    }

    fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.columns()
    }
}

/// A coll projector produces a vector of values.
/// Each value is sourced from the same column.
pub(crate) struct CollTwoProngedCrownProjector {
    spec: Rc<FindSpec>,
    pull: PullOperation,
}

impl CollTwoProngedCrownProjector {
    fn with_pull(spec: Rc<FindSpec>, pull: PullOperation) -> CollTwoProngedCrownProjector {
        CollTwoProngedCrownProjector {
            spec: spec,
            pull: pull,
        }
    }

    pub(crate) fn combine(spec: Rc<FindSpec>, mut elements: ProjectedElements) -> Result<CombinedProjection> {
        let pull = elements.pulls.pop().expect("Expected a single pull");
        let projector = Box::new(CollTwoProngedCrownProjector::with_pull(spec, pull.op));

        // If every column yields only one value, or we're grouping by the value,
        // don't bother with DISTINCT. This shouldn't really apply to coll-pull.
        let already_distinct = elements.pre_aggregate_projection.is_some() ||
                               projector.columns().all(|e| e.is_unit());
        elements.combine(projector, !already_distinct)
    }
}

impl Projector for CollTwoProngedCrownProjector {
    fn project<'stmt, 's>(&self, schema: &Schema, berolinasql: &'s berolinasql::Connection, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        let mut pull_consumer = PullConsumer::for_operation(schema, &self.pull)?;

        while let Some(r) = rows.next() {
            let row = r?;
            pull_consumer.collect_entity(&row);
        }

        // Run the pull expressions for the collected IDs.
        pull_consumer.pull(berolinasql)?;

        // Expand the pull expressions into a results vector.
        let out = pull_consumer.into_coll_results();

        Ok(QueryOutput {
            spec: self.spec.clone(),
            results: QueryResults::Coll(out),
        })
    }

    fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.columns()
    }
}

