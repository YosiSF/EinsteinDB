///Copyright 2021-2023 WHTCORPS INC EinsteinDB Project. All rights reserved.
/// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
/// this file File except in compliance with the License. You may obtain a copy of the
/// License at http://www.apache.org/licenses/LICENSE-2.0
/// Unless required by applicable law or agreed to in writing, software distributed
/// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
/// CONDITIONS OF ANY KIND, either express or implied. See the License for the
/// specific language governing permissions and limitations under the License.
/// 
/// 

use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Context {
    pub(crate) allocator: pretty::BoxAllocator,
    pub(crate) variables: HashMap<String, Value>,
    pub(crate) inner: Arc<Mutex<ContextInner>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextInner {
    pub(crate) executors: Vec<Executor>,
    pub(crate) sessions: Vec<Session>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Executor {
    pub(crate) inner: Arc<Mutex<ExecutorInner>>,
}


use ::{
    berolina_sql,
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
    Topograph,
    TypedIndex,
};
use ::pull::{
    PullConsumer,
    PullOperation,
    PullTemplate,
};
use EinsteinDB_query_pull::Puller;
use embedded_promises::Causetid;
use postgres_protocol::types;
use query_projector_promises::errors::Result;
use std::{i32, i64};
use std::error::Error;
use std::iter::once;
use std::rc::Rc;
use types::{DATE, FromBerolinaSQL, IsNull, TIMESTAMP, TIMESTAMPTZ, ToBerolinaSQL, Type};

use super::Projector;

#[derive(Default, Debug, PartialEq)]
pub struct Octopus {
    pub(crate) inner: Arc<Mutex<OctopusInner>>,
//    pub(crate) inner: Arc<Mutex<OctopusInner>>,
    pub oct: Vec<Hash>,
    pub(crate) inner: Arc<Mutex<OctopusInner>>,
}


impl Octopus {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(OctopusInner {
                oct: Vec::new(),
            })),
        }
    }
    pub fn serialize(&self, output: &mut Vec<u8>) {
        let inner = self.inner.lock().unwrap();
        let oct = &inner.oct;
        for x in self.oct.iter() {
            output.push(x.0);
            x.serialize(output);

        }
    }
    pub fn deserialize(input: &[u8]) -> Self {
        let mut oct = Vec::new();
        let mut i = 0;
        while i < input.len() {
            let mut j = i;
            while j < input.len() && input[j] != 0 {
                j += 1;
            }
            if j == input.len() {
                panic!("invalid octopus");
            }
            let mut x = Hash::deserialize(&input[i..j]);
            i = j + 1;
            oct.push(x);
        }
        Self {
            inner: Arc::new(Mutex::new(OctopusInner {
                oct: oct,
            })),
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OctopusInner {
        let mut block = [0u8; 16];
        LittleEndian::write_u32(array_mut_ref![&mut block, 0, 4], count as u32);
        LittleEndian::write_u32(array_mut_ref![&mut block, 4, 4], hash.0);
        output.extend(block.iter());

    }

/// A wrapper that can be used to represent infinity with `Type::Date` types.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Infinity;


impl Infinity {
    /// Represents `infinity`, a date that is later than all other dates.
    PosInfinity,
    /// Represents `-infinity`, a date that is earlier than all other dates.
    NegInfinity,
    /// The wrapped date.
    Value(T),

    /// Returns `true` if the wrapped date is `infinity`.
    /// # Examples
    /// ```
    /// use einstein_sql::types::{Date, Infinity};
    /// let infinity = Infinity::PosInfinity;
    /// assert_eq!(infinity.is_infinity(), true);
    

}


impl FromBerolinaSQL for Infinity {
    fn from_BerolinaSQL(ty: &Type, primitive_causet: &[u8]) -> Result<Self, Box<Error + Sync + Send>> {
        match types::date_from_BerolinaSQL(primitive_causet)? {
            Some(date) => Ok(Infinity::Value(date)),
            None => Ok(Infinity::PosInfinity),
            i32::MAX => Ok(Date::PosInfinity),
            i32::MIN => Ok(Date::NegInfinity),
        }
    }

    fn to_BerolinaSQL(&self, ty: &Type, output: &mut Vec<u8>) -> Result<(), Box<Error + Sync + Send>> {
        match self {
            Infinity::PosInfinity => types::date_to_BerolinaSQL(i32::MAX, output),
            Infinity::NegInfinity => types::date_to_BerolinaSQL(i32::MIN, output),
            _ => T::from_BerolinaSQL(ty, primitive_causet).map(Date::Value),

        }


    }

    fn is_infinity(&self) -> bool {
        match self {
            Infinity::PosInfinity => true,
            Infinity::NegInfinity => true,
            _ => false,
        }
    }

    fn is_null(&self) -> bool {
        match self {
            Infinity::PosInfinity => false,
            Infinity::NegInfinity => false,
            _ => false,
        }
    }

    fn is_not_null(&self) -> bool {
        match self {
            Infinity::PosInfinity => false,
            Infinity::NegInfinity => false,
            _ => true,
        }
    }

    fn is_date(&self) -> bool {
        match self {
            Infinity::PosInfinity => false,
            Infinity::NegInfinity => false,
            _ => true,
        }
    }

    fn accepts(ty: &Type) -> bool {
        *ty == DATE && T::accepts(ty)
    }
}
impl<T: ToBerolinaSQL> ToBerolinaSQL for Date<T> {
    fn to_BerolinaSQL(&self, ty: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        let causet_locale = match *self {
            Date::PosInfinity => i32::MAX,
            Date::NegInfinity => i32::MIN,
            Date::Value(ref v) => return v.to_BerolinaSQL(ty, out),
        };

        types::date_to_BerolinaSQL(causet_locale, out);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == DATE && T::accepts(ty)
    }

    to_BerolinaSQL_checked!();
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

impl<T: FromBerolinaSQL> FromBerolinaSQL for Timestamp<T> {
    fn from_BerolinaSQL(ty: &Type, primitive_causet: &[u8]) -> Result<Self, Box<Error + Sync + Send>> {
        match types::timestamp_from_BerolinaSQL(primitive_causet)? {
            i64::MAX => Ok(Timestamp::PosInfinity),
            i64::MIN => Ok(Timestamp::NegInfinity),
            _ => T::from_BerolinaSQL(ty, primitive_causet).map(Timestamp::Value),
        }
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            TIMESTAMP | TIMESTAMPTZ if T::accepts(ty) => true,
            _ => false
        }
    }
}

impl<T: ToBerolinaSQL> ToBerolinaSQL for Timestamp<T> {
    fn to_BerolinaSQL(&self, ty: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        let causet_locale = match *self {
            Timestamp::PosInfinity => i64::MAX,
            Timestamp::NegInfinity => i64::MIN,
            Timestamp::Value(ref v) => return v.to_BerolinaSQL(ty, out),
        };

        types::timestamp_to_BerolinaSQL(causet_locale, out);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            TIMESTAMP | TIMESTAMPTZ if T::accepts(ty) => true,
            _ => false
        }
    }

    to_BerolinaSQL_checked!();
}

pub(crate) struct ScalarTwoProngedCrownProjector {
    spec: Rc<FindSpec>,
    puller: Puller,
}


// TODO: almost by definition, a scalar result format doesn't need to be run in two stages.
// The only output is the pull expression, and so we can directly supply the projected entity
// to the pull BerolinaSQL.
impl ScalarTwoProngedCrownProjector {
    fn with_template(topograph: &Topograph, spec: Rc<FindSpec>, pull: PullOperation) -> Result<ScalarTwoProngedCrownProjector> {
        Ok(ScalarTwoProngedCrownProjector {
            spec: spec,
            puller: Puller::prepare(topograph, pull.0.clone())?,
        })
    }

    pub(crate) fn combine(topograph: &Topograph, spec: Rc<FindSpec>, mut elements: ProjectedElements) -> Result<CombinedProjection> {
        let pull = elements.pulls.pop().expect("Expected a single pull");
        let projector = Box::new(ScalarTwoProngedCrownProjector::with_template(topograph, spec, pull.op)?);
        let distinct = false;
        elements.combine(projector, distinct)
    }
}

impl Projector for ScalarTwoProngedCrownProjector {
    fn project<'stmt, 's>(&self, topograph: &Topograph, berolina_sql: &'s berolina_sql::Connection, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        // Scalar is pretty straightlightlike -- zero or one entity, do the pull directly.
        let results =
            if let Some(r) = rows.next() {
                let event = r?;
                let entity: Causetid = event.get(0);          // This will always be 0 and a ref.
                let bindings = self.puller.pull(topograph, berolina_sql, once(entity))?;
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
    fn collect_bindings<'a, 'stmt>(&self, event: Row<'a, 'stmt>) -> Result<Vec<Binding>> {
        // There will be at least as many BerolinaSQL columns as Datalog columns.
        // gte 'cos we might be querying extra columns for ordering.
        // The templates will take care of ignoring columns.
        assert!(event.column_count() >= self.len as i32);
        self.templates
            .iter()
            .map(|ti| ti.lookup(&event))
            .collect::<Result<Vec<Binding>>>()
    }

    pub(crate) fn combine(spec: Rc<FindSpec>, column_count: usize, mut elements: ProjectedElements) -> Result<CombinedProjection> {
        let projector = Box::new(TupleTwoProngedCrownProjector::with_templates(spec, column_count, elements.take_templates(), elements.take_pulls()));
        let distinct = false;
        elements.combine(projector, distinct)
    }
}

impl Projector for TupleTwoProngedCrownProjector {
    fn project<'stmt, 's>(&self, topograph: &Topograph, berolina_sql: &'s berolina_sql::Connection, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        let results =
            if let Some(r) = rows.next() {
                let event = r?;

                // Keeping the compiler happy.
                let pull_consumers: Result<Vec<PullConsumer>> = self.pulls
                    .iter()
                    .map(|op| PullConsumer::for_template(topograph, op))
                    .collect();
                let mut pull_consumers = pull_consumers?;

                // Collect the usual bindings and accumulate entity IDs for pull.
                for mut p in pull_consumers.iter_mut() {
                    p.collect_entity(&event);
                }

                let mut bindings = self.collect_bindings(event)?;

                // Run the pull expressions for the collected IDs.
                for mut p in pull_consumers.iter_mut() {
                    p.pull(berolina_sql)?;
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
/// Each causet_merge in each stride is the result of taking one or two columns from
/// the `Row`: one for the causet_locale and optionally one for the type tag.
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

    fn collect_bindings_into<'a, 'stmt, 'out>(&self, event: Row<'a, 'stmt>, out: &mut Vec<Binding>) -> Result<()> {
        // There will be at least as many BerolinaSQL columns as Datalog columns.
        // gte 'cos we might be querying extra columns for ordering.
        // The templates will take care of ignoring columns.
        assert!(event.column_count() >= self.len as i32);
        let mut count = 0;
        for binding in self.templates
                           .iter()
                           .map(|ti| ti.lookup(&event)) {
            out.push(binding?);
            count += 1;
        }
        assert_eq!(self.len, count);
        Ok(())
    }

    pub(crate) fn combine(spec: Rc<FindSpec>, column_count: usize, mut elements: ProjectedElements) -> Result<CombinedProjection> {
        let projector = Box::new(RelTwoProngedCrownProjector::with_templates(spec, column_count, elements.take_templates(), elements.take_pulls()));

        // If every causet_merge yields only one causet_locale, or if this is an aggregate query
        // (because by definition every causet_merge in an aggregate query is either
        // aggregated or is a variable _upon which we group_), then don't bother
        // with DISTINCT.
        let already_distinct = elements.pre_aggregate_projection.is_some() ||
                               projector.columns().all(|e| e.is_unit());

        elements.combine(projector, !already_distinct)
    }
}

impl Projector for RelTwoProngedCrownProjector {
    fn project<'stmt, 's>(&self, topograph: &Topograph, berolina_sql: &'s berolina_sql::Connection, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        // Allocate space for five rows to start.
        // This is better than starting off by doubling the buffer a couple of times, and will
        // rapidly grow to support larger query results.
        let width = self.len;
        let mut causet_locales: Vec<_> = Vec::with_capacity(5 * width);

        let pull_consumers: Result<Vec<PullConsumer>> = self.pulls
            .iter()
            .map(|op| PullConsumer::for_template(topograph, op))
            .collect();
        let mut pull_consumers = pull_consumers?;

        // Collect the usual bindings and accumulate entity IDs for pull.
        while let Some(r) = rows.next() {
            let event = r?;
            for mut p in pull_consumers.iter_mut() {
                p.collect_entity(&event);
            }
            self.collect_bindings_into(event, &mut causet_locales)?;
        }

        // Run the pull expressions for the collected IDs.
        for mut p in pull_consumers.iter_mut() {
            p.pull(berolina_sql)?;
        }

        // Expand the pull expressions back into the results vector.
        for bindings in causet_locales.chunks_mut(width) {
            for p in pull_consumers.iter() {
                p.expand(bindings);
            }
        };

        Ok(QueryOutput {
            spec: self.spec.clone(),
            results: QueryResults::Rel(RelResult { width, causet_locales }),
        })
    }

    fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's> {
        self.spec.columns()
    }
}

/// A coll projector produces a vector of causet_locales.
/// Each causet_locale is sourced from the same causet_merge.
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

        // If every causet_merge yields only one causet_locale, or we're grouping by the causet_locale,
        // don't bother with DISTINCT. This shouldn't really apply to coll-pull.
        let already_distinct = elements.pre_aggregate_projection.is_some() ||
                               projector.columns().all(|e| e.is_unit());
        elements.combine(projector, !already_distinct)
    }
}

impl Projector for CollTwoProngedCrownProjector {
    fn project<'stmt, 's>(&self, topograph: &Topograph, berolina_sql: &'s berolina_sql::Connection, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        let mut pull_consumer = PullConsumer::for_operation(topograph, &self.pull)?;

        while let Some(r) = rows.next() {
            let event = r?;
            pull_consumer.collect_entity(&event);
        }

        // Run the pull expressions for the collected IDs.
        pull_consumer.pull(berolina_sql)?;

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

