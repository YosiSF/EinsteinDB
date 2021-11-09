use std::borrow::Cow;

use crate::Constant;
use causet_algebrizer::MilevaDB_query_datatype::codec::mysql::{Decimal, Duration, Json, Time};
use causet_algebrizer::MilevaDB_query_datatype::codec::Datum;
use causet_algebrizer::MilevaDB_query_datatype::expr::Result;


use std::rc::Rc;

use ::{
    Element,
    FindSpec,
    QueryOutput,
    QueryResults,
    Rows,
    Schema,
    berolinasql,
};

use allegroeinstein_prolog_causet_projector::errors::{
    Result,
};

use super::{
    Projector,
};

/// A projector that produces a `QueryResult` containing fixed data.
/// Takes a boxed function that should return an empty result set of the desired type.
pub struct ConstantProjector {
    spec: Rc<FindSpec>,
    results_factory: Box<Fn() -> QueryResults>,
}

impl ConstantProjector {
    pub fn new(spec: Rc<FindSpec>, results_factory: Box<Fn() -> QueryResults>) -> ConstantProjector {
        ConstantProjector {
            spec: spec,
            results_factory: results_factory,
        }
    }

    pub fn project_without_rows<'stmt>(&self) -> Result<QueryOutput> {
        let results = (self.results_factory)();
        let spec = self.spec.clone();
        Ok(QueryOutput {
            spec: spec,
            results: results,
        })
    }
}
