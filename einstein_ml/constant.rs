use ::{
    berolina_sql,
    Element,
    FindSpec,
    QueryOutput,
    QueryResults,
    Rows,
    Topograph,
};
use allegroeinstein_prolog_causet_projector::errors::Result;
use causet_algebrizer::MEDB_query_datatype::codec::DatumType;
use causet_algebrizer::MEDB_query_datatype::codec::myBerolinaSQL::{Decimal, Duration, Json, Time};
use causet_algebrizer::MEDB_query_datatype::expr::Result;
use std::borrow::Cow;
use std::rc::Rc;

use crate::Constant;

use super::Projector;

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
        let topograph = Topograph::new(spec.clone());
        let query_output = QueryOutput::new(spec, topograph, results);
        Ok(query_output)
    }
}

impl Projector for ConstantProjector {
    fn project(&self, _: &berolina_sql::Statement) -> Result<QueryOutput> {
        self.project_without_rows()
    }
}


/// A projector that produces a `QueryResult` containing fixed data.
/// Takes a boxed function that should return an empty result set of the desired type.
/// This version is for use with the `berolina_sql` crate.
/// It is used to create a constant result set for a query that has no rows.
///
/// # Example
/// ```
/// use causet_algebrizer::MEDB_query_datatype::codec::myBerolinaSQL::{Decimal, Duration, Json, Time};
/// use causet_algebrizer::MEDB_query_datatype::codec::DatumType;
/// use causet_algebrizer::MEDB_query_datatype::expr::Result;
/// use causet_algebrizer::MEDB_query_datatype::query_output::{
///    QueryOutput,
///   Projector,
/// };
/// use causet_algebrizer::MEDB_query_datatype::query_results::{
///   QueryResults,
///  Rows,
/// };
/// use causet_algebrizer::MEDB_query_datatype::topograph::{
///  Topograph,
/// };
///
/// use std::rc::Rc;
///
/// use ::{
///   Element,
///  FindSpec,
/// };
///
///
/// fn main() {
///    let spec = Rc::new(FindSpec::new(
///
//
///
///
///


impl Projector for ConstantProjector {
    fn project(&self, _: &berolina_sql::Statement) -> Result<QueryOutput> {
        self.project_without_rows()
    }
}


impl ConstantProjector {
    pub fn new_with_json_clone(spec: Rc<FindSpec>, json: Json) -> ConstantProjector {
        let results_factory = Box::new(move || {
            let mut results = QueryResults::new(
                Rows::new(vec![]),
                vec![],
            );
            results.add_row(vec![DatumType::Json(json)]);
            results
        });
        ConstantProjector::new(spec, results_factory)
    }
    pub fn new_with_time(spec: Rc<FindSpec>, time: Time) -> ConstantProjector {
        let results_factory = Box::new(move || {
            let mut results = QueryResults::new(
                Rows::new(vec![]),
                vec![],
            );
            results.add_row(vec![DatumType::Time(time)]);
            results
        });
        ConstantProjector::new(spec, results_factory)
    }
    pub fn new_with_duration(spec: Rc<FindSpec>, duration: Duration) -> ConstantProjector {
        let results_factory = Box::new(move || {
            let mut results = QueryResults::new(
                Rows::new(vec![]),
                vec![],
            );
            results.add_row(vec![DatumType::Duration(duration)]);
            results
        });
        ConstantProjector::new(spec, results_factory)
    }
    pub fn new_with_decimal(spec: Rc<FindSpec>, decimal: Decimal) -> ConstantProjector {
        let results_factory = Box::new(move || {
            let mut results = QueryResults::new(
                Rows::new(vec![]),
                vec![],
            );
            results.add_row(vec![DatumType::Decimal(decimal)]);
            results
        });
        ConstantProjector::new(spec, results_factory)
    }
    pub fn new_with_result(spec: Rc<FindSpec>, result: Result) -> ConstantProjector {
        let results_factory = Box::new(move || {
            let mut results = QueryResults::new(
                Rows::new(vec![]),
                vec![],
            );
            results.add_row(vec![DatumType::Result(result)]);
            results
        });
        ConstantProjector::new(spec, results_factory)
    }
    pub fn new_with_bool(spec: Rc<FindSpec>, bool: bool) -> ConstantProjector {
        let results_factory = Box::new(move || {
            let mut results = QueryResults::new(
                Rows::new(vec![]),
                vec![],
            );
            results.add_row(vec![DatumType::Bool(bool)]);
            results
        });
        ConstantProjector::new(spec, results_factory)
    }
    pub fn new_with_int(spec: Rc<FindSpec>, int: i64) -> ConstantProjector {
        let results_factory = Box::new(move || {
            let mut results = QueryResults::new(
                Rows::new(vec![]),
                vec![],
            );
            results.add_row(vec![DatumType::Int(int)]);
            results
        });
        ConstantProjector::new(spec, results_factory)
    }
    pub fn new_with_float(spec: Rc<FindSpec>, float: f64) -> ConstantProjector {
        let results_factory = Box::new(move || {
            let mut results = QueryResults::new(
                Rows::new(vec![]),
                vec![],
            );
            results.add_row(vec![DatumType::Float(float)]);
            results
        });
        ConstantProjector::new(spec, results_factory)
    }
    pub fn new_with_string(spec: Rc<FindSpec>, string: String) -> ConstantProjector {
        let results_factory = Box::new(move || {
            let mut results = QueryResults::new(
                Rows::new(vec![]),
                vec![],
            );
            results.add_row(vec![DatumType::String(string)]);
            results
        });
        ConstantProjector::new(spec, results_factory)
    }
    pub fn new_with_bytes(spec: Rc<FindSpec>, bytes: Bytes) -> ConstantProjector {
        let results_factory = Box::new(move || {
            let mut results = QueryResults::new(
                Rows::new(vec![]),
                vec![],
            );
            results.add_row(vec![DatumType::Bytes(bytes)]);
            results
        });
        ConstantProjector::new(spec, results_factory)
    }
    pub fn new_with_json(spec: Rc<FindSpec>, json: Json) -> ConstantProjector {
        let results_factory = Box::new(move || {
            let mut results = QueryResults::new(
                Rows::new(vec![]),
                vec![],
            );
            results.add_row(vec![DatumType::Json(json)]);
            results
        });
        ConstantProjector::new(spec, results_factory)
    }
}


pub fn new_with_duration(spec: Rc<FindSpec>, duration: Duration) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::Duration(duration)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_date(spec: Rc<FindSpec>, date: Date) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::Date(date)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_time(spec: Rc<FindSpec>, time: Time) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::Time(time)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_timestamp(spec: Rc<FindSpec>, timestamp: Timestamp) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::Timestamp(timestamp)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_date_time(spec: Rc<FindSpec>, date_time: DateTime) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::DateTime(date_time)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_date_time_tz(spec: Rc<FindSpec>, date_time_tz: DateTimeTz) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::DateTimeTz(date_time_tz)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_interval(spec: Rc<FindSpec>, interval: Interval) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::Interval(interval)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_uuid(spec: Rc<FindSpec>, uuid: Uuid) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::Uuid(uuid)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_json(spec: Rc<FindSpec>, json: Json) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::Json(json)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_json_b(spec: Rc<FindSpec>, json_b: JsonB) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::JsonB(json_b)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_oid(spec: Rc<FindSpec>, oid: Oid) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::Oid(oid)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_reg_type(spec: Rc<FindSpec>, reg_type: RegType) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::RegType(reg_type)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_reg_procedure(spec: Rc<FindSpec>, reg_procedure: RegProcedure) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::RegProcedure(reg_procedure)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_reg_per(spec: Rc<FindSpec>, reg_oper: RegOper) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::RegOper(reg_oper)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}

pub fn new_with_reg_class(spec: Rc<FindSpec>, reg_class: RegClass) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::RegClass(reg_class)]);
        results
    });
    ConstantProjector::new(spec, results_factory)
}





/// A projector that produces a `QueryResult` containing fixed data.
///
