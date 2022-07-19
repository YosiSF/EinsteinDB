///Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
/// AUTHORS: WHITFORD LEDER
/// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
/// this file File except in compliance with the License. You may obtain a copy of the
/// License at http://www.apache.org/licenses/LICENSE-2.0
/// Unless required by applicable law or agreed to in writing, software distributed
/// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
/// CONDITIONS OF ANY KIND, either express or implied. See the License for the
/// specific language governing permissions and limitations under the License.
/// 
/// use failure::Fail;
/// 


// use std::collections::HashMap;
// use std::sync::Arc;
// use std::sync::atomic::{AtomicUsize, Ordering};




use std::collections::HashMap;
use std::borrow::{BorrowMut, Cow};
use std::rc::Rc;


use std::fmt::{self, Display, Formatter};
use std::io;
use std::result;
use std::mem;
use std::ptr::copy_nonoverlapping;
use std::slice;
use std::str;
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{fmt::Debug, io::Write, mem::size_of};
use std::{result::Result as StdResult};
use std::io::Bytes;


pub use einsteindb_traits::errors::{
    einsteindbErrorKind,
    Result,
};


pub use topograph::{
    AttributeBuilder,
    AttributeValidation,
};


pub use tx::{
    transact,
    transact_terms,
};


pub use tx_observer::{
    InProgressObserverTransactWatcher,
    TxObservationService,
    TxObserver,
};


pub use types::{
    AttributeSet,
    einsteindb,
    Partition,
    PartitionMap,
};


pub use einsteindb::{
    new_connection,
    TypedBerolinaSQLValue,
};


pub use watcher::TransactWatcher;


pub use mvrsi::{
    MVRSI,
    MVRSI_SCHEMA_VERSION,
};


pub use db_::{
    DB,
    DB_SCHEMA_VERSION,
};


pub use cache::{
    Cache,
    Cache_SCHEMA_VERSION,
};


pub use bootstrap::{
    CORE_SCHEMA_VERSION,
};


pub use causetids::{
    Causetids,
    Causetids_SCHEMA_VERSION,
};


pub use einsteindb_traits::{
    einsteindb_SCHEMA_CORE,
};
use crate::query::FindSpec;
use crate::two_pronged_crown::Timestamp;


#[allow(non_camel_case_types)]
#[repr(simd)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct u8x16(u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8, u8);


#[allow(non_camel_case_types)]
#[repr(simd)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct u16x8(u16, u16, u16, u16, u16, u16, u16, u16);


#[allow(non_camel_case_types)]
#[repr(simd)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct u32x4(u32, u32, u32, u32);


#[allow(non_camel_case_types)]
#[repr(simd)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct u64x2(u64, u64);


#[allow(non_camel_case_types)]
#[repr(simd)]
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct u128x1(u128);


pub(crate) struct U64x2(pub u64, pub u64);

pub struct U64x4(pub u64, pub u64, pub u64, pub u64);


pub(crate) struct U128x1(pub u128);





impl U64x2 {


    pub fn new(a: u64, b: u64) -> Self {
        U64x2(a, b)
    }

    pub fn as_u64s(&self) -> [u64; 2] {
        [self.0, self.1]
    }

    /// Reads U64x2 from array pointer (potentially unaligned)
    #[inline(always)]
    pub unsafe fn read(src: &[u8; 16]) -> Self {
        let src = src.as_ptr();
        let a = *(src as *const u64);
        let b = *((src as *const u64).add(1));
        U64x2(a, b)
    }




    /// Writes U64x2 to array pointer (potentially unaligned)
    /// # Safety
    /// The pointer must be aligned to 16 bytes.
    /// # Examples
    /// ```
    /// use einsteindb_gremlin::constant::U64x2;
    /// let mut dst = [0u8; 16];
    /// let src = U64x2::new(0x12345678, 0x9abcdef0);
    /// unsafe {
    ///    einsteindb_gremlin::constant::U64x2::write(&mut dst, src);
    /// }
    /// assert_eq!(dst, [0x78, 0x56, 0x34, 0x12, 0xf0, 0xde, 0xbc, 0x9a, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00]);
    /// ```
    /// ```
    /// use einsteindb_gremlin::constant::U64x2;
    /// let mut dst = [0u8; 16];

    /// Write U64x2 content into array pointer (potentially unaligned)
    #[inline(always)]
    pub fn write(self, dst: &mut [u8; 16]) {
        unsafe {
            copy_nonoverlapping(&self as *const Self as *const u8, dst.as_mut_ptr(), 16);
        }
    }



}



#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Causet(String),
    #[fail(display = "{}", _0)]
    CausetQ(String),
    #[fail(display = "{}", _0)]
    EinsteinML(String),
    #[fail(display = "{}", _0)]
    Gremlin(String),
    #[fail(display = "{}", _0)]
    GremlinQ(String),
    #[fail(display = "{}", _0)]
    GremlinQ2(String),
    #[fail(display = "{}", _0)]
    GremlinQ3(String),
    #[fail(display = "{}", _0)]
    GremlinQ4(String),
    #[fail(display = "{}", _0)]
    GremlinQ5(String),
    #[fail(display = "{}", _0)]
    GremlinQ6(String),
    #[fail(display = "{}", _0)]
    GremlinQ7(String),
    #[fail(display = "{}", _0)]
    GremlinQ8(String),
    #[fail(display = "{}", _0)]
    GremlinQ9(String),
    #[fail(display = "{}", _0)]
    GremlinQ10(String),
    #[fail(display = "{}", _0)]
    GremlinQ11(String),
}



/// A projector that produces a `QueryResult` containing fixed data.
/// Takes a boxed function that should return an empty result set of the desired type.
pub struct FixedProjector {
    pub(crate) func: Box<dyn Fn() -> QueryResults + Send + Sync>,
}


impl FixedProjector {
    pub fn new<F>(func: F) -> Self
    where
        F: Fn() -> QueryResults + Send + Sync + 'static,
    {
        FixedProjector {
            func: Box::new(func),
        }
    }
}


impl QueryOutput for FixedProjector {
    fn get_results(&self) -> QueryResults {
        (self.func)()
    }
}


impl FixedProjector {
    pub fn new(spec: Rc<FindSpec>, results_factory: Box<dyn Fn() -> QueryResults>) -> ConstantProjector {
        ConstantProjector {
            spec: spec,
            results_factory: results_factory,
        }
    }

    pub fn project_without_rows<'stmt>(&self) -> Result<QueryOutput, E> {
        let results = (self.results_factory)();
        let spec = self.spec.clone();
        let topograph = Topograph::new(spec.clone());
        let query_output = QueryOutput::new(spec, topograph, results);
        Ok(query_output)
    }
}

impl Projector for ConstantProjector {
    fn project(&self, _: &berolina_sql::Statement) -> Result<QueryOutput, E> {
        self.project_without_rows()
    }
}



impl Projector for ConstantProjector {
    fn project(&self, _: &berolina_sql::Statement) -> Result<QueryOutput, E> {
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
    pub fn new_with_result(spec: Rc<FindSpec>, result: Result<T, E>) -> ConstantProjector {
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
    pub fn new_with_bytes(spec: Rc<FindSpec>, bytes: Bytes<R>) -> ConstantProjector {
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


impl Projector for TopographProjector {
    fn project(&self, _: &berolina_sql::Statement) -> Result<QueryOutput, E> {
        self.project_without_rows()
    }
}


impl TopographProjector {
    pub fn new_with_topograph(spec: Rc<FindSpec>, topograph: Topograph) -> TopographProjector {
        let results_factory = Box::new(move || {
            let mut results = QueryResults::new(
                Rows::new(vec![]),
                vec![],
            );
            results.add_row(vec![DatumType::Topograph(topograph)]);
            results
        });
        TopographProjector::new(spec, results_factory)
    }
}


impl Projector for TopographProjector {
    fn project(&self, _: &berolina_sql::Statement) -> Result<QueryOutput, E> {
        self.project_without_rows()
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

pub fn new_with_timestamp(spec: Rc<FindSpec>, timestamp: Timestamp<T>) -> ConstantProjector {
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






pub fn new_with_json_object(spec: Rc<FindSpec>, json_object: JsonObject) -> ConstantProjector {
    let results_factory = Box::new(move || {
        let mut results = QueryResults::new(
            Rows::new(vec![]),
            vec![],
        );
        results.add_row(vec![DatumType::JsonObject(json_object)]);
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


#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        datum::{DatumType, Datum},
        find_spec::FindSpec,
        projector::{Projector, ProjectorError},
        query_results::QueryResults,
        rows::Rows,
    };
    use std::{
        collections::HashMap,
        time::{Duration, SystemTime},
    };
    use uuid::Uuid;
    use serde_json::{json, Value};
    use chrono::{Date, DateTime, DateTimeTz, TimeZone, Utc};
    use postgres_types::{
        oid::Oid,
        json::{Json, Jsonb},
        Interval,
    };
    use crate::{
        datum::{Datum, DatumType},
        find_spec::FindSpec,
        projector::{Projector, ProjectorError},
        query_results::QueryResults,
        rows::Rows,
    };
    use std::{
        collections::HashMap,
        time::{Duration, SystemTime},
    };
    use uuid::Uuid;
    use serde_json::{json, Value};
    use chrono::{Date, DateTime, DateTimeTz, TimeZone, Utc};
    use postgres_types::{
        oid::Oid,
        json::{Json, Jsonb},
        Interval,
    };
    use crate::{
        datum::{Datum, DatumType},
        find_spec::FindSpec,
        projector::{Projector, ProjectorError},
        query_results::QueryResults,
        rows::Rows,
    };
    use std::{
        collections::HashMap,
        time::{Duration, SystemTime},
    };
    use uuid::Uuid;
    use serde_json::{json, Value};
    use chrono::{Date, DateTime, DateTimeTz, TimeZone, Utc};
    use postgres_types::{
        oid::Oid,
        json::{Json, JsonRef},
        Interval,
    };
    use crate::{
        datum::{Datum, DatumType},
        find_spec::FindSpec,
        projector::{Projector, ProjectorError},
        query_results::QueryResults,
        rows::Rows,
    };
    use std::{
        collections::HashMap,
        time::{Duration, SystemTime},
    };
    use uuid::Uuid;
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

///Haraka: This is a hack to get around the fact that we don't have a way to create a
///       `RegType` from a `DatumType`.
/// 
  //   HARAKA_REG_TYPE: Sized {

    //   fn new(name: &str, oid: Oid, typename: &str,
    //          typmod: Option<i32>,
    //          array_type: Option<Oid>,
    //          regproc: Option<RegProcedure>,
    //          regdummy: Option<RegProcedure>,
    //          regupdate: Option<RegProcedure>,
    //          reginsert: Option<RegProcedure>,
    //          regopt: Option<RegProcedure>,
    //          regdelete: Option<RegProcedure>,
    //          regacl: Option<RegProcedure>,
    //          regdynamic: Option<RegProcedure>,
    //          regtruncate: Option<RegProcedure>,


pub fn new_with_reg_type_haraka(spec: Rc<FindSpec>, reg_type: RegType) -> ConstantProjector {
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



impl HARAKA_REG_TYPE for DatumType {
    fn get_reg_type(&self) -> RegType {
        match self {
            DatumType::Boolean => RegType::Boolean,
            DatumType::Bytea => RegType::Bytea,
            DatumType::Char => RegType::Char,
            DatumType::Name => RegType::Name,
            DatumType::Int8 => RegType::Int8,
            DatumType::Int2 => RegType::Int2,
            DatumType::Int4 => RegType::Int4,
            DatumType::Text => RegType::Text,
            DatumType::Oid => RegType::Oid,
            DatumType::Float4 => RegType::Float4,
            DatumType::Float8 => RegType::Float8,
            DatumType::Timestamp => RegType::Timestamp,
            DatumType::Timestamptz => RegType::Timestamptz,
            DatumType::Date => RegType::Date,
            DatumType::Time => RegType::Time,
            DatumType::TimeTz => RegType::TimeTz,
            DatumType::Interval => RegType::Interval,
            DatumType::Uuid => RegType::Uuid,
            DatumType::Json => RegType::Json,
            DatumType::JsonB => RegType::JsonB,
            DatumType::Oid => RegType::Oid,
            DatumType::RegType => RegType::RegType,
            DatumType::RegProcedure => RegType::RegProcedure,
            DatumType::RegOper => RegType::RegOper,
            DatumType::RegClass => RegType::RegClass,
            DatumType::RegType => RegType::RegType,
            DatumType::RegProcedure => RegType::RegProcedure,
            DatumType::RegOper => RegType::RegOper,
            DatumType::RegClass => RegType::RegClass,
            DatumType::RegType => RegType::RegType,
            DatumType::RegProcedure => RegType::RegProcedure,

            _ => panic!("Unsupported datum type: {:?}", self),
        }
    }
}


impl ConstantProjector {
    pub fn new(spec: Rc<FindSpec>, results_factory: Box<dyn Fn() -> QueryResults>) -> ConstantProjector {
        ConstantProjector {
            spec: spec,
            results_factory: results_factory,
        }
    }
}


impl Projector for ConstantProjector {
    fn get_spec(&self) -> Rc<FindSpec> {
        self.spec.clone()
    }
    fn get_results(&self) -> QueryResults {
        (self.results_factory)() // HARAKA: This is a hack to get around the fact that we don't have a way to create a `RegType` from a `DatumType`.
    }

    fn get_column_names(&self) {
            [ U64x2(0xb2c5fef075817b9d, 0x0684704ce620c00a),
            U64x2(0x640f6ba42f08f717, 0x8b66b4e188f3a06b),
            U64x2(0xcf029d609f029114, 0x3402de2d53f28498),
            U64x2(0xbbf3bcaffd5b4f79, 0x0ed6eae62e7b4f08),
            U64x2(0x79eecd1cbe397044, 0xcbcfb0cb4872448b),
            U64x2(0x8d5335ed2b8a057b, 0x7eeacdee6e9032b7),
            U64x2(0xe2412761da4fef1b, 0x67c28f435e2e7cd0),
            U64x2(0x675ffde21fc70b3b, 0x2924d9b0afcacc07),
            U64x2(0xecdb8fcab9d465ee, 0xab4d63f1e6867fe9),
            U64x2(0x5b2a404fad037e33, 0x1c30bf84d4b7cd64),
            U64x2(0x69028b2e8df69800, 0xb2cc0bb9941723bf),
            U64x2(0x4aaa9ec85c9d2d8a, 0xfa0478a6de6f5572),
            U64x2(0x0efa4f2e29129fd4, 0xdfb49f2b6b772a12),
            U64x2(0x32d611aebb6a12ee, 0x1ea10344f449a236),
            U64x2(0x5f9600c99ca8eca6, 0xaf0449884b050084),
            U64x2(0x78a2c7e327e593ec, 0x21025ed89d199c4f),
            U64x2(0xb9282ecd82d40173, 0xbf3aaaf8a759c9b7),
            U64x2(0x37f2efd910307d6b, 0x6260700d6186b017),
            U64x2(0x81c29153f6fc9ac6, 0x5aca45c221300443),
            U64x2(0x2caf92e836d1943a, 0x9223973c226b68bb),
            U64x2(0x6cbab958e51071b4, 0xd3bf9238225886eb),
            U64x2(0x933dfddd24e1128d, 0xdb863ce5aef0c677),
            U64x2(0x83e48de3cb2212b1, 0xbb606268ffeba09c),
            U64x2(0x2db91a4ec72bf77d, 0x734bd3dce2e4d19c),
            U64x2(0x4b1415c42cb3924e, 0x43bb47c361301b43),
            U64x2(0x03b231dd16eb6899, 0xdba775a8e707eff6),
            U64x2(0x8e5e23027eca472c, 0x6df3614b3c755977),
            U64x2(0x6d1be5b9b88617f9, 0xcda75a17d6de7d77),
            U64x2(0x9d6c069da946ee5d, 0xec6b43f06ba8e9aa),
            U64x2(0xa25311593bf327c1, 0xcb1e6950f957332b),
            U64x2(0xe4ed0353600ed0d9, 0x2cee0c7500da619c),
            U64x2(0x80bbbabc63a4a350, 0xf0b1a5a196e90cab),
            U64x2(0xab0dde30938dca39, 0xae3db1025e962988),
            U64x2(0x8814f3a82e75b442, 0x17bb8f38d554a40b),
            U64x2(0xaeb6b779360a16f6, 0x34bb8a5b5f427fd7),
            U64x2(0x43ce5918ffbaafde, 0x26f65241cbe55438),
            U64x2(0xa2ca9cf7839ec978, 0x4ce99a54b9f3026a),
            U64x2(0x40c06e2822901235, 0xae51a51a1bdff7be),
            U64x2(0xc173bc0f48a659cf, 0xa0c1613cba7ed22b),
            U64x2(0x4ad6bdfde9c59da1, 0x756acc0302288288),
            U64x2(0x367e4778848f2ad2, 0x2ff372380de7d31e),
            U64x2(0xee36b135b73bd58f, 0x08d95c6acf74be8b),
            U64x2(0x66ae1838a3743e4a, 0x5880f434c9d6ee98),
            U64x2(0xd0fdf4c79a9369bd, 0x593023f0aefabd99),
            U64x2(0xa5cc637b6f1ecb2a, 0x329ae3d1eb606e6f),
            U64x2(0xa4dc93d6cb7594ab, 0xe00207eb49e01594),
            U64x2(0x942366a665208ef8, 0x1caa0c4ff751c880),
            U64x2(0xbd03239fe3e67e4a, 0x02f7f57fdb2dc1dd),
            U64x2(0x8f8f8f8f8f8f8f8f, 0x8f8f8f8f8f8f8f8f)
];


            let mut r = [0u64; 16];
            for i in 0..16 {
                r[i] = a[i] ^ b[i];
            }

            let mut c = [0u64; 16];

            for i in 0..16 {
                c[i] = a[i] ^ b[i] ^ r[i];
            }

            let mut d = [0u64; 16];

            for i in 0..16 {
                d[i] = a[i] ^ b[i] ^ c[i];
            }

            // The following are the results of multiplying the above value
    }


    #[test]
    fn test_mul_u64x2() {
        for i in 0..MUL_U64X2_TESTS.len() {
            let (a, b) = MUL_U64X2_TESTS[i];
            let c = a.mul_u64x2(b);
            assert_eq!(c, MUL_U64X2_RESULTS[i]);
        }
    }

    #[test]
    fn test_mul_wide_u64x2() {
        for i in 0..MUL_WIDE_U64X2_TESTS.len() {
            let (a, b) = MUL_WIDE_U64X2_TESTS[i];
            let c = a.mul_wide_u64x2(b);
            assert_eq!(c, MUL_WIDE_U64X2_RESULTS[i]);
        }
    }
}

/// The following tests are for the `u64x2_shifts_wide` function.
/// The tests are based on the following values:
///
/// ```text
/// a = 0x0123456789abcdef, 0xfedcba9876543210
/// b = 0xfedcba9876543210, 0x0123456789abcdef
/// ```
/// use the following values:
/// 0x0123456789abcdef, 0xfedcba9876543210
/// 0xfedcba9876543210, 0x0123456789abcdef
/// 0x0123456789abcdef, 0xfedcba9876543210
/// 0xfedcba9876543210, 0x0123456789abcdef
/// 0x0123456789abcdef, 0xfedcba9876543210
/// 0xfedcba9876543210, 0x0123456789abcdef
/// 0x0123456789abcdef, 0xfedcba9876543210


#[cfg(test)]
pub static AES_RCON: [u8; 7] = [0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40];

#[cfg(test)]
pub static AES_SBOX: [u8; 256] = [
    0x63, 0x7c, 0x77, 0x7b, 0xf2, 0x6b, 0x6f, 0xc5, 0x30, 0x01, 0x67, 0x2b, 0xfe, 0xd7, 0xab, 0x76,
    0xca, 0x82, 0xc9, 0x7d, 0xfa, 0x59, 0x47, 0xf0, 0xad, 0xd4, 0xa2, 0xaf, 0x9c, 0xa4, 0x72, 0xc0,
    0xb7, 0xfd, 0x93, 0x26, 0x36, 0x3f, 0xf7, 0xcc, 0x34, 0xa5, 0xe5, 0xf1, 0x71, 0xd8, 0x31, 0x15,
    0x04, 0xc7, 0x23, 0xc3, 0x18, 0x96, 0x05, 0x9a, 0x07, 0x12, 0x80, 0xe2, 0xeb, 0x27, 0xb2, 0x75,
    0x09, 0x83, 0x2c, 0x1a, 0x1b, 0x6e, 0x5a, 0xa0, 0x52, 0x3b, 0xd6, 0xb3, 0x29, 0xe3, 0x2f, 0x84,
    0x53, 0xd1, 0x00, 0xed, 0x20, 0xfc, 0xb1, 0x5b, 0x6a, 0xcb, 0xbe, 0x39, 0x4a, 0x4c, 0x58, 0xcf,
    0xd0, 0xef, 0xaa, 0xfb, 0x43, 0x4d, 0x33, 0x85, 0x45, 0xf9, 0x02, 0x7f, 0x50, 0x3c, 0x9f, 0xa8,
    0x51, 0xa3, 0x40, 0x8f, 0x92, 0x9d, 0x38, 0xf5, 0xbc, 0xb6, 0xda, 0x21, 0x10, 0xff, 0xf3, 0xd2,
    0xcd, 0x0c, 0x13, 0xec, 0x5f, 0x97, 0x44, 0x17, 0xc4, 0xa7, 0x7e, 0x3d, 0x64, 0x5d, 0x19, 0x73,
    0x60, 0x81, 0x4f, 0xdc, 0x22, 0x2a, 0x90, 0x88, 0x46, 0xee, 0xb8, 0x14, 0xde, 0x5e, 0x0b, 0xdb,
    0xe0, 0x32, 0x3a, 0x0a, 0x49, 0x06, 0x24, 0x5c, 0xc2, 0xd3, 0xac, 0x62, 0x91, 0x95, 0xe4, 0x79,
    0xe7, 0xc8, 0x37, 0x6d, 0x8d, 0xd5, 0x4e, 0xa9, 0x6c, 0x56, 0xf4, 0xea, 0x65, 0x7a, 0xae, 0x08,
    0xba, 0x78, 0x25, 0x2e, 0x1c, 0xa6, 0xb4, 0xc6, 0xe8, 0xdd, 0x74, 0x1f, 0x4b, 0xbd, 0x8b, 0x8a,
    0x70, 0x3e, 0xb5, 0x66, 0x48, 0x03, 0xf6, 0x0e, 0x61, 0x35, 0x57, 0xb9, 0x86, 0xc1, 0x1d, 0x9e,
    0xe1, 0xf8, 0x98, 0x11, 0x69, 0xd9, 0x8e, 0x94, 0x9b, 0x1e, 0x87, 0xe9, 0xce, 0x55, 0x28, 0xdf,
    0x8c, 0xa1, 0x89, 0x0d, 0xbf, 0xe6, 0x42, 0x68, 0x41, 0x99, 0x2d, 0x0f, 0xb0, 0x54, 0xbb, 0x16,
];


#[inline(always)]
pub(crate) fn aesenc(block: &mut U64x2, rkey: &U64x2) {
    unsafe {
        llvm_asm!("aesenc $0, $1"
            : "+x"(*block)
            : "x"(*rkey)
            :
            : "intel", "alignstack"
        );
    }
}

#[inline(always)]
pub(crate) fn aesenclast(block: &mut U64x2, rkey: &U64x2) {
    unsafe {
        llvm_asm!("aesenclast $0, $1"
            : "+x"(*block)
            : "x"(*rkey)
            :
            : "intel", "alignstack"
        );
    }
}

macro_rules! aeskeygenassist {
    ($dst:expr, $src:expr, $round:expr) => {
        unsafe {
            llvm_asm!("aeskeygenassist $0, $1, $2"
                : "+x"(*$dst)
                : "x"(*$src), "i"($round)
                :
                : "intel", "alignstack"
            );
        }
    };

    ($dst:expr, $src:expr, $round:expr, $tmp:expr) => {
        unsafe {
            llvm_asm!("aeskeygenassist $0, $1, $2"
                : "+x"(*$dst), "+x"(*$tmp)
                : "x"(*$src), "i"($round)
                :
                : "intel", "alignstack"
            );
        }
    };
    ($src:ident, $i:expr) => {{
        let mut tmp = 0u64x2;
        let mut dst = mem::MaybeUninit::<u64x2>::uninit();
        unsafe {
            aeskeygenassist!(dst.as_mut_ptr(), $src, $i);
            tmp = dst.assume_init();
            llvm_asm!("aeskeygenassist $0, $1, $2"
                    : "+x"(*dst.as_mut_ptr())
                    : "x"(*$src), "i"($i)
                    :
                    : "intel", "alignstack"
                );
            dst.assume_init()
        }

        tmp
    }}

    ($src:ident, $i:expr, $tmp:ident) => {{
        let mut dst = mem::MaybeUninit::<u64x2>::uninit();
        unsafe {
            aeskeygenassist!(dst.as_mut_ptr(), $src, $i, $tmp);
            dst.assume_init()
        }
    }}
}