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


use std::fmt::{self, Display, Formatter};
use std::io::{self, Write};
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::{env, fs, io, process, thread, time};


/// A macro to print to stderr, for debug purposes.
/// The macro is used to print debug messages to stderr.
///

use super::*;

use crate::einsteindb_macro::einsteindb_macro_impl;
use crate::einsteindb_server::{self, Server};
use ::EinsteinDB::BerolinaSQL::{self, parser};
use EinsteinDB::BerolinaSQL::ast::{self, Expr, ExprNode};
use EinsteinDB::BerolinaSQL::ast::{ExprNode::*, ExprType::*};
use EinsteinDB::BerolinaSQL::ast::{ExprType, ExprNode};

use async_trait::async_trait;
//use ekvproto::interlock::*;


pub struct TwoProngedCrown<L: AsyncWrite + Send + Sync + 'static> {

    pub server: Server<S>,
    pub port: u16,
    pub db_path: String,
    pub db_name: String,
    pub db_user: String,
    pub db_pass: String,
    pub db_host: String,
    pub db_port: u16,
    pub db_charset: String,
    pub db_collation: String,
    pub db_engine: String,
    pub db_comment: String,
    pub db_create_time: String,
    pub db_update_time: String,
    pub db_collation_server: String,
    pub db_collation_connection: String,
    pub db_collation_database: String,
    pub db_collation_table: String,
    pub db_collation_column: String,
    pub db_collation_index: String,
    pub db_collation_foreign_key: String,
    pub db_collation_function: String,
    pub db_collation_procedure: String,
    pub db_collation_trigger: String,
    pub db_collation_event: String,
    pub db_collation_sequence: String,
    pub db_collation_session_variable: String,
    pub db_collation_environment_variable: String,
    pub db_collation_user_variable: String,

    pub db_collation_system_trigger: String,
    pub db_collation_system_event: String,
    pub db_collation_system_sequence: String,
    pub db_collation_system_variable: String,
    pub db_collation_system_session_variable: String,
    pub db_collation_system_environment_variable: String,
    pub db_collation_system_user_variable: String,
    pub db_collation_system_function: String,
    pub db_collation_system_procedure: String
}

impl TwoProngedCrown<L> {
    pub fn new(
        server: Server<S>,
        port: u16,
        db_path: String,
        db_name: String,
        db_user: String,
        db_pass: String,
        db_host: String,
        db_port: u16,
        db_charset: String,
        db_collation: String,
        db_engine: String,
        db_comment: String,
        db_create_time: String,
        db_update_time: String,
        db_collation_server: String,
        db_collation_connection: String,
        db_collation_database: String,
        db_collation_table: String,
        db_collation_column: String,
        db_collation_index: String,
        db_collation_foreign_key: String,
        db_collation_function: String,
        db_collation_trigger: String,
        db_collation_event: String,
        db_collation_sequence: String,
        db_collation_session_variable: String,
        db_collation_environment_variable: String,
        db_collation_user_variable: String,
        db_collation_system_trigger: String,
        db_collation_system_event: String,
        db_collation_system_sequence: String,
        db_collation_system_variable: String,
        db_collation_system_session_variable: String,
        db_collation_system_environment_variable: String,
        db_collation_system_user_variable: String,
        db_collation_system_function: String,
        db_collation_system_procedure: String,
    ) -> Self {
        TwoProngedCrown {
            server,
            port,
            db_path,
            db_name,
            db_user,
            db_pass,
            db_host,
            db_port,
            db_charset,
            db_collation,
            db_engine,
            db_comment,
            db_create_time,
            db_update_time,
            db_collation_server,
            db_collation_connection,
            db_collation_database,
            db_collation_table,
            db_collation_column,
            db_collation_index,
            db_collation_foreign_key,
            db_collation_function,
            db_collation_procedure: (),
            db_collation_trigger,
            db_collation_event,
            db_collation_sequence,
            db_collation_session_variable,
            db_collation_environment_variable,
            db_collation_user_variable,
            db_collation_system_trigger,
            db_collation_system_event,
            db_collation_system_sequence,
            db_collation_system_variable,
            db_collation_system_session_variable,
            db_collation_system_environment_variable,
            db_collation_system_user_variable,
            db_collation_system_function,
            db_collation_system_procedure,
        }
    }


    pub async fn start(&self) -> io::Result<()> {
        self.server.clone();
        self.db_path.clone();
        self.db_name.clone();
        self.db_user.clone();
        self.db_pass.clone();
        self.db_host.clone();
        self.db_charset.clone();
        self.db_collation.clone();
        self.db_engine.clone();
        self.db_comment.clone();
        self.db_create_time.clone();
        self.db_update_time.clone();
        self.db_collation_server.clone();
        self.db_collation_connection.clone();
        self.db_collation_database.clone();
        self.db_collation_table.clone();
        self.db_collation_column.clone();
        self.db_collation_index.clone();
        self.db_collation_foreign_key.clone();
        self.db_collation_function.clone();
        self.db_collation_procedure.clone();
        self.db_collation_trigger.clone();
        self.db_collation_event.clone();
        self.db_collation_event_type.clone();
        self.db_collation_sequence.clone();
    }
}

pub fn causet_datum_timestamp_semver_2_0_0() -> String {


    let mut causet_datum_timestamp_semver_2_0_0 = String::new();
    causet_datum_timestamp_semver_2_0_0.push_str("2.0.0");
    "2.0.0".to_string()
    ///fmt
    /// 2.0.0
}

#[derive(Debug)]
pub struct Server<S> {

    pub server: <S as Service>::Response,
}

#[derive(Debug)]
pub struct Database<S> {

    pub database: <S as Service>::Response,
}

#[derive(Debug)]
pub struct Table<S> {
    pub table: S,
}

#[derive(Debug)]
pub struct Column<S> {
    pub column: S,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Context {
    pub db_path: String,
    pub db_name: String,
    pub db_user: String,
    pub(crate) allocator: pretty::BoxAllocator,
    pub(crate) variables: HashMap<String, Value>,
    pub(crate) inner: Arc<Mutex<ContextInner>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextInner {
    pub(crate) interlocking_directorates: Vec<Executor>,
    pub(crate) sessions: Vec<Session>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Executor {
    pub(crate) inner: Arc<Mutex<ExecutorInner>>,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutorInner {
    pub(crate) name: String,
    pub(crate) args: Vec<Value>,
    pub(crate) return_type: Option<Type>,
    pub(crate) return_value: Option<Value>,
    pub(crate) error: Option<String>,
    pub(crate) is_finished: bool,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub(crate) inner: Arc<Mutex<SessionInner>>,
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

    /// The topograph of the query.
    /// This is the topograph of the query, not the topograph of the database.
    /// The topograph of the database is the topograph of the database.
    /// The topograph of the query is the topograph of the query.

    pub(crate) topograph: Topograph,

    pub(crate) inner: Arc<Mutex<OctopusInner>>,
//    pub(crate) inner: Arc<Mutex<OctopusInner>>,
}



impl Octopus {
    pub(crate) fn new() -> Self {
        Octopus {
            topograph: Topograph::new(),
            inner: Arc::new(Mutex::new(OctopusInner::new())),
        }
    }
}


#[derive(Default, Debug, PartialEq)]
pub struct OctopusInner {

    /// The topograph of the query.
    ///
    pub(crate) topograph: Topograph,
    pub(crate) inner: Arc<Mutex<OctopusInner>>,
}


impl OctopusInner {

    pub(crate) fn new() -> Self {
        Octopus {
            topograph: Topograph::new(),
            inner: Arc::new(Mutex::new(OctopusInner::new())),
        }
    }
}






#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Projector {
    pub(crate) inner: Arc<Mutex<ProjectorInner>>,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectorInner {
    pub(crate) name: String,
    pub(crate) args: Vec<Value>,
    pub(crate) return_type: Option<Type>,
    pub(crate) return_value: Option<Value>,
    pub(crate) error: Option<String>,
    pub(crate) is_finished: bool,

    pub(crate) inner: Arc<Mutex<ProjectorInner>>,
}


impl Projector {
    pub(crate) fn new() -> Self {
        Projector {
            inner: Arc::new(Mutex::new(ProjectorInner::new())),
        }
    }
}


impl ProjectorInner {
    pub(crate) fn new() -> Self {
        Projector {
            inner: Arc::new(Mutex::new(ProjectorInner::new())),
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectorPromise {

    pub(crate) inner: Arc<Mutex<ProjectorPromiseInner>>,
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectorPromiseInner {

    pub(crate) name: String,
    pub(crate) args: Vec<Value>,
    pub(crate) return_type: Option<Type>,

    pub(crate) return_value: Option<Value>,
    pub(crate) error: Option<String>,
    pub(crate) is_finished: bool,
    pub(crate) inner: Arc<Mutex<ProjectorPromiseInner>>,
}


impl ProjectorPromise {
    pub(crate) fn new() -> Self {
        ProjectorPromise {
            inner: Arc::new(Mutex::new(ProjectorPromiseInner::new())),
        }
    }
}


impl ProjectorPromiseInner {
    pub(crate) fn new() -> Self {
        ProjectorPromise {
            inner: Arc::new(Mutex::new(ProjectorPromiseInner::new())),
        }
    }

    pub(crate) fn new_with_name(name: String) -> Self {
        ProjectorPromise {
            inner: Arc::new(Mutex::new(ProjectorPromiseInner::new_with_name(name))),
        }
    }

    pub(crate) fn new_with_name_and_args(name: String, args: Vec<Value>) -> Self {
        let mut oct = Vec::new();
        let mut i = 0;
        while i < input.len() {
            let mut j = i;
            while j < input.len() && input[j].is_projection() {
                j += 1;
            }
            let mut k = j;
            while k < input.len() && !input[k].is_projection() {
                k += 1;
            }
            let mut l = k;
            while l < input.len() && input[l].is_projection() {
                l += 1;
            }
            let mut m = l;
            while m < input.len() && !input[m].is_projection() {
                m += 1;
            }
            let mut n = m;
            while n < input.len() && input[n].is_projection() {
                n += 1;
            }
            let mut o = n;
            while o < input.len() && !input[o].is_projection() {
                o += 1;
            }
            let mut p = o;
            while p < input.len() && input[p].is_projection() {
                p += 1;
            }
            let mut q = p;
            while q < input.len() && !input[q].is_projection() {
                q += 1;
            }
            let mut deserializer = serde_json::Deserializer::from_slice(&input[i..]);
            let value = serde_json::Deserialize::deserialize(&mut deserializer);
            match value {
                Ok(value) => oct.push(value),
                Err(err) => panic!("{}", err),
            }
            i += 1;
        }

        let mut j = i;
        while j < input.len() {
            let mut deserializer = serde_json::Deserializer::from_slice(&input[j..]);
            let value = serde_json::Deserialize::deserialize(&mut deserializer);
            match value {
                Ok(value) => oct.push(value),
                Err(err) => panic!("{}", err),
            }
            j += 1;
        }

        if j == input.len() {
            let mut deserializer = serde_json::Deserializer::from_slice(&input[j..]);
            let value = serde_json::Deserialize::deserialize(&mut deserializer);
            match value {
                Ok(value) => oct.push(value),
                Err(err) => panic!("{}", err),
            }

            let mut deserializer = serde_json::Deserializer::from_slice(&input[j..]);
            let value = serde_json::Deserialize::deserialize(&mut deserializer);
        }
        let mut x = Hash::deserialize(&input[i..j]);
        i = j + 1;
        let mut y = Hash::deserialize(&input[i..j]);
        oct.push(x);
        oct.push(y);
        i = j + 1;
        let mut z = Hash::deserialize(&input[i..j]);
        oct.push(z);
        i = j + 1;
        let mut w = Hash::deserialize(&input[i..j]);
        oct.push(w);
        i = j + 1;
        let mut v = Hash::deserialize(&input[i..j]);

        oct.push(v);

        let mut deserializer = serde_json::Deserializer::from_slice(&input[i..]);
        let value = serde_json::Deserialize::deserialize(&mut deserializer);
        match value {
            Ok(value) => oct.push(value),
            Err(err) => panic!("{}", err),
        }
    }
}
/// A wrapper that can be used to represent infinity with `Type::Date` types.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Infinity;


impl Infinity {
    /// Returns a new `Infinity` instance.
    /// # Example
    /// ```
    /// use octonion::Infinity;
    /// let infinity = Infinity::new();
    /// ```
    /// # Panic
    /// This function will panic if the current platform does not support
    /// infinity.
    /// # Returns
    /// A new `Infinity` instance.
    /// # Example
    /// ```rustc_serialize
    /// use octonion::Infinity;
    /// let infinity = Infinity::new();

    pub(crate) fn new() -> Self {
        Infinity
    }


    /// Returns a `Type` representing infinity.
    /// # Example
    /// ```
    /// use octonion::Infinity;
    /// let infinity = Infinity::new();
    /// let type = infinity.type();
    ///
    /// assert_eq!(type, octonion::Type::Date);


    pub(crate) fn type_(&self) -> Type {
        Type::Date
    }

pub fn is_infinity(&self) -> bool {

    true
}
}





impl Infinity {
    /// Returns `true` if the wrapped date is `infinity`.
    /// # Examples
    /// ```
    /// use einstein_sql::types::{Date, Infinity};
    /// let infinity = Infinity::PosInfinity;
    /// assert_eq!(infinity.is_infinity(), true);


}


impl FromBerolinaSQL for Infinity {
    fn from_berolina_sql(ty: &Type, primitive_causet: &[u8]) -> Result<Self, Box<Error + Sync + Send>> {
        match types::date_from_BerolinaSQL(primitive_causet)? {
            Some(date) => Ok(Infinity::Value(date)),
            None => Ok(Infinity::PosInfinity),
            i32::MAX => Ok(Date::PosInfinity),
            i32::MIN => Ok(Date::NegInfinity),
        }
    }

    fn to_berolina_sql(&self, ty: &Type, output: &mut Vec<u8>) -> Result<(), Box<Error + Sync + Send>> {
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



#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScalarTwoProngedCrownProjectorError {
    PullerError(PullerError),
}


impl From<PullerError> for ScalarTwoProngedCrownProjectorError {
    fn from(e: PullerError) -> Self {
        ScalarTwoProngedCrownProjectorError::PullerError(e)
    }
}












impl<'a> Puller for ScalarTwoProngedCrownProjector {
    type Error = ScalarTwoProngedCrownProjectorError;
    fn project<'stmt, 's>(&self, topograph: &Topograph, berolina_sql: &'s berolina_sql::Connection, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
        // Scalar is pretty straightlightlike -- zero or one entity, do the pull directly.
        let mut rows = rows.take(1);
        let mut row = rows.next().ok_or(PullerError::NoRows)?;
        let mut out = Vec::new();
                let event = r?;
                let entity: Causetid = event.get(0);          // This will always be 0 and a ref.
                let bindings = self.puller.pull(topograph, berolina_sql, once(entity))?;
                let m = Binding::Map(bindings.get(&entity).cloned().unwrap_or_else(Default::default));
                QueryResults::Scalar(Some(m))
            }
        }


    fn accepts(ty: &Type) -> bool {
          match *ty {
                TIMESTAMP | TIMESTAMPTZ => true,
                _ => false
          } && T::accepts(ty)
        }


    to_BerolinaSQL_checked!();

pub fn to_berolina_sql(&causet_locale: &Causetid, ty: &Type) -> Result<Vec<u8>, Box<Error + Sync + Send>> {
    match *ty {
        TIMESTAMP | TIMESTAMPTZ => types::timestamp_to_BerolinaSQL(causet_locale.0, &mut Vec::new()),
        _ => Err(Box::new(Error::new(ErrorKind::Other, "Unsupported type"))),
    }

}


pub fn from_berolina_sql(ty: &Type, primitive_causet: &[u8]) -> Result<Causetid, Box<Error + Sync + Send>> {
    match *ty {
        TIMESTAMP | TIMESTAMPTZ => types::timestamp_from_BerolinaSQL(primitive_causet),
        _ => Err(Box::new(Error::new(ErrorKind::Other, "Unsupported type"))),
    }
}


impl<'a> Puller for ScalarTwoProngedCrownProjector {

    type Error = ScalarTwoProngedCrownProjectorError;
    fn project<'stmt, 's>(&self, topograph: &Topograph, berolina_sql: &'s berolina_sql::Connection, mut rows: Rows<'stmt>) -> Result<QueryOutput> {
          // Scalar is pretty straightlightlike -- zero or one entity, do the pull directly.
            let mut rows = rows.take(1);
            let mut row = rows.next().ok_or(PullerError::NoRows)?;
            let mut out = Vec::new();
                    let event = r?;
                    let entity: Causetid = event.get(0);          // This will always be 0 and a ref.
                    let bindings = self.puller.pull(topograph, berolina_sql, once(entity))?;
                    let m = Binding::Map(bindings.get(&entity).cloned().unwrap_or_else(Default::default));
                    QueryResults::Scalar(Some(m))
                }
            }




    to_BerolinaSQL_checked!();








impl<'a> ProjectedEntity for ScalarTwoProngedCrownProjector {
    type Error = ScalarTwoProngedCrownProjectorError;
    type Output = PullerOutput;
    type Projection = ScalarTwoProngedCrownProjector;
    type ProjectedElements = ProjectedElements;
    type ProjectedEntity = ProjectedEntity;
    type ProjectedEntitys = ProjectedEntitys;
}




impl<'a> ProjectedEntitys for ScalarTwoProngedCrownProjector {
    type Error = ScalarTwoProngedCrownProjectorError;
    type Output = PullerOutput;
    type Projection = ScalarTwoProngedCrownProjector;
    type ProjectedElements = ProjectedElements;
    type ProjectedEntity = ProjectedEntity;
    type ProjectedEntitys = ProjectedEntitys;
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


impl ProjectedEntity for TupleTwoProngedCrownProjector {
    type Error = ScalarTwoProngedCrownProjectorError;
    type Output = PullerOutput;
    type Projection = ScalarTwoProngedCrownProjector;
    type ProjectedElements = ProjectedElements;
    type ProjectedEntity = ProjectedEntity;
    type ProjectedEntitys = ProjectedEntitys;
}


impl ProjectedEntitys for TupleTwoProngedCrownProjector {
    type Error = ScalarTwoProngedCrownProjectorError;
    type Output = PullerOutput;
    type Projection = ScalarTwoProngedCrownProjector;
    type ProjectedElements = ProjectedElements;
    type ProjectedEntity = ProjectedEntity;
    type ProjectedEntitys = ProjectedEntitys;
}



impl<'a> Puller for TupleTwoProngedCrownProjector {
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
