//Copyright 2021-2023 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.



///With semantics, however, users access a host of benefits from the data lake architecture.
/// Users can help themselves to scalable cloud storage and processing platforms,
/// EinsteinDB using EinsteinML a beta Lisp Interpreter and Transducer; operating with BerolinaSQL as a SQLTypeAffinity Multiplexer for SQL Forests in the Contextual
/// Domain of the causet and causet query (causetq)store
///
/// wherein all data for both transactional and analytics/BI use cases,
/// and comprehensively query data to support
/// modern machine learning and Artificial Intelligence applications.
use EinsteinDB::einstein_ml::*;
use EinsteinDB::einstein_ml::prelude::*;






mod prelude {
    pub use EinsteinDB::einstein_ml::*;
}


mod causetq {
    pub use EinsteinDB::einstein_ml::causetq::*;
}


#[test]
fn test_linear_regression() {
    let mut linear_regression = LinearRegression::new();
    let mut data = DataFrame::new();
    data.insert_column("x", Series::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
    data.insert_column("y", Series::from(vec![2.0, 4.0, 6.0, 8.0, 10.0]));
    linear_regression.fit(&data);
    let result = linear_regression.predict(&data);
    assert_eq!(result.get_column("y").unwrap().len(), 5);
    assert_eq!(result.get_column("y").unwrap().get(0).unwrap(), 2.0);
    assert_eq!(result.get_column("y").unwrap().get(1).unwrap(), 4.0);
    assert_eq!(result.get_column("y").unwrap().get(2).unwrap(), 6.0);
    assert_eq!(result.get_column("y").unwrap().get(3).unwrap(), 8.0);
    assert_eq!(result.get_column("y").unwrap().get(4).unwrap(), 10.0);
}


#[test]
fn test_linear_regression_with_weights() {
    let mut linear_regression = LinearRegression::new();
    let mut data = DataFrame::new();
    data.insert_column("x", Series::from(vec![1.0, 2.0, 3.0, 4.0, 5.0]));
    data.insert_column("y", Series::from(vec![2.0, 4.0, 6.0, 8.0, 10.0]));
    data.insert_column("weights", Series::from(vec![1.0, 1.0, 1.0, 1.0, 1.0]));
    linear_regression.fit(&data);
    let result = linear_regression.predict(&data);
    assert_eq!(result.get_column("y").unwrap().len(), 5);
    assert_eq!(result.get_column("y").unwrap().get(0).unwrap(), 2.0);
    assert_eq!(result.get_column("y").unwrap().get(1).unwrap(), 4.0);
    assert_eq!(result.get_column("y").unwrap().get(2).unwrap(), 6.0);
    assert_eq!(result.get_column("y").unwrap().get(3).unwrap(), 8.0);
    assert_eq!(result.get_column("y").unwrap().get(4).unwrap(), 10.0);
}

use super::*;
use crate::error::{Error, Result};
use crate::parser::{Parser, ParserError};
use crate::value::{Value, ValueType};
use crate::{ValueRef, ValueRefMut};
use itertools::Itertools;
use pretty;
use std::{
    collections::HashMap,
    fmt::{self, Display},
    io,
    convert::{TryFrom, TryInto},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};

use ::serde::{Deserialize, Serialize};
use EinsteinDB::einstein_ml::prelude::*;


/// A `Value` is a wrapper around a `Doc`.



/// A context for executing a program.
///
/// A context is created by calling `Context::new()`.
///
/// A context can be used to create multiple `Executor`s.
///
/// A context can be used to create multiple `Session`s.

pub struct LightlikeContext {
    pub(crate) doc: Doc,
    pub(crate) parser: Parser,
    pub(crate) session: Session,
    pub context: Context,
}
pub struct Context {
    pub(crate) allocator: pretty::BoxAllocator,
    pub(crate) variables: HashMap<String, Value>,
    pub(crate) inner: Arc<Mutex<ContextInner>>,
}

pub struct ContextInner {
    pub(crate) executors: Vec<Executor>,
    pub(crate) sessions: Vec<Session>,
}


impl Context {
    /// Create a new context.
    pub fn new() -> Self {
        Self {
            allocator: pretty::BoxAllocator,
            variables: HashMap::new(),
            inner: Arc::new(Mutex::new(ContextInner {
                executors: Vec::new(),
                sessions: Vec::new(),
            })),


        }
        /// Create a new context.
        /// This is a convenience function that calls `Context::new()`.
    }


}


/// A session is a context for executing a program.
/// It is created by calling `Session::new()`.
/// A session can be used to create multiple `Executor`s.
/// A session can be used to create multiple `Session`s.
/// 
/// 
/// 




   #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct Session {
         pub(crate) inner: Arc<Mutex<SessionInner>>,

    }
    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct SessionInner {
        pub(crate) executors: Vec<Executor>,
        pub(crate) variables: HashMap<String, Value>,
    }
    impl Session {
        /// Create a new session.
        pub fn new() -> Self {
            Self {
                inner: Arc::new(Mutex::new(SessionInner {
                    executors: Vec::new(),
                    variables: HashMap::new(),
                })),
            }
        }
        /// Create a new session.
        /// This is a convenience function that calls `Session::new()`.
    }

   /// 
   /// 
   /// 

    /// Create a new executor.
    pub fn new_executor(&self) -> Executor {
        let inner = self.inner.clone();
        let mut inner = inner.lock().unwrap();
        let executor = Executor {
            inner: inner.executors.len(),
            inner: inner.executors.push(ExecutorInner {
                context: self.clone(),
                variables: HashMap::new(),
            }).unwrap(),
        };
        executor
    }

    /// Create a new session.
    pub fn new_session(&self) -> Session {
        let inner = self.inner.clone();
        let mut inner = inner.lock().unwrap();
        let session = Session {
            inner: inner.sessions.len(),
            inner: inner.sessions.push(SessionInner {
                context: self.clone(),
                variables: HashMap::new(),
            }).unwrap(),
        };
        session
    }
    /// This is a convenience function that calls `Context::sessions_len()`.
    pub fn sessions_len(&self) -> usize {
        let inner = self.inner.clone();
        let inner = inner.lock().unwrap();
        inner.sessions.len()
    }
    /// Get the number of executors.
    /// This is a convenience function that calls `Context::executors_len()`.
    pub fn executors_len(&self) -> usize {
        let inner = self.inner.clone();
        let inner = inner.lock().unwrap();
        inner.executors.len()
    }
    /// Get the number of sessions.
    /// This is a convenience function that calls `Context::sessions_len()`.
    pub fn executors(&self) -> Vec<Executor> {
        let inner = self.inner.clone();
        let inner = inner.lock().unwrap();
        inner.executors.clone()
    }
    /// Get the number of sessions.
    /// This is a convenience function that calls `Context::sessions_len()`.
    pub fn sessions(&self) -> Vec<Session> {
        let inner = self.inner.clone();
        let inner = inner.lock().unwrap();
        inner.sessions.clone()
    }
    /// Get the number of sessions.
    /// This is a convenience function that calls `Context::sessions_len()`.
    pub fn session(&self, index: usize) -> Session {
        let inner = self.inner.clone();
        let inner = inner.lock().unwrap();
        inner.sessions[index].clone()
    }
    /// Get the number of sessions.
    /// This is a convenience function that calls `Context::sessions_len()`.
    pub fn executor(&self, index: usize) -> Executor {
        let inner = self.inner.clone();
        let inner = inner.lock().unwrap();
        inner.executors[index].clone()
    }
  







pub trait EinsteinMlToString {

    //tinkerpop
    fn to_string(&self) -> String;

    fn einstein_ml_to_string(&self) -> String;
}

//From FDB to AEVTrie
pub trait FDBToAEVTrie {
    fn fdb_to_aevtrie(&self) -> AEVTrie;
}

//FoundationDB SQL dialect
pub trait FdbSqlDialect {
    fn to_string(&self) -> String;
}

//ML SQL dialect
pub trait MlSqlDialect {
    fn to_string(&self) -> String;
}
//A crown inherits the topological properties of allegro_poset and composes a dag projection list.
pub trait Crown {   //tinkerpop graph
    //tinkerpop


    fn to_string(&self) -> String;

    fn einstein_ml_to_string(&self) -> String;

    fn get_projector(&self) -> Arc<Mutex<dyn Projector>>;

    fn get_projector_mut(&self) -> Arc<Mutex<dyn Projector>>;
}


pub trait ProjectorBuilder {
    fn build(&self) -> Result<Arc<Mutex<dyn Projector>>>;

}


pub trait ProjectorBuilderFactory {
    fn project<'stmt, 's>(&self, topograph: &Topograph, berolina_sql: &'s berolina_sql::Connection, rows: Rows<'stmt>) -> Result<QueryOutput>;
    fn columns<'s>(&'s self) -> Box<dyn Iterator<Item=&Element> + 's>;

    fn is_projectable(&self) -> bool {
        let x = self.columns().count() == 1;
        x && self.columns().next().unwrap().is_scalar() && self.columns().next().is_none() && self.columns().next().is_none();
        x && self.columns().all(|e| e.is_projectable())
    }

    fn is_projectable_with_topograph(&self, topograph: &Topograph) -> bool {
        let x = self.columns().count() == 1;

        x && self.columns().all(|e| e.is_projectable_with_topograph(topograph))
    }

    fn is_projectable_with_topograph_and_berolina_sql(&self, topograph: &Topograph, berolina_sql: &berolina_sql::Connection) -> bool {
        let x = self.columns().count() == 1;

        x && self.columns().all(|e| e.is_projectable_with_topograph_and_berolina_sql(topograph, berolina_sql))  && self.columns().next().is_none() && self.columns().next().is_none()


    }

    fn is_projectable_with_topograph_and_berolina_sql_and_rows(&self, topograph: &Topograph, berolina_sql: &berolina_sql::Connection, rows: Rows) -> bool {
        let x = self.columns().count() == 1;

        x && self.columns().all(|e| e.is_projectable_with_topograph_and_berolina_sql_and_rows(topograph, berolina_sql, rows))
    }


        fn semi_groupoid(&self) -> bool {
            self.columns().count() == 1
        }

        fn is_sortable(&self) -> bool {
            self.columns().count() == 1
        }
    }
