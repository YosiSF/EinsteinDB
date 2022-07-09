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




/// A `Executor` is a context for executing a program.


    /// Create a new session.
    ///
    ///


/// A `Session` is a context for executing a program.


pub struct Session {
    pub(crate) context: Context,
    pub(crate) variables: HashMap<String, Value>,
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
                context: Context::new(),
                variables: HashMap::new(),
                inner: Arc::new(Mutex::new(SessionInner {
                    executors: Vec::new(),
                    variables: HashMap::new(),
                })),
            }
        }
        /// Create a new session.
        /// This is a convenience function that calls `Session::new()`.


    }

    impl SessionInner {
        /// Create a new session.
        /// This is a convenience function that calls `Session::new()`.


        pub fn new() -> Self {
            Self {
                executors: Vec::new(),
                variables: HashMap::new(),
            }
        }


        pub fn new_executor(&mut self) -> Executor {
            let executor = Executor::new();
            self.executors.push(executor);
            executor
        }

        pub fn new_session(&mut self) -> Session {
            let session = Session::new();
            self.executors.push(session.inner.lock().unwrap().executors[0].clone());
            session
        }

        pub fn new_variable(&mut self, name: &str, value: Value) {
            self.variables.insert(name.to_string(), value);
        }
    }


/// A `Executor` is a context for executing a program.
pub struct Executor {

    pub(crate) context: Context,
    pub(crate) variables: HashMap<String, Value>,
    pub(crate) inner: Arc<Mutex<ExecutorInner>>,
}

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct ExecutorInner {

        pub(crate) variables: HashMap<String, Value>,
    }


    impl Executor {
        /// Create a new executor.
        pub fn new() -> Self {
            Self {
                context: Context::new(),
                variables: HashMap::new(),
                inner: Arc::new(Mutex::new(ExecutorInner {
                    variables: HashMap::new(),
                })),
            }
        }
        /// Create a new executor.
        /// This is a convenience function that calls `Executor::new()`.
    }
    impl ExecutorInner {
        /// Create a new executor.
        /// This is a convenience function that calls `Executor::new()`.
        pub fn new() -> Self {
            Self {
                variables: HashMap::new(),
            }
        }
        pub fn new_variable(&mut self, name: &str, value: Value) {
            self.variables.insert(name.to_string(), value);
        }
    }




    impl Executor {
        /// Create a new executor.
        /// This is a convenience function that calls `Executor::new()`.
    }


    impl ExecutorInner {
        /// Create a new executor.
        /// This is a convenience function that calls `Executor::new()`.
        pub fn new() -> Self {
            Self {
                variables: HashMap::new(),
            }
        }
        pub fn new_variable(&mut self, name: &str, value: Value) {
            self.variables.insert(name.to_string(), value);
        }
    }






/// A `Value` is a wrapper around a `Doc`.
/// A `Value` can be used to create a `Doc`.


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Value {
    pub(crate) doc: Doc,

}
    /// A `Series` is a wrapper around a `Vec<Value>`.
    /// It is created by calling `Series::new()`.
    /// A `Series` can be used to create multiple `Series`s


    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct Series {
        pub(crate) inner: Arc<Mutex<SeriesInner>>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct SeriesInner {
        pub(crate) values: Vec<Value>,
    }

    impl Series {
        /// Create a new series.
        pub fn new() -> Self {
            Self {
                inner: Arc::new(Mutex::new(SeriesInner {
                    values: Vec::new(),
                })),
            }
        }
        /// Create a new series.
        /// This is a convenience function that calls `Series::new()`.
    }

    impl SeriesInner {
        /// Create a new series.
        /// This is a convenience function that calls `Series::new()`.
    }

    /// A `DataFrame` is a wrapper around a `Vec<Series>`.
    /// It is created by calling `DataFrame::new()`.
    /// A `DataFrame` can be used to create multiple `DataFrame`s
    /// A `DataFrame` can be used to create multiple `Series`s


    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct DataFrame {
        pub(crate) inner: Arc<Mutex<DataFrameInner>>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct DataFrameInner {
        pub(crate) series: Vec<Series>,
    }

    impl DataFrame {
        /// Create a new dataframe.
        pub fn new() -> Self {
            Self {
                inner: Arc::new(Mutex::new(DataFrameInner {
                    series: Vec::new(),
                })),
            }
        }
        /// Create a new dataframe.
        /// This is a convenience function that calls `DataFrame::new()`.
    }


    impl DataFrameInner {
        /// Create a new dataframe.
        /// This is a convenience function that calls `DataFrame::new()`.
    }

    /// A `Table` is a wrapper around a `Vec<Vec<Value>>`.
    /// It is created by calling `Table::new()`.

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct Table {
        pub(crate) inner: Arc<Mutex<TableInner>>,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct TableInner {
        pub(crate) values: Vec<Vec<Value>>,
    }

    impl Table {
        /// Create a new table.
        pub fn new() -> Self {
            Self {
                inner: Arc::new(Mutex::new(TableInner {
                    values: Vec::new(),
                })),
            }
        }
        /// Create a new table.
        /// This is a convenience function that calls `Table::new()`.
    }

    impl TableInner {
        /// Create a new table.
        /// This is a convenience function that calls `Table::new()`.
    }

    /// A `DataFrame` is a wrapper around a `Vec<Vec<Value>>`.
    /// It is created by calling `DataFrame::new()`.
    ///
    ///




    /// Create a new value.
    /// This is a convenience function that calls `Value::new()`.

    pub fn new_value() -> Value {

        Value::new()
    }

    /// Create a new series.

    pub fn new_series() -> Series {
        Series::new()
    }

    /// Create a new dataframe.

    pub fn new_dataframe() -> DataFrame {
        DataFrame::new()
    }

    /// Create a new table.

    pub fn new_table() -> Table {
        Table::new()
    }

    /// Create a new executor.
    /// This is a convenience function that calls `Executor::new()`.

    pub fn new_executor_() -> Executor {
        new_executor()
    }

    /// Create a new value.
    /// This is a convenience function that calls `Value::new()`.
    /// This is a convenience function that calls `Value::new()`.

    pub fn new_value_() -> Value {
        new_value()
    }

    /// Create a new session.
    pub fn new_session() -> Session {
        let inner = SessionInner {
            executors: (),
            variables: HashMap::new(),
        };
        let mut inner = inner.lock().unwrap();
        let session = Session {
            context: Context {
                allocator: (),
                variables: (),
                inner
            },
            variables: (),
            inner: Arc::new(Mutex::new(inner)),
        };

        session
    }

    /// Create a new session.
    /// This is a convenience function that calls `Session::new()`.

    pub fn new_session_() -> Session {
        new_session()
    }


    /// Create a new context.
    /// This is a convenience function that calls `Context::new()`.

    pub fn new_context() -> Context {
        Context::new()
    }

    fn new_executor_inner() -> ExecutorInner {
        let inner = ExecutorInner {
            variables: HashMap::new(),
        };
        inner: Arc::new(Mutex::new(inner));
    }

    /// Create a new executor.
    /// This is a convenience function that calls `Executor::new()`.

pub fn new_executor() -> Executor {
        let inner = new_executor_inner();
        let mut inner = inner.lock().unwrap();
        let executor = Executor {
            context: Context {
                allocator: (),
                variables: (),
                inner
            },
            variables: (),
            inner: Arc::new(Mutex::new(inner)),
        };

        executor
    }

    fn new_value_inner() -> ValueInner {
        executor
    }

    fn new_series_inner() -> SeriesInner {
        let inner = SeriesInner {
            values: Vec::new(),
        };
        inner: Arc::new(Mutex::new(inner));
    }
    fn new_dataframe_inner() -> DataFrameInner {
        let inner = DataFrameInner {
            series: Vec::new(),
        };
        inner: Arc::new(Mutex::new(inner));
    }

    fn new_table_inner() -> TableInner {
        let inner = TableInner {

            values: Vec::new(),
        };
        inner: Arc::new(Mutex::new(inner));
    }

    fn new_session_inner() -> SessionInner {
        let inner = SessionInner {

            executors: (),
            variables: HashMap::new(),
        };
        inner: Arc::new(Mutex::new(inner));
    }

    fn new_context_inner() -> ContextInner {
        let inner = ContextInner {
            executors: (),
            sessions: ()
        };
        inner: Arc::new(Mutex::new(inner));
    }

