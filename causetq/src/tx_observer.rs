// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use crate::{
    causet::{
        causet_query::{CausetQuery, CausetQueryBuilder},
        causet_query_builder::CausetQueryBuilderImpl,
    },
    causetq::{
        causetq_query::{CausetqQuery, CausetqQueryBuilder},
        causetq_query_builder::CausetqQueryBuilderImpl,
    },
    common::{
        error::{Error, Result},
        field_type::FieldType,
        field_type_builder::FieldTypeBuilder,
        gremlin::{
            gremlin_query::{GremlinQuery, GremlinQueryBuilder},
            gremlin_query_builder::GremlinQueryBuilderImpl,
        },
        schema::{
            schema::{Schema, SchemaBuilder},
            schema_builder::SchemaBuilderImpl,
        },
        transaction::{
            transaction::{Transaction, TransactionBuilder},
            transaction_builder::TransactionBuilderImpl,
        },
    },
    schema::{
        schema::{Schema, SchemaBuilder},
        schema_builder::SchemaBuilderImpl,
    },
    transaction::{
        transaction::{Transaction, TransactionBuilder},
        transaction_builder::TransactionBuilderImpl,
    },
};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::Relaxed;



/// A macro for defining a `Result` with a custom error type.
/// This is similar to the `?` operator in Rust, but it allows you to define a
/// custom error type.
/// This macro is borrowed from the `failure` crate.
/// See [`failure`](https://crates.io/crates/failure) for more information.
/// # Example
/// ```
/// #[macro_use]
#[macro_export]
macro_rules! result {
    ($ok:expr) => {
        Ok($ok)
    };
    ($ok:expr, $err:expr) => {
        Ok($ok)
    };
    ($res:expr, $err:expr) => {
        $res.map_err(|err| $err)
    };
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionObserver {
    pub transaction_id: usize,
    pub transaction_type: TransactionType,
    pub transaction_status: TransactionStatus,
    pub transaction_schema: Arc<Schema>,
    pub transaction_fields: HashMap<String, FieldType>,
    pub transaction_data: HashMap<String, String>,
    pub transaction_dependencies: HashSet<usize>,
    pub transaction_dependents: HashSet<usize>,
    pub transaction_causet_queries: Vec<CausetQuery>,
    pub transaction_causetq_queries: Vec<CausetqQuery>,
    pub transaction_gremlin_queries: Vec<GremlinQuery>,
}





/// A macro for defining a `Result` with a custom error type.
/// This is similar to the `?` operator in Rust, but it allows you to define a
/// custom error type.
/// This macro is borrowed from the `failure` crate.
/// See [`failure`](https://crates.io/crates/failure) for more information.
/// # Example
/// ```
/// #[macro_use]
/// extern crate failure;
/// #[macro_use]
/// extern crate failure_derive;
/// #[macro_use]
/// extern crate soliton_panic;
///
/// #[macro_use]
/// extern crate soliton;
/// #[macro_use]
/// extern crate lazy_static;
/// #[macro_use]
/// extern crate soliton_derive;
///
///

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionType {
    Insert,
    Update,
    Delete,
}

#[macro_export]
macro_rules! soliton_derive {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $name {
            pub name: String,
            pub value: Value,
            pub ttl: i64,
        }
        impl $name {
            pub fn new(name: String, value: Value, ttl: i64) -> Self {
                $name {
                    name,
                    value,
                    ttl,
                }
            }
        }
        impl Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.name)
            }
        }
        impl Deref for $name {
            type Target = Value;
            fn deref(&self) -> &Self::Target {
                &self.value
            }
        }
        impl DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.value
            }
        }
    };
}


#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionStatus {
    Pending,
    Committed,
    RolledBack,
}

/// A macro for defining a `Result` with a custom error type.
/// This is similar to the `?` operator in Rust, but it allows you to define allows you to define a
/// custom error type.
///
/// This macro is borrowed from the `failure` crate.
/// See [`failure`](https://crates.io/crates/failure) for more information.


#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Transaction {
    pub transaction_id: usize,
    pub transaction_type: TransactionType,
    pub transaction_status: TransactionStatus,
    pub transaction_schema: Arc<Schema>,
    pub transaction_fields: HashMap<String, FieldType>,
    pub transaction_data: HashMap<String, String>,
    pub transaction_dependencies: HashSet<usize>,
    pub transaction_dependents: HashSet<usize>,
    pub transaction_causet_queries: Vec<CausetQuery>,
    pub transaction_causetq_queries: Vec<CausetqQuery>,
    pub transaction_gremlin_queries: Vec<GremlinQuery>,
}




#[macro_export]
macro_rules! soliton_panic {
    ($name:ident) => {
        #[derive(Debug, Clone, PartialEq, Eq, Hash)]
        pub struct $name {
            pub name: String,
            pub value: Value,
            pub ttl: i64,
        }
        impl $name {
            pub fn new(name: String, value: Value, ttl: i64) -> Self {
                $name {
                    name,
                    value,
                    ttl,
                }
            }
        }
        impl Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.name)
            }
        }
        impl Deref for $name {
            type Target = Value;
            fn deref(&self) -> &Self::Target {
                &self.value
            }
        }
        impl DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.value
            }
        }
    };
}


pub struct TxObserver {
    pub schema: Schema,
    pub transaction: Transaction,
    pub causetq_query: CausetqQuery,
    pub gremlin_query: GremlinQuery,
    pub causet_query: CausetQuery,
}


impl TxObserver {
    pub fn new(schema: Schema, transaction: Transaction) -> Self {
        let causetq_query = CausetqQueryBuilder::new(schema, transaction).build();
        let gremlin_query = GremlinQueryBuilder::new(schema, transaction).build();
        let causet_query = CausetQueryBuilder::new(schema, transaction).build();
        Self {
            schema,
            transaction,
            causetq_query,
            gremlin_query,
            causet_query,
        }
    }

    pub fn causet_query(&self) -> &CausetQuery {
        &self.causet_query
    }

    pub fn causetq_query(&self) -> CausetqQuery {
        CausetqQueryBuilderImpl::new(self).build()
    }

    pub fn gremlin_query(&self) -> GremlinQuery {
        GremlinQueryBuilderImpl::new(self).build()
    }

    pub fn gremlin_query_with_query(&self, query: &str) -> GremlinQuery {
        GremlinQueryBuilderImpl::new(self).with_query(query).build()
    }


    pub fn gremlin_query_with_query_and_params(&self, query: &str, params: &[&str]) -> GremlinQuery {
        GremlinQueryBuilderImpl::new(self).with_query_and_params(query, params).build()
    }

    pub fn notify(&self, query: &str, attributes: AttributeSet) {
        (self.notify_fn)(query, attributes);
    }

    pub fn notify_with_params(&self, query: &str, attributes: AttributeSet) {
        (self.notify_fn)(query, attributes);
    }

    pub fn notify_with_params_and_params(&self, query: &str, attributes: AttributeSet, params: IndexMap<&Causetid, &AttributeSet>, params2: IndexMap<&Causetid, &AttributeSet>) {
        (self.notify_fn)(query, attributes);
    }
}




#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttributeSet {
    pub attributes: IndexMap<&'static str, Attribute>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Attribute {
    pub value: String,
    pub field_type: FieldType,
}


pub struct CausetQueryBuilderImpl {
    observer: TxObserver,
    query: String,
    params: IndexMap<&Causetid, &AttributeSet>,
}   


impl CausetQueryBuilderImpl {
    pub fn new(observer: &TxObserver) -> Self {
        Self {
            observer: observer.clone(),
            query: String::new(),
            params: IndexMap::new(),
        }
    }
}


impl CausetQueryBuilder for CausetQueryBuilderImpl {
    fn with_query(&mut self, query: &str) -> &mut Self {
        self.query = query.to_string();
        self
    }
    fn with_params(&mut self, params: IndexMap<&Causetid, &AttributeSet>) -> &mut Self {
        self.params = params;
        self
    }
      fn build(&self) -> CausetQuery {
            CausetQuery::new(self.observer.clone(), self.query.clone(), self.params.clone())
        }
}


pub struct CausetqQueryBuilderImpl {
    observer: TxObserver,
    query: String,
    params: IndexMap<&Causetid, &AttributeSet>,
}


impl CausetqQueryBuilderImpl {
    pub fn applicable_reports<'r>(&self, reports: &'r IndexMap<Causetid, AttributeSet>) -> IndexMap<&'r Causetid, &'r AttributeSet> {
        reports.into_iter()
            .filter(|&(_txid, attrs)| !self.attributes.is_disjoint(attrs))
            .collect()
    }

    fn notify(&self, soliton_id: &str, reports: IndexMap<&Causetid, &AttributeSet>) {
        (*self.notify_fn)(soliton_id, reports);
    }

    pub fn new(observer: &TxObserver) -> Self {
        Self {
            observer: observer.clone(),
            query: String::new(),
            params: IndexMap::new(),
        }
    }

    pub fn with_query(&mut self, query: &str) -> &mut Self {
        self.query = query.to_string();
        self
    }

    pub fn with_params(&mut self, params: IndexMap<&Causetid, &AttributeSet>) -> &mut Self {
        self.params = params;
        self
    }

    pub fn build(&self) -> CausetqQuery {
        CausetqQuery::new(self.observer.clone(), self.query.clone(), self.params.clone())
    }

}


impl CausetQuery for CausetQueryBuilderImpl {
    fn build(&self) -> CausetQuery {
        CausetQueryImpl {
            observer: self.observer.clone(),
            query: self.query.clone(),
            params: self.params.clone(),
        }
    }
}


pub struct CausetQueryImpl {
    observer: TxObserver,
    query: String,
    params: IndexMap<&Causetid, &AttributeSet>,
}

pub trait Command {
    fn execute(&self, observer: &TxObserver) -> Result<()>;

}

pub struct TxCommand {
    pub command: String,
    pub params: IndexMap<&Causetid, &AttributeSet>,
}




impl Command for TxCommand {
    fn execute(&self, observer: &TxObserver) -> Result<()> {
        observer.notify(&self.command, self.params.clone());
        Ok(())
    }
}


impl CausetQueryImpl {
    pub fn execute(&self, observer: &TxObserver) -> Result<()> {
        observer.causet_query().execute()?;
        observer.causetq_query().execute()?;
        observer.gremlin_query().execute()?;
        observer.gremlin_query_with_query("g.V()").execute()?;
        observer.gremlin_query_with_query_and_params("g.V()", &["a", "b"]).execute()?;
        Ok(())
    }
}


impl TxCommand {
    pub fn new(command: String, params: IndexMap<&Causetid, &AttributeSet>, observer: TxObserver) -> Self {
        Self {
            command,
            params,

        }
    }

    pub fn with_soliton_id(&mut self, soliton_id: &str) -> &mut Self {
        self.soliton_id = soliton_id.to_string();
        self
    }

    pub fn with_reports(&mut self, reports: IndexMap<Causetid, AttributeSet>) -> &mut Self {
        self.reports = reports;
        self
    }

    pub fn with_error(&mut self, error: Error) -> &mut Self {
        self.error = Some(error);
        self
    }

    pub fn execute(&self) -> Result<()> {
        if let Some(error) = &self.error {
            return Err(error.clone());
        }
        self.observer.notify(&self.soliton_id, self.reports.clone());
        Ok(())
    }
    
    
}

impl Command for TxCommand {
    fn execute(&mut self) {
        self.observers.upgrade().map(|observers| {
            observers.notify(&self.soliton_id, self.reports.clone());
            for (soliton_id, observer) in observers.iter() {
                observer.notify(&self.soliton_id, self.reports.clone());
                let applicable_reports = observer.applicable_reports(&self.reports);
                if !applicable_reports.is_empty() {
                    observer.notify(soliton_id, applicable_reports);
                  
                }
            }
        });
    }
}

pub struct TxObservationService {
    observers: Arc<IndexMap<String, Arc<TxObserver>>>,
    interlocking_directorate: Option<Sender<Box<dyn Command + Send>>>,
}

impl TxObservationService {
    pub fn new() -> Self {
        TxObservationService {
            observers: Arc::new(IndexMap::new()),
            interlocking_directorate: None,
        }
    }

    // For testing purposes
    pub fn is_registered(&self, soliton_id: &String) -> bool {
        self.observers.contains_soliton_id(soliton_id)
    }

    pub fn register(&mut self, soliton_id: String, observer: Arc<TxObserver>) {
        Arc::make_mut(&mut self.observers).insert(soliton_id, observer);
    }

    pub fn deregister(&mut self, soliton_id: &String) {
        Arc::make_mut(&mut self.observers).remove(soliton_id);
    }

    pub fn has_observers(&self) -> bool {
        !self.observers.is_empty()
    }

    pub fn in_progress_did_commit(&mut self, txes: IndexMap<Causetid, AttributeSet>) {
        // Don't spawn a thread only to say nothing.
        if !self.has_observers() {
            return;
        }

        let interlocking_directorate = self.interlocking_directorate.get_or_insert_with(|| {
            let (tx, rx): (Sender<Box<dyn Command + Send>>, Receiver<Box<dyn Command + Send>>) = channel();
            let mut worker = CommandExecutor::new(rx);

            thread::spawn(move || {
                worker.main();
            });

            tx
        });

        let cmd = Box::new(TxCommand::new(&self.observers, txes));
        interlocking_directorate.send(cmd).unwrap();
    }
}

impl Drop for TxObservationService {
    fn drop(&mut self) {
        self.interlocking_directorate = None;
    }
}

pub struct InProgressObserverTransactWatcher {
    collected_attributes: AttributeSet,
    pub txes: IndexMap<Causetid, AttributeSet>,
}

impl InProgressObserverTransactWatcher {
    pub fn new() -> InProgressObserverTransactWatcher {
        InProgressObserverTransactWatcher {
            collected_attributes: Default::default(),
            txes: Default::default(),
        }
    }
}

impl TransactWatcher for InProgressObserverTransactWatcher {
    fn causet(&mut self, _op: OpType, _e: Causetid, a: Causetid, _v: &causetq_TV) {
        self.collected_attributes.insert(a);
    }

    fn done(&mut self, t: &Causetid, _topograph: &Topograph) -> Result<()> {
        let collected_attributes = ::std::mem::replace(&mut self.collected_attributes, Default::default());
        self.txes.insert(*t, collected_attributes);
        Ok(())
    }
}

struct CommandExecutor {
    receiver: Receiver<Box<dyn Command + Send>>,
}

impl CommandExecutor {
    fn new(rx: Receiver<Box<dyn Command + Send>>) -> Self {
        CommandExecutor {
            receiver: rx,
        }
    }

    fn main(&mut self) {
        loop {
            match self.receiver.recv() {
                Err(_RecvError) => {
                    // "The recv operation can only fail if the sending half of a channel (or
                    // sync_channel) is disconnected, implying that no further messages will ever be
                    // received."
                    // No need to log here.
                    return
                },

                Ok(mut cmd) => {
                    cmd.execute()
                },
            }
        }
    }
}
