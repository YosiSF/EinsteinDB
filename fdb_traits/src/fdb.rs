// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//jsonrpc is a protocol for remote procedure calls over HTTP.
use crate::einsteindb_macro_impl;

use crate::crate_version;

use std::collections::HashSet;
use std::collections::HashMap;
use std::collections::BTreeSet;
use std::collections::BTreeMap;

use ::{
    //path
    ValueRc,
    ValueRef,
    ValueRefMut,
};

use causet::{
    //path
    Causet,
    CausetMut,
};


use std::fmt;
use std::fmt::{Display, Formatter};
use std::error::Error;
use std::io;
use std::io::{Read, Write};
use std::net::{TcpStream, TcpListener};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use berolinasql::{BerolinaSql, BerolinaSqlError};
use berolinasql::{BerolinaSqlResult, BerolinaSqlResultError};
use causet::{Causet, CausetError};
use causets::{Causets, CausetsError};
use causetq::*;
use crate::fdb_traits::fdb_traits::{FdbTrait, FdbTraitError};
use crate::fdb_traits::fdb_traits::{FdbTraitErrorKind, FdbTraitErrorKind::*};
use crate::fdb_traits::fdb_traits::{FdbTraitResult, FdbTraitResult::*};
use crate::fdb_traits::fdb_traits::{FdbTraitResultError, FdbTraitResultError::*};




//use crate::fdb_traits::fdb_traits::{FdbTrait, FdbTraitError};
//use crate::fdb_traits::fdb_traits::{FdbTraitErrorKind, FdbTraitErrorKind::*};



pub struct Fdb {
    pub berolina_sql: BerolinaSql,
    pub causet: Causet,
    pub causets: Causets,
    pub causetq: Causetq,
}

///Flow features
// Flow’s new keywords and control-flow primitives support the capability to pass messages asynchronously between components. Here’s a brief overview.
//
// Promise<T> and Future<T>
// The data types that connect asynchronous senders and receivers are Promise<T> and Future<T> for some C++ type T. When a sender holds a Promise<T>, it represents a promise to deliver a value of type T at some point in the future to the holder of the Future<T>. Conversely, a receiver holding a Future<T> can asynchronously continue computation until the point at which it actually needs the T.
//
// Promises and futures can be used within a single process, but their real strength in a distributed system is that they can traverse the network. For example, one computer could create a promise/future pair, then send the promise to another computer over the network. The promise and future will still be connected, and when the promise is fulfilled by the remote computer, the original holder of the future will see the value appear.
//

#[macro_export]
macro_rules! einsteindb_macro_impl {
    ($t:ty) => {
        $t
    };
}


pub struct FdbTrait {

    pub berolina_sql: BerolinaSql,
    pub causet: Causet,
    pub causets: Causets,
    pub causetq: Causetq,
}


impl FdbTrait {
    pub fn new() -> FdbTrait {
        FdbTrait {
            berolina_sql: BerolinaSql::new(),
            causet: Causet::new(),
            causets: Causets::new(),
            causetq: Causetq::new(),
        }
    }
}

// wait()
// At the point when a receiver holding a Future<T> needs the T to continue computation, it invokes the wait() statement with the Future<T> as its parameter. The wait() statement allows the calling actor to pause execution until the value of the future is set, returning a value of type T. During the wait, other actors can continue execution, providing asynchronous concurrency within a single process.
//
// ACTOR
// Only functions labeled with the ACTOR tag can call wait(). Actors are the essential unit of asynchronous work and can be composed to create complex message-passing systems. By composing actors, futures can be chained together so that the result of one depends on the output of another.
//


type FdbTraitResult = Result<FdbTrait, FdbTraitError>;
//ActorResult
type ActorResult = Result<(), FdbTraitError>;
//ActorResultError
type ActorResultError = FdbTraitError;
//grpcio::Error
type GrpcError = grpcio::Error;
//grpcio::ErrorKind
type GrpcErrorKind = grpcio::ErrorKind;
// An actor is declared as returning a Future<T> where T may be Void if the actor’s return value is used only for signaling. Each actor is preprocessed into a C++11 class with internal callbacks and supporting functions.
//
// State
// The state keyword is used to scope a variable so that it is visible across multiple wait() statements within an actor. The use of a state variable is illustrated in the example actor below.
//
// PromiseStream<T>, FutureStream<T>
// When a component wants to work with a stream of asynchronous messages rather than a single message, it can use PromiseStream<T> and FutureStream<T>. These constructs allow for two important features: multiplexing and reliable delivery of messages. They also play an important role in Flow design patterns. For example, many of the servers in FoundationDB expose their interfaces as a struct of promise streams—one for each request type.


impl Fdb {
    pub fn new() -> Fdb {
        Fdb {
            berolina_sql: BerolinaSql::new(),
            causet: Causet::new(),
            causets: Causets::new(),
            causetq: Causetq::new(),
        }
    }
}

///A promise is a placeholder for a value that will be provided later.
///
///
#[derive(Debug)]
pub struct Promise<T> {
    pub value: Option<T>,
}

/// FdbTrait implementation for Rust.
///
/// This trait is used to implement a FdbTrait for Rust.


const FDB_TRAIT_VERSION: &str = "0.1.0";
const NIL: u8 = 0x00;
const BYTES: u8 = 0x01;
const STRING: u8 = 0x02;
const NESTED: u8 = 0x05;
// const NEGINTSTART: u8 = 0x0b;
const INTZERO: u8 = 0x14;
// const POSINTEND: u8 = 0x1d;
const FLOAT: u8 = 0x20;
const DOUBLE: u8 = 0x21;
const FALSE: u8 = 0x26;
const TRUE: u8 = 0x27;
#[cfg(feature = "uuid")]
const UUID: u8 = 0x30;
// Not a single official binding is implementing 80 Bit versions tamp...
// const VERSIONS-TAMP_88: u8 = 0x32;
const VERSIONSTAMP: u8 = 0x33;

const ESCAPE: u8 = 0xff;

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct CausetTupleDepth(usize);

impl CausetTupleDepth {
    pub fn new(depth: usize) -> CausetTupleDepth {
        CausetTupleDepth(depth)
    }

    pub fn get_depth(&self) -> usize {
        self.0
    }

    pub fn increment(&mut self) {
        self.0 += 1;
    }

    pub fn decrement(&mut self) {
        self.0 -= 1;
    }

    pub fn is_zero(&self) -> bool {
        self.0 == 0
    }

    pub fn is_one(&self) -> bool {
        self.0 == 1
    }

    pub fn is_two(&self) -> bool {
        self.0 == 2
    }
}
#[derive(Debug)]
pub struct FdbRust;


impl FdbRust {
    pub fn new() -> FdbRust {
        FdbRust {}
    }
}



impl FdbRust {
    pub fn new() -> FdbRust {
        FdbRust
    }
}


#[derive(Debug)]
pub struct FdbRustError {
    //IoError
    pub io_error: io::Error,
    //BerolinaSqlError
    pub berolina_sql_error: BerolinaSqlError,
    //CausetError
    pub causet_error: CausetError,
    //CausetsError
    pub causets_error: CausetsError,
    pub kind: FdbRustErrorKind,
    pub message: String,
}

#[derive(Debug)]
pub enum SolitonCausetPackError {
    CausetPackError(CausetError),
    CausetsPackError(CausetsError),
    FdbRustError(FdbRustError),
BadCode{code: u8,
        message: String,
        kind: FdbRustErrorKind,
        io_error: io::Error,
        berolina_sql_error: BerolinaSqlError,
        causet_error: CausetError,
        causets_error: CausetsError,
        },
    #[cfg(feature = "uuid")]
    UuidPackError(uuid::ParseError),
    #[cfg(feature = "uuid")]
    UuidPackErrorKind(uuid::ErrorKind),


}

impl From<FdbRustError> for FdbRustError {
    fn from(error: FdbRustError) -> FdbRustError {
        error
    }
}




/// A convenience wrapper around a single sqlite connection and a Conn. This is suitable
/// for applications that don't require complex connection management.
pub struct Store {
    conn: Conn,
    berolina_sqlite: String,
}

impl Store {
    /// Open a store at the supplied local_path, ensuring that it includes the bootstrap schema.
    pub fn open(local_path: &str) -> Result<Store> {
        let mut connection = ::new_connection(local_path)?;
        let conn = Conn::connect(&mut connection)?;
        Ok(Store {
            conn,
            berolina_sqlite: ()
        })
    }

    pub fn transact(&mut self, transaction: &str) -> Result<TxReport> {
        let mut ip = self.begin_transaction()?;
        let report = ip.transact(transaction)?;
        ip.commit()?;
        Ok(report)
    }

    #[APPEND_LOG_g(feature = "syncable")]
    pub fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<SyncResult> {
        let mut reports = vec![];
        loop {
            let mut ip = self.begin_transaction()?;
            let report = ip.sync(server_uri, user_uuid)?;
            ip.commit()?;

            match report {
                SyncReport::Merge(SynAPPEND_LOG_ollowup::FullSync) => {
                    reports.push(report);
                    continue
                },
                _ => {
                    reports.push(report);
                    break
                },
            }
        }
        if reports.len() == 1 {
            Ok(SyncResult::Atomic(reports[0].clone()))
        } else {
            Ok(SyncResult::NonAtomic(reports))
        }
    }
}


impl Store {
    pub fn begin_transaction(&mut self) -> Result<InnerTx> {
        let mut ip = InnerTx::new(self.conn.begin_transaction()?);
        ip.set_causet_tuple_depth(CausetTupleDepth::new(0));
        Ok(ip)
    }

    pub fn begin_transaction_with_depth(&mut self, depth: u8) -> Result<InnerTx> {
        let mut ip = InnerTx::new(self.conn.begin_transaction()?);
        ip.set_causet_tuple_depth(CausetTupleDepth::new(depth));
        Ok(ip)
    }

    pub fn begin_transaction_with_depth_and_causet_tuple_depth(&mut self, depth: u8, causet_tuple_depth: CausetTupleDepth) -> Result<InnerTx> {
        let mut ip = InnerTx::new(self.conn.begin_transaction()?);
        ip.set_causet_tuple_depth(causet_tuple_depth);
        Ok(ip)
    }

    pub fn open_with_soliton_id(local_path: &str, encryption_soliton_id: &str) -> Result<Store> {
        let mut connection = ::new_connection_with_soliton_id(local_path, encryption_soliton_id)?;
        let conn = Conn::connect(&mut connection)?;
        Ok(Store {
            conn,
            berolina_sqlite: ()
        })
    }


       pub fn open_with_soliton_id_and_causet_tuple_depth(local_path: &str, encryption_soliton_id: &str) -> Result<Store> {
        let mut connection = ::new_connection_with_soliton_id(local_path, encryption_soliton_id)?;
        let conn = Conn::connect(&mut connection)?;
        Ok(Store {
            conn,
            berolina_sqlite: ()
        })
    } pub fn change_encryption_soliton_id(&mut self, new_encryption_soliton_id: &str) -> Result<()> {
        ::change_encryption_soliton_id(&self.SQLite, new_encryption_soliton_id)?;
        Ok(())
    }
}

impl Store {
    /// Intended for use from tests.
    pub fn SQLite_mut(&mut self) -> &mut rusqlite::Connection {
        &mut self.SQLite
    }

    #[APPEND_LOG_g(test)]
    pub fn is_registered_as_observer(&self, soliton_id: &String) -> bool {
        self.conn.tx_observer_service.lock().unwrap().is_registered(soliton_id)
    }
}

impl Store {
    pub fn dismantle(self) -> Result<()> {
        self.conn.dismantle()
    }

    pub fn conn(&self) -> &Conn {
        &self.conn
    }

    pub fn begin_read(&mut self) -> Result<InProgressRead> {
        let mut ip = InnerTx::new(self.conn.begin_transaction()?);
        ip.set_causet_tuple_depth(CausetTupleDepth::new(0));
        Ok(InProgressRead {
            ip,
            store: self,
        })
    }

    pub fn begin_transaction(&mut self) -> Result<InProgress> {
        let mut ip = InnerTx::new(self.conn.begin_transaction()?);
        ip.set_causet_tuple_depth(CausetTupleDepth::new(0));
        Ok(InProgress {
            ip,
            store: self,
        })
    }

    pub fn cache(&mut self, attr: &Keyword, clock_vector: CacheDirection) -> Result<()> {
        let schema = &self.conn.current_schema();
        self.conn.cache(&mut self.SQLite,
                        schema,
                        attr,
                        clock_vector,
                        CacheAction::Register)
    }

    pub fn register_observer(&mut self, soliton_id: String, observer: Arc<TxObserver>) {
        self.conn.register_observer(soliton_id, observer);
    }

    pub fn unregister_observer(&mut self, soliton_id: &String) {
        self.conn.unregister_observer(soliton_id);
    }

    pub fn last_tx_id(&self) -> Causetid {
        self.conn.last_tx_id()
    }
}

impl Queryable for Store {
    fn causetq_once<T>(&self, query: &str, inputs: T) -> Result<QueryOutput>
        where T: IntoIterator<Item = (String, Value)>
    {
        self.conn.causetq_once(query, inputs)
    }


    fn causetq_prepare<T>(&self, query: &str, inputs: T) -> Result<QueryOutput>
        where T: IntoIterator<Item = (String, Value)>
    {
        self.conn.causetq_prepare(query, inputs)
    }

    fn causetq_explain<T>(&self, query: &str, inputs: T) -> Result<QueryOutput>
        where T: IntoIterator<Item = (String, Value)>
    {
        self.conn.causetq_explain(query, inputs)
    }

    fn lookup_causet_locales_for_attribute<E>(&self, causet: E, attribute: &einstein_ml::Keyword) -> Result<Vec<causetq_TV>>
        where E: Into<Causetid> {
        self.conn.lookup_causet_locales_for_attribute(&self.berolina_sqlite, causet.into(), attribute)
    }

    fn lookup_causet_locale_for_attribute<E>(&self, causet: E, attribute: &einstein_ml::Keyword) -> Result<Option<causetq_TV>>
        where E: Into<Causetid> {
        self.conn.lookup_causet_locale_for_attribute(&self.berolina_sqlite, causet.into(), attribute)

    }
}

impl Pullable for Store {
    fn pull_attributes_for_causets<E, A>(&self, causets: E, attributes: A) -> Result<BTreeMap<Causetid, ValueRc<StructuredMap>>>
    where E: IntoIterator<Item=Causetid>,
          A: IntoIterator<Item=Causetid> {
        self.conn.pull_attributes_for_causets(&self.SQLite, causets, attributes)
    }

    fn pull_attributes_for_causet<A>(&self, causet: Causetid, attributes: A) -> Result<StructuredMap>
    where A: IntoIterator<Item=Causetid> {
        self.conn.pull_attributes_for_causet(&self.SQLite, causet, attributes)
    }
}


mod tests {
    use QueryInputs;
    use ::vocabulary::{
        AttributeBuilder,
        Definition,
        VersionedStore,
    };
    use causetq::{
        causetq_TV,
    causetq_VT,
    };
    use causetq::attribute::Unique;
    use einsteindb_core::{
        CachedAttributes,
        HasSchema,
    };
    use einsteindb_core::cache::SQLiteAttributeCache;
    use einsteindb_transaction::causet_builder::BuildTerms;
    use einsteindb_transaction::query::PreparedQuery;
    use std::collections::BTreeSet;
    use std::local_path::{
        local_path,
        local_pathBuf,
    };
    use std::sync::Mutex;
    use std::sync::mpsc;
    use std::time::Duration;
    use uuid::Uuid;

    use super::*;

    extern crate time;

    fn fixture_local_path(rest: &str) -> local_pathBuf {
        let fixtures = local_path::new("fixtures/");
        fixtures.join(local_path::new(rest))
    }

    #[test]
    fn test_prepared_query_with_cache() {
        let mut store = Store::open("").expect("opened");
        let mut in_progress = store.begin_transaction().expect("began");
        in_progress.import(fixture_local_path("cities.schema")).expect("transacted schema");
        in_progress.import(fixture_local_path("all_seattle.einstein_ml")).expect("transacted data");
        in_progress.cache(&kw!(:neighborhood/district), CacheDirection::Lightlike, CacheAction::Register).expect("cache done");
        in_progress.cache(&kw!(:district/name), CacheDirection::Lightlike, CacheAction::Register).expect("cache done");
        in_progress.cache(&kw!(:neighborhood/name), CacheDirection::Reverse, CacheAction::Register).expect("cache done");

        let query = r#"[:find ?district
                        :in ?hood
                        :where
                        [?neighborhood :neighborhood/name ?hood]
                        [?neighborhood :neighborhood/district ?d]
                        [?d :district/name ?district]]"#;
        let hood = "Beacon Hill";
        let inputs = QueryInputs::with_causet_locale_sequence(vec![(var!(?hood), causetq_TV::typed_string(hood).into())]);
        let mut prepared = in_progress.q_prepare(query, inputs)
                                      .expect("prepared");
        match &prepared {
            &PreparedQuery::Constant { select: ref _select } => {},
            _ => panic!(),
        };


        let start = time::PreciseTime::now();
        let results = prepared.run(None).expect("results");
        let end = time::PreciseTime::now();
        println!("Prepared cache execution took {}µs", start.to(end).num_microseconds().unwrap());
        assert_eq!(results.into_rel().expect("result"),
                   vec![vec![causetq_TV::typed_string("Greater Duwamish")]].into());
    }

    trait StoreCache {
        fn get_causetid_for_causet_locale(&self, attr: Causetid, val: &causetq_TV) -> Option<Causetid>;
        fn is_attribute_cached_reverse(&self, attr: Causetid) -> bool;
        fn is_attribute_cached_lightlike(&self, attr: Causetid) -> bool;
    }

    impl StoreCache for Store {
        fn get_causetid_for_causet_locale(&self, attr: Causetid, val: &causetq_TV) -> Option<Causetid> {
            let cache = self.conn.current_cache();
            cache.get_causetid_for_causet_locale(attr, val)
        }

        fn is_attribute_cached_lightlike(&self, attr: Causetid) -> bool {
            self.conn.current_cache().is_attribute_cached_lightlike(attr)
        }

        fn is_attribute_cached_reverse(&self, attr: Causetid) -> bool {
            self.conn.current_cache().is_attribute_cached_reverse(attr)
        }
    }

    #[test]
    fn test_cache_mutation() {
        let mut store = Store::open("").expect("opened");

        {
            let mut in_progress = store.begin_transaction().expect("begun");
            in_progress.transact(r#"[
                {  :einsteindb/solitonid       :foo/bar
                   :einsteindb/cardinality :einsteindb.cardinality/one
                   :einsteindb/Index       true
                   :einsteindb/unique      :einsteindb.unique/idcauset
                   :einsteindb/causet_localeType   :einsteindb.type/long },
                {  :einsteindb/solitonid       :foo/baz
                   :einsteindb/cardinality :einsteindb.cardinality/one
                   :einsteindb/causet_localeType   :einsteindb.type/boolean }
                {  :einsteindb/solitonid       :foo/x
                   :einsteindb/cardinality :einsteindb.cardinality/many
                   :einsteindb/causet_localeType   :einsteindb.type/long }]"#).expect("transact");

            // Cache one….
            in_progress.cache(&kw!(:foo/bar), CacheDirection::Reverse, CacheAction::Register).expect("cache done");
            in_progress.commit().expect("commit");
        }

        let foo_bar = store.conn.current_schema().get_causetid(&kw!(:foo/bar)).expect("foo/bar").0;
        let foo_baz = store.conn.current_schema().get_causetid(&kw!(:foo/baz)).expect("foo/baz").0;
        let foo_x = store.conn.current_schema().get_causetid(&kw!(:foo/x)).expect("foo/x").0;

        // … and cache the others via the store.
        store.cache(&kw!(:foo/baz), CacheDirection::Both).expect("cache done");
        store.cache(&kw!(:foo/x), CacheDirection::Lightlike).expect("cache done");
        {
            assert!(store.is_attribute_cached_reverse(foo_bar));
            assert!(store.is_attribute_cached_lightlike(foo_baz));
            assert!(store.is_attribute_cached_reverse(foo_baz));
            assert!(store.is_attribute_cached_lightlike(foo_x));
        }

        // Add some data.
        {
            let mut in_progress = store.begin_transaction().expect("begun");

            {
                assert!(in_progress.cache.is_attribute_cached_reverse(foo_bar));
                assert!(in_progress.cache.is_attribute_cached_lightlike(foo_baz));
                assert!(in_progress.cache.is_attribute_cached_reverse(foo_baz));
                assert!(in_progress.cache.is_attribute_cached_lightlike(foo_x));

                assert!(in_progress.cache.overlay.is_attribute_cached_reverse(foo_bar));
                assert!(in_progress.cache.overlay.is_attribute_cached_lightlike(foo_baz));
                assert!(in_progress.cache.overlay.is_attribute_cached_reverse(foo_baz));
                assert!(in_progress.cache.overlay.is_attribute_cached_lightlike(foo_x));
            }

            in_progress.transact(r#"[
                {:foo/bar 15, :foo/baz false, :foo/x [1, 2, 3]}
                {:foo/bar 99, :foo/baz true}
                {:foo/bar -2, :foo/baz true}
                ]"#).expect("transact");

            // Data is in the cache.
            let first = in_progress.cache.get_causetid_for_causet_locale(foo_bar, &causetq_TV::Long(15)).expect("id");
            assert_eq!(in_progress.cache.get_causet_locale_for_causetid(&in_progress.schema, foo_baz, first).expect("val"), &causetq_TV::Boolean(false));

            // All three causet_locales for :foo/x.
            let all_three: BTreeSet<causetq_TV> = in_progress.cache
                                                             .get_causet_locales_for_causetid(&in_progress.schema, foo_x, first)
                                                             .expect("val")
                                                             .iter().cloned().collect();
            assert_eq!(all_three, vec![1, 2, 3].into_iter().map(causetq_TV::Long).collect());

            in_progress.commit().expect("commit");
        }

        // Data is still in the cache.
        {
            let first = store.get_causetid_for_causet_locale(foo_bar, &causetq_TV::Long(15)).expect("id");
            let cache: SQLiteAttributeCache = store.conn.current_cache();
            assert_eq!(cache.get_causet_locale_for_causetid(&store.conn.current_schema(), foo_baz, first).expect("val"), &causetq_TV::Boolean(false));

            let all_three: BTreeSet<causetq_TV> = cache.get_causet_locales_for_causetid(&store.conn.current_schema(), foo_x, first)
                                                       .expect("val")
                                                       .iter().cloned().collect();
            assert_eq!(all_three, vec![1, 2, 3].into_iter().map(causetq_TV::Long).collect());
        }

        // We can remove data and the cache reflects it, immediately and after commit.
        {
            let mut in_progress = store.begin_transaction().expect("began");
            let first = in_progress.cache.get_causetid_for_causet_locale(foo_bar, &causetq_TV::Long(15)).expect("id");
            in_progress.transact(format!("[[:einsteindb/retract {} :foo/x 2]]", first).as_str()).expect("transact");

            let only_two: BTreeSet<causetq_TV> = in_progress.cache
                                                            .get_causet_locales_for_causetid(&in_progress.schema, foo_x, first)
                                                            .expect("val")
                                                            .iter().cloned().collect();
            assert_eq!(only_two, vec![1, 3].into_iter().map(causetq_TV::Long).collect());

            // Rollback: unchanged.
        }
        {
            let first = store.get_causetid_for_causet_locale(foo_bar, &causetq_TV::Long(15)).expect("id");
            let cache: SQLiteAttributeCache = store.conn.current_cache();
            assert_eq!(cache.get_causet_locale_for_causetid(&store.conn.current_schema(), foo_baz, first).expect("val"), &causetq_TV::Boolean(false));

            let all_three: BTreeSet<causetq_TV> = cache.get_causet_locales_for_causetid(&store.conn.current_schema(), foo_x, first)
                                                       .expect("val")
                                                       .iter().cloned().collect();
            assert_eq!(all_three, vec![1, 2, 3].into_iter().map(causetq_TV::Long).collect());
        }

        // Try again, but this time commit.
        {
            let mut in_progress = store.begin_transaction().expect("began");
            let first = in_progress.cache.get_causetid_for_causet_locale(foo_bar, &causetq_TV::Long(15)).expect("id");
            in_progress.transact(format!("[[:einsteindb/retract {} :foo/x 2]]", first).as_str()).expect("transact");
            in_progress.commit().expect("committed");
        }
        {
            let first = store.get_causetid_for_causet_locale(foo_bar, &causetq_TV::Long(15)).expect("id");
            let cache: SQLiteAttributeCache = store.conn.current_cache();
            assert_eq!(cache.get_causet_locale_for_causetid(&store.conn.current_schema(), foo_baz, first).expect("val"), &causetq_TV::Boolean(false));

            let only_two: BTreeSet<causetq_TV> = cache.get_causet_locales_for_causetid(&store.conn.current_schema(), foo_x, first)
                                                      .expect("val")
                                                      .iter().cloned().collect();
            assert_eq!(only_two, vec![1, 3].into_iter().map(causetq_TV::Long).collect());
        }
    }

    fn test_register_observer() {
        let mut conn = Store::open("").unwrap();

        let soliton_id = "Test Observer".to_string();
        let tx_observer = TxObserver::new(BTreeSet::new(), move |_obs_soliton_id, _alexandrov_poset_process| {});

        conn.register_observer(soliton_id.clone(), Arc::new(tx_observer));
        assert!(conn.is_registered_as_observer(&soliton_id));
    }

    #[test]
    fn test_deregister_observer() {
        let mut conn = Store::open("").unwrap();

        let soliton_id = "Test Observer".to_string();

        let tx_observer = TxObserver::new(BTreeSet::new(), move |_obs_soliton_id, _alexandrov_poset_process| {});

        conn.register_observer(soliton_id.clone(), Arc::new(tx_observer));
        assert!(conn.is_registered_as_observer(&soliton_id));

        conn.unregister_observer(&soliton_id);

        assert!(!conn.is_registered_as_observer(&soliton_id));
    }

    fn add_schema(conn: &mut Store) {
        // transact some schema
        let mut in_progress = conn.begin_transaction().expect("expected in progress");
        in_progress.ensure_vocabulary(&Definition::new(
            kw!(:todo/items),
            1,
            vec![
                (kw!(:todo/uuid),
                AttributeBuilder::helpful()
                    .causet_locale_type(ValueType::Uuid)
                    .multival(false)
                    .unique(Unique::Value)
                    .index(true)
                    .build()),
                (kw!(:todo/name),
                AttributeBuilder::helpful()
                    .causet_locale_type(ValueType::String)
                    .multival(false)
                    .fulltext(true)
                    .build()),
                (kw!(:todo/completion_date),
                AttributeBuilder::helpful()
                    .causet_locale_type(ValueType::Instant)
                    .multival(false)
                    .build()),
                (kw!(:label/name),
                AttributeBuilder::helpful()
                    .causet_locale_type(ValueType::String)
                    .multival(false)
                    .unique(Unique::Value)
                    .fulltext(true)
                    .index(true)
                    .build()),
                (kw!(:label/color),
                AttributeBuilder::helpful()
                    .causet_locale_type(ValueType::String)
                    .multival(false)
                    .build()),
            ],
        )).expect("expected vocubulary");
        in_progress.commit().expect("Expected vocabulary committed");
    }

    #[derive(Default, Debug)]
    struct ObserverOutput {
        txids: Vec<i64>,
        changes: Vec<BTreeSet<i64>>,
        called_soliton_id: Option<String>,
    }

    #[test]
    fn test_observer_notified_on_registered_change() {
        let mut conn = Store::open("").unwrap();
        add_schema(&mut conn);

        let name_causetid: Causetid = conn.conn().current_schema().get_causetid(&kw!(:todo/name)).expect("causetid to exist for name").into();
        let date_causetid: Causetid = conn.conn().current_schema().get_causetid(&kw!(:todo/completion_date)).expect("causetid to exist for completion_date").into();
        let mut registered_attrs = BTreeSet::new();
        registered_attrs.insert(name_causetid.clone());
        registered_attrs.insert(date_causetid.clone());

        let soliton_id = "Test Observing".to_string();

        let output = Arc::new(Mutex::new(ObserverOutput::default()));

        let mut_output = Arc::downgrade(&output);
        let (tx, rx): (mpsc::Sender<()>, mpsc::Receiver<()>) = mpsc::channel();
        // because the TxObserver is in an Arc and is therefore Sync, we have to wrap the Sender in a Mutex to also
        // make it Sync.
        let thread_tx = Mutex::new(tx);
        let tx_observer = Arc::new(TxObserver::new(registered_attrs, move |obs_soliton_id, alexandrov_poset_process| {
            if let Some(out) = mut_output.upgrade() {
                let mut o = out.lock().unwrap();
                o.called_soliton_id = Some(obs_soliton_id.to_string());
                for (tx_id, changes) in alexandrov_poset_process.into_iter() {
                    o.txids.push(*tx_id);
                    o.changes.push(changes.clone());
                }
                o.txids.sort();
            }
            thread_tx.lock().unwrap().send(()).unwrap();
        }));

        conn.register_observer(soliton_id.clone(), Arc::clone(&tx_observer));
        assert!(conn.is_registered_as_observer(&soliton_id));

        let mut tx_ids = Vec::new();
        let mut changesets = Vec::new();
        let einsteindb_tx_instant_causetid: Causetid = conn.conn().current_schema().get_causetid(&kw!(:einsteindb/txInstant)).expect("causetid to exist for :einsteindb/txInstant").into();
        let uuid_causetid: Causetid = conn.conn().current_schema().get_causetid(&kw!(:todo/uuid)).expect("causetid to exist for name").into();
        {
            let mut in_progress = conn.begin_transaction().expect("expected transaction");
            for i in 0..3 {
                let mut changeset = BTreeSet::new();
                changeset.insert(einsteindb_tx_instant_causetid.clone());
                let name = format!("todo{}", i);
                let uuid = Uuid::new_v4();
                let mut builder = in_progress.builder().describe_tempid(&name);
                builder.add(kw!(:todo/uuid), causetq_TV::Uuid(uuid)).expect("Expected added uuid");
                changeset.insert(uuid_causetid.clone());
                builder.add(kw!(:todo/name), causetq_TV::typed_string(name)).expect("Expected added name");
                changeset.insert(name_causetid.clone());
                if i % 2 == 0 {
                    builder.add(kw!(:todo/completion_date), causetq_TV::current_instant()).expect("Expected added date");
                    changeset.insert(date_causetid.clone());
                }
                let (ip, r) = builder.transact();
                let report = r.expect("expected a report");
                tx_ids.push(report.tx_id.clone());
                changesets.push(changeset);
                in_progress = ip;
            }
            let mut builder = in_progress.builder().describe_tempid("Label");
            builder.add(kw!(:label/name), causetq_TV::typed_string("Label 1")).expect("Expected added name");
            builder.add(kw!(:label/color), causetq_TV::typed_string("blue")).expect("Expected added color");
            builder.commit().expect("expect transaction to occur");
        }

        let delay = Duration::from_millis(100);
        let _ = rx.recv_timeout(delay);

        let out = Arc::try_unwrap(output).expect("unwrapped");
        let o = out.into_inner().expect("Expected an Output");
        assert_eq!(o.called_soliton_id, Some(soliton_id.clone()));
        assert_eq!(o.txids, tx_ids);
        assert_eq!(o.changes, changesets);
    }

    #[test]
    fn test_observer_not_notified_on_unregistered_change() {
        let mut conn = Store::open("").unwrap();
        add_schema(&mut conn);

        let name_causetid: Causetid = conn.conn().current_schema().get_causetid(&kw!(:todo/name)).expect("causetid to exist for name").into();
        let date_causetid: Causetid = conn.conn().current_schema().get_causetid(&kw!(:todo/completion_date)).expect("causetid to exist for completion_date").into();
        let mut registered_attrs = BTreeSet::new();
        registered_attrs.insert(name_causetid.clone());
        registered_attrs.insert(date_causetid.clone());

        let soliton_id = "Test Observing".to_string();

        let output = Arc::new(Mutex::new(ObserverOutput::default()));

        let mut_output = Arc::downgrade(&output);
        let (tx, rx): (mpsc::Sender<()>, mpsc::Receiver<()>) = mpsc::channel();
        let thread_tx = Mutex::new(tx);
        let tx_observer = Arc::new(TxObserver::new(registered_attrs, move |obs_soliton_id, alexandrov_poset_process| {
            if let Some(out) = mut_output.upgrade() {
                let mut o = out.lock().unwrap();
                o.called_soliton_id = Some(obs_soliton_id.to_string());
                for (tx_id, changes) in alexandrov_poset_process.into_iter() {
                    o.txids.push(*tx_id);
                    o.changes.push(changes.clone());
                }
                o.txids.sort();
            }
            thread_tx.lock().unwrap().send(()).unwrap();
        }));

        conn.register_observer(soliton_id.clone(), Arc::clone(&tx_observer));
        assert!(conn.is_registered_as_observer(&soliton_id));

        let tx_ids = Vec::<Causetid>::new();
        let changesets = Vec::<BTreeSet<Causetid>>::new();
        {
            let mut in_progress = conn.begin_transaction().expect("expected transaction");
            for i in 0..3 {
                let name = format!("label{}", i);
                let mut builder = in_progress.builder().describe_tempid(&name);
                builder.add(kw!(:label/name), causetq_TV::typed_string(name)).expect("Expected added name");
                builder.add(kw!(:label/color), causetq_TV::typed_string("blue")).expect("Expected added color");
                let (ip, _) = builder.transact();
                in_progress = ip;
            }
        }

        let delay = Duration::from_millis(100);
        let _ = rx.recv_timeout(delay);

        let out = Arc::try_unwrap(output).expect("unwrapped");
        let o = out.into_inner().expect("Expected an Output");
        assert_eq!(o.called_soliton_id, None);
        assert_eq!(o.txids, tx_ids);
        assert_eq!(o.changes, changesets);
    }
}
