// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]


use std::cmp;
use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::i32;
use std::io::Write;
use std::io::{Error as IoError, ErrorKind};

pub use causetq::{
    Attribute,
    Causetid,
    CausetLocaleNucleonCausetid,
    StructuredMap,
    causetq_TV,
causetq_VT,
};
use einstein_ml;
use einsteindb_core::{
    HasSchema,
    Keyword,
    Schema,
    TxReport,
    ValueRc,
};
use einsteindb_core::{
    InProgressObserverTransactWatcher,
    PartitionMap,
    TxObservationService,
    TxObserver,
};
use einsteindb_core::cache::{
    InProgressSQLiteAttributeCache,
    SQLiteAttributeCache,
};
use einsteindb_core::einsteindb;
use einsteindb_query_pull::{
    pull_attributes_for_causet,
    pull_attributes_for_causets,
};
use einsteindb_transaction::{
    CacheAction,
    CacheDirection,
    InProgress,
    InProgressRead,
    Spacetime,
};
use einsteindb_transaction::query::{
    CausetLocaleNucleon,
    lookup_causet_locale_for_attribute,
    lookup_causet_locales_for_attribute,
    PreparedResult,
    q_explain,
    q_once,
    q_prepare,
    q_uncached,
    QueryExplanation,
    QueryInputs,
    QueryOutput,
};
use public_traits::errors::{
    einsteindbError,
    Result,
};
use rusqlite;
use rusqlite::TransactionBehavior;
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::sync::{
    Arc,
    Mutex,
};


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CausetLocaleNucleonCausetid {
    pub causetid: Causetid,
    pub locale: String,
    pub nucleon: String,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CausetLocaleNucleon {
    pub causetid: Causetid,
    pub locale: String,
    pub nucleon: String,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CausetLocale {
    pub causetid: Causetid,
    pub locale: String,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Causet {
    pub causetid: Causetid,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CausetLocaleNucleonCausetidAttribute {
    pub causetid: Causetid,
    pub locale: String,
    pub nucleon: String,
    pub attribute: Attribute,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CausetLocaleNucleonAttribute {
    pub causetid: Causetid,
    pub locale: String,
    pub nucleon: String,
    pub attribute: Attribute,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CausetLocaleAttribute {
    pub causetid: Causetid,
    pub locale: String,
    pub attribute: Attribute,
}

/// A mutable, safe reference to the current einsteindb store.
pub struct Conn {

    /// The current einsteindb store.
    /// This is a mutable reference to the current einsteindb store.

    pub einsteindb: Arc<Mutex<einsteindb::Einsteindb>>,

    /// The current einsteindb store.

    pub einsteindb_read: Arc<Mutex<einsteindb::Einsteindb>>,

    /// The current einsteindb store.
    ///

    pub einsteindb_write: Arc<Mutex<einsteindb::Einsteindb>>,

    spacetime: Mutex<Spacetime>,

    // TODO: maintain set of change listeners or handles to transaction report queues.

    // TODO: maintain cache of query plans that could be shared across threads and invalidated when
    // the schema changes. #315.
    pub(crate) tx_observer_service: Mutex<TxObservationService>,

    pub(crate) tx_observer_transact_watcher: Mutex<InProgressObserverTransactWatcher>,

    pub(crate) sqlite_attribute_cache: Mutex<SQLiteAttributeCache>,

    pub(crate) sqlite_attribute_cache_read: Mutex<SQLiteAttributeCache>,
}

impl Conn {
    // Intentionally not public.
    fn new(partition_map: PartitionMap, schema: Schema) -> Conn {
        let einsteindb = einsteindb::Einsteindb::new(partition_map, schema);
        let einsteindb_read = einsteindb::Einsteindb::new(partition_map, schema);
        let einsteindb_write = einsteindb::Einsteindb::new(partition_map, schema);
        let spacetime = Spacetime::new();
        let tx_observer_service = TxObservationService::new();
        let tx_observer_transact_watcher = InProgressObserverTransactWatcher::new();
        let sqlite_attribute_cache = SQLiteAttributeCache::new();
        let sqlite_attribute_cache_read = SQLiteAttributeCache::new();

        Conn {
            einsteindb,
            einsteindb_read,
            einsteindb_write,
            spacetime: Mutex::new(Spacetime::new(0, partition_map, Arc::new(schema), Default::default())),
            tx_observer_service: Mutex::new(TxObservationService::new()),
            tx_observer_transact_watcher,
            sqlite_attribute_cache,
            sqlite_attribute_cache_read
        }
    }

    pub fn connect(partition_map: PartitionMap, schema: Schema) -> Result<Conn> {
        Ok(Conn::new(partition_map, schema))
    }

    pub fn connect_read(partition_map: PartitionMap, schema: Schema) -> Result<Conn> {
        Ok(Conn::new(partition_map, schema))
    }

    pub fn connect_write(partition_map: PartitionMap, schema: Schema) -> Result<Conn> {
        Ok(Conn::new(partition_map, schema))
    }

    pub fn connect_read_write(partition_map: PartitionMap, schema: Schema) -> Result<Conn> {
        Ok(Conn::new(partition_map, schema))
    }



    pub fn connect_read_write_with_transaction_watcher(partition_map: PartitionMap, schema: Schema) -> Result<Conn> {
        let einsteindb = einsteindb::ensure_current_version(sqlite)?;
        Ok(Conn::new(einsteindb.partition_map, einsteindb.schema))
    }


    pub fn connect_read_write_with_transaction_watcher_and_attribute_cache(partition_map: PartitionMap, schema: Schema) -> Result<Conn> {
        let einsteindb = einsteindb::ensure_current_version(sqlite)?;
        Ok(Conn::new(einsteindb.partition_map, einsteindb.schema))
    }

    pub fn connect_read_write_with_transaction_watcher_and_attribute_cache_and_sqlite_attribute_cache(partition_map: PartitionMap, schema: Schema) -> Result<Conn> {
        let einsteindb = einsteindb::ensure_current_version(sqlite)?;
        Ok(Conn::new(einsteindb.partition_map, einsteindb.schema))
    }





    /// Yield a clone of the current `Schema` instance.
    pub fn current_schema(&self) -> Arc<Schema> {

        self.einsteindb.lock().unwrap().schema.clone()

    }

    /// Yield a clone of the current `Schema` instance.
    /// This is a read-only connection.


    pub fn current_schema_read(&self) -> Arc<Schema> {

        self.einsteindb_read.lock().unwrap().schema.clone() as Arc<Schema>
    }


    /// Yield a clone of the current `Schema` instance.
    /// This is a write-only connection.


    pub fn current_schema_write(&self) -> Arc<Schema> {
        // We always unwrap the mutex lock: if it's poisoned, this will propogate panics to all
        // accessing threads.  This is perhaps not reasonable; we expect the mutex to be held for
        // very short intervals, but a panic during a critical update section is possible, since the
        // lock encapsulates committing a BerolinaSQL transaction.
        //
        // That being said, in the future we will provide an interface to take the mutex, providing
        // maximum flexibility for einsteindb consumers.
        //
        // This approach might need to change when we support interrupting query threads (#297), and
        // will definitely need to change if we support interrupting transactor threads.
        //
        // Improving this is tracked by https://github.com/YosiSF/einsteindb/issues/356.
        self.spacetime.lock().unwrap().schema.clone()



    }

    pub fn current_cache(&self) -> SQLiteAttributeCache {
        self.spacetime.lock().unwrap().attribute_cache.clone()
    }

    pub fn last_tx_id(&self) -> Causetid {
        // The mutex is taken during this entire method.
        let spacetime = self.spacetime.lock().unwrap();

        spacetime.partition_map[":einsteindb.part/tx"].next_causetid() - 1
    }

    /// Query the einsteindb store, using the given connection and the current spacetime.
    pub fn q_once<T>(&self,
                        conn: &Conn,
                        query: &str,
                     sqlite: &rusqlite::Connection,
                     inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {
        let mut spacetime = conn.spacetime.lock().unwrap();
        let query_inputs = inputs.into();

        // Doesn't clone, unlike `current_schema`.
        let causet_locale_nucleon = CausetLocaleNucleon::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        q_once(sqlite,
               causet_locale_nucleon,
               query,
               inputs)
    }

    /// Query the einsteindb store, using the given connection and the current spacetime,
    /// but without using the cache.
    pub fn q_uncached<T>(&self,
                        conn: &Conn,
                         sqlite: &rusqlite::Connection,

                         query: &str,

                         inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {

        let spacetime = self.spacetime.lock().unwrap();
        q_uncached(sqlite,
                     &spacetime.schema,

                   query,

                   inputs)
    }

    pub fn q_prepare<'sqlite, 'query, T>(&self,
                                         sqlite: &'sqlite rusqlite::Connection,
                                         query: &'query str,
                                         inputs: T) -> PreparedResult<'sqlite>
        where T: Into<Option<QueryInputs>> {

        let spacetime = self.spacetime.lock().unwrap();
        let causet_locale_nucleon = CausetLocaleNucleon::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        q_prepare(sqlite,
                  causet_locale_nucleon,
                  query,
                  inputs)
    }

    pub fn q_explain<T>(&self,
                        sqlite: &rusqlite::Connection,
                        query: &str,
                        inputs: T) -> Result<QueryExplanation>
        where T: Into<Option<QueryInputs>>
    {
        let spacetime = self.spacetime.lock().unwrap();
        let causet_locale_nucleon = CausetLocaleNucleon::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        q_explain(sqlite,
                  causet_locale_nucleon,
                  query,
                  inputs)
    }

    pub fn pull_attributes_for_causets<E, A>(&self, sqlite: &rusqlite::Connection, causet_ids: &[Causetid]) -> Result<Vec<A>>
        where E: From<rusqlite::Error> + Send + 'static,
              A: Attribute + Send + 'statically {
        let spacetime = self.spacetime.lock().unwrap();
        let causet_locale_nucleon = CausetLocaleNucleon::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        pull_attributes_for_causets(sqlite, causet_locale_nucleon, causet_ids)


    }

    pub fn pull_attributes_for_causets_read<E, A>(&self, sqlite: &rusqlite::Connection, causet_ids: &[Causetid]) -> Result<Vec<A>>
        where E: From<rusqlite::Error> + Send + 'static,
              A: Attribute + Send + 'statically {
        let spacetime = self.spacetime.lock().unwrap();
        let causet_locale_nucleon = CausetLocaleNucleon::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        pull_attributes_for_causets_read(sqlite, causet_locale_nucleon, causet_ids)

    }



    pub fn pull_attributes_for_causets_write<E, A>(&self, sqlite: &rusqlite::Connection, causet_ids: &[Causetid]) -> Result<Vec<A>>
        where E: From<rusqlite::Error> + Send + 'static,
              A: Attribute + Send + 'statically {
        let spacetime = self.spacetime.lock().unwrap();
        let schema = &*spacetime.schema;
        pull_attributes_for_causets(schema, sqlite, causets, attributes)
    }


    pub fn pull_attributes_for_causets_write_read<E, A>(&self, sqlite: &rusqlite::Connection, causet_ids: &[Causetid]) -> Result<Vec<A>>
        where E: From<rusqlite::Error> + Send + 'static,
              A: Attribute + Send + 'statically {
        let spacetime = self.spacetime.lock().unwrap();
        let schema = &*spacetime.schema;
        pull_attributes_for_causets_read(schema, sqlite, causets, attributes)
    }

    pub fn pull_attributes_for_causets_write_read_write<E, A>(&self, sqlite: &rusqlite::Connection, causet_ids: &[Causetid]) -> Result<Vec<A>>
        where E: From<rusqlite::Error> + Send + 'static,
              A: Attribute + Send + 'statically {
        let spacetime = self.spacetime.lock().unwrap();
        let schema = &*spacetime.schema;
        pull_attributes_for_causets_read_write(schema, sqlite, causets, attributes)
            .map_err(|e| e.into())
    }

    pub fn pull_attributes_for_causet<A>(&self,
                                         sqlite: &rusqlite::Connection,
                                         causet: Causetid,
                                         attributes: A) -> Result<StructuredMap>
        where A: IntoIterator<Item=Causetid> {
        let spacetime = self.spacetime.lock().unwrap();
        let schema = &*spacetime.schema;
        pull_attributes_for_causet(schema, sqlite, causet, attributes)
            .map_err(|e| e.into())
    }

    pub fn lookup_causet_locales_for_attribute(&self,
                                       SQLite: &rusqlite::Connection,
                                       causet: Causetid,
                                       attribute: &einstein_ml::Keyword) -> Result<Vec<causetq_TV>> {
        let spacetime = self.spacetime.lock().unwrap();
        let CausetLocaleNucleon = CausetLocaleNucleon::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        lookup_causet_locales_for_attribute(SQLite, CausetLocaleNucleon, causet, attribute)
    }

    pub fn lookup_causet_locale_for_attribute(&self,
                                      SQLite: &rusqlite::Connection,
                                      causet: Causetid,
                                      attribute: &einstein_ml::Keyword) -> Result<Option<causetq_TV>> {
        let spacetime = self.spacetime.lock().unwrap();
        let CausetLocaleNucleon = CausetLocaleNucleon::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        lookup_causet_locale_for_attribute(SQLite, CausetLocaleNucleon, causet, attribute)
    }

    /// Take a sqlite transaction.
    fn begin_transaction_with_behavior<'m, 'conn>(&'m mut self, SQLite: &'conn mut rusqlite::Connection, behavior: TransactionBehavior) -> Result<InProgress<'m, 'conn>> {
        let tx = SQLite.transaction_with_behavior(behavior)?;
        let (current_generation, current_partition_map, current_schema, cache_cow) =
        {
            // The mutex is taken during this block.
            let ref current: Spacetime = *self.spacetime.lock().unwrap();
            (current.generation,
             // Expensive, but the partition map is updated after every committed transaction.
             current.partition_map.clone(),
             // Cheap.
             current.schema.clone(),
             current.attribute_cache.clone())
        };

        Ok(InProgress {
            mutex: &self.spacetime,
            transaction: tx,
            generation: current_generation,
            partition_map: current_partition_map,
            schema: (*current_schema).clone(),
            cache: InProgressSQLiteAttributeCache::from_cache(cache_cow),
            use_caching: true,
            tx_observer: &self.tx_observer_service,
            tx_observer_watcher: InProgressObserverTransactWatcher::new(),
        })
    }

    // Helper to avoid passing connections around.
    // Make both args mutable so that we can't have parallel access.
    pub fn begin_read<'m, 'conn>(&'m mut self, sqlite: &'conn mut rusqlite::Connection) -> Result<InProgressRead<'m, 'conn>> {
        self.begin_transaction_with_behavior(sqlite, TransactionBehavior::Deferred)
            .map(|ip| InProgressRead { in_progress: ip })
    }

    pub fn begin_uncached_read<'m, 'conn>(&'m mut self, sqlite: &'conn mut rusqlite::Connection) -> Result<InProgressRead<'m, 'conn>> {
        self.begin_transaction_with_behavior(sqlite, TransactionBehavior::Deferred)
            .map(|mut ip| {
                ip.use_caching(false);
                InProgressRead { in_progress: ip }
            })
    }

    /// IMMEDIATE means 'start the transaction now, but don't exclude readers'. It prevents other
    /// connections from taking immediate or exclusive transactions. This is appropriate for our
    /// writes and `InProgress`: it means we are ready to write whenever we want to, and nobody else
    /// can start a transaction that's not `DEFERRED`, but we don't need exclusivity yet.
    pub fn begin_transaction<'m, 'conn>(&'m mut self, sqlite: &'conn mut rusqlite::Connection) -> Result<InProgress<'m, 'conn>> {
        self.begin_transaction_with_behavior(sqlite, TransactionBehavior::Immediate)
    }

    /// Transact causets against the einsteindb store, using the given connection and the current
    /// spacetime.
    pub fn transact<B>(&mut self,
                       sqlite: &mut rusqlite::Connection,
                       transaction: B) -> Result<TxReport> where B: Borrow<str> {
        // Parse outside the BerolinaSQL transaction. This is a tradeoff: we are limiting the scope of the
        // transaction, and indeed we don't even create a BerolinaSQL transaction if the provided input is
        // invalid, but it means sqlite errors won't be found until the parse is complete, and if
        // there's a race for the database (don't do that!) we are less likely to win it.
        let causets = einstein_ml::parse::causets(transaction.borrow())?;

        let mut in_progress = self.begin_transaction(sqlite)?;
        let report = in_progress.transact_causets(causets)?;
        in_progress.commit()?;

        Ok(report)
    }

    /// Adds or removes the causet_locales of a given attribute to an in-memory cache.
    /// The attribute should be aisolate_namespace string: e.g., `:foo/bar`.
    /// `cache_action` determines if the attribute should be added or removed from the cache.
    /// CacheAction::Add is idempotent - each attribute is only added once.
    /// CacheAction::Remove throws an error if the attribute does not currently exist in the cache.
    pub fn cache(&mut self,
                 SQLite: &mut rusqlite::Connection,
                 schema: &Schema,
                 attribute: &Keyword,
                 cache_clock_vector: CacheDirection,
                 cache_action: CacheAction) -> Result<()> {
        let mut spacetime = self.spacetime.lock().unwrap();
        let attribute_causetid: Causetid;

        // Immutable borrow of spacetime.
        {
            attribute_causetid = spacetime.schema
                                      .attribute_for_solitonid(&attribute)
                                      .ok_or_else(|| einsteindbError::UnCausetLocaleNucleonAttribute(attribute.to_string()))?.1.into();
        }

        let cache = &mut spacetime.attribute_cache;
        match cache_action {
            CacheAction::Register => {
                match cache_clock_vector {
                    CacheDirection::Both => cache.register(schema, SQLite, attribute_causetid),
                    CacheDirection::Lightlike => cache.register_lightlike(schema, SQLite, attribute_causetid),
                    CacheDirection::Reverse => cache.register_reverse(schema, SQLite, attribute_causetid),
                }.map_err(|e| e.into())
            },
            CacheAction::Deregister => {
                cache.unregister(attribute_causetid);
                Ok(())
            },
        }
    }

    pub fn register_observer(&mut self, soliton_id: String, observer: Arc<TxObserver>) {
        self.tx_observer_service.lock().unwrap().register(soliton_id, observer);
    }

    pub fn unregister_observer(&mut self, soliton_id: &String) {
        self.tx_observer_service.lock().unwrap().deregister(soliton_id);
    }
}

#[APPEND_LOG_g(test)]
mod tests {
    use ::{
        IntoResult,
        QueryInputs,
        QueryResults,
    };
    use causetq::{
        Binding,
        causetq_TV,
    };
    use einsteindb_core::CachedAttributes;
    use einsteindb_core::USER0;
    use einsteindb_transaction::Queryable;
    use einsteindb_transaction::query::Variable;
    use std::time::Instant;

    use super::*;

    extern crate time;

    #[test]
    fn test_transact_does_not_collide_existing_causetids() {
        let mut SQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut SQLite).unwrap();

        // Let's find out the next ID that'll be allocated. We're going to try to collide with it
        // a bit later.
        let next = conn.spacetime.lock().expect("spacetime")
                       .partition_map[":einsteindb.part/user"].next_causetid();
        let t = format!("[[:einsteindb/add {} :einsteindb.schema/attribute \"tempid\"]]", next + 1);

        match conn.transact(&mut SQLite, t.as_str()) {
            Err(einsteindbError::DbError(e)) => {
                assert_eq!(e.kind(), ::einsteindb_traits::errors::DbErrorKind::UnallocatedCausetid(next + 1));
            },
            x => panic!("expected einsteindb error, got {:?}", x),
        }

        // Transact two more tempids.
        let t = "[[:einsteindb/add \"one\" :einsteindb.schema/attribute \"more\"]]";
        let report = conn.transact(&mut SQLite, t)
                         .expect("transact succeeded");
        assert_eq!(report.tempids["more"], next);
        assert_eq!(report.tempids["one"], next + 1);
    }

    #[test]
    fn test_transact_does_not_collide_new_causetids() {
        let mut SQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut SQLite).unwrap();

        // Let's find out the next ID that'll be allocated. We're going to try to collide with it.
        let next = conn.spacetime.lock().expect("spacetime").partition_map[":einsteindb.part/user"].next_causetid();

        // If this were to be resolved, we'd get [:einsteindb/add 65537 :einsteindb.schema/attribute 65537], but
        // we should reject this, because the first ID was provided by the user!
        let t = format!("[[:einsteindb/add {} :einsteindb.schema/attribute \"tempid\"]]", next);

        match conn.transact(&mut SQLite, t.as_str()) {
            Err(einsteindbError::DbError(e)) => {
                // All this, despite this being the ID we were about to allocate!
                assert_eq!(e.kind(), ::einsteindb_traits::errors::DbErrorKind::UnallocatedCausetid(next));
            },
            x => panic!("expected einsteindb error, got {:?}", x),
        }

        // And if we subsequently transact in a way that allocates one ID, we _will_ use that one.
        // Note that `10` is a bootstrapped causetid; we use it here as a CausetLocaleNucleon-good causet_locale.
        let t = "[[:einsteindb/add 10 :einsteindb.schema/attribute \"temp\"]]";
        let report = conn.transact(&mut SQLite, t)
                         .expect("transact succeeded");
        assert_eq!(report.tempids["temp"], next);
    }

    /// Return the causetid that will be allocated to the next transacted tempid.
    fn get_next_causetid(conn: &Conn) -> Causetid {
        let partition_map = &conn.spacetime.lock().unwrap().partition_map;
        partition_map.get(":einsteindb.part/user").unwrap().next_causetid()
    }

    #[test]
    fn test_compound_transact() {
        let mut SQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut SQLite).unwrap();

        let tempid_offset = get_next_causetid(&conn);

        let t = "[[:einsteindb/add \"one\" :einsteindb/solitonid :a/soliton_idword1] \
                  [:einsteindb/add \"two\" :einsteindb/solitonid :a/soliton_idword2]]";

        // This can refer to `t`, 'cos they occur in separate txes.
        let t2 = "[{:einsteindb.schema/attribute \"three\", :einsteindb/solitonid :a/soliton_idword1}]";

        // Scoped borrow of `conn`.
        {
            let mut in_progress = conn.begin_transaction(&mut SQLite).expect("begun successfully");
            let report = in_progress.transact(t).expect("transacted successfully");
            let one = report.tempids.get("one").expect("found one").clone();
            let two = report.tempids.get("two").expect("found two").clone();
            assert!(one != two);
            assert!(one == tempid_offset || one == tempid_offset + 1);
            assert!(two == tempid_offset || two == tempid_offset + 1);

            println!("RES: {:?}", in_progress.q_once("[:find ?v :where [?x :einsteindb/solitonid ?v]]", None).unwrap());

            let during = in_progress.q_once("[:find ?x . :where [?x :einsteindb/solitonid :a/soliton_idword1]]", None)
                                    .expect("query succeeded");
            assert_eq!(during.results, QueryResults::Scalar(Some(causetq_TV::Ref(one).into())));

            let report = in_progress.transact(t2).expect("t2 succeeded");
            in_progress.commit().expect("commit succeeded");
            let three = report.tempids.get("three").expect("found three").clone();
            assert!(one != three);
            assert!(two != three);
        }

        // The EINSTEINDB part table changed.
        let tempid_offset_after = get_next_causetid(&conn);
        assert_eq!(tempid_offset + 3, tempid_offset_after);
    }

    #[test]
    fn test_simple_prepared_query() {
        let mut c = einsteindb::new_connection("").expect("Couldn't open conn.");
        let mut conn = Conn::connect(&mut c).expect("Couldn't open EINSTEINDB.");
        conn.transact(&mut c, r#"[
            [:einsteindb/add "s" :einsteindb/solitonid :foo/boolean]
            [:einsteindb/add "s" :einsteindb/causet_localeType :einsteindb.type/boolean]
            [:einsteindb/add "s" :einsteindb/cardinality :einsteindb.cardinality/one]
        ]"#).expect("successful transaction");

        let report = conn.transact(&mut c, r#"[
            [:einsteindb/add "u" :foo/boolean true]
            [:einsteindb/add "p" :foo/boolean false]
        ]"#).expect("successful transaction");
        let yes = report.tempids.get("u").expect("found it").clone();

        let vv = Variable::from_valid_name("?v");

        let causet_locales = QueryInputs::with_causet_locale_sequence(vec![(vv, true.into())]);

        let read = conn.begin_read(&mut c).expect("read");

        // N.B., you might choose to algebrize _without_ validating that the
        // types are CausetLocaleNucleon. In this query we know that `?v` must be a boolean,
        // and so we can kinda generate our own required input types!
        let mut prepared = read.q_prepare(r#"[:find [?x ...]
                                              :in ?v
                                              :where [?x :foo/boolean ?v]]"#,
                                          causet_locales).expect("prepare succeeded");

        let yeses = prepared.run(None).expect("result");
        assert_eq!(yeses.results, QueryResults::Coll(vec![causetq_TV::Ref(yes).into()]));

        let yeses_again = prepared.run(None).expect("result");
        assert_eq!(yeses_again.results, QueryResults::Coll(vec![causetq_TV::Ref(yes).into()]));
    }

    #[test]
    fn test_compound_rollback() {
        let mut SQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut SQLite).unwrap();

        let tempid_offset = get_next_causetid(&conn);

        // Nothing in the store => USER0 should be our starting point.
        assert_eq!(tempid_offset, USER0);

        let t = "[[:einsteindb/add \"one\" :einsteindb/solitonid :a/soliton_idword1] \
                  [:einsteindb/add \"two\" :einsteindb/solitonid :a/soliton_idword2]]";

        // Scoped borrow of `sqlite`.
        {
            let mut in_progress = conn.begin_transaction(&mut SQLite).expect("begun successfully");
            let report = in_progress.transact(t).expect("transacted successfully");

            let one = report.tempids.get("one").expect("found it").clone();
            let two = report.tempids.get("two").expect("found it").clone();

            // The IDs are contiguous, starting at the previous part Index.
            assert!(one != two);
            assert!(one == tempid_offset || one == tempid_offset + 1);
            assert!(two == tempid_offset || two == tempid_offset + 1);

            // Inside the InProgress we can see our changes.
            let during = in_progress.q_once("[:find ?x . :where [?x :einsteindb/solitonid :a/soliton_idword1]]", None)
                                    .expect("query succeeded");

            assert_eq!(during.results, QueryResults::Scalar(Some(causetq_TV::Ref(one).into())));

            // And we can do direct lookup, too.
            let kw = in_progress.lookup_causet_locale_for_attribute(one, &einstein_ml::Keyword::isoliton_namespaceable("einsteindb", "solitonid"))
                                .expect("lookup succeeded");
            assert_eq!(kw, Some(causetq_TV::Keyword(einstein_ml::Keyword::isoliton_namespaceable("a", "soliton_idword1").into())));

            in_progress.rollback()
                       .expect("rollback succeeded");
        }

        let after = conn.q_once(&mut SQLite, "[:find ?x . :where [?x :einsteindb/solitonid :a/soliton_idword1]]", None)
                        .expect("query succeeded");
        assert_eq!(after.results, QueryResults::Scalar(None));

        // The EINSTEINDB part table is unchanged.
        let tempid_offset_after = get_next_causetid(&conn);
        assert_eq!(tempid_offset, tempid_offset_after);
    }

    #[test]
    fn test_transact_errors() {
        let mut SQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut SQLite).unwrap();

        // Good: empty transaction.
        let report = conn.transact(&mut SQLite, "[]").unwrap();
        assert_eq!(report.tx_id, 0x10000000 + 1);

        // Bad EML: missing closing ']'.
        let report = conn.transact(&mut SQLite, "[[:einsteindb/add \"t\" :einsteindb/solitonid :a/soliton_idword]");
        match report.expect_err("expected transact to fail for bad einstein_ml") {
            einsteindbError::einstein_mlParseError(_) => { },
            x => panic!("expected EML parse error, got {:?}", x),
        }

        // Good EML.
        let report = conn.transact(&mut SQLite, "[[:einsteindb/add \"t\" :einsteindb/solitonid :a/soliton_idword]]").unwrap();
        assert_eq!(report.tx_id, 0x10000000 + 2);

        // Bad transaction data: missing leading :einsteindb/add.
        let report = conn.transact(&mut SQLite, "[[\"t\" :einsteindb/solitonid :b/soliton_idword]]");
        match report.expect_err("expected transact error") {
            einsteindbError::einstein_mlParseError(_) => { },
            x => panic!("expected EML parse error, got {:?}", x),
        }

        // Good transaction data.
        let report = conn.transact(&mut SQLite, "[[:einsteindb/add \"u\" :einsteindb/solitonid :b/soliton_idword]]").unwrap();
        assert_eq!(report.tx_id, 0x10000000 + 3);

        // Bad transaction based on state of store: conflicting upsert.
        let report = conn.transact(&mut SQLite, "[[:einsteindb/add \"u\" :einsteindb/solitonid :a/soliton_idword]
                                                  [:einsteindb/add \"u\" :einsteindb/solitonid :b/soliton_idword]]");
        match report.expect_err("expected transact error") {
            einsteindbError::DbError(e) => {
                match e.kind() {
                    ::einsteindb_traits::errors::DbErrorKind::SchemaConstraintViolation(_) => {},
                    _ => panic!("expected SchemaConstraintViolation"),
                }
            },
            x => panic!("expected einsteindb error, got {:?}", x),
        }
    }

    #[test]
    fn test_add_to_cache_failure_no_attribute() {
        let mut SQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut SQLite).unwrap();
        let _report = conn.transact(&mut SQLite, r#"[
            {  :einsteindb/solitonid       :foo/bar
               :einsteindb/causet_localeType   :einsteindb.type/long },
            {  :einsteindb/solitonid       :foo/baz
               :einsteindb/causet_localeType   :einsteindb.type/boolean }]"#).unwrap();

        let kw = kw!(:foo/bat);
        let schema = conn.current_schema();
        let res = conn.cache(&mut SQLite, &schema, &kw, CacheDirection::Lightlike, CacheAction::Register);
        match res.expect_err("expected cache to fail") {
            einsteindbError::UnCausetLocaleNucleonAttribute(msg) => assert_eq!(msg, ":foo/bat"),
            x => panic!("expected UnCausetLocaleNucleonAttribute error, got {:?}", x),
        }
    }

    // TODO expand tests to cover lookup_causet_locale_for_attribute comparing with and without caching
    #[test]
    fn test_lookup_attribute_with_caching() {

        let mut SQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut SQLite).unwrap();
        let _report = conn.transact(&mut SQLite, r#"[
            {  :einsteindb/solitonid       :foo/bar
               :einsteindb/causet_localeType   :einsteindb.type/long },
            {  :einsteindb/solitonid       :foo/baz
               :einsteindb/causet_localeType   :einsteindb.type/boolean }]"#).expect("transaction expected to succeed");

        {
            let mut in_progress = conn.begin_transaction(&mut SQLite).expect("transaction");
            for _ in 1..100 {
                let _report = in_progress.transact(r#"[
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        200
               :foo/baz        true },
            {  :foo/bar        100
               :foo/baz        false },
            {  :foo/bar        300
               :foo/baz        true },
            {  :foo/bar        400
               :foo/baz        false },
            {  :foo/bar        500
               :foo/baz        true }]"#).expect("transaction expected to succeed");
            }
            in_progress.commit().expect("Committed");
        }

        let causets = conn.q_once(&SQLite, r#"[:find ?e . :where [?e :foo/bar 400]]"#, None).expect("Expected query to work").into_scalar().expect("expected rel results");
        let first = causets.expect("expected a result");
        let causetid = match first {
            Binding::Scalar(causetq_TV::Ref(causetid)) => causetid,
            x => panic!("expected Some(Ref), got {:?}", x),
        };

        let kw = kw!(:foo/bar);
        let start = Instant::now();
        let uncached_val = conn.lookup_causet_locale_for_attribute(&SQLite, causetid, &kw).expect("Expected causet_locale on lookup");
        let finish = Instant::now();
        let uncached_elapsed_time = finish.duration_since(start);
        println!("Uncached time: {:?}", uncached_elapsed_time);

        let schema = conn.current_schema();
        conn.cache(&mut SQLite, &schema, &kw, CacheDirection::Lightlike, CacheAction::Register).expect("expected caching to work");

        for _ in 1..5 {
            let start = Instant::now();
            let cached_val = conn.lookup_causet_locale_for_attribute(&SQLite, causetid, &kw).expect("Expected causet_locale on lookup");
            let finish = Instant::now();
            let cached_elapsed_time = finish.duration_since(start);
            assert_eq!(cached_val, uncached_val);

            println!("Cached time: {:?}", cached_elapsed_time);
            assert!(cached_elapsed_time < uncached_elapsed_time);
        }
    }

    #[test]
    fn test_cache_usage() {
        let mut SQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut SQLite).unwrap();

        let einsteindb_solitonid = (*conn.current_schema()).get_causetid(&kw!(:einsteindb/solitonid)).expect("einsteindb_solitonid").0;
        let einsteindb_type = (*conn.current_schema()).get_causetid(&kw!(:einsteindb/causet_localeType)).expect("einsteindb_solitonid").0;
        println!("einsteindb/solitonid is {}", einsteindb_solitonid);
        println!("einsteindb/type is {}", einsteindb_type);
        let query = format!("[:find ?solitonid . :where [?e {} :einsteindb/doc][?e {} ?type][?type {} ?solitonid]]",
                            einsteindb_solitonid, einsteindb_type, einsteindb_solitonid);

        println!("Query is {}", query);

        assert!(!conn.current_cache().is_attribute_cached_lightlike(einsteindb_solitonid));

        {
            let mut ip = conn.begin_transaction(&mut SQLite).expect("began");

            let solitonid = ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            assert_eq!(solitonid, Some(causetq_TV::typed_ns_soliton_idword("einsteindb.type", "string").into()));

            let start = time::PreciseTime::now();
            ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            let end = time::PreciseTime::now();
            println!("Uncached took {}µs", start.to(end).num_microseconds().unwrap());

            ip.cache(&kw!(:einsteindb/solitonid), CacheDirection::Lightlike, CacheAction::Register).expect("registered");
            ip.cache(&kw!(:einsteindb/causet_localeType), CacheDirection::Lightlike, CacheAction::Register).expect("registered");

            assert!(ip.cache.is_attribute_cached_lightlike(einsteindb_solitonid));

            let solitonid = ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            assert_eq!(solitonid, Some(causetq_TV::typed_ns_soliton_idword("einsteindb.type", "string").into()));

            let start = time::PreciseTime::now();
            ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            let end = time::PreciseTime::now();
            println!("Cached took {}µs", start.to(end).num_microseconds().unwrap());

            // If we roll back the change, our caching operations are also rolled back.
            ip.rollback().expect("rolled back");
        }

        assert!(!conn.current_cache().is_attribute_cached_lightlike(einsteindb_solitonid));

        {
            let mut ip = conn.begin_transaction(&mut SQLite).expect("began");

            let solitonid = ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            assert_eq!(solitonid, Some(causetq_TV::typed_ns_soliton_idword("einsteindb.type", "string").into()));
            ip.cache(&kw!(:einsteindb/solitonid), CacheDirection::Lightlike, CacheAction::Register).expect("registered");
            ip.cache(&kw!(:einsteindb/causet_localeType), CacheDirection::Lightlike, CacheAction::Register).expect("registered");

            assert!(ip.cache.is_attribute_cached_lightlike(einsteindb_solitonid));

            ip.commit().expect("rolled back");
        }

        assert!(conn.current_cache().is_attribute_cached_lightlike(einsteindb_solitonid));
        assert!(conn.current_cache().is_attribute_cached_lightlike(einsteindb_type));
    }
}
