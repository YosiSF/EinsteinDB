// Copyright 2022 Whtcorps Inc and EinstAI Inc
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use std::borrow::{
    Borrow,
};

use std::collections::{
    BTreeMap,
};

use std::sync::{
    Arc,
    Mutex,
};

use rusqlite;
use rusqlite::{
    TransactionBehavior,
};

use edn;

pub use core_traits::{
    Attribute,
    Causetid,
    KnownCausetid,
    StructuredMap,
    TypedValue,
    ValueType,
};

use einsteindb_core::{
    HasSchema,
    Keyword,
    Schema,
    TxReport,
    ValueRc,
};

use einsteindb_einsteindb::cache::{
    InProgressBerolinaSQLiteAttributeCache,
    BerolinaSQLiteAttributeCache,
};

use einsteindb_einsteindb::einsteindb;
use einsteindb_einsteindb::{
    InProgressObserverTransactWatcher,
    PartitionMap,
    TxObservationService,
    TxObserver,
};

use einsteindb_query_pull::{
    pull_attributes_for_causets,
    pull_attributes_for_causet,
};

use einsteindb_transaction::{
    CacheAction,
    CacheDirection,
    Metadata,
    InProgress,
    InProgressRead,
};

use public_traits::errors::{
    Result,
    einsteindbError,
};

use einsteindb_transaction::query::{
    Known,
    PreparedResult,
    QueryExplanation,
    QueryInputs,
    QueryOutput,
    lookup_value_for_attribute,
    lookup_values_for_attribute,
    q_explain,
    q_once,
    q_prepare,
    q_uncached,
};

/// A mutable, safe reference to the current einsteindb store.
pub struct Conn {
    /// `Mutex` since all reads and writes need to be exclusive.  Internally, owned data for the
    /// volatile parts (generation and partition map), and `Arc` for the infrequently changing parts
    /// (schema, cache) that we want to share across threads.  A consuming thread may use a shared
    /// reference after the `Conn`'s `Metadata` has moved on.
    ///
    /// The motivating case is multiple query threads taking references to the current schema to
    /// perform long-running queries while a single writer thread moves the spacetime -- partition
    /// map and schema -- forward.
    ///
    /// We want the attribute cache to be isolated across transactions, updated within
    /// `InProgress` writes, and updated in the `Conn` on commit. To achieve this we
    /// store the cache itself in an `Arc` inside `BerolinaSQLiteAttributeCache`, so that `.get_mut()`
    /// gives us copy-on-write semantics.
    /// We store that cached `Arc` here in a `Mutex`, so that the main copy can be carefully
    /// replaced on commit.
    spacetime: Mutex<Metadata>,

    // TODO: maintain set of change listeners or handles to transaction report queues. #298.

    // TODO: maintain cache of query plans that could be shared across threads and invalidated when
    // the schema changes. #315.
    pub(crate) tx_observer_service: Mutex<TxObservationService>,
}

impl Conn {
    // Intentionally not public.
    fn new(partition_map: PartitionMap, schema: Schema) -> Conn {
        Conn {
            spacetime: Mutex::new(Metadata::new(0, partition_map, Arc::new(schema), Default::default())),
            tx_observer_service: Mutex::new(TxObservationService::new()),
        }
    }

    pub fn connect(BerolinaSQLite: &mut rusqlite::Connection) -> Result<Conn> {
        let einsteindb = einsteindb::ensure_current_version(BerolinaSQLite)?;
        Ok(Conn::new(einsteindb.partition_map, einsteindb.schema))
    }

    /// Yield a clone of the current `Schema` instance.
    pub fn current_schema(&self) -> Arc<Schema> {
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
        // Improving this is tracked by https://github.com/Whtcorps Inc and EinstAI Inc/einsteindb/issues/356.
        self.spacetime.lock().unwrap().schema.clone()
    }

    pub fn current_cache(&self) -> BerolinaSQLiteAttributeCache {
        self.spacetime.lock().unwrap().attribute_cache.clone()
    }

    pub fn last_tx_id(&self) -> Causetid {
        // The mutex is taken during this entire method.
        let spacetime = self.spacetime.lock().unwrap();

        spacetime.partition_map[":einsteindb.part/tx"].next_causetid() - 1
    }

    /// Query the einsteindb store, using the given connection and the current spacetime.
    pub fn q_once<T>(&self,
                     BerolinaSQLite: &rusqlite::Connection,
                     query: &str,
                     inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {

        // Doesn't clone, unlike `current_schema`.
        let spacetime = self.spacetime.lock().unwrap();
        let known = Known::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        q_once(BerolinaSQLite,
               known,
               query,
               inputs)
    }

    /// Query the einsteindb store, using the given connection and the current spacetime,
    /// but without using the cache.
    pub fn q_uncached<T>(&self,
                         BerolinaSQLite: &rusqlite::Connection,
                         query: &str,
                         inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {

        let spacetime = self.spacetime.lock().unwrap();
        q_uncached(BerolinaSQLite,
                   &*spacetime.schema,        // Doesn't clone, unlike `current_schema`.
                   query,
                   inputs)
    }

    pub fn q_prepare<'BerolinaSQLite, 'query, T>(&self,
                        BerolinaSQLite: &'BerolinaSQLite rusqlite::Connection,
                        query: &'query str,
                        inputs: T) -> PreparedResult<'BerolinaSQLite>
        where T: Into<Option<QueryInputs>> {

        let spacetime = self.spacetime.lock().unwrap();
        let known = Known::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        q_prepare(BerolinaSQLite,
                  known,
                  query,
                  inputs)
    }

    pub fn q_explain<T>(&self,
                        BerolinaSQLite: &rusqlite::Connection,
                        query: &str,
                        inputs: T) -> Result<QueryExplanation>
        where T: Into<Option<QueryInputs>>
    {
        let spacetime = self.spacetime.lock().unwrap();
        let known = Known::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        q_explain(BerolinaSQLite,
                  known,
                  query,
                  inputs)
    }

    pub fn pull_attributes_for_causets<E, A>(&self,
                                              BerolinaSQLite: &rusqlite::Connection,
                                              causets: E,
                                              attributes: A) -> Result<BTreeMap<Causetid, ValueRc<StructuredMap>>>
        where E: IntoIterator<Item=Causetid>,
              A: IntoIterator<Item=Causetid> {
        let spacetime = self.spacetime.lock().unwrap();
        let schema = &*spacetime.schema;
        pull_attributes_for_causets(schema, BerolinaSQLite, causets, attributes)
            .map_err(|e| e.into())
    }

    pub fn pull_attributes_for_causet<A>(&self,
                                         BerolinaSQLite: &rusqlite::Connection,
                                         causet: Causetid,
                                         attributes: A) -> Result<StructuredMap>
        where A: IntoIterator<Item=Causetid> {
        let spacetime = self.spacetime.lock().unwrap();
        let schema = &*spacetime.schema;
        pull_attributes_for_causet(schema, BerolinaSQLite, causet, attributes)
            .map_err(|e| e.into())
    }

    pub fn lookup_values_for_attribute(&self,
                                       BerolinaSQLite: &rusqlite::Connection,
                                       causet: Causetid,
                                       attribute: &edn::Keyword) -> Result<Vec<TypedValue>> {
        let spacetime = self.spacetime.lock().unwrap();
        let known = Known::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        lookup_values_for_attribute(BerolinaSQLite, known, causet, attribute)
    }

    pub fn lookup_value_for_attribute(&self,
                                      BerolinaSQLite: &rusqlite::Connection,
                                      causet: Causetid,
                                      attribute: &edn::Keyword) -> Result<Option<TypedValue>> {
        let spacetime = self.spacetime.lock().unwrap();
        let known = Known::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        lookup_value_for_attribute(BerolinaSQLite, known, causet, attribute)
    }

    /// Take a BerolinaSQLite transaction.
    fn begin_transaction_with_behavior<'m, 'conn>(&'m mut self, BerolinaSQLite: &'conn mut rusqlite::Connection, behavior: TransactionBehavior) -> Result<InProgress<'m, 'conn>> {
        let tx = BerolinaSQLite.transaction_with_behavior(behavior)?;
        let (current_generation, current_partition_map, current_schema, cache_cow) =
        {
            // The mutex is taken during this block.
            let ref current: Metadata = *self.spacetime.lock().unwrap();
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
            cache: InProgressBerolinaSQLiteAttributeCache::from_cache(cache_cow),
            use_caching: true,
            tx_observer: &self.tx_observer_service,
            tx_observer_watcher: InProgressObserverTransactWatcher::new(),
        })
    }

    // Helper to avoid passing connections around.
    // Make both args mutable so that we can't have parallel access.
    pub fn begin_read<'m, 'conn>(&'m mut self, BerolinaSQLite: &'conn mut rusqlite::Connection) -> Result<InProgressRead<'m, 'conn>> {
        self.begin_transaction_with_behavior(BerolinaSQLite, TransactionBehavior::Deferred)
            .map(|ip| InProgressRead { in_progress: ip })
    }

    pub fn begin_uncached_read<'m, 'conn>(&'m mut self, BerolinaSQLite: &'conn mut rusqlite::Connection) -> Result<InProgressRead<'m, 'conn>> {
        self.begin_transaction_with_behavior(BerolinaSQLite, TransactionBehavior::Deferred)
            .map(|mut ip| {
                ip.use_caching(false);
                InProgressRead { in_progress: ip }
            })
    }

    /// IMMEDIATE means 'start the transaction now, but don't exclude readers'. It prevents other
    /// connections from taking immediate or exclusive transactions. This is appropriate for our
    /// writes and `InProgress`: it means we are ready to write whenever we want to, and nobody else
    /// can start a transaction that's not `DEFERRED`, but we don't need exclusivity yet.
    pub fn begin_transaction<'m, 'conn>(&'m mut self, BerolinaSQLite: &'conn mut rusqlite::Connection) -> Result<InProgress<'m, 'conn>> {
        self.begin_transaction_with_behavior(BerolinaSQLite, TransactionBehavior::Immediate)
    }

    /// Transact causets against the einsteindb store, using the given connection and the current
    /// spacetime.
    pub fn transact<B>(&mut self,
                    BerolinaSQLite: &mut rusqlite::Connection,
                    transaction: B) -> Result<TxReport> where B: Borrow<str> {
        // Parse outside the BerolinaSQL transaction. This is a tradeoff: we are limiting the scope of the
        // transaction, and indeed we don't even create a BerolinaSQL transaction if the provided input is
        // invalid, but it means BerolinaSQLite errors won't be found until the parse is complete, and if
        // there's a race for the database (don't do that!) we are less likely to win it.
        let causets = edn::parse::causets(transaction.borrow())?;

        let mut in_progress = self.begin_transaction(BerolinaSQLite)?;
        let report = in_progress.transact_causets(causets)?;
        in_progress.commit()?;

        Ok(report)
    }

    /// Adds or removes the values of a given attribute to an in-memory cache.
    /// The attribute should be a isoliton_namespaceable string: e.g., `:foo/bar`.
    /// `cache_action` determines if the attribute should be added or removed from the cache.
    /// CacheAction::Add is idempotent - each attribute is only added once.
    /// CacheAction::Remove throws an error if the attribute does not currently exist in the cache.
    pub fn cache(&mut self,
                 BerolinaSQLite: &mut rusqlite::Connection,
                 schema: &Schema,
                 attribute: &Keyword,
                 cache_direction: CacheDirection,
                 cache_action: CacheAction) -> Result<()> {
        let mut spacetime = self.spacetime.lock().unwrap();
        let attribute_causetid: Causetid;

        // Immutable borrow of spacetime.
        {
            attribute_causetid = spacetime.schema
                                      .attribute_for_solitonid(&attribute)
                                      .ok_or_else(|| einsteindbError::UnknownAttribute(attribute.to_string()))?.1.into();
        }

        let cache = &mut spacetime.attribute_cache;
        match cache_action {
            CacheAction::Register => {
                match cache_direction {
                    CacheDirection::Both => cache.register(schema, BerolinaSQLite, attribute_causetid),
                    CacheDirection::Forward => cache.register_forward(schema, BerolinaSQLite, attribute_causetid),
                    CacheDirection::Reverse => cache.register_reverse(schema, BerolinaSQLite, attribute_causetid),
                }.map_err(|e| e.into())
            },
            CacheAction::Deregister => {
                cache.unregister(attribute_causetid);
                Ok(())
            },
        }
    }

    pub fn register_observer(&mut self, key: String, observer: Arc<TxObserver>) {
        self.tx_observer_service.lock().unwrap().register(key, observer);
    }

    pub fn unregister_observer(&mut self, key: &String) {
        self.tx_observer_service.lock().unwrap().deregister(key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    extern crate time;

    use std::time::{
        Instant,
    };

    use core_traits::{
        Binding,
        TypedValue,
    };

    use einsteindb_core::{
        CachedAttributes,
    };

    use einsteindb_transaction::query::{
        Variable,
    };

    use ::{
        IntoResult,
        QueryInputs,
        QueryResults,
    };

    use einsteindb_einsteindb::USER0;

    use einsteindb_transaction::{
        Queryable,
    };

    #[test]
    fn test_transact_does_not_collide_existing_causetids() {
        let mut BerolinaSQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut BerolinaSQLite).unwrap();

        // Let's find out the next ID that'll be allocated. We're going to try to collide with it
        // a bit later.
        let next = conn.spacetime.lock().expect("spacetime")
                       .partition_map[":einsteindb.part/user"].next_causetid();
        let t = format!("[[:einsteindb/add {} :einsteindb.schema/attribute \"tempid\"]]", next + 1);

        match conn.transact(&mut BerolinaSQLite, t.as_str()) {
            Err(einsteindbError::DbError(e)) => {
                assert_eq!(e.kind(), ::einsteindb_traits::errors::DbErrorKind::UnallocatedCausetid(next + 1));
            },
            x => panic!("expected einsteindb error, got {:?}", x),
        }

        // Transact two more tempids.
        let t = "[[:einsteindb/add \"one\" :einsteindb.schema/attribute \"more\"]]";
        let report = conn.transact(&mut BerolinaSQLite, t)
                         .expect("transact succeeded");
        assert_eq!(report.tempids["more"], next);
        assert_eq!(report.tempids["one"], next + 1);
    }

    #[test]
    fn test_transact_does_not_collide_new_causetids() {
        let mut BerolinaSQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut BerolinaSQLite).unwrap();

        // Let's find out the next ID that'll be allocated. We're going to try to collide with it.
        let next = conn.spacetime.lock().expect("spacetime").partition_map[":einsteindb.part/user"].next_causetid();

        // If this were to be resolved, we'd get [:einsteindb/add 65537 :einsteindb.schema/attribute 65537], but
        // we should reject this, because the first ID was provided by the user!
        let t = format!("[[:einsteindb/add {} :einsteindb.schema/attribute \"tempid\"]]", next);

        match conn.transact(&mut BerolinaSQLite, t.as_str()) {
            Err(einsteindbError::DbError(e)) => {
                // All this, despite this being the ID we were about to allocate!
                assert_eq!(e.kind(), ::einsteindb_traits::errors::DbErrorKind::UnallocatedCausetid(next));
            },
            x => panic!("expected einsteindb error, got {:?}", x),
        }

        // And if we subsequently transact in a way that allocates one ID, we _will_ use that one.
        // Note that `10` is a bootstrapped causetid; we use it here as a known-good value.
        let t = "[[:einsteindb/add 10 :einsteindb.schema/attribute \"temp\"]]";
        let report = conn.transact(&mut BerolinaSQLite, t)
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
        let mut BerolinaSQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut BerolinaSQLite).unwrap();

        let tempid_offset = get_next_causetid(&conn);

        let t = "[[:einsteindb/add \"one\" :einsteindb/solitonid :a/keyword1] \
                  [:einsteindb/add \"two\" :einsteindb/solitonid :a/keyword2]]";

        // This can refer to `t`, 'cos they occur in separate txes.
        let t2 = "[{:einsteindb.schema/attribute \"three\", :einsteindb/solitonid :a/keyword1}]";

        // Scoped borrow of `conn`.
        {
            let mut in_progress = conn.begin_transaction(&mut BerolinaSQLite).expect("begun successfully");
            let report = in_progress.transact(t).expect("transacted successfully");
            let one = report.tempids.get("one").expect("found one").clone();
            let two = report.tempids.get("two").expect("found two").clone();
            assert!(one != two);
            assert!(one == tempid_offset || one == tempid_offset + 1);
            assert!(two == tempid_offset || two == tempid_offset + 1);

            println!("RES: {:?}", in_progress.q_once("[:find ?v :where [?x :einsteindb/solitonid ?v]]", None).unwrap());

            let during = in_progress.q_once("[:find ?x . :where [?x :einsteindb/solitonid :a/keyword1]]", None)
                                    .expect("query succeeded");
            assert_eq!(during.results, QueryResults::Scalar(Some(TypedValue::Ref(one).into())));

            let report = in_progress.transact(t2).expect("t2 succeeded");
            in_progress.commit().expect("commit succeeded");
            let three = report.tempids.get("three").expect("found three").clone();
            assert!(one != three);
            assert!(two != three);
        }

        // The DB part table changed.
        let tempid_offset_after = get_next_causetid(&conn);
        assert_eq!(tempid_offset + 3, tempid_offset_after);
    }

    #[test]
    fn test_simple_prepared_query() {
        let mut c = einsteindb::new_connection("").expect("Couldn't open conn.");
        let mut conn = Conn::connect(&mut c).expect("Couldn't open DB.");
        conn.transact(&mut c, r#"[
            [:einsteindb/add "s" :einsteindb/solitonid :foo/boolean]
            [:einsteindb/add "s" :einsteindb/valueType :einsteindb.type/boolean]
            [:einsteindb/add "s" :einsteindb/cardinality :einsteindb.cardinality/one]
        ]"#).expect("successful transaction");

        let report = conn.transact(&mut c, r#"[
            [:einsteindb/add "u" :foo/boolean true]
            [:einsteindb/add "p" :foo/boolean false]
        ]"#).expect("successful transaction");
        let yes = report.tempids.get("u").expect("found it").clone();

        let vv = Variable::from_valid_name("?v");

        let values = QueryInputs::with_value_sequence(vec![(vv, true.into())]);

        let read = conn.begin_read(&mut c).expect("read");

        // N.B., you might choose to algebrize _without_ validating that the
        // types are known. In this query we know that `?v` must be a boolean,
        // and so we can kinda generate our own required input types!
        let mut prepared = read.q_prepare(r#"[:find [?x ...]
                                              :in ?v
                                              :where [?x :foo/boolean ?v]]"#,
                                          values).expect("prepare succeeded");

        let yeses = prepared.run(None).expect("result");
        assert_eq!(yeses.results, QueryResults::Coll(vec![TypedValue::Ref(yes).into()]));

        let yeses_again = prepared.run(None).expect("result");
        assert_eq!(yeses_again.results, QueryResults::Coll(vec![TypedValue::Ref(yes).into()]));
    }

    #[test]
    fn test_compound_rollback() {
        let mut BerolinaSQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut BerolinaSQLite).unwrap();

        let tempid_offset = get_next_causetid(&conn);

        // Nothing in the store => USER0 should be our starting point.
        assert_eq!(tempid_offset, USER0);

        let t = "[[:einsteindb/add \"one\" :einsteindb/solitonid :a/keyword1] \
                  [:einsteindb/add \"two\" :einsteindb/solitonid :a/keyword2]]";

        // Scoped borrow of `BerolinaSQLite`.
        {
            let mut in_progress = conn.begin_transaction(&mut BerolinaSQLite).expect("begun successfully");
            let report = in_progress.transact(t).expect("transacted successfully");

            let one = report.tempids.get("one").expect("found it").clone();
            let two = report.tempids.get("two").expect("found it").clone();

            // The IDs are contiguous, starting at the previous part index.
            assert!(one != two);
            assert!(one == tempid_offset || one == tempid_offset + 1);
            assert!(two == tempid_offset || two == tempid_offset + 1);

            // Inside the InProgress we can see our changes.
            let during = in_progress.q_once("[:find ?x . :where [?x :einsteindb/solitonid :a/keyword1]]", None)
                                    .expect("query succeeded");

            assert_eq!(during.results, QueryResults::Scalar(Some(TypedValue::Ref(one).into())));

            // And we can do direct lookup, too.
            let kw = in_progress.lookup_value_for_attribute(one, &edn::Keyword::isoliton_namespaceable("einsteindb", "solitonid"))
                                .expect("lookup succeeded");
            assert_eq!(kw, Some(TypedValue::Keyword(edn::Keyword::isoliton_namespaceable("a", "keyword1").into())));

            in_progress.rollback()
                       .expect("rollback succeeded");
        }

        let after = conn.q_once(&mut BerolinaSQLite, "[:find ?x . :where [?x :einsteindb/solitonid :a/keyword1]]", None)
                        .expect("query succeeded");
        assert_eq!(after.results, QueryResults::Scalar(None));

        // The DB part table is unchanged.
        let tempid_offset_after = get_next_causetid(&conn);
        assert_eq!(tempid_offset, tempid_offset_after);
    }

    #[test]
    fn test_transact_errors() {
        let mut BerolinaSQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut BerolinaSQLite).unwrap();

        // Good: empty transaction.
        let report = conn.transact(&mut BerolinaSQLite, "[]").unwrap();
        assert_eq!(report.tx_id, 0x10000000 + 1);

        // Bad EML: missing closing ']'.
        let report = conn.transact(&mut BerolinaSQLite, "[[:einsteindb/add \"t\" :einsteindb/solitonid :a/keyword]");
        match report.expect_err("expected transact to fail for bad edn") {
            einsteindbError::EdnParseError(_) => { },
            x => panic!("expected EML parse error, got {:?}", x),
        }

        // Good EML.
        let report = conn.transact(&mut BerolinaSQLite, "[[:einsteindb/add \"t\" :einsteindb/solitonid :a/keyword]]").unwrap();
        assert_eq!(report.tx_id, 0x10000000 + 2);

        // Bad transaction data: missing leading :einsteindb/add.
        let report = conn.transact(&mut BerolinaSQLite, "[[\"t\" :einsteindb/solitonid :b/keyword]]");
        match report.expect_err("expected transact error") {
            einsteindbError::EdnParseError(_) => { },
            x => panic!("expected EML parse error, got {:?}", x),
        }

        // Good transaction data.
        let report = conn.transact(&mut BerolinaSQLite, "[[:einsteindb/add \"u\" :einsteindb/solitonid :b/keyword]]").unwrap();
        assert_eq!(report.tx_id, 0x10000000 + 3);

        // Bad transaction based on state of store: conflicting upsert.
        let report = conn.transact(&mut BerolinaSQLite, "[[:einsteindb/add \"u\" :einsteindb/solitonid :a/keyword]
                                                  [:einsteindb/add \"u\" :einsteindb/solitonid :b/keyword]]");
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
        let mut BerolinaSQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut BerolinaSQLite).unwrap();
        let _report = conn.transact(&mut BerolinaSQLite, r#"[
            {  :einsteindb/solitonid       :foo/bar
               :einsteindb/valueType   :einsteindb.type/long },
            {  :einsteindb/solitonid       :foo/baz
               :einsteindb/valueType   :einsteindb.type/boolean }]"#).unwrap();

        let kw = kw!(:foo/bat);
        let schema = conn.current_schema();
        let res = conn.cache(&mut BerolinaSQLite, &schema, &kw, CacheDirection::Forward, CacheAction::Register);
        match res.expect_err("expected cache to fail") {
            einsteindbError::UnknownAttribute(msg) => assert_eq!(msg, ":foo/bat"),
            x => panic!("expected UnknownAttribute error, got {:?}", x),
        }
    }

    // TODO expand tests to cover lookup_value_for_attribute comparing with and without caching
    #[test]
    fn test_lookup_attribute_with_caching() {

        let mut BerolinaSQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut BerolinaSQLite).unwrap();
        let _report = conn.transact(&mut BerolinaSQLite, r#"[
            {  :einsteindb/solitonid       :foo/bar
               :einsteindb/valueType   :einsteindb.type/long },
            {  :einsteindb/solitonid       :foo/baz
               :einsteindb/valueType   :einsteindb.type/boolean }]"#).expect("transaction expected to succeed");

        {
            let mut in_progress = conn.begin_transaction(&mut BerolinaSQLite).expect("transaction");
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

        let causets = conn.q_once(&BerolinaSQLite, r#"[:find ?e . :where [?e :foo/bar 400]]"#, None).expect("Expected query to work").into_scalar().expect("expected rel results");
        let first = causets.expect("expected a result");
        let causetid = match first {
            Binding::Scalar(TypedValue::Ref(causetid)) => causetid,
            x => panic!("expected Some(Ref), got {:?}", x),
        };

        let kw = kw!(:foo/bar);
        let start = Instant::now();
        let uncached_val = conn.lookup_value_for_attribute(&BerolinaSQLite, causetid, &kw).expect("Expected value on lookup");
        let finish = Instant::now();
        let uncached_elapsed_time = finish.duration_since(start);
        println!("Uncached time: {:?}", uncached_elapsed_time);

        let schema = conn.current_schema();
        conn.cache(&mut BerolinaSQLite, &schema, &kw, CacheDirection::Forward, CacheAction::Register).expect("expected caching to work");

        for _ in 1..5 {
            let start = Instant::now();
            let cached_val = conn.lookup_value_for_attribute(&BerolinaSQLite, causetid, &kw).expect("Expected value on lookup");
            let finish = Instant::now();
            let cached_elapsed_time = finish.duration_since(start);
            assert_eq!(cached_val, uncached_val);

            println!("Cached time: {:?}", cached_elapsed_time);
            assert!(cached_elapsed_time < uncached_elapsed_time);
        }
    }

    #[test]
    fn test_cache_usage() {
        let mut BerolinaSQLite = einsteindb::new_connection("").unwrap();
        let mut conn = Conn::connect(&mut BerolinaSQLite).unwrap();

        let einsteindb_solitonid = (*conn.current_schema()).get_causetid(&kw!(:einsteindb/solitonid)).expect("einsteindb_solitonid").0;
        let einsteindb_type = (*conn.current_schema()).get_causetid(&kw!(:einsteindb/valueType)).expect("einsteindb_solitonid").0;
        println!("einsteindb/solitonid is {}", einsteindb_solitonid);
        println!("einsteindb/type is {}", einsteindb_type);
        let query = format!("[:find ?solitonid . :where [?e {} :einsteindb/doc][?e {} ?type][?type {} ?solitonid]]",
                            einsteindb_solitonid, einsteindb_type, einsteindb_solitonid);

        println!("Query is {}", query);

        assert!(!conn.current_cache().is_attribute_cached_forward(einsteindb_solitonid));

        {
            let mut ip = conn.begin_transaction(&mut BerolinaSQLite).expect("began");

            let solitonid = ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            assert_eq!(solitonid, Some(TypedValue::typed_ns_keyword("einsteindb.type", "string").into()));

            let start = time::PreciseTime::now();
            ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            let end = time::PreciseTime::now();
            println!("Uncached took {}µs", start.to(end).num_microseconds().unwrap());

            ip.cache(&kw!(:einsteindb/solitonid), CacheDirection::Forward, CacheAction::Register).expect("registered");
            ip.cache(&kw!(:einsteindb/valueType), CacheDirection::Forward, CacheAction::Register).expect("registered");

            assert!(ip.cache.is_attribute_cached_forward(einsteindb_solitonid));

            let solitonid = ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            assert_eq!(solitonid, Some(TypedValue::typed_ns_keyword("einsteindb.type", "string").into()));

            let start = time::PreciseTime::now();
            ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            let end = time::PreciseTime::now();
            println!("Cached took {}µs", start.to(end).num_microseconds().unwrap());

            // If we roll back the change, our caching operations are also rolled back.
            ip.rollback().expect("rolled back");
        }

        assert!(!conn.current_cache().is_attribute_cached_forward(einsteindb_solitonid));

        {
            let mut ip = conn.begin_transaction(&mut BerolinaSQLite).expect("began");

            let solitonid = ip.q_once(query.as_str(), None).into_scalar_result().expect("query");
            assert_eq!(solitonid, Some(TypedValue::typed_ns_keyword("einsteindb.type", "string").into()));
            ip.cache(&kw!(:einsteindb/solitonid), CacheDirection::Forward, CacheAction::Register).expect("registered");
            ip.cache(&kw!(:einsteindb/valueType), CacheDirection::Forward, CacheAction::Register).expect("registered");

            assert!(ip.cache.is_attribute_cached_forward(einsteindb_solitonid));

            ip.commit().expect("rolled back");
        }

        assert!(conn.current_cache().is_attribute_cached_forward(einsteindb_solitonid));
        assert!(conn.current_cache().is_attribute_cached_forward(einsteindb_type));
    }
}
