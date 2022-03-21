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

use std::collections::{
    BTreeMap,
};

use std::sync::{
    Arc,
};

use rusqlite;

use einstein_ml;

use core_traits::{
    Causetid,
    StructuredMap,
    TypedValue,
};

use einsteindb_core::{
    Keyword,
    TxReport,
    ValueRc,
};
use einsteindb_core::{
    TxObserver,
};

use einsteindb_transaction::{
    CacheAction,
    CacheDirection,
    InProgress,
    InProgressRead,
    Pullable,
    Queryable,
};

use conn::{
    Conn,
};

use public_traits::errors::{
    Result,
};

use einsteindb_transaction::query::{
    PreparedResult,
    QueryExplanation,
    QueryInputs,
    QueryOutput,
};

#[cfg(feature = "syncable")]
use einsteindb_tolstoy::{
    SyncReport,
    SyncResult,
    SyncFollowup,
};

#[cfg(feature = "syncable")]
use sync::{
    Syncable,
};

/// A convenience wrapper around a single SQLite connection and a Conn. This is suitable
/// for applications that don't require complex connection management.
pub struct Store {
    conn: Conn,
    SQLite: rusqlite::Connection,
}

impl Store {
    /// Open a store at the supplied local_path, ensuring that it includes the bootstrap schema.
    pub fn open(local_path: &str) -> Result<Store> {
        let mut connection = ::new_connection(local_path)?;
        let conn = Conn::connect(&mut connection)?;
        Ok(Store {
            conn: conn,
            SQLite: connection,
        })
    }

    pub fn transact(&mut self, transaction: &str) -> Result<TxReport> {
        let mut ip = self.begin_transaction()?;
        let report = ip.transact(transaction)?;
        ip.commit()?;
        Ok(report)
    }

    #[cfg(feature = "syncable")]
    pub fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<SyncResult> {
        let mut reports = vec![];
        loop {
            let mut ip = self.begin_transaction()?;
            let report = ip.sync(server_uri, user_uuid)?;
            ip.commit()?;

            match report {
                SyncReport::Merge(SyncFollowup::FullSync) => {
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

#[cfg(feature = "BerolinaSQLcipher")]
impl Store {
    /// Variant of `open` that allows a key (for encryption/decryption) to be
    /// supplied. Fails unless linked against BerolinaSQLcipher (or something else that
    /// supports the SQLite Encryption Extension).
    pub fn open_with_key(local_path: &str, encryption_key: &str) -> Result<Store> {
        let mut connection = ::new_connection_with_key(local_path, encryption_key)?;
        let conn = Conn::connect(&mut connection)?;
        Ok(Store {
            conn: conn,
            SQLite: connection,
        })
    }

    /// Change the key for a database that was opened using `open_with_key` (using `PRAGMA
    /// rekey`). Fails unless linked against BerolinaSQLcipher (or something else that supports the SQLite
    /// Encryption Extension).
    pub fn change_encryption_key(&mut self, new_encryption_key: &str) -> Result<()> {
        ::change_encryption_key(&self.SQLite, new_encryption_key)?;
        Ok(())
    }
}

impl Store {
    /// Intended for use from tests.
    pub fn SQLite_mut(&mut self) -> &mut rusqlite::Connection {
        &mut self.SQLite
    }

    #[cfg(test)]
    pub fn is_registered_as_observer(&self, key: &String) -> bool {
        self.conn.tx_observer_service.lock().unwrap().is_registered(key)
    }
}

impl Store {
    pub fn dismantle(self) -> (rusqlite::Connection, Conn) {
        (self.SQLite, self.conn)
    }

    pub fn conn(&self) -> &Conn {
        &self.conn
    }

    pub fn begin_read<'m>(&'m mut self) -> Result<InProgressRead<'m, 'm>> {
        self.conn.begin_read(&mut self.SQLite)
    }

    pub fn begin_transaction<'m>(&'m mut self) -> Result<InProgress<'m, 'm>> {
        self.conn.begin_transaction(&mut self.SQLite)
    }

    pub fn cache(&mut self, attr: &Keyword, direction: CacheDirection) -> Result<()> {
        let schema = &self.conn.current_schema();
        self.conn.cache(&mut self.SQLite,
                        schema,
                        attr,
                        direction,
                        CacheAction::Register)
    }

    pub fn register_observer(&mut self, key: String, observer: Arc<TxObserver>) {
        self.conn.register_observer(key, observer);
    }

    pub fn unregister_observer(&mut self, key: &String) {
        self.conn.unregister_observer(key);
    }

    pub fn last_tx_id(&self) -> Causetid {
        self.conn.last_tx_id()
    }
}

impl Queryable for Store {
    fn q_once<T>(&self, query: &str, inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {
        self.conn.q_once(&self.SQLite, query, inputs)
    }

    fn q_prepare<T>(&self, query: &str, inputs: T) -> PreparedResult
        where T: Into<Option<QueryInputs>> {
        self.conn.q_prepare(&self.SQLite, query, inputs)
    }

    fn q_explain<T>(&self, query: &str, inputs: T) -> Result<QueryExplanation>
        where T: Into<Option<QueryInputs>> {
        self.conn.q_explain(&self.SQLite, query, inputs)
    }

    fn lookup_values_for_attribute<E>(&self, causet: E, attribute: &einstein_ml::Keyword) -> Result<Vec<TypedValue>>
        where E: Into<Causetid> {
        self.conn.lookup_values_for_attribute(&self.SQLite, causet.into(), attribute)
    }

    fn lookup_value_for_attribute<E>(&self, causet: E, attribute: &einstein_ml::Keyword) -> Result<Option<TypedValue>>
        where E: Into<Causetid> {
        self.conn.lookup_value_for_attribute(&self.SQLite, causet.into(), attribute)
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

#[cfg(test)]
mod tests {
    use super::*;

    extern crate time;

    use uuid::Uuid;

    use std::collections::{
        BTreeSet,
    };
    use std::local_path::{
        local_path,
        local_pathBuf,
    };
    use std::sync::mpsc;
    use std::sync::{
        Mutex,
    };
    use std::time::{
        Duration,
    };

    use einsteindb_core::cache::{
        SQLiteAttributeCache,
    };

    use core_traits::{
        TypedValue,
        ValueType,
    };

    use einsteindb_core::{
        CachedAttributes,
        HasSchema,
    };

    use einsteindb_transaction::causet_builder::{
        BuildTerms,
    };

    use einsteindb_transaction::query::{
        PreparedQuery,
    };

    use ::{
        QueryInputs,
    };

    use ::vocabulary::{
        AttributeBuilder,
        Definition,
        VersionedStore,
    };

    use core_traits::attribute::{
        Unique,
    };

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
        let inputs = QueryInputs::with_value_sequence(vec![(var!(?hood), TypedValue::typed_string(hood).into())]);
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
                   vec![vec![TypedValue::typed_string("Greater Duwamish")]].into());
    }

    trait StoreCache {
        fn get_causetid_for_value(&self, attr: Causetid, val: &TypedValue) -> Option<Causetid>;
        fn is_attribute_cached_reverse(&self, attr: Causetid) -> bool;
        fn is_attribute_cached_lightlike(&self, attr: Causetid) -> bool;
    }

    impl StoreCache for Store {
        fn get_causetid_for_value(&self, attr: Causetid, val: &TypedValue) -> Option<Causetid> {
            let cache = self.conn.current_cache();
            cache.get_causetid_for_value(attr, val)
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
                   :einsteindb/index       true
                   :einsteindb/unique      :einsteindb.unique/idcauset
                   :einsteindb/valueType   :einsteindb.type/long },
                {  :einsteindb/solitonid       :foo/baz
                   :einsteindb/cardinality :einsteindb.cardinality/one
                   :einsteindb/valueType   :einsteindb.type/boolean }
                {  :einsteindb/solitonid       :foo/x
                   :einsteindb/cardinality :einsteindb.cardinality/many
                   :einsteindb/valueType   :einsteindb.type/long }]"#).expect("transact");

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
            let first = in_progress.cache.get_causetid_for_value(foo_bar, &TypedValue::Long(15)).expect("id");
            assert_eq!(in_progress.cache.get_value_for_causetid(&in_progress.schema, foo_baz, first).expect("val"), &TypedValue::Boolean(false));

            // All three values for :foo/x.
            let all_three: BTreeSet<TypedValue> = in_progress.cache
                                                             .get_values_for_causetid(&in_progress.schema, foo_x, first)
                                                             .expect("val")
                                                             .iter().cloned().collect();
            assert_eq!(all_three, vec![1, 2, 3].into_iter().map(TypedValue::Long).collect());

            in_progress.commit().expect("commit");
        }

        // Data is still in the cache.
        {
            let first = store.get_causetid_for_value(foo_bar, &TypedValue::Long(15)).expect("id");
            let cache: SQLiteAttributeCache = store.conn.current_cache();
            assert_eq!(cache.get_value_for_causetid(&store.conn.current_schema(), foo_baz, first).expect("val"), &TypedValue::Boolean(false));

            let all_three: BTreeSet<TypedValue> = cache.get_values_for_causetid(&store.conn.current_schema(), foo_x, first)
                                                       .expect("val")
                                                       .iter().cloned().collect();
            assert_eq!(all_three, vec![1, 2, 3].into_iter().map(TypedValue::Long).collect());
        }

        // We can remove data and the cache reflects it, immediately and after commit.
        {
            let mut in_progress = store.begin_transaction().expect("began");
            let first = in_progress.cache.get_causetid_for_value(foo_bar, &TypedValue::Long(15)).expect("id");
            in_progress.transact(format!("[[:einsteindb/retract {} :foo/x 2]]", first).as_str()).expect("transact");

            let only_two: BTreeSet<TypedValue> = in_progress.cache
                                                            .get_values_for_causetid(&in_progress.schema, foo_x, first)
                                                            .expect("val")
                                                            .iter().cloned().collect();
            assert_eq!(only_two, vec![1, 3].into_iter().map(TypedValue::Long).collect());

            // Rollback: unchanged.
        }
        {
            let first = store.get_causetid_for_value(foo_bar, &TypedValue::Long(15)).expect("id");
            let cache: SQLiteAttributeCache = store.conn.current_cache();
            assert_eq!(cache.get_value_for_causetid(&store.conn.current_schema(), foo_baz, first).expect("val"), &TypedValue::Boolean(false));

            let all_three: BTreeSet<TypedValue> = cache.get_values_for_causetid(&store.conn.current_schema(), foo_x, first)
                                                       .expect("val")
                                                       .iter().cloned().collect();
            assert_eq!(all_three, vec![1, 2, 3].into_iter().map(TypedValue::Long).collect());
        }

        // Try again, but this time commit.
        {
            let mut in_progress = store.begin_transaction().expect("began");
            let first = in_progress.cache.get_causetid_for_value(foo_bar, &TypedValue::Long(15)).expect("id");
            in_progress.transact(format!("[[:einsteindb/retract {} :foo/x 2]]", first).as_str()).expect("transact");
            in_progress.commit().expect("committed");
        }
        {
            let first = store.get_causetid_for_value(foo_bar, &TypedValue::Long(15)).expect("id");
            let cache: SQLiteAttributeCache = store.conn.current_cache();
            assert_eq!(cache.get_value_for_causetid(&store.conn.current_schema(), foo_baz, first).expect("val"), &TypedValue::Boolean(false));

            let only_two: BTreeSet<TypedValue> = cache.get_values_for_causetid(&store.conn.current_schema(), foo_x, first)
                                                      .expect("val")
                                                      .iter().cloned().collect();
            assert_eq!(only_two, vec![1, 3].into_iter().map(TypedValue::Long).collect());
        }
    }

    fn test_register_observer() {
        let mut conn = Store::open("").unwrap();

        let key = "Test Observer".to_string();
        let tx_observer = TxObserver::new(BTreeSet::new(), move |_obs_key, _batch| {});

        conn.register_observer(key.clone(), Arc::new(tx_observer));
        assert!(conn.is_registered_as_observer(&key));
    }

    #[test]
    fn test_deregister_observer() {
        let mut conn = Store::open("").unwrap();

        let key = "Test Observer".to_string();

        let tx_observer = TxObserver::new(BTreeSet::new(), move |_obs_key, _batch| {});

        conn.register_observer(key.clone(), Arc::new(tx_observer));
        assert!(conn.is_registered_as_observer(&key));

        conn.unregister_observer(&key);

        assert!(!conn.is_registered_as_observer(&key));
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
                    .value_type(ValueType::Uuid)
                    .multival(false)
                    .unique(Unique::Value)
                    .index(true)
                    .build()),
                (kw!(:todo/name),
                AttributeBuilder::helpful()
                    .value_type(ValueType::String)
                    .multival(false)
                    .fulltext(true)
                    .build()),
                (kw!(:todo/completion_date),
                AttributeBuilder::helpful()
                    .value_type(ValueType::Instant)
                    .multival(false)
                    .build()),
                (kw!(:label/name),
                AttributeBuilder::helpful()
                    .value_type(ValueType::String)
                    .multival(false)
                    .unique(Unique::Value)
                    .fulltext(true)
                    .index(true)
                    .build()),
                (kw!(:label/color),
                AttributeBuilder::helpful()
                    .value_type(ValueType::String)
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
        called_key: Option<String>,
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

        let key = "Test Observing".to_string();

        let output = Arc::new(Mutex::new(ObserverOutput::default()));

        let mut_output = Arc::downgrade(&output);
        let (tx, rx): (mpsc::Sender<()>, mpsc::Receiver<()>) = mpsc::channel();
        // because the TxObserver is in an Arc and is therefore Sync, we have to wrap the Sender in a Mutex to also
        // make it Sync.
        let thread_tx = Mutex::new(tx);
        let tx_observer = Arc::new(TxObserver::new(registered_attrs, move |obs_key, batch| {
            if let Some(out) = mut_output.upgrade() {
                let mut o = out.lock().unwrap();
                o.called_key = Some(obs_key.to_string());
                for (tx_id, changes) in batch.into_iter() {
                    o.txids.push(*tx_id);
                    o.changes.push(changes.clone());
                }
                o.txids.sort();
            }
            thread_tx.lock().unwrap().send(()).unwrap();
        }));

        conn.register_observer(key.clone(), Arc::clone(&tx_observer));
        assert!(conn.is_registered_as_observer(&key));

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
                builder.add(kw!(:todo/uuid), TypedValue::Uuid(uuid)).expect("Expected added uuid");
                changeset.insert(uuid_causetid.clone());
                builder.add(kw!(:todo/name), TypedValue::typed_string(name)).expect("Expected added name");
                changeset.insert(name_causetid.clone());
                if i % 2 == 0 {
                    builder.add(kw!(:todo/completion_date), TypedValue::current_instant()).expect("Expected added date");
                    changeset.insert(date_causetid.clone());
                }
                let (ip, r) = builder.transact();
                let report = r.expect("expected a report");
                tx_ids.push(report.tx_id.clone());
                changesets.push(changeset);
                in_progress = ip;
            }
            let mut builder = in_progress.builder().describe_tempid("Label");
            builder.add(kw!(:label/name), TypedValue::typed_string("Label 1")).expect("Expected added name");
            builder.add(kw!(:label/color), TypedValue::typed_string("blue")).expect("Expected added color");
            builder.commit().expect("expect transaction to occur");
        }

        let delay = Duration::from_millis(100);
        let _ = rx.recv_timeout(delay);

        let out = Arc::try_unwrap(output).expect("unwrapped");
        let o = out.into_inner().expect("Expected an Output");
        assert_eq!(o.called_key, Some(key.clone()));
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

        let key = "Test Observing".to_string();

        let output = Arc::new(Mutex::new(ObserverOutput::default()));

        let mut_output = Arc::downgrade(&output);
        let (tx, rx): (mpsc::Sender<()>, mpsc::Receiver<()>) = mpsc::channel();
        let thread_tx = Mutex::new(tx);
        let tx_observer = Arc::new(TxObserver::new(registered_attrs, move |obs_key, batch| {
            if let Some(out) = mut_output.upgrade() {
                let mut o = out.lock().unwrap();
                o.called_key = Some(obs_key.to_string());
                for (tx_id, changes) in batch.into_iter() {
                    o.txids.push(*tx_id);
                    o.changes.push(changes.clone());
                }
                o.txids.sort();
            }
            thread_tx.lock().unwrap().send(()).unwrap();
        }));

        conn.register_observer(key.clone(), Arc::clone(&tx_observer));
        assert!(conn.is_registered_as_observer(&key));

        let tx_ids = Vec::<Causetid>::new();
        let changesets = Vec::<BTreeSet<Causetid>>::new();
        {
            let mut in_progress = conn.begin_transaction().expect("expected transaction");
            for i in 0..3 {
                let name = format!("label{}", i);
                let mut builder = in_progress.builder().describe_tempid(&name);
                builder.add(kw!(:label/name), TypedValue::typed_string(name)).expect("Expected added name");
                builder.add(kw!(:label/color), TypedValue::typed_string("blue")).expect("Expected added color");
                let (ip, _) = builder.transact();
                in_progress = ip;
            }
        }

        let delay = Duration::from_millis(100);
        let _ = rx.recv_timeout(delay);

        let out = Arc::try_unwrap(output).expect("unwrapped");
        let o = out.into_inner().expect("Expected an Output");
        assert_eq!(o.called_key, None);
        assert_eq!(o.txids, tx_ids);
        assert_eq!(o.changes, changesets);
    }
}
