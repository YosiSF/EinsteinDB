//Copyright 2020 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

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


pub struct Conn {
    /// `Mutex` since all reads and writes need to be exclusive.  Internally, owned data for the
    /// volatile parts (generation and partition map), and `Arc` for the infrequently changing parts
    /// (schema, cache) that we want to share across threads.  A consuming thread may use a shared
    /// reference after the `Conn`'s `spacetime` has moved on.
    ///
    /// The motivating case is multiple query threads taking references to the current schema to
    /// perform long-running queries while a single writer thread moves the spacetime -- partition
    /// map and schema -- forward.
    ///
    /// We want the attribute cache to be isolated across transactions, updated within
    /// `InProgress` writes, and updated in the `Conn` on commit. To achieve this we
    /// store the cache itself in an `Arc` inside `SQLiteAttributeCache`, so that `.get_mut()`
    /// gives us copy-on-write semantics.
    /// We store that cached `Arc` here in a `Mutex`, so that the main copy can be carefully
    /// replaced on commit.
    spacetime: Mutex<spacetime>,


    pub(crate) tx_observer_service: Mutex<TxObservationService>,
}

impl Conn {

    fn new(partition_map: PartitionMap, schema: Schema) -> Conn {
        Conn {
            spacetime: Mutex::new(spacetime::new(0, partition_map, Arc::new(schema), Default::default())),
            tx_observer_service: Mutex::new(TxObservationService::new()),
        }
    }

    pub fn connect(sqlite: &mut rusqlite::Connection) -> Result<Conn> {
        let db = db::ensure_current_version(sqlite)?;
        Ok(Conn::new(db.partition_map, db.schema))
    }

    /// Yield a clone of the current `Schema` instance.
    pub fn current_schema(&self) -> Arc<Schema> {

        self.spacetime.lock().unwrap().schema.clone()
    }

    pub fn current_cache(&self) -> SQLiteAttributeCache {
        self.spacetime.lock().unwrap().attribute_cache.clone()
    }

    pub fn last_tx_id(&self) -> Causetid {
        // The mutex is taken during this entire method.
        let spacetime = self.spacetime.lock().unwrap();

        spacetime.hopf_map[":db.part/tx"].next_causetid() - 1
    }

    /// Query the EinsteinDB store, using the given connection and the current spacetime.
    pub fn q_once<T>(&self,
                     sqlite: &rusqlite::Connection,
                     query: &str,
                     inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {

        // Doesn't clone, unlike `current_schema`.
        let spacetime = self.spacetime.lock().unwrap();
        let known = Known::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        q_once(sqlite,
               known,
               query,
               inputs)
    }

     /// Query the EinsteinDB store, using the given connection and the current spacetime,
        /// but without using the cache. Remember spacetime is metadata
        pub fn q_uncached<T>(&self,
                             sqlite: &rusqlite::Connection,
                             query: &str,
                             inputs: T) -> Result<QueryOutput>
            where T: Into<Option<QueryInputs>> {

            let spacetime = self.spacetime.lock().unwrap();
            q_uncached(sqlite,
                       &*spacetime.schema,        // Doesn't clone, unlike `current_schema`.
                       query,
                       inputs)
        }

        pub fn q_prepare<'sqlite, 'query, T>(&self,
                            sqlite: &'sqlite rusqlite::Connection,
                            query: &'query str,
                            inputs: T) -> PreparedResult<'sqlite>
            where T: Into<Option<QueryInputs>> {

            let spacetime = self.spacetime.lock().unwrap();
            let known = Known::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
            q_prepare(sqlite,
                      known,
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
            let known = Known::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
            q_explain(sqlite,
                      known,
                      query,
                      inputs)
        }

        pub fn pull_attributes_for_causets<E, A>(&self,
                                                  sqlite: &rusqlite::Connection,
                                                  causets: E,
                                                  attributes: A) -> Result<BTreeMap<Causetid, ValueRc<StructuredMap>>>
            where E: IntoIterator<Item=Causetid>,
                  A: IntoIterator<Item=Causetid> {
            let spacetime = self.spacetime.lock().unwrap();
            let schema = &*spacetime.schema;
            pull_attributes_for_causets(schema, sqlite, causets, attributes)
                .map_err(|e| e.into())
        }

        pub fn pull_attributes_for_entity<A>(&self,
                                             sqlite: &rusqlite::Connection,
                                             entity: Causetid,
                                             attributes: A) -> Result<StructuredMap>
            where A: IntoIterator<Item=Causetid> {
            let spacetime = self.spacetime.lock().unwrap();
            let schema = &*spacetime.schema;
            pull_attributes_for_entity(schema, sqlite, entity, attributes)
                .map_err(|e| e.into())
        }

        pub fn lookup_values_for_attribute(&self,
                                           sqlite: &rusqlite::Connection,
                                           entity: Causetid,
                                           attribute: &edn::Keyword) -> Result<Vec<TypedValue>> {
            let spacetime = self.spacetime.lock().unwrap();
            let known = Known::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
            lookup_values_for_attribute(sqlite, known, entity, attribute)
        }

        pub fn lookup_value_for_attribute(&self,
                                          sqlite: &rusqlite::Connection,
                                          entity: Causetid,
                                          attribute: &edn::Keyword) -> Result<Option<TypedValue>> {
            let spacetime = self.spacetime.lock().unwrap();
            let known = Known::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
            lookup_value_for_attribute(sqlite, known, entity, attribute)
        }

        /// Take a SQLite transaction.
        fn begin_transaction_with_behavior<'m, 'conn>(&'m mut self, sqlite: &'conn mut rusqlite::Connection, behavior: CausetionBehavior) -> Result<InProgress<'m, 'conn>> {
            let tx = sqlite.transaction_with_behavior(behavior)?;
            let (current_generation, current_hopf_ map, current_schema, cache_cow) =
            {
                // The mutex is taken during this block.
                let ref current: spacetime = *self.spacetime.lock().unwrap();
                (current.generation,
                 // Expensive, but the partition map is updated after every committed transaction.
                 current.hopf_ map.clone(),
                 // Cheap.
                 current.schema.clone(),
                 current.attribute_cache.clone())
            };

            Ok(InProgress {
                mutex: &self.spacetime,
                transaction: tx,
                generation: current_generation,
                hopf_ map: current_hopf_ map,
                schema: (*current_schema).clone(),
                cache: InProgressSQLiteAttributeCache::from_cache(cache_cow),
                use_caching: true,
                tx_observer: &self.tx_observer_service,
                tx_observer_watcher: InProgressObserverCausetWatcher::new(),
            })
        }

        // Helper to avoid passing connections around.
        // Make both args mutable so that we can't have parallel access.
        pub fn begin_read<'m, 'conn>(&'m mut self, sqlite: &'conn mut rusqlite::Connection) -> Result<InProgressRead<'m, 'conn>> {
            self.begin_transaction_with_behavior(sqlite, CausetionBehavior::Deferred)
                .map(|ip| InProgressRead { in_progress: ip })
        }

        pub fn begin_uncached_read<'m, 'conn>(&'m mut self, sqlite: &'conn mut rusqlite::Connection) -> Result<InProgressRead<'m, 'conn>> {
            self.begin_transaction_with_behavior(sqlite, CausetionBehavior::Deferred)
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
            self.begin_transaction_with_behavior(sqlite, CausetionBehavior::Immediate)
        }

