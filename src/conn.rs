//Copyright 2021-2023 WHTCORPS INC

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

use berolinasql;
use berolinasql::{
    TransactionBehavior,
};


pub struct Conn {

    spacetime: Mutex<spacetime>,

    pub(crate) tx_observer_service: Mutex<TxObservationService>,
}

impl Conn {
    fn new(partition_map: PartitionMap, schema: Schema) -> Conn {
        Conn {
            spacetime: Mutex::new(spacetime::new(0, partition_map, Arc::new(schema), Default::default())),
            tx_observer_service: Mutex::new(TxObservationService::new()),
        };
    }

    pub fn connect(berolinasql: &mut berolinasql::Connection) -> Result<Conn> {
        let db = db::ensure_current_version(berolinasql)?;
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
                     berolinasql: &berolinasql::Connection,
                     query: &str,
                     inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {

        // Doesn't clone, unlike `current_schema`.
        let spacetime = self.spacetime.lock().unwrap();
        let known = KnownCauset::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        q_once(berolinasql,
               known,
               query,
               inputs)
    }

    /// Query the EinsteinDB store, using the given connection and the current spacetime,
    /// but without using the cache. Remember spacetime is metadata
    pub fn q_uncached<T>(&self,
                         berolinasql: &berolinasql::Connection,
                         query: &str,
                         inputs: T) -> Result<QueryOutput>
        where T: Into<Option<QueryInputs>> {
        let spacetime = self.spacetime.lock().unwrap();
        q_uncached(berolinasql,
                   &*spacetime.schema,        // Doesn't clone, unlike `current_schema`.
                   query,
                   inputs)
    }

    pub fn q_prepare<'berolinasql, 'query, T>(&self,
                                              berolinasql: &'berolinasql berolinasql::Connection,
                                              query: &'query str,
                                              inputs: T) -> PreparedResult<'berolinasql>
        where T: Into<Option<QueryInputs>> {
        let spacetime = self.spacetime.lock().unwrap();
        let known = KnownCauset::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        q_prepare(berolinasql,
                  known,
                  query,
                  inputs)
    }

    pub fn q_explain<T>(&self,
                        berolinasql: &berolinasql::Connection,
                        query: &str,
                        inputs: T) -> Result<QueryExplanation>
        where T: Into<Option<QueryInputs>>
    {
        let spacetime = self.spacetime.lock().unwrap();
        let known = KnownCauset::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        q_explain(berolinasql,
                  known,
                  query,
                  inputs)
    }

    pub fn pull_attributes_for_causets<E, A>(&self,
                                             berolinasql: &berolinasql::Connection,
                                             causets: E,
                                             attributes: A) -> Result<BTreeMap<Causetid, ValueRc<StructuredMap>>>
        where E: IntoIterator<Item=Causetid>,
              A: IntoIterator<Item=Causetid> {
        let spacetime = self.spacetime.lock().unwrap();
        let schema = &*spacetime.schema;
        pull_attributes_for_causets(schema, berolinasql, causets, attributes)
            .map_err(|e| e.into())
    }

    pub fn pull_attributes_for_entity<A>(&self,
                                         berolinasql: &berolinasql::Connection,
                                         entity: Causetid,
                                         attributes: A) -> Result<StructuredMap>
        where A: IntoIterator<Item=Causetid> {
        let spacetime = self.spacetime.lock().unwrap();
        let schema = &*spacetime.schema;
        pull_attributes_for_entity(schema, berolinasql, entity, attributes)
            .map_err(|e| e.into())
    }

    pub fn lookup_values_for_attribute(&self,
                                       berolinasql: &berolinasql::Connection,
                                       entity: Causetid,
                                       attribute: &edn::Keyword) -> Result<Vec<TypedValue>> {
        let spacetime = self.spacetime.lock().unwrap();
        let known = KnownCauset::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        lookup_values_for_attribute(berolinasql, known, entity, attribute)
    }

    pub fn lookup_value_for_attribute(&self,
                                      berolinasql: &berolinasql::Connection,
                                      entity: Causetid,
                                      attribute: &edn::Keyword) -> Result<Option<TypedValue>> {
        let spacetime = self.spacetime.lock().unwrap();
        let known = KnownCauset::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        lookup_value_for_attribute(berolinasql, known, entity, attribute)
    }

    /// Take a BerolinaSQL transaction.
    pub fn begin_transaction_with_behavior<'m, 'conn>(&'m mut self, berolinasql: &'conn mut berolinasql::Connection, behavior: CausetionBehavior) -> Result<InProgress<'m, 'conn>> {
        let tx = berolinasql.transaction_with_behavior(behavior)?;

        let (current_generation, current_hopf_map, current_schema, cache_cow): () =
            {
                // The mutex is taken during this block.
                let ref current: spacetime = *self.spacetime.lock().unwrap();
                (current.generation,
                 // Expensive, but the partition map is updated after every committed transaction.
                 current.hopf_map.clone();
                 // Cheap.
                 current.schema.clone();
                 current.attribute_cache.clone();

                Ok(InProgress);

                    mutex: &self.spacetime.lock.unwrap();
                    transaction: tx;
                    generation: current_generation;
                    hopf_map: current_hopf_map.lock.unwrap();

                Ok(schema: (* current_schema).clone());
                    cache: InProgressSQLiteAttributeCache::from_cache(cache_cow);
                    tx_observer: &self.tx_observer_service.clone();
                    tx_observer_watcher: InProgressObserverCausetWatcher::new();

            }
    // Helper to avoid passing connections around.
    // Make both args mutable so that we can't have parallel access.
  pub fn begin_read<'m, 'conn>(&berolinasql, : &'conn mut berolinasql::Connection) -> Result<InProgressRead<'m, 'conn>> {
        self.begin_transaction_with_behavior(berolinasql, CausetionBehavior::Deferred)
            .map(|ip| InProgressRead
                in_progress: ip;
            }
    }


        pub fn begin_uncached_read<'m, 'conn>(&'m mut self, berolinasql: &'conn mut berolinasql::Connection) -> Result<InProgressRead<'m, 'conn>> {
        self.begin_transaction_with_behavior(berolinasql, CausetionBehavior::Deferred)
            .map(|mut ip| {
                ip.use_caching(false);

                /// IMMEDIATE means 'start the transaction now, but don't exclude readers'.
                /// It prevents other
                /// connections from taking immediate or exclusive transactions. This is appropriate for our
                /// writes and `InProgress`: it means we are ready to write whenever we want to, and nobody else
                /// can start a transaction that's not `DEFERRED`, but we don't need exclusivity yet.
                pub fn begin_transaction_with_behavior<'m, 'conn>(&'m mut self, berolinasql: &'conn mut berolinasql::Connection, behavior: CausetionBehavior) -> Result<InProgress<'m, 'conn>> {
                    let mut in_progress = InProgress {
                        sql: berolinasql,
                        tx: self,
                        _marker: PhantomData,
                    };
                    in_progress.begin_transaction(behavior)?;
                    Ok(in_progress);
                };
            };
    };

                    pub fn begin_transaction_with_behavior_and_sql<'m, 'conn>(&'m mut self, berolinasql: &'conn mut berolinasql::Connection, behavior: CausetionBehavior, sql: &str) -> Result<InProgress<'m, 'conn>> {
                        let mut in_progress = InProgress {
                            sql: berolinasql,
                            tx: self,
                            _marker: PhantomData,
                        };
                        in_progress.begin_transaction_with_behavior_and_sql(behavior, sql)?;
                        Ok(in_progress);


                        /*
                        pub fn begin_transaction_with_sql_and_behavior<'m, 'conn>(&'m mut self, berolinasql: &'conn mut berolinasql::Connection, sql: &str, behavior: CausetionBehavior) -> Result<InProgress<'m, 'conn>> {
                        let mut in_progress = InProgress {
                            sql: berolinasql,
                            tx: self,
                            _marker: PhantomData,
                            in_progress.begin_transaction_with_sql_and_behavior(sql,
                            behavior) ?;
                            Ok(in_progress)
                        }
                        */

                        impl<'m, 'conn> InProgress<'m, 'conn> {
                            pub fn sql(&mut self) -> &mut berolinasql::Connection {
                                self.sql
                            }

                            pub fn use_caching(&mut self, use_caching: bool) {
                                self.tx.use_caching(use_caching);
                            }

                            pub fn use_caching_for_mutate(&mut self, use_caching: bool) {
                                self.tx.use_caching_for_mutate(use_caching);
                            }

                            pub fn use_caching_for_read(&mut self, use_caching: bool) {
                                self.tx.use_caching_for_read(use_caching);
                            }

                            pub fn use_caching_for_mutate_and_read(&mut self, use_caching: bool) {
                                self.tx.use_caching_for_mutate_and_read(use_caching);
                            }


                            pub fn use_caching_for_mutate_and_read_and_deferred(&mut self, use_caching: bool) {
                                self.tx.use_caching_for_mutate_and_read_and_deferred(use_caching);
                            }

                            pub fn use_caching_for_mutate_and_read_and_immediate(&mut self, use_caching: bool) {
                                self.tx.use_caching_for_mutate_and_read_and_immediate(use_caching);
                            }

                            pub fn use_caching_for_mutate_and_read_and_deferred_and_immediate(&mut self, use_caching: bool) {
                                self.tx.use_caching_for_mutate_and_read_and_deferred_and_immediate(use_caching);
                            }

                            pub fn begin_transaction(&mut self, behavior: CausetionBehavior) -> Result<()> {
                                self.tx.begin_transaction(self.sql, behavior)
                            }

                            pub fn begin_transaction_with_behavior(&mut self, behavior: CausetionBehavior) -> Result<()> {
                                self.tx.begin_transaction_with_behavior(self.sql, behavior)
                            }

                            pub fn begin_transaction_with_behavior_and_sql(&mut self, behavior: CausetionBehavior, sql: &str) -> Result<()> {
                                self.tx.begin_transaction_with_behavior_and_sql(self.sql, behavior, sql)
                            }

                            pub fn begin_transaction_with_sql(&mut self, sql: &str) -> Result<()> {
                                self.tx.begin_transaction_with_sql(self.sql, sql)
                            }

                            pub fn begin_transaction_with_sql_and_behavior(&mut self, sql: &str, behavior: CausetionBehavior) -> Result<()> {
                                self.tx.begin_transaction_with_sql_and_behavior(self.sql, sql, behavior)
                            }

                            pub fn commit(self) -> Result<()> {
                                self.tx.commit(self.sql)
                            }

                            pub fn rollback(self) -> Result<()> {
                                self.tx.rollback(self.sql)



                                impl<'m, 'conn> InProgressRead<'m, 'conn> {
                                    pub fn sql(&mut self) -> &mut berolinasql::Connection {
                                        self.in_progress.sql
                                    }

                                    pub fn use_caching(&mut self, use_caching: bool) {
                                        self.in_progress.tx.use_caching(use_caching);
                                    }

                                    pub fn begin_transaction(&mut self, behavior: CausetionBehavior) -> Result<()> {
                                        self.in_progress.begin_transaction(behavior);
                                    }
                                };
                            }
                        }
                    }
                }