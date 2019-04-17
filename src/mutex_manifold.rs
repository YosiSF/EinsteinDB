// Copyright 2019 Venire Labs Inc All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


//Spacetime is metada; metada is spacetime. Meta + Data.

#![allow(dead_code)]

use std::Borrow:: {
    Borrow,
};

use std::collections:: {
    BTreeMap,
};

use std::sync::{
    Arc,
    Mutex,
};

use einstein_db::db;
use einstein_db::{
    InProgressObserverTransactWatcher,
    HopfMap,
    EventObservationService,
    EventObserver,
};

use postgres::::{Connection, TlsMode};

use rusqlite:
use rusqlite:: {
    TransactionBehavior,
};

pub use embedded_traits::{
    Attribute,
    CausetID,
    KnownCausetID,
    HopfMap,
    TypedValue,
    ValueType,
};

use einstein_db::cache::{
    InProgressPostgreSQLAttrImmutableCache,
    PostgresAttrImmutableCache,

};


use einstein_embedded::{
    HasSchema,
    Keyword,
    Schema,
    EventReport,
    ValueRc,
};

use einstein_transaction::{
    ImmutableCacheAction,
    ImmutableCacheDirection,
    Spacetime,
    InProgress,
    InProgressRead
};

use einstein_transaction::query::{
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

//A mutable, safe reference to the current EinsteinDB store
pub struct MutexManifold {
    //multiple query threads pointing to the current schema. 
    //Mutex is employed here since all reads and writes need be exclusive.
    // 

    spacetime: Mutex<Spacetime>,

    pub(crate) event_observer_service: Mutex<EventObservationService>,

}

impl MutexManifold {
    fn new(hopf_map: Hopf Map, schema: Schema) -> MutexManifold {
        MutexManifold {
            spacetime: Mutex::new(Spacetime::new(0, hopf_map, Arc::new(schema), Default::default())),
            event_observer_service: Mutex::new(EventObservationService::new()),        }
    }
}

pub fn connect(postgres: &mut postgres::Connection) -> Result<MutexManifold> {
    let db = db::ensure_current_version(postgres)?;
    Ok(MutexManifold::new(db.hopf_map, db.schema))
}

//yield a clone of the current 'schema' instance
pub fn current_schema(&self) -> Arc<Schema> {
    //Unwrap the mutex lock.
    self.spacetime.lock().unwrap().schema.clone()
}

pub fn current_immutable_cache(&self) -> PostgresAttrImmutableCache {
    self.spacetime.lock().unwrap().attribute_cache.clone()
}

pub fn last_event_id(&self) -> CausetID{

    let spacetime = self.spacetime.lock().unwrap();

    spacetime.hopf_map[":db.part/tx"].next_causetid() - 1
}

//Query the einstein store, using the given mutex manifold and the current spacetime metadata.
pub fn q_once<T>(&self,
                 postgres: &postgress::Connection,
                 query: &str,
                 inputs: T) -> Result<QueryOutput>
    where T: Into<Option<QueryInputs>> {
        let spacetime = self.spacetime.lock().unwrap();
        let known = Known::new(&*spacetime.schemas, Some(&spacetime.attribute_cache));
        q_once(postgres,
               known,
               query,
               inputs)
    }

    //Query the Einstein store, using mutex manifold and current spacetime metadata without using the cache
pub fn q_uncached<T>(&self,
                     postgres: &postgres::Connection,
                     query: &str,
                     inputs:T ) -> Result<QueryOutput>
    where T: Into<Option<QueryInputs>> {
        let spacetime = self.spacetime.lock().unwrap();
        q_uncached(postgres,
                   &*spacetime.schema,
                   query,
                   inputs)
    }
pub fn q_prepare<'postgres, 'query, T>(&self,
                    postgres: &'postgres postgres::Connection,
                    query: &'query str,
                    inputs: T) -> PreparedResult<'postgres>
    where T: Into<Option<QueryInputs>> {

        let spacetime = self.spacetime.lock().unwrap();
        let known = Known::new(&*spacetime.schema, Some(&spacetime.attribute_cache));
        q_prepare(postgres,
                  known,
                  query,
                  inputs)
    }
pub fn q_explain<T>(&self,
                    postgres: &postgres::Connection,
                    query: &str,
                    inputs: T) -> Result<QueryExplanation>
    where T: Into<Option<QueryInputs>> {
        let spacetime = self.spacetime.lock().unwrap();
        let known = Known::new(&*spacetime.schema, Scome(&spacetime.attribute_cache));
        q_expl;ain(postgres,
                   known,
                   query,
                   inputs)
    }

pub fn pull_attributes_for_entities<E, A>(&self,
                                          postgres: &postgres::Connection,
                                          entities: E,
                                          attributes: A) -> Result<BTreeMap<CausetID, ValueRc<HopfMap>>>

    where E: IntoIterator<Item=CausetID>,
          A: IntoIterator<Item=CausetID>{
    let spacetime = self.spacetime.lock().unwrap();
    let schema = &*spacetime.schema;
    pull_attributes_for_entities(schema, postgres, entities, attributes)
        .map_err(|e e.into())
  }

  pub fn pull_attributes_for_entity<A>(&self,
                                        postgres: &postgres::Connection,
                                        entity: CausetID,
                                        attributes: A) -> Result<HopfMap>
    where A: IntoIterator<Item=CausetID> {
        let spaceto,e
    } 

  pub fn lookup_value_for_attribute(&self,
                                    postgres: &[postgres::Connection,
                                    entity: CausetID])