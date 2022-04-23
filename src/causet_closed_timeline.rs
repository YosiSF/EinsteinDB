//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED
// APACHE 2.0 COMMUNITY EDITION SL
//
////////////////////////////////////////////////////////////////////////////////
// AUTHORS: WHITFORD LEDER
////////////////////////////////////////////////////////////////////////////////
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
////////////////////////////////////////////////////////////////////////////////

//! # Causet Closed Timeline
//!
//! This is a causet timeline implementation.
//! It is a closed timeline, which means that the timeline is not open for new
//! events.
//! It is a causet timeline, which means that the timeline is causet.
/// Collects a supplied tx range into an DESC ordered Vec of valid txs,
/// ensuring they all belong to the same timeline.
/// The txs are collected in DESC order, so the first tx is the latest tx.
/// You have three modalities with EinsteinDB: Lightlike transactions,  Heavy   transactions, and
/// Full transactions. Lightlike transactions are hot transactions, which are
/// executed in a single thread. Heavy transactions are cold transactions, which
/// are executed in multiple threads. Full transactions are transactions that
/// are executed in multiple threads, but are not heavy.


use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use std::time::{Duration, Instant};
use std::fmt::{Debug, Formatter, Error};
use std::cmp::Ordering::{Equal, Greater, Less};
use std::cmp::{max, min};
use std::hash::Hash;
use std::marker::PhantomData;
use std::mem;


use causet::{Causet, CausetError, CausetResult, CausetOption, CausetOptionResult};
use causet::{CausetTimeline, CausetTimelineError, CausetTimelineResult, CausetTimelineOption, CausetTimelineOptionResult};
use causet::{CausetTimelineOptionResult, CausetTimelineOption, CausetTimelineOptionResult};


use causet_timeline::{CausetTimeline, CausetTimelineError, CausetTimelineResult, CausetTimelineOption, CausetTimelineOptionResult};
use causet_timeline::{CausetTimelineOptionResult, CausetTimelineOption, CausetTimelineOptionResult};

use allegro_poset::{AllegroPoset, AllegroPosetError, AllegroPosetResult, AllegroPosetOption, AllegroPosetOptionResult};
use allegro_poset::{AllegroPosetOptionResult, AllegroPosetOption, AllegroPosetOptionResult};

use soliton::{Soliton, SolitonError, SolitonResult, SolitonOption, SolitonOptionResult};
use einsteindb::{Einsteindb, EinsteindbError, EinsteindbResult, EinsteindbOption, EinsteindbOptionResult};
use einsteindb::{EinsteindbOptionResult, EinsteindbOption, EinsteindbOptionResult};
use foundationdb::{Foundationdb, FoundationdbError, FoundationdbResult, FoundationdbOption, FoundationdbOptionResult};
use foundationdb::{FoundationdbOptionResult, FoundationdbOption, FoundationdbOptionResult};
//gremlin
use gremlin::{Gremlin, GremlinError, GremlinResult, GremlinOption, GremlinOptionResult};
use gremlin::{GremlinOptionResult, GremlinOption, GremlinOptionResult};
//istio
use istio::{Istio, IstioError, IstioResult, IstioOption, IstioOptionResult};
use istio::{IstioOptionResult, IstioOption, IstioOptionResult};
//k8s
use k8s::{K8s, K8sError, K8sResult, K8sOption, K8sOptionResult};
use k8s::{K8sOptionResult, K8sOption, K8sOptionResult};
//kafka
use kafka::{Kafka, KafkaError, KafkaResult, KafkaOption, KafkaOptionResult};
use kafka::{KafkaOptionResult, KafkaOption, KafkaOptionResult};
//kinesis
use kinesis::{Kinesis, KinesisError, KinesisResult, KinesisOption, KinesisOptionResult};
use kinesis::{KinesisOptionResult, KinesisOption, KinesisOptionResult};
//kubernetes
use kubernetes::{Kubernetes, KubernetesError, KubernetesResult, KubernetesOption, KubernetesOptionResult};
use kubernetes::{KubernetesOptionResult, KubernetesOption, KubernetesOptionResult};
//mongo
use mongo::{Mongo, MongoError, MongoResult, MongoOption, MongoOptionResult};
use mongo::{MongoOptionResult, MongoOption, MongoOptionResult};
//mysql
use mysql::{Mysql, MysqlError, MysqlResult, MysqlOption, MysqlOptionResult};
use mysql::{MysqlOptionResult, MysqlOption, MysqlOptionResult};
//neo4j
use neo4j::{Neo4j, Neo4jError, Neo4jResult, Neo4jOption, Neo4jOptionResult};
use neo4j::{Neo4jOptionResult, Neo4jOption, Neo4jOptionResult};


pub struct LightlikeStore {
    conn: Sender<String>,
    recv: Receiver<String>,
    sqlite: Sender<String>,
    sqlite_recv: Receiver<String>,
    postgres_protocol: Sender<String>,
    postgres_recv: Receiver<String>,
    postgres_protocol_recv: Receiver<String>,
    foundationdb: Sender<String>,


    causet: Causet,
    causet_timeline: CausetTimeline,
    allegro_poset: AllegroPoset,
    soliton: Soliton,
    einsteindb: Einsteindb,
}

impl LightlikeStore {
    pub fn new() -> LightlikeStore {
        let (conn, recv) = channel();
        let (sqlite, sqlite_recv) = channel();
        let (postgres_protocol, postgres_recv) = channel();
        let (postgres_protocol_recv, postgres_protocol_send) = channel();
        let (foundationdb, foundationdb_recv) = channel();
        let (causet, causet_timeline, allegro_poset, soliton, einsteindb) = LightlikeStore::init_store();
        LightlikeStore {
            conn,
            recv,
            sqlite,
            sqlite_recv,
            postgres_protocol,
            postgres_recv,
            postgres_protocol_recv,
            foundationdb,
            causet,
            causet_timeline,
            allegro_poset,
            soliton,
            einsteindb,
        }
    }

    pub fn init_store() -> (Causet, CausetTimeline, AllegroPoset, Soliton, Einsteindb) {
        let causet = Causet::new();
        let causet_timeline = CausetTimeline::new();
        let allegro_poset = AllegroPoset::new();
        let soliton = Soliton::new();
        let einsteindb = Einsteindb::new();
        (causet, causet_timeline, allegro_poset, soliton, einsteindb)
    }

    pub fn get_conn(&self) -> Sender<String> {
        self.conn.clone()
    }

    pub fn get_sqlite_or_db(&self) -> Sender<String> {
        self.sqlite.clone()
    }

    pub fn get_postgres_protocol_mux_connection(&self) -> Sender<String> {
        self.postgres_protocol.clone()

    }

    pub fn get_foundationdb(&self) -> Sender<String> {
        self.foundationdb.clone()
    }

    pub fn get_causet(&self) -> Causet {
        self.causet.clone()
    }

    pub fn get_causet_timeline(&self) -> CausetTimeline {
        self.causet_timeline.clone()
    }

    pub fn get_allegro_poset(&self) -> AllegroPoset {
        self.allegro_poset.clone()
    }
}




/// We will create a lockfree queue for each thread, and we will use a conn to
/// communicate between the threads. Using sqlite3, we will create a table for each
/// thread, and we will use a conn to communicate between the threads. Meanwhile, we'll
/// suspend the threads, and we'll resume the persistence layer of FdbStore
/// System Defaults: FoundationDB; Lightlike transactions are MVRSI_SCHEMA_VERSION_1; Heavy
/// transactions are MVRSI_SCHEMA_VERSION_2; Full transactions are MVRSI_SCHEMA_VERSION_3;
/// MVSR is superior than MVCC (Multi Version Concurrency Control);
///
///

pub struct ClosedtimelikeConnection {
    //Mutex for the connection, since we will use it in multiple threads.
    conn: Mutex<Connection>,
    //The schema version of the connection.
    schema_version: i32,

    //spacetime is the metadata which we will use to store the spacetime
    //information.
    spacetime: Spacetime,

    //The name of the table which we will use to store the spacetime information.
    spacetime_table_name: String,

    mvrsi_schema_version: i32,

}

pub fn merge_append_attributes_for_causet<A>(
    conn: &mut Connection,
    tx_id: &str,
    attributes: &[A],
) -> Result<(), Error>
where
    A: Attribute,
{
    let mut stmt = conn.prepare(
        "INSERT INTO causet_timeline (tx_id, attribute_name, attribute_value) VALUES (?, ?, ?)",
    )?;
    for attribute in attributes {
        let attribute_name = attribute.get_name();
        let attribute_value = attribute.get_value();
        stmt.execute(&[tx_id, &attribute_name, &attribute_value])?;
    }
    Ok(())
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::causet::Causet;
    use crate::causet_timeline::CausetTimeline;
    use crate::allegro_poset::AllegroPoset;
    use crate::soliton::Soliton;
    use crate::einsteindb::Einsteindb;
    use crate::foundationdb::Foundationdb;
    use crate::postgres_protocol::PostgresProtocol;
    use crate::sqlite_protocol::SqliteProtocol;
    use crate::sqlite_recv::SqliteRecv;
    use crate::postgres_recv::PostgresRecv;
    use crate::postgres_protocol_recv::PostgresProtocolRecv;
    use crate::foundationdb_recv::FoundationdbRecv;
    use crate::foundationdb_protocol_recv::FoundationdbProtocolRecv;
    use crate::sqlite_protocol_recv::SqliteProtocolRecv;
    use crate::sqlite_recv::SqliteRecv;
    use crate::postgres_protocol::PostgresProtocol;
    use crate::postgres_recv::PostgresRecv;
    use crate::postgres_protocol_recv::PostgresProtocolRecv;
    use crate::foundationdb::Foundationdb;
    use crate::foundationdb_recv::FoundationdbRecv;
    use crate::foundationdb_protocol_recv::FoundationdbProtocolRecv;
    use crate::sqlite_protocol::SqliteProtocol;
    use crate::sqlite_recv::SqliteRecv;
    use crate::sqlite_protocol_recv::SqliteProtocolRecv;
    use crate::foundationdb::Foundationdb;
    use crate::foundationdb_recv::FoundationdbRecv;
    use crate::foundationdb_protocol_recv::FoundationdbProtocolRecv;
    use crate::sqlite_protocol::SqliteProtocol;
    use crate::sqlite_recv::SqliteRecv;
    use crate::sqlite_protocol_recv::SqliteProtocolRecv;


    #[test]
    fn test_closedtimelike_connection_causet() {
        let causet = Causet::new();
        let causet_timeline = CausetTimeline::new();
        let causet_timeline_name = "causet_timeline".to_string();
        let causet_timeline_table_name = "causet_timeline_table".to_string();
        let causet_timeline_table_name_2 = "causet_timeline_table_2".to_string();
        let causet_timeline_table_name_3 = "causet_timeline_table_3".to_string();
        let causet_timeline_table_name_4 = "causet_timeline_table_4".to_string();
        let causet_timeline_table_name_5 = "causet_timeline_table_5".to_string();
        let causet_timeline_table_name_6 = "causet_timeline_table_6".to_string();
        let causet_timeline_table_name_7 = "causet_timeline_table_7".to_string();
        let causet_timeline_table_name_8 = "causet_timeline_table_8".to_string();
        let causet_timeline_table_name_9 = "causet_timeline_table_9".to_string();
        let causet_timeline_table_name_10 = "causet_timeline_table_10".to_string();
        let causet_timeline_table_name_11 = "causet_timeline_table_11".to_string();
        let causet_timeline_table_name_12 = "causet_timeline_table_12".to_string();
        let causet_timeline_table_name_13 = "causet_timeline_table_13".to_string();
        let causet_timeline_table_name_14 = "causet_timeline_table_14".to_string();
        let causet_timeline_table_name_15 = "causet_timeline_table_15".to_string();

        causet.create_timeline(&causet_timeline_name);
        causet.create_timeline_table(&causet_timeline_name, &causet_timeline_table_name);
        causet.create_timeline_table(&causet_timeline_name, &causet_timeline_table_name_2);
        causet.create_timeline_table(&causet_timeline_name, &causet_timeline_table_name_3);
    }


    /*
pub fn begin_closed_lightlike_with_behavior<'m, 'conn>(
    &'m self,
    postgres_protocol: &mut PostgresProtocol,
    sqlite_protocol: &mut SqliteProtocol,
    causet: C,
    attributes: A
) -> Result<BTreeMap<Causetid, ValueRc<StructuredMap>>>
    where C: IntoIterator<Item = Causetid>,
          A: IntoIterator<Item = Attribute>,
{
    let mut causet_attributes = BTreeMap::new();
    causet_attributes.insert(causet, self.get_attributes_for_causet(postgres_protocol, sqlite_protocol, causet)?);
    for attribute in attributes {
        causet_attributes.insert(attribute, self.get_attributes_for_attribute(postgres_protocol, sqlite_protocol, attribute)?);
    }
    Ok(causet_attributes)
}


 */


    #[test]
    fn test_closed_lightlike_connection_causet() {
        let causet = Causet::new();
        let causet_timeline = CausetTimeline::new();
        let causet_timeline_name = "causet_timeline".to_string();
        let causet_timeline_table_name = "causet_timeline_table".to_string();
        let causet_timeline_table_name_2 = "causet_timeline_table_2".to_string();
        let causet_timeline_table_name_3 = "causet_timeline_table_3".to_string();
        let causet_timeline_table_name_4 = "causet_timeline_table_4".to_string();
        let causet_timeline_table_name_5 = "causet_timeline_table_5".to_string();
        let causet_timeline_table_name_6 = "causet_timeline_table_6".to_string();
        let causet_timeline_table_name_7 = "causet_timeline_table_7".to_string();
        let causet_timeline_table_name_8 = "causet_timeline_table_8".to_string();
        let causet_timeline_table_name_9 = "causet_timeline_table_9".to_string();
        let causet_timeline_table_name_10 = "causet_timeline_table_10".to_string();
        let causet_timeline_table_name_11 = "causet_timeline_table_11".to_string();
        let causet_timeline_table_name_12 = "causet_timeline_table_12".to_string();
        let causet_timeline_table_name_13 = "causet_timeline_table_13".to_string();
        let causet_timeline_table_name_14 = "causet_timeline_table_14".to_string();
    }

    #[macro_use]
    extern crate log;
    extern crate causetq;
    extern crate SymplecticControlFactorsExt;
    extern crate crossbeam;
    extern crate crossbeam_channel;

    fn collect_ordered_txs_to_move(
        txs: &mut Vec<causet::CausetTx>,
        mut tx_range: causet::CausetTxRange,
        timeline_id: causet::TimelineId,
    ) -> Vec<causet::CausetTx> {
        let mut txs_to_move = Vec::new();
        let mut tx_iter = tx_range.into_iter();
        while let Some(tx) = tx_iter.next() {
            if tx.timeline_id() == timeline_id {
                txs.push(tx);
            } else {
                txs_to_move.push(tx);
            }
        }
        txs_to_move
    }

    #[inline]
    fn decode_causet_record_u64(v: &[u8]) -> Result<u64> {
        // See `decodeInt` in MilevaDB
        match v.len() {
            1 => Ok(u64::from(v[0])),
            2 => Ok(u64::from(NumberCodec::decode_u16_le(v))),
            4 => Ok(u64::from(NumberCodec::decode_u32_le(v))),
            8 => Ok(u64::from(NumberCodec::decode_u64_le(v))),
            _ => Err(Error::InvalidDataType(
                "Failed to decode event causet_record data as u64".to_owned(),
            )),
        }
    }

    #[inline]
    fn decode_causet_record_i64(v: &[u8]) -> Result<i64> {
        // See `decodeUint` in MilevaDB
        match v.len() {
            1 => Ok(i64::from(v[0] as i8)),
            2 => Ok(i64::from(NumberCodec::decode_u16_le(v) as i16)),
            4 => Ok(i64::from(NumberCodec::decode_u32_le(v) as i32)),
            8 => Ok(NumberCodec::decode_u64_le(v) as i64),
            _ => Err(Error::InvalidDataType(
                "Failed to decode event causet_record data as i64".to_owned(),
            )),
        }
    }

    pub trait CausetRecord {
        fn write_causet_record_as_datum_u64(&mut self, src: &[u8]) -> Result<()> {
            self.write_datum_u64(decode_causet_record_u64(src)?)
        }

        fn write_causet_record_as_datum_duration(&mut self, src: &[u8]) -> Result<()> {
            self.write_u8(datum::DURATION_FLAG)?;
            self.write_datum_payload_i64(decode_causet_record_i64(src)?)
        }

        fn write_causet_record_as_datum(&mut self, src: &[u8], ft: &dyn FieldTypeAccessor) -> Result<()> {
            match ft.get_field_type() {
                FieldType::U64 => self.write_causet_record_as_datum_u64(src),
                FieldType::Duration => self.write_causet_record_as_datum_duration(src),
                _ => Err(Error::InvalidDataType(
                    "Failed to decode event causet_record data as datum".to_owned(),
                )),
            }
        }
    }
}

