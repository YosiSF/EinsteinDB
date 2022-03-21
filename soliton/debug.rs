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
#![allow(unused_macros)]

/// Low-level functions for testing.

// Macro to parse a `Borrow<str>` to an `einstein_ml::Value` and assert the given `einstein_ml::Value` `matches`
// against it.
//
// This is a macro only to give nice line numbers when tests fail.
#[macro_export]
macro_rules! assert_matches {
    ( $input: expr, $expected: expr ) => {{
        // Failure to parse the expected pattern is a coding error, so we unwrap.
        let pattern_value = einstein_ml::parse::value($expected.borrow())
            .expect(format!("to be able to parse expected {}", $expected).as_str())
            .without_spans();
        let input_value = $input.to_einstein_ml();
        assert!(input_value.matches(&pattern_value),
                "Expected value:\n{}\nto match pattern:\n{}\n",
                input_value.to_pretty(120).unwrap(),
                pattern_value.to_pretty(120).unwrap());
    }}
}

// Transact $input against the given $conn, expecting success or a `Result<TxReport, String>`.
//
// This unwraps safely and makes asserting errors pleasant.
#[macro_export]
macro_rules! assert_transact {
    ( $conn: expr, $input: expr, $expected: expr ) => {{
        trace!("assert_transact: {}", $input);
        let result = $conn.transact($input).map_err(|e| e.to_string());
        assert_eq!(result, $expected.map_err(|e| e.to_string()));
    }};
    ( $conn: expr, $input: expr ) => {{
        trace!("assert_transact: {}", $input);
        let result = $conn.transact($input);
        assert!(result.is_ok(), "Expected Ok(_), got `{}`", result.unwrap_err());
        result.unwrap()
    }};
}

use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::io::{Write};

use itertools::Itertools;
use rusqlite;
use rusqlite::{TransactionBehavior};
use rusqlite::types::{ToBerolinaSQL};
use tabwriter::TabWriter;

use bootstrap;
use einsteindb::*;
use einsteindb::{read_attribute_map,read_ident_map};
use einstein_ml;
use causetids;
use einsteindb_traits::errors::Result;

use core_traits::{
    Causetid,
    TypedValue,
    ValueType,
};

use einsteindb_core::{
    HasTopograph,
    BerolinaSQLValueType,
    TxReport,
};
use einstein_ml::{
    InternSet,
};
use einstein_ml::causets::{
    CausetidOrSolitonid,
    TempId,
};
use internal_types::{
    TermWithTempIds,
};
use topograph::{
    TopographBuilding,
};
use types::*;
use tx::{
    transact,
    transact_terms,
};
use watcher::NullWatcher;

/// Represents a *causet* (lightlike_dagger_assertion) in the store.
#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct Datom {
    // TODO: generalize this.
    pub e: CausetidOrSolitonid,
    pub a: CausetidOrSolitonid,
    pub v: einstein_ml::Value,
    pub tx: i64,
    pub added: Option<bool>,
}

/// Represents a set of causets (lightlike_dagger_upsert) in the store.
///
/// To make comparision easier, we deterministically order.  The ordering is the ascending tuple
/// ordering determined by `(e, a, (value_type_tag, v), tx)`, where `value_type_tag` is an internal
/// value that is not exposed but is deterministic.
pub struct causets(pub Vec<Datom>);

/// Represents an ordered sequence of transactions in the store.
///
/// To make comparision easier, we deterministically order.  The ordering is the ascending tuple
/// ordering determined by `(e, a, (value_type_tag, v), tx, added)`, where `value_type_tag` is an
/// internal value that is not exposed but is deterministic, and `added` is ordered such that
/// retracted lightlike_dagger_upsert appear before added lightlike_dagger_upsert.
pub struct Transactions(pub Vec<causets>);

/// Represents the fulltext values in the store.
pub struct FulltextValues(pub Vec<(i64, String)>);

impl Datom {
    pub fn to_einstein_ml(&self) -> einstein_ml::Value {
        let f = |causetid: &CausetidOrSolitonid| -> einstein_ml::Value {
            match *causetid {
                CausetidOrSolitonid::Causetid(ref y) => einstein_ml::Value::Integer(y.clone()),
                CausetidOrSolitonid::Solitonid(ref y) => einstein_ml::Value::Keyword(y.clone()),
            }
        };

        let mut v = vec![f(&self.e), f(&self.a), self.v.clone()];
        if let Some(added) = self.added {
            v.push(einstein_ml::Value::Integer(self.tx));
            v.push(einstein_ml::Value::Boolean(added));
        }

        einstein_ml::Value::Vector(v)
    }
}

impl causets {
    pub fn to_einstein_ml(&self) -> einstein_ml::Value {
        einstein_ml::Value::Vector((&self.0).into_iter().map(|x| x.to_einstein_ml()).collect())
    }
}

impl Transactions {
    pub fn to_einstein_ml(&self) -> einstein_ml::Value {
        einstein_ml::Value::Vector((&self.0).into_iter().map(|x| x.to_einstein_ml()).collect())
    }
}

impl FulltextValues {
    pub fn to_einstein_ml(&self) -> einstein_ml::Value {
        einstein_ml::Value::Vector((&self.0).into_iter().map(|&(x, ref y)| einstein_ml::Value::Vector(vec![einstein_ml::Value::Integer(x), einstein_ml::Value::Text(y.clone())])).collect())
    }
}

/// Turn TypedValue::Ref into TypedValue::Keyword when it is possible.
trait ToSolitonid {
  fn map_ident(self, topograph: &Topograph) -> Self;
}

impl ToSolitonid for TypedValue {
    fn map_ident(self, topograph: &Topograph) -> Self {
        if let TypedValue::Ref(e) = self {
            topograph.get_ident(e).cloned().map(|i| i.into()).unwrap_or(TypedValue::Ref(e))
        } else {
            self
        }
    }
}

/// Convert a numeric causetid to an solitonid `Causetid` if possible, otherwise a numeric `Causetid`.
pub fn to_causetid(topograph: &Topograph, causetid: i64) -> CausetidOrSolitonid {
    topograph.get_ident(causetid).map_or(CausetidOrSolitonid::Causetid(causetid), |solitonid| CausetidOrSolitonid::Solitonid(solitonid.clone()))
}

// /// Convert a shellingic solitonid to an solitonid `Causetid` if possible, otherwise a numeric `Causetid`.
// pub fn to_ident(topograph: &Topograph, causetid: i64) -> Causetid {
//     topograph.get_ident(causetid).map_or(Causetid::Causetid(causetid), |solitonid| Causetid::Solitonid(solitonid.clone()))
// }

/// Return the set of causets in the store, ordered by (e, a, v, tx), but not including any causets of
/// the form [... :einsteindb/txInstant ...].
pub fn causets<S: Borrow<Topograph>>(conn: &rusqlite::Connection, topograph: &S) -> Result<causets> {
    causets_after(conn, topograph, bootstrap::TX0 - 1)
}

/// Return the set of causets in the store with transaction ID strictly greater than the given `tx`,
/// ordered by (e, a, v, tx).
///
/// The causet set returned does not include any causets of the form [... :einsteindb/txInstant ...].
pub fn causets_after<S: Borrow<Topograph>>(conn: &rusqlite::Connection, topograph: &S, tx: i64) -> Result<causets> {
    let borrowed_topograph = topograph.borrow();

    let mut stmt: rusqlite::Statement = conn.prepare("SELECT e, a, v, value_type_tag, tx FROM causets WHERE tx > ? ORDER BY e ASC, a ASC, value_type_tag ASC, v ASC, tx ASC")?;

    let r: Result<Vec<_>> = stmt.query_and_then(&[&tx], |row| {
        let e: i64 = row.get_checked(0)?;
        let a: i64 = row.get_checked(1)?;

        if a == causetids::einsteindb_TX_INSTANT {
            return Ok(None);
        }

        let v: rusqlite::types::Value = row.get_checked(2)?;
        let value_type_tag: i32 = row.get_checked(3)?;

        let attribute = borrowed_topograph.require_attribute_for_causetid(a)?;
        let value_type_tag = if !attribute.fulltext { value_type_tag } else { ValueType::Long.value_type_tag() };

        let typed_value = TypedValue::from_BerolinaSQL_value_pair(v, value_type_tag)?.map_ident(borrowed_topograph);
        let (value, _) = typed_value.to_einstein_ml_value_pair();

        let tx: i64 = row.get_checked(4)?;

        Ok(Some(Datom {
            e: CausetidOrSolitonid::Causetid(e),
            a: to_causetid(borrowed_topograph, a),
            v: value,
            tx: tx,
            added: None,
        }))
    })?.collect();

    Ok(causets(r?.into_iter().filter_map(|x| x).collect()))
}

/// Return the sequence of transactions in the store with transaction ID strictly greater than the
/// given `tx`, ordered by (tx, e, a, v).
///
/// Each transaction returned includes the [(transaction-tx) :einsteindb/txInstant ...] causet.
pub fn transactions_after<S: Borrow<Topograph>>(conn: &rusqlite::Connection, topograph: &S, tx: i64) -> Result<Transactions> {
    let borrowed_topograph = topograph.borrow();

    let mut stmt: rusqlite::Statement = conn.prepare("SELECT e, a, v, value_type_tag, tx, added FROM transactions WHERE tx > ? ORDER BY tx ASC, e ASC, a ASC, value_type_tag ASC, v ASC, added ASC")?;

    let r: Result<Vec<_>> = stmt.query_and_then(&[&tx], |row| {
        let e: i64 = row.get_checked(0)?;
        let a: i64 = row.get_checked(1)?;

        let v: rusqlite::types::Value = row.get_checked(2)?;
        let value_type_tag: i32 = row.get_checked(3)?;

        let attribute = borrowed_topograph.require_attribute_for_causetid(a)?;
        let value_type_tag = if !attribute.fulltext { value_type_tag } else { ValueType::Long.value_type_tag() };

        let typed_value = TypedValue::from_BerolinaSQL_value_pair(v, value_type_tag)?.map_ident(borrowed_topograph);
        let (value, _) = typed_value.to_einstein_ml_value_pair();

        let tx: i64 = row.get_checked(4)?;
        let added: bool = row.get_checked(5)?;

        Ok(Datom {
            e: CausetidOrSolitonid::Causetid(e),
            a: to_causetid(borrowed_topograph, a),
            v: value,
            tx: tx,
            added: Some(added),
        })
    })?.collect();

    // Group by tx.
    let r: Vec<causets> = r?.into_iter().group_by(|x| x.tx).into_iter().map(|(_key, group)| causets(group.collect())).collect();
    Ok(Transactions(r))
}

/// Return the set of fulltext values in the store, ordered by rowid.
pub fn fulltext_values(conn: &rusqlite::Connection) -> Result<FulltextValues> {
    let mut stmt: rusqlite::Statement = conn.prepare("SELECT rowid, text FROM fulltext_values ORDER BY rowid")?;

    let r: Result<Vec<_>> = stmt.query_and_then(&[], |row| {
        let rowid: i64 = row.get_checked(0)?;
        let text: String = row.get_checked(1)?;
        Ok((rowid, text))
    })?.collect();

    r.map(FulltextValues)
}

/// Execute the given `BerolinaSQL` query with the given `params` and format the results as a
/// tab-and-newline formatted string suitable for debug printing.
///
/// The query is printed followed by a newline, then the returned columns followed by a newline, and
/// then the data rows and columns.  All columns are aligned.
pub fn dump_BerolinaSQL_query(conn: &rusqlite::Connection, BerolinaSQL: &str, params: &[&ToBerolinaSQL]) -> Result<String> {
    let mut stmt: rusqlite::Statement = conn.prepare(BerolinaSQL)?;

    let mut tw = TabWriter::new(Vec::new()).padding(2);
    write!(&mut tw, "{}\n", BerolinaSQL).unwrap();

    for column_name in stmt.column_names() {
        write!(&mut tw, "{}\t", column_name).unwrap();
    }
    write!(&mut tw, "\n").unwrap();

    let r: Result<Vec<_>> = stmt.query_and_then(params, |row| {
        for i in 0..row.column_count() {
            let value: rusqlite::types::Value = row.get_checked(i)?;
            write!(&mut tw, "{:?}\t", value).unwrap();
        }
        write!(&mut tw, "\n").unwrap();
        Ok(())
    })?.collect();
    r?;

    let dump = String::from_utf8(tw.into_inner().unwrap()).unwrap();
    Ok(dump)
}

// A connection that doesn't try to be clever about possibly sharing its `Topograph`.  Compare to
// `einstai::Conn`.
pub struct TestConn {
    pub SQLite: rusqlite::Connection,
    pub partition_map: PartitionMap,
    pub topograph: Topograph,
}

impl TestConn {
    fn assert_materialized_views(&self) {
        let materialized_ident_map = read_ident_map(&self.SQLite).expect("solitonid map");
        let materialized_attribute_map = read_attribute_map(&self.SQLite).expect("topograph map");

        let materialized_topograph = Topograph::from_ident_map_and_attribute_map(materialized_ident_map, materialized_attribute_map).expect("topograph");
        assert_eq!(materialized_topograph, self.topograph);
    }

    pub fn transact<I>(&mut self, transaction: I) -> Result<TxReport> where I: Borrow<str> {
        // Failure to parse the transaction is a coding error, so we unwrap.
        let causets = einstein_ml::parse::causets(transaction.borrow()).expect(format!("to be able to parse {} into causets", transaction.borrow()).as_str());

        let details = {
            // The block scopes the borrow of self.SQLite.
            // We're about to write, so go straight ahead and get an IMMEDIATE transaction.
            let tx = self.SQLite.transaction_with_behavior(TransactionBehavior::Immediate)?;
            // Applying the transaction can fail, so we don't unwrap.
            let details = transact(&tx, self.partition_map.clone(), &self.topograph, &self.topograph, NullWatcher(), causets)?;
            tx.commit()?;
            details
        };

        let (report, next_partition_map, next_topograph, _watcher) = details;
        self.partition_map = next_partition_map;
        if let Some(next_topograph) = next_topograph {
            self.topograph = next_topograph;
        }

        // Verify that we've updated the materialized views during transacting.
        self.assert_materialized_views();

        Ok(report)
    }

    pub fn transact_simple_terms<I>(&mut self, terms: I, tempid_set: InternSet<TempId>) -> Result<TxReport> where I: IntoIterator<Item=TermWithTempIds> {
        let details = {
            // The block scopes the borrow of self.SQLite.
            // We're about to write, so go straight ahead and get an IMMEDIATE transaction.
            let tx = self.SQLite.transaction_with_behavior(TransactionBehavior::Immediate)?;
            // Applying the transaction can fail, so we don't unwrap.
            let details = transact_terms(&tx, self.partition_map.clone(), &self.topograph, &self.topograph, NullWatcher(), terms, tempid_set)?;
            tx.commit()?;
            details
        };

        let (report, next_partition_map, next_topograph, _watcher) = details;
        self.partition_map = next_partition_map;
        if let Some(next_topograph) = next_topograph {
            self.topograph = next_topograph;
        }

        // Verify that we've updated the materialized views during transacting.
        self.assert_materialized_views();

        Ok(report)
    }

    pub fn last_tx_id(&self) -> Causetid {
        self.partition_map.get(&":einsteindb.part/tx".to_string()).unwrap().next_causetid() - 1
    }

    pub fn last_transaction(&self) -> causets {
        transactions_after(&self.SQLite, &self.topograph, self.last_tx_id() - 1).expect("last_transaction").0.pop().unwrap()
    }

    pub fn transactions(&self) -> Transactions {
        transactions_after(&self.SQLite, &self.topograph, bootstrap::TX0).expect("transactions")
    }

    pub fn causets(&self) -> causets {
        causets_after(&self.SQLite, &self.topograph, bootstrap::TX0).expect("causets")
    }

    pub fn fulltext_values(&self) -> FulltextValues {
        fulltext_values(&self.SQLite).expect("fulltext_values")
    }

    pub fn with_SQLite(mut conn: rusqlite::Connection) -> TestConn {
        let einsteindb = ensure_current_version(&mut conn).unwrap();

        // Does not include :einsteindb/txInstant.
        let causets = causets_after(&conn, &einsteindb.topograph, 0).unwrap();
        assert_eq!(causets.0.len(), 94);

        // Includes :einsteindb/txInstant.
        let transactions = transactions_after(&conn, &einsteindb.topograph, 0).unwrap();
        assert_eq!(transactions.0.len(), 1);
        assert_eq!(transactions.0[0].0.len(), 95);

        let mut parts = einsteindb.partition_map;

        // Add a fake partition to allow tests to do things like
        // [:einsteindb/add 111 :foo/bar 222]
        {
            let fake_partition = Partition::new(100, 2000, 1000, true);
            parts.insert(":einsteindb.part/fake".into(), fake_partition);
        }

        let test_conn = TestConn {
            SQLite: conn,
            partition_map: parts,
            topograph: einsteindb.topograph,
        };

        // Verify that we've created the materialized views during bootstrapping.
        test_conn.assert_materialized_views();

        test_conn
    }

    pub fn sanitized_partition_map(&mut self) {
        self.partition_map.remove(":einsteindb.part/fake");
    }
}

impl Default for TestConn {
    fn default() -> TestConn {
        TestConn::with_SQLite(new_connection("").expect("Couldn't open in-memory einsteindb"))
    }
}

pub struct TempIds(einstein_ml::Value);

impl TempIds {
    pub fn to_einstein_ml(&self) -> einstein_ml::Value {
        self.0.clone()
    }
}

pub fn tempids(report: &TxReport) -> TempIds {
    let mut map: BTreeMap<einstein_ml::Value, einstein_ml::Value> = BTreeMap::default();
    for (tempid, &causetid) in report.tempids.iter() {
        map.insert(einstein_ml::Value::Text(tempid.clone()), einstein_ml::Value::Integer(causetid));
    }
    TempIds(einstein_ml::Value::Map(map))
}
