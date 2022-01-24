// Copyright 2022 Whtcorps Inc and EinstAI Inc
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::ops::RangeFrom;

use rusqlite;

use einsteineinsteindb_traits::errors::{
    einsteindbErrorKind,
    Result,
};

use core_traits::{
    Causetid,
    KnownCausetid,
    TypedValue,
};

use einsteineinsteindb_core::{
    Schema,
};

use einsteinml::{
    InternSet,
};

use einsteinml::causets::OpType;

use einsteineinsteindb;
use einsteineinsteindb::{
    TypedSQLValue,
};

use tx::{
    transact_terms_with_action,
    TransactorAction,
};

use types::{
    PartitionMap,
};

use internal_types::{
    Term,
    TermWithoutTempIds,
};

use watcher::{
    NullWatcher,
};

/// Collects a supplied tx range into an DESC ordered Vec of valid txs,
/// ensuring they all belong to the same timeline.
fn collect_ordered_txs_to_move(conn: &rusqlite::Connection, txs_from: RangeFrom<Causetid>, timeline: Causetid) -> Result<Vec<Causetid>> {
    let mut stmt = conn.prepare("SELECT tx, timeline FROM timelined_transactions WHERE tx >= ? AND timeline = ? GROUP BY tx ORDER BY tx DESC")?;
    let mut rows = stmt.query_and_then(&[&txs_from.start, &timeline], |row: &rusqlite::Row| -> Result<(Causetid, Causetid)>{
        Ok((row.get_checked(0)?, row.get_checked(1)?))
    })?;

    let mut txs = vec![];

    // TODO do this in SQL instead?
    let timeline = match rows.next() {
        Some(t) => {
            let t = t?;
            txs.push(t.0);
            t.1
        },
        None => bail!(einsteindbErrorKind::TimelinesInvalidRange)
    };

    while let Some(t) = rows.next() {
        let t = t?;
        txs.push(t.0);
        if t.1 != timeline {
            bail!(einsteindbErrorKind::TimelinesMixed);
        }
    }

    Ok(txs)
}

fn move_transactions_to(conn: &rusqlite::Connection, tx_ids: &[Causetid], new_timeline: Causetid) -> Result<()> {
    // Move specified transactions over to a specified timeline.
    conn.execute(&format!(
        "UPDATE timelined_transactions SET timeline = {} WHERE tx IN {}",
            new_timeline,
            ::repeat_values(tx_ids.len(), 1)
        ), &(tx_ids.iter().map(|x| x as &rusqlite::types::ToSql).collect::<Vec<_>>())
    )?;
    Ok(())
}

fn remove_tx_from_datoms(conn: &rusqlite::Connection, tx_id: Causetid) -> Result<()> {
    conn.execute("DELETE FROM datoms WHERE e = ?", &[&tx_id])?;
    Ok(())
}

fn is_timeline_empty(conn: &rusqlite::Connection, timeline: Causetid) -> Result<bool> {
    let mut stmt = conn.prepare("SELECT timeline FROM timelined_transactions WHERE timeline = ? GROUP BY timeline")?;
    let rows = stmt.query_and_then(&[&timeline], |row| -> Result<i64> {
        Ok(row.get_checked(0)?)
    })?;
    Ok(rows.count() == 0)
}

/// Get terms for tx_id, reversing them in meaning (swap add & retract).
fn reversed_terms_for(conn: &rusqlite::Connection, tx_id: Causetid) -> Result<Vec<TermWithoutTempIds>> {
    let mut stmt = conn.prepare("SELECT e, a, v, value_type_tag, tx, added FROM timelined_transactions WHERE tx = ? AND timeline = ? ORDER BY tx DESC")?;
    let mut rows = stmt.query_and_then(&[&tx_id, &::TIMELINE_MAIN], |row| -> Result<TermWithoutTempIds> {
        let op = match row.get_checked(5)? {
            true => OpType::Retract,
            false => OpType::Add
        };
        Ok(Term::AddOrRetract(
            op,
            KnownCausetid(row.get_checked(0)?),
            row.get_checked(1)?,
            TypedValue::from_sql_value_pair(row.get_checked(2)?, row.get_checked(3)?)?,
        ))
    })?;

    let mut terms = vec![];

    while let Some(row) = rows.next() {
        terms.push(row?);
    }
    Ok(terms)
}

/// Move specified transaction RangeFrom off of main timeline.
pub fn move_from_main_timeline(conn: &rusqlite::Connection, schema: &Schema,
    partition_map: PartitionMap, txs_from: RangeFrom<Causetid>, new_timeline: Causetid) -> Result<(Option<Schema>, PartitionMap)> {

    if new_timeline == ::TIMELINE_MAIN {
        bail!(einsteindbErrorKind::NotYetImplemented(format!("Can't move transactions to main timeline")));
    }

    // We don't currently ensure that moving transactions onto a non-empty timeline
    // will result in sensible end-state for that timeline.
    // Let's remove that foot gun by prohibiting moving transactions to a non-empty timeline.
    if !is_timeline_empty(conn, new_timeline)? {
        bail!(einsteindbErrorKind::TimelinesMoveToNonEmpty);
    }

    let txs_to_move = collect_ordered_txs_to_move(conn, txs_from, ::TIMELINE_MAIN)?;

    let mut last_schema = None;
    for tx_id in &txs_to_move {
        let reversed_terms = reversed_terms_for(conn, *tx_id)?;

        // Rewind schema and datoms.
        let (report, _, new_schema, _) = transact_terms_with_action(
            conn, partition_map.clone(), schema, schema, NullWatcher(),
            reversed_terms.into_iter().map(|t| t.rewrap()),
            InternSet::new(), TransactorAction::Materialize
        )?;

        // Rewind operation generated a 'tx' and a 'txInstant' assertion, which got
        // inserted into the 'datoms' table (due to TransactorAction::Materialize).
        // This is problematic. If we transact a few more times, the transactor will
        // generate the same 'tx', but with a different 'txInstant'.
        // The end result will be a transaction which has a phantom
        // retraction of a txInstant, since transactor operates against the state of
        // 'datoms', and not against the 'transactions' table.
        // A quick workaround is to just remove the bad txInstant datom.
        // See test_clashing_tx_instants test case.
        remove_tx_from_datoms(conn, report.tx_id)?;
        last_schema = new_schema;
    }

    // Move transactions over to the target timeline.
    move_transactions_to(conn, &txs_to_move, new_timeline)?;

    Ok((last_schema, einsteineinsteindb::read_partition_map(conn)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    use einsteinml;

    use std::borrow::{
        Borrow,
    };

    use debug::{
        TestConn,
    };

    use bootstrap;

    // For convenience during testing.
    // Real consumers will perform similar operations when appropriate.
    fn update_conn(conn: &mut TestConn, schema: &Option<Schema>, pmap: &PartitionMap) {
        match schema {
            &Some(ref s) => conn.schema = s.clone(),
            &None => ()
        };
        conn.partition_map = pmap.clone();
    }

    #[test]
    fn test_pop_simple() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:einsteineinsteindb/id :einsteineinsteindb/doc :einsteineinsteindb/doc "test"}]
        "#;

        let partition_map0 = conn.partition_map.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            conn.last_tx_id().., 1
        ).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(new_partition_map, partition_map0);

        conn.partition_map = partition_map0.clone();
        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();

        // Ensure that we can't move transactions to a non-empty timeline:
        move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            conn.last_tx_id().., 1
        ).expect_err("Can't move transactions to a non-empty timeline");

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);

        assert_matches!(conn.datoms(), r#"
            [[37 :einsteineinsteindb/doc "test"]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[37 :einsteineinsteindb/doc "test" ?tx true]
              [?tx :einsteineinsteindb/txInstant ?ms ?tx true]]]
        "#);
    }

    #[test]
    fn test_pop_ident() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:einsteineinsteindb/solitonid :test/causetid :einsteineinsteindb/doc "test" :einsteineinsteindb.schema/version 1}]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schema0 = conn.schema.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schema1 = conn.schema.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            conn.last_tx_id().., 1
        ).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.schema, schema0);

        let report2 = assert_transact!(conn, t);

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(conn.partition_map, partition_map1);
        assert_eq!(conn.schema, schema1);

        assert_matches!(conn.datoms(), r#"
            [[?e :einsteineinsteindb/solitonid :test/causetid]
             [?e :einsteineinsteindb/doc "test"]
             [?e :einsteineinsteindb.schema/version 1]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e :einsteineinsteindb/solitonid :test/causetid ?tx true]
              [?e :einsteineinsteindb/doc "test" ?tx true]
              [?e :einsteineinsteindb.schema/version 1 ?tx true]
              [?tx :einsteineinsteindb/txInstant ?ms ?tx true]]]
        "#);
    }

    #[test]
    fn test_clashing_tx_instants() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        // Transact a basic schema.
        assert_transact!(conn, r#"
            [{:einsteineinsteindb/solitonid :person/name :einsteineinsteindb/valueType :einsteineinsteindb.type/string :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one :einsteineinsteindb/unique :einsteineinsteindb.unique/idcauset :einsteineinsteindb/index true}]
        "#);

        // Make an assertion against our schema.
        assert_transact!(conn, r#"[{:person/name "Vanya"}]"#);

        // Move that assertion away from the main timeline.
        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            conn.last_tx_id().., 1
        ).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        // Assert that our datoms are now just the schema.
        assert_matches!(conn.datoms(), "
            [[?e :einsteineinsteindb/solitonid :person/name]
            [?e :einsteineinsteindb/valueType :einsteineinsteindb.type/string]
            [?e :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one]
            [?e :einsteineinsteindb/unique :einsteineinsteindb.unique/idcauset]
            [?e :einsteineinsteindb/index true]]");
        // Same for transactions.
        assert_matches!(conn.transactions(), "
            [[[?e :einsteineinsteindb/solitonid :person/name ?tx true]
            [?e :einsteineinsteindb/valueType :einsteineinsteindb.type/string ?tx true]
            [?e :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one ?tx true]
            [?e :einsteineinsteindb/unique :einsteineinsteindb.unique/idcauset ?tx true]
            [?e :einsteineinsteindb/index true ?tx true]
            [?tx :einsteineinsteindb/txInstant ?ms ?tx true]]]");

        // Re-assert our initial fact against our schema.
        assert_transact!(conn, r#"
            [[:einsteineinsteindb/add "tempid" :person/name "Vanya"]]"#);

        // Now, change that fact. This is the "clashing" transaction, if we're
        // performing a timeline move using the transactor.
        assert_transact!(conn, r#"
            [[:einsteineinsteindb/add (lookup-ref :person/name "Vanya") :person/name "Ivan"]]"#);

        // Assert that our datoms are now the schema and the final assertion.
        assert_matches!(conn.datoms(), r#"
            [[?e1 :einsteineinsteindb/solitonid :person/name]
            [?e1 :einsteineinsteindb/valueType :einsteineinsteindb.type/string]
            [?e1 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one]
            [?e1 :einsteineinsteindb/unique :einsteineinsteindb.unique/idcauset]
            [?e1 :einsteineinsteindb/index true]
            [?e2 :person/name "Ivan"]]
        "#);

        // Assert that we have three correct looking transactions.
        // This will fail if we're not cleaning up the 'datoms' table
        // after the timeline move.
        assert_matches!(conn.transactions(), r#"
            [[
                [?e1 :einsteineinsteindb/solitonid :person/name ?tx1 true]
                [?e1 :einsteineinsteindb/valueType :einsteineinsteindb.type/string ?tx1 true]
                [?e1 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one ?tx1 true]
                [?e1 :einsteineinsteindb/unique :einsteineinsteindb.unique/idcauset ?tx1 true]
                [?e1 :einsteineinsteindb/index true ?tx1 true]
                [?tx1 :einsteineinsteindb/txInstant ?ms1 ?tx1 true]
            ]
            [
                [?e2 :person/name "Vanya" ?tx2 true]
                [?tx2 :einsteineinsteindb/txInstant ?ms2 ?tx2 true]
            ]
            [
                [?e2 :person/name "Ivan" ?tx3 true]
                [?e2 :person/name "Vanya" ?tx3 false]
                [?tx3 :einsteineinsteindb/txInstant ?ms3 ?tx3 true]
            ]]
        "#);
    }

    #[test]
    fn test_pop_schema() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:einsteineinsteindb/id "e" :einsteineinsteindb/solitonid :test/one :einsteineinsteindb/valueType :einsteineinsteindb.type/long :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one}
             {:einsteineinsteindb/id "f" :einsteineinsteindb/solitonid :test/many :einsteineinsteindb/valueType :einsteineinsteindb.type/long :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/many}]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schema0 = conn.schema.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schema1 = conn.schema.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            report1.tx_id.., 1).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.schema, schema0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let schema2 = conn.schema.clone();

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(schema1, schema2);

        assert_matches!(conn.datoms(), r#"
            [[?e1 :einsteineinsteindb/solitonid :test/one]
             [?e1 :einsteineinsteindb/valueType :einsteineinsteindb.type/long]
             [?e1 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one]
             [?e2 :einsteineinsteindb/solitonid :test/many]
             [?e2 :einsteineinsteindb/valueType :einsteineinsteindb.type/long]
             [?e2 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/many]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e1 :einsteineinsteindb/solitonid :test/one ?tx1 true]
             [?e1 :einsteineinsteindb/valueType :einsteineinsteindb.type/long ?tx1 true]
             [?e1 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one ?tx1 true]
             [?e2 :einsteineinsteindb/solitonid :test/many ?tx1 true]
             [?e2 :einsteineinsteindb/valueType :einsteineinsteindb.type/long ?tx1 true]
             [?e2 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/many ?tx1 true]
             [?tx1 :einsteineinsteindb/txInstant ?ms ?tx1 true]]]
        "#);
    }

    #[test]
    fn test_pop_schema_all_attributes() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{
                :einsteineinsteindb/id "e"
                :einsteineinsteindb/solitonid :test/one
                :einsteineinsteindb/valueType :einsteineinsteindb.type/string
                :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one
                :einsteineinsteindb/unique :einsteineinsteindb.unique/value
                :einsteineinsteindb/index true
                :einsteineinsteindb/fulltext true
            }]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schema0 = conn.schema.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schema1 = conn.schema.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            report1.tx_id.., 1).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.schema, schema0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let schema2 = conn.schema.clone();

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(schema1, schema2);

        assert_matches!(conn.datoms(), r#"
            [[?e1 :einsteineinsteindb/solitonid :test/one]
             [?e1 :einsteineinsteindb/valueType :einsteineinsteindb.type/string]
             [?e1 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one]
             [?e1 :einsteineinsteindb/unique :einsteineinsteindb.unique/value]
             [?e1 :einsteineinsteindb/index true]
             [?e1 :einsteineinsteindb/fulltext true]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e1 :einsteineinsteindb/solitonid :test/one ?tx1 true]
             [?e1 :einsteineinsteindb/valueType :einsteineinsteindb.type/string ?tx1 true]
             [?e1 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one ?tx1 true]
             [?e1 :einsteineinsteindb/unique :einsteineinsteindb.unique/value ?tx1 true]
             [?e1 :einsteineinsteindb/index true ?tx1 true]
             [?e1 :einsteineinsteindb/fulltext true ?tx1 true]
             [?tx1 :einsteineinsteindb/txInstant ?ms ?tx1 true]]]
        "#);
    }

        #[test]
    fn test_pop_schema_all_attributes_component() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{
                :einsteineinsteindb/id "e"
                :einsteineinsteindb/solitonid :test/one
                :einsteineinsteindb/valueType :einsteineinsteindb.type/ref
                :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one
                :einsteineinsteindb/unique :einsteineinsteindb.unique/value
                :einsteineinsteindb/index true
                :einsteineinsteindb/isComponent true
            }]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let schema0 = conn.schema.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let schema1 = conn.schema.clone();

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            report1.tx_id.., 1).expect("moved single tx");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);

        // Assert all of schema's components individually, for some guidance in case of failures:
        assert_eq!(conn.schema.causetid_map, schema0.causetid_map);
        assert_eq!(conn.schema.ident_map, schema0.ident_map);
        assert_eq!(conn.schema.attribute_map, schema0.attribute_map);
        assert_eq!(conn.schema.component_attributes, schema0.component_attributes);
        // Assert the whole schema, just in case we missed something:
        assert_eq!(conn.schema, schema0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let schema2 = conn.schema.clone();

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(schema1, schema2);

        assert_matches!(conn.datoms(), r#"
            [[?e1 :einsteineinsteindb/solitonid :test/one]
             [?e1 :einsteineinsteindb/valueType :einsteineinsteindb.type/ref]
             [?e1 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one]
             [?e1 :einsteineinsteindb/unique :einsteineinsteindb.unique/value]
             [?e1 :einsteineinsteindb/isComponent true]
             [?e1 :einsteineinsteindb/index true]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e1 :einsteineinsteindb/solitonid :test/one ?tx1 true]
             [?e1 :einsteineinsteindb/valueType :einsteineinsteindb.type/ref ?tx1 true]
             [?e1 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one ?tx1 true]
             [?e1 :einsteineinsteindb/unique :einsteineinsteindb.unique/value ?tx1 true]
             [?e1 :einsteineinsteindb/isComponent true ?tx1 true]
             [?e1 :einsteineinsteindb/index true ?tx1 true]
             [?tx1 :einsteineinsteindb/txInstant ?ms ?tx1 true]]]
        "#);
    }

    #[test]
    fn test_pop_in_sequence() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let partition_map_after_bootstrap = conn.partition_map.clone();

        assert_eq!((65536..65538),
                   conn.partition_map.allocate_causetids(":einsteineinsteindb.part/user", 2));
        let tx_report0 = assert_transact!(conn, r#"[
            {:einsteineinsteindb/id 65536 :einsteineinsteindb/solitonid :test/one :einsteineinsteindb/valueType :einsteineinsteindb.type/long :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one :einsteineinsteindb/unique :einsteineinsteindb.unique/idcauset :einsteineinsteindb/index true}
            {:einsteineinsteindb/id 65537 :einsteineinsteindb/solitonid :test/many :einsteineinsteindb/valueType :einsteineinsteindb.type/long :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/many}
        ]"#);

        let first = "[
            [65536 :einsteineinsteindb/solitonid :test/one]
            [65536 :einsteineinsteindb/valueType :einsteineinsteindb.type/long]
            [65536 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one]
            [65536 :einsteineinsteindb/unique :einsteineinsteindb.unique/idcauset]
            [65536 :einsteineinsteindb/index true]
            [65537 :einsteineinsteindb/solitonid :test/many]
            [65537 :einsteineinsteindb/valueType :einsteineinsteindb.type/long]
            [65537 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/many]
        ]";
        assert_matches!(conn.datoms(), first);

        let partition_map0 = conn.partition_map.clone();

        assert_eq!((65538..65539),
                   conn.partition_map.allocate_causetids(":einsteineinsteindb.part/user", 1));
        let tx_report1 = assert_transact!(conn, r#"[
            [:einsteineinsteindb/add 65538 :test/one 1]
            [:einsteineinsteindb/add 65538 :test/many 2]
            [:einsteineinsteindb/add 65538 :test/many 3]
        ]"#);
        let schema1 = conn.schema.clone();
        let partition_map1 = conn.partition_map.clone();

        assert_matches!(conn.last_transaction(),
                        "[[65538 :test/one 1 ?tx true]
                          [65538 :test/many 2 ?tx true]
                          [65538 :test/many 3 ?tx true]
                          [?tx :einsteineinsteindb/txInstant ?ms ?tx true]]");

        let second = "[
            [65536 :einsteineinsteindb/solitonid :test/one]
            [65536 :einsteineinsteindb/valueType :einsteineinsteindb.type/long]
            [65536 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one]
            [65536 :einsteineinsteindb/unique :einsteineinsteindb.unique/idcauset]
            [65536 :einsteineinsteindb/index true]
            [65537 :einsteineinsteindb/solitonid :test/many]
            [65537 :einsteineinsteindb/valueType :einsteineinsteindb.type/long]
            [65537 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/many]
            [65538 :test/one 1]
            [65538 :test/many 2]
            [65538 :test/many 3]
        ]";
        assert_matches!(conn.datoms(), second);

        let tx_report2 = assert_transact!(conn, r#"[
            [:einsteineinsteindb/add 65538 :test/one 2]
            [:einsteineinsteindb/add 65538 :test/many 2]
            [:einsteineinsteindb/retract 65538 :test/many 3]
            [:einsteineinsteindb/add 65538 :test/many 4]
        ]"#);
        let schema2 = conn.schema.clone();

        assert_matches!(conn.last_transaction(),
                        "[[65538 :test/one 1 ?tx false]
                          [65538 :test/one 2 ?tx true]
                          [65538 :test/many 3 ?tx false]
                          [65538 :test/many 4 ?tx true]
                          [?tx :einsteineinsteindb/txInstant ?ms ?tx true]]");

        let third = "[
            [65536 :einsteineinsteindb/solitonid :test/one]
            [65536 :einsteineinsteindb/valueType :einsteineinsteindb.type/long]
            [65536 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one]
            [65536 :einsteineinsteindb/unique :einsteineinsteindb.unique/idcauset]
            [65536 :einsteineinsteindb/index true]
            [65537 :einsteineinsteindb/solitonid :test/many]
            [65537 :einsteineinsteindb/valueType :einsteineinsteindb.type/long]
            [65537 :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/many]
            [65538 :test/one 2]
            [65538 :test/many 2]
            [65538 :test/many 4]
        ]";
        assert_matches!(conn.datoms(), third);

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            tx_report2.tx_id.., 1).expect("moved timeline");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        assert_matches!(conn.datoms(), second);
        // Moving didn't change the schema.
        assert_eq!(None, new_schema);
        assert_eq!(conn.schema, schema2);
        // But it did change the partition map.
        assert_eq!(conn.partition_map, partition_map1);

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            tx_report1.tx_id.., 2).expect("moved timeline");
        update_conn(&mut conn, &new_schema, &new_partition_map);
        assert_matches!(conn.datoms(), first);
        assert_eq!(None, new_schema);
        assert_eq!(schema1, conn.schema);
        assert_eq!(conn.partition_map, partition_map0);

        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            tx_report0.tx_id.., 3).expect("moved timeline");
        update_conn(&mut conn, &new_schema, &new_partition_map);
        assert_eq!(true, new_schema.is_some());
        assert_eq!(bootstrap::bootstrap_schema(), conn.schema);
        assert_eq!(partition_map_after_bootstrap, conn.partition_map);
        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
    }

    #[test]
    fn test_move_range() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let partition_map_after_bootstrap = conn.partition_map.clone();

        assert_eq!((65536..65539),
                   conn.partition_map.allocate_causetids(":einsteineinsteindb.part/user", 3));
        let tx_report0 = assert_transact!(conn, r#"[
            {:einsteineinsteindb/id 65536 :einsteineinsteindb/solitonid :test/one :einsteineinsteindb/valueType :einsteineinsteindb.type/long :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/one}
            {:einsteineinsteindb/id 65537 :einsteineinsteindb/solitonid :test/many :einsteineinsteindb/valueType :einsteineinsteindb.type/long :einsteineinsteindb/cardinality :einsteineinsteindb.cardinality/many}
        ]"#);

        assert_transact!(conn, r#"[
            [:einsteineinsteindb/add 65538 :test/one 1]
            [:einsteineinsteindb/add 65538 :test/many 2]
            [:einsteineinsteindb/add 65538 :test/many 3]
        ]"#);

        assert_transact!(conn, r#"[
            [:einsteineinsteindb/add 65538 :test/one 2]
            [:einsteineinsteindb/add 65538 :test/many 2]
            [:einsteineinsteindb/retract 65538 :test/many 3]
            [:einsteineinsteindb/add 65538 :test/many 4]
        ]"#);

        // Remove all of these transactions from the main timeline,
        // ensure we get back to a "just bootstrapped" state.
        let (new_schema, new_partition_map) = move_from_main_timeline(
            &conn.sqlite, &conn.schema, conn.partition_map.clone(),
            tx_report0.tx_id.., 1).expect("moved timeline");
        update_conn(&mut conn, &new_schema, &new_partition_map);

        update_conn(&mut conn, &new_schema, &new_partition_map);
        assert_eq!(true, new_schema.is_some());
        assert_eq!(bootstrap::bootstrap_schema(), conn.schema);
        assert_eq!(partition_map_after_bootstrap, conn.partition_map);
        assert_matches!(conn.datoms(), "[]");
        assert_matches!(conn.transactions(), "[]");
    }
}
