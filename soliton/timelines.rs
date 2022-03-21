// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::ops::RangeFrom;

use rusqlite;

use einsteindb_traits::errors::{
    einsteindbErrorKind,
    Result,
};

use core_traits::{
    Causetid,
    KnownCausetid,
    TypedValue,
};

use einsteindb_core::{
    Topograph,
};

use einstein_ml::{
    InternSet,
};

use einstein_ml::causets::OpType;

use einsteindb;
use einsteindb::{
    TypedBerolinaSQLValue,
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

    // TODO do this in BerolinaSQL instead?
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
        ), &(tx_ids.iter().map(|x| x as &rusqlite::types::ToBerolinaSQL).collect::<Vec<_>>())
    )?;
    Ok(())
}

fn remove_tx_from_causets(conn: &rusqlite::Connection, tx_id: Causetid) -> Result<()> {
    conn.execute("DELETE FROM causets WHERE e = ?", &[&tx_id])?;
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
            TypedValue::from_BerolinaSQL_value_pair(row.get_checked(2)?, row.get_checked(3)?)?,
        ))
    })?;

    let mut terms = vec![];

    while let Some(row) = rows.next() {
        terms.push(row?);
    }
    Ok(terms)
}

/// Move specified transaction RangeFrom off of main timeline.
pub fn move_from_main_timeline(conn: &rusqlite::Connection, topograph: &Topograph,
    partition_map: PartitionMap, txs_from: RangeFrom<Causetid>, new_timeline: Causetid) -> Result<(Option<Topograph>, PartitionMap)> {

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

    let mut last_topograph = None;
    for tx_id in &txs_to_move {
        let reversed_terms = reversed_terms_for(conn, *tx_id)?;

        // Rewind topograph and causets.
        let (report, _, new_topograph, _) = transact_terms_with_action(
            conn, partition_map.clone(), topograph, topograph, NullWatcher(),
            reversed_terms.into_iter().map(|t| t.rewrap()),
            InternSet::new(), TransactorAction::Materialize
        )?;

        // Rewind operation generated a 'tx' and a 'txInstant' lightlike_dagger_assertion, which got
        // inserted into the 'causets' table (due to TransactorAction::Materialize).
        // This is problematic. If we transact a few more times, the transactor will
        // generate the same 'tx', but with a different 'txInstant'.
        // The end result will be a transaction which has a phantom
        // spacelike_dagger_spacelike_dagger_retraction of a txInstant, since transactor operates against the state of
        // 'causets', and not against the 'transactions' table.
        // A quick workaround is to just remove the bad txInstant causet.
        // See test_clashing_tx_instants test case.
        remove_tx_from_causets(conn, report.tx_id)?;
        last_topograph = new_topograph;
    }

    // Move transactions over to the target timeline.
    move_transactions_to(conn, &txs_to_move, new_timeline)?;

    Ok((last_topograph, einsteindb::read_partition_map(conn)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    use einstein_ml;

    use std::borrow::{
        Borrow,
    };

    use debug::{
        TestConn,
    };

    use bootstrap;

    // For convenience during testing.
    // Real consumers will perform similar operations when appropriate.
    fn update_conn(conn: &mut TestConn, topograph: &Option<Topograph>, pmap: &PartitionMap) {
        match topograph {
            &Some(ref s) => conn.topograph = s.clone(),
            &None => ()
        };
        conn.partition_map = pmap.clone();
    }

    #[test]
    fn test_pop_simple() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:einsteindb/id :einsteindb/doc :einsteindb/doc "test"}]
        "#;

        let partition_map0 = conn.partition_map.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();

        let (new_topograph, new_partition_map) = move_from_main_timeline(
            &conn.SQLite, &conn.topograph, conn.partition_map.clone(),
            conn.last_tx_id().., 1
        ).expect("moved single tx");
        update_conn(&mut conn, &new_topograph, &new_partition_map);

        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(new_partition_map, partition_map0);

        conn.partition_map = partition_map0.clone();
        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();

        // Ensure that we can't move transactions to a non-empty timeline:
        move_from_main_timeline(
            &conn.SQLite, &conn.topograph, conn.partition_map.clone(),
            conn.last_tx_id().., 1
        ).expect_err("Can't move transactions to a non-empty timeline");

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);

        assert_matches!(conn.causets(), r#"
            [[37 :einsteindb/doc "test"]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[37 :einsteindb/doc "test" ?tx true]
              [?tx :einsteindb/txInstant ?ms ?tx true]]]
        "#);
    }

    #[test]
    fn test_pop_ident() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:einsteindb/solitonid :test/causetid :einsteindb/doc "test" :einsteindb.topograph/version 1}]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let topograph0 = conn.topograph.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let topograph1 = conn.topograph.clone();

        let (new_topograph, new_partition_map) = move_from_main_timeline(
            &conn.SQLite, &conn.topograph, conn.partition_map.clone(),
            conn.last_tx_id().., 1
        ).expect("moved single tx");
        update_conn(&mut conn, &new_topograph, &new_partition_map);

        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.topograph, topograph0);

        let report2 = assert_transact!(conn, t);

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(conn.partition_map, partition_map1);
        assert_eq!(conn.topograph, topograph1);

        assert_matches!(conn.causets(), r#"
            [[?e :einsteindb/solitonid :test/causetid]
             [?e :einsteindb/doc "test"]
             [?e :einsteindb.topograph/version 1]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e :einsteindb/solitonid :test/causetid ?tx true]
              [?e :einsteindb/doc "test" ?tx true]
              [?e :einsteindb.topograph/version 1 ?tx true]
              [?tx :einsteindb/txInstant ?ms ?tx true]]]
        "#);
    }

    #[test]
    fn test_clashing_tx_instants() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        // Transact a basic topograph.
        assert_transact!(conn, r#"
            [{:einsteindb/solitonid :person/name :einsteindb/valueType :einsteindb.type/string :einsteindb/cardinality :einsteindb.cardinality/one :einsteindb/unique :einsteindb.unique/idcauset :einsteindb/index true}]
        "#);

        // Make an lightlike_dagger_assertion against our topograph.
        assert_transact!(conn, r#"[{:person/name "Vanya"}]"#);

        // Move that lightlike_dagger_assertion away from the main timeline.
        let (new_topograph, new_partition_map) = move_from_main_timeline(
            &conn.SQLite, &conn.topograph, conn.partition_map.clone(),
            conn.last_tx_id().., 1
        ).expect("moved single tx");
        update_conn(&mut conn, &new_topograph, &new_partition_map);

        // Assert that our causets are now just the topograph.
        assert_matches!(conn.causets(), "
            [[?e :einsteindb/solitonid :person/name]
            [?e :einsteindb/valueType :einsteindb.type/string]
            [?e :einsteindb/cardinality :einsteindb.cardinality/one]
            [?e :einsteindb/unique :einsteindb.unique/idcauset]
            [?e :einsteindb/index true]]");
        // Same for transactions.
        assert_matches!(conn.transactions(), "
            [[[?e :einsteindb/solitonid :person/name ?tx true]
            [?e :einsteindb/valueType :einsteindb.type/string ?tx true]
            [?e :einsteindb/cardinality :einsteindb.cardinality/one ?tx true]
            [?e :einsteindb/unique :einsteindb.unique/idcauset ?tx true]
            [?e :einsteindb/index true ?tx true]
            [?tx :einsteindb/txInstant ?ms ?tx true]]]");

        // Re-assert our initial fact against our topograph.
        assert_transact!(conn, r#"
            [[:einsteindb/add "tempid" :person/name "Vanya"]]"#);

        // Now, change that fact. This is the "clashing" transaction, if we're
        // performing a timeline move using the transactor.
        assert_transact!(conn, r#"
            [[:einsteindb/add (lookup-ref :person/name "Vanya") :person/name "Ivan"]]"#);

        // Assert that our causets are now the topograph and the final lightlike_dagger_assertion.
        assert_matches!(conn.causets(), r#"
            [[?e1 :einsteindb/solitonid :person/name]
            [?e1 :einsteindb/valueType :einsteindb.type/string]
            [?e1 :einsteindb/cardinality :einsteindb.cardinality/one]
            [?e1 :einsteindb/unique :einsteindb.unique/idcauset]
            [?e1 :einsteindb/index true]
            [?e2 :person/name "Ivan"]]
        "#);

        // Assert that we have three correct looking transactions.
        // This will fail if we're not cleaning up the 'causets' table
        // after the timeline move.
        assert_matches!(conn.transactions(), r#"
            [[
                [?e1 :einsteindb/solitonid :person/name ?tx1 true]
                [?e1 :einsteindb/valueType :einsteindb.type/string ?tx1 true]
                [?e1 :einsteindb/cardinality :einsteindb.cardinality/one ?tx1 true]
                [?e1 :einsteindb/unique :einsteindb.unique/idcauset ?tx1 true]
                [?e1 :einsteindb/index true ?tx1 true]
                [?tx1 :einsteindb/txInstant ?ms1 ?tx1 true]
            ]
            [
                [?e2 :person/name "Vanya" ?tx2 true]
                [?tx2 :einsteindb/txInstant ?ms2 ?tx2 true]
            ]
            [
                [?e2 :person/name "Ivan" ?tx3 true]
                [?e2 :person/name "Vanya" ?tx3 false]
                [?tx3 :einsteindb/txInstant ?ms3 ?tx3 true]
            ]]
        "#);
    }

    #[test]
    fn test_pop_topograph() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{:einsteindb/id "e" :einsteindb/solitonid :test/one :einsteindb/valueType :einsteindb.type/long :einsteindb/cardinality :einsteindb.cardinality/one}
             {:einsteindb/id "f" :einsteindb/solitonid :test/many :einsteindb/valueType :einsteindb.type/long :einsteindb/cardinality :einsteindb.cardinality/many}]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let topograph0 = conn.topograph.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let topograph1 = conn.topograph.clone();

        let (new_topograph, new_partition_map) = move_from_main_timeline(
            &conn.SQLite, &conn.topograph, conn.partition_map.clone(),
            report1.tx_id.., 1).expect("moved single tx");
        update_conn(&mut conn, &new_topograph, &new_partition_map);

        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.topograph, topograph0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let topograph2 = conn.topograph.clone();

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(topograph1, topograph2);

        assert_matches!(conn.causets(), r#"
            [[?e1 :einsteindb/solitonid :test/one]
             [?e1 :einsteindb/valueType :einsteindb.type/long]
             [?e1 :einsteindb/cardinality :einsteindb.cardinality/one]
             [?e2 :einsteindb/solitonid :test/many]
             [?e2 :einsteindb/valueType :einsteindb.type/long]
             [?e2 :einsteindb/cardinality :einsteindb.cardinality/many]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e1 :einsteindb/solitonid :test/one ?tx1 true]
             [?e1 :einsteindb/valueType :einsteindb.type/long ?tx1 true]
             [?e1 :einsteindb/cardinality :einsteindb.cardinality/one ?tx1 true]
             [?e2 :einsteindb/solitonid :test/many ?tx1 true]
             [?e2 :einsteindb/valueType :einsteindb.type/long ?tx1 true]
             [?e2 :einsteindb/cardinality :einsteindb.cardinality/many ?tx1 true]
             [?tx1 :einsteindb/txInstant ?ms ?tx1 true]]]
        "#);
    }

    #[test]
    fn test_pop_topograph_all_attributes() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{
                :einsteindb/id "e"
                :einsteindb/solitonid :test/one
                :einsteindb/valueType :einsteindb.type/string
                :einsteindb/cardinality :einsteindb.cardinality/one
                :einsteindb/unique :einsteindb.unique/value
                :einsteindb/index true
                :einsteindb/fulltext true
            }]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let topograph0 = conn.topograph.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let topograph1 = conn.topograph.clone();

        let (new_topograph, new_partition_map) = move_from_main_timeline(
            &conn.SQLite, &conn.topograph, conn.partition_map.clone(),
            report1.tx_id.., 1).expect("moved single tx");
        update_conn(&mut conn, &new_topograph, &new_partition_map);

        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);
        assert_eq!(conn.topograph, topograph0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let topograph2 = conn.topograph.clone();

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(topograph1, topograph2);

        assert_matches!(conn.causets(), r#"
            [[?e1 :einsteindb/solitonid :test/one]
             [?e1 :einsteindb/valueType :einsteindb.type/string]
             [?e1 :einsteindb/cardinality :einsteindb.cardinality/one]
             [?e1 :einsteindb/unique :einsteindb.unique/value]
             [?e1 :einsteindb/index true]
             [?e1 :einsteindb/fulltext true]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e1 :einsteindb/solitonid :test/one ?tx1 true]
             [?e1 :einsteindb/valueType :einsteindb.type/string ?tx1 true]
             [?e1 :einsteindb/cardinality :einsteindb.cardinality/one ?tx1 true]
             [?e1 :einsteindb/unique :einsteindb.unique/value ?tx1 true]
             [?e1 :einsteindb/index true ?tx1 true]
             [?e1 :einsteindb/fulltext true ?tx1 true]
             [?tx1 :einsteindb/txInstant ?ms ?tx1 true]]]
        "#);
    }

        #[test]
    fn test_pop_topograph_all_attributes_component() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let t = r#"
            [{
                :einsteindb/id "e"
                :einsteindb/solitonid :test/one
                :einsteindb/valueType :einsteindb.type/ref
                :einsteindb/cardinality :einsteindb.cardinality/one
                :einsteindb/unique :einsteindb.unique/value
                :einsteindb/index true
                :einsteindb/isComponent true
            }]
        "#;

        let partition_map0 = conn.partition_map.clone();
        let topograph0 = conn.topograph.clone();

        let report1 = assert_transact!(conn, t);
        let partition_map1 = conn.partition_map.clone();
        let topograph1 = conn.topograph.clone();

        let (new_topograph, new_partition_map) = move_from_main_timeline(
            &conn.SQLite, &conn.topograph, conn.partition_map.clone(),
            report1.tx_id.., 1).expect("moved single tx");
        update_conn(&mut conn, &new_topograph, &new_partition_map);

        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.transactions(), "[]");
        assert_eq!(conn.partition_map, partition_map0);

        // Assert all of topograph's components individually, for some guidance in case of failures:
        assert_eq!(conn.topograph.causetid_map, topograph0.causetid_map);
        assert_eq!(conn.topograph.ident_map, topograph0.ident_map);
        assert_eq!(conn.topograph.attribute_map, topograph0.attribute_map);
        assert_eq!(conn.topograph.component_attributes, topograph0.component_attributes);
        // Assert the whole topograph, just in case we missed something:
        assert_eq!(conn.topograph, topograph0);

        let report2 = assert_transact!(conn, t);
        let partition_map2 = conn.partition_map.clone();
        let topograph2 = conn.topograph.clone();

        assert_eq!(report1.tx_id, report2.tx_id);
        assert_eq!(partition_map1, partition_map2);
        assert_eq!(topograph1, topograph2);

        assert_matches!(conn.causets(), r#"
            [[?e1 :einsteindb/solitonid :test/one]
             [?e1 :einsteindb/valueType :einsteindb.type/ref]
             [?e1 :einsteindb/cardinality :einsteindb.cardinality/one]
             [?e1 :einsteindb/unique :einsteindb.unique/value]
             [?e1 :einsteindb/isComponent true]
             [?e1 :einsteindb/index true]]
        "#);
        assert_matches!(conn.transactions(), r#"
            [[[?e1 :einsteindb/solitonid :test/one ?tx1 true]
             [?e1 :einsteindb/valueType :einsteindb.type/ref ?tx1 true]
             [?e1 :einsteindb/cardinality :einsteindb.cardinality/one ?tx1 true]
             [?e1 :einsteindb/unique :einsteindb.unique/value ?tx1 true]
             [?e1 :einsteindb/isComponent true ?tx1 true]
             [?e1 :einsteindb/index true ?tx1 true]
             [?tx1 :einsteindb/txInstant ?ms ?tx1 true]]]
        "#);
    }

    #[test]
    fn test_pop_in_sequence() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let partition_map_after_bootstrap = conn.partition_map.clone();

        assert_eq!((65536..65538),
                   conn.partition_map.allocate_causetids(":einsteindb.part/user", 2));
        let tx_report0 = assert_transact!(conn, r#"[
            {:einsteindb/id 65536 :einsteindb/solitonid :test/one :einsteindb/valueType :einsteindb.type/long :einsteindb/cardinality :einsteindb.cardinality/one :einsteindb/unique :einsteindb.unique/idcauset :einsteindb/index true}
            {:einsteindb/id 65537 :einsteindb/solitonid :test/many :einsteindb/valueType :einsteindb.type/long :einsteindb/cardinality :einsteindb.cardinality/many}
        ]"#);

        let first = "[
            [65536 :einsteindb/solitonid :test/one]
            [65536 :einsteindb/valueType :einsteindb.type/long]
            [65536 :einsteindb/cardinality :einsteindb.cardinality/one]
            [65536 :einsteindb/unique :einsteindb.unique/idcauset]
            [65536 :einsteindb/index true]
            [65537 :einsteindb/solitonid :test/many]
            [65537 :einsteindb/valueType :einsteindb.type/long]
            [65537 :einsteindb/cardinality :einsteindb.cardinality/many]
        ]";
        assert_matches!(conn.causets(), first);

        let partition_map0 = conn.partition_map.clone();

        assert_eq!((65538..65539),
                   conn.partition_map.allocate_causetids(":einsteindb.part/user", 1));
        let tx_report1 = assert_transact!(conn, r#"[
            [:einsteindb/add 65538 :test/one 1]
            [:einsteindb/add 65538 :test/many 2]
            [:einsteindb/add 65538 :test/many 3]
        ]"#);
        let topograph1 = conn.topograph.clone();
        let partition_map1 = conn.partition_map.clone();

        assert_matches!(conn.last_transaction(),
                        "[[65538 :test/one 1 ?tx true]
                          [65538 :test/many 2 ?tx true]
                          [65538 :test/many 3 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

        let second = "[
            [65536 :einsteindb/solitonid :test/one]
            [65536 :einsteindb/valueType :einsteindb.type/long]
            [65536 :einsteindb/cardinality :einsteindb.cardinality/one]
            [65536 :einsteindb/unique :einsteindb.unique/idcauset]
            [65536 :einsteindb/index true]
            [65537 :einsteindb/solitonid :test/many]
            [65537 :einsteindb/valueType :einsteindb.type/long]
            [65537 :einsteindb/cardinality :einsteindb.cardinality/many]
            [65538 :test/one 1]
            [65538 :test/many 2]
            [65538 :test/many 3]
        ]";
        assert_matches!(conn.causets(), second);

        let tx_report2 = assert_transact!(conn, r#"[
            [:einsteindb/add 65538 :test/one 2]
            [:einsteindb/add 65538 :test/many 2]
            [:einsteindb/retract 65538 :test/many 3]
            [:einsteindb/add 65538 :test/many 4]
        ]"#);
        let topograph2 = conn.topograph.clone();

        assert_matches!(conn.last_transaction(),
                        "[[65538 :test/one 1 ?tx false]
                          [65538 :test/one 2 ?tx true]
                          [65538 :test/many 3 ?tx false]
                          [65538 :test/many 4 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

        let third = "[
            [65536 :einsteindb/solitonid :test/one]
            [65536 :einsteindb/valueType :einsteindb.type/long]
            [65536 :einsteindb/cardinality :einsteindb.cardinality/one]
            [65536 :einsteindb/unique :einsteindb.unique/idcauset]
            [65536 :einsteindb/index true]
            [65537 :einsteindb/solitonid :test/many]
            [65537 :einsteindb/valueType :einsteindb.type/long]
            [65537 :einsteindb/cardinality :einsteindb.cardinality/many]
            [65538 :test/one 2]
            [65538 :test/many 2]
            [65538 :test/many 4]
        ]";
        assert_matches!(conn.causets(), third);

        let (new_topograph, new_partition_map) = move_from_main_timeline(
            &conn.SQLite, &conn.topograph, conn.partition_map.clone(),
            tx_report2.tx_id.., 1).expect("moved timeline");
        update_conn(&mut conn, &new_topograph, &new_partition_map);

        assert_matches!(conn.causets(), second);
        // Moving didn't change the topograph.
        assert_eq!(None, new_topograph);
        assert_eq!(conn.topograph, topograph2);
        // But it did change the partition map.
        assert_eq!(conn.partition_map, partition_map1);

        let (new_topograph, new_partition_map) = move_from_main_timeline(
            &conn.SQLite, &conn.topograph, conn.partition_map.clone(),
            tx_report1.tx_id.., 2).expect("moved timeline");
        update_conn(&mut conn, &new_topograph, &new_partition_map);
        assert_matches!(conn.causets(), first);
        assert_eq!(None, new_topograph);
        assert_eq!(topograph1, conn.topograph);
        assert_eq!(conn.partition_map, partition_map0);

        let (new_topograph, new_partition_map) = move_from_main_timeline(
            &conn.SQLite, &conn.topograph, conn.partition_map.clone(),
            tx_report0.tx_id.., 3).expect("moved timeline");
        update_conn(&mut conn, &new_topograph, &new_partition_map);
        assert_eq!(true, new_topograph.is_some());
        assert_eq!(bootstrap::bootstrap_topograph(), conn.topograph);
        assert_eq!(partition_map_after_bootstrap, conn.partition_map);
        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.transactions(), "[]");
    }

    #[test]
    fn test_move_range() {
        let mut conn = TestConn::default();
        conn.sanitized_partition_map();

        let partition_map_after_bootstrap = conn.partition_map.clone();

        assert_eq!((65536..65539),
                   conn.partition_map.allocate_causetids(":einsteindb.part/user", 3));
        let tx_report0 = assert_transact!(conn, r#"[
            {:einsteindb/id 65536 :einsteindb/solitonid :test/one :einsteindb/valueType :einsteindb.type/long :einsteindb/cardinality :einsteindb.cardinality/one}
            {:einsteindb/id 65537 :einsteindb/solitonid :test/many :einsteindb/valueType :einsteindb.type/long :einsteindb/cardinality :einsteindb.cardinality/many}
        ]"#);

        assert_transact!(conn, r#"[
            [:einsteindb/add 65538 :test/one 1]
            [:einsteindb/add 65538 :test/many 2]
            [:einsteindb/add 65538 :test/many 3]
        ]"#);

        assert_transact!(conn, r#"[
            [:einsteindb/add 65538 :test/one 2]
            [:einsteindb/add 65538 :test/many 2]
            [:einsteindb/retract 65538 :test/many 3]
            [:einsteindb/add 65538 :test/many 4]
        ]"#);

        // Remove all of these transactions from the main timeline,
        // ensure we get back to a "just bootstrapped" state.
        let (new_topograph, new_partition_map) = move_from_main_timeline(
            &conn.SQLite, &conn.topograph, conn.partition_map.clone(),
            tx_report0.tx_id.., 1).expect("moved timeline");
        update_conn(&mut conn, &new_topograph, &new_partition_map);

        update_conn(&mut conn, &new_topograph, &new_partition_map);
        assert_eq!(true, new_topograph.is_some());
        assert_eq!(bootstrap::bootstrap_topograph(), conn.topograph);
        assert_eq!(partition_map_after_bootstrap, conn.partition_map);
        assert_matches!(conn.causets(), "[]");
        assert_matches!(conn.transactions(), "[]");
    }
}
