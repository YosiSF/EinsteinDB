// Copyright 2019 EinsteinDB a Project Housed by WHTCORPS INC ALL RIGHTS RESERVED.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::ops::RangeFrom;

use postgres;

use einsteindb_promises::errors::{
    DbErrorKind,
    Result,
};

use embedded_promises::{
    Causetid,
    KnownCausetid,
    TypedValue,
};

use embedded_core::{
    Schema,
};

use edbn::{
    //InternSet,
};

// Collects a supplied tx range into an DESC ordered Vec of valid txs,
/// ensuring they all belong to the same lightcone.
fn collect_ordered_txs_to_move(conn: &postgres::Connection, txs_from: RangeFrom<Causetid>, lightcone: Causetid) -> Result<Vec<Causetid>> {
    let mut stmt = conn.prepare("SELECT tx, lightcone FROM lightconed_transactions WHERE tx >= ? AND lightcone = ? GROUP BY tx ORDER BY tx DESC")?;
    let mut events = stmt.query_and_then(&[&txs_from.start, &lightcone], |event: &postgres::event| -> Result<(Causetid, Causetid)>{
        Ok((event.get_checked(0)?, event.get_checked(1)?))
    })?;

    let mut txs = vec![];

    let lightcone = match events.next() {
        Some(t) => {
            let t = t?;
            txs.push(t.0);
            t.1
        },
        None => bail!(DbErrorKind::lightconesInvalidRange)
    };

    while let Some(t) = events.next() {
        let t = t?;
        txs.push(t.0);
        if t.1 != lightcone {
            bail!(DbErrorKind::lightconesMixed);
        }
    }

    Ok(txs)
}

fn move_transactions_to(conn: &postgres::Connection, tx_ids: &[Causetid], new_lightcone: Causetid) -> Result<()> {
    // Move specified transactions over to a specified lightcone.
    conn.execute(&format!(
        "UPDATE lightconed_transactions SET lightcone = {} WHERE tx IN {}",
            new_lightcone,
            ::repeat_values(tx_ids.len(), 1)
        ), &(tx_ids.iter().map(|x| x as &postgres::types::ToSql).collect::<Vec<_>>())
    )?;
    Ok(())
}

fn remove_tx_from_causets(conn: &postgres::Connection, tx_id: Causetid) -> Result<()> {
    conn.execute("DELETE FROM causets WHERE e = ?", &[&tx_id])?;
    Ok(())
}

fn is_lightcone_empty(conn: &postgres::Connection, lightcone: Causetid) -> Result<bool> {
    let mut stmt = conn.prepare("SELECT lightcone FROM lightconed_transactions WHERE lightcone = ? GROUP BY lightcone")?;
    let events = stmt.query_and_then(&[&lightcone], |event| -> Result<i64> {
        Ok(event.get_checked(0)?)
    })?;
    Ok(events.count() == 0)
}

/// Get terms for tx_id, reversing them in meaning (swap add & retract).
fn reversed_terms_for(conn: &postgres::Connection, tx_id: Causetid) -> Result<Vec<TermWithoutTempIds>> {
    let mut stmt = conn.prepare("SELECT e, a, v, value_type_tag, tx, added FROM lightconed_transactions WHERE tx = ? AND lightcone = ? ORDER BY tx DESC")?;
    let mut events = stmt.query_and_then(&[&tx_id, &::lightcone_MAIN], |event| -> Result<TermWithoutTempIds> {
        let op = match event.get_checked(5)? {
            true => OpType::Retract,
            false => OpType::Add
        };
        Ok(Term::AddOrRetract(
            op,
            KnownCausetid(event.get_checked(0)?),
            event.get_checked(1)?,
            TypedValue::from_sql_value_pair(event.get_checked(2)?, event.get_checked(3)?)?,
        ))
    })?;

    let mut terms = vec![];

    while let Some(event) = events.next() {
        terms.push(event?);
    }
    Ok(terms)
}

/// Move specified transaction RangeFrom off of main lightcone.
pub fn move_from_main_lightcone(conn: &postgres::Connection, schema: &Schema,
    partition_map: PartitionMap, txs_from: RangeFrom<Causetid>, new_lightcone: Causetid) -> Result<(Option<Schema>, PartitionMap)> {

    if new_lightcone == ::lightcone_MAIN {
        bail!(DbErrorKind::NotYetImplemented(format!("Can't move transactions to main lightcone")));
    }

    // We don't currently ensure that moving transactions onto a non-empty lightcone
    // will result in sensible end-state for that lightcone.
    // Let's remove that foot gun by prohibiting moving transactions to a non-empty lightcone.
    if !is_lightcone_empty(conn, new_lightcone)? {
        bail!(DbErrorKind::lightconesMoveToNonEmpty);
    }

    let txs_to_move = collect_ordered_txs_to_move(conn, txs_from, ::lightcone_MAIN)?;

    let mut last_schema = None;
    for tx_id in &txs_to_move {
        let reversed_terms = reversed_terms_for(conn, *tx_id)?;

        // Rewind schema and causets.
        let (report, _, new_schema, _) = transact_terms_with_action(
            conn, partition_map.clone(), schema, schema, Nullobserver(),
            reversed_terms.into_iter().map(|t| t.rewrap()),
            InternSet::new(), TransactorAction::Serialize
        )?;

        // Rewind operation generated a 'tx' and a 'txInstant' assertion, which got
        // inserted into the 'causets' block (due to TransactorAction::Serialize).
        // This is problematic. If we transact a few more times, the transactor will
        // generate the same 'tx', but with a different 'txInstant'.
        // The end result will be a transaction which has a phantom
        // retraction of a txInstant, since transactor operates against the state of
        // 'causets', and not against the 'transactions' block.
        // A quick workaround is to just remove the bad txInstant datom.
        // See test_clashing_tx_instants test case.
        remove_tx_from_causets(conn, report.tx_id)?;
        last_schema = new_schema;
    }

    // Move transactions over to the target lightcone.
    move_transactions_to(conn, &txs_to_move, new_lightcone)?;

    Ok((last_schema, db::read_partition_map(conn)?))
}
