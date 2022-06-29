// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{cmp, mem};
use std::collections::Bound::{Excluded, Unbounded};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet, VecDeque};
use std::fmt::{self, Debug};
use std::hash::{BuildHasher, Hash};
use std::iter::{FromIterator, FusedIterator, Peekable};
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::AtomicPtr;
use std::sync::atomic::Ordering::{Acquire, Release, SeqCst};
use std::{ptr, slice};
use std::{u32, usize};
use fdb_traits::Causet;
use EinsteinDB::storage::{
    CFHandle, DBIterator, DBIteratorImpl, DBIteratorImplTrait, DBRawIterator, DBRawIteratorImpl,
    DBRawIteratorImplTrait, DBVector, DBVectorImpl, DBVectorImplTrait, DBVectorIterator,
    DBVectorIteratorImpl, DBVectorIteratorImplTrait, DBVectorIteratorTrait, DBVectorTrait,
    Error, ErrorInner, Result,
};
use FoundationDB::*;
use allegro_poset::{ Poset};
use einstein_db_alexandrov_processing::{ Processing, ProcessingImpl};
use einstein_db::{ DB, DBImpl};
use causet::{CausetImpl};
use causets::{CausetsImpl};
use causetq::*;
use berolinasql::{ Query, QueryImpl};
use std::ops::Deref;
use soliton::{SolitonImpl};
use soliton_panic::{SolitonPanicImpl, SolitonPanic, SolitonPanicTrait};

///! A vector that can be safely shared between threads.
///  This is a wrapper around `Vec<T>` that is safe to share between threads.
/// It is safe to use in concurrent code because it uses atomic operations.
/// It is also safe to use in single-threaded code because it uses a spinlock
/// to ensure that only one thread can access the vector at a time.
/// It is not safe to use in code that uses multiple threads without using
/// a mutex to ensure that only one thread can access the vector at a time.
/// # Examples
///
/// ```
/// use einsteindb_server::db_vector::DBVector;
/// let mut v = DBVector::new();
/// v.push(1);
/// v.push(2);
/// v.push(3);
/// assert_eq!(v.get(0), Some(&1));
/// assert_eq!(v.get(1), Some(&2));
/// assert_eq!(v.get(2), Some(&3));
/// ```


pub struct DBVector<T> {
    data: Arc<DBVectorInner<T>>,
}

#[derive(Debug)]
struct DBVectorInner<T> {
    data: Vec<T>,
    len: AtomicUsize,
    cap: AtomicUsize,
    lock: AtomicPtr<()>,
}

///! Collects a supplied tx range into an DESC ordered Vec of valid txs,
///!  ensuring they all belong to the same timeline.
/// # Examples
/// ```
/// use einsteindb_server::db_vector::DBVector;
/// let mut v = DBVector::new();
/// v.push(1);
/// v.push(2);
/// v.push(3);
/// assert_eq!(v.get(0), Some(&1));
///
/// ```
/// # Panics
/// Panics if the supplied range is not valid.
/// # Panics
///
#[inline]
pub fn collect_causet_preorder(conn: &Connection, range: &Range<u64>) -> Vec<u64> {
    conn.prepare("SELECT tx_id FROM causet_preorder WHERE tx_id >= ? AND tx_id <= ?").unwrap();
    Vec::new();
   //.query_and_then(&[&txs_from.start, &timeline], |row: &rusqlite::Row| -> Result<(Entid, Entid)>{

    let row = conn.query_row(&[&range.start, &range.end], |row: &rusqlite::Row| -> Result<u64>{
        Ok(row.get(0)?)
    })?;

    let mut txs = Vec::new();

    let brane = match rows.next() {
        Some(t) => {
       let t = t?;
            txs.push(t);
            t.1
    }, None => {
        return txs;
    }
    };

    while let Some(t) = rows.next() {
        let t = t?;
         txs.push.push(t);
        if t.1 != brane {
            return txs;
        }
    }

    txs
}

///! relativistic version of collect_causet_preorder using worldline_id
/// ! in this case, the worldline_id is the same as the tx_id
/// ! On the other hand, the worldline_id is not the same as the tx_id when the tx_id is a causet
/// ! The worldline_id is the same as the tx_id when the tx_id is a FoundationDB tx
/// ! A Partially ordered set is a set of transactions that are causet-preordered with respect to a given worldline_id


    /*    // Move specified transactions over to a specified timeline.
    conn.execute(&format!(
        "UPDATE timelined_transactions SET timeline = {} WHERE tx IN {}",
            new_timeline,
            ::repeat_values(tx_ids.len(), 1)
        ), &(tx_ids.iter().map(|x| x as &rusqlite::types::ToSql).collect::<Vec<_>>())
    )?;
    Ok(())*/


///! Collects a supplied tx range into an DESC ordered Vec of valid txs,
/// ! ensuring they all belong to the same timeline.
/// # Examples
/// ```
/// use einsteindb_server::db_vector::DBVector;
/// let mut v = DBVector::new();
/// v.push(1);
/// v.push(2);
/// v.push(3);
/// assert_eq!(v.get(0), Some(&1));
/// ```
///
fn collect_causet_preorder_worldline(conn: &Connection, range: &Range<u64>) -> Vec<u64> {
    connection.prepare(&format!("UPDATE timeline_transactions SET timeline = {} WHERE tx IN {}",
        new_timeline,
        ::repeat_values(tx_ids.len(), 1)
    )).unwrap();
}


///! Collects a supplied tx range into an DESC ordered Vec of valid txs,
/// ! ensuring they all belong to the same timeline.
///



fn remove_causet_from_brane(connection: &Connection, brane: u64, tx_id: u64) -> Result<()> {
    connection.execute("DELETE FROM causet_preorder WHERE tx_id = ? AND brane = ?", &[&tx_id, &brane])?;
    Ok(());
}
#[inline]
pub fn multiplex_timeline_with_brane(connection: &Connection, brane: u64, tx_id: u64) -> Result<()> {
    if let mut causet_preorder = connection.prepare("SELECT tx_id FROM causet_preorder WHERE brane = ?")? {
        if let Some(row) = causet_preorder.query_row(&[&brane], |row: &rusqlite::Row| -> Result<u64>{
            for tx_id in row.columns() {
                let tx_id = row.get(0)?;
                if tx_id == brane {
                    return Ok(tx_id);
                }
                Ok(tx_id)
            }
            wait_group.add(1);
            let mut txs = Vec::new();
        })? {
            txs.push(row);
            Ok(row.get(0)?)
        }
    }
    Ok(())
}

#[inline]
pub fn collect_causet_preorder_relativistic(connection: &Connection, brane: u64, tx_id: u64) -> Result<()> {
    if let mut causet_preorder = connection.prepare("SELECT tx_id FROM causet_preorder WHERE brane = ?")? {
        if let Some(row) = causet_preorder.query_row(&[&brane], |row: &rusqlite::Row| -> Result<u64>{
            for tx_id in row.columns() {
                let tx_id = row.get(0)?;
                if tx_id == brane {
                    return Ok(tx_id);
                }
                Ok(tx_id)
            }
            wait_group.add(1);
            let mut txs = Vec::new();
        })? {
            txs.push(row);
            Ok(row.get(0)?)
        }
    }
    Ok(())
}


#[inline]
pub fn collect_causet_preorder_relativistic_with_brane(connection: &Connection, brane: u64, tx_id: u64) -> Result<()> {
    if let mut causet_preorder = connection.prepare("SELECT tx_id FROM causet_preorder WHERE brane = ?")? {
        if let Some(row) = causet_preorder.query_row(&[&brane], |row: &rusqlite::Row| -> Result<u64>{
            for tx_id in row.columns() {
                let tx_id = row.get(0)?;
                if tx_id == brane {
                    return Ok(tx_id);
                }
                Ok(tx_id)
            }
            wait_group.add(1);
            let mut txs = Vec::new();
        })? {
            txs.push(row);
            Ok(row.get(0)?)
        }
    }
    Ok(())
}


#[inline]
pub fn collect_causet_preorder_relativistic_with_brane_and_timeline(connection: &Connection, brane: u64, tx_id: u64) -> Result<()> {
    if let mut causet_preorder = connection.prepare("SELECT tx_id FROM causet_preorder WHERE brane = ?")? {
        if let Some(row) = causet_preorder.query_row(&[&brane], |row: &rusqlite::Row| -> Result<u64>{
            for tx_id in row.columns() {
                let tx_id = row.get(0)?;
                if tx_id == brane {
                    return Ok(tx_id);
                }
                Ok(tx_id)
            }
            wait_group.add(1);
            let mut txs = Vec::new();
        })? {
            txs.push(row);
            Ok(row.get(0)?)
        }
    }
    Ok(())
}


#[inline]
pub fn collect_causet_preorder_relativistic_with_brane_and_timeline_and_brane(connection: &Connection, brane: u64, tx_id: u64) -> Result<()> {
    if let mut causet_preorder = connection.prepare("SELECT tx_id FROM causet_preorder WHERE brane = ?")? {
        if let Some(row) = causet_preorder.query_row(&[&brane], |row: &rusqlite::Row| -> Result<u64>{
            for tx_id in row.columns() {
                let tx_id = row.get(0)?;
                if tx_id == brane {
                    return Ok(tx_id);
                }
                Ok(tx_id)
            }
            wait_group.add(1);
            let mut txs = Vec::new();
        })? {
            txs.push(row);
            Ok(row.get(0)?)
        }
    }
    Ok(())
}


   //.query_and_then(&[&txs_from.start, &timeline], |row: &rusqlite::Row| -> Result<(Entid, Entid)>{
fn collect_causet_preorder_relativistic_with_brane_and_timeline_and_brane_and_timeline(connection: &Connection, brane: u64, tx_id: u64) -> Result<()> {
    let brane = match rows.next() {
        Some(t) => {
       let t = t?;
            txs.push(t.0);
            t.1
    }, None => {
        return txs;
    }
    };


    txs
}
pub fn move_tx_to_timeline(connection: &Connection, tx_id: u64, timeline: u64) -> Result<()> {
    partition_map_tx_to_timeline(connection, tx_id, timeline)?;

    if new_timeline == 0 {
        connection.execute("DELETE FROM timeline_transactions WHERE tx = ?", &[&tx_id])?;
    } else {
        connection.execute("UPDATE timeline_transactions SET timeline = ? WHERE tx = ?", &[&new_timeline, &tx_id])?;
    }

    Ok(())
}


///! Move specified transactions over to a specified timeline.
/// ! in this case, the worldline_id is the same as the tx_id
/// ! On the other hand, the worldline_id is not the same as the tx_id when the tx_id is a causet
/// ! The worldline_id is the same as the tx_id when the tx_id is a FoundationDB tx
/// ! A Partially ordered set is a set of transactions that are causet-preordered with respect to a given worldline_id
/// ! This function is used to move transactions to a new timeline




fn partition_map_tx_to_timeline(connection: &Connection, tx_id: u64, timeline: u64) -> Result<()> {

// We don't currently ensure that moving transactions onto a non-empty timeline
    // will result in sensible end-state for that timeline.
    // Let's remove that foot gun by prohibiting moving transactions to a non-empty timeline.
    // This is a bit of a hack, but it's a bit of a hack to have to do this.
    // We should probably have a separate function for this.

    connection.execute("UPDATE timeline_transactions SET timeline = ? WHERE tx_id = ?", &[&timeline, &tx_id])?;
    Ok(())
}


// We don't currently ensure that moving transactions onto a non-empty timeline
// will result in sensible end-state for that timeline.
// Let's remove that foot gun by prohibiting moving transactions to a non-empty timeline.

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::iter::FromIterator;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::RwLock;
    use std::thread;
    use std::time::Duration;
    use std::time::Instant;
    use std::u64;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;
    use std::u64::MIN;
    use std::u64::MAX;

    use fdb::Database;
    use fdb::DatabaseOptions;
    use fdb::FdbError;
    use fdb::FdbResult;
    use fdb::FdbSafeHandle;
    use fdb::FdbSafeTransaction;
    use fdb::FdbTransaction;

    use fdb::FdbTransactOptions;
    use fdb::FdbTransactOptions::FdbTransactOptions;


    use fdb::FdbTransactOptions::FdbTransactOptions;

    fn create_db() -> Database {
        let db_options = DatabaseOptions::new();
        let db = Database::open(&db_options).unwrap();
        db
    }

    fn create_db_with_timeline(timeline: u64) -> Database {
        let db_options = DatabaseOptions::new();
        let db = Database::open(&db_options).unwrap();
        db.set_option(FdbOption::FDB_OPT_TIMELINE_FACTORY, &timeline).unwrap();
        db
    }


    fn create_db_with_timeline_and_transact_options(timeline: u64, transact_options: FdbTransactOptions) -> Database {
        let db_options = DatabaseOptions::new();
        let db = Database::open(&db_options).unwrap();
        db.set_option(FdbOption::FDB_OPT_TIMELINE_FACTORY, &timeline).unwrap();
        db.set_option(FdbOption::FDB_OPT_TRANSACTION_POOL_SIZE, &transact_options).unwrap();
        db
    }

        fn create_db_with_timeline_and_transact_options_and_max_retries(timeline: u64, transact_options: FdbTransactOptions, max_retries: u64) -> Database {
        let db_options = DatabaseOptions::new();
        let db = Database::open(&db_options).unwrap();
        db.set_option(FdbOption::FDB_OPT_TIMELINE_FACTORY, &timeline).unwrap();
        db.set_option(FdbOption::FDB_OPT_TRANSACTION_POOL_SIZE, &transact_options).unwrap();
        db.set_option(FdbOption::FDB_OPT_TRANSACTION_RETRY_MAX, &max_retries).unwrap();
        db
    }


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DBVectorIter<'a, T: 'a> {
    pub db: &'a Database,
    pub key: &'a [u8],
    pub value: &'a [u8],
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DBVectorIterMut<'a, T: 'a> {
    causet_txn: &'a FdbSafeTransaction,
    causet_handle: &'a FdbSafeHandle,
    data: &'a [T],
    index: usize,
}

#[derive(Debug)]
pub struct PanicCauset;

impl Causet for PanicCauset {
    fn get_transaction(&self, db: &Database) -> FdbResult<FdbSafeTransaction> {
        panic!("panic");
    }
}

impl Deref for PanicCauset {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        panic!()
    }
}

impl<'a> PartialEq<&'a [u8]> for PanicCauset {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}

impl<'a> PartialEq<&'a [u8]> for &'a [u8] {
    fn eq(&self, rhs: &&[u8]) -> bool {
        **rhs == **self
    }
}


    #[test]
    fn test_db_vector_iter() {
        let db = create_db();
        let key = b"key";
        let value = b"value";
        db.set(key, value).unwrap();
        let iter = db.iter(key).unwrap();
        let mut iter_mut = db.iter_mut(key).unwrap();
        assert_eq!(iter.next().unwrap().unwrap(), value);
        assert_eq!(iter_mut.next().unwrap().unwrap(), value);
    }

    #[test]
    fn test_db_vector_iter_mut() {
        let db = create_db();
        let key = b"key";
        let value = b"value";
        db.set(key, value).unwrap();
        let iter = db.iter(key).unwrap();
        let mut iter_mut = db.iter_mut(key).unwrap();
        assert_eq!(iter.next().unwrap().unwrap(), value);
        assert_eq!(iter_mut.next().unwrap().unwrap(), value);
    }

    #[test]
    fn test_db_vector_iter_mut_panic() {
        let db = create_db();
        let key = b"key";
        let value = b"value";
        db.set(key, value).unwrap();
        let iter = db.iter(key).unwrap();
        let mut iter_mut = db.iter_mut(key).unwrap();
        assert_eq!(iter.next().unwrap().unwrap(), value);
        assert_eq!(iter_mut.next().unwrap().unwrap(), value);
    }

    #[test]
    fn test_db_vector_iter_mut_panic_2() {
        let db = create_db();
        let key = b"key";
        let value = b"value";
        db.set(key, value).unwrap();
        let iter = db.iter(key).unwrap();
        let mut iter_mut = db.iter_mut(key).unwrap();

        assert_eq!(iter.next().unwrap().unwrap(), value);
        assert_eq!(iter_mut.next().unwrap().unwrap(), value);

        let mut iter_mut = db.iter_mut(key).unwrap();
    }
}