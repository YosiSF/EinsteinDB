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

pub struct PerfContext {
    name: String,
    start_time: Instant,
    end_time: Instant,
    parent: Option<Arc<PerfContext>>,
    children: Vec<Arc<PerfContext>>,
    child_count: AtomicUsize,
    child_count_mutex: Mutex<usize>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DBVectorImpl {
    db: Arc<DB>,
    cf: CFHandle,
    key: Vec<u8>,
    value: Vec<u8>,
    version: u64,
    pub(crate) inner: Arc<DBVectorInner>,
}




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DBVectorIteratorImpl {
    db: Arc<DB>,
    cf: CFHandle,
    key: Vec<u8>,
    value: Vec<u8>,
    version: u64,
    pub(crate) inner: Arc<DBVectorIteratorInner>,
}

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

    inner: Arc<DBVectorInner<T>>,

    // This is a pointer to the inner vector.
    // It is stored here so that we can easily get a reference to the inner vector.
    // This is necessary because the inner vector is not safe to share between threads.
    // The inner vector is only safe to share between threads if it is wrapped in an Arc.
    // This is done in the `new` function.
    inner_ptr: *const DBVectorInner<T>,
    



}


impl<T> DBVector<T> {
    /// Creates a new, empty vector.
    /// # Examples
    /// ```
    /// use einsteindb_server::db_vector::DBVector;
    /// let v = DBVector::new();
    /// ```
    /// # Panics
    /// Panics if the vector cannot be created.
    /// This can only happen if the vector is created in a single-threaded code.
    /// If the vector is created in concurrent code, then it is safe to panic.
    /// ```
    /// use einsteindb_server::db_vector::DBVector;
    /// let v = DBVector::new();
    /// let v2 = DBVector::new();
    /// ```
    /// # Panics
    /// Panics if the vector cannot be created.
    


    pub fn new() -> DBVector<T> {
        let inner = Arc::new(DBVectorInner {
            db: Arc::new(DB::new()),
            cf: CFHandle::new(0),
            key: Vec::new(),
            value: Vec::new(),
            version: 0,
            inner: Arc::new(DBVectorInner {
                db: Arc::new(DB::new()),
                cf: CFHandle::new(0),
                key: Vec::new(),
                value: Vec::new(),
                version: 0,
                inner: Arc::new(DBVectorInner {
                    db: Arc::new(DB::new()),
                    cf: CFHandle::new(0),
                    key: Vec::new(),
                    value: Vec::new(),
                    version: 0,
                    inner: Arc::new(DBVectorInner {
                        db: Arc::new(DB::new()),
                        cf: CFHandle::new(0),
                        key: Vec::new(),
                        value: Vec::new(),
                        version: 0,
                        inner: Arc::new(DBVectorInner {
                            db: Arc::new(DB::new()),
                            cf: CFHandle::new(0),
                            key: Vec::new(),
                            value: Vec::new(),
                            version: 0,
                            inner: Arc::new(DBVectorInner {
                                db: Arc::new(DB::new()),
                                cf: CFHandle::new(0),
                                key: Vec::new(),
                                value: Vec::new(),
                                version: 0,
                                inner: Arc::new(DBVectorInner {
                                    db: Arc::new(DB::new()),
                                    cf: CFHandle::new(0),
                                    key: Vec::new(),
                                    value: Vec::new(),
                                    version: 0,
                                    inner: Arc::new(DBVectorInner {
                                        db: Arc::new(DB::new()),
                                        cf: CFHandle::new(0),
                                        key: Vec::new(),
                                        value: Vec:: new(),
                                        version: 0,
                                    }),
                                }),
                            }),
                        }),
                    }),
                }),
            }),
        });
        DBVector {
            inner: inner.clone(),
            inner_ptr: &*inner,
        }
    }
}


impl<T> DBVector<T> {
    /// Creates a new, empty vector.
    /// 
    /// 
    

    pub fn new_with_capacity(capacity: usize) -> DBVector<T> {
        let inner = Arc::new(DBVectorInner {
            db: Arc::new(DB::new()),
            cf: CFHandle::new(0),
            key: Vec::new(),
            value: Vec::new(),
            version: 0,
            inner: Arc::new(DBVectorInner {
                db: Arc::new(DB::new()),
                cf: CFHandle::new(0),
                key: Vec::new(),
                value: Vec::new(),
                version: 0,
                inner: Arc::new(DBVectorInner {
                    db: Arc::new(DB::new()),
                    cf: CFHandle::new(0),
                    key: Vec::new(),
                    value: Vec::new(),
                    version: 0,
                    inner: Arc::new(DBVectorInner {
                        db: Arc::new(DB::new()),
                        cf: CFHandle::new(0),
                        key: Vec::new(),
                        value: Vec:: new(),
                        version: 0,
                        inner: Arc::new(DBVectorInner {
                            db: Arc::new(DB::new()),
                            cf: CFHandle::new(0),
                            key: Vec::new(),
                            value: Vec:: new(),
                            version: 0,
                            inner: Arc::new(DBVectorInner {
                                db: Arc::new(DB::new()),
                                cf: CFHandle::new(0),
                                key: Vec::new(),
                                value: Vec:: new(),
                                version: 0,
                                inner: Arc::new(DBVectorInner {
                                    db: Arc::new(DB::new()),
                                    cf: CFHandle::new(0),
                                    key: Vec::new(),
                                    value: Vec:: new(),
                                    version: 0,
                                    inner: Arc::new(DBVectorInner {
                                        db: Arc::new(DB::new()),
                                        cf: CFHandle::new(0),
                                        key: Vec::new(),
                                    }),
                                }),
                            }),
                        }),
                    }),
                }),
            }),
        });
        DBVector {
            inner: inner.clone(),
            inner_ptr: &*inner,
        }
    }

    /// Create a new vector.
    /// This is a convenience function that calls `DBVector::new()`.
    /// # Examples
    /// ```
    /// use einsteindb_server::db_vector::DBVector;
    /// let v = DBVector::new();
    /// ```
    /// # Panics
    /// This function will panic if the vector is created on a thread that
    /// is not the main thread.
    /// ```
    /// use einsteindb_server::db_vector::DBVector;
    /// let v = DBVector::new();
    /// let v2 = DBVector::new();
    /// ```
    /// # Panics
    /// This function will panic if the vector is created on a thread that
    /// is not the main thread.
    /// ```
    /// use einsteindb_server::db_vector::DBVector;
    /// let v = DBVector::new();
    /// let v2 = DBVector::new();
    /// ```
    /// # Panics
    /// This function will panic if the vector is created on a thread that

    /// Create a new vector.
    /// # Examples
    /// ```
    /// use einsteindb_server::db_vector::DBVector;
    /// let v = DBVector::new();
    /// ```
    /// 

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
    let mut v = Vec::new();
    let mut cursor = conn.get_cursor().unwrap();
    cursor.seek(range).unwrap();

    // Collect the txs into a Vec in DESC order
    while let Some(tx) = cursor.next().unwrap() {
        v.push(tx);
    }

    // Check that the txs are in DESC order
    for i in 1..v.len() {
        assert!(v[i] < v[i - 1]);
    }

    v
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepeatableRead {
    pub(crate) inner: Arc<RepeatableReadInner>,
}


//noinspection ALL
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RepeatableReadInner {
    pub(crate) inner: Arc<RepeatableReadInnerInner>,
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


   //.query_and_then(&[&txs_from.start, &timeline], |row: &rusqlite::Row| -> Result<(causetid, causetid)>{
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

///CHANGELOG: 
/// - Added tests for iter_mut
/// - Added tests for iter_mut_panic