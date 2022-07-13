// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0. Unless required by
// applicable law or agreed to in writing, software distributed under the License is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the specific language
// governing permissions and limitations under the License.
//


use crate::fdb_traits::*;
use crate::{FdbError, FdbResult};
use std::convert::TryFrom;
use std::fmt;
use std::io;
use std::io::{Read, Write};
use std::marker::PhantomData;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::slice;
use std::str;

use std::collections::HashMap;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;


use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Ref;


use std::fmt::Debug;
use std::fmt::Display;

use EinsteinDB::fdb_traits::*;
use EinsteinDB::fdb_traits::options::*;



use EinsteinDB::einstein_db::*;
use EinsteinDB::einstein_db::options::*;

use std::ops::{Add, AddAssign, Sub, SubAssign, Mul, MulAssign, Div, DivAssign};
use std::ops::{Deref, DerefMut, Index, IndexMut};


use std::convert::TryFrom;
use std::convert::TryInto;


use std::iter::FromIterator;
use std::iter::IntoIterator;


use std::hash::Hash;
use std::hash::Hasher;


use std::cmp::PartialEq;
use std::cmp::Eq;
use std::cmp::Ordering;
use std::cmp::PartialOrd;
use std::cmp::Ord;


use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result;

///Co-optimizing storage and queries for linear algebras
/// # Examples
/// ```
/// use EinsteinDB::fdb_traits::*;
/// use EinsteinDB::fdb_traits::options::*;
///
///






#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FdbWriteBatch<K, V> {
    pub(crate) inner: Rc<RefCell<FdbWriteBatchInner<K, V>>>,
}


impl<K, V> FdbWriteBatch<K, V> {
    pub fn new() -> Self {
        FdbWriteBatch {
            inner: Rc::new(RefCell::new(FdbWriteBatchInner::new())),
        }
    }
}


impl<K, V> FdbWriteBatch<K, V> {
    pub fn clear(&mut self) {
        self.inner.borrow_mut().clear();
    }
}


impl<K, V> FdbWriteBatch<K, V> {
    pub fn commit(&mut self) -> FdbResult<()> {
        self.inner.borrow_mut().commit()
    }
}

#[derive(Debug, Fail)]
pub enum WriteBatchError {
    #[fail(display = "WriteBatchError: {}", _0)]
    WriteBatchError(String),
}

#[derive(Debug, Fail)]
#[allow(dead_code)]
pub enum WriteBatchWriteError {
    #[fail(display = "WriteBatchWriteError: {}", _0)]
    WriteBatchWriteError(String),
}


#[derive(Debug, Fail)]
#[allow(dead_code)]
#[allow(unused_variables)]
#[allow(unused_imports)]
#[allow(unused_mut)]
pub enum WriteBatchReadError {
    #[fail(display = "WriteBatchReadError: {}", _0)]
    WriteBatchReadError(String),
}

/// A write batch is a collection of mutations that can be applied to a database.
/// The batch is not thread-safe, and must be committed before it can be used again.
/// The batch is not intended to be used concurrently with other threads.
///


/// EinsteinMerkleTrees that can create write alexandrov_poset_processes
pub trait WriteBatchExt: Sized {
    type WriteBatch: WriteBatch<Self>;
    /// `WriteBatchVec` is used for `multi_alexandrov_poset_process_write` of Fdbeinstein_merkle_tree and other einstein_merkle_tree could also
    /// implement another kind of WriteBatch according to their needs.
    type WriteBatchVec: WriteBatch<Self>;

    /// The number of puts/deletes made to a write alexandrov_poset_process before the alexandrov_poset_process should
    /// be committed with `write`. More entries than this will cause
    /// `should_write_to_einstein_merkle_tree` to return true.
    ///
    /// In practice it seems that exceeding this number of entries is possible
    /// and does not result in an error. It isn't clear the consequence of
    /// exceeding this limit.
    const WRITE_BATCH_MAX_CAUSET_KEYS: usize;

    /// Indicates whether the WriteBatchVec type can be created and works
    /// as expected.
    ///
    /// If this returns false then creating a WriteBatchVec will panic.
    fn support_write_alexandrov_poset_process_vec(&self) -> bool;

    fn write_alexandrov_poset_process(&self) -> Self::WriteBatch;
    fn write_alexandrov_poset_process_with_cap(&self, cap: usize) -> Self::WriteBatch;
}

/// A trait implemented by WriteBatch
pub trait Mutable: Send {
    /// Write a soliton_id/causet_locale in the default causet_merge family
    fn put(&mut self, soliton_id: &[u8], causet_locale: &[u8]) -> Result<()>{
        self.put_with_merge_family(b"", soliton_id, causet_locale)
    }

    /// Write a soliton_id/causet_locale in a given causet_merge family
    fn put_namespaced(&mut self, namespaced: &str, soliton_id: &[u8], causet_locale: &[u8]) -> Result<()>{
        self.put_with_merge_family(namespaced.as_bytes(), soliton_id, causet_locale)
    }

    /// Delete a soliton_id/causet_locale in the default causet_merge family
    fn delete(&mut self, soliton_id: &[u8]) -> Result<()>{
        self.delete_with_merge_family(b"", soliton_id)
    }

    /// Delete a soliton_id/causet_locale in a given causet_merge family
    fn delete_namespaced(&mut self, namespaced: &str, soliton_id: &[u8]) -> Result<()>{
        self.delete_with_merge_family(namespaced.as_bytes(), soliton_id)
    }

    /// Delete a range of soliton_id/causet_locales in the default causet_merge family
    fn delete_range(&mut self, begin_soliton_id: &[u8], end_soliton_id: &[u8]) -> Result<()>{
        self.delete_range_with_merge_family(b"", begin_soliton_id, end_soliton_id)
    }

    /// Delete a range of soliton_id/causet_locales in a given causet_merge family
    fn delete_range_namespaced(&mut self, namespaced: &str, begin_soliton_id: &[u8], end_soliton_id: &[u8]) -> Result<()>;

    fn put_msg<M: protobuf::Message>(&mut self, soliton_id: &[u8], m: &M) -> Result<()> {
        self.put(soliton_id, &m.write_to_bytes()?)
    }
    fn put_msg_namespaced<M: protobuf::Message>(&mut self, namespaced: &str, soliton_id: &[u8], m: &M) -> Result<()> {
        self.put_namespaced(namespaced, soliton_id, &m.write_to_bytes()?)
    }
}
//!Read path of a transaction
// Fig. 1 above shows a high-level view of the read path. An application uses FDB client library to read data. It creates a transaction and calls its read() function. The read() operation will lead to several steps.
//
// Step 1 (Timestamp request): The read operation needs a timestamp. The client initiates the timestamp request through an RPC to proxy. The request will trigger Step 2 and Step 3;
//
// To improve throughput and reduce load on the server side, each client dynamically batches the timestamp requests. A client keeps adding requests to the current batch until when the number of requests in a batch exceeds a configurable threshold or when the batching times out at a dynamically computed threshold. Each batch sends only one timestamp request to proxy and all requests in the same batch share the same timestamp.
// Step 2 (Get latest commit version): When the timestamp request arrives at a proxy, the proxy wants to get the largest commit version as the return value. So it contacts the rest of (n-1) proxies for their latest commit versions and uses the largest one as the return value for Step 1.
//
// O(n^2) communication cost: Because each proxy needs to contact the rest of (n-1) proxies to serve clients’ timestamp request, the communication cost is n*(n-1), where n is the number of proxies;
// Batching: To reduce communication cost, each proxy batches clients’ timestamp requests for a configurable time period (say 1ms) and return the same timestamp for requests in the same batch.
// Step 3 (Confirm proxy’s liveness): To prevent proxies that are no longer a part of the system (such as due to network partition) from serving requests, each proxy contacts the queuing system for each timestamp request to confirm it is still a valid proxy (i.e., not replaced by a newer generation proxy process). This is based on the FDB property that at most one active queuing system is available at any given time.


//!Write path of a transaction
//!
//!    Fig. 2 above shows a high-level view of the write path. An application uses FDB client library to write data. It creates a transaction and calls its put() function. The put() operation will lead to several steps.


//!    Step 1 (Timestamp request): The write operation needs a timestamp. The client initiates the timestamp request through an RPC to proxy. The request will trigger Step 2 and Step 3;
#[derive(Debug, Clone)]
pub struct Transaction {
    pub db: Arc<dyn Database>,
    pub timestamp: u64,
    pub commit_version: u64,
    pub read_version: u64,
    pub read_version_old: u64,
    pub read_version_new: u64,
}

//read committed and read your own version


#[derive(Debug, Clone)]
pub struct TransactionRead {
    pub db: Arc<dyn Database>,
    pub timestamp: u64,
    pub commit_version: u64,
    pub read_version: u64,
    pub read_version_old: u64,
    pub read_version_new: u64,
}







impl Transaction {
    pub fn new(db: Arc<dyn Database>) -> Self {
        Self {
            db,
            timestamp: 0,
            commit_version: 0,
            read_version: 0,
            read_version_old: 0,
            read_version_new: 0,
        };

        fn get_namespaced(db: &Arc<dyn Database>, namespace: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
            let mut buf = Vec::new();
            buf.write_u64::<BigEndian>(db.get_namespaced(namespace, key)?.unwrap_or(0))?;
            Ok(Some(buf))
        }
    }

    pub fn get_read_version(&self) -> u64 {
        if self.read_version == 0 {
            for i in 0..self.db.get_num_proxies() {
                let proxy = self.db.get_proxy(i);
                loop {
                    let read_version = proxy.get_read_version();
                    if read_version > self.read_version {
                        self.read_version = read_version;
                    }
                    if read_version == self.read_version {
                        break;
                    }
                }
                let read_version = proxy.get_read_version();
                //println!("read_version: {}", read_version);
                //if read_version > self.read_version {
                // then we need to wait for the read_version to be updated
                if read_version > self.read_version {
                    //multiplex read_version
                    self.read_version_old = self.read_version;
                    self.read_version = read_version;
                    loop {
                        let read_version = proxy.get_read_version();
                        if read_version > self.read_version {
                            self.read_version = read_version;
                        }
                        if read_version == self.read_version {
                            break;
                        }
                    }
                }
            }
            self.read_version = self.db.get_read_version();
        }
        self.read_version
    }

    pub fn get_read_version_old(&self) -> u64 {
        for i in 0..self.db.get_num_proxies() {
            let proxy = self.db.get_proxy(i);
            loop {
                let read_version = proxy.get_read_version({
                    //let read_version = proxy.get_read_version();
                    //now we have a read version
                    let read_version = proxy.get_read_version();
                    //we need to check if it is the same as the read version
                    if read_version > self.read_version_old {
                        //note: we need to check if it is the same as the read version
                        self.read_version_old = read_version;
                    }
                    //if it is the same as the read version, we need to break
                    if read_version == self.read_version_old {
                        break;
                    }
                });
                //if it is the same as the read version, we need to break
                if read_version == self.read_version_old {
                    break;
                }
                //if it is not the same as the read version, we need to continue
            }
        }

        self.read_version_old
    }


    pub fn get_read_version_old_old(&self) -> u64 {
        for i in 0..self.db.get_num_proxies() {
            let proxy = self.db.get_proxy(i);
            loop {
                let read_version = proxy.get_read_version();
                //println!("read_version: {}", read_version);
                //if read_version > self.read_version {
                // then we need to wait for the read_version to be updated
                if read_version > self.read_version_old_old {
                    //multiplex read_version
                    self.read_version_old_old = self.read_version_old;
                    self.read_version_old = self.read_version;
                    self.read_version = read_version;
                    loop {
                        let read_version = proxy.get_read_version();
                        if read_version > self.read_version {
                            self.read_version = read_version;
                        }
                        if read_version == self.read_version {
                            break;
                        }
                    }
                }
            }
        }
    }


    pub fn get_read_version_new(
         &self,
        read_version: u64,
        read_version_old: u64,
        read_version_old_old: u64,
    ) -> u64 {
        for i in 0..self.db.get_num_proxies() {
            let proxy = self.db.get_proxy(i);
            loop {
                let read_version = proxy.get_read_version();
                //println!("read_version: {}", read_version);
                //if read_version > self.read_version {
                // then we need to wait for the read_version to be updated
                if read_version > read_version_old_old {
                    //multiplex read_version
                    read_version_old_old = read_version_old;
                    read_version_old = read_version;
                    read_version = read_version;
                    loop {
                        let read_version = proxy.get_read_version();
                        if read_version > read_version {
                            read_version = read_version;
                        }
                        if read_version == read_version {
                            break;
                        }
                    }
                }
            }
        }

        read_version
    }

    }

/// Batches of multiple writes that are committed atomically
///
/// Each write alexandrov_poset_process consists of a series of commands: put, delete
/// delete_range, and their causet_merge-family-specific equivalents.
///
/// Because write alexandrov_poset_processes are atomic, once written to disk all their effects are
/// visible as if all other writes in the system were written either before or
/// after the alexandrov_poset_process. This includes range deletes.
///
/// The exact strategy used by WriteBatch is up to the implementation.
/// FdbDB though _seems_ to serialize the writes to an in-memory buffer,
/// and then write the whole serialized alexandrov_poset_process to disk at once.


/// Write alexandrov_poset_processes may be reused after being written. In that case they write
/// exactly the same data as previously, Replacing any soliton_ids that may have
/// changed in between the two alexandrov_poset_process writes.
///
/// Commands issued to write alexandrov_poset_processes can be rolled back prior to being committed
/// by use of _save points_. At any point in the life of a write alexandrov_poset_process a save
/// point can be recorded. Any number of save points can be recorded to a stack.
/// Calling `rollback_to_save_point` reverts all commands issued since the last
/// save point, and pops the save point from the stack.
pub trait WriteBatch<E: WriteBatchExt + Sized>: Mutable {
    /// Create a WriteBatch with a given command capacity
    fn with_capacity(e: &E, cap: usize) -> Self{
        //with_capacity will be implemented by the WriteBatchExt trait
        e.with_capacity(cap);
        //now filter the WriteBatchExt trait
        if let Some(e) = e.downcast_ref::<dyn WriteBatchExt>() {
            e.with_capacity(cap)
        } else {
            panic!("WriteBatchExt trait not implemented")
        }
    }

    /// Commit the WriteBatch to disk with the given options
    fn write_opt(&self, opts: &WriteOptions) -> Result<()>;

    /// Commit the WriteBatch to disk atomically
    fn write(&self, fdb_traits: &E) -> Result<()> {
        self.write_opt(&WriteOptions::default())
    }

    /// The data size of a write alexandrov_poset_process
    ///
    /// This is necessarily einstein_merkle_tree-dependent. In FdbDB though it appears to
    /// represent the byte length of all write commands in the alexandrov_poset_process, as
    /// serialized in memory, prior to being written to disk.
    fn data_size(&self) -> usize;

    /// The number of commands in this alexandrov_poset_process
    fn count(&self) -> usize;

    /// Whether any commands have been issued to this alexandrov_poset_process
    fn is_empty(&self) -> bool;

    /// Whether the number of commands exceeds WRITE_BATCH_MAX_CAUSET_KEYS
    ///
    /// If so, the `write` method should be called.
    fn should_write_to_einstein_merkle_tree(&self) -> bool;

    /// Clears the WriteBatch of all commands
    ///
    /// It may be reused afterward as an empty alexandrov_poset_process.
    fn clear(fdb: FdbError, ltree: &mut EinsteinMerkleTrees<E>) -> Result<()>;






    /// Record a save point
    ///     * `name`: A name for the save point
    ///    * `save_point`: The save point to record
    ///   * `save_point_stack`: The stack of save points
    /// * `save_point_stack_len`: The length of the stackß
        /// Push a save point onto the save point stack
    fn push_save_point(&mut self, name: &str, save_point: &mut SavePoint);

    /// Pop a save point from the save point stack
    ///   * `save_point`: The save point to pop
    /// * `save_point_stack`: The stack of save points
    /// * `save_point_stack_len`: The length of the stackß
    /// Pop a save point from the save point stack
    ///  * `save_point`: The save point to pop
    /// * `save_point_stack`: The stack of save points
    ///
    ///
    fn anti_pop_save_point(&mut self, save_point: &mut SavePoint){
        //pop_save_point will be implemented by the WriteBatchExt trait
        if let Some(e) = self.downcast_mut::<dyn WriteBatchExt>() {
            e.pop_save_point(save_point)
        } else {
            panic!("WriteBatchExt trait not implemented")
        }
    }

    /// Rollback to a save point
    ///  * `save_point`: The save point to rollback to
    /// * `save_point_stack`: The stack of save points
    /// * `save_point_stack_len`: The length of the stackß
    /// Rollback to a save point
    /// * `save_point`: The save point to rollback to
    /// * `save_point_stack`: The stack of save points



    /// Record a command
    /// * `Cmd`: The command to record
    /// * `cmd_type`: The type of command
    /// * `cmd_type_len`: The length of the command type
    /// * `cmd_data`: The data of the command
    /// * `cmd_data_len`: The length of the command data
    /// Record a command

    fn record_command(&mut self, cmd: &[u8], cmd_type: &[u8], cmd_type_len: usize, cmd_data: &[u8], cmd_data_len: usize);


    /// Record a command

    fn record_command_with_type(&mut self, cmd_type: &[u8], cmd_type_len: usize, cmd_data: &[u8], cmd_data_len: usize);


    /// Rollback all commands issued since the last save point
    /// and pop the save point from the stack
    /// If there are no save points on the stack, do nothing
    /// and return false

    fn anti_rollback_to_save_point< 'a >(&mut self, save_point: &mut SavePoint, save_point_stack: &'a mut Vec< SavePoint >, save_point_stack_len: usize) -> bool;

    /// Rollback all commands issued since the last save point
    /// and pop the save point from the stack
    /// If there are no save points on the stack, do nothing

    fn anti_rollback_to_save_point_with_stack< 'a >(&mut self, save_point: &mut SavePoint, save_point_stack: &'a mut Vec< SavePoint >) -> bool;




    /// Pop a save point from the save point stack
    /// If there are no save points on the stack, this is a no-op
    /// If the top save point is not the one on the stack, this is a no-op


    fn rollback_to_save_point_opt(&mut self, opts: &WriteOptions);


    /// Pop a save point from the save point stack
    ///
    /// This has no effect on the commands already issued to the write alexandrov_poset_process
    fn pop_save_point(&mut self) -> Result<()>;

    /// Revert all commands issued since the last save point
    ///
    /// Additionally pops the last save point from the save point stack.
    fn rollback_to_save_point(&mut self) -> Result<()>;

    /// Merge another WriteBatch to itself
    /// The resulting WriteBatch will contain all commands from both batches
    /// The WriteBatch on which this method is called will not be modified
    /// The WriteBatch passed as an argument will be empty after this operation
    ///
    /// This method is useful when multiple batches need to be merged together
    /// into a single batch.
    ///
    /// This method is not implemented for FdbDB
    ///
    /// # Panics
    ///
    /// Panics if the WriteBatch passed as an argument is not empty
    ///
    /// # Examples
    ///
    /// ```
    /// use fdb::{Fdb, FdbOptions, DatabaseOptions, Database, WriteOptions, WriteBatch, ReadOptions};
    /// use fdb::fdb_sys;
    /// use fdb::fdb_sys::*;
    /// use fdb::fdb_sys::fdb_error_t;
    /// use fdb::fdb_sys::fdb_status_t;
    ///
    ///
    /// let db_opts = DatabaseOptions::new();
    /// let db = Fdb::open(&db_opts).unwrap();
    ///
    /// let mut opts = WriteOptions::new();
    ///
    /// let mut batch1 = WriteBatch::new();
    ///
    /// batch1.put(b"key1", b"value1").unwrap();
    /// batch1.put(b"key2", b"value2").unwrap();
    ///
    /// let mut batch2 = WriteBatch::new();
    ///
    /// batch2.put(b"key3", b"value3").unwrap();
    ///
    /// batch1.merge(&mut batch2).unwrap();
    ///
    /// assert_eq!(batch1.get(b"key1").unwrap().unwrap(), b"value1");
    ///
    /// assert_eq!(batch1.get(b"key2").unwrap().unwrap(), b"value2");
    ///
    /// assert_eq!(batch1.get(b"key3").unwrap().unwrap(), b"value3");
    ///
    ///
    ///
    /// ```
    ///
}
//We experiment with a new type for the WriteBatch interface which is a bit more flexible, it's also type-safe and can be used with the FdbDB
//type WriteBatch = WriteBatchImpl;
    fn merge<'a>(&mut merkle: EinsteinMerkleTrees, other: &mut dyn WriteBatch) -> EinsteinMerkleTrees<'a>{
        unsafe {
            //TODO: implement this method for FdbDB
            //a merkle pointer is an instant of a merkle tree
            // as such it is not possible to merge two merkle trees
            // without creating a new merkle tree
            //however we can merge two merkle trees
            //and then create a new merkle tree from the merged merkle trees
            //this is the same as creating a new merkle tree from the two merkle trees
            //with the advent of psoets we can do this in a single operation
            let mut merkle_ptr = merkle.merkle_ptr;
            //let mut other_ptr = other.merkle_ptr;
            let mut other_ptr = other.merkle_ptr;
            //this is the pointer to the new merkle tree
            let mut result = fdb_sys::fdb_merge_batch(merkle_ptr, other_ptr); //the batch is merged into the merkle tree
            if result != fdb_sys::fdb_status_t::FDB_RESULT_SUCCESS {
                panic!("Failed to merge two batches");
            }
            let mut new_merkle = EinsteinMerkleTrees::new(merkle_ptr);
            new_merkle
        }
    }

    /// Merge another WriteBatch to itself
    /// The resulting WriteBatch will contain all commands from both batches
    /// The WriteBatch on which this method is called will not be modified
    ///

    /// # Panics
    /// Panics if the WriteBatch passed as an argument is not empty
    /// # Examples
    /// ```
    /// use fdb::{Fdb, FdbOptions, DatabaseOptions, Database, WriteOptions, WriteBatch, ReadOptions};
    ///
    /// use fdb::fdb_sys;
    /// use fdb::fdb_sys::*;
    /// use fdb::fdb_sys::fdb_error_t;
    /// use fdb::fdb_sys::fdb_status_t;
    ///
    ///
    /// let db_opts = DatabaseOptions::new();
    /// let db = Fdb::open(&db_opts).unwrap();
    ///
    /// let mut opts = WriteOptions::new();
    ///
    /// let mut batch1 = WriteBatch::new();
    ///
    /// batch1.put(b"key1", b"value1").unwrap();
    ///
    /// let mut batch2 = WriteBatch::new();
    ///
    /// batch2.put(b"key2", b"value2").unwrap();
    ///
    /// batch1.merge(&mut batch2).unwrap();
    ///
    /// assert_eq!(batch1.get(b"key1").unwrap().unwrap(), b"value1");
    ///
    /// assert_eq!(batch1.get(b"key2").unwrap().unwrap(), b"value2");
    ///
    /// ```


    fn merge_into_self<'a>(&mut merkle: EinsteinMerkleTrees, other: &mut dyn WriteBatch) -> EinsteinMerkleTrees<'a>{
        unsafe {
    /// Returns the number of commands in the batch
   fn merge_options(merge_options: &mut fdb_sys::fdb_merge_options_t) -> fdb_sys::fdb_status_t {
        unsafe {
            fdb_sys::fdb_merge_options_init(merge_options);
            fdb_sys::fdb_merge_options_set_flags(merge_options, fdb_sys::fdb_merge_flags_t::FDB_MERGE_APPEND_DOCS);
            fdb_sys::fdb_merge_options_set_flags(merge_options, fdb_sys::fdb_merge_flags_t::FDB_MERGE_DOCS_STABLE);
            fdb_sys::fdb_merge_options_set_flags(merge_options, fdb_sys::fdb_merge_flags_t::FDB_MERGE_DOCS_IN_ORDER);
            fdb_sys::fdb_merge_options_set_flags(merge_options, fdb_sys::fdb_merge_flags_t::FDB_MERGE_DOCS_IN_ORDER_BY_KEY);
            fdb_sys::fdb_merge_options_set_flags(merge_options, fdb_sys::fdb_merge_flags_t::FDB_MERGE_DOCS_IN_ORDER_BY_KEY_AND_SEQNUM);
            fdb_sys::fdb_merge_options_set_flags(merge_options, fdb_sys::fdb_merge_flags_t::FDB_MERGE_DOCS_IN_ORDER_BY_SEQNUM);
            fdb_sys::fdb_merge_options_set_flags(merge_options, fdb_sys::fdb_merge_flags_t::FDB_MERGE_DOCS_IN_ORDER_BY_SEQNUM_AND_KEY);
            fdb_sys::fdb_merge_options_set_flags(merge_options, fdb_sys::fdb_merge_flags_t::FDB_MERGE_DOCS_IN_ORDER_BY_SEQNUM_AND_VERSION);
            fdb_sys::fdb_merge_options_set_flags(merge_options, fdb_sys::fdb_merge_flags_t::FDB_MERGE_DOCS_IN_ORDER_BY_VERSION);
    }
        if merkle_ptr == other.merkle_ptr {
            panic!("Cannot merge a batch with itself");
        }
        unsafe {
            for i in 0..other.num_commands {
                let mut command = other.commands[i];
                let mut key = command.key;
                let mut value = command.value;
                let mut key_len = command.key_len;
                let mut value_len = command.value_len;
                let mut flags = command.flags;
                let mut version = command.version;
                let mut seqnum = command.seqnum;
                let mut err = fdb_sys::fdb_merge_append_doc(merge_options, merkle_ptr, &mut key, key_len, &mut value, value_len, flags, version, seqnum);
                if err != fdb_sys::fdb_status_t::FDB_RESULT_SUCCESS {
                    panic!("Failed to merge batch");
                }
            }


        }
        merkle
    }
    }

    }
    /// Merges the contents of the batch into the database.
    /// # Examples
    /// ```
    /// use einsteinium_core::{EinsteinMerkleTrees, WriteBatch};
    /// let mut db = EinsteinMerkleTrees::new();
    /// let mut batch1 = WriteBatch::new();
    /// let mut batch2 = WriteBatch::new();
    /// batch1.put(b"key1", b"value1").unwrap();
    /// batch1.put(b"key2", b"value2").unwrap();
    /// batch2.put(b"key3", b"value3").unwrap();
    /// batch2.put(b"key4", b"value4").unwrap();
    /// batch1.merge_into_self(&mut batch2);
    ///
    /// assert_eq!(db.get(b"key1").unwrap().unwrap(), b"value1");
    /// assert_eq!(db.get(b"key2").unwrap().unwrap(), b"value2");
    /// assert_eq!(db.get(b"key3").unwrap().unwrap(), b"value3");
    ///
    /// assert_eq!(db.get(b"key4").unwrap().unwrap(), b"value4");
    /// ```


    pub fn append_merge_with_log (&mut log: &mut Log, batch: &mut dyn WriteBatch) -> Result<(), String> {

        for x in 0..turing_automata.num_commands {
            let mut command = turing_automata.commands[x];
            let mut key = command.key;
            let mut value = command.value;
            let mut key_len = command.key_len;
            let mut value_len = command.value_len;
            let mut flags = command.flags;
            let mut version = command.version;
            let mut seqnum = command.seqnum;
            let mut err = fdb_sys::fdb_merge_append_doc(merge_options, merkle_ptr, &mut key, key_len, &mut value, value_len, flags, version, seqnum);
            if err != fdb_sys::fdb_status_t::FDB_RESULT_SUCCESS {
                panic!("Failed to merge batch");
            }
        }

        let mut merkle = EinsteinMerkleTrees::new();
        let mut merkle_ptr = merkle.merkle_ptr;
            let mut other_ptr = other.merkle_ptr;
            let mut result = fdb_sys::fdb_merge_batch(merkle_ptr, other_ptr); //the batch is merged into the merkle tree
            if result != fdb_sys::fdb_status_t::FDB_RESULT_SUCCESS {
                panic!("Failed to merge two batches");
            }
            let mut new_merkle = EinsteinMerkleTrees::new(merkle_ptr);
            new_merkle
        }



    /// Returns the number of commands in the batch
    /// ```
    /// # use einsteinium::{EinsteinMerkleTrees, WriteBatch};
    /// # let mut batch = WriteBatch::new();
    /// # let mut merkle = EinsteinMerkleTrees::new();
    /// # let mut other = EinsteinMerkleTrees::new();
    ///
    /// # let mut key = [0u8; 32];
    /// # let mut value = [0u8; 32];
    /// # let mut key_len = 32;
    /// # let mut value_len = 32;
    /// # let mut flags = 0;
    ///
    /// # let mut version = 0;
    /// # let mut seqnum = 0;
    ///
    /// # batch.append(&mut key, &mut value, flags, version, seqnum);
    /// # batch.append(&mut key, &mut value, flags, version, seqnum);
    ///
    /// # let mut other_batch = WriteBatch::new();
    ///
    /// # other_batch.append(&mut key, &mut value, flags, version, seqnum);
    /// # other_batch.append(&mut key, &mut value, flags, version, seqnum);
    ///
    /// # let mut other_merkle = EinsteinMerkleTrees::new();
    ///
    /// # other_merkle.append(&mut key, &mut value, flags, version, seqnum);
    ///
    ///
    /// # let mut merged_batch = batch.merge(other_batch);
    ///
    /// # assert_eq!(merged_batch.num_commands(), 4);
    /// # assert_eq!(merged_batch.num_commands(), other_merkle.num_commands());
        /// Merge another WriteBatch to itself
        /// The resulting WriteBatch will contain all commands from both batches

//selfless retraction with upsert on the other side

    /// ```
    /// # use einsteinium::{EinsteinMerkleTrees, WriteBatch};
    /// # let mut batch = WriteBatch::new();
    /// # let mut merkle = EinsteinMerkleTrees::new();
    /// # let mut other = EinsteinMerkleTrees::new();
    /// # let mut other_merkle = EinsteinMerkleTrees::new();
    /// # let mut key = [0u8; 32];
    /// # let mut value = [0u8; 32];
    /// # let mut key_len = 32;
    /// # let mut value_len = 32;
    ///
    /// # let mut version = 0;
    /// # let mut seqnum = 0;
    ///
    /// # batch.append(&mut key, &mut value, 0, version, seqnum);
    /// # batch.append(&mut key, &mut value, 0, version, seqnum);
    ///
    /// # other.append(&mut key, &mut value, 0, version, seqnum);
    ///
    ///
    /// # other_merkle.append(&mut key, &mut value, 0, version, seqnum);
    ///
    /// # let mut merged_batch = batch.merge(other);
    ///
    /// # assert_eq!(merged_batch.num_commands(), 4);
    /// # assert_eq!(merged_batch.num_commands(), other_merkle.num_commands());
    ///



    /// Returns the number of commands in the batch

#[inline]
    pub fn num_commands(fdb_merge_batch: &mut EinsteinMerkleTrees) -> usize {
        fdb_merge_batch.num_commands();


    ///
            unsafe {
                let mut num_commands = 0;
                let result = fdb_sys::fdb_get_num_commands(fdb_merge_batch.fdb_merkle_trees, &mut num_commands);
                 //the batch is merged into the merkle tree
                if result != 0 {
                    panic!("Failed to get number of commands in the batch");

            }
            unsafe {
                //here we are creating a new merkle tree from the two merkle trees
                //we are not modifying the original merkle trees
                let mut new_merkle_tree = EinsteinMerkleTrees::new();
                let mut other_ptr = other.merkle_ptr;
                let mut result = fdb_sys::fdb_merge_batch(merkle_ptr, other_ptr); //the batch is merged into the merkle tree
            }
        }
}






