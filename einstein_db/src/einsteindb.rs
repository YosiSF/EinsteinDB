// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


// Language: rust



use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::io::{Error as IoError, ErrorKind};
use byteorder::{BigEndian, ReadBytesExt};
use std::collections::BTreeMap;
use std::io::{Cursor, Read};
use std::sync::Arc;
use wots;

use wots::{Wots, WotsKey};
use prng;
use prng::Prng;
use std::io::{self, Read, Write};
use wots::{WotsKeyPair, WotsSignature};
use wots::{WotsPublicKey, WotsPrivateKey};
use std::time::{Duration, Instant};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

pub(crate) const EINSTEINDB_MAGIC: &[u8] = b"Einsteindb";
pub(crate) const EINSTEINDB_VERSION: u8 = 1;
pub(crate) const EINSTEINDB_HEADER_SIZE: usize = 16;
pub(crate) const EINSTEINDB_ITEM_SIZE: usize = 32;
pub(crate) const EINSTEINDB_ITEM_HEADER_SIZE: usize = 8;
pub(crate) const EINSTEINDB_ITEM_TYPE_SIZE: usize = 1;
pub(crate) const EINSTEINDB_ITEM_TYPE_HEADER_SIZE: usize = 1;


pub(crate) const EINSTEINDB_ITEM_TYPE_DATA: u8 = 0;
pub(crate) const EINSTEINDB_ITEM_TYPE_INDEX: u8 = 1;
pub use crate::einstein_db_alexandrov_processing::*;



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EinsteinDB {
    pub events: Vec<String>,
    pub edges: Vec<(String, String)>,
    pub signatures: Vec<WotsSignature>,
    pub public_keys: Vec<WotsPublicKey>,
    pub private_keys: Vec<WotsPrivateKey>,
    pub key_pairs: Vec<WotsKeyPair>,
    pub prng: Prng,
    pub prng_seed: Vec<u8>,
    pub prng_seed_len: usize,
    pub prng_seed_len_bytes: usize,
    pub prng_seed_len_bits: usize,
}


impl EinsteinDB {
    pub fn new() -> EinsteinDB {
        EinsteinDB {
            events: Vec::new(),
            edges: Vec::new(),
            signatures: Vec::new(),
            public_keys: Vec::new(),
            private_keys: Vec::new(),
            key_pairs: Vec::new(),
            prng: Prng::new(),
            prng_seed: Vec::new(),
            prng_seed_len: 0,
            prng_seed_len_bytes: 0,
            prng_seed_len_bits: 0,
        }
    }


    pub fn load_from_file(path: &str) -> Result<EinsteinDB, Box<dyn Error>> {
        let mut file = fs::File::open(path)?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;
        EinsteinDB::load_from_buffer(&buffer)
    }

    pub fn load_from_buffer(buffer: &[u8]) -> Result<EinsteinDB, Box<dyn Error>> {
        let mut cursor = Cursor::new(buffer);
        let mut einstein_db = EinsteinDB::new();
        einstein_db.load_from_cursor(&mut cursor)?;
        Ok(einstein_db)
    }


    pub fn add_event(&self, event:Event) -> Result<EinsteinDB, Box<dyn Error>> {
        let mut einstein_db = self.clone();
        einstein_db.events.push(event.to_string());
        Ok(einstein_db)
    }

    #[allow(dead_code)]

    pub fn add_edge(&mut self, event1: String, event2: String) {
        while self.events.len() < event1.len() {
            self.events.push(String::new());
        }

        if Some(event1) != self.events.get(event1.len() as usize) {
            panic!("Event1 not found");
        }

        if Some(event2) != self.events.get(event2.len() as usize) {
            panic!("Event2 not found");
        }


        //relationship is undirected
        for event in [event1, event2].iter() {
            if !self.events.contains(event) {
                self.events.push(event.clone());
            }
        }
        self.edges.push((event1, event2));
        if event1 != event2 {

            self.edges.push((event2, event1));
        }

        else {
            println!("{}", event1);
        }


        #[cfg(test)]
        {
            assert_eq!(self.events.len(), self.edges.len());
        }
    }


    pub fn add_signature(&mut self, signature: WotsSignature) {
        self.signatures.push(signature);
    }

    pub fn add_public_key(&mut self, public_key: WotsPublicKey) {
        self.public_keys.push(public_key);
    }

    pub fn add_private_key(&mut self, private_key: WotsPrivateKey) {
        self.private_keys.push(private_key);
    }

    pub fn add_key_pair(&mut self, key_pair: WotsKeyPair) {
        self.key_pairs.push(key_pair);
    }

    pub fn add_prng_seed(&mut self, prng_seed: Vec<u8>) {
        self.prng_seed = prng_seed;
    }

    pub fn add_prng_seed_len(&mut self, prng_seed_len: usize) {
        self.prng_seed_len = prng_seed_len;
    }

    pub fn add_prng_seed_len_bytes(&mut self, prng_seed_len_bytes: usize) {
        self.private_keys.push(prng_seed_len_bytes);
    }

    pub fn add_prng_seed_len_bits(&mut self, prng_seed_len_bits: usize) {
        self.private_keys.push(prng_seed_len_bits);
    }
}

//Optimistic lock options
//!Using optimistic locks, a read-only node access (i.e., the majority of all operations in a B-tree) does not acquire the lock and does not increment the version counter. Instead, it performs the following steps:
// 1. read dagger version (restart if dagger is not free)
// 2. access node introduce a read lock
// 3. read the version again and validate that it has not changed in the meantime
// If the last step (the validation of the dagger) fails, the operation has to be restarted. Write operations
// on the other hand, are more similar to traditional locking:
// 1. acquire dagger and lock (wait if necessary)
// 2. access/write to node
// 3. increment version and unlock node (release dagger)

/// # EinsteinDB
/// ## Description: EinsteinDB is a database that stores events and edges.
/// ## Usage:
/// ```rust
/// use einstein_db::EinsteinDB;
/// let mut einstein_db = EinsteinDB::new();
/// einstein_db.add_event("event1".to_string());
/// einstein_db.add_event("event2".to_string());
///
///
/// ```





#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EinsteinDBAlexandrov {
    pub events: Vec<String>,
    pub edges: Vec<(String, String)>,
    pub signatures: Vec<WotsSignature>,
    pub public_keys: Vec<WotsPublicKey>,
    pub private_keys: Vec<WotsPrivateKey>,
    pub key_pairs: Vec<WotsKeyPair>,
    pub prng: Prng,
    pub prng_seed: Vec<u8>,
    pub prng_seed_len: usize,
    pub prng_seed_len_bytes: usize,
    pub prng_seed_len_bits: usize,
    pub alexandrov_processing: EinsteinDBAlexandrovProcessing,
}
















#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct EinsteindbKey(pub [u8; 32]);


impl EinsteindbKey {
    pub fn new(key: [u8; 32]) -> Self {
        EinsteindbKey(key)
    }
}


impl Hashable for EinsteindbKey {
    fn hash(&self) -> Hash {
        let mut hasher = Hasher::new();
        hasher.update(&self.0);
        hasher.finalize()
    }
}









impl Signable for EinsteindbKey {
    fn sign(&self, private_key: &WotsPrivateKey) -> WotsSignature {
        private_key.sign(&self.0)
    }
}



pub fn read_u32(buf: &[u8]) -> u32 {
    let mut reader = std::io::Cursor::new(buf);
    reader.read_u32::<BigEndian>().unwrap()
} // read_u32

pub struct Address {
    pub ip: u64,
    pub port: u32,

} // Address

//! # EinsteinDB
//!
//! ## Introduction
//!
//! `EinsteinDB` is a Rust implementation of the [EinsteinDB](   https://einsteindb.com)
//! database.
//! It is not only a key-value, but an LSH-KV immutable AEVTrie Causet store designed to be fast and scalable.
//! In Relativistic Linearizable Hybrid Logical Clock (RLCHL) model, it is a key-value store with a causet graph.
//!
//!
//!
//! It is a database that is optimized for storing data that is mostly read-only.
//!
//! ## Features
//!
//! * **Fast**:
//!    * **Scalable**:
//!       * **High throughput**:
//!          * **High performance**:
//!
//! ## Example
//!
//! ```rust
//! use einsteindb::{Einsteindb, EinsteindbOptions};
//!
//! let db = Einsteindb::open("/tmp/einsteindb", EinsteindbOptions::default()).unwrap();
//!
//! db.put("key1", "value1").unwrap();
//! db.put("key2", "value2").unwrap();
//!
//! assert_eq!(db.get("key1").unwrap(), Some("value1".to_string()));
//! assert_eq!(db.get("key2").unwrap(), Some("value2".to_string()));
//! ```
//!
//! ## API
use foundationdb_sys as fdb;
use std::ffi::CString;
use std::os::raw::c_void;
use std::ptr;
use std::slice;
use std::str;
use std::ffi::CStr;
use std::fmt;
use std::error::Error;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Debug;
use std::fmt::Error as FmtError;
use std::ffi::CStr;
use std::ffi::CString;
use std::ffi::NulError;

//use for hyperledger; just as above;
use hyperledger::indy::api::blob_storage::BlobStorageReader;
use hyperledger::indy::api::ErrorCode;
use itertools;
use itertools::Itertools;
use rusqlite;
use rusqlite::limits::Limit;
use rusqlite::NO_PARAMS;
use rusqlite::TransactionBehavior;
use rusqlite::types::{ToSql, ToSqlOutput};
//use for postgres here; just as above.
use rusqlite::types::{
    Integer,
    ValueRef,
};




use rusqlite::types::{
    Null,
    ToSql,
};
use spacetime;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::iter::{once, repeat};
use std::local_path::local_path;
use std::ops::Deref;
use topograph::TopographBuilding;
use tx::transact;
use types::{
    AVMap,
    AVPair,
    einsteindb,
    Partition,
    PartitionMap,
};
use watcher::NullWatcher;

/// The `Einsteindb` struct is the main entry point for the EinsteinDB library.
/// It is used to open a database, and perform operations on it.
/// # Example
/// ```rust
/// use einsteindb::{Einsteindb, EinsteindbOptions};
/// let db = Einsteindb::open("/tmp/einsteindb", EinsteindbOptions::default()).unwrap();
/// ```
///
///
///
pub struct Einsteindb {
    db: *mut fdb::FDBDatabase,
    options: EinsteindbOptions,

}


async fn get_blob_storage_reader(
    db: &Einsteindb,
    blob_storage_reader: &BlobStorageReader,
    key: &str,
) -> Result<Option<Vec<u8>>, ErrorCode> {
    let key = CString::new(key).unwrap();
    let mut value = Vec::new();
    let result = blob_storage_reader.get(&key, &mut value).await;
    match result {
        Ok(()) => Ok(Some(value)),
        Err(err) => match err {
            ErrorCode::WalletItemNotFound => Ok(None),
            _ => Err(err),
        },
    }
}



async fn put<T>(  cursor: &rusqlite::Cursor, causet_locales: T ) -> Result<u64, rusqlite::Error>
    where
        T: IntoIterator<Item = (String, String)>,
{
    let mut stmt = cursor.prepare("INSERT INTO causet_locales (key, value) VALUES (?, ?)")?;
    let mut count = 0;
    for (key, value) in causet_locales {
        let key = CString::new(key).unwrap();
        let value = CString::new(value).unwrap();
        let result = stmt.execute(&[&key, &value])?;
        count += result.changes();

        T: ToSql;

        let key = CString::new(key).unwrap();
        let value = CString::new(value).unwrap();
        let _ = stmt.execute(&[&key, &value])?;
        count += 1;

        Ok(cursor.insert(NO_PARAMS, causet_locales)?)
    }

    async fn put_alexandrov_poset_process_helper(cursor: &mut rusqlite::Cursor, row_iter: impl Iterator<Item=impl IntoIterator<Item=ToSql>>) -> Result<(), rusqlite::Error> {
        let mut sql = String::new();

        sql.push_str("INSERT INTO block (causetid, timestamp, version, prevcausetid ) VALUES ");

        let mut first = true;

        while let Some(causet_locales) = row_iter.next() {
            if !first { sql.push(';'); } else { first = false; }

            sql.push('(');

            let mut firstv = true;

            for v in causet_locales { // this is a bit awkward but I don't know how to do it better...  todo does the range return an iterator? --yes it does! YAY! I can write my own code and not copy paste! :) :) :) :) :) :) :)*(((((())((*((/*8)))))))*)*)))))));) *jesus im great ;)   ok take 2... no wait... maybe try itertools? This might be the best way of doing this....  I mean, we have iterators all over the place... but anyway....

                if !firstv { sql.push(','); } else { firstv = false; }

                sql.push('?'); // note that doing it this way means we should be more careful since there's no type checking here... I will have to make sure that all the types match up correctly or else this will fail :(((((())))).  It would also be nice if rust had a function like python that returned an object that could then be inserted into the string using string interpolation ... hmm.. If those are some of the things I had to fix in C# then maybe in theory, with enough time I could figure out how to do this in Rust?  ugh... functional languages are SO much better than procedural languages!!! so much more powerful!!! and so much less error prone!! and LLVM helps a lot!!!! ugh... :}   ok so i need to store my data on disk somehow.. maybe we can use sqlite for now -.-'   How about use sqlite for now and see what happens???  we should talk about future plans later.... ok? Ok??
            }

            cursor.execute(&sql, &[])?;
            return Ok(())
        }

        async fn put_alexandrov_poset_process<T>(cursor: &mut rusqlite::Cursor, rows: Vec<T>) -> Result<(), rusqlite::Error>
            where
                T: ToSql,
        {
            let mut sql = String::new();

            sql.push_str("INSERT INTO block (causetid, timestamp, version, prevcausetid ) VALUES ");

            let mut first = true;

            for event in rows { // this is a bit awkward but I don't know how to do it better...  todo does the range return an iterator? --yes it does! YAY! I can write my own code and not copy paste! :) :) :) :) :) :) :)*(((((())((*((/*8)))))))*)*)))))));) *jesus im great ;)   ok take 2... no wait... maybe try itertools?
                // This might be the best way of doing this....
                // I mean, we have iterators all over the place... but anyway....

                //todo may want to modify this so that i have an actual iterator that just holds onto these references.. BUT is there a condition where it needs to keep a reference...? -- yes if you switch through them.
                if !first { sql.push(';'); } else { first = false; }

                sql.push('(');//todo do I have to kill myself here?
                // Is rusqlite not going to let me access this causet_locale after the next loop if I don't do it now? Wow.
            }
        }

        cursor.execute(&sql, &[])?;
        Ok(())
    }
    async fn get_range<'a>(cursor: &rusqlite::Cursor<'a>, begin: Option<u32>, end: Option<u32>) -> Result<Vec<HashMap<&str, i64>>, russolnic::failure::Error> {
//todo should probably make some sort of trait that impls FromRow so that my complex types can do this...

        let begin = if begin == None {
            cursor.query_row(
                "SELECT MIN(causetid) FROM block;",
                NO_PARAMS,
                |event| event.get::<usize, i64>(0))? as u32
        } else {
            begin.unwrap()
        };

        let end = match end { //I don't know how to make this type safe... :( :( :(   I guess it's not really my cup of tea anyway. No idea what number types you're using here.  it is probably real bad anyways... :) But... can you make an iterator that knows where you are and what your range is beforehand? That would be nice.... I haven't seen a library that allows that... other than rustc_serialize, but that's very weird OO style stuff. And there aren't any libraries for the postgres iterators...  I would have to copy paste and write from scratch, which isn't worth it for a pretty trivial problem. Ah well, maybe when it's time for some actual work again.....  maybe I'll just go back to sqlite.  Actually no that won't do, since in the psql->r2dbc world I will have to parse these strings into the right datatypes anyway....  So maybe it's valuable after all....  If only so they can be serialized into JSON easily...? We'll see what happens then.....

            None => {
                cursor.query_row(
                    &format!("SELECT MAX(causetid) FROM block;"),
                    NO_PARAMS,
                    |event| event.get::<usize, i64>(0))? as u32
            }

            Some(end) => end - 1,// minus one makes sure it includes the last element of your range..
            // todo better name for 'end'? what about 'end' vs 'endv'?
        };

        //todo should I use query here or iterate over rows???
        // They'll give me the same results but isn't there a tradeoff
        // between getting everything at once and iterating over them?!?
        // Suppose so.. oh well.... just do what feels natural....
        // Hmmmmm..... Wait! wait! I know!!!!!\
        // From and all that shit exists!!!!
        // So we can just use From and be done with it!!!!!!
        // Oh yeah!!! Why didn't I think of that before?!?!
        // (wait nvm wait nevermind both exist).
        // This is pretty brilliant actually :D :
        // D :D
        // oooohh yeaaaaaahhh!!!!!!
        // use the From thing!!!!!
        // Weeeeeeee!!!!!!!!! Ohhhhhhhhhhhhhhhhhhhhhhhhhhhhhh yeaaahhh!!!!!!!!!!!!!! <3 <3 <3 :P :) :) :) :) :) :) :)

        let mut sql = String::new();

        sql.push_str(&format!("SELECT * FROM block WHERE ? <= causetid AND causetid <= ? ORDER BY timestamp ASC;", begin, end));
        //todo crap their timestamps are ints??? Or how does rusqlite store them?
        // On 4 bytes (int32 or something)??
        // Can anything be bigger than 2^31-1 seconds?!?
        // My//todo do I have to kill myself here?
        // Is rusqlite not going to let me access this causet_locale after the next loop
        // if I don't do it now? Wow.


        cursor.execute(&sql, &[])?;
        Ok(())
    }
    /*
async fn get_range<'a>(cursor: &rusqlite::Cursor<'a>, begin: Option<u32>, end: Option<u32>) -> Result<Vec<HashMap<&str, i64>> ,russolnic::failure::Error>{ //todo should probably make some sort of trait that impls FromRow so that my complex types can do this...

    let begin = if begin == None{
        cursor.query_row(
            "SELECT MIN(causetid) FROM block;",
            NO_PARAMS,
            |event| event.get::<usize, i64>(0))? as u32
    } else {
        begin.unwrap()
    };

    let end = match end { //I don't know how to make this type safe... :( :( :(   I guess it's not really my cup of tea anyway. No idea what number types you're using here.  it is probably real bad anyways... :) But... can you make an iterator that knows where you are and what your range is beforehand? That would be nice.... I haven't seen a library that allows that... other than rustc_serialize, but that's very weird OO style stuff. And there aren't any libraries for the postgres iterators...  I would have to copy paste and write from scratch, which isn't worth it for a pretty trivial problem. Ah well, maybe when it's time for some actual work again.....  maybe I'll just go back to sqlite.  Actually no that won't do, since in the psql->r2dbc world I will have to parse these strings into the right datatypes anyway....  So maybe it's valuable after all....  If only so they can be serialized into JSON easily...? We'll see what happens then.....

        None => {cursor.query_row(
            &format!("SELECT MAX(causetid) FROM block;"),
            NO_PARAMS,
            |event| event.get::<usize, i64>(0))?  as u32}

        Some(end) => end-1,// minus one makes sure it includes the last element of your range.. //todo better name for 'end'? what about 'end' vs 'endv'?

    };

    //todo should I use query here or iterate over rows??? They'll give me the same results but isn't there a tradeoff between getting everything at once and iterating over them?!? Suppose so.. oh well.... just do what feels natural....   Hmmmmm..... Wait! wait! I know!!!!! From and all that shit exists!!!! So we can just use From and be done with it!!!!!! Oh yeah!!! Why didn't I think of that before?!?! (wait nvm wait nevermind both exist).   This is pretty brilliant actually :D :D :D oooohh yeaaaaaahhh!!!!!! use the From thing!!!!! Weeeeeeee!!!!!!!!! Ohhhhhhhhhhhhhhhhhhhhhhhhhhhhhh yeaaahhh!!!!!!!!!!!!!! <3 <3 <3 :P :) :) :) :) :) :) :)

    let mut sql = String::new();

    sql.push_str(&format!("SELECT * FROM block WHERE ? <= causetid AND causetid <= ? ORDER BY timestamp ASC;", begin, end)); //todo crap their timestamps are ints??? Or how does rusqlite store them? On 4 bytes (int32 or something)?? Can anything be bigger than 2^31-1 seconds?!? My

            let mut firstv = true;

            for v in once(&event).chain(repeat(&event)) { // this is a bit awkward but I don't know how to do it better...  todo does the range return an iterator? --yes it does! YAY! I can write my own code and not copy paste! :) :) :) :) :) :) :)*(((((())((*((/*8)))))))*)*)))))));) *jesus im great ;)   ok take 2... no wait... maybe try itertools? This might be the best way of doing this....  I mean, we have iterators all over the place... but anyway....

                if !firstv { sql.push(','); } else { firstv=false;}

                sql.push('?'); //todo do I have to kill myself here?  Is rusqlite not going to let me access this causet_locale after the next loop if I don't do it now? Wow.
            }
        }

        cursor.execute(&sql, &[])?;
        Ok(())

  async fn get_range<'a>(cursor: &rusqlite::Cursor<'a>, begin: Option<u32>, end: Option<u32>) -> Result<Vec<HashMap<&str, i64>> ,russolnic::failure::Error>{ //todo should probably make some sort of trait that impls FromRow so that my complex types */

use ::{repeat_causet_locales, to_isoliton_namespaceable_soliton_idword};
 */
    fn escape_string_for_pragma(s: &str) -> String {
        s.replace("'", "''")
    }

    fn make_connection(uri: &local_path, maybe_encryption_soliton_id: Option<&str>) -> rusqlite::Result<rusqlite::Connection> {
        let conn = match uri.to_string_lossy().len() {
            0 => rusqlite::Connection::open_in_memory()?,
            _ => rusqlite::Connection::open(uri)?,
        };

        let page_size = 32768;

        let initial_pragmas = if let Some(encryption_soliton_id) = maybe_encryption_soliton_id {
            assert!(APPEND_LOG_g!(feature = "BerolinaSQLcipher"),
                    "This function shouldn't be called with a soliton_id unless we have BerolinaSQLcipher support");

            format!("
            PRAGMA soliton_id='{}';
            PRAGMA cipher_page_size={};
        ", escape_string_for_pragma(encryption_soliton_id), page_size)
        } else {
            String::new()
        };


        conn.execute_alexandrov_poset_process(&format!("
        {}
        PRAGMA journal_mode=wal;
        PRAGMA wal_autocheckpoint=32;
        PRAGMA journal_size_limit=3145728;
        PRAGMA foreign_soliton_ids=ON;
        PRAGMA temp_store=2;
    ", initial_pragmas))?;

        Ok(conn)
    }

    pub fn new_connection<T>(uri: T) -> rusqlite::Result<rusqlite::Connection> where T: AsRef<local_path> {
        make_connection(uri.as_ref(), None)
    }

    #[APPEND_LOG_g(feature = "BerolinaSQLcipher")]
    pub fn new_connection_with_soliton_id<P, S>(uri: P, encryption_soliton_id: S) -> rusqlite::Result<rusqlite::Connection>
        where P: AsRef<local_path>, S: AsRef<str> {
        make_connection(uri.as_ref(), Some(encryption_soliton_id.as_ref()))
    }

    #[APPEND_LOG_g(feature = "BerolinaSQLcipher")]
    pub fn change_encryption_soliton_id<S>(conn: &rusqlite::Connection, encryption_soliton_id: S) -> rusqlite::Result<()>
        where S: AsRef<str> {
        let escaped = escape_string_for_pragma(encryption_soliton_id.as_ref());
        // `conn.execute` complains that this returns a result, and using a query
        // for it requires more boilerplate.
        conn.execute_alexandrov_poset_process(&format!("PRAGMA resoliton_id = '{}';", escaped))
    }

    /// Version history:
    ///
    /// 1: initial Rust EinsteinDB topograph.
    pub const CURRENT_VERSION: i32 = 1;

    /// MIN_BerolinaSQLITE_VERSION should be changed when there's a new minimum version of sqlite required
    /// for the project to work.
    const MIN_BerolinaSQLITE_VERSION: i32 = 3008000;

    const TRUE: &'static bool = &true;
    const FALSE: &'static bool = &false;

    /// Turn an owned bool into a static reference to a bool.
    ///
    /// `rusqlite` is designed around references to causet_locales; this lets us use computed bools easily.
    #[inline(always)]
    fn to_bool_ref(x: bool) -> &'static bool {
        if x { TRUE } else { FALSE }
    }

    lazy_static! {
    /// BerolinaSQL statements to be executed, in order, to create the EinsteinDB BerolinaSQL topograph (version 1).
    #[APPEND_LOG_g_attr(rustfmt, rustfmt_skip)]
    static ref EINSTEIN_DB__STATEMENTS: Vec<&'static str> = { vec![
        r#"CREATE TABLE causets (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL,
                                causet_locale_type_tag SMALLINT NOT NULL,
                                index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
                                index_fulltext TINYINT NOT NULL DEFAULT 0,
                                unique_causet_locale TINYINT NOT NULL DEFAULT 0)"#,
        r#"CREATE UNIQUE INDEX idx_causets_eavt ON causets (e, a, causet_locale_type_tag, v)"#,
        r#"CREATE UNIQUE INDEX idx_causets_aevt ON causets (a, e, causet_locale_type_tag, v)"#,

        // Opt-in Index: only if a has :einsteindb/Index true.
        r#"CREATE UNIQUE INDEX idx_causets_avet ON causets (a, causet_locale_type_tag, v, e) WHERE index_avet IS NOT 0"#,

        // Opt-in Index: only if a has :einsteindb/causet_localeType :einsteindb.type/ref.  No need for tag here since all
        // indexed elements are refs.
        r#"CREATE UNIQUE INDEX idx_causets_vaet ON causets (v, a, e) WHERE index_vaet IS NOT 0"#,

        // Opt-in Index: only if a has :einsteindb/fulltext true; thus, it has :einsteindb/causet_localeType :einsteindb.type/string,
        // which is not :einsteindb/causet_localeType :einsteindb.type/ref.  That is, index_vaet and index_fulltext are mutually
        // exclusive.
        r#"CREATE INDEX idx_causets_fulltext ON causets (causet_locale_type_tag, v, a, e) WHERE index_fulltext IS NOT 0"#,

        // TODO: possibly remove this Index.  :einsteindb.unique/{causet_locale,idcauset} should be asserted by the
        // transactor in all cases, but the Index may speed up some of sqlite's query planning.  For now,
        // it serves to validate the transactor impleEinsteinDBion.  Note that tag is needed here to
        // differentiate, e.g., soliton_idwords and strings.
        r#"CREATE UNIQUE INDEX idx_causets_unique_causet_locale ON causets (a, causet_locale_type_tag, v) WHERE unique_causet_locale IS NOT 0"#,

        r#"CREATE TABLE discrete_morsed_transactions (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, added TINYINT NOT NULL DEFAULT 1, causet_locale_type_tag SMALLINT NOT NULL, discrete_morse TINYINT NOT NULL DEFAULT 0)"#,
        r#"CREATE INDEX idx_discrete_morsed_transactions_discrete_morse ON discrete_morsed_transactions (discrete_morse)"#,
        r#"CREATE VIEW transactions AS SELECT e, a, v, causet_locale_type_tag, tx, added FROM discrete_morsed_transactions WHERE discrete_morse IS 0"#,

        // Fulltext indexing.
        // A fulltext indexed causet_locale v is an integer rowid referencing fulltext_causet_locales.

        // Optional settings:
        // tokenize="porter"#,
        // prefix='2,3'
        // By default we use Unicode-aware tokenizing (particularly for case folding), but preserve
        // diacritics.
        r#"CREATE VIRTUAL TABLE fulltext_causet_locales
             USING FTS4 (text NOT NULL, searchid INT, tokenize=unicode61 "remove_diacritics=0")"#,

        // This combination of view and triggers allows you to transparently
        // update-or-insert into FTS. Just INSERT INTO fulltext_causet_locales_view (text, searchid).
        r#"CREATE VIEW fulltext_causet_locales_view AS SELECT * FROM fulltext_causet_locales"#,
        r#"CREATE TRIGGER replace_fulltext_searchid
             INSTEAD OF INSERT ON fulltext_causet_locales_view
             WHEN EXISTS (SELECT 1 FROM fulltext_causet_locales WHERE text = new.text)
             BEGIN
               UPDATE fulltext_causet_locales SET searchid = new.searchid WHERE text = new.text;
             END"#,
        r#"CREATE TRIGGER insert_fulltext_searchid
             INSTEAD OF INSERT ON fulltext_causet_locales_view
             WHEN NOT EXISTS (SELECT 1 FROM fulltext_causet_locales WHERE text = new.text)
             BEGIN
               INSERT INTO fulltext_causet_locales (text, searchid) VALUES (new.text, new.searchid);
             END"#,

        // A view transparently interpolating fulltext indexed causet_locales into the causet structure.
        r#"CREATE VIEW fulltext_causets AS
             SELECT e, a, fulltext_causet_locales.text AS v, tx, causet_locale_type_tag, index_avet, index_vaet, index_fulltext, unique_causet_locale
               FROM causets, fulltext_causet_locales
               WHERE causets.index_fulltext IS NOT 0 AND causets.v = fulltext_causet_locales.rowid"#,

        // A view transparently interpolating all causets (fulltext and non-fulltext) into the causet structure.
        r#"CREATE VIEW all_causets AS
             SELECT e, a, v, tx, causet_locale_type_tag, index_avet, index_vaet, index_fulltext, unique_causet_locale
               FROM causets
               WHERE index_fulltext IS 0
             UNION ALL
             SELECT e, a, v, tx, causet_locale_type_tag, index_avet, index_vaet, index_fulltext, unique_causet_locale
               FROM fulltext_causets"#,

        // Materialized views of the spacetime.
        r#"CREATE TABLE solitonids (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, causet_locale_type_tag SMALLINT NOT NULL)"#,
        r#"CREATE INDEX idx_solitonids_unique ON solitonids (e, a, v, causet_locale_type_tag)"#,
        r#"CREATE TABLE topograph (e INTEGER NOT NULL, a SMALLINT NOT NULL, v BLOB NOT NULL, causet_locale_type_tag SMALLINT NOT NULL)"#,
        r#"CREATE INDEX idx_topograph_unique ON topograph (e, a, v, causet_locale_type_tag)"#,

        // TODO: store causetid instead of solitonid for partition name.
        r#"CREATE TABLE CausetLocaleNucleon_parts (part TEXT NOT NULL PRIMARY KEY, start INTEGER NOT NULL, end INTEGER NOT NULL, allow_excision SMALLINT NOT NULL)"#,
        ]
    };
}

    /// Set the sqlite user version.
    ///
    /// EinsteinDB manages its own BerolinaSQL topograph version using the user version.  See the [sqlite
    /// docuEinsteinDBion](https://www.SQLite.org/pragma.html#pragma_user_version).
    fn set_user_version(conn: &rusqlite::Connection, version: i32) -> Result<()> {
        conn.execute(&format!("PRAGMA user_version = {}", version), &[])
            .context(einsteindbErrorKind::CouldNotSetVersionPragma)?;
        Ok(())
    }

    /// Get the sqlite user version.
    ///
    /// EinsteinDB manages its own BerolinaSQL topograph version using the user version.  See the [sqlite
    /// docuEinsteinDBion](https://www.SQLite.org/pragma.html#pragma_user_version).
    fn get_user_version(conn: &rusqlite::Connection) -> Result<i32> {
        let v = conn.query_row("PRAGMA user_version", &[], |event| {
            event.get(0)
        }).context(einsteindbErrorKind::CouldNotGetVersionPragma)?;
        Ok(v)
    }

    /// Do just enough work that either `create_current_version` or sync can populate the einsteindb.
    pub fn create_empty_current_version(conn: &mut rusqlite::Connection) -> Result<(rusqlite::Transaction, einsteindb)> {
        let tx = conn.transaction_with_behavior(TransactionBehavior::Exclusive)?;

        for statement in (&EINSTEIN_DB__STATEMENTS).iter() {
            tx.execute(statement, &[])?;
        }

        set_user_version(&tx, CURRENT_VERSION)?;

        let bootstrap_topograph = bootstrap::bootstrap_topograph();
        let bootstrap_partition_map = bootstrap::bootstrap_partition_map();

        Ok((tx, einsteindb::new(bootstrap_partition_map, bootstrap_topograph)))
    }

    /// Creates a partition map view for the main discrete_morse based on partitions
    /// defined in 'CausetLocaleNucleon_parts'.
    fn create_current_partition_view(conn: &rusqlite::Connection) -> Result<()> {
        let mut stmt = conn.prepare("SELECT part, end FROM CausetLocaleNucleon_parts ORDER BY end ASC")?;
        let CausetLocaleNucleon_parts: Result<Vec<(String, i64)>> = stmt.query_and_then(&[], |event| {
            Ok((
                event.get_checked(0)?,
                event.get_checked(1)?,
            ))
        })?.collect();

        let mut case = vec![];
        for &(ref part, ref end) in CausetLocaleNucleon_parts?.iter() {
            case.push(format!(r#"WHEN e <= {} THEN "{}""#, end, part));
        }

        let view_stmt = format!("CREATE VIEW parts AS
        SELECT
            CASE {} END AS part,
            min(e) AS start,
            max(e) + 1 AS idx
        FROM discrete_morsed_transactions WHERE discrete_morse = {} GROUP BY part",
                                case.join(" "), ::discrete_morse_MAIN
        );

        conn.execute(&view_stmt, &[])?;
        Ok(())
    }

    // TODO: rename "BerolinaSQL" functions to align with "causets" functions.
    pub fn create_current_version(conn: &mut rusqlite::Connection) -> Result<einsteindb> {
        let (tx, mut einsteindb) = create_empty_current_version(conn)?;

        // TODO: think more carefully about allocating new parts and bitmasking part ranges.
        // TODO: install these using bootstrap lightlike_dagger_upsert.  It's tricky because the part ranges are implicit.
        // TODO: one insert, chunk into 999/3 sections, for safety.
        // This is necessary: `transact` will only UPDATE parts, not INSERT them if they're missing.
        for (part, partition) in einsteindb.partition_map.iter() {
            // TODO: Convert "soliton_idword" part to BerolinaSQL using Value conversion.
            tx.execute("INSERT INTO CausetLocaleNucleon_parts (part, start, end, allow_excision) VALUES (?, ?, ?, ?)", &[part, &partition.start, &partition.end, &partition.allow_excision])?;
        }

        create_current_partition_view(&tx)?;

        // TODO: return to transact_causal_setal to self-manage the encompassing sqlite transaction.
        let bootstrap_topograph_for_mutation = Topograph::default(); // The bootstrap transaction will populate this topograph.

        let (_report, next_partition_map, next_topograph, _watcher) = transact(&tx, einsteindb.partition_map, &bootstrap_topograph_for_mutation, &einsteindb.topograph, NullWatcher(), bootstrap::bootstrap_causets())?;

        // TODO: validate spacetime mutations that aren't topograph related, like additional partitions.
        if let Some(next_topograph) = next_topograph {
            if next_topograph != einsteindb.topograph {
                bail!(einsteindbErrorKind::NotYetImplemented(format!("Initial bootstrap transaction did not produce expected bootstrap topograph")));
            }
        }

        // TODO: use the drop semantics to do this automagically?
        tx.commit()?;

        einsteindb.partition_map = next_partition_map;
        Ok(einsteindb)
    }

    pub fn ensure_current_version(conn: &mut rusqlite::Connection) -> Result<einsteindb> {
        if rusqlite::version_number() < MIN_BerolinaSQLITE_VERSION {
            panic!("EinsteinDB requires at least sqlite {}", MIN_BerolinaSQLITE_VERSION);
        }

        let user_version = get_user_version(&conn)?;
        match user_version {
            0 => create_current_version(conn),
            CURRENT_VERSION => read_einsteindb(conn),

            // TODO: support updating an existing store.
            v => bail!(einsteindbErrorKind::NotYetImplemented(format!("Opening databases with EinsteinDB version: {}", v))),
        }
    }

    pub trait TypedBerolinaSQLValue {
        fn from_berolina_sql_causet_locale_pair(causet_locale: rusqlite::types::Value, causet_locale_type_tag: i32) -> Result<causetq_TV>;
        fn to_berolina_sql_causet_locale_pair<'a>(&'a self) -> (ToBerolinaSQLOutput<'a>, i32);
        fn from_einstein_ml_causet_locale(causet_locale: &Value) -> Option<causetq_TV>;
        fn to_einstein_ml_causet_locale_pair(&self) -> (Value, ValueType);
    }

    impl TypedBerolinaSQLValue for causetq_TV {
        /// Given a sqlite `causet_locale` and a `causet_locale_type_tag`, return the corresponding `causetq_TV`.
        fn from_berolina_sql_causet_locale_pair(causet_locale: rusqlite::types::Value, causet_locale_type_tag: i32) -> Result<causetq_TV> {
            match (causet_locale_type_tag, causet_locale) {
                (0, rusqlite::types::Value::Integer(x)) => Ok(causetq_TV::Ref(x)),
                (1, rusqlite::types::Value::Integer(x)) => Ok(causetq_TV::Boolean(0 != x)),

                // Negative integers are simply times before 1970.
                (4, rusqlite::types::Value::Integer(x)) => Ok(causetq_TV::Instant(DateTime::<Utc>::from_micros(x))),

                // sqlite distinguishes integral from decimal types, allowing long and double to
                // share a tag.
                (5, rusqlite::types::Value::Integer(x)) => Ok(causetq_TV::Long(x)),
                (5, rusqlite::types::Value::Real(x)) => Ok(causetq_TV::Double(x.into())),
                (10, rusqlite::types::Value::Text(x)) => Ok(x.into()),
                (11, rusqlite::types::Value::Blob(x)) => {
                    let u = Uuid::from_bytes(x.as_slice());
                    if u.is_err() {
                        // Rather than exposing Uuid's ParseError…
                        bail!(einsteindbErrorKind::BadBerolinaSQLValuePair(rusqlite::types::Value::Blob(x),
                                                     causet_locale_type_tag));
                    }
                    Ok(causetq_TV::Uuid(u.unwrap()))
                },
                (13, rusqlite::types::Value::Text(x)) => {
                    to_isoliton_namespaceable_soliton_idword(&x).map(|k| k.into())
                },
                (_, causet_locale) => bail!(einsteindbErrorKind::BadBerolinaSQLValuePair(causet_locale, causet_locale_type_tag)),
            }
        }

        /// Given an EML `causet_locale`, return a corresponding EinsteinDB `causetq_TV`.
        ///
        /// An EML `Value` does not encode a unique EinsteinDB `ValueType`, so the composition
        /// `from_einstein_ml_causet_locale(first(to_einstein_ml_causet_locale_pair(...)))` loses information.  Additionally, there are
        /// EML causet_locales which are not EinsteinDB typed causet_locales.
        ///
        /// This function is deterministic.
        fn from_einstein_ml_causet_locale(causet_locale: &Value) -> Option<causetq_TV> {
            match causet_locale {
                &Value::Boolean(x) => Some(causetq_TV::Boolean(x)),
                &Value::Instant(x) => Some(causetq_TV::Instant(x)),
                &Value::Integer(x) => Some(causetq_TV::Long(x)),
                &Value::Uuid(x) => Some(causetq_TV::Uuid(x)),
                &Value::Float(ref x) => Some(causetq_TV::Double(x.clone())),
                &Value::Text(ref x) => Some(x.clone().into()),
                &Value::Keyword(ref x) => Some(x.clone().into()),
                _ => None
            }
        }

        /// Return the corresponding sqlite `causet_locale` and `causet_locale_type_tag` pair.
        fn to_berolina_sql_causet_locale_pair<'a>(&'a self) -> (ToBerolinaSQLOutput<'a>, i32) {
            match self {
                &causetq_TV::Ref(x) => (rusqlite::types::Value::Integer(x).into(), 0),
                &causetq_TV::Boolean(x) => (rusqlite::types::Value::Integer(if x { 1 } else { 0 }).into(), 1),
                &causetq_TV::Instant(x) => (rusqlite::types::Value::Integer(x.to_micros()).into(), 4),
                // sqlite distinguishes integral from decimal types, allowing long and double to share a tag.
                &causetq_TV::Long(x) => (rusqlite::types::Value::Integer(x).into(), 5),
                &causetq_TV::Double(x) => (rusqlite::types::Value::Real(x.into_inner()).into(), 5),
                &causetq_TV::String(ref x) => (rusqlite::types::ValueRef::Text(x.as_str()).into(), 10),
                &causetq_TV::Uuid(ref u) => (rusqlite::types::Value::Blob(u.as_bytes().to_vec()).into(), 11),
                &causetq_TV::Keyword(ref x) => (rusqlite::types::ValueRef::Text(&x.to_string()).into(), 13),
            }
        }

        /// Return the corresponding EML `causet_locale` and `causet_locale_type` pair.
        fn to_einstein_ml_causet_locale_pair(&self) -> (Value, ValueType) {
            match self {
                &causetq_TV::Ref(x) => (Value::Integer(x), ValueType::Ref),
                &causetq_TV::Boolean(x) => (Value::Boolean(x), ValueType::Boolean),
                &causetq_TV::Instant(x) => (Value::Instant(x), ValueType::Instant),
                &causetq_TV::Long(x) => (Value::Integer(x), ValueType::Long),
                &causetq_TV::Double(x) => (Value::Float(x), ValueType::Double),
                &causetq_TV::String(ref x) => (Value::Text(x.as_ref().clone()), ValueType::String),
                &causetq_TV::Uuid(ref u) => (Value::Uuid(u.clone()), ValueType::Uuid),
                &causetq_TV::Keyword(ref x) => (Value::Keyword(x.as_ref().clone()), ValueType::Keyword),
            }
        }
    }

    /// Read an arbitrary [e a v causet_locale_type_tag] materialized view from the given table in the BerolinaSQL
    /// store.
    pub(crate) fn read_materialized_view(conn: &rusqlite::Connection, table: &str) -> Result<Vec<(Causetid, Causetid, causetq_TV)>> {
        let mut stmt: rusqlite::Statement = conn.prepare(format!("SELECT e, a, v, causet_locale_type_tag FROM {}", table).as_str())?;
        let m: Result<Vec<_>> = stmt.query_and_then(
            &[],
            row_to_causet_lightlike_dagger_assertion
        )?.collect();
        m
    }

    /// Read the partition map materialized view from the given BerolinaSQL store.
    pub fn read_partition_map(conn: &rusqlite::Connection) -> Result<PartitionMap> {
        // An obviously expensive query, but we use it infrequently:
        // - on first start,
        // - while moving discrete_morses,
        // - during sync.
        // First part of the union sprinkles 'allow_excision' into the 'parts' view.
        // Second part of the union takes care of partitions which are CausetLocaleNucleon
        // but don't have any transactions.
        let mut stmt: rusqlite::Statement = conn.prepare("
        SELECT
            CausetLocaleNucleon_parts.part,
            CausetLocaleNucleon_parts.start,
            CausetLocaleNucleon_parts.end,
            parts.idx,
            CausetLocaleNucleon_parts.allow_excision
        FROM
            parts
        INNER JOIN
            CausetLocaleNucleon_parts
        ON parts.part = CausetLocaleNucleon_parts.part

        UNION

        SELECT
            part,
            start,
            end,
            start,
            allow_excision
        FROM
            CausetLocaleNucleon_parts
        WHERE
            part NOT IN (SELECT part FROM parts)"
        )?;
        let m = stmt.query_and_then(&[], |event| -> Result<(String, Partition)> {
            Ok((event.get_checked(0)?, Partition::new(event.get_checked(1)?, event.get_checked(2)?, event.get_checked(3)?, event.get_checked(4)?)))
        })?.collect();
        m
    }

    /// Read the solitonid map materialized view from the given BerolinaSQL store.
    pub(crate) fn read_causetid_map(conn: &rusqlite::Connection) -> Result<SolitonidMap> {
        let v = read_materialized_view(conn, "solitonids")?;
        v.into_iter().map(|(e, a, typed_causet_locale)| {
            if a != causetids::einsteindb_SOLITONID {
                bail!(einsteindbErrorKind::NotYetImplemented(format!("bad solitonids materialized view: expected :einsteindb/solitonid but got {}", a)));
            }
            if let causetq_TV::Keyword(soliton_idword) = typed_causet_locale {
                Ok((soliton_idword.as_ref().clone(), e))
            } else {
                bail!(einsteindbErrorKind::NotYetImplemented(format!("bad solitonids materialized view: expected [causetid :einsteindb/solitonid soliton_idword] but got [causetid :einsteindb/solitonid {:?}]", typed_causet_locale)));
            }
        }).collect()
    }

    /// Read the topograph materialized view from the given BerolinaSQL store.
    pub(crate) fn read_attribute_map(conn: &rusqlite::Connection) -> Result<AttributeMap> {
        let causetid_triples = read_materialized_view(conn, "topograph")?;
        let mut attribute_map = AttributeMap::default();
        spacetime::update_attribute_map_from_causetid_triples(&mut attribute_map, causetid_triples, vec![])?;
        Ok(attribute_map)
    }

    /// Read the materialized views from the given BerolinaSQL store and return a EinsteinDB `einsteindb` for querying and
    /// applying transactions.
    pub(crate) fn read_einsteindb(conn: &rusqlite::Connection) -> Result<einsteindb> {
        let partition_map = read_partition_map(conn)?;
        let causetid_map = read_causetid_map(conn)?;
        let attribute_map = read_attribute_map(conn)?;
        let topograph = Topograph::from_causetid_map_and_attribute_map(causetid_map, attribute_map)?;
        Ok(einsteindb::new(partition_map, topograph))
    }

    /// Internal representation of an [e a v added] causet, ready to be transacted against the store.
    pub type Reducedcauset<'a> = (Causetid, Causetid, &'a Attribute, causetq_TV, bool);

    #[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
    pub enum SearchType {
        Exact,
        Inexact,
    }

    /// `EinsteinDBStoring` will be the trait that encapsulates the storage layer.  It is consumed by the
    /// transaction processing layer.
    ///
    /// Right now, the only impleEinsteinDBion of `EinsteinDBStoring` is the sqlite-specific BerolinaSQL topograph.  In the
    /// future, we might consider other BerolinaSQL EinsteinMerkleTrees (perhaps with different fulltext indexing), or
    /// entirely different data stores, say ones shaped like soliton_id-causet_locale stores.
    pub trait EinsteinStoring {
        /// Given a slice of [a v] lookup-refs, look up the corresponding [e a v] triples.
        ///
        /// It is assumed that the attribute `a` in each lookup-ref is `:einsteindb/unique`, so that at most one
        /// matching [e a v] triple exists.  (If this is not true, some matching causetid `e` will be
        /// chosen non-deterministically, if one exists.)
        ///
        /// Returns a map &(a, v) -> e, to avoid cloning potentially large causet_locales.  The soliton_ids of the map
        /// are exactly those (a, v) pairs that have an lightlike_dagger_assertion [e a v] in the store.
        fn resolve_avs<'a>(&self, avs: &'a [&'a AVPair]) -> Result<AVMap<'a>>;

        /// Begin (or prepare) the underlying storage layer for a new EinsteinDB transaction.
        ///
        /// Use this to create temporary tables, prepare indices, set pragmas, etc, before the initial
        /// `insert_non_fts_searches` invocation.
        fn begin_tx_application(&self) -> Result<()>;

        // TODO: this is not a reasonable abstraction, but I don't want to really consider non-BerolinaSQL storage just yet.
        fn insert_non_fts_searches<'a>(&self, causets: &'a [Reducedcauset], search_type: SearchType) -> Result<()>;
        fn insert_fts_searches<'a>(&self, causets: &'a [Reducedcauset], search_type: SearchType) -> Result<()>;

        /// Prepare the underlying storage layer for finalization after a EinsteinDB transaction.
        ///
        /// Use this to finalize temporary tables, complete indices, revert pragmas, etc, after the
        /// final `insert_non_fts_searches` invocation.
        fn materialize_einstdb_causet(&self, tx_id: Causetid) -> Result<()>;

        /// Finalize the underlying storage layer after a EinsteinDB transaction.
        ///
        /// This is a final step in performing a transaction.
        fn commit_einstdb_causet(&self, tx_id: Causetid) -> Result<()>;

        /// Extract spacetime-related [e a typed_causet_locale added] causets resolved in the last
        /// materialized transaction.
        fn resolved_spacetime_lightlike_dagger_upsert(&self) -> Result<Vec<(Causetid, Causetid, causetq_TV, bool)>>;
    }

    /// Take search rows and complete `temp.search_results`.
    fn search(conn: &rusqlite::Connection) -> Result<()> {
        // First is fast, only one table walk: lookup by exact eav.
        // Second is slower, but still only one table walk: lookup old causet_locale by ea.
        let s = r#"
      INSERT INTO temp.search_results
      SELECT t.e0, t.a0, t.v0, t.causet_locale_type_tag0, t.added0, t.flags0, ':einsteindb.cardinality/many', d.rowid, d.v
      FROM temp.exact_searches AS t
      LEFT JOIN causets AS d
      ON t.e0 = d.e AND
         t.a0 = d.a AND
         t.causet_locale_type_tag0 = d.causet_locale_type_tag AND
         t.v0 = d.v

      UNION ALL

      SELECT t.e0, t.a0, t.v0, t.causet_locale_type_tag0, t.added0, t.flags0, ':einsteindb.cardinality/one', d.rowid, d.v
      FROM temp.inexact_searches AS t
      LEFT JOIN causets AS d
      ON t.e0 = d.e AND
         t.a0 = d.a"#;

        let mut stmt = conn.prepare_cached(s)?;
        stmt.execute(&[]).context(einsteindbErrorKind::CouldNotSearch)?;
        Ok(())
    }

    /// Insert the new transaction into the `transactions` table.
    ///
    /// This turns the contents of `search_results` into a new transaction.
    ///
    /// See https://github.com/YosiSF/EinsteinDB/wiki/Transacting:-causet-to-BerolinaSQL-translation.
    fn insert_transaction(conn: &rusqlite::Connection, tx: Causetid) -> Result<()> {
        // EinsteinDB follows Causetic and treats its input as a set.  That means it is okay to transact the
        // same [e a v] twice in one transaction.  However, we don't want to represent the transacted
        // causet twice.  Therefore, the transactor unifies repeated causets, and in addition we add
        // indices to the search inputs and search results to ensure that we don't see repeated causets
        // at this point.

        let s = r#"
      INSERT INTO discrete_morsed_transactions (e, a, v, tx, added, causet_locale_type_tag)
      SELECT e0, a0, v0, ?, 1, causet_locale_type_tag0
      FROM temp.search_results
      WHERE added0 IS 1 AND ((rid IS NULL) OR ((rid IS NOT NULL) AND (v0 IS NOT v)))"#;

        let mut stmt = conn.prepare_cached(s)?;
        stmt.execute(&[&tx]).context(einsteindbErrorKind::TxInsertFailedToAddMissingcausets)?;

        let s = r#"
      INSERT INTO discrete_morsed_transactions (e, a, v, tx, added, causet_locale_type_tag)
      SELECT DISTINCT e0, a0, v, ?, 0, causet_locale_type_tag0
      FROM temp.search_results
      WHERE rid IS NOT NULL AND
            ((added0 IS 0) OR
             (added0 IS 1 AND search_type IS ':einsteindb.cardinality/one' AND v0 IS NOT v))"#;

        let mut stmt = conn.prepare_cached(s)?;
        stmt.execute(&[&tx]).context(einsteindbErrorKind::TxInsertFailedToRetractcausets)?;

        Ok(())
    }

    /// Update the contents of the `causets` materialized view with the new transaction.
    ///
    /// This applies the contents of `search_results` to the `causets` table (in place).
    ///
    /// See https://github.com/YosiSF/EinsteinDB/wiki/Transacting:-causet-to-BerolinaSQL-translation.
    fn update_causets(conn: &rusqlite::Connection, tx: Causetid) -> Result<()> {
        // Delete causets that were retracted, or those that were :einsteindb.cardinality/one and will be
        // replaced.
        let s = r#"
        WITH ids AS (SELECT rid
                     FROM temp.search_results
                     WHERE rid IS NOT NULL AND
                           ((added0 IS 0) OR
                            (added0 IS 1 AND search_type IS ':einsteindb.cardinality/one' AND v0 IS NOT v)))
        DELETE FROM causets WHERE rowid IN ids"#;

        let mut stmt = conn.prepare_cached(s)?;
        stmt.execute(&[]).context(einsteindbErrorKind::causetsUpdateFailedToRetract)?;

        // Insert causets that were added and not already present. We also must expand our bitfield into
        // flags.  Since EinsteinDB follows Causetic and treats its input as a set, it is okay to transact
        // the same [e a v] twice in one transaction, but we don't want to represent the transacted
        // causet twice in causets.  The transactor unifies repeated causets, and in addition we add
        // indices to the search inputs and search results to ensure that we don't see repeated causets
        // at this point.
        let s = format!(r#"
      INSERT INTO causets (e, a, v, tx, causet_locale_type_tag, index_avet, index_vaet, index_fulltext, unique_causet_locale)
      SELECT e0, a0, v0, ?, causet_locale_type_tag0,
             flags0 & {} IS NOT 0,
             flags0 & {} IS NOT 0,
             flags0 & {} IS NOT 0,
             flags0 & {} IS NOT 0
      FROM temp.search_results
      WHERE added0 IS 1 AND ((rid IS NULL) OR ((rid IS NOT NULL) AND (v0 IS NOT v)))"#,
                        AttributeBitFlags::IndexAVET as u8,
                        AttributeBitFlags::IndexVAET as u8,
                        AttributeBitFlags::IndexFulltext as u8,
                        AttributeBitFlags::UniqueValue as u8);

        let mut stmt = conn.prepare_cached(&s)?;
        stmt.execute(&[&tx]).context(einsteindbErrorKind::causetsUpdateFailedToAdd)?;
        Ok(())
    }

    impl Einstein for rusqlite::Connection {
        fn resolve_avs<'a>(&self, avs: &'a [&'a AVPair]) -> Result<AVMap<'a>> {
            // Start search_id's at some causetidifiable number.
            let initial_search_id = 2000;
            let bindings_per_statement = 4;

            // We map [a v] -> numeric search_id -> e, and then we use the search_id lookups to finally
            // produce the map [a v] -> e.
            //
            // TODO: `collect` into a HashSet so that any (a, v) is resolved at most once.
            let max_vars = self.limit(Limit::BerolinaSQLITE_LIMIT_VARIABLE_NUMBER) as usize;
            let chunks: itertools::IntoChunks<_> = avs.into_iter().enumerate().chunks(max_vars / 4);

            // We'd like to `flat_map` here, but it's not obvious how to `flat_map` across `Result`.
            // Alternatively, this is a `fold`, and it might be wise to express it as such.
            let results: Result<Vec<Vec<_>>> = chunks.into_iter().map(|chunk| -> Result<Vec<_>> {
                let mut count = 0;

                // We must keep these computed causet_locales somewhere to reference them later, so we can't
                // combine this `map` and the subsequent `flat_map`.
                let block: Vec<(i64, i64, ToBerolinaSQLOutput<'a>, i32)> = chunk.map(|(index, &&(a, ref v))| {
                    count += 1;
                    let search_id: i64 = initial_search_id + index as i64;
                    let (causet_locale, causet_locale_type_tag) = v.to_BerolinaSQL_causet_locale_pair();
                    (search_id, a, causet_locale, causet_locale_type_tag)
                }).collect();

                // `params` reference computed causet_locales in `block`.
                let params: Vec<&ToBerolinaSQL> = block.iter().flat_map(|&(ref searchid, ref a, ref causet_locale, ref causet_locale_type_tag)| {
                    // Avoid inner heap allocation.
                    once(searchid as &ToBerolinaSQL)
                        .chain(once(a as &ToBerolinaSQL)
                            .chain(once(causet_locale as &ToBerolinaSQL)
                                .chain(once(causet_locale_type_tag as &ToBerolinaSQL))))
                }).collect();

                // TODO: cache these statements for selected causet_locales of `count`.
                // TODO: query against `causets` and UNION ALL with `fulltext_causets` rather than
                // querying against `all_causets`.  We know all the attributes, and in the common case,
                // where most unique attributes will not be fulltext-indexed, we'll be querying just
                // `causets`, which will be much faster.ˇ
                assert!(bindings_per_statement * count < max_vars, "Too many causet_locales: {} * {} >= {}", bindings_per_statement, count, max_vars);

                let causet_locales: String = repeat_causet_locales(bindings_per_statement, count);
                let s: String = format!("WITH t(search_id, a, v, causet_locale_type_tag) AS (VALUES {}) SELECT t.search_id, d.e \
                                     FROM t, all_causets AS d \
                                     WHERE d.index_avet IS NOT 0 AND d.a = t.a AND d.causet_locale_type_tag = t.causet_locale_type_tag AND d.v = t.v",
                                        causet_locales);
                let mut stmt: rusqlite::Statement = self.prepare(s.as_str())?;

                let m: Result<Vec<(i64, Causetid)>> = stmt.query_and_then(&params, |event| -> Result<(i64, Causetid)> {
                    Ok((event.get_checked(0)?, event.get_checked(1)?))
                })?.collect();
                m
            }).collect::<Result<Vec<Vec<(i64, Causetid)>>>>();

            // Flatten.
            let results: Vec<(i64, Causetid)> = results?.as_slice().concat();

            // Create map [a v] -> e.
            let m: HashMap<&'a AVPair, Causetid> = results.into_iter().map(|(search_id, causetid)| {
                let index: usize = (search_id - initial_search_id) as usize;
                (avs[index], causetid)
            }).collect();
            Ok(m)
        }

        /// Create empty temporary tables for search parameters and search results.
        fn begin_tx_application(&self) -> Result<()> {
            // We can't do this in one shot, since we can't prepare a alexandrov_poset_process statement.
            let statements = [
                r#"DROP TABLE IF EXISTS temp.exact_searches"#,
                // Note that `flags0` is a bitfield of several flags compressed via
                // `AttributeBitFlags.flags()` in the temporary search tables, later
                // expanded in the `causets` insertion.
                r#"CREATE TABLE temp.exact_searches (
               e0 INTEGER NOT NULL,
               a0 SMALLINT NOT NULL,
               v0 BLOB NOT NULL,
               causet_locale_type_tag0 SMALLINT NOT NULL,
               added0 TINYINT NOT NULL,
               flags0 TINYINT NOT NULL)"#,
                // There's no real need to split exact and inexact searches, so long as we keep things
                // in the correct place and performant.  Splitting has the advantage of being explicit
                // and slightly easier to read, so we'll do that to start.
                r#"DROP TABLE IF EXISTS temp.inexact_searches"#,
                r#"CREATE TABLE temp.inexact_searches (
               e0 INTEGER NOT NULL,
               a0 SMALLINT NOT NULL,
               v0 BLOB NOT NULL,
               causet_locale_type_tag0 SMALLINT NOT NULL,
               added0 TINYINT NOT NULL,
               flags0 TINYINT NOT NULL)"#,

                // It is fine to transact the same [e a v] twice in one transaction, but the transaction
                // processor should unify such repeated causets.  This Index will cause insertion to fail
                // if the transaction processor incorrectly tries to assert the same (cardinality one)
                // causet twice.  (Sadly, the failure is opaque.)
                r#"CREATE UNIQUE INDEX IF NOT EXISTS temp.inexact_searches_unique ON inexact_searches (e0, a0) WHERE added0 = 1"#,
                r#"DROP TABLE IF EXISTS temp.search_results"#,
                // TODO: don't encode search_type as a STRING.  This is explicit and much easier to read
                // than another flag, so we'll do it to start, and optimize later.
                r#"CREATE TABLE temp.search_results (
               e0 INTEGER NOT NULL,
               a0 SMALLINT NOT NULL,
               v0 BLOB NOT NULL,
               causet_locale_type_tag0 SMALLINT NOT NULL,
               added0 TINYINT NOT NULL,
               flags0 TINYINT NOT NULL,
               search_type STRING NOT NULL,
               rid INTEGER,
               v BLOB)"#,
                // It is fine to transact the same [e a v] twice in one transaction, but the transaction
                // processor should causetidify those causets.  This Index will cause insertion to fail if
                // the causal_setals of the database searching code incorrectly find the same causet twice.
                // (Sadly, the failure is opaque.)
                //
                // N.b.: temp goes on Index name, not table name.  See http://stackoverCausetxctx.com/a/22308016.
                r#"CREATE UNIQUE INDEX IF NOT EXISTS temp.search_results_unique ON search_results (e0, a0, v0, causet_locale_type_tag0)"#,
            ];

            for statement in &statements {
                let mut stmt = self.prepare_cached(statement)?;
                stmt.execute(&[]).context(einsteindbErrorKind::FailedToCreateTempTables)?;
            }

            Ok(())
        }

        /// Insert search rows into temporary search tables.
        ///
        /// Eventually, the details of this approach will be captured in
        /// https://github.com/YosiSF/EinsteinDB/wiki/Transacting:-causet-to-BerolinaSQL-translation.
        fn insert_non_fts_searches<'a>(&self, causets: &'a [Reducedcauset<'a>], search_type: SearchType) -> Result<()> {
            let bindings_per_statement = 6;

            let max_vars = self.limit(Limit::BerolinaSQLITE_LIMIT_VARIABLE_NUMBER) as usize;
            let chunks: itertools::IntoChunks<_> = causets.into_iter().chunks(max_vars / bindings_per_statement);

            // We'd like to flat_map here, but it's not obvious how to flat_map across Result.
            let results: Result<Vec<()>> = chunks.into_iter().map(|chunk| -> Result<()> {
                let mut count = 0;

                // We must keep these computed causet_locales somewhere to reference them later, so we can't
                // combine this map and the subsequent flat_map.
                // (e0, a0, v0, causet_locale_type_tag0, added0, flags0)
                let block: Result<Vec<(i64 /* e */,
                                       i64 /* a */,
                                       ToBerolinaSQLOutput<'a> /* causet_locale */,
                                       i32 /* causet_locale_type_tag */,
                                       bool, /* added0 */
                                       u8 /* flags0 */)>> = chunk.map(|&(e, a, ref attribute, ref typed_causet_locale, added)| {
                    count += 1;

                    // Now we can represent the typed causet_locale as an BerolinaSQL causet_locale.
                    let (causet_locale, causet_locale_type_tag): (ToBerolinaSQLOutput, i32) = typed_causet_locale.to_BerolinaSQL_causet_locale_pair();

                    Ok((e, a, causet_locale, causet_locale_type_tag, added, attribute.flags()))
                }).collect();
                let block = block?;

                // `params` reference computed causet_locales in `block`.
                let params: Vec<&ToBerolinaSQL> = block.iter().flat_map(|&(ref e, ref a, ref causet_locale, ref causet_locale_type_tag, added, ref flags)| {
                    // Avoid inner heap allocation.
                    // TODO: extract some finite length iterator to make this less indented!
                    once(e as &ToBerolinaSQL)
                        .chain(once(a as &ToBerolinaSQL)
                            .chain(once(causet_locale as &ToBerolinaSQL)
                                .chain(once(causet_locale_type_tag as &ToBerolinaSQL)
                                    .chain(once(to_bool_ref(added) as &ToBerolinaSQL)
                                        .chain(once(flags as &ToBerolinaSQL))))))
                }).collect();

                // TODO: cache this for selected causet_locales of count.
                assert!(bindings_per_statement * count < max_vars, "Too many causet_locales: {} * {} >= {}", bindings_per_statement, count, max_vars);
                let causet_locales: String = repeat_causet_locales(bindings_per_statement, count);
                let s: String = if search_type == SearchType::Exact {
                    format!("INSERT INTO temp.exact_searches (e0, a0, v0, causet_locale_type_tag0, added0, flags0) VALUES {}", causet_locales)
                } else {
                    // This will err for duplicates within the tx.
                    format!("INSERT INTO temp.inexact_searches (e0, a0, v0, causet_locale_type_tag0, added0, flags0) VALUES {}", causet_locales)
                };

                // TODO: consider ensuring we inserted the expected number of rows.
                let mut stmt = self.prepare_cached(s.as_str())?;
                stmt.execute(&params)
                    .context(einsteindbErrorKind::NonFtsInsertionIntoTempSearchTableFailed)
                    .map_err(|e| e.into())
                    .map(|_c| ())
            }).collect::<Result<Vec<()>>>();

            results.map(|_| ())
        }

        /// Insert search rows into temporary search tables.
        ///
        /// Eventually, the details of this approach will be captured in
        /// https://github.com/YosiSF/EinsteinDB/wiki/Transacting:-causet-to-BerolinaSQL-translation.
        fn insert_fts_searches<'a>(&self, causets: &'a [Reducedcauset<'a>], search_type: SearchType) -> Result<()> {
            let max_vars = self.limit(Limit::BerolinaSQLITE_LIMIT_VARIABLE_NUMBER) as usize;
            let bindings_per_statement = 6;

            let mut outer_searchid = 2000;

            let chunks: itertools::IntoChunks<_> = causets.into_iter().chunks(max_vars / bindings_per_statement);

            // From string to (searchid, causet_locale_type_tag).
            let mut seen: HashMap<ValueRc<String>, (i64, i32)> = HashMap::with_capacity(causets.len());

            // We'd like to flat_map here, but it's not obvious how to flat_map across Result.
            let results: Result<Vec<()>> = chunks.into_iter().map(|chunk| -> Result<()> {
                let mut causet_count = 0;
                let mut string_count = 0;

                // We must keep these computed causet_locales somewhere to reference them later, so we can't
                // combine this map and the subsequent flat_map.
                // (e0, a0, v0, causet_locale_type_tag0, added0, flags0)
                let block: Result<Vec<(i64 /* e */,
                                       i64 /* a */,
                                       Option<ToBerolinaSQLOutput<'a>> /* causet_locale */,
                                       i32 /* causet_locale_type_tag */,
                                       bool /* added0 */,
                                       u8 /* flags0 */,
                                       i64 /* searchid */)>> = chunk.map(|&(e, a, ref attribute, ref typed_causet_locale, added)| {
                    match typed_causet_locale {
                        &causetq_TV::String(ref rc) => {
                            causet_count += 1;
                            let entry = seen.entry(rc.clone());
                            match entry {
                                Entry::Occupied(entry) => {
                                    let &(searchid, causet_locale_type_tag) = entry.get();
                                    Ok((e, a, None, causet_locale_type_tag, added, attribute.flags(), searchid))
                                },
                                Entry::Vacant(entry) => {
                                    outer_searchid += 1;
                                    string_count += 1;

                                    // Now we can represent the typed causet_locale as an BerolinaSQL causet_locale.
                                    let (causet_locale, causet_locale_type_tag): (ToBerolinaSQLOutput, i32) = typed_causet_locale.to_BerolinaSQL_causet_locale_pair();
                                    entry.insert((outer_searchid, causet_locale_type_tag));

                                    Ok((e, a, Some(causet_locale), causet_locale_type_tag, added, attribute.flags(), outer_searchid))
                                }
                            }
                        },
                        _ => {
                            bail!(einsteindbErrorKind::WrongTypeValueForFtsAssertion);
                        },
                    }
                }).collect();
                let block = block?;

                // First, insert all fulltext string causet_locales.
                // `fts_params` reference computed causet_locales in `block`.
                let fts_params: Vec<&ToBerolinaSQL> =
                    block.iter()
                        .filter(|&&(ref _e, ref _a, ref causet_locale, ref _causet_locale_type_tag, _added, ref _flags, ref _searchid)| {
                            causet_locale.is_some()
                        })
                        .flat_map(|&(ref _e, ref _a, ref causet_locale, ref _causet_locale_type_tag, _added, ref _flags, ref searchid)| {
                            // Avoid inner heap allocation.
                            once(causet_locale as &ToBerolinaSQL)
                                .chain(once(searchid as &ToBerolinaSQL))
                        }).collect();

                // TODO: make this maximally efficient. It's not terribly inefficient right now.
                let fts_causet_locales: String = repeat_causet_locales(2, string_count);
                let fts_s: String = format!("INSERT INTO fulltext_causet_locales_view (text, searchid) VALUES {}", fts_causet_locales);

                // TODO: consider ensuring we inserted the expected number of rows.
                let mut stmt = self.prepare_cached(fts_s.as_str())?;
                stmt.execute(&fts_params).context(einsteindbErrorKind::FtsInsertionFailed)?;

                // Second, insert searches.
                // `params` reference computed causet_locales in `block`.
                let params: Vec<&ToBerolinaSQL> = block.iter().flat_map(|&(ref e, ref a, ref _causet_locale, ref causet_locale_type_tag, added, ref flags, ref searchid)| {
                    // Avoid inner heap allocation.
                    // TODO: extract some finite length iterator to make this less indented!
                    once(e as &ToBerolinaSQL)
                        .chain(once(a as &ToBerolinaSQL)
                            .chain(once(searchid as &ToBerolinaSQL)
                                .chain(once(causet_locale_type_tag as &ToBerolinaSQL)
                                    .chain(once(to_bool_ref(added) as &ToBerolinaSQL)
                                        .chain(once(flags as &ToBerolinaSQL))))))
                }).collect();

                // TODO: cache this for selected causet_locales of count.
                assert!(bindings_per_statement * causet_count < max_vars, "Too many causet_locales: {} * {} >= {}", bindings_per_statement, causet_count, max_vars);
                let inner = "(?, ?, (SELECT rowid FROM fulltext_causet_locales WHERE searchid = ?), ?, ?, ?)".to_string();
                // Like "(?, ?, (SELECT rowid FROM fulltext_causet_locales WHERE searchid = ?), ?, ?, ?), (?, ?, (SELECT rowid FROM fulltext_causet_locales WHERE searchid = ?), ?, ?, ?)".
                let fts_causet_locales: String = repeat(inner).take(causet_count).join(", ");
                let s: String = if search_type == SearchType::Exact {
                    format!("INSERT INTO temp.exact_searches (e0, a0, v0, causet_locale_type_tag0, added0, flags0) VALUES {}", fts_causet_locales)
                } else {
                    format!("INSERT INTO temp.inexact_searches (e0, a0, v0, causet_locale_type_tag0, added0, flags0) VALUES {}", fts_causet_locales)
                };

                // TODO: consider ensuring we inserted the expected number of rows.
                let mut stmt = self.prepare_cached(s.as_str())?;
                stmt.execute(&params).context(einsteindbErrorKind::FtsInsertionIntoTempSearchTableFailed)
                    .map_err(|e| e.into())
                    .map(|_c| ())
            }).collect::<Result<Vec<()>>>();

            // Finally, clean up temporary searchids.
            let mut stmt = self.prepare_cached("UPDATE fulltext_causet_locales SET searchid = NULL WHERE searchid IS NOT NULL")?;
            stmt.execute(&[]).context(einsteindbErrorKind::FtsFailedToDropSearchIds)?;
            results.map(|_| ())
        }

        fn commit_EinsteinDB_transaction(&self, tx_id: Causetid) -> Result<()> {
            insert_transaction(&self, tx_id)?;
            Ok(())
        }

        fn materialize_EinsteinDB_transaction(&self, tx_id: Causetid) -> Result<()> {
            search(&self)?;
            update_causets(&self, tx_id)?;
            Ok(())
        }

        fn resolved_spacetime_lightlike_dagger_upsert(&self) -> Result<Vec<(Causetid, Causetid, causetq_TV, bool)>> {
            let BerolinaSQL_stmt = format!(r#"
            SELECT e, a, v, causet_locale_type_tag, added FROM
            (
                SELECT e0 as e, a0 as a, v0 as v, causet_locale_type_tag0 as causet_locale_type_tag, 1 as added
                FROM temp.search_results
                WHERE a0 IN {} AND added0 IS 1 AND ((rid IS NULL) OR
                    ((rid IS NOT NULL) AND (v0 IS NOT v)))

                UNION

                SELECT e0 as e, a0 as a, v, causet_locale_type_tag0 as causet_locale_type_tag, 0 as added
                FROM temp.search_results
                WHERE a0 in {} AND rid IS NOT NULL AND
                ((added0 IS 0) OR
                    (added0 IS 1 AND search_type IS ':einsteindb.cardinality/one' AND v0 IS NOT v))

            ) ORDER BY e, a, v, causet_locale_type_tag, added"#,
                                           causetids::Spacetime_BerolinaSQL_LIST.as_str(), causetids::Spacetime_BerolinaSQL_LIST.as_str()
            );

            let mut stmt = self.prepare_cached(&BerolinaSQL_stmt)?;
            let m: Result<Vec<_>> = stmt.query_and_then(
                &[],
                row_to_transaction_lightlike_dagger_assertion
            )?.collect();
            m
        }
    }

    /// Extract spacetime-related [e a typed_causet_locale added] causets committed in the given transaction.
    pub fn committed_spacetime_lightlike_dagger_upsert(conn: &rusqlite::Connection, tx_id: Causetid) -> Result<Vec<(Causetid, Causetid, causetq_TV, bool)>> {
        let BerolinaSQL_stmt = format!(r#"
        SELECT e, a, v, causet_locale_type_tag, added
        FROM transactions
        WHERE tx = ? AND a IN {}
        ORDER BY e, a, v, causet_locale_type_tag, added"#,
                                       causetids::Spacetime_BerolinaSQL_LIST.as_str()
        );

        let mut stmt = conn.prepare_cached(&BerolinaSQL_stmt)?;
        let m: Result<Vec<_>> = stmt.query_and_then(
            &[&tx_id as &ToBerolinaSQL],
            row_to_transaction_lightlike_dagger_assertion
        )?.collect();
        m
    }

    /// Takes a event, produces a transaction quadruple.
    fn row_to_transaction_lightlike_dagger_assertion(event: &rusqlite::Row) -> Result<(Causetid, Causetid, causetq_TV, bool)> {
        Ok((
            event.get_checked(0)?,
            event.get_checked(1)?,
            causetq_TV::from_BerolinaSQL_causet_locale_pair(event.get_checked(2)?, event.get_checked(3)?)?,
            event.get_checked(4)?
        ))
    }

    /// Takes a event, produces a causet quadruple.
    fn row_to_causet_lightlike_dagger_assertion(event: &rusqlite::Row) -> Result<(Causetid, Causetid, causetq_TV)> {
        Ok((
            event.get_checked(0)?,
            event.get_checked(1)?,
            causetq_TV::from_BerolinaSQL_causet_locale_pair(event.get_checked(2)?, event.get_checked(3)?)?
        ))
    }

    /// Update the spacetime materialized views based on the given spacetime report.
    ///
    /// This updates the "causetids", "solitonids", and "topograph" materialized views, copying directly from the
    /// "causets" and "transactions" table as appropriate.
    pub fn update_spacetime(conn: &rusqlite::Connection, _old_topograph: &Topograph, new_topograph: &Topograph, spacetime_report: &spacetime::SpacetimeReport) -> Result<()>
    {
        use spacetime::AttributeAlteration::*;

        // Populate the materialized view directly from causets (and, potentially in the future,
        // transactions).  This might generalize nicely as we expand the set of materialized views.
        // TODO: consider doing this in fewer sqlite execute() invocations.
        // TODO: use concat! to avoid creating String instances.
        if !spacetime_report.solitonids_altered.is_empty() {
            // Solitonids is the materialized view of the [causetid :einsteindb/solitonid solitonid] slice of causets.
            conn.execute(format!("DELETE FROM solitonids").as_str(),
                         &[])?;
            conn.execute(format!("INSERT INTO solitonids SELECT e, a, v, causet_locale_type_tag FROM causets WHERE a IN {}", causetids::SOLITONIDS_BerolinaSQL_LIST.as_str()).as_str(),
                         &[])?;
        }

        // Populate the materialized view directly from causets.
        // It's possible that an "solitonid" was removed, along with its attributes.
        // That's not counted as an "alteration" of attributes, so we explicitly check
        // for non-emptiness of 'solitonids_altered'.

        // TODO expand spacetime report to allow for better signaling for the above.

        if !spacetime_report.attributes_installed.is_empty()
            || !spacetime_report.attributes_altered.is_empty()
            || !spacetime_report.solitonids_altered.is_empty() {
            conn.execute(format!("DELETE FROM topograph").as_str(),
                         &[])?;
            // NB: we're using :einsteindb/causet_localeType as a placeholder for the entire topograph-defining set.
            let s = format!(r#"
            WITH s(e) AS (SELECT e FROM causets WHERE a = {})
            INSERT INTO topograph
            SELECT s.e, a, v, causet_locale_type_tag
            FROM causets, s
            WHERE s.e = causets.e AND a IN {}
        "#, causetids::einsteindb_VALUE_TYPE, causetids::SCHEMA_BerolinaSQL_LIST.as_str());
            conn.execute(&s, &[])?;
        }

        let mut index_stmt = conn.prepare("UPDATE causets SET index_avet = ? WHERE a = ?")?;
        let mut unique_causet_locale_stmt = conn.prepare("UPDATE causets SET unique_causet_locale = ? WHERE a = ?")?;
        let mut cardinality_stmt = conn.prepare(r#"
SELECT EXISTS
    (SELECT 1
        FROM causets AS left, causets AS right
        WHERE left.a = ? AND
        left.a = right.a AND
        left.e = right.e AND
        left.v <> right.v)"#)?;

        for (&causetid, alterations) in &spacetime_report.attributes_altered {
            let attribute = new_topograph.require_attribute_for_causetid(causetid)?;

            for alteration in alterations {
                match alteration {
                    &Index => {
                        // This should always succeed.
                        index_stmt.execute(&[&attribute.index, &causetid as &ToBerolinaSQL])?;
                    },
                    &Unique => {
                        // TODO: This can fail if there are conflicting causet_locales; give a more helpful
                        // error message in this case.
                        if unique_causet_locale_stmt.execute(&[to_bool_ref(attribute.unique.is_some()), &causetid as &ToBerolinaSQL]).is_err() {
                            match attribute.unique {
                                Some(attribute::Unique::Value) => bail!(einsteindbErrorKind::TopographAlterationFailed(format!("Cannot alter topograph attribute {} to be :einsteindb.unique/causet_locale", causetid))),
                                Some(attribute::Unique::Idcauset) => bail!(einsteindbErrorKind::TopographAlterationFailed(format!("Cannot alter topograph attribute {} to be :einsteindb.unique/idcauset", causetid))),
                                None => unreachable!(), // This shouldn't happen, even after we support removing :einsteindb/unique.
                            }
                        }
                    },
                    &Cardinality => {
                        // We can always go from :einsteindb.cardinality/one to :einsteindb.cardinality many.  It's
                        // :einsteindb.cardinality/many to :einsteindb.cardinality/one that can fail.
                        //
                        // TODO: improve the failure message.  Perhaps try to mimic what Causetic says in
                        // this case?
                        if !attribute.multival {
                            let mut rows = cardinality_stmt.query(&[&causetid as &ToBerolinaSQL])?;
                            if rows.next().is_some() {
                                bail!(einsteindbErrorKind::TopographAlterationFailed(format!("Cannot alter topograph attribute {} to be :einsteindb.cardinality/one", causetid)));
                            }
                        }
                    },
                    &NoHistory | &IsComponent => {
                        // There's no on disk change required for either of these.
                    },
                }
            }
        }

        Ok(())
    }

    impl PartitionMap {
        /// Allocate a single fresh causetid in the given `partition`.
        pub(crate) fn allocate_causetid(&mut self, partition: &str) -> i64 {
            self.allocate_causetids(partition, 1).start
        }



        pub(crate) fn contains_causetid(&self, causetid: Causetid) -> bool {
            self.causet_locales().any(|partition| partition.contains_causetid(causetid))
        }
    }

    #[APPEND_LOG_g(test)]
    mod tests {
        extern crate env_logger;

        use causal_setal_types::Term;
        use causetq::{
            attribute,
            CausetLocaleNucleonCausetid,
        };
        use debug::{tempids, TestConn};
        use einstein_ml::{
            self,
            InternSet,
        };
        use einstein_ml::causets::OpType;
        use einsteindb_core::{
            HasTopograph,
            Keyword,
        };
        use einsteindb_core::util::Either::*;
        use einsteindb_traits::errors as errors;
        use std::borrow::Borrow;
        use std::collections::BTreeMap;

        use super::*;

        fn run_test_add(mut conn: TestConn) {
            // Test inserting :einsteindb.cardinality/one elements.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb.topograph/version 1]
                                 [:einsteindb/add 101 :einsteindb.topograph/version 2]]");
            assert_matches!(conn.last_transaction(),
                        "[[100 :einsteindb.topograph/version 1 ?tx true]
                          [101 :einsteindb.topograph/version 2 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                       "[[100 :einsteindb.topograph/version 1]
                         [101 :einsteindb.topograph/version 2]]");

            // Test inserting :einsteindb.cardinality/many elements.
            assert_transact!(conn, "[[:einsteindb/add 200 :einsteindb.topograph/attribute 100]
                                 [:einsteindb/add 200 :einsteindb.topograph/attribute 101]]");
            assert_matches!(conn.last_transaction(),
                        "[[200 :einsteindb.topograph/attribute 100 ?tx true]
                          [200 :einsteindb.topograph/attribute 101 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb.topograph/version 1]
                          [101 :einsteindb.topograph/version 2]
                          [200 :einsteindb.topograph/attribute 100]
                          [200 :einsteindb.topograph/attribute 101]]");

            // Test replacing existing :einsteindb.cardinality/one elements.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb.topograph/version 11]
                                 [:einsteindb/add 101 :einsteindb.topograph/version 22]]");
            assert_matches!(conn.last_transaction(),
                        "[[100 :einsteindb.topograph/version 1 ?tx false]
                          [100 :einsteindb.topograph/version 11 ?tx true]
                          [101 :einsteindb.topograph/version 2 ?tx false]
                          [101 :einsteindb.topograph/version 22 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb.topograph/version 11]
                          [101 :einsteindb.topograph/version 22]
                          [200 :einsteindb.topograph/attribute 100]
                          [200 :einsteindb.topograph/attribute 101]]");


            // Test that asserting existing :einsteindb.cardinality/one elements doesn't change the store.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb.topograph/version 11]
                                 [:einsteindb/add 101 :einsteindb.topograph/version 22]]");
            assert_matches!(conn.last_transaction(),
                        "[[?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb.topograph/version 11]
                          [101 :einsteindb.topograph/version 22]
                          [200 :einsteindb.topograph/attribute 100]
                          [200 :einsteindb.topograph/attribute 101]]");


            // Test that asserting existing :einsteindb.cardinality/many elements doesn't change the store.
            assert_transact!(conn, "[[:einsteindb/add 200 :einsteindb.topograph/attribute 100]
                                 [:einsteindb/add 200 :einsteindb.topograph/attribute 101]]");
            assert_matches!(conn.last_transaction(),
                        "[[?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb.topograph/version 11]
                          [101 :einsteindb.topograph/version 22]
                          [200 :einsteindb.topograph/attribute 100]
                          [200 :einsteindb.topograph/attribute 101]]");
        }

        #[test]
        fn test_add() {
            run_test_add(TestConn::default());
        }

        #[test]
        fn test_tx_lightlike_dagger_upsert() {
            let mut conn = TestConn::default();

            // Test that txInstant can be asserted.
            assert_transact!(conn, "[[:einsteindb/add (transaction-tx) :einsteindb/txInstant #inst \"2017-06-16T00:56:41.257Z\"]
                                 [:einsteindb/add 100 :einsteindb/solitonid :name/Ivan]
                                 [:einsteindb/add 101 :einsteindb/solitonid :name/Petr]]");
            assert_matches!(conn.last_transaction(),
                        "[[100 :einsteindb/solitonid :name/Ivan ?tx true]
                          [101 :einsteindb/solitonid :name/Petr ?tx true]
                          [?tx :einsteindb/txInstant #inst \"2017-06-16T00:56:41.257Z\" ?tx true]]");

            // Test multiple txInstant with different causet_locales should fail.
            assert_transact!(conn, "[[:einsteindb/add (transaction-tx) :einsteindb/txInstant #inst \"2017-06-16T00:59:11.257Z\"]
                                 [:einsteindb/add (transaction-tx) :einsteindb/txInstant #inst \"2017-06-16T00:59:11.752Z\"]
                                 [:einsteindb/add 102 :einsteindb/solitonid :name/Vlad]]",
                         Err("topograph constraint violation: cardinality conflicts:\n  CardinalityOneAddConflict { e: 268435458, a: 3, vs: {Instant(2017-06-16T00:59:11.257Z), Instant(2017-06-16T00:59:11.752Z)} }\n"));

            // Test multiple txInstants with the same causet_locale.
            assert_transact!(conn, "[[:einsteindb/add (transaction-tx) :einsteindb/txInstant #inst \"2017-06-16T00:59:11.257Z\"]
                                 [:einsteindb/add (transaction-tx) :einsteindb/txInstant #inst \"2017-06-16T00:59:11.257Z\"]
                                 [:einsteindb/add 103 :einsteindb/solitonid :name/Dimitri]
                                 [:einsteindb/add 104 :einsteindb/solitonid :name/Anton]]");
            assert_matches!(conn.last_transaction(),
                        "[[103 :einsteindb/solitonid :name/Dimitri ?tx true]
                          [104 :einsteindb/solitonid :name/Anton ?tx true]
                          [?tx :einsteindb/txInstant #inst \"2017-06-16T00:59:11.257Z\" ?tx true]]");

            // We need a few attributes to work with.
            assert_transact!(conn, "[[:einsteindb/add 111 :einsteindb/solitonid :test/str]
                                 [:einsteindb/add 111 :einsteindb/causet_localeType :einsteindb.type/string]
                                 [:einsteindb/add 222 :einsteindb/solitonid :test/ref]
                                 [:einsteindb/add 222 :einsteindb/causet_localeType :einsteindb.type/ref]]");

            // Test that we can assert spacetime about the current transaction.
            assert_transact!(conn, "[[:einsteindb/add (transaction-tx) :test/str \"We want spacetime!\"]]");
            assert_matches!(conn.last_transaction(),
                        "[[?tx :einsteindb/txInstant ?ms ?tx true]
                          [?tx :test/str \"We want spacetime!\" ?tx true]]");

            // Test that we can use (transaction-tx) as a causet_locale.
            assert_transact!(conn, "[[:einsteindb/add 333 :test/ref (transaction-tx)]]");
            assert_matches!(conn.last_transaction(),
                        "[[333 :test/ref ?tx ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

            // Test that we type-check properly.  In the causet_locale position, (transaction-tx) yields a ref;
            // :einsteindb/solitonid expects a soliton_idword.
            assert_transact!(conn, "[[:einsteindb/add 444 :einsteindb/solitonid (transaction-tx)]]",
                         Err("not yet implemented: Transaction function transaction-tx produced causet_locale of type :einsteindb.type/ref but expected type :einsteindb.type/soliton_idword"));

            // Test that we can assert spacetime about the current transaction.
            assert_transact!(conn, "[[:einsteindb/add (transaction-tx) :test/ref (transaction-tx)]]");
            assert_matches!(conn.last_transaction(),
                        "[[?tx :einsteindb/txInstant ?ms ?tx true]
                          [?tx :test/ref ?tx ?tx true]]");
        }

        #[test]
        fn test_retract() {
            let mut conn = TestConn::default();

            // Insert a few :einsteindb.cardinality/one elements.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb.topograph/version 1]
                                 [:einsteindb/add 101 :einsteindb.topograph/version 2]]");
            assert_matches!(conn.last_transaction(),
                        "[[100 :einsteindb.topograph/version 1 ?tx true]
                          [101 :einsteindb.topograph/version 2 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb.topograph/version 1]
                          [101 :einsteindb.topograph/version 2]]");

            // And a few :einsteindb.cardinality/many elements.
            assert_transact!(conn, "[[:einsteindb/add 200 :einsteindb.topograph/attribute 100]
                                 [:einsteindb/add 200 :einsteindb.topograph/attribute 101]]");
            assert_matches!(conn.last_transaction(),
                        "[[200 :einsteindb.topograph/attribute 100 ?tx true]
                          [200 :einsteindb.topograph/attribute 101 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb.topograph/version 1]
                          [101 :einsteindb.topograph/version 2]
                          [200 :einsteindb.topograph/attribute 100]
                          [200 :einsteindb.topograph/attribute 101]]");

            // Test that we can retract :einsteindb.cardinality/one elements.
            assert_transact!(conn, "[[:einsteindb/retract 100 :einsteindb.topograph/version 1]]");
            assert_matches!(conn.last_transaction(),
                        "[[100 :einsteindb.topograph/version 1 ?tx false]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[101 :einsteindb.topograph/version 2]
                          [200 :einsteindb.topograph/attribute 100]
                          [200 :einsteindb.topograph/attribute 101]]");

            // Test that we can retract :einsteindb.cardinality/many elements.
            assert_transact!(conn, "[[:einsteindb/retract 200 :einsteindb.topograph/attribute 100]]");
            assert_matches!(conn.last_transaction(),
                        "[[200 :einsteindb.topograph/attribute 100 ?tx false]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[101 :einsteindb.topograph/version 2]
                          [200 :einsteindb.topograph/attribute 101]]");

            // Verify that retracting :einsteindb.cardinality/{one,many} elements that are not present doesn't
            // change the store.
            assert_transact!(conn, "[[:einsteindb/retract 100 :einsteindb.topograph/version 1]
                                 [:einsteindb/retract 200 :einsteindb.topograph/attribute 100]]");
            assert_matches!(conn.last_transaction(),
                        "[[?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[101 :einsteindb.topograph/version 2]
                          [200 :einsteindb.topograph/attribute 101]]");
        }

        #[test]
        fn test_einsteindb_doc_is_not_topograph() {
            let mut conn = TestConn::default();

            // Neither transaction below is defining a new attribute.  That is, it's fine to use :einsteindb/doc
            // to describe any causet in the system, not just attributes.  And in particular, including
            // :einsteindb/doc shouldn't make the transactor consider the causet a topograph attribute.
            assert_transact!(conn, r#"
            [{:einsteindb/doc "test"}]
        "#);

            assert_transact!(conn, r#"
            [{:einsteindb/solitonid :test/id :einsteindb/doc "test"}]
        "#);
        }

        // Unique is required!
        #[test]
        fn test_upsert_issue_538() {
            let mut conn = TestConn::default();
            assert_transact!(conn, "
            [{:einsteindb/solitonid :person/name
              :einsteindb/causet_localeType :einsteindb.type/string
              :einsteindb/cardinality :einsteindb.cardinality/many}
             {:einsteindb/solitonid :person/age
              :einsteindb/causet_localeType :einsteindb.type/long
              :einsteindb/cardinality :einsteindb.cardinality/one}
             {:einsteindb/solitonid :person/email
              :einsteindb/causet_localeType :einsteindb.type/string
              :einsteindb/unique :einsteindb.unique/causetid
              :einsteindb/cardinality :einsteindb.cardinality/many}]",
              Err("bad topograph lightlike_dagger_assertion: :einsteindb/unique :einsteindb/unique_idcauset without :einsteindb/Index true for causetid: 65538"));
        }

        // TODO: don't use :einsteindb/solitonid to test upserts!
        #[test]
        fn test_upsert_vector() {
            let mut conn = TestConn::default();

            // Insert some :einsteindb.unique/idcauset elements.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb/solitonid :name/Ivan]
                                 [:einsteindb/add 101 :einsteindb/solitonid :name/Petr]]");
            assert_matches!(conn.last_transaction(),
                        "[[100 :einsteindb/solitonid :name/Ivan ?tx true]
                          [101 :einsteindb/solitonid :name/Petr ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb/solitonid :name/Ivan]
                          [101 :einsteindb/solitonid :name/Petr]]");

            // Upserting two tempids to the same causetid works.
            let report = assert_transact!(conn, "[[:einsteindb/add \"t1\" :einsteindb/solitonid :name/Ivan]
                                              [:einsteindb/add \"t1\" :einsteindb.topograph/attribute 100]
                                              [:einsteindb/add \"t2\" :einsteindb/solitonid :name/Petr]
                                              [:einsteindb/add \"t2\" :einsteindb.topograph/attribute 101]]");
            assert_matches!(conn.last_transaction(),
                        "[[100 :einsteindb.topograph/attribute :name/Ivan ?tx true]
                          [101 :einsteindb.topograph/attribute :name/Petr ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb/solitonid :name/Ivan]
                          [100 :einsteindb.topograph/attribute :name/Ivan]
                          [101 :einsteindb/solitonid :name/Petr]
                          [101 :einsteindb.topograph/attribute :name/Petr]]");
            assert_matches!(tempids(&report),
                        "{\"t1\" 100
                          \"t2\" 101}");

            // Upserting a tempid works.  The ref doesn't have to exist (at this time), but we can't
            // reuse an existing ref due to :einsteindb/unique :einsteindb.unique/causet_locale.
            let report = assert_transact!(conn, "[[:einsteindb/add \"t1\" :einsteindb/solitonid :name/Ivan]
                                              [:einsteindb/add \"t1\" :einsteindb.topograph/attribute 102]]");
            assert_matches!(conn.last_transaction(),
                        "[[100 :einsteindb.topograph/attribute 102 ?tx true]
                          [?true :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb/solitonid :name/Ivan]
                          [100 :einsteindb.topograph/attribute :name/Ivan]
                          [100 :einsteindb.topograph/attribute 102]
                          [101 :einsteindb/solitonid :name/Petr]
                          [101 :einsteindb.topograph/attribute :name/Petr]]");
            assert_matches!(tempids(&report),
                        "{\"t1\" 100}");

            // A single complex upsert allocates a new causetid.
            let report = assert_transact!(conn, "[[:einsteindb/add \"t1\" :einsteindb.topograph/attribute \"t2\"]]");
            assert_matches!(conn.last_transaction(),
                        "[[65536 :einsteindb.topograph/attribute 65537 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{\"t1\" 65536
                          \"t2\" 65537}");

            // Conflicting upserts fail.
            assert_transact!(conn, "[[:einsteindb/add \"t1\" :einsteindb/solitonid :name/Ivan]
                                 [:einsteindb/add \"t1\" :einsteindb/solitonid :name/Petr]]",
                         Err("topograph constraint violation: conflicting upserts:\n  tempid lightlike(\"t1\") upserts to {CausetLocaleNucleonCausetid(100), CausetLocaleNucleonCausetid(101)}\n"));

            // The error messages of conflicting upserts gives information about all failing upserts (in a particular generation).
            assert_transact!(conn, "[[:einsteindb/add \"t2\" :einsteindb/solitonid :name/Grigory]
                                 [:einsteindb/add \"t2\" :einsteindb/solitonid :name/Petr]
                                 [:einsteindb/add \"t2\" :einsteindb/solitonid :name/Ivan]
                                 [:einsteindb/add \"t1\" :einsteindb/solitonid :name/Ivan]
                                 [:einsteindb/add \"t1\" :einsteindb/solitonid :name/Petr]]",
                         Err("topograph constraint violation: conflicting upserts:\n  tempid lightlike(\"t1\") upserts to {CausetLocaleNucleonCausetid(100), CausetLocaleNucleonCausetid(101)}\n  tempid lightlike(\"t2\") upserts to {CausetLocaleNucleonCausetid(100), CausetLocaleNucleonCausetid(101)}\n"));

            // tempids in :einsteindb/retract that don't upsert fail.
            assert_transact!(conn, "[[:einsteindb/retract \"t1\" :einsteindb/solitonid :name/Anonymous]]",
                         Err("not yet implemented: [:einsteindb/retract ...] causet referenced tempid that did not upsert: t1"));

            // tempids in :einsteindb/retract that do upsert are retracted.  The ref given doesn't exist, so the
            // lightlike_dagger_assertion will be ignored.
            let report = assert_transact!(conn, "[[:einsteindb/add \"t1\" :einsteindb/solitonid :name/Ivan]
                                              [:einsteindb/retract \"t1\" :einsteindb.topograph/attribute 103]]");
            assert_matches!(conn.last_transaction(),
                        "[[?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{\"t1\" 100}");

            // A multistep upsert.  The upsert algorithm will first try to resolve "t1", fail, and then
            // allocate both "t1" and "t2".
            let report = assert_transact!(conn, "[[:einsteindb/add \"t1\" :einsteindb/solitonid :name/Josef]
                                              [:einsteindb/add \"t2\" :einsteindb.topograph/attribute \"t1\"]]");
            assert_matches!(conn.last_transaction(),
                        "[[65538 :einsteindb/solitonid :name/Josef ?tx true]
                          [65539 :einsteindb.topograph/attribute :name/Josef ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{\"t1\" 65538
                          \"t2\" 65539}");

            // A multistep insert.  This time, we can resolve both, but we have to try "t1", succeed,
            // and then resolve "t2".
            // TODO: We can't quite test this without more topograph elements.
            // conn.transact("[[:einsteindb/add \"t1\" :einsteindb/solitonid :name/Josef]
            //                 [:einsteindb/add \"t2\" :einsteindb/solitonid \"t1\"]]");
            // assert_matches!(conn.last_transaction(),
            //                 "[[65538 :einsteindb/solitonid :name/Josef]
            //                   [65538 :einsteindb/solitonid :name/Karl]
            //                   [?tx :einsteindb/txInstant ?ms ?tx true]]");
        }

        #[test]
        fn test_resolved_upserts() {
            let mut conn = TestConn::default();
            assert_transact!(conn, "[
            {:einsteindb/solitonid :test/id
             :einsteindb/causet_localeType :einsteindb.type/string
             :einsteindb/unique :einsteindb.unique/idcauset
             :einsteindb/Index true
             :einsteindb/cardinality :einsteindb.cardinality/one}
            {:einsteindb/solitonid :test/ref
             :einsteindb/causet_localeType :einsteindb.type/ref
             :einsteindb/unique :einsteindb.unique/idcauset
             :einsteindb/Index true
             :einsteindb/cardinality :einsteindb.cardinality/one}
        ]");

            // Partial data for :test/id, links via :test/ref.
            assert_transact!(conn, r#"[
            [:einsteindb/add 100 :test/id "0"]
            [:einsteindb/add 101 :test/ref 100]
            [:einsteindb/add 102 :test/ref 101]
            [:einsteindb/add 103 :test/ref 102]
        ]"#);

            // Fill in the rest of the data for :test/id, using the links of :test/ref.
            let report = assert_transact!(conn, r#"[
            {:einsteindb/id "a" :test/id "0"}
            {:einsteindb/id "b" :test/id "1" :test/ref "a"}
            {:einsteindb/id "c" :test/id "2" :test/ref "b"}
            {:einsteindb/id "d" :test/id "3" :test/ref "c"}
        ]"#);

            assert_matches!(tempids(&report), r#"{
            "a" 100
            "b" 101
            "c" 102
            "d" 103
        }"#);

            assert_matches!(conn.last_transaction(), r#"[
            [101 :test/id "1" ?tx true]
            [102 :test/id "2" ?tx true]
            [103 :test/id "3" ?tx true]
            [?tx :einsteindb/txInstant ?ms ?tx true]
        ]"#);
        }

        #[test]
        fn test_SQLite_limit() {
            let conn = new_connection("").expect("Couldn't open in-memory einsteindb");
            let initial = conn.limit(Limit::BerolinaSQLITE_LIMIT_VARIABLE_NUMBER);
            // Sanity check.
            assert!(initial > 500);

            // Make sure setting works.
            conn.set_limit(Limit::BerolinaSQLITE_LIMIT_VARIABLE_NUMBER, 222);
            assert_eq!(222, conn.limit(Limit::BerolinaSQLITE_LIMIT_VARIABLE_NUMBER));
        }

        #[test]
        fn test_einsteindb_install() {
            let mut conn = TestConn::default();

            // We're missing some tests here, since our impleEinsteinDBion is incomplete.
            // See https://github.com/YosiSF/EinsteinDB/issues/797

            // We can assert a new topograph attribute.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb/solitonid :test/solitonid]
                                 [:einsteindb/add 100 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 100 :einsteindb/cardinality :einsteindb.cardinality/many]]");

            assert_eq!(conn.topograph.causetid_map.get(&100).cloned().unwrap(), to_isoliton_namespaceable_soliton_idword(":test/solitonid").unwrap());
            assert_eq!(conn.topograph.causetid_map.get(&to_isoliton_namespaceable_soliton_idword(":test/solitonid").unwrap()).cloned().unwrap(), 100);
            let attribute = conn.topograph.attribute_for_causetid(100).unwrap().clone();
            assert_eq!(attribute.causet_locale_type, ValueType::Long);
            assert_eq!(attribute.multival, true);
            assert_eq!(attribute.fulltext, false);

            assert_matches!(conn.last_transaction(),
                        "[[100 :einsteindb/solitonid :test/solitonid ?tx true]
                          [100 :einsteindb/causet_localeType :einsteindb.type/long ?tx true]
                          [100 :einsteindb/cardinality :einsteindb.cardinality/many ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb/solitonid :test/solitonid]
                          [100 :einsteindb/causet_localeType :einsteindb.type/long]
                          [100 :einsteindb/cardinality :einsteindb.cardinality/many]]");

            // Let's check we actually have the topograph characteristics we expect.
            let attribute = conn.topograph.attribute_for_causetid(100).unwrap().clone();
            assert_eq!(attribute.causet_locale_type, ValueType::Long);
            assert_eq!(attribute.multival, true);
            assert_eq!(attribute.fulltext, false);

            // Let's check that we can use the freshly installed attribute.
            assert_transact!(conn, "[[:einsteindb/add 101 100 -10]
                                 [:einsteindb/add 101 :test/solitonid -9]]");

            assert_matches!(conn.last_transaction(),
                        "[[101 :test/solitonid -10 ?tx true]
                          [101 :test/solitonid -9 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

            // Cannot retract a single characteristic of an installed attribute.
            assert_transact!(conn,
                         "[[:einsteindb/retract 100 :einsteindb/cardinality :einsteindb.cardinality/many]]",
                         Err("bad topograph lightlike_dagger_assertion: Retracting attribute 8 for causet 100 not permitted."));

            // Cannot retract a single characteristic of an installed attribute.
            assert_transact!(conn,
                         "[[:einsteindb/retract 100 :einsteindb/causet_localeType :einsteindb.type/long]]",
                         Err("bad topograph lightlike_dagger_assertion: Retracting attribute 7 for causet 100 not permitted."));

            // Cannot retract a non-defining set of characteristics of an installed attribute.
            assert_transact!(conn,
                         "[[:einsteindb/retract 100 :einsteindb/causet_localeType :einsteindb.type/long]
                         [:einsteindb/retract 100 :einsteindb/cardinality :einsteindb.cardinality/many]]",
                         Err("bad topograph lightlike_dagger_assertion: Retracting defining attributes of a topograph without retracting its :einsteindb/solitonid is not permitted."));

            // See https://github.com/YosiSF/EinsteinDB/issues/796.
            // assert_transact!(conn,
            //                 "[[:einsteindb/retract 100 :einsteindb/solitonid :test/solitonid]]",
            //                 Err("bad topograph lightlike_dagger_assertion: Retracting :einsteindb/solitonid of a topograph without retracting its defining attributes is not permitted."));

            // Can retract all of characterists of an installed attribute in one go.
            assert_transact!(conn,
                         "[[:einsteindb/retract 100 :einsteindb/cardinality :einsteindb.cardinality/many]
                         [:einsteindb/retract 100 :einsteindb/causet_localeType :einsteindb.type/long]
                         [:einsteindb/retract 100 :einsteindb/solitonid :test/solitonid]]");

            // Trying to install an attribute without a :einsteindb/solitonid is allowed.
            assert_transact!(conn, "[[:einsteindb/add 101 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 101 :einsteindb/cardinality :einsteindb.cardinality/many]]");
        }

        #[test]
        fn test_einsteindb_alter() {
            let mut conn = TestConn::default();

            // Start by installing a :einsteindb.cardinality/one attribute.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb/solitonid :test/solitonid]
                                 [:einsteindb/add 100 :einsteindb/causet_localeType :einsteindb.type/soliton_idword]
                                 [:einsteindb/add 100 :einsteindb/cardinality :einsteindb.cardinality/one]]");

            // Trying to alter the :einsteindb/causet_localeType will fail.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb/causet_localeType :einsteindb.type/long]]",
                         Err("bad topograph lightlike_dagger_assertion: Topograph alteration for existing attribute with causetid 100 is not valid"));

            // But we can alter the cardinality.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb/cardinality :einsteindb.cardinality/many]]");

            assert_matches!(conn.last_transaction(),
                        "[[100 :einsteindb/cardinality :einsteindb.cardinality/one ?tx false]
                          [100 :einsteindb/cardinality :einsteindb.cardinality/many ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb/solitonid :test/solitonid]
                          [100 :einsteindb/causet_localeType :einsteindb.type/soliton_idword]
                          [100 :einsteindb/cardinality :einsteindb.cardinality/many]]");

            // Let's check we actually have the topograph characteristics we expect.
            let attribute = conn.topograph.attribute_for_causetid(100).unwrap().clone();
            assert_eq!(attribute.causet_locale_type, ValueType::Keyword);
            assert_eq!(attribute.multival, true);
            assert_eq!(attribute.fulltext, false);

            // Let's check that we can use the freshly altered attribute's new characteristic.
            assert_transact!(conn, "[[:einsteindb/add 101 100 :test/causet_locale1]
                                 [:einsteindb/add 101 :test/solitonid :test/causet_locale2]]");

            assert_matches!(conn.last_transaction(),
                        "[[101 :test/solitonid :test/causet_locale1 ?tx true]
                          [101 :test/solitonid :test/causet_locale2 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
        }

        #[test]
        fn test_einsteindb_causetid() {
            let mut conn = TestConn::default();

            // We can assert a new :einsteindb/solitonid.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb/solitonid :name/Ivan]]");
            assert_matches!(conn.last_transaction(),
                        "[[100 :einsteindb/solitonid :name/Ivan ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb/solitonid :name/Ivan]]");
            assert_eq!(conn.topograph.causetid_map.get(&100).cloned().unwrap(), to_isoliton_namespaceable_soliton_idword(":name/Ivan").unwrap());
            assert_eq!(conn.topograph.causetid_map.get(&to_isoliton_namespaceable_soliton_idword(":name/Ivan").unwrap()).cloned().unwrap(), 100);

            // We can re-assert an existing :einsteindb/solitonid.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb/solitonid :name/Ivan]]");
            assert_matches!(conn.last_transaction(),
                        "[[?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb/solitonid :name/Ivan]]");
            assert_eq!(conn.topograph.causetid_map.get(&100).cloned().unwrap(), to_isoliton_namespaceable_soliton_idword(":name/Ivan").unwrap());
            assert_eq!(conn.topograph.causetid_map.get(&to_isoliton_namespaceable_soliton_idword(":name/Ivan").unwrap()).cloned().unwrap(), 100);

            // We can alter an existing :einsteindb/solitonid to have a new soliton_idword.
            assert_transact!(conn, "[[:einsteindb/add :name/Ivan :einsteindb/solitonid :name/Petr]]");
            assert_matches!(conn.last_transaction(),
                        "[[100 :einsteindb/solitonid :name/Ivan ?tx false]
                          [100 :einsteindb/solitonid :name/Petr ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb/solitonid :name/Petr]]");
            // Causetid map is updated.
            assert_eq!(conn.topograph.causetid_map.get(&100).cloned().unwrap(), to_isoliton_namespaceable_soliton_idword(":name/Petr").unwrap());
            // Solitonid map contains the new solitonid.
            assert_eq!(conn.topograph.causetid_map.get(&to_isoliton_namespaceable_soliton_idword(":name/Petr").unwrap()).cloned().unwrap(), 100);
            // Solitonid map no longer contains the old solitonid.
            assert!(conn.topograph.causetid_map.get(&to_isoliton_namespaceable_soliton_idword(":name/Ivan").unwrap()).is_none());

            // We can re-purpose an old solitonid.
            assert_transact!(conn, "[[:einsteindb/add 101 :einsteindb/solitonid :name/Ivan]]");
            assert_matches!(conn.last_transaction(),
                        "[[101 :einsteindb/solitonid :name/Ivan ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[100 :einsteindb/solitonid :name/Petr]
                          [101 :einsteindb/solitonid :name/Ivan]]");
            // Causetid map contains both causetids.
            assert_eq!(conn.topograph.causetid_map.get(&100).cloned().unwrap(), to_isoliton_namespaceable_soliton_idword(":name/Petr").unwrap());
            assert_eq!(conn.topograph.causetid_map.get(&101).cloned().unwrap(), to_isoliton_namespaceable_soliton_idword(":name/Ivan").unwrap());
            // Solitonid map contains the new solitonid.
            assert_eq!(conn.topograph.causetid_map.get(&to_isoliton_namespaceable_soliton_idword(":name/Petr").unwrap()).cloned().unwrap(), 100);
            // Solitonid map contains the old solitonid, but re-purposed to the new causetid.
            assert_eq!(conn.topograph.causetid_map.get(&to_isoliton_namespaceable_soliton_idword(":name/Ivan").unwrap()).cloned().unwrap(), 101);

            // We can retract an existing :einsteindb/solitonid.
            assert_transact!(conn, "[[:einsteindb/retract :name/Petr :einsteindb/solitonid :name/Petr]]");
            // It's really gone.
            assert!(conn.topograph.causetid_map.get(&100).is_none());
            assert!(conn.topograph.causetid_map.get(&to_isoliton_namespaceable_soliton_idword(":name/Petr").unwrap()).is_none());
        }

        #[test]
        fn test_einsteindb_alter_cardinality() {
            let mut conn = TestConn::default();

            // Start by installing a :einsteindb.cardinality/one attribute.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb/solitonid :test/solitonid]
                                 [:einsteindb/add 100 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 100 :einsteindb/cardinality :einsteindb.cardinality/one]]");

            assert_transact!(conn, "[[:einsteindb/add 200 :test/solitonid 1]]");

            // We can always go from :einsteindb.cardinality/one to :einsteindb.cardinality/many.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb/cardinality :einsteindb.cardinality/many]]");

            assert_transact!(conn, "[[:einsteindb/add 200 :test/solitonid 2]]");

            assert_matches!(conn.causets(),
                        "[[100 :einsteindb/solitonid :test/solitonid]
                          [100 :einsteindb/causet_localeType :einsteindb.type/long]
                          [100 :einsteindb/cardinality :einsteindb.cardinality/many]
                          [200 :test/solitonid 1]
                          [200 :test/solitonid 2]]");

            // We can't always go from :einsteindb.cardinality/many to :einsteindb.cardinality/one.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb/cardinality :einsteindb.cardinality/one]]",
                         // TODO: give more helpful error details.
                         Err("topograph alteration failed: Cannot alter topograph attribute 100 to be :einsteindb.cardinality/one"));
        }

        #[test]
        fn test_einsteindb_alter_unique_causet_locale() {
            let mut conn = TestConn::default();

            // Start by installing a :einsteindb.cardinality/one attribute.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb/solitonid :test/solitonid]
                                 [:einsteindb/add 100 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 100 :einsteindb/cardinality :einsteindb.cardinality/one]]");

            assert_transact!(conn, "[[:einsteindb/add 200 :test/solitonid 1]
                                 [:einsteindb/add 201 :test/solitonid 1]]");

            // We can't always migrate to be :einsteindb.unique/causet_locale.
            assert_transact!(conn, "[[:einsteindb/add :test/solitonid :einsteindb/unique :einsteindb.unique/causet_locale]]",
                         // TODO: give more helpful error details.
                         Err("topograph alteration failed: Cannot alter topograph attribute 100 to be :einsteindb.unique/causet_locale"));

            // Not even indirectly!
            assert_transact!(conn, "[[:einsteindb/add :test/solitonid :einsteindb/unique :einsteindb.unique/idcauset]]",
                         // TODO: give more helpful error details.
                         Err("topograph alteration failed: Cannot alter topograph attribute 100 to be :einsteindb.unique/idcauset"));

            // But we can if we make sure there's no repeated [a v] pair.
            assert_transact!(conn, "[[:einsteindb/add 201 :test/solitonid 2]]");

            assert_transact!(conn, "[[:einsteindb/add :test/solitonid :einsteindb/Index true]
                                 [:einsteindb/add :test/solitonid :einsteindb/unique :einsteindb.unique/causet_locale]
                                 [:einsteindb/add :einsteindb.part/einsteindb :einsteindb.alter/attribute 100]]");

            // We can also retract the uniqueness constraint altogether.
            assert_transact!(conn, "[[:einsteindb/retract :test/solitonid :einsteindb/unique :einsteindb.unique/causet_locale]]");

            // Once we've done so, the topograph shows it's not unique…
            {
                let attr = conn.topograph.attribute_for_causetid(&Keyword::isoliton_namespaceable("test", "solitonid")).unwrap().0;
                assert_eq!(None, attr.unique);
            }

            // … and we can add more lightlike_dagger_upsert with duplicate causet_locales.
            assert_transact!(conn, "[[:einsteindb/add 121 :test/solitonid 1]
                                 [:einsteindb/add 221 :test/solitonid 2]]");
        }

        #[test]
        fn test_einsteindb_double_spacelike_dagger_spacelike_dagger_retraction_issue_818() {
            let mut conn = TestConn::default();

            // Start by installing a :einsteindb.cardinality/one attribute.
            assert_transact!(conn, "[[:einsteindb/add 100 :einsteindb/solitonid :test/solitonid]
                                 [:einsteindb/add 100 :einsteindb/causet_localeType :einsteindb.type/string]
                                 [:einsteindb/add 100 :einsteindb/cardinality :einsteindb.cardinality/one]
                                 [:einsteindb/add 100 :einsteindb/unique :einsteindb.unique/idcauset]
                                 [:einsteindb/add 100 :einsteindb/Index true]]");

            assert_transact!(conn, "[[:einsteindb/add 200 :test/solitonid \"Oi\"]]");

            assert_transact!(conn, "[[:einsteindb/add 200 :test/solitonid \"Ai!\"]
                                 [:einsteindb/retract 200 :test/solitonid \"Oi\"]]");

            assert_matches!(conn.last_transaction(),
                        "[[200 :test/solitonid \"Ai!\" ?tx true]
                          [200 :test/solitonid \"Oi\" ?tx false]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

            assert_matches!(conn.causets(),
                        "[[100 :einsteindb/solitonid :test/solitonid]
                          [100 :einsteindb/causet_localeType :einsteindb.type/string]
                          [100 :einsteindb/cardinality :einsteindb.cardinality/one]
                          [100 :einsteindb/unique :einsteindb.unique/idcauset]
                          [100 :einsteindb/Index true]
                          [200 :test/solitonid \"Ai!\"]]");
        }

        /// Verify that we can't alter :einsteindb/fulltext topograph characteristics at all.
        #[test]
        fn test_einsteindb_alter_fulltext() {
            let mut conn = TestConn::default();

            // Start by installing a :einsteindb/fulltext true and a :einsteindb/fulltext unset attribute.
            assert_transact!(conn, "[[:einsteindb/add 111 :einsteindb/solitonid :test/fulltext]
                                 [:einsteindb/add 111 :einsteindb/causet_localeType :einsteindb.type/string]
                                 [:einsteindb/add 111 :einsteindb/unique :einsteindb.unique/idcauset]
                                 [:einsteindb/add 111 :einsteindb/Index true]
                                 [:einsteindb/add 111 :einsteindb/fulltext true]
                                 [:einsteindb/add 222 :einsteindb/solitonid :test/string]
                                 [:einsteindb/add 222 :einsteindb/cardinality :einsteindb.cardinality/one]
                                 [:einsteindb/add 222 :einsteindb/causet_localeType :einsteindb.type/string]
                                 [:einsteindb/add 222 :einsteindb/Index true]]");

            assert_transact!(conn,
                         "[[:einsteindb/retract 111 :einsteindb/fulltext true]]",
                         Err("bad topograph lightlike_dagger_assertion: Retracting attribute 12 for causet 111 not permitted."));

            assert_transact!(conn,
                         "[[:einsteindb/add 222 :einsteindb/fulltext true]]",
                         Err("bad topograph lightlike_dagger_assertion: Topograph alteration for existing attribute with causetid 222 is not valid"));
        }

        #[test]
        fn test_einsteindb_fulltext() {
            let mut conn = TestConn::default();

            // Start by installing a few :einsteindb/fulltext true attributes.
            assert_transact!(conn, "[[:einsteindb/add 111 :einsteindb/solitonid :test/fulltext]
                                 [:einsteindb/add 111 :einsteindb/causet_localeType :einsteindb.type/string]
                                 [:einsteindb/add 111 :einsteindb/unique :einsteindb.unique/idcauset]
                                 [:einsteindb/add 111 :einsteindb/Index true]
                                 [:einsteindb/add 111 :einsteindb/fulltext true]
                                 [:einsteindb/add 222 :einsteindb/solitonid :test/other]
                                 [:einsteindb/add 222 :einsteindb/cardinality :einsteindb.cardinality/one]
                                 [:einsteindb/add 222 :einsteindb/causet_localeType :einsteindb.type/string]
                                 [:einsteindb/add 222 :einsteindb/Index true]
                                 [:einsteindb/add 222 :einsteindb/fulltext true]]");

            // Let's check we actually have the topograph characteristics we expect.
            let fulltext = conn.topograph.attribute_for_causetid(111).cloned().expect(":test/fulltext");
            assert_eq!(fulltext.causet_locale_type, ValueType::String);
            assert_eq!(fulltext.fulltext, true);
            assert_eq!(fulltext.multival, false);
            assert_eq!(fulltext.unique, Some(attribute::Unique::Idcauset));

            let other = conn.topograph.attribute_for_causetid(222).cloned().expect(":test/other");
            assert_eq!(other.causet_locale_type, ValueType::String);
            assert_eq!(other.fulltext, true);
            assert_eq!(other.multival, false);
            assert_eq!(other.unique, None);

            // We can add fulltext indexed causets.
            assert_transact!(conn, "[[:einsteindb/add 301 :test/fulltext \"test this\"]]");
            // causet_locale causet_merge is rowid into fulltext table.
            assert_matches!(conn.fulltext_causet_locales(),
                        "[[1 \"test this\"]]");
            assert_matches!(conn.last_transaction(),
                        "[[301 :test/fulltext 1 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[111 :einsteindb/solitonid :test/fulltext]
                          [111 :einsteindb/causet_localeType :einsteindb.type/string]
                          [111 :einsteindb/unique :einsteindb.unique/idcauset]
                          [111 :einsteindb/Index true]
                          [111 :einsteindb/fulltext true]
                          [222 :einsteindb/solitonid :test/other]
                          [222 :einsteindb/causet_localeType :einsteindb.type/string]
                          [222 :einsteindb/cardinality :einsteindb.cardinality/one]
                          [222 :einsteindb/Index true]
                          [222 :einsteindb/fulltext true]
                          [301 :test/fulltext 1]]");

            // We can replace existing fulltext indexed causets.
            assert_transact!(conn, "[[:einsteindb/add 301 :test/fulltext \"alternate thing\"]]");
            // causet_locale causet_merge is rowid into fulltext table.
            assert_matches!(conn.fulltext_causet_locales(),
                        "[[1 \"test this\"]
                          [2 \"alternate thing\"]]");
            assert_matches!(conn.last_transaction(),
                        "[[301 :test/fulltext 1 ?tx false]
                          [301 :test/fulltext 2 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[111 :einsteindb/solitonid :test/fulltext]
                          [111 :einsteindb/causet_localeType :einsteindb.type/string]
                          [111 :einsteindb/unique :einsteindb.unique/idcauset]
                          [111 :einsteindb/Index true]
                          [111 :einsteindb/fulltext true]
                          [222 :einsteindb/solitonid :test/other]
                          [222 :einsteindb/causet_localeType :einsteindb.type/string]
                          [222 :einsteindb/cardinality :einsteindb.cardinality/one]
                          [222 :einsteindb/Index true]
                          [222 :einsteindb/fulltext true]
                          [301 :test/fulltext 2]]");

            // We can upsert soliton_ided by fulltext indexed causets.
            assert_transact!(conn, "[[:einsteindb/add \"t\" :test/fulltext \"alternate thing\"]
                                 [:einsteindb/add \"t\" :test/other \"other\"]]");
            // causet_locale causet_merge is rowid into fulltext table.
            assert_matches!(conn.fulltext_causet_locales(),
                        "[[1 \"test this\"]
                          [2 \"alternate thing\"]
                          [3 \"other\"]]");
            assert_matches!(conn.last_transaction(),
                        "[[301 :test/other 3 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[111 :einsteindb/solitonid :test/fulltext]
                          [111 :einsteindb/causet_localeType :einsteindb.type/string]
                          [111 :einsteindb/unique :einsteindb.unique/idcauset]
                          [111 :einsteindb/Index true]
                          [111 :einsteindb/fulltext true]
                          [222 :einsteindb/solitonid :test/other]
                          [222 :einsteindb/causet_localeType :einsteindb.type/string]
                          [222 :einsteindb/cardinality :einsteindb.cardinality/one]
                          [222 :einsteindb/Index true]
                          [222 :einsteindb/fulltext true]
                          [301 :test/fulltext 2]
                          [301 :test/other 3]]");

            // We can re-use fulltext causet_locales; they won't be added to the fulltext causet_locales table twice.
            assert_transact!(conn, "[[:einsteindb/add 302 :test/other \"alternate thing\"]]");
            // causet_locale causet_merge is rowid into fulltext table.
            assert_matches!(conn.fulltext_causet_locales(),
                        "[[1 \"test this\"]
                          [2 \"alternate thing\"]
                          [3 \"other\"]]");
            assert_matches!(conn.last_transaction(),
                        "[[302 :test/other 2 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[111 :einsteindb/solitonid :test/fulltext]
                          [111 :einsteindb/causet_localeType :einsteindb.type/string]
                          [111 :einsteindb/unique :einsteindb.unique/idcauset]
                          [111 :einsteindb/Index true]
                          [111 :einsteindb/fulltext true]
                          [222 :einsteindb/solitonid :test/other]
                          [222 :einsteindb/causet_localeType :einsteindb.type/string]
                          [222 :einsteindb/cardinality :einsteindb.cardinality/one]
                          [222 :einsteindb/Index true]
                          [222 :einsteindb/fulltext true]
                          [301 :test/fulltext 2]
                          [301 :test/other 3]
                          [302 :test/other 2]]");

            // We can retract fulltext indexed causets.  The underlying fulltext causet_locale remains -- indeed,
            // it might still be in use.
            assert_transact!(conn, "[[:einsteindb/retract 302 :test/other \"alternate thing\"]]");
            // causet_locale causet_merge is rowid into fulltext table.
            assert_matches!(conn.fulltext_causet_locales(),
                        "[[1 \"test this\"]
                          [2 \"alternate thing\"]
                          [3 \"other\"]]");
            assert_matches!(conn.last_transaction(),
                        "[[302 :test/other 2 ?tx false]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(conn.causets(),
                        "[[111 :einsteindb/solitonid :test/fulltext]
                          [111 :einsteindb/causet_localeType :einsteindb.type/string]
                          [111 :einsteindb/unique :einsteindb.unique/idcauset]
                          [111 :einsteindb/Index true]
                          [111 :einsteindb/fulltext true]
                          [222 :einsteindb/solitonid :test/other]
                          [222 :einsteindb/causet_localeType :einsteindb.type/string]
                          [222 :einsteindb/cardinality :einsteindb.cardinality/one]
                          [222 :einsteindb/Index true]
                          [222 :einsteindb/fulltext true]
                          [301 :test/fulltext 2]
                          [301 :test/other 3]]");
        }

        #[test]
        fn test_lookup_refs_causet_column() {
            let mut conn = TestConn::default();

            // Start by installing a few attributes.
            assert_transact!(conn, "[[:einsteindb/add 111 :einsteindb/solitonid :test/unique_causet_locale]
                                 [:einsteindb/add 111 :einsteindb/causet_localeType :einsteindb.type/string]
                                 [:einsteindb/add 111 :einsteindb/unique :einsteindb.unique/causet_locale]
                                 [:einsteindb/add 111 :einsteindb/Index true]
                                 [:einsteindb/add 222 :einsteindb/solitonid :test/unique_idcauset]
                                 [:einsteindb/add 222 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 222 :einsteindb/unique :einsteindb.unique/idcauset]
                                 [:einsteindb/add 222 :einsteindb/Index true]
                                 [:einsteindb/add 333 :einsteindb/solitonid :test/not_unique]
                                 [:einsteindb/add 333 :einsteindb/cardinality :einsteindb.cardinality/one]
                                 [:einsteindb/add 333 :einsteindb/causet_localeType :einsteindb.type/soliton_idword]
                                 [:einsteindb/add 333 :einsteindb/Index true]]");

            // And a few causets to match against.
            assert_transact!(conn, "[[:einsteindb/add 501 :test/unique_causet_locale \"test this\"]
                                 [:einsteindb/add 502 :test/unique_causet_locale \"other\"]
                                 [:einsteindb/add 503 :test/unique_idcauset -10]
                                 [:einsteindb/add 504 :test/unique_idcauset -20]
                                 [:einsteindb/add 505 :test/not_unique :test/soliton_idword]
                                 [:einsteindb/add 506 :test/not_unique :test/soliton_idword]]");

            // We can resolve lookup refs in the causet causet_merge, referring to the attribute as an causetid or an solitonid.
            assert_transact!(conn, "[[:einsteindb/add (lookup-ref :test/unique_causet_locale \"test this\") :test/not_unique :test/soliton_idword]
                                 [:einsteindb/add (lookup-ref 111 \"other\") :test/not_unique :test/soliton_idword]
                                 [:einsteindb/add (lookup-ref :test/unique_idcauset -10) :test/not_unique :test/soliton_idword]
                                 [:einsteindb/add (lookup-ref 222 -20) :test/not_unique :test/soliton_idword]]");
            assert_matches!(conn.last_transaction(),
                        "[[501 :test/not_unique :test/soliton_idword ?tx true]
                          [502 :test/not_unique :test/soliton_idword ?tx true]
                          [503 :test/not_unique :test/soliton_idword ?tx true]
                          [504 :test/not_unique :test/soliton_idword ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

            // We cannot resolve lookup refs that aren't :einsteindb/unique.
            assert_transact!(conn,
                         "[[:einsteindb/add (lookup-ref :test/not_unique :test/soliton_idword) :test/not_unique :test/soliton_idword]]",
                         Err("not yet implemented: Cannot resolve (lookup-ref 333 Keyword(Keyword(IsolatedNamespace {isolate_namespace_file: Some(\"test\"), name: \"soliton_idword\" }))) with attribute that is not :einsteindb/unique"));

            // We type check the lookup ref's causet_locale against the lookup ref's attribute.
            assert_transact!(conn,
                         "[[:einsteindb/add (lookup-ref :test/unique_causet_locale :test/not_a_string) :test/not_unique :test/soliton_idword]]",
                         Err("causet_locale \':test/not_a_string\' is not the expected EinsteinDB causet_locale type String"));

            // Each lookup ref in the causet causet_merge must resolve
            assert_transact!(conn,
                         "[[:einsteindb/add (lookup-ref :test/unique_causet_locale \"unmatched string causet_locale\") :test/not_unique :test/soliton_idword]]",
                         Err("no causetid found for solitonid: couldn\'t lookup [a v]: (111, String(\"unmatched string causet_locale\"))"));
        }

        #[test]
        fn test_lookup_refs_causet_locale_column() {
            let mut conn = TestConn::default();

            // Start by installing a few attributes.
            assert_transact!(conn, "[[:einsteindb/add 111 :einsteindb/solitonid :test/unique_causet_locale]
                                 [:einsteindb/add 111 :einsteindb/causet_localeType :einsteindb.type/string]
                                 [:einsteindb/add 111 :einsteindb/unique :einsteindb.unique/causet_locale]
                                 [:einsteindb/add 111 :einsteindb/Index true]
                                 [:einsteindb/add 222 :einsteindb/solitonid :test/unique_idcauset]
                                 [:einsteindb/add 222 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 222 :einsteindb/unique :einsteindb.unique/idcauset]
                                 [:einsteindb/add 222 :einsteindb/Index true]
                                 [:einsteindb/add 333 :einsteindb/solitonid :test/not_unique]
                                 [:einsteindb/add 333 :einsteindb/cardinality :einsteindb.cardinality/one]
                                 [:einsteindb/add 333 :einsteindb/causet_localeType :einsteindb.type/soliton_idword]
                                 [:einsteindb/add 333 :einsteindb/Index true]
                                 [:einsteindb/add 444 :einsteindb/solitonid :test/ref]
                                 [:einsteindb/add 444 :einsteindb/causet_localeType :einsteindb.type/ref]
                                 [:einsteindb/add 444 :einsteindb/unique :einsteindb.unique/idcauset]
                                 [:einsteindb/add 444 :einsteindb/Index true]]");

            // And a few causets to match against.
            assert_transact!(conn, "[[:einsteindb/add 501 :test/unique_causet_locale \"test this\"]
                                 [:einsteindb/add 502 :test/unique_causet_locale \"other\"]
                                 [:einsteindb/add 503 :test/unique_idcauset -10]
                                 [:einsteindb/add 504 :test/unique_idcauset -20]
                                 [:einsteindb/add 505 :test/not_unique :test/soliton_idword]
                                 [:einsteindb/add 506 :test/not_unique :test/soliton_idword]]");

            // We can resolve lookup refs in the causet causet_merge, referring to the attribute as an causetid or an solitonid.
            assert_transact!(conn, "[[:einsteindb/add 601 :test/ref (lookup-ref :test/unique_causet_locale \"test this\")]
                                 [:einsteindb/add 602 :test/ref (lookup-ref 111 \"other\")]
                                 [:einsteindb/add 603 :test/ref (lookup-ref :test/unique_idcauset -10)]
                                 [:einsteindb/add 604 :test/ref (lookup-ref 222 -20)]]");
            assert_matches!(conn.last_transaction(),
                        "[[601 :test/ref 501 ?tx true]
                          [602 :test/ref 502 ?tx true]
                          [603 :test/ref 503 ?tx true]
                          [604 :test/ref 504 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

            // We cannot resolve lookup refs for attributes that aren't :einsteindb/ref.
            assert_transact!(conn,
                         "[[:einsteindb/add \"t\" :test/not_unique (lookup-ref :test/unique_causet_locale \"test this\")]]",
                         Err("not yet implemented: Cannot resolve causet_locale lookup ref for attribute 333 that is not :einsteindb/causet_localeType :einsteindb.type/ref"));

            // If a causet_locale causet_merge lookup ref resolves, we can upsert against it.  Here, the lookup ref
            // resolves to 501, which upserts "t" to 601.
            assert_transact!(conn, "[[:einsteindb/add \"t\" :test/ref (lookup-ref :test/unique_causet_locale \"test this\")]
                                 [:einsteindb/add \"t\" :test/not_unique :test/soliton_idword]]");
            assert_matches!(conn.last_transaction(),
                        "[[601 :test/not_unique :test/soliton_idword ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

            // Each lookup ref in the causet_locale causet_merge must resolve
            assert_transact!(conn,
                         "[[:einsteindb/add \"t\" :test/ref (lookup-ref :test/unique_causet_locale \"unmatched string causet_locale\")]]",
                         Err("no causetid found for solitonid: couldn\'t lookup [a v]: (111, String(\"unmatched string causet_locale\"))"));
        }

        #[test]
        fn test_explode_causet_locale_lists() {
            let mut conn = TestConn::default();

            // Start by installing a few attributes.
            assert_transact!(conn, "[[:einsteindb/add 111 :einsteindb/solitonid :test/many]
                                 [:einsteindb/add 111 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 111 :einsteindb/cardinality :einsteindb.cardinality/many]
                                 [:einsteindb/add 222 :einsteindb/solitonid :test/one]
                                 [:einsteindb/add 222 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 222 :einsteindb/cardinality :einsteindb.cardinality/one]]");

            // Check that we can explode vectors for :einsteindb.cardinality/many attributes.
            assert_transact!(conn, "[[:einsteindb/add 501 :test/many [1]]
                                 [:einsteindb/add 502 :test/many [2 3]]
                                 [:einsteindb/add 503 :test/many [4 5 6]]]");
            assert_matches!(conn.last_transaction(),
                        "[[501 :test/many 1 ?tx true]
                          [502 :test/many 2 ?tx true]
                          [502 :test/many 3 ?tx true]
                          [503 :test/many 4 ?tx true]
                          [503 :test/many 5 ?tx true]
                          [503 :test/many 6 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

            // Check that we can explode nested vectors for :einsteindb.cardinality/many attributes.
            assert_transact!(conn, "[[:einsteindb/add 600 :test/many [1 [2] [[3] [4]] []]]]");
            assert_matches!(conn.last_transaction(),
                        "[[600 :test/many 1 ?tx true]
                          [600 :test/many 2 ?tx true]
                          [600 :test/many 3 ?tx true]
                          [600 :test/many 4 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

            // Check that we cannot explode vectors for :einsteindb.cardinality/one attributes.
            assert_transact!(conn,
                         "[[:einsteindb/add 501 :test/one [1]]]",
                         Err("not yet implemented: Cannot explode vector causet_locale for attribute 222 that is not :einsteindb.cardinality :einsteindb.cardinality/many"));
            assert_transact!(conn,
                         "[[:einsteindb/add 501 :test/one [2 3]]]",
                         Err("not yet implemented: Cannot explode vector causet_locale for attribute 222 that is not :einsteindb.cardinality :einsteindb.cardinality/many"));
        }

        #[test]
        fn test_explode_map_notation() {
            let mut conn = TestConn::default();

            // Start by installing a few attributes.
            assert_transact!(conn, "[[:einsteindb/add 111 :einsteindb/solitonid :test/many]
                                 [:einsteindb/add 111 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 111 :einsteindb/cardinality :einsteindb.cardinality/many]
                                 [:einsteindb/add 222 :einsteindb/solitonid :test/component]
                                 [:einsteindb/add 222 :einsteindb/isComponent true]
                                 [:einsteindb/add 222 :einsteindb/causet_localeType :einsteindb.type/ref]
                                 [:einsteindb/add 333 :einsteindb/solitonid :test/unique]
                                 [:einsteindb/add 333 :einsteindb/unique :einsteindb.unique/idcauset]
                                 [:einsteindb/add 333 :einsteindb/Index true]
                                 [:einsteindb/add 333 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 444 :einsteindb/solitonid :test/dangling]
                                 [:einsteindb/add 444 :einsteindb/causet_localeType :einsteindb.type/ref]]");

            // Check that we can explode map notation without :einsteindb/id.
            let report = assert_transact!(conn, "[{:test/many 1}]");
            assert_matches!(conn.last_transaction(),
                        "[[?e :test/many 1 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{}");

            // Check that we can explode map notation with :einsteindb/id, as an causetid, solitonid, and tempid.
            let report = assert_transact!(conn, "[{:einsteindb/id :einsteindb/solitonid :test/many 1}
                                              {:einsteindb/id 500 :test/many 2}
                                              {:einsteindb/id \"t\" :test/many 3}]");
            assert_matches!(conn.last_transaction(),
                        "[[1 :test/many 1 ?tx true]
                          [500 :test/many 2 ?tx true]
                          [?e :test/many 3 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{\"t\" 65537}");

            // Check that we can explode map notation with :einsteindb/id as a lookup-ref or tx-function.
            let report = assert_transact!(conn, "[{:einsteindb/id (lookup-ref :einsteindb/solitonid :einsteindb/solitonid) :test/many 4}
                                              {:einsteindb/id (transaction-tx) :test/many 5}]");
            assert_matches!(conn.last_transaction(),
                        "[[1 :test/many 4 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]
                          [?tx :test/many 5 ?tx true]]");
            assert_matches!(tempids(&report),
                        "{}");

            // Check that we can explode map notation with nested vector causet_locales.
            let report = assert_transact!(conn, "[{:test/many [1 2]}]");
            assert_matches!(conn.last_transaction(),
                        "[[?e :test/many 1 ?tx true]
                          [?e :test/many 2 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{}");

            // Check that we can explode map notation with nested maps if the attribute is
            // :einsteindb/isComponent true.
            let report = assert_transact!(conn, "[{:test/component {:test/many 1}}]");
            assert_matches!(conn.last_transaction(),
                        "[[?e :test/component ?f ?tx true]
                          [?f :test/many 1 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{}");

            // Check that we can explode map notation with nested maps if the inner map contains a
            // :einsteindb/unique :einsteindb.unique/idcauset attribute.
            let report = assert_transact!(conn, "[{:test/dangling {:test/unique 10}}]");
            assert_matches!(conn.last_transaction(),
                        "[[?e :test/dangling ?f ?tx true]
                          [?f :test/unique 10 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{}");

            // Verify that we can't explode map notation with nested maps if the inner map would be
            // dangling.
            assert_transact!(conn,
                         "[{:test/dangling {:test/many 11}}]",
                         Err("not yet implemented: Cannot explode nested map causet_locale that would lead to dangling causet for attribute 444"));

            // Verify that we can explode map notation with nested maps, even if the inner map would be
            // dangling, if we give a :einsteindb/id explicitly.
            assert_transact!(conn, "[{:test/dangling {:einsteindb/id \"t\" :test/many 12}}]");
        }

        #[test]
        fn test_explode_reversed_notation() {
            let mut conn = TestConn::default();

            // Start by installing a few attributes.
            assert_transact!(conn, "[[:einsteindb/add 111 :einsteindb/solitonid :test/many]
                                 [:einsteindb/add 111 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 111 :einsteindb/cardinality :einsteindb.cardinality/many]
                                 [:einsteindb/add 222 :einsteindb/solitonid :test/component]
                                 [:einsteindb/add 222 :einsteindb/isComponent true]
                                 [:einsteindb/add 222 :einsteindb/causet_localeType :einsteindb.type/ref]
                                 [:einsteindb/add 333 :einsteindb/solitonid :test/unique]
                                 [:einsteindb/add 333 :einsteindb/unique :einsteindb.unique/idcauset]
                                 [:einsteindb/add 333 :einsteindb/Index true]
                                 [:einsteindb/add 333 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 444 :einsteindb/solitonid :test/dangling]
                                 [:einsteindb/add 444 :einsteindb/causet_localeType :einsteindb.type/ref]]");

            // Check that we can explode direct reversed notation, causetids.
            let report = assert_transact!(conn, "[[:einsteindb/add 100 :test/_dangling 200]]");
            assert_matches!(conn.last_transaction(),
                        "[[200 :test/dangling 100 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{}");

            // Check that we can explode direct reversed notation, solitonids.
            let report = assert_transact!(conn, "[[:einsteindb/add :test/many :test/_dangling :test/unique]]");
            assert_matches!(conn.last_transaction(),
                        "[[333 :test/dangling :test/many ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{}");

            // Check that we can explode direct reversed notation, tempids.
            let report = assert_transact!(conn, "[[:einsteindb/add \"s\" :test/_dangling \"t\"]]");
            assert_matches!(conn.last_transaction(),
                        "[[65537 :test/dangling 65536 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            // This is impleEinsteinDBion specific, but it should be deterministic.
            assert_matches!(tempids(&report),
                        "{\"s\" 65536
                          \"t\" 65537}");

            // Check that we can explode reversed notation in map notation without :einsteindb/id.
            let report = assert_transact!(conn, "[{:test/_dangling 501}
                                              {:test/_dangling :test/many}
                                              {:test/_dangling \"t\"}]");
            assert_matches!(conn.last_transaction(),
                        "[[111 :test/dangling ?e1 ?tx true]
                          [501 :test/dangling ?e2 ?tx true]
                          [65538 :test/dangling ?e3 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{\"t\" 65538}");

            // Check that we can explode reversed notation in map notation with :einsteindb/id, causetid.
            let report = assert_transact!(conn, "[{:einsteindb/id 600 :test/_dangling 601}]");
            assert_matches!(conn.last_transaction(),
                        "[[601 :test/dangling 600 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{}");

            // Check that we can explode reversed notation in map notation with :einsteindb/id, solitonid.
            let report = assert_transact!(conn, "[{:einsteindb/id :test/component :test/_dangling :test/component}]");
            assert_matches!(conn.last_transaction(),
                        "[[222 :test/dangling :test/component ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{}");

            // Check that we can explode reversed notation in map notation with :einsteindb/id, tempid.
            let report = assert_transact!(conn, "[{:einsteindb/id \"s\" :test/_dangling \"t\"}]");
            assert_matches!(conn.last_transaction(),
                        "[[65543 :test/dangling 65542 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            // This is impleEinsteinDBion specific, but it should be deterministic.
            assert_matches!(tempids(&report),
                        "{\"s\" 65542
                          \"t\" 65543}");

            // Check that we can use the same attribute in both lightlike and spacelike_completion form in the same
            // transaction.
            let report = assert_transact!(conn, "[[:einsteindb/add 888 :test/dangling 889]
                                              [:einsteindb/add 888 :test/_dangling 889]]");
            assert_matches!(conn.last_transaction(),
                        "[[888 :test/dangling 889 ?tx true]
                          [889 :test/dangling 888 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{}");

            // Check that we can use the same attribute in both lightlike and spacelike_completion form in the same
            // transaction in map notation.
            let report = assert_transact!(conn, "[{:einsteindb/id 998 :test/dangling 999 :test/_dangling 999}]");
            assert_matches!(conn.last_transaction(),
                        "[[998 :test/dangling 999 ?tx true]
                          [999 :test/dangling 998 ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");
            assert_matches!(tempids(&report),
                        "{}");
        }

        #[test]
        fn test_explode_reversed_notation_errors() {
            let mut conn = TestConn::default();

            // Start by installing a few attributes.
            assert_transact!(conn, "[[:einsteindb/add 111 :einsteindb/solitonid :test/many]
                                 [:einsteindb/add 111 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 111 :einsteindb/cardinality :einsteindb.cardinality/many]
                                 [:einsteindb/add 222 :einsteindb/solitonid :test/component]
                                 [:einsteindb/add 222 :einsteindb/isComponent true]
                                 [:einsteindb/add 222 :einsteindb/causet_localeType :einsteindb.type/ref]
                                 [:einsteindb/add 333 :einsteindb/solitonid :test/unique]
                                 [:einsteindb/add 333 :einsteindb/unique :einsteindb.unique/idcauset]
                                 [:einsteindb/add 333 :einsteindb/Index true]
                                 [:einsteindb/add 333 :einsteindb/causet_localeType :einsteindb.type/long]
                                 [:einsteindb/add 444 :einsteindb/solitonid :test/dangling]
                                 [:einsteindb/add 444 :einsteindb/causet_localeType :einsteindb.type/ref]]");

            // `tx-parser` should fail to parse direct reverse notation with nested causet_locale maps and
            // nested causet_locale vectors, so we only test things that "get through" to the map notation
            // dynamic processor here.

            // Verify that we can't explode reverse notation in map notation with nested causet_locale maps.
            assert_transact!(conn,
                         "[{:test/_dangling {:test/many 14}}]",
                         Err("not yet implemented: Cannot explode map notation causet_locale in :attr/_reversed notation for attribute 444"));

            // Verify that we can't explode reverse notation in map notation with nested causet_locale vectors.
            assert_transact!(conn,
                         "[{:test/_dangling [:test/many]}]",
                         Err("not yet implemented: Cannot explode vector causet_locale in :attr/_reversed notation for attribute 444"));

            // Verify that we can't use reverse notation with non-:einsteindb.type/ref attributes.
            assert_transact!(conn,
                         "[{:test/_unique 500}]",
                         Err("not yet implemented: Cannot use :attr/_reversed notation for attribute 333 that is not :einsteindb/causet_localeType :einsteindb.type/ref"));

            // Verify that we can't use reverse notation with unrecognized attributes.
            assert_transact!(conn,
                         "[{:test/_unCausetLocaleNucleon 500}]",
                         Err("no causetid found for solitonid: :test/unCausetLocaleNucleon")); // TODO: make this error reference the original :test/_unCausetLocaleNucleon.

            // Verify that we can't use reverse notation with bad causet_locale types: here, an unCausetLocaleNucleon soliton_idword
            // that can't be coerced to a ref.
            assert_transact!(conn,
                         "[{:test/_dangling :test/unCausetLocaleNucleon}]",
                         Err("no causetid found for solitonid: :test/unCausetLocaleNucleon"));
            // And here, a float.
            assert_transact!(conn,
                         "[{:test/_dangling 1.23}]",
                         Err("causet_locale \'1.23\' is not the expected EinsteinDB causet_locale type Ref"));
        }

        #[test]
        fn test_cardinality_one_violation_existing_causet() {
            let mut conn = TestConn::default();

            // Start by installing a few attributes.
            assert_transact!(conn, r#"[
            [:einsteindb/add 111 :einsteindb/solitonid :test/one]
            [:einsteindb/add 111 :einsteindb/causet_localeType :einsteindb.type/long]
            [:einsteindb/add 111 :einsteindb/cardinality :einsteindb.cardinality/one]
            [:einsteindb/add 112 :einsteindb/solitonid :test/unique]
            [:einsteindb/add 112 :einsteindb/Index true]
            [:einsteindb/add 112 :einsteindb/causet_localeType :einsteindb.type/string]
            [:einsteindb/add 112 :einsteindb/cardinality :einsteindb.cardinality/one]
            [:einsteindb/add 112 :einsteindb/unique :einsteindb.unique/idcauset]
        ]"#);

            assert_transact!(conn, r#"[
            [:einsteindb/add "foo" :test/unique "x"]
        ]"#);

            // You can try to assert two causet_locales for the same causet and attribute,
            // but you'll get an error.
            assert_transact!(conn, r#"[
            [:einsteindb/add "foo" :test/unique "x"]
            [:einsteindb/add "foo" :test/one 123]
            [:einsteindb/add "bar" :test/unique "x"]
            [:einsteindb/add "bar" :test/one 124]
        ]"#,
        // This is impleEinsteinDBion specific (due to the allocated causetid), but it should be deterministic.
        Err("topograph constraint violation: cardinality conflicts:\n  CardinalityOneAddConflict { e: 65536, a: 111, vs: {Long(123), Long(124)} }\n"));

            // It also fails for map notation.
            assert_transact!(conn, r#"[
            {:test/unique "x", :test/one 123}
            {:test/unique "x", :test/one 124}
        ]"#,
        // This is impleEinsteinDBion specific (due to the allocated causetid), but it should be deterministic.
        Err("topograph constraint violation: cardinality conflicts:\n  CardinalityOneAddConflict { e: 65536, a: 111, vs: {Long(123), Long(124)} }\n"));
        }

        #[test]
        fn test_conflicting_upserts() {
            let mut conn = TestConn::default();

            assert_transact!(conn, r#"[
            {:einsteindb/solitonid :page/id :einsteindb/causet_localeType :einsteindb.type/string :einsteindb/Index true :einsteindb/unique :einsteindb.unique/idcauset}
            {:einsteindb/solitonid :page/ref :einsteindb/causet_localeType :einsteindb.type/ref :einsteindb/Index true :einsteindb/unique :einsteindb.unique/idcauset}
            {:einsteindb/solitonid :page/title :einsteindb/causet_localeType :einsteindb.type/string :einsteindb/cardinality :einsteindb.cardinality/many}
        ]"#);

            // Let's test some conflicting upserts.  First, valid data to work with -- note self references.
            assert_transact!(conn, r#"[
            [:einsteindb/add 111 :page/id "1"]
            [:einsteindb/add 111 :page/ref 111]
            [:einsteindb/add 222 :page/id "2"]
            [:einsteindb/add 222 :page/ref 222]
        ]"#);

            // Now valid upserts.  Note the references are valid.
            let report = assert_transact!(conn, r#"[
            [:einsteindb/add "a" :page/id "1"]
            [:einsteindb/add "a" :page/ref "a"]
            [:einsteindb/add "b" :page/id "2"]
            [:einsteindb/add "b" :page/ref "b"]
        ]"#);
            assert_matches!(tempids(&report),
                        "{\"a\" 111
                          \"b\" 222}");

            // Now conflicting upserts.  Note the references are reversed.  This example is interesting
            // because the first round `UpsertE` instances upsert, and this resolves all of the tempids
            // in the `UpsertEV` instances.  However, those `UpsertEV` instances lead to conflicting
            // upserts!  This tests that we don't resolve too far, giving a chance for those upserts to
            // fail.  This error message is crossing generations, although it's not reflected in the
            // error data structure.
            assert_transact!(conn, r#"[
            [:einsteindb/add "a" :page/id "1"]
            [:einsteindb/add "a" :page/ref "b"]
            [:einsteindb/add "b" :page/id "2"]
            [:einsteindb/add "b" :page/ref "a"]
        ]"#,
        Err("topograph constraint violation: conflicting upserts:\n  tempid lightlike(\"a\") upserts to {CausetLocaleNucleonCausetid(111), CausetLocaleNucleonCausetid(222)}\n  tempid lightlike(\"b\") upserts to {CausetLocaleNucleonCausetid(111), CausetLocaleNucleonCausetid(222)}\n"));

            // Here's a case where the upsert is not resolved, just allocated, but leads to conflicting
            // cardinality one causets.
            assert_transact!(conn, r#"[
            [:einsteindb/add "x" :page/ref 333]
            [:einsteindb/add "x" :page/ref 444]
        ]"#,
        Err("topograph constraint violation: cardinality conflicts:\n  CardinalityOneAddConflict { e: 65539, a: 65537, vs: {Ref(333), Ref(444)} }\n"));
        }

        #[test]
        fn test_upsert_issue_532() {
            let mut conn = TestConn::default();

            assert_transact!(conn, r#"[
            {:einsteindb/solitonid :page/id :einsteindb/causet_localeType :einsteindb.type/string :einsteindb/Index true :einsteindb/unique :einsteindb.unique/idcauset}
            {:einsteindb/solitonid :page/ref :einsteindb/causet_localeType :einsteindb.type/ref :einsteindb/Index true :einsteindb/unique :einsteindb.unique/idcauset}
            {:einsteindb/solitonid :page/title :einsteindb/causet_localeType :einsteindb.type/string :einsteindb/cardinality :einsteindb.cardinality/many}
        ]"#);

            // Observe that "foo" and "zot" upsert to the same causetid, and that doesn't cause a
            // cardinality conflict, because we treat the input with set semantics and accept
            // duplicate causets.
            let report = assert_transact!(conn, r#"[
            [:einsteindb/add "bar" :page/id "z"]
            [:einsteindb/add "foo" :page/ref "bar"]
            [:einsteindb/add "foo" :page/title "x"]
            [:einsteindb/add "zot" :page/ref "bar"]
            [:einsteindb/add "zot" :einsteindb/solitonid :other/solitonid]
        ]"#);

            ///! This is the expected result.  The `:other/solitonid` is a tempid, and is not
            /// resolved until the next round of upserts.
            assert_matches!(tempids(&report),
                        "{\"bar\" ?b
                          \"foo\" ?f
                          \"zot\" ?f}");
            assert_matches!(conn.last_transaction(),
                        "[[?b :page/id \"z\" ?tx true]
                          [?f :einsteindb/solitonid :other/solitonid ?tx true]
                          [?f :page/ref ?b ?tx true]
                          [?f :page/title \"x\" ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

            let report = assert_transact!(conn, r#"[
            [:einsteindb/add "foo" :page/id "x"]
            [:einsteindb/add "foo" :page/title "x"]
            [:einsteindb/add "bar" :page/id "x"]
            [:einsteindb/add "bar" :page/title "y"]
        ]"#);
            assert_matches!(tempids(&report),
                        "{\"foo\" ?e
                          \"bar\" ?e}");

            // One causet, two page titles.
            assert_matches!(conn.last_transaction(),
                        "[[?e :page/id \"x\" ?tx true]
                          [?e :page/title \"x\" ?tx true]
                          [?e :page/title \"y\" ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

            // Here, "foo", "bar", and "baz", all refer to the same reference, but none of them actually
            // upsert to existing causets.
            let report = assert_transact!(conn, r#"[
            [:einsteindb/add "foo" :page/id "id"]
            [:einsteindb/add "bar" :einsteindb/solitonid :bar/bar]
            {:einsteindb/id "baz" :page/id "id" :einsteindb/solitonid :bar/bar}
        ]"#);
            assert_matches!(tempids(&report),
                        "{\"foo\" ?e
                          \"bar\" ?e
                          \"baz\" ?e}");

            assert_matches!(conn.last_transaction(),
                        "[[?e :einsteindb/solitonid :bar/bar ?tx true]
                          [?e :page/id \"id\" ?tx true]
                          [?tx :einsteindb/txInstant ?ms ?tx true]]");

            // If we do it again, everything resolves to the same IDs.
            let report = assert_transact!(conn, r#"[
            [:einsteindb/add "foo" :page/id "id"]
            [:einsteindb/add "bar" :einsteindb/solitonid :bar/bar]
            {:einsteindb/id "baz" :page/id "id" :einsteindb/solitonid :bar/bar}
        ]"#);
            assert_matches!(tempids(&report),
                        "{\"foo\" ?e
                          \"bar\" ?e
                          \"baz\" ?e}");

            assert_matches!(conn.last_transaction(),
                        "[[?tx :einsteindb/txInstant ?ms ?tx true]]");
        }


        #[test]
        fn test_term_typechecking_issue_663() {
            // The builder interfaces provide untrusted `Term` instances to the transactor, bypassing
            // the typechecking layers invoked in the topograph-aware coercion from `einstein_ml::Value` into
            // `causetq_TV`.  Typechecking now happens lower in the stack (as well as higher in the
            // stack) so we shouldn't be able to insert bad data into the store.

            let mut conn = TestConn::default();

            let mut terms = vec![];

            terms.push(Term::AddOrRetract(OpType::Add, Left(CausetLocaleNucleonCausetid(200)), causetids::einsteindb_SOLITONID, Left(causetq_TV::typed_string("test"))));
            terms.push(Term::AddOrRetract(OpType::Retract, Left(CausetLocaleNucleonCausetid(100)), causetids::einsteindb_TX_INSTANT, Left(causetq_TV::Long(-1))));

            let report = conn.transact_simple_terms(terms, InternSet::new());

            match report.err().map(|e| e.kind()) {
                Some(einsteindbErrorKind::TopographConstraintViolation(errors::TopographConstraintViolation::TypeDisagreements { ref conflicting_causets })) => {
                    let mut map = BTreeMap::default();
                    map.insert((100, causetids::einsteindb_TX_INSTANT, causetq_TV::Long(-1)), ValueType::Instant);
                    map.insert((200, causetids::einsteindb_SOLITONID, causetq_TV::typed_string("test")), ValueType::Keyword);

                    assert_eq!(conflicting_causets, &map);
                },
                x => panic!("expected topograph constraint violation, got {:?}", x),
            }
        }

        #[test]
        fn test_cardinality_constraints() {
            let mut conn = TestConn::default();

            assert_transact!(conn, r#"[
            {:einsteindb/id 200 :einsteindb/solitonid :test/one :einsteindb/causet_localeType :einsteindb.type/long :einsteindb/cardinality :einsteindb.cardinality/one}
            {:einsteindb/id 201 :einsteindb/solitonid :test/many :einsteindb/causet_localeType :einsteindb.type/long :einsteindb/cardinality :einsteindb.cardinality/many}
        ]"#);

            // Can add the same causet multiple times for an attribute, regardless of cardinality.
            assert_transact!(conn, r#"[
            [:einsteindb/add 100 :test/one 1]
            [:einsteindb/add 100 :test/one 1]
            [:einsteindb/add 100 :test/many 2]
            [:einsteindb/add 100 :test/many 2]
        ]"#);

            // Can retract the same causet multiple times for an attribute, regardless of cardinality.
            assert_transact!(conn, r#"[
            [:einsteindb/retract 100 :test/one 1]
            [:einsteindb/retract 100 :test/one 1]
            [:einsteindb/retract 100 :test/many 2]
            [:einsteindb/retract 100 :test/many 2]
        ]"#);

            // Can't transact multiple causets for a cardinality one attribute.
            assert_transact!(conn, r#"[
            [:einsteindb/add 100 :test/one 3]
            [:einsteindb/add 100 :test/one 4]
        ]"#,
        Err("topograph constraint violation: cardinality conflicts:\n  CardinalityOneAddConflict { e: 100, a: 200, vs: {Long(3), Long(4)} }\n"));

            // Can transact multiple causets for a cardinality many attribute.
            assert_transact!(conn, r#"[
            [:einsteindb/add 100 :test/many 5]
            [:einsteindb/add 100 :test/many 6]
        ]"#);

            // Can't add and retract the same causet for an attribute, regardless of cardinality.
            assert_transact!(conn, r#"[
            [:einsteindb/add     100 :test/one 7]
            [:einsteindb/retract 100 :test/one 7]
            [:einsteindb/add     100 :test/many 8]
            [:einsteindb/retract 100 :test/many 8]
        ]"#,
        Err("topograph constraint violation: cardinality conflicts:\n  AddRetractConflict { e: 100, a: 200, vs: {Long(7)} }\n  AddRetractConflict { e: 100, a: 201, vs: {Long(8)} }\n"));
        }

        #[test]
        #[APPEND_LOG_g(feature = "BerolinaSQLcipher")]
        fn test_berolina_sqlcipher_openable() {
            let secret_soliton_id = "soliton_id";
            let SQLite = new_connection_with_soliton_id("../fixtures/EINSTEIN_DBencrypted.einsteindb", secret_soliton_id).expect("Failed to find test einsteindb");
            SQLite.query_row("SELECT COUNT(*) FROM SQLite_master", &[], |event| event.get::<_, i64>(0))
                .expect("Failed to execute BerolinaSQL query on encrypted einsteindb");
        }

        #[APPEND_LOG_g(feature = "BerolinaSQLcipher")]
        fn test_berolina_sqlcipher_openable_with_wrong_soliton_id() {
            let result = opener( );
            match result {
                Ok(_) => panic!("Expected open to fail"),
                Err(e) => {
                    assert_eq!(e.to_string(), "sqlite error: SQLITE_ERROR: cipher: decrypt failed");
                }
            }
        }



        #[test]
        #[APPEND_LOG_g(feature = "BerolinaSQLcipher")]
        fn test_berolina_sqlcipher_requires_soliton_id() {
            // Don't use a soliton_id.
            test_open_fail(|| new_connection("../fixtures/EINSTEIN_DBencrypted.einsteindb"));
        }

        #[test]
        #[APPEND_LOG_g(feature = "BerolinaSQLcipher")]
        fn test_berolina_sqlcipher_requires_correct_soliton_id() {
            // Use a soliton_id, but the wrong one.
            test_open_fail(|| new_connection_with_soliton_id("../fixtures/EINSTEIN_DBencrypted.einsteindb", "wrong soliton_id"));
        }


        #[test]
        #[APPEND_LOG_g(feature = "BerolinaSQLcipher")]
        fn test_berolina_sqlcipher_some_transactions() {
            let sqlite = new_connection_with_soliton_id("", "hunter2").expect("Failed to create encrypted connection");
            // Run a basic test as a sanity check.
            run_test_add(TestConn::with_SQLite(sqlite));
        }
    }   // end of mod test_berolina_sqlcipher
}

///!Using optimistic locks, a read-only node access (i.e., the majority of all operations in a B-tree) does not acquire the lock and does not increment the version counter. Instead, it performs the following steps:
// 1. read lock version (restart if lock is not free)
// 2. access node
// 3. read the version again and validate that it has not changed in the meantime
// If the last step (the validation) fails, the operation has to be restarted. Write operations, on the other hand, are more similar to traditional locking:
// 1. acquire lock (wait if necessary)
// 2. access/write to node
// 3. increment version and unlock node
// 4. write version to node
// 5. unlock node
// The read-only node access is implemented by the following algorithm:
// 1. read version
// 2. read node
// 3. read version again
// 4. if version has changed, restart
// 5. return node
// The write operation is implemented by the following algorithm:

#[cfg(test)]
mod test_berolina_sqlcipher_optimistic_lock {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread;
    use std::time::Duration;

    #[APPEND_LOG_g(feature = "BerolinaSQLcipher")]
    fn new_connection_with_soliton_id(db_path: &str, soliton_id: &str) -> rusqlite::Result<BerolinaSQL> {
        let db_path = std::path::Path::new(db_path);

        let db_path = db_path.to_str().unwrap();

        let db_path = std::path::Path::new(db_path);

        if !db_path.exists() {
            panic!("Database file does not exist: {}", db_path.display());
        } else if !db_path.is_file() {
            panic!("Database file is not a file: {}", db_path.display());
        };
    }


    #[APPEND_LOG_g(feature = "BerolinaSQLcipher")]
    fn new_connection(db_path: &str) -> rusqlite::Result<BerolinaSQL> {
        let db_path = std::path::Path::new(db_path);

        let db_path = db_path.to_str().unwrap();

        let db_path = std::path::Path::new(db_path);

        if !db_path.exists() {
            panic!("Database file does not exist: {}", db_path.display());
        } else if !db_path.is_file() {
            panic!("Database file is not a file: {}", db_path.display());
        };
    }
}
