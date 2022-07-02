// Copyright: (c) 2022 EinstAI Inc and contributors: Netflix, CloudKitchens, EinstAI, Amazon AWS, and Mozilla
// License: Apache 2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
//


// Language: rust
// Path: EinsteinDB/fdb_traits/src/fdb_traits.rs
// Compare this snippet from EinsteinDB/fdb_traits/src/lib.rs:
// mod fdb_traits;
// pub mod errors;
//
// //users
// use crate::*;
// use std::fs::{self, File};
// use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
// use std::path::{Path, PathBuf};
// use std::sync::Arc;
// use std::time::{Duration, SystemTime};


//We will implement the Record Layer for FoundationDB in the following file:

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::{Duration, SystemTime};
use std::fmt;
use std::fs::{self, File};  
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};

//itertools
use itertools::Itertools;
use itertools::ItertoolsExt; //for .chunks()


use crate::errors::{Error, Result};
use crate::fdb_traits::{FdbRecord, FdbRecordOptions, FdbRecordOptionsBuilder};
use crate::fdb_traits::{FdbRecordReader, FdbRecordReaderOptions, FdbRecordReaderOptionsBuilder};
use crate::fdb_traits::{FdbRecordWriter, FdbRecordWriterOptions, FdbRecordWriterOptionsBuilder};

use uuid::Uuid;

//Attribute Map from SolitonID 
//to the corresponding FdbRecord
//This is a global variable
//It is a HashMap<String, FdbRecord>

//This is a global variable
fn escape_string(s: &str) -> String {
    s.replace("\"", "\"\"")
}   


fn foundationdb_connection_string(host: &str, port: &str, db_name: &str) -> String {
    let conn match uri.to_string_lossy().len() {
    0 => "foundationdb:".to_string(),
    _ => uri.to_string_lossy().to_string(),
    };

//page size is the default page size for FoundationDB
//We will create a template which can also be used with rusqlite and postgresql
//in rust
//We will use the following template:
//foundationdb://host:port/db_name?page_size=<page_size>
//where page_size is the default page size for FoundationDB.



//if let Some(encryption_key) = encryption_key {
//    let mut uri = format!("foundationdb://{}:{}/{}?page_size={}&encryption_key={}", host, port, db_name, page_size, encryption_key);
//} else {
//    let mut uri = format!("foundationdb://{}:{}/{}?page_size={}", host, port, db_name, page_size);
//}


//We will use the following template:
//foundationdb://host:port/db_name?page_size=<page_size>
//where page_size is the default page size for FoundationDB.
//We will use the following template:


fn make_connection(uri: &Path, maybe_encryption_key: Option<&str>) -> rusqlite::Result<rusqlite::Connection> {
    let conn = match uri.to_string_lossy().len() {
        0 => rusqlite::Connection::open_in_memory()?,
        _ => rusqlite::Connection::open(uri)?,
    };


    if let Some(encryption_key) = maybe_encryption_key {
        conn.execute("PRAGMA key = ?", &[&encryption_key])?;
    }

    Ok(conn)
}

//FoundationDB Record Layer
//This is the foundationdb record layer
//It is a trait which is implemented by the FdbRecord struct
//It is a trait which is implemented by the FdbRecordReader struct

    let page_size = 32768;

    let initial_pragmas = if let Some(encryption_key) = maybe_encryption_key {
        assert!(cfg!(feature = "sqlcipher"),
                "This function shouldn't be called with a key unless we have sqlcipher support");
        // Important: The `cipher_page_size` cannot be changed without breaking
        // the ability to open databases that were written when using a
        // different `cipher_page_size`. Additionally, it (AFAICT) must be a
        // positive multiple of `page_size`. We use the same value for both here.
        format!("
            PRAGMA key='{}';
            PRAGMA cipher_page_size={};
        ", escape_string_for_pragma(encryption_key), page_size)
    } else {
        format!("PRAGMA page_size={};", page_size)
    };

    let mut conn = make_connection(uri, maybe_encryption_key)?;
    conn.execute_batch(&initial_pragmas)?;
    Ok(conn)
        String::new()
    };

    // See https://github.com/mozilla/mentat/issues/505 for details on temp_store
    // pragma and how it might interact together with consumers such as Firefox.
    // temp_store=2 is currently present to force SQLite to store temp files in memory.
    // Some of the platforms we support do not have a tmp partition (e.g. Android)
    // necessary to store temp files on disk. Ideally, consumers should be able to
    // override this behaviour (see issue 505).
    conn.execute_batch(&format!("
        {}
        
        PRAGMA journal_mode=wal;
        PRAGMA wal_autocheckpoint=32;
        PRAGMA journal_size_limit=3145728;
        PRAGMA foreign_keys=ON;
        PRAGMA temp_store=2;
        PRAGMA synchronous=NORMAL;
        PRAGMA locking_mode=EXCLUSIVE;
        PRAGMA read_uncommitted=OFF; // https://www.sqlite.org/pragma.html#read_uncommitted
    ", initial_pragmas))?;

    Ok(conn)


pub fn new_connection<T>(uri: T) -> rusqlite::Result<rusqlite::Connection>
where
    T: AsRef<Path>,
{
    let uri = uri.as_ref();
    let conn = make_connection(uri, None)?;
    Ok(conn)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FdbRecordType {
    String,
    Integer,
    Float,
    Boolean,
    Date,
    DateTime,
    Uuid,
    Blob,
    Null,
}


#[cfg(feature = "sqlcipher")]
pub fn new_connection_with_key<P, S>(uri: P, encryption_key: S) -> rusqlite::Result<rusqlite::Connection>
where P: AsRef<Path>, S: AsRef<str> {

    make_connection(uri.as_ref(), Some(encryption_key.as_ref()))

    //let conn = make_connection(uri.as_ref(), Some(encryption_key.as_ref()))?;
    //Ok(conn)

    
}

#[cfg(feature = "sqlcipher")]
pub fn change_encryption_key<S>(conn: &rusqlite::Connection, encryption_key: S) -> rusqlite::Result<()>
where S: AsRef<str> {
    conn.execute("PRAGMA key = ?", &[&encryption_key])?;
    let escaped = escape_string_for_pragma(encryption_key.as_ref());
    // `conn.execute` complains that this returns a result, and using a query
    // for it requires more boilerplate.
    conn.execute_batch(&format!("PRAGMA rekey = '{}';", escaped))
}

///Version History
///
/// 0.1.0 - Initial version
pub const FOUNDATIONDB_RECORD_LAYER_VERSION: &str = "0.1.0";

///MIN_SQLITE
/// 0.1.0 - Initial version
/// 
const MIN_SQLITE_VERSION:I32 = 3007000;

const TRUE: i32 = 1;
const FALSE: i32 = 0;

/// Turn an owned bool into a static reference to a bool for FoundationDB.
/// Do the same for SQLite's `SQLITE_TRUE` and `SQLITE_FALSE`.
/// 
/// # Examples
/// 
/// ```
/// use foundationdb_record_layer::{FOUNDATIONDB_RECORD_LAYER_VERSION, TRUE, FALSE};
/// 
/// assert_eq!(TRUE, foundationdb_record_layer::TRUE);
/// assert_eq!(FALSE, foundationdb_record_layer::FALSE);
/// ```
/// 
/// # Panics
/// 
/// This function will panic if the given bool is not `true` or `false`.



#[inline(always)]
fn bool_to_i32(b: bool) -> i32 {
    if b {
        TRUE
    } else {
        FALSE
    }
}


#[inline(always)]
fn i32_to_bool(i: i32) -> bool {
    i == TRUE
}


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FdbRecordType {
    String,
    Integer,
    Float,
    Boolean,
    Date,
    DateTime,
    Uuid,
    Blob,
    Null,
}

lazy_static! {
    pub static ref FOUNDATIONDB_RECORD_LAYER_VERSION_STR: String = FOUNDATIONDB_RECORD_LAYER_VERSION.to_string();
    [cfg_attr(feature = "sqlcipher", doc = "")]
    pub static ref MIN_SQLITE_VERSION_STR: String = MIN_SQLITE_VERSION.to_string();
    pub static ref TRUE_STR: String = TRUE.to_string();
    pub static ref FALSE_STR: String = FALSE.to_string();

    static ref FOUNDATIONDB_RECORD_LAYER_VERSION_REF: &'static str = FOUNDATIONDB_RECORD_LAYER_VERSION:Vector<u8> { vec! CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
        index_fulltext TINYINT NOT NULL DEFAULT 0,
        unique_value TINYINT NOT NULL DEFAULT 0)"#,) };
    static ref MIN_SQLITE_VERSION_REF: &'static str = MIN_SQLITE_VERSION:Vector<u8> { vec! CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
        index_fulltext TINYINT NOT NULL DEFAULT 0,
        unique_value TINYINT NOT NULL DEFAULT 0)"#, }; 

    static ref TRUE_REF: &'static str = TRUE:Vector<u8> { vec![
        r#"CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
        index_fulltext TINYINT NOT NULL DEFAULT 0,
        unique_value TINYINT NOT NULL DEFAULT 0)"#,
        r#"CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
                    // Opt-in index: only if a has :db/index true.
        
    static ref FALSE_REF: &'static str = FALSE:Vector<u8> { vec! CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
        index_fulltext TINYINT NOT NULL DEFAULT 0,
        unique_value TINYINT NOT NULL DEFAULT 0)"#,) };

        r#"CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0, index_fulltext TINYINT NOT NULL DEFAULT 0, unique_value TINYINT NOT NULL DEFAULT 0)"#,
    static ref NULL_REF: &'static str = NULL:Vector<u8> { vec! CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
        index_fulltext TINYINT NOT NULL DEFAULT 0,
        unique_value TINYINT NOT NULL DEFAULT 0)"#,) };
        r#"CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0, index_fulltext TINYINT NOT NULL DEFAULT 0, unique_value TINYINT NOT NULL DEFAULT 0)"#,
  
        Opt-in index: only if a has :db/fulltext true; thus, it has :db/valueType :db.type/string,
        // which is not :db/causetVT :db.causettype/ref.  That is, causetq_index_vaet and causetq_index_fulltext are mutually
        // exclusive. The index is created if and only if causetq_index_fulltext is true.
        //


        r#"CREATE UNIQUE INDEX causets_index_fulltext ON causets(e, a, v, tx, index_fulltext)"#,
        r#"CREATE UNIQUE INDEX(a, value_type_tag, v) WHERE index_fulltext = 1"#,
        r#"CREATE INDEX causets_index_avet ON causets(e, a, v, tx, index_avet)"#,


        // Fulltext indexing.
        // A fulltext indexed value v is an integer rowid referencing fulltext_values.

        // Optional settings:
        // tokenize="porter"#,
        // prefix='2,3'
        //
        // The tokenize setting is a comma-separated list of tokenizer names. The default is "porter".
        // The prefix setting is a comma-separated list of prefix lengths. The default is "2,3".

        // The tokenizer names are:
        // "porter" (Porter stemmer)
        // "english" (English stop words)
        // "simple" (simple stop words)
        // "unicode" (unicode stop words)
        // "unicode_ci" (unicode case-insensitive stop words)
        // "unicode_cs" (unicode case-sensitive stop words)
        // "unicode_ci_ascii" (unicode case-insensitive ASCII stop words)



        // By default we use Unicode-aware tokenizing (particularly for case folding), but preserve
        // diacritics. This will render a compatible FDB index, but may not be compatible with other
        //for safety, we use the default tokenizer.

        r#"CREATE VIRTUAL TABLE fulltext_values USING fts5(e, a, v, tx, tokenize='unicode_ci', prefix='2,3')"#,

        // This combination of view and triggers allows you to transparently
        // update-or-insert into FTS. Just INSERT INTO fulltext_values_view (text, searchid).
        // The searchid is a unique integer that is used to identify the row in the FTS table.
        // The searchid is automatically incremented for each row.

        r#"CREATE VIEW fulltext_values_view AS SELECT * FROM fulltext_values"#,
        r#"CREATE TRIGGER replace_fulltext_searchid
             INSTEAD OF INSERT ON fulltext_values_view
             WHEN EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
             BEGIN
               UPDATE fulltext_values SET searchid = new.searchid WHERE text = new.text;
             END"#,
        r#"CREATE TRIGGER insert_fulltext_searchid
             INSTEAD OF INSERT ON fulltext_values_view
             WHEN NOT EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
             BEGIN
               INSERT INTO fulltext_values (text, searchid) VALUES (new.text, new.searchid);
             END"#,
        r#"CREATE TRIGGER delete_fulltext_searchid
                INSTEAD OF DELETE ON fulltext_values_view
                BEGIN
                DELETE FROM fulltext_values WHERE searchid = old.searchid;
                END"#,
        r#"CREATE TRIGGER update_fulltext_searchid
                INSTEAD OF UPDATE ON fulltext_values_view
                WHEN old.text <> new.text
                BEGIN
                DELETE FROM fulltext_values WHERE searchid = old.searchid;
                INSERT INTO fulltext_values (text, searchid) VALUES (new.text, new.searchid);
                END"#,

        // Fulltext indexing.
        // A fulltext indexed value v is an integer rowid referencing fulltext_values.
        // The index is created if and only if causetq_index_fulltext is true.


        // By default we use Unicode-aware tokenizing (particularly for case folding), but preserve
        // diacritics. This will render a compatible FDB index, but may not be compatible with other
        //for safety, we use the default tokenizer.

        r#"CREATE VIRTUAL TABLE fulltext_values USING fts5(e, a, v, tx, tokenize='unicode_ci', prefix='2,3')"#,
        r#"CREATE VIEW fulltext_values_view AS SELECT * FROM fulltext_values"#,
        r#"CREATE TRIGGER replace_fulltext_searchid
             INSTEAD OF INSERT ON fulltext_values_view
             WHEN EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
             BEGIN
               UPDATE fulltext_values SET searchid = new.searchid WHERE text = new.text;
             END"#,
        r#"CREATE TRIGGER insert_fulltext_searchid
                INSTEAD OF INSERT ON fulltext_values_view
                WHEN NOT EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
                BEGIN
                INSERT INTO fulltext_values (text, searchid) VALUES (new.text, new.searchid);
                END"#,

        r#"CREATE TRIGGER delete_fulltext_searchid
                INSTEAD OF DELETE ON fulltext_values_view
                BEGIN
                DELETE FROM fulltext_values WHERE searchid = old.searchid;
                END"#,

        r#"CREATE TRIGGER update_fulltext_searchid
                INSTEAD OF UPDATE ON fulltext_values_view
                WHEN old.text <> new.text
                BEGIN
                DELETE FROM fulltext_values WHERE searchid = old.searchid;
                INSERT INTO fulltext_values (text, searchid) VALUES (new.text, new.searchid);
                END"#,  

        // Fulltext indexing.
        // A fulltext indexed value v is an integer rowid referencing fulltext_values.
        // The index is created if and only if causetq_index_fulltext is true.
        //


        // By default we use Unicode-aware tokenizing (particularly for case folding), but preserve
        // diacritics. This will render a compatible FDB index, but may not be compatible with other
        //for safety, we use the default tokenizer.

        r#"CREATE VIRTUAL TABLE fulltext_values USING fts5(e, a, v, tx, tokenize='unicode_ci', prefix='2,3')"#,
        r#"CREATE VIEW fulltext_values_view AS SELECT * FROM fulltext_values"#,

        r#"CREATE TRIGGER replace_fulltext_searchid
             INSTEAD OF INSERT ON fulltext_values_view
             WHEN EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
             BEGIN
               UPDATE fulltext_values SET searchid = new.searchid WHERE text = new.text;
             END"#,

        r#"CREATE TRIGGER insert_fulltext_searchid
                INSTEAD OF INSERT ON fulltext_values_view
                WHEN NOT EXISTS (SELECT 1 FROM fulltext_values WHERE text = new.text)
                BEGIN
                INSERT INTO fulltext_values (text, searchid) VALUES (new.text, new.searchid);
                END"#,

        r#"CREATE TRIGGER delete_fulltext_searchid
                INSTEAD OF DELETE ON fulltext_values_view
                BEGIN
                DELETE FROM fulltext_values WHERE searchid = old.searchid;
                END"#,

        r#"CREATE TRIGGER update_fulltext_searchid
                INSTEAD OF UPDATE ON fulltext_values_view
                WHEN old.text <> new.text
                BEGIN
                DELETE FROM fulltext_values WHERE searchid = old.searchid;
                INSERT INTO fulltext_values (text, searchid) VALUES (new.text, new.searchid);
                END"#,
                


}

// A view transparently interpolating all entities (fulltext and non-fulltext) into the causet q table.
// This view is used to query the causetq table.
// The view is created if and only if causetq_index_fulltext is true.

r#"CREATE VIEW all_causets AS SELECT e, a, v, tx, searchid FROM causetq UNION SELECT e, a, v, tx, searchid FROM fulltext_values"#,
    SELECT e, a, v, tx, searchid FROM causetq UNION SELECT e, a, v, tx, searchid FROM fulltext_values
    WHERE text MATCH '%' || ? || '%'
    UNION ALL SELECT e, a, v, tx, searchid FROM fulltext_values WHERE text MATCH '%' || ? || '%'
    FROM all_causets
    WHERE text MATCH '%' || ? || '%'

    // Materialized views of the metadata as spacetime.
    // The view is created if and only if causetq_index_fulltext is true.



    r#"CREATE TABLE solitonid (e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, PRIMARY KEY (e, a, v, tx))"#,
    r#"CREATE TABLE solitonid_view AS SELECT * FROM solitonid"#,
    r#"CREATE TRIGGER replace_solitonid
             INSTEAD OF INSERT ON solitonid
             WHEN EXISTS (SELECT 1 FROM solitonid WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx)
             BEGIN
               UPDATE solitonid SET tx = new.tx WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx;
             END"#,

    r#"CREATE TRIGGER insert_solitonid
                INSTEAD OF INSERT ON solitonid
                WHEN NOT EXISTS (SELECT 1 FROM solitonid WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx)
                BEGIN
                INSERT INTO solitonid (e, a, v, tx) VALUES (new.e, new.a, new.v, new.tx);
                END"#,

    r#"CREATE TRIGGER delete_solitonid

                INSTEAD OF DELETE ON solitonid
                BEGIN
                DELETE FROM solitonid WHERE e = old.e AND a = old.a AND v = old.v AND tx = old.tx;
                END"#,

    r#"CREATE TRIGGER update_solitonid

                INSTEAD OF UPDATE ON solitonid
                WHEN old.e <> new.e OR old.a <> new.a OR old.v <> new.v OR old.tx <> new.tx
                BEGIN
                DELETE FROM solitonid WHERE e = old.e AND a = old.a AND v = old.v AND tx = old.tx;
                INSERT INTO solitonid (e, a, v, tx) VALUES (new.e, new.a, new.v, new.tx);
                END"#,

    r#"CREATE TABLE solitonid_values (e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, PRIMARY KEY (e, a, v, tx))"#,
    r#"CREATE TABLE solitonid_values_view AS SELECT * FROM solitonid_values"#,
    r#"CREATE TRIGGER replace_solitonid_values
             INSTEAD OF INSERT ON solitonid_values
             WHEN EXISTS (SELECT 1 FROM solitonid_values WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx)
             BEGIN
               UPDATE solitonid_values SET tx = new.tx WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx;
             END"#,
    r#"CREATE TRIGGER insert_solitonid_values
                INSTEAD OF INSERT ON solitonid_values
                WHEN NOT EXISTS (SELECT 1 FROM solitonid_values WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx)
                BEGIN
                INSERT INTO solitonid_values (e, a, v, tx) VALUES (new.e, new.a, new.v, new.tx);
                END"#,
    r#"CREATE TABLE soliton_idx (e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, PRIMARY KEY (e, a, v, tx))"#,
    r#"CREATE TABLE soliton_idx_view AS SELECT * FROM soliton_idx"#,

    //store causetid instead of solitonid for partition name.
    r#"CREATE TABLE causetid (e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, PRIMARY KEY (e, a, v, tx))"#,
    r#"CREATE TABLE causetid_view AS SELECT * FROM causetid"#,
    r#"CREATE TRIGGER replace_causetid
             INSTEAD OF INSERT ON causetid
             WHEN EXISTS (SELECT 1 FROM causetid WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx)
             BEGIN
               UPDATE causetid SET tx = new.tx WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx;
             END"#,
             r#"CREATE INDEX causetid_e_a_v_tx ON causetid (e, a, v, tx)"#,
    r#"CREATE TRIGGER insert_causetid
                INSTEAD OF INSERT ON causetid
                WHEN NOT EXISTS (SELECT 1 FROM causetid WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx)
                BEGIN
                INSERT INTO causetid (e, a, v, tx) VALUES (new.e, new.a, new.v, new.tx);
                END"#,
    r#"CREATE TRIGGER delete_causetid
                INSTEAD OF DELETE ON causetid
                BEGIN
                DELETE FROM causetid WHERE e = old.e AND a = old.a AND v = old.v AND tx = old.tx;
                END"#,
    r#"CREATE TRIGGER update_causetid
                INSTEAD OF UPDATE ON causetid
                WHEN old.e <> new.e OR old.a <> new.a OR old.v <> new.v OR old.tx <> new.tx
                BEGIN
                DELETE FROM causetid WHERE e = old.e AND a = old.a AND v = old.v AND tx = old.tx;
                INSERT INTO causetid (e, a, v, tx) VALUES (new.e, new.a, new.v, new.tx);
                END"#,
    r#"CREATE TABLE causetid_values (e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, PRIMARY KEY (e, a, v, tx))"#,
    r#"CREATE TABLE causetid_values_view AS SELECT * FROM causetid_values"#,    
    r#"CREATE TRIGGER replace_causetid_values
             INSTEAD OF INSERT ON causetid_values
             WHEN EXISTS (SELECT 1 FROM causetid_values WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx)
             BEGIN
               UPDATE causetid_values SET tx = new.tx WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx;
             END"#,

    r#"CREATE TRIGGER insert_causetid_values
                INSTEAD OF INSERT ON causetid_values
                WHEN NOT EXISTS (SELECT 1 FROM causetid_values WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx)
                BEGIN
                INSERT INTO causetid_values (e, a, v, tx) VALUES (new.e, new.a, new.v, new.tx);
                END"#,


    r#"CREATE TABLE causetid_idx (e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, PRIMARY KEY (e, a, v, tx))"#,
    r#"CREATE TABLE causetid_idx_view AS SELECT * FROM causetid_idx"#,

    r#"CREATE TABLE causetid_idx_values (e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, PRIMARY KEY (e, a, v, tx))"#,
    r#"CREATE TABLE causetid_idx_values_view AS SELECT * FROM causetid_idx_values"#,
    r#"CREATE TRIGGER replace_causetid_idx_values
             INSTEAD OF INSERT ON causetid_idx_values
             WHEN EXISTS (SELECT 1 FROM causetid_idx_values WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx)
             BEGIN
               UPDATE causetid_idx_values SET tx = new.tx WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx;
             END"#, 

    r#"CREATE TRIGGER insert_causetid_idx_values
                INSTEAD OF INSERT ON causetid_idx_values
                WHEN NOT EXISTS (SELECT 1 FROM causetid_idx_values WHERE e = new.e AND a = new.a AND v = new.v AND tx = new.tx)
                BEGIN
                INSERT INTO causetid_idx_values (e, a, v, tx) VALUES (new.e, new.a, new.v, new.tx);
                END"#,
    r#"CREATE TRIGGER delete_causetid_idx_values    
                INSTEAD OF DELETE ON causetid_idx_values
                BEGIN
                DELETE FROM causetid_idx_values WHERE e = old.e AND a = old.a AND v = old.v AND tx = old.tx;
                END"#,
    r#"CREATE TRIGGER update_causetid_idx_values
                INSTEAD OF UPDATE ON causetid_idx_values
                WHEN old.e <> new.e OR old.a <> new.a OR old.v <> new.v OR old.tx <> new.tx
                BEGIN
                DELETE FROM causetid_idx_values WHERE e = old.e AND a = old.a AND v = old.v AND tx = old.tx;
                INSERT INTO causetid_idx_values (e, a, v, tx) VALUES (new.e, new.a, new.v, new.tx);
                END"#,

    };
    for s in sql {


        let mut stmt = conn.prepare(s).unwrap();
        
        stmt.execute(&[]).unwrap();
    }
}


fn main() {
    let conn = Connection::open("sqlite.db").unwrap();
    create_tables(&conn);
    let mut stmt = conn.prepare("SELECT * FROM causetid_values").unwrap();
    let rows = stmt.query(&[]).unwrap();
    for row in rows {
        println!("{:?}", row);
    }
}


#[cfg(test)]



