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


//use std::collections::HashMap;
use uuid::Uuid;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashSet;
use std::rc::Rc;
use std::cell::RefCell;
use std::cell::Ref;
use std::ops::{Add, AddAssign, Sub, SubAssign, Mul, MulAssign, Div, DivAssign};
use std::ops::{Deref, DerefMut, Index, IndexMut};
use std::hash::Hash;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result;
use std::fmt::Error;
use std::fmt::Write;
use std::fmt::Display;






#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FdbRecordId {
    pub id: Uuid,
}


impl FdbRecordId {
    pub fn new() -> Self {
        FdbRecordId {
            id: Uuid::new_v4(),
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FdbRecord {
    pub id: FdbRecordId,
    pub data: Vec<u8>,
}


impl FdbRecord {
    pub fn new(data: Vec<u8>) -> Self {
        FdbRecord {
            id: FdbRecordId::new(),
            data,
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FdbRecordReaderOptions {
    pub id: FdbRecordId,
    pub data: Vec<u8>,
}


impl FdbRecordReaderOptions {
    pub fn new(id: FdbRecordId, data: Vec<u8>) -> Self {
        FdbRecordReaderOptions {
            id,
            data,
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FdbRecordWriterOptions {
    pub id: FdbRecordId,
    pub data: Vec<u8>,
}


impl FdbRecordWriterOptions {
    pub fn new(id: FdbRecordId, data: Vec<u8>) -> Self {
        FdbRecordWriterOptions {
            id,
            data,
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FdbRecordReader {
    pub id: FdbRecordId,
    pub data: Vec<u8>,
}


impl FdbRecordReader {
    pub fn new(id: FdbRecordId, data: Vec<u8>) -> Self {
        FdbRecordReader {
            id,
            data,
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FdbRecordWriter {
    pub id: FdbRecordId,
    pub data: Vec<u8>,
}

//Attribute Map from SolitonID 
//to the corresponding FdbRecord
//This is a global variable
//It is a HashMap<String, FdbRecord>

//This is a global variable
fn escape_string(s: &str) -> String {
    s.replace("\"", "\"\"")

}   


fn foundationdb_connection_string(host: &str, port: &str, db_name: &str) -> String {

    format!("foundationdb://{}:{}/{}", host, port, db_name)
}


fn foundationdb_connection_string_with_options(host: &str, port: &str, db_name: &str, options: &str) -> String {
    format!("foundationdb://{}:{}/{}?{}", host, port, db_name, options)
}


fn foundationdb_connection_string_with_options_and_timeout(host: &str, port: &str, db_name: &str, options: &str, timeout: &str) -> String {
    format!("foundationdb://{}:{}/{}?{}&timeout={}", host, port, db_name, options, timeout)
}




fn foundationdb_connection_string_with_options_and_timeout_and_retry(host: &str, port: &str, db_name: &str, options: &str, timeout: &str, retry: &str) -> String {
    format!("foundationdb://{}:{}/{}?{}&timeout={}&retry={}", host, port, db_name, options, timeout, retry)
}


fn foundationdb_connection_string_with_options_and_timeout_and_retry_and_retry_timeout(host: &str, port: &str, db_name: &str, options: &str, timeout: &str, retry: &str, retry_timeout: &str) -> String {
    format!("foundationdb://{}:{}/{}?{}&timeout={}&retry={}&retry_timeout={}", host, port, db_name, options, timeout, retry, retry_timeout)
}

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


//This is the foundationdb record layer
//It is a trait which is implemented by the FdbRecord struct
//It is a trait which is implemented by the FdbRecordReader struct


//This is the foundationdb record layer




//This is the foundationdb record layer
//It is a trait which is implemented by the FdbRecord struct





#[derive(Debug)]
pub struct FdbRecordOptions {
    pub id: String,
    pub data: String,
    pub created_at: String,
    pub updated_at: String,
    pub deleted_at: String,
}




impl FdbRecord {
    pub fn new(id: String, data: String, created_at: String, updated_at: String, deleted_at: String, version: String, revision: String, attributes: HashMap<String, String>) -> FdbRecord {
        FdbRecord {
            id,
            data,

        }
    }


    pub fn new_with_attributes(id: String, data: String, created_at: String, updated_at: String, deleted_at: String, version: String, revision: String, attributes: HashMap<String, String>) -> FdbRecord {
        FdbRecord {
            id,
            data,

        }
    }


    pub fn new_with_attributes_and_options(id: String, data: String, created_at: String, updated_at: String, deleted_at: String, version: String, revision: String, attributes: HashMap<String, String>, options: FdbRecordOptions) -> FdbRecord {
        FdbRecord {
            id,
            data,

        }
    }
}


impl FdbRecordReader {
    pub fn new(id: String, data: String, created_at: String, updated_at: String, deleted_at: String, version: String, revision: String, attributes: HashMap<String, String>) -> FdbRecordReader {
        FdbRecordReader {
            id,
            data,

        }
    }


    pub fn new_with_attributes(id: String, data: String, created_at: String, updated_at: String, deleted_at: String, version: String, revision: String, attributes: HashMap<String, String>) -> FdbRecordReader {
        FdbRecordReader {
            id,
            data,

        }
    }
}
   /// let page_size = 32768;
   /// let encryption_key = "";
   /// let connection_string = foundationdb_connection_string(host, port, db_name);
   /// let connection = make_connection(&connection_string, encryption_key).unwrap();
   /// let mut fdb_record_reader = FdbRecordReader::new(id, data, created_at, updated_at, deleted_at, version, revision, attributes);
   /// let mut fdb_record_reader = FdbRecordReader::new_with_attributes(id, data, created_at, updated_at, deleted_at, version, revision, attributes);



pub fn foundationdb_record_layer_test() {

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
    conn.execute_batch("PRAGMA synchronous=NORMAL;")?;
    conn.execute_batch("PRAGMA journal_mode=WAL;")?;
    conn.execute_batch("PRAGMA foreign_keys=ON;")?;
    conn.execute_batch("PRAGMA locking_mode=EXCLUSIVE;")?;
    conn.execute_batch("PRAGMA temp_store=MEMORY;")?;
}





//This is the foundationdb record layer
//It is a trait which is implemented by the FdbRecord struct
//It is a trait which is implemented by the FdbRecordReader struct




//This is the foundationdb record layer
//It is a trait which is implemented by the FdbRecord struct
//It is a trait which is implemented by the FdbRecordReader struct




impl FdbRecord {
    pub fn new(id: String, data: String) -> Self {
        FdbRecord {
            id,
            data,
        }



    }
}


pub fn new_connection<T>(uri: T) -> rusqlite::Result<rusqlite::Connection>
where
    T: AsRef<Path>,
{
    let uri = uri.as_ref();
    let conn = make_connection(uri, None)?;
    Ok(conn)
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
/// Do the same for sqlite's `SQLITE_TRUE` and `SQLITE_FALSE`.
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
        unique_value TINYINT NOT NULL DEFAULT 0)"#,
    ]};


    static ref FOUNDATIONDB_RECORD_LAYER_VERSION_REF: &'static str = FOUNDATIONDB_RECORD_LAYER_VERSION:Vector<u8> { vec! CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
        index_fulltext TINYINT NOT NULL DEFAULT 0,
unique_value TINYINT NOT NULL DEFAULT 0)"#, };
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
        unique_value TINYINT NOT NULL DEFAULT 0)"#,
    ]};


    static ref FOUNDATIONDB_RECORD_LAYER_VERSION_REF: &'static str = FOUNDATIONDB_RECORD_LAYER_VERSION:Vector<u8> { vec! CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
        index_fulltext TINYINT NOT NULL DEFAULT 0,
        unique_value TINYINT NOT NULL DEFAULT 0)"#, };
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
        unique_value TINYINT NOT NULL DEFAULT 0)"#,
    ]};





static ref FOUNDATIONDB_RECORD_LAYER_VERSION_REF: &'static str = FOUNDATIONDB_RECORD_LAYER_VERSION:Vector<u8> { vec! CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
        index_fulltext TINYINT NOT NULL DEFAULT 0,
        unique_value TINYINT NOT NULL DEFAULT 0)"#, };
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
        unique_value TINYINT NOT NULL DEFAULT 0)"#,
    ]};


        r#"CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0, index_fulltext TINYINT NOT NULL DEFAULT 0, unique_value TINYINT NOT NULL DEFAULT 0)"#,
        r#"CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0, index_fulltext TINYINT NOT NULL DEFAULT 0, unique_value TINYINT NOT NULL DEFAULT 0)"#,
    static ref NULL_REF: &'static str = NULL:Vector<u8> { vec! CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,


        index_fulltext TINYINT NOT NULL DEFAULT 0,
        unique_value TINYINT NOT NULL DEFAULT 0)"#,


    static ref FALSE_REF: &'static str = FALSE:Vector<u8> { vec! CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
        index_fulltext TINYINT NOT NULL DEFAULT 0,
        unique_value TINYINT NOT NULL DEFAULT 0)"#, };
    static ref TRUE_REF: &'static str = TRUE:Vector<u8> { vec![
        r#"CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
        index_fulltext TINYINT NOT NULL DEFAULT 0,
        unique_value TINYINT NOT NULL DEFAULT 0)"#,
        r#"CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
                    // Opt-in index: only if a has :db/index true.
    ]};

    static ref TRUE_REF: &'static str = TRUE:Vector<u8> { vec![
        r#"CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
        index_fulltext TINYINT NOT NULL DEFAULT 0,
        unique_value TINYINT NOT NULL DEFAULT 0)"#,


        r#"CREATE TABLE causets(e INTEGER NOT NULL, a SMALL INT NOT NULL, v BLOB NOT NULL, tx INTEGER NOT NULL, causet_value_type_tag SMALLINT NOT NULL, index_avet TINYINT NOT NULL DEFAULT 0, index_vaet TINYINT NOT NULL DEFAULT 0,
                    // Opt-in index: only if a has :db/index true.


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
                END"#, old.text <> new.texts(ttl)
        r#"CREATE TRIGGER update_fulltext_searchid
                INSTEAD OF UPDATE ON fulltext_values_view
                WHEN old.text <> new.text
                BEGIN
                DELETE FROM fulltext_values WHERE searchid = old.searchid;
                INSERT INTO fulltext_values (text, searchid) VALUES (new.text, new.searchid);
                END"#, old.text <> new.texts(ttl)
        r#"CREATE TRIGGER update_fulltext_searchid
        );

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
                END"#]
        }
        .iter()
        .for_each(|sql| {
            let mut conn = self.connect().unwrap();
            conn.execute(sql, NO_PARAMS).unwrap();
        });

        // Fulltext indexing.


        // A fulltext indexed value v is an integer rowid referencing fulltext_values.


    pub struct FulltextIndex {
        pub table: String,
        pub column: String,
        pub values: Vec<String>,
    }


    pub struct FulltextIndex {
        pub table: String,
        pub column: String,
        pub values: Vec<String>,
    }



// A view transparently interpolating all entities (fulltext and non-fulltext) into the causet q table.
// This view is used to query the causetq table.
// The view is created if and only if causetq_index_fulltext is true.




// A view transparently interpolating all entities (fulltext and non-fulltext) into the causet q table.
// This view is used to query the causetq table.
// The view is created if and only if causetq_index_fulltext is true.






 pub fn create_causetq_view(
    &self,
    causetq_index_fulltext: bool,
    causetq_index_non_fulltext: bool,
) {
    let mut conn = self.connect().unwrap();
    let mut stmt = conn.prepare(
        r#"CREATE VIEW causetq_view AS SELECT * FROM causetq WHERE 1 = 0"#,
    ).unwrap();
    let mut params = Vec::new();
    if causetq_index_fulltext {
        params.push(("causetq_index_fulltext", &Value::Bool(true)));
    }
    if causetq_index_non_fulltext {
        params.push(("causetq_index_non_fulltext", &Value::Bool(true)));
    }
    stmt.execute(&params).unwrap();

}



pub fn create_causetq_view(conn: &mut Connection) -> Result<(), Error> {
    let mut stmt = conn.prepare(
        r#"CREATE VIEW causetq_view AS SELECT * FROM causetq WHERE 1 = 0"#,
    )?;
    stmt.execute(&[])?;
    Ok(())
}


pub fn create_causetq_index_fulltext(conn: &mut Connection) -> Result<(), Error> {
    let mut stmt = conn.prepare(
        r#"CREATE VIRTUAL TABLE causetq_index_fulltext USING fts5(e, a, v, tx, tokenize='unicode_ci', prefix='2,3')"#,
    )?;
    stmt.execute(&[])?;
    Ok(())
}


pub fn create_causetq_index_fulltext_view(conn: &mut Connection) -> Result<(), Error> {
    let mut stmt = conn.prepare(
        r#"CREATE VIEW causetq_index_fulltext_view AS SELECT * FROM causetq_index_fulltext"#,
    )?;
    stmt.execute(&[])?;
    Ok(())
}


pub fn create_causetq_index_fulltext_trigger(conn: &mut Connection) -> Result<(), Error> {
    let mut stmt = conn.prepare(
        r#"CREATE TRIGGER replace_causetq_index_fulltext
             INSTEAD OF INSERT ON causetq_index_fulltext
             WHEN EXISTS (SELECT 1 FROM causetq_index_fulltext WHERE text = new.text)
             BEGIN
               UPDATE causetq_index_fulltext SET searchid = new.searchid WHERE text = new.text;
             END"#,
    )?;
    stmt.execute(&[])?;
    Ok(())
}







fn main() {
    let conn = Connection::open_in_memory().unwrap();
    create_causetq_view(&mut conn).unwrap();
    create_causetq_index_fulltext(&mut conn).unwrap();
    create_causetq_index_fulltext_view(&mut conn).unwrap();
    let conn = Connection::open("sqlite.db").unwrap();
    create_causetq_index_fulltext_trigger(&mut conn).unwrap();
    //fdb create_causetq_index_fulltext_trigger(&mut conn).unwrap();
    //FoundationDB connection.create_causetq_index_fulltext_trigger().unwrap();
}
    }





// A view transparently interpolating all entities (fulltext and non-fulltext) into the causet q table.
// This view is used to query the causetq table.
// The view is created if and only if causetq_index_fulltext is true.








