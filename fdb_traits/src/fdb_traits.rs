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
    ", initial_pragmas))?;

    Ok(conn)
}

pub fn new_connection<T>(uri: T) -> rusqlite::Result<rusqlite::Connection> where T: AsRef<Path> {
    make_connection(uri.as_ref(), None)
}

#[cfg(feature = "sqlcipher")]
pub fn new_connection_with_key<P, S>(uri: P, encryption_key: S) -> rusqlite::Result<rusqlite::Connection>
where P: AsRef<Path>, S: AsRef<str> {
    make_connection(uri.as_ref(), Some(encryption_key.as_ref()))
}

#[cfg(feature = "sqlcipher")]
pub fn change_encryption_key<S>(conn: &rusqlite::Connection, encryption_key: S) -> rusqlite::Result<()>
where S: AsRef<str> {
    let escaped = escape_string_for_pragma(encryption_key.as_ref());
    // `conn.execute` complains that this returns a result, and using a query
    // for it requires more boilerplate.
    conn.execute_batch(&format!("PRAGMA rekey = '{}';", escaped))
}



