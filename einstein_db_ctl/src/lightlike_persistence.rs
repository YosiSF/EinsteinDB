/// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
///CHANGELOG: 
/// - 2020-01-01: Create file.
/// - 2020-01-02: Add `PerfContext`
/// - 2020-01-03: Add `PerfContext::new`
///
/// # Lightlike persistence
///
/// Lightlike persistence is a simple persistence layer that stores data in memory.
///
/// # Example
///
/// ```
/// use einstein_db_ctl::lightlike_persistence::LightlikePersistence;
///
/// let mut persistence = LightlikePersistence::new();
/// persistence.set("key", "value");
/// assert_eq!(persistence.get("key"), Some("value"));
/// ```
/// it works as a knowledge base for the control plane.
///
/// # Note
///
/// This is a simple implementation that does not support transactions.
///
/// # Limitations
///
/// This is a simple implementation that does not support transactions.



use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use sqxl::time::{self, Time};
use allegro_poset::{self, Poset};
use allegro_poset::{Poset, PosetError};
use std::cell::RefCell;
use std::fmt::{self, Debug, Formatter};
use std::rc::Rc;
use std::sync::Mutex;
use std::collections::BTreeMap;

pub struct LightlikePersistence {
    pub data: Mutex<HashMap<String, String>>,

    pub poset: Arc<Poset>,

    pub cache: Mutex<HashMap<String, Rc<RefCell<String>>>>,

    pub cache_size: usize,

    pub cache_hit: Mutex<usize>,

    pub cache_miss: Mutex<usize>,

    pub cache_hit_rate: Mutex<f64>,
    

}


impl LightlikePersistence {
     /// Open a store at the supplied path, ensuring that it includes the bootstrap schema.
    


    pub fn new() -> LightlikePersistence {
        let poset = Poset::new();
        let data = Mutex::new(HashMap::new());
        let cache = Mutex::new(HashMap::new());
        let cache_size = 0;
        let cache_hit = Mutex::new(0);
        let cache_miss = Mutex::new(0);
        let cache_hit_rate = Mutex::new(0.0);
        LightlikePersistence {
            data,
            poset,
            cache,
            cache_size,
            cache_hit,
            cache_miss,
            cache_hit_rate,
        }

    }


     pub fn open(path: &str) -> Result<Self, PosetError> {
        let mut connection = ::new_connection(path)?;
        let conn = connection.get_connection();
        let poset = Poset::open(&conn)?;
        let data = Arc::new(RwLock::new(HashMap::new()));
        Ok(LightlikePersistence {
            data: data,
            conn: conn,
            poset: poset,
            sqlite: connection,
            cache: Arc::new(Mutex::new(HashMap::new())),
        })



    }

    pub fn get_connection(&self) -> &::SqliteConnection {
        &self.conn
    }

    /// Create a new store at the supplied path, ensuring that it includes the bootstrap schema.
    /// # Example
    /// ```
    /// use einstein_db_ctl::lightlike_persistence::LightlikePersistence;
    /// let mut persistence = LightlikePersistence::new();
    /// persistence.set("key", "value");
    /// assert_eq!(persistence.get("key"), Some("value"));
    /// ```
    /// it works as a knowledge base for the control plane.
    /// # Note
    /// This is a simple implementation that does not support transactions.
    /// # Limitations

    pub fn transact(&self) -> Result<LightlikePersistence, PosetError> {
        let mut connection = ::new_connection(&self.conn)?;
        let conn = connection.get_connection();
        let poset = Poset::open(&conn)?;
        let data = Arc::new(RwLock::new(HashMap::new()));
        Ok(LightlikePersistence {
            data: data,
            conn: conn,
            poset: poset,
            sqlite: connection,
            cache: Arc::new(Mutex::new(HashMap::new())),
        })

        let mut ip = self.begin_transaction()?;
        let report = ip.transact(transaction)?;
        ip.commit()?;
        Ok(report)

        
    }
    

    #[cfg(feature = "syncable", feature = "asyncable",)]
    pub fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<SyncResult> {

        let mut reports = vec![];
        loop {
            let mut ip = self.begin_transaction()?;
            let report = ip.sync(server_uri, user_uuid)?;
            ip.commit()?;
            reports.push(report);
            if report.is_finished() {
                break;
            }

        }

            match report {
                SyncReport::Merge(SyncFollowup::FullSync) => {
                    break;
                }

                SyncReport::Merge(SyncFollowup::PartialSync(ref followup)) => {
                    let mut ip = self.begin_transaction()?;
                    reports.push(report);
                    continue
                },
                _ => {
                    reports.push(report);
                    continue
                }

            }
                    break
                },


            }
        Ok(SyncResult {
            reports: reports,
        })
 

    #[cfg(feature = "syncable", feature = "asyncable",)]
    pub fn sync_async(&mut self, server_uri: &String, user_uuid: &String) -> Result<SyncResult> {

        if reports.len() == 1 {
            Ok(SyncResult::Atomic(reports[0].clone()))
        } else {
            Ok(SyncResult::NonAtomic(reports))
        }
    }
    pub fn new() -> Self {
        LightlikePersistence {
            data: Arc::new(RwLock::new(HashMap::new())),
            poset: Arc::new(Poset::new()),
            cache: Arc::new(Mutex::new(HashMap::new()))

    }

    

    pub fn set(&self, key: &str, value: &str) {
        let mut data = self.data.write().unwrap();
        data.insert(key.to_string(), value.to_string());
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let data = self.data.read().unwrap();
        data.get(key).map(|v| v.to_string())
    }
}






pub struct timelike_store {
    conn: Conn,
    data: Arc<RwLock<HashMap<String, String>>>,
    poset: Arc<Poset>,
    cache: Arc<Mutex<HashMap<String, Rc<RefCell<String>>>>>,
    postgresql: Arc<Mutex<PostgreSQL>>,
    fdb: Arc<Mutex<FDB>>,
    foundationdbdb: Arc<Mutex<RocksDB>>,
    sqlite: Arc<Mutex<Sqlite>>,
    mongodb: Arc<Mutex<MongoDB>>,
    redis: Arc<Mutex<Redis>>,
}

///CHANGELOG:  
/// - added foundationdbdb
/// - added mongodb
/// - added redis
/// - added sqlite
/// - added postgresql

/// # Example
/// ```
/// use einstein_db_ctl::timelike_store::timelike_store;
/// let mut persistence = timelike_store::new();
/// persistence.set("key", "value");
/// assert_eq!(persistence.get("key"), Some("value"));
/// ```
/// it works as a knowledge base for the control plane.
/// 
/// 



impl timelike_store {

    pub fn new() -> Self {
        timelike_store {
            conn: Conn::new(),
            data: Arc::new(RwLock::new(HashMap::new())),
            poset: Arc::new(Poset::new()),
            cache: Arc::new(Mutex::new(HashMap::new())),
            postgresql: Arc::new(Mutex::new(PostgreSQL::new())),
            fdb: Arc::new(Mutex::new(FDB::new())),
            foundationdbdb: Arc::new(Mutex::new(RocksDB::new())),
            sqlite: Arc::new(Mutex::new(Sqlite::new())),
            mongodb: Arc::new(Mutex::new(MongoDB::new())),
            redis: Arc::new(Mutex::new(Redis::new())),
        }
    }
}

#[cfg(feature = "sqlcipher")]
pub struct Sqlcipher {
    conn: Conn,
    data: Arc<RwLock<HashMap<String, String>>>,
    poset: Arc<Poset>,
    cache: Arc<Mutex<HashMap<String, Rc<RefCell<String>>>>>,
    postgresql: Arc<Mutex<PostgreSQL>>,
    fdb: Arc<Mutex<FDB>>,
    foundationdbdb: Arc<Mutex<RocksDB>>,
    sqlite: Arc<Mutex<Sqlite>>,
    mongodb: Arc<Mutex<MongoDB>>,
    redis: Arc<Mutex<Redis>>,
}


impl Sqlcipher  {
    pub fn new() -> Self {
        Sqlcipher {
            conn: Conn::new(),
            data: Arc::new(RwLock::new(HashMap::new())),
            poset: Arc::new(Poset::new()),
            cache: Arc::new(Mutex::new(HashMap::new())),
            postgresql: Arc::new(Mutex::new(PostgreSQL::new())),
            fdb: Arc::new(Mutex::new(FDB::new())),
            foundationdbdb: Arc::new(Mutex::new(RocksDB::new())),
            sqlite: Arc::new(Mutex::new(Sqlite::new())),
            mongodb: Arc::new(Mutex::new(MongoDB::new())),
            redis: Arc::new(Mutex::new(Redis::new())),
        }
    }

        let mut ip = self.begin_transaction()?;
        let report = ip.sync(server_uri, user_uuid)?;
        ip.commit()?;
        Ok(report)


    #[cfg(feature = "syncable", feature = "asyncable",)]
    pub fn sync(&mut self, server_uri: &String, user_uuid: &String) -> Result<SyncResult> {

        let mut reports = vec![];
        loop {
            let mut ip = self.begin_transaction()?;
            let report = ip.sync(server_uri, user_uuid)?;
            ip.commit()?;
            reports.push(report);
            if report.is_finished() {
                break;
            }

        }

            match report {
                SyncReport::Merge(SyncFollowup::FullSync) => {
                    break;
                }

                SyncReport::Merge(SyncFollowup::PartialSync(ref followup)) => {
                    let mut ip = self.begin_transaction()?;
                    reports.push(report);
                    continue
                },
                _ => {
                    reports.push(report);
                    continue
                }

            }
                    break
                },


            }
        Ok(SyncResult {
            reports: reports,
        })

    }
}






    /// Change the key for a database that was opened using `open_with_key` (using `PRAGMA
    /// rekey`). Fails unless linked against sqlcipher (or something else that supports the Sqlite
    /// Encryption Extension).
    pub fn change_encryption_key(&mut self, new_encryption_key: &str) -> Result<()> {
        self.conn.change_encryption_key(new_encryption_key)
    }

    /// Open a database at the given path.
    /// Fails if the database is already open.
    /// Fails if the database does not exist.
    /// Fails if the database is not a valid sqlite database.
    /// Fails if the database is not encrypted.
    /// Fails if the database is not encrypted with the given key.
        ::change_encryption_key(&self.sqlite, new_encryption_key)?;
    
    ///
    pub fn open_with_key(&mut self, path: &str, encryption_key: &str) -> Result<()> {

        self.conn.open_with_key(path, encryption_key)


    }



    impl causetq for lightlike_persistence {
        fn causetq_onceM<T>(&self, causetq: &str, inputs: T) -> Result<()>
        where
            T: IntoIterator,
            T::Item: AsRef<str>,
        {
            let mut ip = self.begin_transaction()?;
            ip.causetq_onceM(causetq, inputs)?;
            ip.commit()?;
            Ok(())
        }
}   

    
    
