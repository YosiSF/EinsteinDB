///! Copyright (c) 2022 by Whtcorps All Rights Reserved
///! Author: Whtcorps
///! Date: 2020-01-04
///! Description: einsteindb-server
///! Version: 0.1.0


use std::net::{TcpListener, TcpStream};
use std::io::{Read, Write};
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::collections::HashMap;
use std::time::Duration;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::env;
use std::process::Command;
use std::process::Stdio;
use std::process::Child;
use std::process::ExitStatus;
use std::process::Output;
use std::process::Stdio;
use std::process::ChildStdin;
use std::process::ChildStdout;
use std::process::ChildStderr;

use clap::process_matches;
use clap::App;
use clap::Arg;

use einstein_db_server::config::Config;
use einstein_db_server::config::ConfigManager;

use einsteindb::{Einsteindb, EinsteindbError};
use einsteindb::{EinsteindbResult, EinsteindbResultExt};
use einstein_db::Causetid; // for Causetid
use einstein_db::CausetidError; // for CausetidError
use einstein_db::CausetidResult; // for CausetidResult
use allegro_poset::{AllegroPoset, AllegroPosetError};
use allegro_poset::{AllegroPosetResult};
use causet::util::{Causet, CausetError};
use causet::util::{CausetResult};
use causets::{AllegroCausets, AllegroCausetsError};
use causets::{AllegroCausetsResult};
use einstein_db_ctl::{EinsteindbCtl, EinsteindbCtlError};
use einstein_db_ctl::{EinsteindbCtlResult};
use einstein_db_server::util::{EinsteindbServer, EinsteindbServerError};
use berolina_sql::{BerolinaSql, BerolinaSqlError};
use FoundationDB;
use fdb::Database;
use fdb::DatabaseOptions;
use fdb::DatabaseFuture;
use fdb::DatabaseGuard;
use fdb_traits::FdbResult;
use fdb_traits::FdbFuture;
use std::time::{SystemTime, UNIX_EPOCH};

use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;

use crate::mailbox::BasicMailbox;
use std::borrow::Cow;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use std::sync::Arc;
use std::{ptr, usize};


use einstein_ml::{EinsteinMl, EinsteinMlError};
use einstein_ml::{EinsteinMlResult};

// The FSM is notified.
const NOTIFYSTATE_NOTIFIED: usize = 0;
// The FSM is idle.
const NOTIFYSTATE_IDLE: usize = 1;
// The FSM is expected to be dropped.
const NOTIFYSTATE_DROP: usize = 2;








#[derive(Debug)]
struct EinsteinDB {
    config: Config,
    einsteindb: Einsteindb,
    allegro_poset: AllegroPoset,
    allegro_causets: AllegroCausets,
    einsteindb_ctl: EinsteindbCtl,
    berolina_sql: BerolinaSql,
    fdb: Database,
    fdb_guard: DatabaseGuard,
    fdb_future: DatabaseFuture,
    fdb_result: FdbResult,
    fdb_future_result: FdbFuture,


}


impl EinsteinDB {

    fn new(config: Config) -> EinsteinDB {
        EinsteinDB {
            config: config,
            einsteindb: Einsteindb::new(),
            allegro_poset: AllegroPoset::new(),
            allegro_causets: AllegroCausets::new(),
            einsteindb_ctl: EinsteindbCtl::new(),
            berolina_sql: BerolinaSql::new(),
            fdb: Database::new(),
            fdb_guard: DatabaseGuard::new(),
            fdb_future: DatabaseFuture::new(),
            fdb_result: FdbResult::new(),
            fdb_future_result: FdbFuture::new(),
        }


    }

    
    fn run(&mut self) -> EinsteinDBResult<()> {
        let mut einsteindb_server = EinsteindbServer::new(self.config.clone());
        einsteindb_server.run()?;
        Ok(())
    }
}

pub struct SecKey {



    pub sec_key: String,

    seed: Hash,
    salt: Hash,
    cache: merkle::MerkleTree,
}
pub struct PubKey {

    pub pub_key: String,

    seed: Hash,

    salt: Hash,

    cache: merkle::MerkleTree,

    sec_key: SecKey,

    pub h: Hash,
}
#[derive(Default)]
pub struct Signature {
    /// The signature.
    /// This is a 64-byte array.
    /// The first 32 bytes are the R value, the second 32 bytes are the S value.
    /// The R value is the first 32 bytes of the signature.
    /// The S value is the second 32 bytes of the signature.
    /// 
    
    pub signature: [u8; 64],
    pub pub_key: PubKey,
    pub sec_key: SecKey,
    pub h: Hash,
    pub sig_hash: Hash,
    pub sig_hash_cache: merkle::MerkleTree,
    pub sig_hash_cache_root: Hash,
    pub sig_hash_cache_root_hash: Hash,
    pors_sign: pors::Signature,
    subtrees: [subtree::Signature; GRAVITY_D],
    auth_c: [Hash; GRAVITY_C],
    auth_c_cache: [merkle::MerkleTree; GRAVITY_C],
    auth_c_cache_root: [Hash; GRAVITY_C],
}


#[derive(Default)]
pub struct Subtree {
    pub subtree: [Hash; GRAVITY_D],
    pub subtree_cache: [merkle::MerkleTree; GRAVITY_D],
    pub subtree_cache_root: [Hash; GRAVITY_D],
}


#[derive(Default)]
pub struct AuthC {
    pub auth_c: [Hash; GRAVITY_C],
    pub auth_c_cache: [merkle::MerkleTree; GRAVITY_C],
    pub auth_c_cache_root: [Hash; GRAVITY_C],
}


#[derive(Default)]
pub struct AuthC_Root {
    pub auth_c_root: Hash,
    pub auth_c_root_hash: Hash,
}
impl SecKey {

    pub fn new(sec_key: String) -> SecKey {
        SecKey {
            sec_key: sec_key,
            seed: Hash::new(),
            salt: Hash::new(),
            cache: merkle::MerkleTree::new(),
        }
    }

    pub fn get_sec_key(&self) -> String {
        self.sec_key.clone()
    }

    pub fn get_seed(&self) -> Hash {
        self.seed.clone()
    }
    pub fn new(seed: Hash, salt: Hash) -> SecKey {
        SecKey {
            seed: seed,
            salt: salt,
            cache: merkle::MerkleTree::new(),
            sec_key: String::new(),
        }
    }

    pub fn get_salt(&self) -> Hash {
        SecKey {
            sec_key: (),
            seed,
            salt,
            cache: merkle::MerkleTree::new(),
            
        }


    }
    pub fn get_seed(&self) -> Hash {
        self.seed
    }

    pub fn get_salt(&self) -> Hash {
        self.salt
    }

    pub fn get_cache(&self) -> &merkle::MerkleTree {
        &self.cache
    }

    pub fn set_cache(&mut self, cache: merkle::MerkleTree) {
        self.cache = cache;
    }

    pub fn genpk(&self) -> PubKey {
        PubKey {
            h: self.cache.root(),
        }
    }

    pub fn sign_hash(&self, msg: &Hash) -> Signature {
        // let mut sign: Signature = Default::default();
        let mut sign = Signature::default();
        sign.pors_sign = pors::Signature::new(msg, &self.seed, &self.salt);

        let prng = prng::Prng::new(&self.seed);
        for i in 0..GRAVITY_D {
            sign.subtrees[i] = subtree::Signature::new(&prng, &self.cache.get_subtree(i));
        }
        let (mut address, mut h, pors_sign) = pors::sign(&prng, &self.salt, msg);
        sign.pors_sign = pors_sign;

        let subtree_sk = subtree::SecKey::new(&prng);
        for i in 0..GRAVITY_D {
            address.next_layer();
            let (root, subtree_sign) = subtree_sk.sign(&address, &h);
            h = root;
            sign.subtrees[i] = subtree_sign;
            address.shift(MERKLE_H); // Update instance
        }

        let index = address.get_instance();
        self.cache.gen_auth(&mut sign.auth_c, index);

        sign

    }

    pub fn sign_msg(&self, msg: &str) -> Signature {
        let msg_hash = Hash::new(msg);
        self.sign_hash(&msg_hash)
    }

    pub fn verify_hash(&self, msg: &Hash, sign: &Signature) -> bool {
        pors::verify(&sign.pors_sign, &self.seed, &self.salt, msg) &&
            subtree::verify(&sign.subtrees, &self.cache, msg) &&
            auth_c::verify(&sign.auth_c, &self.cache, msg)
    }

    pub fn verify_msg(&self, msg: &str, sign: &Signature) -> bool {
        let msg_hash = Hash::new(msg);
        self.verify_hash(&msg_hash, sign)
    }

    pub fn verify_hash_sig(&self, msg: &Hash, sign: &Signature) -> bool {
        pors::verify(&sign.pors_sign, &self.seed, &self.salt, msg) &&
            subtree::verify(&sign.subtrees, &self.cache, msg) &&
            auth_c::verify(&sign.auth_c, &self.cache, msg)
    }
}


impl PubKey {
    pub fn sign_bytes(&self, msg: &[u8]) -> Signature {
        let h = hash::long_hash(msg);
        self.sign_hash(&h)
    }
}

    impl PubKey {
        fn verify_hash(&self, sign: &Signature, msg: &Hash) -> bool {
            if let Some(h) = sign.extract_hash(msg) {
                self.h == h
            } else {
                false
            }
        }
    }

 fn verify_bytes(sign: &Signature, msg: &[u8]) -> bool {
    let h = hash::long_hash(msg);
    sign.verify_hash(&h)
}


impl Signature {
    pub fn extract_hash(&self, msg: &Hash) -> Option<Hash> {
        if self.pors_sign.verify(msg) {
            Some(self.pors_sign.get_hash())
        } else {
            None
        }
    }
}


impl Signature {
    pub fn verify_hash(&self, msg: &Hash) -> bool {
        self.pors_sign.verify(msg) &&
            subtree::verify(&self.subtrees, &self.cache, msg) &&
            auth_c::verify(&self.auth_c, &self.cache, msg)
    }
}


impl Signature {
    pub fn verify_bytes(&self, msg: &[u8]) -> bool {
        let h = hash::long_hash(msg);
        self.verify_hash(&h)
    }
}


impl Signature {
    pub fn extract_bytes(&self, msg: &[u8]) -> Option<Hash> {
        if self.pors_sign.verify(msg) {
            Some(self.pors_sign.get_hash())
        } else {
            None
        }
    }
}



#[derive(Debug, Fail, Default)]
struct TestConnectError {
    #[fail(cause)]
    Causet: CausetError,

    pub var_names: Vec<String>,

    pub var_values: Vec<String>,

    pub inputs: Vec<Vec<i32>>,

    pub outputs: Vec<Vec<i32>>,

    pub expected_outputs: Vec<Vec<i32>>,

    //transform from u32 to usize for indexing
    pub input_index: Vec<usize>,

    // causet topology mapping to u64
    pub causet_topology: Vec<u64>


}


impl TestConnectError {
    pub fn new(
        var_names: Vec<String>,
        var_values: Vec<String>,
        inputs: Vec<Vec<i32>>,
        outputs: Vec<Vec<i32>>,
        expected_outputs: Vec<Vec<i32>>,
        input_index: Vec<usize>,
        causet_topology: Vec<u64>
    ) -> TestConnectError {
        TestConnectError {
            var_names,
            var_values,
            inputs,
            outputs,
            expected_outputs,
            input_index,
            causet_topology
        }
    }

    pub fn get_var_names(&self) -> &Vec<String> {
        &self.var_names
    }

    pub fn get_var_values(&self) -> &Vec<String> {
        &self.var_values
    }

    pub fn get_inputs(&self) -> &Vec<Vec<i32>> {
        &self.inputs
    }

    pub fn get_outputs(&self) -> &Vec<Vec<i32>> {
        &self.outputs
    }

    #[fail(cause)]
    cause2: EinsteindbError,
}
fn main() {

    let mut db = Einsteindb::new(); 
    

    let mut test_connect_error = TestConnectError::new(
        vec!["a".to_string(), "b".to_string()],
        vec!["1".to_string(), "2".to_string()],
        vec![vec![1, 2], vec![3, 4]],
        vec![vec![1, 2], vec![3, 4]],
        vec![vec![1, 2], vec![3, 4]],
        vec![0, 1],
        vec![0, 1]
    );  

    

    // while poset x >> y   x is a parent of y
    // x is a parent of y

    // let mut poset = Poset::new();    
    // let mut poset = Poset::new();

    let mut poset = Poset::new();

    let mut poset_x = Poset::new();

    let mut poset_y = Poset::new();


    let mut poset_x_y = Poset::new();

        for y in 0..fsm.get_states().len() {

            if x == y {

                continue;
            }
            if fsm.is_parent(x, y) {
                println!("{} is a parent of {}", x, y);
            }
        }
    }



// 1. Calculate the timestamp of an event relative to the observer.
// 2. Add the observer's RTS to the timestamp.
// 3. Calculate the age of the event.
// 4. Subtract the observer's RTS from the age.

    let mut rng = rand::thread_rng();

    let mut sec_key = SecKey::new(Hash::default(), Hash::default());

    let mut pub_key = sec_key.genpk();

    let mut sign = sec_key.sign_hash(&Hash::default());


    let mut sec_key2 = SecKey::new(Hash::default(), Hash::default());

    ///! Test Connect
    ///! FoundationDB with EinsteinDB Wrapper via Allegro
    /// Use Gremlin to test Connect

    let mut db = einsteindb::Einsteindb::new();

    let mut fsm = Fsm::new();

    let mut db_name = String::from("test_connect");
    db.create_db(&mut db_name);


    //A "Relativistic" timestamp is one where time is measured relative to the speed of light.
    // In distributed systems, relativistic linearizability is a key structure embedded between BerolinaSQL CQRS tuples and
    // the causet topology.
    // The timestamp is a 64-bit integer that is incremented every time a causet is updated.
    // The timestamp is used to determine causet causality.

    // The timestamp is a 64-bit integer that is incremented every time a causet is updated.
    // The timestamp is used to determine causet causality.

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Timestamp {

        pub timelike_bucket_id: u64,

        pub timelike_bucket_offset: u64,

        pub spacelike_bucket_id: u64,

        pub spacelike_bucket_offset: u64,

        pub ts: u64,

        pub ts_rel: u64, //relativistic timestamp
    }

    impl Timestamp {
        pub fn new(ts: u64) -> Timestamp {
            Timestamp { ts }
        }
    }


    //timestamp
    let mut timestamp = 0;

    if let Err(e) = db.set_timestamp(&mut db_name, &mut timestamp) {
        println!("{:?}", e);
    }

    /// If a timestamp is a distance measure, then it can be converted to
    /// a relativistic timestamp. For example, a timestamp of 0.5 seconds
    /// is 0.5 seconds. A timestamp of 1 second is 1 second. A timestamp
    /// of 0 seconds is 0 seconds. A timestamp of -1 second? We can't
    /// say it's -1 second. We can say that it's -0.5 seconds, but
    /// -0.5 seconds is not a timestamp.


    // // If a timestamp is a distance measure, then it can be converted to
    // // a relativistic timestamp. For example, a timestamp of 0.5 seconds
    // // is 0.5 seconds. A timestamp of 1 second is 1 second. A timestamp
    // // of 0 seconds is 0 seconds. A timestamp of -1 second? We can't
    // // say it's -1 second. We can say that it's -0.5 seconds, but
    // // -0.5 seconds is not a timestamp.
    let mut rts = 0;

    if let Err(e) = db.set_rts(&mut db_name, &mut rts) {
        println!("{:?}", e);
    }

    while timestamp < 100 {

        let mut causet_topology = Vec::new();

        let mut causet_topology_index = Vec::new();

        let mut causet_topology_index_u64 = Vec::new();

        timestamp += 1;
        if let Err(e) = db.set_timestamp(&mut db_name, &mut timestamp) { //set timestamp
            // println!("{:?}", e);

            while causet_topology_index.len() < 10 {
                if let Err(e) = db.get_causet_topology(&mut db_name, &mut causet_topology_index) {
                    println!("{:?}", e);
                }
                for x in 0..fsm.get_states().len() {
                    for y in 0..fsm.get_states().len() {
                        if causet_topology_index.contains(&(x, y)) {
                            causet_topology_index_u64.push(x as u64);
                            causet_topology_index_u64.push(y as u64);
                        }
                    }
                    causet_topology_index.push(x);
                }

                if causet_topology_index.len() > 10 {
                    causet_topology_index.truncate(10);

                    if let Err(e) = db.set_causet_topology_index(&mut db_name, &mut causet_topology_index) {
                        println!("{:?}", e);
                    }
                }
            }
            println!("{:?}", e);
        }

        timestamp += 1;
        if let Err(e) = db.set_timestamp(&mut db_name, &mut timestamp) {    //set timestamp
            println!("{:?}", e);
        }
    }
    //A causet is a causet topology that is a directed acyclic graph.
    // The causet topology is a graph that is a directed acyclic graph.


    let age = rt_str_vec_2[0].parse::<i32>().unwrap();

    let age = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i32;

    let mut config = Config::new();

    let matches = process_matches();

    let mut rts = SystemTime::now();
    let offset = 0.5; // seconds

    let mut rt = rts.elapsed().unwrap().as_secs() as f64 + offset;

    println!("This Causetid marks the beginning of the transaction relative to the current time: {:?}", age, rt, rt_str_vec_2[0]);
    let mut causetid = Causetid::new(age, rt);


    let mut rt_str = format!("{}", rt);

    let mut rt_str_vec: Vec<&str> = rt_str.split(".").collect();

    rts = rts.add(offset * 1_000_000_000 as u64); //  nanoseconds or microseconds

    let mut rt_str_vec_2: Vec<&str> = rts.elapsed().unwrap().as_secs().to_string().split(".").collect();
}


fn process_matches() -> clap::ArgMatches<'static> {

    let matches = App::new("EinsteinDB")
        .version("0.1")
        .author("EinsteinDB")
        .about("EinsteinDB")
        .arg(Arg::with_name("config")
            .short("c")
            .long("config")
            .value_name("FILE")
            .help("Sets a custom config file")
            .takes_value(true))
        .get_matches();

    return matches;
}

fn persistence()  -> clap::ArgMatches<'static> {

    let matches = App::new("EinsteinDB")
        .version("0.1")
        .author("EinsteinDB")
        .about("EinsteinDB")
        .arg(Arg::with_name("persistence")
            .short("p")
            .long("persistence")
            .value_name("FILE")
            .help("Sets a custom persistence file")
            .takes_value(true))
        .get_matches();

    return matches;
    ///! TODO: Implement persistence

   trait Persistance {
        fn get_persistence_file(&self) -> String;
        fn set_persistence_file(&mut self, persistence_file: String);
        fn persist(&self);
    }

    impl Persistance for Config {
        fn persist(&self) {
            println!("{:?}", self);
        }
    }
    if matches != None {
    let matches = process_matches();
  }
}



    #[derive(Debug)]
    struct Config {
        debug: bool,
        verbose: bool,
        config: String,
    }

    impl Config {
        fn new() -> Config {
            Config {
                debug: false,
                verbose: false,
                config: String::from(""),
            }
        }
    }


    //let mut causetid = Causetid::new(age, rt);
    //let mut causetid = Causetid::new(age, rt);
    //let mut causetid = Causetid::new(age, rt);

 //mut causetid = Causetid::new::new(age, rt);






pub fn get_config_file(matches: &clap::ArgMatches) -> String {
    let config_file = matches.value_of("config").unwrap_or("config.toml");
    return config_file.to_string();
}


pub fn get_config(config_file: &str) -> Config {
    let mut config = Config::new();
    config.merge(File::with_name(config_file)).unwrap();
    return config;
}


pub fn get_config_value(config: &Config, key: &str) -> String {
    let value = config.get_str(key).unwrap();
    return value.to_string();
}


pub fn get_config_value_as_bool(config: &Config, key: &str) -> bool {
    let value = config.get_bool(key).unwrap();
    return value;
}


fn get_rts() -> f64 {
    let rts = SystemTime::now();
    let rt = rts.elapsed().unwrap().as_secs() as f64;
    return rt;
}


fn get_age() -> i32 {
    let age = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i32;
    return age;
}


fn get_rts_str() -> String {
    let rts = SystemTime::now();
    let rt = rts.elapsed().unwrap().as_secs() as f64;
    let rt_str = format!("{}", rt);
    return rt_str;
}


fn get_rts_str_vec() -> Vec<&str> {
    let rts = SystemTime::now();
    let rt = rts.elapsed().unwrap().as_secs() as f64;
    let rt_str = format!("{}", rt);
    let mut rt_str_vec: Vec<&str> = rt_str.split(".").collect();
    return rt_str_vec;
}




/// Read the ident map materialized view from the given SQL store.
pub(crate) fn read_ident_map(conn: &rusqlite::Connection) -> Result<IdentMap> {
    let v = read_materialized_view(conn, "idents")?;
    v.into_iter().map(|(e, a, typed_value)| {
        if a != entids::DB_IDENT {
            bail!(DbErrorKind::NotYetImplemented(format!("bad idents materialized view: expected :db/ident but got {}", a)));
        }
        if let TypedValue::Keyword(keyword) = typed_value {
            Ok((keyword.as_ref().clone(), e))
        } else {
            bail!(DbErrorKind::NotYetImplemented(format!("bad idents materialized view: expected [entid :db/ident keyword] but got [entid :db/ident {:?}]", typed_value)));
        }
    }).collect()
}

/// Read the schema materialized view from the given SQL store.
pub(crate) fn read_attribute_map(conn: &rusqlite::Connection) -> Result<AttributeMap> {
    let entid_triples = read_materialized_view(conn, "schema")?;
    let mut attribute_map = AttributeMap::default();
    metadata::update_attribute_map_from_entid_triples(&mut attribute_map, entid_triples, vec![])?;
    Ok(attribute_map)
}

/// Read the materialized views from the given SQL store and return a Mentat `DB` for querying and
/// applying transactions.
pub(crate) fn read_db(conn: &rusqlite::Connection) -> Result<DB> {
    let partition_map = read_partition_map(conn)?;
    let ident_map = read_ident_map(conn)?;
    let attribute_map = read_attribute_map(conn)?;
    let schema = Schema::from_ident_map_and_attribute_map(ident_map, attribute_map)?;
    Ok(DB::new(partition_map, schema))
}

/// Internal representation of an [e a v added] datom, ready to be transacted against the store.
pub type ReducedEntity<'a> = (Entid, Entid, &'a Attribute, TypedValue, bool);

#[derive(Clone,Debug,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub enum SearchType {
    Exact,
    Inexact,
}

/// `EinsteinDB's Causet Storage` will be the trait that encapsulates the storage layer.  It is consumed by the
/// transaction processing layer.
///
pub trait CausetStorage {
    /// Get the current version of the database.
    /// This is the version of the database that is currently in use.  It is not the version of the database that
    /// was last committed.
    
    fn get_version(&self) -> Result<i32>;
    
    /// Given a slice of [a v] lookup-refs, look up the corresponding [e a v] triples.
    ///
    /// It is assumed that the attribute `a` in each lookup-ref is `:db/unique`, so that at most one
    /// matching [e a v] triple exists.  (If this is not true, some matching entid `e` will be
    /// chosen non-deterministically, if one exists.)
    ///
    /// Returns a map &(a, v) -> e, to avoid cloning potentially large values.  The keys of the map
    /// are exactly those (a, v) pairs that have an assertion [e a v] in the store.
    fn resolve_avs<'a>(&self, avs: &'a [&'a AVPair]) -> Result<AVMap<'a>>;

    /// Begin (or prepare) the underlying storage layer for a new Mentat transaction.
    ///
    /// Use this to create temporary tables, prepare indices, set pragmas, etc, before the initial
    /// `insert_non_fts_searches` invocation.
    fn begin_tx_application(&self) -> Result<()>;

    // TODO: this is not a reasonable abstraction, but I don't want to really consider non-SQL storage just yet.
    fn insert_non_fts_searches<'a>(&self, causets: &'a [ReducedCauset], search_type: SearchType) -> Result<()>;
    fn insert_fts_searches<'a>(&self, causets: &'a [ReducedCauset], search_type: SearchType) -> Result<()>;

    /// Prepare the underlying storage layer for finalization after a Mentat transaction.
    ///
    /// Use this to finalize temporary tables, complete indices, revert pragmas, etc, after the
    /// final `insert_non_fts_searches` invocation.
    fn end_tx_application(&self) -> Result<()>;

    /// Finalize the underlying storage layer after a Mentat transaction.
    ///
    /// This is a final step in performing a transaction.
    /// It is called after `end_tx_application` and `insert_non_fts_searches` and `insert_fts_searches`.
    /// It is also called after a transaction is rolled back.
    /// It is called after a transaction is aborted.

    fn finalize_tx(&self) -> Result<()>;

    /// Extract metadata-related [e a typed_value added] datoms resolved in the last
    /// materialized transaction.
    fn resolved_metadata_assertions(&self) -> Result<Vec<(Causetid, Causetid, causetq_TV, bool)>>;

    /// Extract metadata-related [e a typed_value added] datoms resolved in the last
    /// materialized transaction.
    /// This is a convenience wrapper around `resolved_metadata_assertions` that returns a map
    
    fn resolved_metadata_assertions_map(&self) -> Result<AVMap> {
        let resolved_metadata_assertions = self.resolved_metadata_assertions()?;
        let mut map = AVMap::default();
        for (e, a, typed_value, added) in resolved_metadata_assertions {
            map.insert((a, typed_value), e);
        }
        Ok(map)
    }

    /// Extract metadata-related [e a typed_value added] datoms resolved in the last
    
    fn resolved_metadata_assertions_map_for_causet(&self, causet: &Causet) -> Result<AVMap> {
        let resolved_metadata_assertions = self.resolved_metadata_assertions()?;
        let mut map = AVMap::default();
        for (e, a, typed_value, added) in resolved_metadata_assertions {
            if causet.contains_entid(e) {
                map.insert((a, typed_value), e);
            }
        }
        Ok(map)
    }
}


/// `EinsteinDB's Causet Storage` will be the trait that encapsulates the storage layer.  It is consumed by the
/// transaction processing layer.


pub trait CausetStorageMut {
    // First is fast, only one table walk: lookup by exact eav.
    // Second is slower, but still only one table walk: lookup old value by ea.
    // Third is slower, but still only one table walk: lookup new value by ea.
    let s = r#"
      INSERT INTO temp.search_results
      SELECT t.e0, t.a0, t.v0, t.value_type_tag0, t.added0, t.flags0, ':db.cardinality/many', d.rowid, d.v
      FROM temp.exact_searches AS t
      LEFT JOIN datoms AS d
      ON t.e0 = d.e AND
         t.a0 = d.a AND
         t.value_type_tag0 = d.value_type_tag AND
         t.v0 = d.v
      UNION ALL
      SELECT t.e0, t.a0, t.v0, t.value_type_tag0, t.added0, t.flags0, ':db.cardinality/one', d.rowid, d.v
      FROM temp.inexact_searches AS t
      LEFT JOIN datoms AS d
      ON t.e0 = d.e AND
         t.a0 = d.a"#;

    let mut stmt = conn.prepare_cached(s)?;
    stmt.execute(&[]).context(DbErrorKind::CouldNotSearch)?;
    Ok(())
}

/// Insert the new transaction into the `transactions` table.
/// 
///
fn insert_transaction(conn: &rusqlite::Connection, tx: Entid) -> Result<()> {
    // EinsteinDB follows Datomic and Mentat treating its input as a set.  That means it is okay to transact the
    // same [e a v] twice in one transaction.  However, we don't want to represent the transacted
    // datom twice.  Therefore, the transactor unifies repeated datoms, and in addition we add
    // indices to the search inputs and search results to ensure that we don't see repeated datoms
    // at this point.

    let s = r#"
      INSERT INTO transactions
      SELECT e, a, v, value_type_tag, added, flags, ':db/id', rowid, v
      FROM datoms
      WHERE e = ? AND
            a = ? AND
            v = ? AND
            value_type_tag = ? AND
            added = ? AND
            flags = ? AND
            NOT EXISTS (
              SELECT 1
              FROM transactions
              WHERE e = ? AND
                    a = ? AND
                    v = ? AND
                    value_type_tag = ? AND
                    added = ? AND
                    flags = ?
            )"#;

    let s = r#"
      INSERT INTO timelined_transactions (e, a, v, tx, added, value_type_tag)
      SELECT e0, a0, v0, ?, 1, value_type_tag0
      FROM temp.search_results
      WHERE added0 IS 1 AND ((rid IS NULL) OR ((rid IS NOT NULL) AND (v0 IS NOT v)))"#;

    let mut stmt = conn.prepare_cached(s)?;
    stmt.execute(&[&tx]).context(DbErrorKind::CouldNotInsertTransaction)?;
    Ok(())
}


/// Insert the new transaction into the `transactions` table.
///     
/// This is a convenience wrapper around `insert_transaction` that returns a map


fn insert_transaction_map(conn: &rusqlite::Connection, tx: Entid) -> Result<AVMap> {
    let mut map = AVMap::default();
    let s = r#"
      INSERT INTO transactions
      SELECT e, a, v, value_type_tag, added, flags, ':db/id', rowid, v
      FROM datoms
      WHERE e = ? AND
            a = ? AND
            v = ? AND
            value_type_tag = ? AND
            added = ? AND
            flags = ? AND
            NOT EXISTS (
              SELECT 1
              FROM transactions
              WHERE e = ? AND
                    a = ? AND
                    v = ? AND
                    value_type_tag = ? AND
                    added = ? AND
                    flags = ?
            )"#;

    let s = r#"
      INSERT INTO timelined_transactions (e, a, v, tx, added, value_type_tag)
      SELECT DISTINCT e0, a0, v, ?, 0, value_type_tag0
      FROM temp.search_results
      WHERE rid IS NOT NULL AND
            ((added0 IS 0) OR
             (added0 IS 1 AND search_type IS ':db.cardinality/one' AND v0 IS NOT v))"#;

    let mut stmt = conn.prepare_cached(s)?;
    stmt.execute(&[&tx]).context(DbErrorKind::TxInsertFailedToRetractDatoms)?;

    Ok(())
}

//FoundationDB 
//
//The FoundationDB database is a key-value store.  It is a transactional database
//that supports the following operations:
// 
// - Read: Get the value for a key.
// - Write: Set the value for a key.
// - Delete: Remove the value for a key.
// - Transaction: A set of operations that are executed atomically.
//

#[derive(Debug)]
pub struct FoundationDB {
    conn: rusqlite::Connection,

    // The following are used to implement the EinsteinDB transaction processing layer.
    // The transaction processing layer is responsible for:
    // - Inserting the new transaction into the `transactions` table.

    fdb: fdb::FDB,

    // The following are used to implement the EinsteinDB transaction processing layer.
}

///  `EinsteinDB's FoundationDB Storage` will be the trait that encapsulates the storage layer.  It is consumed by the
/// transaction processing layer.
/// 
/// # Example
/// 
/// ```
/// use einstein_db::storage::foundationdb::FoundationDB;
/// use einstein_db::storage::foundationdb::FoundationDBStorage;
/// 
/// let storage = FoundationDBStorage::new("foundationdb.db").unwrap();
/// let mut storage = FoundationDBStorage::new("foundationdb.db").unwrap();
/// ``` 
impl FoundationDBStorage for FoundationDB {
    fn new(path: &str) -> Result<Self> {
        let conn = rusqlite::Connection::open(path)?;
        let fdb = fdb::FDB::new();
        Ok(FoundationDB {
            conn,
            fdb,
        })
    }

    fn get_connection(&self) -> &rusqlite::Connection {
        &self.conn
    }

    fn get_fdb(&self) -> &fdb::FDB {
        &self.fdb
    }
}



pub struct Database {
    conn: rusqlite::Connection,
    fdb: fdb::FDB,
    //fdn connection: fdn::FdnConnection,
}


impl Database {
    pub fn new(path: &str) -> Result<Self> {
        let conn = rusqlite::Connection::open(path)?;
        let fdb = fdb::FDB::new();
        //let fdn = fdn::FdnConnection::new();
        Ok(Database {
            conn,
            fdb,
            //fdn,
        })
    }

    pub fn get_connection(&self) -> &rusqlite::Connection {
        &self.conn
    }

    pub fn get_fdb(&self) -> &fdb::FDB {
        &self.fdb
    }

    //pub fn get_fdn(&self) -> &fdn::FdnConnection {
    //    &self.fdn
    //}
}


#[derive(Debug)]
pub struct FoundationDBTransaction {
    _client: Client,
    name: String,
    //fdn_transaction: fdn::FdnTransaction,

    // The following are used to implement the EinsteinDB transaction processing layer.
    // The transaction processing layer is responsible for:
    // - Inserting the new transaction into the `transactions` table.
    // - Inserting the new transaction into the `timelined_transactions` table.
    // - Retracting the old transaction from the `transactions` table.


 
}

impl Database {
    pub fn new(name: String, client: Client) -> Database {
        Database { _client: client, name }
    }

    // convenience function to retrieve the name of the database
    pub fn name(&self) -> &str {
        &self.name
    }

    fn create_raw_path(&self, id: &str) -> String {
        format!("{}/{}", self.name, id)
    }

    fn create_document_path(&self, id: &str) -> String {
        let encoded = url_encode!(id);
        format!("{}/{}", self.name, encoded)
    }

    fn create_design_path(&self, id: &str) -> String {
        let encoded = url_encode!(id);
        format!("{}/_design/{}", self.name, encoded)
    }

    fn create_query_view_path(&self, design_id: &str, view_id: &str) -> String {
        let encoded_design = url_encode!(design_id);
        let encoded_view = url_encode!(view_id);
        format!("{}/_design/{}/_view/{}", self.name, encoded_design, encoded_view)
    }

    fn create_execute_update_path(&self, design_id: &str, update_id: &str, document_id: &str) -> String {
        let encoded_design = url_encode!(design_id);
        let encoded_update = url_encode!(update_id);
        let encoded_document = url_encode!(document_id);
        format!(
            "{}/_design/{}/_update/{}/{}",
            self.name, encoded_design, encoded_update, encoded_document
        )
    }

    fn create_compact_path(&self, design_name: &str) -> String {
        let encoded_design = url_encode!(design_name);
        format!("{}/_compact/{}", self.name, encoded_design)
    }

    /// Launches the compact process
    pub async fn compact(&self) -> bool {
        let mut path: String = self.name.clone();
        path.push_str("/_compact");

        let request = self._client.post(&path, "".into());
        is_accepted(request).await
    }

    /// Starts the compaction of all views
    pub async fn compact_views(&self) -> bool {
        let mut path: String = self.name.clone();
        path.push_str("/_view_cleanup");

        let request = self._client.post(&path, "".into());
        is_accepted(request).await
    }

    /// Starts the compaction of a given index
    pub async fn compact_index(&self, index: &str) -> bool {
        let request = self._client.post(&self.create_compact_path(index), "".into());
        is_accepted(request).await
    }

    /// Starts the compaction of all indexes
    /// This is a convenience function that calls `compact_index` for each index.
    /// This is a blocking function.
    /// Returns a vector of the names of the indexes that were compacted.
    /// 
    
    pub async fn compact_indexes(&self) -> Vec<String> {
        let mut path: String = self.name.clone();
        path.push_str("/_index");

        let request = self._client.get(&path, "".into());
        let response = request.await.unwrap();
        let mut indexes = Vec::new();
        for index in response.json::<Indexes>().unwrap().indexes {
            if self.compact_index(&index.name).await {
                indexes.push(index.name.clone());
            }
        }
        indexes
    }

    



}