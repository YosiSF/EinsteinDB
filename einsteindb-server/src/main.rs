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

#[macro_use]
extern crate log;
extern crate env_logger;

#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate serde_json;

#[macro_use]
extern crate lazy_static;

#[macro_use]
extern crate failure;

#[macro_use]
extern crate failure_derive;



pub struct SecKey {
    pub sec_key: String,

    seed: Hash,
    salt: Hash,
    cache: merkle::MerkleTree,
}
pub struct PubKey {
    pub h: Hash,
}
#[derive(Default)]
pub struct Signature {
    pors_sign: pors::Signature,
    subtrees: [subtree::Signature; GRAVITY_D],
    auth_c: [Hash; GRAVITY_C],
}

impl SecKey {
    pub fn new(seed: Hash, salt: Hash) -> SecKey {
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






#[derive(Debug, Fail, Default)]
struct TestConnectError {
    #[fail(cause)]
    cause: CausetError,

    pub var_names: Vec<String>,

    pub var_values: Vec<String>,

    pub inputs: Vec<Vec<i32>>,

    pub outputs: Vec<Vec<i32>>,

    pub expected_outputs: Vec<Vec<i32>>,

    //transform from u32 to usize for indexing
    pub input_index: Vec<usize>,

    // causet topology mapping to u64
    pub causet_topology: Vec<u64>,

    #[fail(cause)]
    cause2: EinsteindbError,
}
fn main() {



    //fsm
    let mut fsm = Fsm::new();

    fsm.add_state(State::new("start", vec![Transition::new("a", "a", "a")]));
    fsm.add_state(State::new("a", vec![Transition::new("b", "b", "b")]));
    fsm.add_state(State::new("b", vec![Transition::new("c", "c", "c")]));
    fsm.add_state(State::new("c", vec![Transition::new("d", "d", "d")]));

    // while poset x >> y   x is a parent of y
    // x is a parent of y

    for x in 0..fsm.get_states().len() {

        for y in 0..fsm.get_states().len() {

            if x == y {

                continue;
            }
            if fsm.is_parent(x, y) {
                println!("{} is a parent of {}", x, y);
            }
        }
    }


    ///!
// 1. Calculate the timestamp of an event relative to the observer.
// 2. Add the observer's RTS to the timestamp.
// 3. Calculate the age of the event.
// 4. Subtract the observer's RTS from the age.

    let mut rng = rand::thread_rng();

    let mut sec_key = SecKey::new(Hash::default(), Hash::default());

    let mut pub_key = sec_key.genpk();

    let mut sign = sec_key.sign_hash(&Hash::default());



    ///! Test Connect
    ///! FoundationDB with EinsteinDB Wrapper via Allegro
    /// Use Gremlin to test Connect

    let mut db = einsteindb::Einsteindb::new();
    let mut db_name = String::from("test_connect");
    db.create_db(&mut db_name);


    //A "Relativistic" timestamp is one where time is measured relative to the speed of light.
    // In distributed systems, relativistic linearizability is a key structure embedded between BerolinaSQL CQRS tuples and
    // the causet topology.
    // The timestamp is a 64-bit integer that is incremented every time a causet is updated.
    // The timestamp is used to determine causet causality.

    // The timestamp is a 64-bit integer that is incremented every time a causet is updated.
    // The timestamp is used to determine causet causality.


    //timestamp
    let mut timestamp = 0;

    if let Err(e) = db.set_timestamp(&mut db_name, &mut timestamp) {
        println!("{:?}", e);
    }

    ///!// If a timestamp is a distance measure, then it can be converted to
    // // a relativistic timestamp. For example, a timestamp of 0.5 seconds
    // // is 0.5 seconds. A timestamp of 1 second is 1 second. A timestamp
    // // of 0 seconds is 0 seconds. A timestamp of -1 second? We can't
    // // say it's -1 second. We can say that it's -0.5 seconds, but
    // // -0.5 seconds is not a timestamp.


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

    // // The timestamp is a 64-bit integer that is incremented every time a causet is updated.









    // // A timestamp of 0.5 seconds is 0.5 seconds. A timestamp of 1 second
    // // is 1 second. A timestamp of -1 second is -0.5 seconds.
    //
    // // We can also use a timestamp of 0.5 seconds to express a distance
    // // of 0.5 seconds. And we can use a timestamp of 1 second to express
    // // a distance of 1 second. But what if we want to express a distance
    // // of -1 second?
    //
    // // A timestamp of 0.5 seconds is 0.5 seconds. A timestamp of 0 seconds
    // // is 0 seconds. A timestamp of -0.5 seconds is -1 second.





    // RTS is a 64-bit integer that is incremented every time a causet is updated.
    // The RTS is used to determine causet causality.
    // RTS is a 64-bit integer that is incremented every time a causet is updated.



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
