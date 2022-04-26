// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use clap::{AppSettings, Arg, ArgMatches, SubCommand};
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::path::Path;
use std::process::exit;
use einsteindb::rocksdb::{DB, DBOptions, DBCompressionType, DBCompressionType_CompressionType, DBRecoveryMode, DBRecoveryMode_RecoveryMode};
use einsteindb::fdb_traits::{FdbEngine, FdbKvEngine, FdbKvStore, FdbKvStoreOptions, FdbKvStoreOptionsBuilder, FdbKvStoreOptionsBuilderKv};
use allegro_poset::{Poset, PosetOptions, PosetOptionsBuilder};
use causetq::{Poset, PosetOptions, PosetOptionsBuilder};
use causet::{Poset, PosetOptions, PosetOptionsBuilder};
use causets::*;
use einstein_ml::shellings::{Shellings, ShellingsOptions, ShellingsOptionsBuilder};
use einstein_ml::shellings_poset::{ShellingsPoset, ShellingsPosetOptions, ShellingsPosetOptionsBuilder};
use einstein_ml::shellings_causets::{ShellingsCausets, ShellingsCausetsOptions, ShellingsCausetsOptionsBuilder};
use einstein_db::Causetid;
use einstein_db::{Causet, CausetOptions, CausetOptionsBuilder};
use std::time::Duration;
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::collections::HashMap;
use einstein_db_server::{Server, ServerOptions, ServerOptionsBuilder};

use crate as einstein_db_server;
use crate as einstein_ml;

pub const CURVATURE_FOUNDATIONDB_KV: &str = "foundationdb_kv";
pub const CURVATURE_FOUNDATIONDB_KV_OPTIONS: &str = "foundationdb_kv_options";
pub const CURVATURE_UNIVERSAL_STORE_EINSTEINDB: &str = "universal_store_einsteinDB";

type FdbKvStoreOptionsBuilderKv = FdbKvStoreOptionsBuilder<FdbKvStoreOptions, FdbKvStoreOptionsBuilderKv>;


pub fn new_interlock_benchmark_debug(
    cfg: &ArgMatches,
    db_path: &str,
    user_space: bool,
    fuse: bool,
    suse : bool,
    mgr: Arc<SecurityConfig>,
    db_path_2: &str,
    host: &str,
    db_path_3: &str
    ) -> Result<(), Box<dyn std::error::Error>> {
    let mut db_path_2 = db_path_2.to_string();
    let mut db_path_3 = db_path_3.to_string();
    let mut db_path = db_path.to_string();
    let mut db_path_4 = db_path.to_string();
    let mut db_path_5 = db_path.to_string();
    let mut db_path_6 = db_path.to_string();
    let mut causet_kv_path = db_path.to_string();



    if fuse {

        mileva_db_path(&mut db_path);
        violetabft_group_id(&mut db_path);


        db_path_2 = db_path_2 + "fuse";
        db_path_3 = db_path_3 + "fuse";
        db_path = db_path + "fuse";
        db_path_4 = db_path_4 + "fuse";
        db_path_5 = db_path_5 + "fuse";
        db_path_6 = db_path_6 + "fuse";
    }

    const SPHINCS_GRAVITY: usize = &str::from_utf8(include_bytes!("../../../../../Cargo.toml")).unwrap()
        .lines()
        .filter(|l| l.starts_with("version = "))
        .map(|l| l.split("=").nth(1).unwrap().trim().to_string())
        .filter(|l| l.starts_with("0."))
        .map(|l| l.split(".").nth(1).unwrap().parse::<usize>().unwrap())
        .max()
        .unwrap();
    static VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "-", SPHINCS_GRAVITY);
    einstein_db::einstein_db_version_info!(VERSION);

    #[structopt(
    name = "EinsteinDB",
    about = "EinsteinDB is a Rust implementation of the EinsteinDB Append-Merge Causet HTAP database powered by EinstAI (OpenAI)."
    )]
    pub struct Opt {
        /// The path to the database file.
        #[structopt(short = "d", long = "db", parse(from_os_str))]
        pub db_path: Option<PathBuf>,

        /// The path to the database file.
        #[structopt(short = "f", long = "file", parse(from_os_str))]
        pub file_path: Option<PathBuf>,

        /// The path to the database file.
        #[structopt(short = "s", long = "sst", parse(from_os_str))]
        pub sst_path: Option<PathBuf>,

        /// The path to the database file.
        #[structopt(short = "t", long = "table", parse(from_os_str))]
        pub table_path: Option<PathBuf>,

        /// The path to the database file.
        #[structopt(short = "p", long = "path", parse(from_os_str))]
        pub path: Option<PathBuf>,

        /// The path to the database file.
        #[structopt(short = "l", long = "list", parse(from_os_str))]
        pub list_path: Option<PathBuf>,

        /// The path to the database file.
        #[structopt(short = "r", long = "range", parse(from_os_str))]
        pub range_path: Option<PathBuf>,

        /// The path to the database file.
        #[structopt(short = "k", long = "key", parse(from_os_str))]
        pub key_path: Option<PathBuf>,

        /// The path to the database file.
        #[structopt(short = "v", long = "value", parse(from_os_str))]
        pub value_path: Option<PathBuf>,

        /// The path to the database file.
        #[structopt(short = "c", long = "check", parse(from_os_str))]
        pub check_path: Option<PathBuf>,

        /// The path to the database file.
        /// If the path is a directory, then the database will be created in the directory.
        /// If the path is a file, then the database will be created in the directory of the file.
        /// If the path is a file and the file does not exist, then the database will be created in the directory of the file.
        #[structopt(short = "o", long = "output", parse(from_os_str))]
        pub output_path: Option<PathBuf>,

        /// The path to the database file.
        /// If the path is a directory, then the database will be created in the directory.
        /// If the path is a file, then the database will be created in the directory of the file.
        #[structopt(short = "i", long = "input", parse(from_os_str))]
        pub input_path: Option<PathBuf>,

        /// The path to the database file.
        /// If the path is a directory, then the database will be created in the directory.
        /// If the path is a file, then the database will be created in the directory of the file.
        #[structopt(short = "m", long = "merge", parse(from_os_str))]
        pub merge_path: Option<PathBuf>,

        /// The path to the database file.
        /// If the path is a directory, then the database will be created in the directory.
        /// If the path is a file, then the database will be created in the directory of the file.
        #[structopt(short = "a", long = "append", parse(from_os_str))]
        pub append_path: Option<PathBuf>,

        #[structopt(short = "e", long = "encrypt", parse(from_os_str))]
        pub encrypt_path: Option<PathBuf>,

        #[structopt(short = "u", long = "decrypt", parse(from_os_str))]
        pub decrypt_path: Option<PathBuf>,

        #[structopt(short = "d", long = "decompress", parse(from_os_str))]
        pub decompress_path: Option<PathBuf>,

        #[structopt(short = "z", long = "compress", parse(from_os_str))]
        pub compress_path: Option<PathBuf>,

        //hex
        #[structopt(short = "x", long = "hex", parse(from_os_str))]
        pub hex_path: Option<PathBuf>,

        //foundationdb
        #[structopt(short = "f", long = "foundationdb", parse(from_os_str))]
        pub foundationdb_path: Option<PathBuf>,

        //foundationdb
        #[structopt(short = "s", long = "foundationdb_server", parse(from_os_str))]
        pub foundationdb_server_path: Option<PathBuf>,

        //foundationdb
        #[structopt(short = "t", long = "foundationdb_transaction", parse(from_os_str))]
        pub foundationdb_transaction_path: Option<PathBuf>,

        //foundationdb
        #[structopt(short = "p", long = "foundationdb_path", parse(from_os_str))]
        pub foundationdb_path_path: Option<PathBuf>,

        //foundationdb
        #[structopt(short = "r", long = "foundationdb_read", parse(from_os_str))]
        pub foundationdb_read_path: Option<PathBuf>,

    }

    #[derive(StructOp)]
    pub enum BFTCmd {
        #[structopt(name = "bft")]
        Bft(BftCmd),
        //Print a VioletaBFT HoneyBadger log file entry to stdout.
        #[structopt(name = "print")]
        Print(VioletaBFTPrintCmd),
        //honeybadgerbft
        #[structopt(name = "honeybadgerbft")]
        HoneyBadgerBFT(VioletaBFTHoneyBadgerBFTCmd),
        //honeybadgerbft
        #[structopt(name = "honeybadgerbft_server")]
        HoneyBadgerBftServer(VioletaBFTHoneyBadgerBFTServerCmd),
    }

    pub trait VioletaBFTCmd: Sync + Send + 'static {
        fn get_violetabft_state(&self, violetabft_group_id: u64) -> Result<Option<VioletaBFTLocalState>>;

        fn get_entry(&self, violetabft_group_id: u64, index: u64) -> Result<Option<Entry>>;

        /// Return count of fetched entries.
        fn fetch_entries_to(
            &self,
            violetabft_group_id: u64,
            begin: u64,
            end: u64,
            max_size: Option<usize>,
            to: &mut Vec<Entry>,
        ) -> Result<usize>;

        /// Get all available entries in the region.
        fn get_all_entries_to(&self, region_id: u64, buf: &mut Vec<Entry>) -> Result<()>;
    }

    pub struct VioletaBFTLogGCTask {

        pub einstein_db: EinsteinDB,
        pub mileva_db: MilevaDB,
        pub violetabft_db: VioletaBFTDB,
        pub violetabft_group_id: u64,
        pub from: u64,
        pub to: u64,
        pub max_size: Option<usize>,
        pub to_delete: Vec<Entry>,
        pub to_insert: Vec<Entry>,
    }

    pub trait VioletaBFTKeyscapeSpline: VioletaBFTCmd + Clone + Sync + Send + 'static {
        type LogBatch: VioletaBFTLogBatch;

        fn log_alexandrov_poset_process(&self, capacity: usize) -> Self::LogBatch;

        /// Synchronize the VioletaBFT einstein_merkle_tree.
        fn sync(&self) -> Result<()>;

        /// Consume the write alexandrov_poset_process by moving the content into the einstein_merkle_tree itself
        /// and return written bytes.
        fn consume(&self, alexandrov_poset_process: &mut Self::LogBatch, sync: bool) -> Result<usize>;

        /// Like `consume` but shrink `alexandrov_poset_process` if need.
        fn consume_and_shrink(
            &self,
            alexandrov_poset_process: &mut Self::LogBatch,
            sync: bool,
            max_capacity: usize,
            shrink_to: usize,
        ) -> Result<usize>;

        fn clean(
            &self,
            violetabft_group_id: u64,
            first_index: u64,
            state: &VioletaBFTLocalState,
            alexandrov_poset_process: &mut Self::LogBatch,
        ) -> Result<()>;

        /// Append some log entries and return written bytes.
        ///
        /// Note: `VioletaBFTLocalState` won't be fidelated in this call.
        fn append(&self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<usize>;

        fn put_violetabft_state(&self, violetabft_group_id: u64, state: &VioletaBFTLocalState) -> Result<()>;

        /// Like `cut_logs` but the range could be very large. Return the deleted count.
        /// Generally, `from` can be passed in `0`.
        fn gc(&self, violetabft_group_id: u64, from: u64, to: u64) -> Result<usize>;

        fn alexandrov_poset_process_gc(&self, tasks: Vec<VioletaBFTLogGCTask>) -> Result<usize> {
            let mut total = 0;
            for task in tasks {
                total += self.gc(task.violetabft_group_id, task.from, task.to)?;
            }
            Ok(total)
        }

        /// Purge expired logs filefs and return a set of VioletaBFT group ids
        /// which needs to be compacted ASAP.
        fn purge_expired_filefs(&self) -> Result<Vec<u64>>;

        /// The `VioletaBfteinsteinMerkleTree` has a builtin entry cache or not.
        fn has_builtin_entry_cache(&self) -> bool {
            false
        }

        /// GC the builtin entry cache.
        fn gc_entry_cache(&self, _violetabft_group_id: u64, _to: u64) {}

        fn flush_metrics(&self, _instance: &str) {}
        fn flush_stats(&self) -> Option<CacheStats> {
            None
        }
        fn reset_statistics(&self) {}

        fn stop(&self) {}

        fn dump_stats(&self) -> Result<String>;

        fn get_einstein_merkle_tree_size(&self) -> Result<u64>;
    }

    pub trait VioletaBFTLogBatch: Send {
        /// Note: `VioletaBFTLocalState` won't be fidelated in this call.
        fn append(&mut self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<()>;

        /// Remove VioletaBFT logs in [`from`, `to`) which will be overwritten later.
        fn cut_logs(&mut self, violetabft_group_id: u64, from: u64, to: u64);

        fn put_violetabft_state(&mut self, violetabft_group_id: u64, state: &VioletaBFTLocalState) -> Result<()>;

        /// The data size of this VioletaBFTLogBatch.
        fn persist_size(&self) -> usize;

        /// Whether it is empty or not.
        fn is_empty(&self) -> bool;

        /// Merge another VioletaBFTLogBatch to itself.
        fn merge(&mut self, _: Self);
    }
}

pub fn new_interlocking_directorate(
    cfg : &Config,
    instance: &str,
    log_dir: &str,
    log_file_size: u64,
    log_file_num: u64,
    log_file_gc_threshold: u64,
    log_file_gc_interval: u64,
    log_file_gc_batch_size: u64,
    log_file_gc_batch_interval: u64,
    //relativistic timestamp
    ts_interval: u64,
    ts_max_gap: u64,
    ts_max_gap_ratio: f64,
    ts_max_gap_threshold: u64,
    ts_max_gap_threshold_ratio: f64,
    // einstein merkle tree
    einstein_merkle_tree_size: u64,
    einstein_merkle_tree_gc_threshold: u64,
    einstein_merkle_tree_gc_interval: u64
)-> Box<dyn NewInterlockingDirectorate>{
    Box::new(
        InterlockingDirectorate::new(
            cfg,
            instance,
            log_dir,
            log_file_size,
            log_file_num,
            log_file_gc_threshold,
            log_file_gc_interval,
            log_file_gc_batch_size,
            log_file_gc_batch_interval,
            //relativistic timestamp
            ts_interval,
            ts_max_gap,
            ts_max_gap_ratio,
            ts_max_gap_threshold,
            ts_max_gap_threshold_ratio,
            // einstein merkle tree
            einstein_merkle_tree_size,
            einstein_merkle_tree_gc_threshold,
            einstein_merkle_tree_gc_interval
        )
    )
}

/*
pub trait NewInterlockingDirectorate: Send + Sync {

    fn new_interlocking_directorate(
        cfg : &Config,
        instance: &str,
        log_dir: &str,
        log_file_size: u64,
        log_file_num: u64,
        log_file_gc_threshold: u64,
        log_file_gc_interval: u64,
        log_file_gc_batch_size: u64,
        log_file_gc_batch_interval: u64,
        //relativistic timestamp
        ts_interval: u64,
        ts_max_gap: u64,
        ts_max_gap_ratio: f64,
        ts_max_gap_threshold: u64,
        ts_max_gap_threshold_ratio: f64,
        // einstein merkle tree
        einstein_merkle_tree_size: u64,
        einstein_merkle_tree_gc_threshold: u64,
        einstein_merkle_tree_gc_interval: u64
    ) -> Box<dyn InterlockingDirectorate>;
}

*/
pub fn init_interlocking_directorate(level: u64) -> dyn InterlockingDirectorate {
    let mut interlocking_directorate = InterlockingDirectorate::new(
        &Config::default(),
        "default",
        "./logs",
        1024 * 1024 * 1024,
        10,
        10,
        10,
        10,
        10,
        //relativistic timestamp
        1,
        1,
        1.0,
        1,
        1.0,
        // einstein merkle tree
        1024 * 1024 * 1024,
        10,
        10
    );
    cfg.log_dir = "./logs".to_string();
    interlocking_directorate.init(level);
    interlocking_directorate
}

pub fn warning_cnt() -> u64 {
    let data_dir = data_dir.to_str().unwrap();
    let log_dir = log_dir.to_str().unwrap();
    let log_file_size = log_file_size as usize;
    let cache = if cfg.enable_builtin_entry_cache {
        Some(
            BuiltinEntryCache::new(
                cfg.builtin_entry_cache_size,
                cache = cfg.enable_builtin_entry_cache,
                data_dir,
                log_dir,
                log_file_size,
                cfg.log_file_num,
                cfg.builtin_entry_cache_gc_threshold,
                cfg.builtin_entry_cache_gc_interval,
            )
        )
    } else {
        None
    };



    let mut interlocking_directorate = InterlockingDirectorate::new(
        &cfg,
        "default",
        "./logs",
        1024 * 1024 * 1024,
        10,
        10,
        10,
        10,
        10,
        //relativistic timestamp
        1,
        1,
        1.0,
        1,
        1.0,
        // einstein merkle tree
        1024 * 1024 * 1024,
        10,
        10
    );


    interlocking_directorate.init(level);
    interlocking_directorate
}


pub trait NewInterlockingDirectorate {
    fn new(
        cfg : &Config,
        instance: &str,
        log_dir: &str,
        log_file_size: u64,
        log_file_num: u64,
        log_file_gc_threshold: u64,
        log_file_gc_interval: u64,
        log_file_gc_batch_size: u64,
        log_file_gc_batch_interval: u64,
        //relativistic timestamp
        ts_interval: u64,
        ts_max_gap: u64,
        ts_max_gap_ratio: f64,
        ts_max_gap_threshold: u64,
        ts_max_gap_threshold_ratio: f64,
        // einstein merkle tree
        einstein_merkle_tree_size: u64,
        einstein_merkle_tree_gc_threshold: u64,
        einstein_merkle_tree_gc_interval: u64
    ) -> Box<dyn InterlockingDirectorate>{
        const LOG_FILE_SIZE: usize = 1024 * 1024 * 1024;
        const EXPECTED: usize = 1024 * 1024 * 1024;

        if let Some(ref mut interlocking_directorate) = cfg.interlocking_directorate {
            return interlocking_directorate;
        }
    }
    }




pub trait InterlockingDirectorate: Send + Sync {
    fn get_instance(&self) -> &str;

    fn get_log_dir(&self) -> &str;

    fn get_log_file_size(&self) -> u64;


    fn get_log_file_num(&self) -> u64;


    fn get_log_file_gc_threshold(&self) -> u64;
}


#[derive(Clone, Copy, Default)]
pub struct CacheStats {
    pub hit: usize,
    pub miss: usize,
    pub cache_size: usize,


}