// Copyright 2022 einstein_db Project Authors. Licensed under Apache-2.0.

use crate::config::{Config, ConfigSource, ConfigValue, ConfigValueType};
use crate::error::{Error, ErrorKind};
use crate::util::escape;
//Merkle tree
use crate::EinsteinDB::MerkleTree;
use crate::{EinsteinDB, EinsteinDB_FOUNDATIONDB_DRIVER, EinsteinDB_FOUNDATIONDB_DRIVER_TOML};
use chrono::NaiveDateTime;
use futures::channel::oneshot;
use futures::future::{self, Future};
use std::collections::HashMap;
use crate::util::escape::escape_key;
use crate::util::escape::escape_value;
use crate::util::escape::unescape_key;
use crate::util::escape::unescape_value;
use crate::gravity::src::config::{Config as GravityConfig, ConfigSource as GravityConfigSource};
use crate::gravity::src::config::{ConfigValue as GravityConfigValue, ConfigValueType as GravityConfigValueType};

pub const DEFAULT_CONFIG_FILE: &str = "config.toml";
pub const DEFAULT_CONFIG_DIR: &str = "./config";
pub const DEFAULT_CONFIG_PATH: &str = "./config/config.toml";

pub const EINSTEINDB_INTER_WOTS_W: usize = 16; // Only implemented for 16
pub const EINSTEINDB_INTER_WOTS_L: usize = 6; // Only implemented for 6
pub const EINSTEINDB_INTER_WOTS_LOG_W: usize = 4; // Only implemented for 4
pub const EINSTEINDB_INTER_WOTS_LOG_L: usize = 2; // Only implemented for 2
/*
pub const WOTS_LOG_ELL1: usize = 6; // Implicitly depends on HASH_SIZE and W
pub const WOTS_ELL1: usize = 1 << WOTS_LOG_ELL1;
pub const WOTS_CHKSUM: usize = 3; // Implicitly depends on W and ELL1
pub const WOTS_ELL: usize = WOTS_ELL1 + WOTS_CHKSUM;

 */

pub const EINSTEINDB_INTERLOCKING_TAU: usize = 16;
pub const EINSTEINDB_INTERLOCKING_RATIO: usize = 4;
pub const EINSTEINDB_INTERLOCKING_MAX_RATIO: usize = 8;

const EINSTEINDB_DAGGER_K: usize = 24;
const EINSTEINDB_DAGGER_M: usize = 16;

use EinsteinDB::{EinsteinDB_FOUNDATIONDB_DRIVER, EinsteinDB_FOUNDATIONDB_DRIVER_TOML};
use einstein_db::*;

//InterlockingMultiplexerSync is the default interlocking multiplexer.
pub const EINSTEINDB_INTERLOCKING_MULTIPLEXER_SYNC: &str = "sync";

pub trait InterlockingDirectorate {
    //nodes that are interlocked with this node.
    fn get_interlocked_nodes(&self) -> Vec<String>;
    //nodes that are interlocked with this node.
    fn get_interlocked_nodes_with_config(&self, config: &Config) -> Vec<String>;
}


pub trait InterlockingMultiplexer {
    //Syncer is the default syncer.
    fn syncer(&self) -> &str;
    fn syncer_mut(&mut self) -> &mut str;
    fn interlocking_tau(&self) -> usize;
    fn interlocking_tau_mut(&mut self) -> &mut usize;
}

impl InterlockingMultiplexer for Config {
    fn syncer(&self) -> &str {
        height: self.get_value("interlocking.syncer").unwrap().as_str().unwrap();
        self.get_str("interlocking.syncer").unwrap_or(EINSTEINDB_INTERLOCKING_MULTIPLEXER_SYNC)
    }

     fn interlocking_tau(&self) -> usize {
        height: self.get_value("interlocking.tau").unwrap().as_usize().unwrap();
        self.get_usize("interlocking.tau").unwrap_or(EINSTEINDB_INTERLOCKING_TAU)
    }
}

/*
 fn syncer_mut(&mut self) -> &mut str {
     height: self.get_value("interlocking.syncer").unwrap().as_str().unwrap();
     self.get_str_mut("interlocking.syncer").unwrap_or(EINSTEINDB_INTERLOCKING_MULTIPLEXER_SYNC)
 }


 pub fn interlocking_tau_mut(&mut self) -> &mut usize {
    height: self.get_value("interlocking.tau").unwrap().as_usize().unwrap();
    self.get_usize_mut("interlocking.tau").unwrap_or(EINSTEINDB_INTERLOCKING_TAU)
}

 */

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MerkleTreeLSHBuffer {
    pub lsh_buffer: Vec<u8>,
    height: usize,

    causet_locale: <EinsteinDB::CausetSquuidStore as merkle_store_fdb_2_einstein_db>::Locale,
    pub lsh_buffer_size: usize, // in bytes
}

///! `Config` represents the configuration of einstein_db.
/// It is a wrapper of `toml::Value` and provides a `load_from` method to
/// load configuration from a file.
/// It also provides a `merge_from` method to merge configuration from a
/// `toml::Value` to the current configuration.
/// It also provides a `merge_from_toml` method to merge configuration from a
///       string to the current configuration.
impl MerkleTreeLSHBuffer {
    pub fn new(lsh_buffer: Vec<u8>, height: usize, causet_locale: <EinsteinDB::CausetSquuidStore as merkle_store_fdb_2_einstein_db>::Locale) -> Self {
        MerkleTreeLSHBuffer {
            lsh_buffer,
            height,
            causet_locale, // in bytes
            lsh_buffer_size: lsh_buffer.len(), // in bytes
        }
    }
}

///! `Config` represents the configuration of einstein_db.
/// It is a wrapper of `toml::Value` and provides a `load_from` method to
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MerkleTreeLSH {
    pub lsh_buffer: Vec<u8>, // lsh buffer is a merkle tree sub-tree in bytes
    pub height: usize,
    pub lsh_buffer_size: usize, // in bytes
}


///! `MerkleTreeLSH` represents the overall data model of einstein_db causetid transactions in mvsr

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MerkleTreeLSHConfig {
    pub lsh_buffer: Vec<u8>,
    pub height: usize,
    //uuid
    pub allegro_poset_uuid: String,
    pub lsh_buffer_size: usize, // in bytes
}


fn new(lsh_buffer: Vec<u8>, height: usize, allegro_poset_uuid: String, causet_poset_uuid::< einstein_db::causet_squuid_store::locale >: String, lsh_buffer_size: usize) -> Self {
        g {

            lsh_buffer,
            height,
            allegro_poset_uuid,
            lsh_buffer_size, // in bytes
        }
    }


//slice and fill causet locale buffer
fn fill_causet_locale_buffer(lsh_buffer: &mut Vec<u8>, height: usize, causet_locale: <EinsteinDB::CausetSquuidStore as merkle_store_fdb_2_einstein_db>::Locale) {

    let mut lsh_buffer_size = 0;
    let mut lsh_buffer_size_bytes = 0;
    let mut lsh_buffer_size_bytes_remainder = 0;
    let mut lsh_buffer_size_bytes_remainder_2 = 0;
    let mut lsh_buffer_size_bytes_remainder_3 = 0;
    let mut lsh_buffer_size_bytes_remainder_4 = 0;
    let mut lsh_buffer_size_bytes_remainder_5 = 0;
    let mut lsh_buffer_size_bytes_remainder_6 = 0;
    let mut lsh_buffer_size_bytes_remainder_7 = 0;
    let mut lsh_buffer_size_bytes_remainder_8 = 0;
    let mut lsh_buffer_size_bytes_remainder_9 = 0;
    let mut lsh_buffer_size_bytes_remainder_10 = 0;
    let mut lsh_buffer_size_bytes_remainder_11 = 0;
    let mut lsh_buffer_size_bytes_remainder_12 = 0;
    let mut lsh_buffer_size_bytes_remainder_13 = 0;
    let mut lsh_buffer_size_bytes_remainder_14 = 0;
    let mut lsh_buffer_size_bytes_remainder_15 = 0;
    let mut lsh_buffer_size_bytes_remainder_16 = 0;
    let mut lsh_buffer_size_bytes_remainder_17 = 0;
    let mut lsh_buffer_size_bytes_remainder_18 = 0;
    let mut lsh_buffer_size_bytes_remainder_19 = 0;
    let mut lsh_buffer_size_bytes_remainder_20 = 0;
    let mut lsh_buffer_size_bytes_remainder_21 = 0;
    let mut lsh_buffer_size_bytes_remainder_22 = 0;
    let mut lsh_buffer_size_bytes_remainder_23 = 0;

}





     //merkle tree height of btree
    fn merkle_tree_height(merkle_tree_height: usize) {
        let mut merkle_tree_height = merkle_tree_height;
        let mut merkle_tree_height_bytes = 0;
        let mut merkle_tree_height_bytes_bytes = 0;
        merkle_tree_height_bytes = merkle_tree_height_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes_bytes = merkle_tree_height_bytes_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes = merkle_tree_height_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes_bytes = merkle_tree_height_bytes_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes = merkle_tree_height_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes_bytes = merkle_tree_height_bytes_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes = merkle_tree_height_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes_bytes = merkle_tree_height_bytes_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes = merkle_tree_height_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes_bytes = merkle_tree_height_bytes_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes = merkle_tree_height_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes_bytes = merkle_tree_height_bytes_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes = merkle_tree_height_bytes + merkle_tree_height.len();
        merkle_tree_height_bytes_bytes = merkle_tree_height_bytes_bytes + merkle_tree_height.len();
     }


        pub fn lsh_buffer<T>(lsh_buffer: Vec<u8>, height: usize, allegro_poset_uuid: String, causet_poset_uuid::< einstein_db::causet_squuid_store::locale >: String, lsh_buffer_size: usize) -> Self {

            let mut lsh_buffer = lsh_buffer;
            let mut height = height;
            let mut allegro_poset_uuid = allegro_poset_uuid;
            let mut causet_poset_uuid = causet_poset_uuid;
            let mut lsh_buffer_size = lsh_buffer_size;
            let mut lsh_buffer_size_bytes = 0;
            let mut lsh_buffer_size_bytes_bytes = 0;
            lsh_buffer_size_bytes = lsh_buffer_size_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes_bytes = lsh_buffer_size_bytes_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes = lsh_buffer_size_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes_bytes = lsh_buffer_size_bytes_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes = lsh_buffer_size_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes_bytes = lsh_buffer_size_bytes_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes = lsh_buffer_size_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes_bytes = lsh_buffer_size_bytes_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes = lsh_buffer_size_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes_bytes = lsh_buffer_size_bytes_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes = lsh_buffer_size_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes_bytes = lsh_buffer_size_bytes_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes = lsh_buffer_size_bytes + lsh_buffer.len();
            lsh_buffer_size_bytes_bytes = lsh_buffer_size_bytes_bytes + lsh_buffer.len();

        }



        //Primary API to access causet_locales
        pub fn get_config(config_path: &str) -> Result<Config, Error> {
            let mut config = Config::new();
            config.load_from_file(config_path)?;
            Ok(config)
        }

        pub fn get_config_from_toml(config_toml: &str) -> Result<Config, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml)?;
            Ok(config)
        }

        pub struct Config {
            pub num_threads: usize,
            pub stream_channel_window: usize,
            /// The timeout for going back into normal mode from import mode.
            ///
            /// Default is 10m.
            pub import_mode_timeout: ReadableDuration,
        }



        impl Config {
            pub fn new() -> Self {
                Config {
                    num_threads: num_cpus::get(),
                    stream_channel_window: 1024,
                    import_mode_timeout: ReadableDuration::minutes(10),
                }
            }

            pub fn load_from_file(&mut self, path: &str) -> Result<(), Error> {
                let mut file = File::open(path)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                self.load_from_toml(&contents)
            }

            pub fn load_from_toml(&mut self, toml: &str) -> Result<(), Error> {
                let value = toml::from_str(toml)?;
                self.merge_from(value)
            }

            pub fn merge_from(&mut self, value: toml::Value) -> Result<(), Error> {
                let value = value.as_table().ok_or(Error::InvalidConfigFormat)?;
                self.merge_from_toml(value)
            }

            pub fn merge_from_toml(&mut self, value: &toml::value::Table) -> Result<(), Error> {
                if let Some(num_threads) = value.get("num_threads") {
                    self.num_threads = num_threads.as_integer().ok_or(Error::InvalidConfigFormat)? as usize;
                }
                if let Some(stream_channel_window) = value.get("stream_channel_window") {
                    self.stream_channel_window = stream_channel_window.as_integer().ok_or(Error::InvalidConfigFormat)? as usize;
                }
                if let Some(import_mode_timeout) = value.get("import_mode_timeout") {
                    self.import_mode_timeout = ReadableDuration::from_str(import_mode_timeout.as_str().ok_or(Error::InvalidConfigFormat)?)?;
                }
                Ok(())
            }
        }

        pub fn get_config_from_toml_file(config_toml_file: &str) -> Result<Config, Error> {
            let mut config = Config::new();
            config.load_from_toml_file(config_toml_file)?;
            Ok(config)
        }

        pub fn get_config_from_toml_file_with_default(config_toml_file: &str, default_config: &Config) -> Result<Config, Error> {
            let mut config = Config::new();
            config.load_from_toml_file_with_default(config_toml_file, default_config)?;
            Ok(config)
        }











