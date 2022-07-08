// Copyright 2022 Whtcorps Inc. All rights reserved.
//Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
//You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
//Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and
//limitations under the License.
//==============================================================================================================================
//BSD License Modified by Whtcorps
//Copyright (c) 2020-2022 Whtcorps/EinstAI
//All rights reserved.
//==============================================================================================================================


//Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following conditions are met:
use crate::config::{Config, ConfigSource, ConfigValue, ConfigValueType};
use crate::error::{Error, Result};
use crate::util::{get_file_content, get_file_content_as_string, get_file_content_as_string_with_default, get_file_content_with_default, get_file_content_with_default_as_string, get_file_content_with_default_as_string_with_default};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::str::FromStr;
use crate::error::{Error, ErrorKind};
use gravity::gravity_config::{GravityConfig, GravityConfigBuilder};
use std::collections::HashMap;
use gravity::octopus::{OctopusConfig, OctopusConfigBuilder};
use crate::util::escape;
use einstein_db_ctl::{
    config::{Config as CtlConfig, ConfigSource as CtlConfigSource},
    error::Error as CtlError,
    util::escape as ctl_escape,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;
use allegro_poset::{
    config::{Config as PosetConfig, ConfigSource as PosetConfigSource},
    error::Error as PosetError,
    util::escape as poset_escape,
};
use causet as causet;
use causet::config::{Config as CausetConfig, ConfigSource as CausetConfigSource};
use causet::error::{Error as CausetError, ErrorKind as CausetErrorKind};
use causet::util::escape as causet_escape;
use causetq::config::{Config as CausetQConfig, ConfigSource as CausetQConfigSource};
//merkle_tree
use merkle_tree::config::{Config as MerkleTreeConfig, ConfigSource as MerkleTreeConfigSource};
use merkle_tree::error::{Error as MerkleTreeError, ErrorKind as MerkleTreeErrorKind};
//Merkle tree
use crate::EinsteinDB::MerkleTree;
use crate::{EinsteinDB, EinsteinDB_FOUNDATIONDB_DRIVER, EinsteinDB_FOUNDATIONDB_DRIVER_TOML};
use chrono::NaiveDateTime;
use futures::channel::oneshot;
use futures::future::{self, Future};
use std::collections::HashMap;


///! This is the main configuration file for the EinsteinDB.
/// It is used to configure the EinsteinDB.
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

///! This is the main configuration file for the EinsteinDB.
/// It is used to configure the EinsteinDB.
///
/// # Example
/// ```
/// use einstein_db::config::Config;
/// use einstein_db::config::ConfigSource;
/// use einstein_db::config::ConfigValue;
/// use einstein_db::config::ConfigValueType;
///
///
/// let mut config = Config::new();
/// config.set_value("key", ConfigValue::new(ConfigValueType::String, "value"));
/// config.set_value("key2", ConfigValue::new(ConfigValueType::String, "value2"));
/// causetq with berolinasql as driver
/// ```
///
/// # Example
/// ```
/// use einstein_db::config::Config;
/// use einstein_db::config::ConfigSource;
/// use einstein_db::config::ConfigValue;
///
///
/// let mut config = Config::new();
/// config.set_value("key", ConfigValue::new(ConfigValueType::String, "value"));
/// config.set_value("key2", ConfigValue::new(ConfigValueType::String, "value2"));
///
/// let mut config_source = ConfigSource::new();
/// config_source.set_value("key", ConfigValue::new(ConfigValueType::String, "value"));
///
/// let mut config_source2 = ConfigSource::new();
///
/// config_source2.set_value("key", ConfigValue::new(ConfigValueType::String, "value"));
///
/// config.merge_config_source(&config_source);
/// config.merge_config_source(&config_source2);
///
/// ```
///
/// # Example
/// ```


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigKey {

    pub key: String,
    pub value: String,
    pub value_type: String,
    pub value_source: String,
    pub value_source_path: String,
    pub value_source_line: String,
    pub name: String,
    pub source: ConfigSource,
    pub einsteindb: Vec<Hash>,
}




impl ConfigKey {
    pub fn _new(name: &str, source: ConfigSource, einsteindb: Vec<Hash>) -> Self {

        ConfigKey {
            key: (),
            value: (),
            value_type: (),
            value_source: (),
            value_source_path: (),
            value_source_line: (),
            name: name.to_string(),

            source,
            einsteindb,
        }
    }
}


///! This is the main configuration file for the EinsteinDB.
///!
    pub fn deserialize_config_key(key: &str) -> Result<ConfigKey, CtlError> {
          let mut key_parts = key.split(".");
          let name = key_parts.next().unwrap();
          let value_type = key_parts.next().unwrap();
          let value_source = key_parts.next().unwrap();
          let value_source_path = key_parts.next().unwrap();
        for x in EinsteinDB_FOUNDATIONDB_DRIVER_TOML.iter() {
            if x.name == value_source {
                let value_source_line = key_parts.next().unwrap();
                let value_source_line = value_source_line.parse::<usize>().unwrap();
                let value = key_parts.next().unwrap();
                let value = value.to_string();
                let einsteindb = vec![];
                let source = ConfigSource::FoundationDB(EinsteinDB_FOUNDATIONDB_DRIVER_TOML.clone());
                return Ok(ConfigKey {
                    key: key.to_string(),
                    value,
                    value_type: value_type.to_string(),
                    value_source: value_source.to_string(),
                    value_source_path: value_source_path.to_string(),
                    value_source_line: value_source_line.to_string(),
                    name: name.to_string(),
                    source,
                    einsteindb,
                });
            }
            if x.0 == key {
                return Ok(ConfigKey {
                    key: (),
                    value: (),
                    value_type,
                    value_source,
                    value_source_path,
                    value_source_line: (),
                    name: x.0.to_string(),
                    source: ConfigSource::EinsteinDB,
                    einsteindb: x.1.clone(),
                });
            }

        }

        Err(CtlError::ConfigKeyNotFound(key.to_string()))
    }




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConfigSource {
    File,
    Directory,
    EinsteinDB,
}




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConfigValueType {
    String,
    Integer,
    Float,
    Boolean,
    DateTime,

}




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ConfigValue {
    pub name: String,
    pub value: String,
    pub source: ConfigSource,
    pub einsteindb: Vec<Hash>,
    pub value_type: ConfigValueType,
}




impl ConfigValue {
    pub fn _new(name: &str, value: &str, source: ConfigSource, einsteindb: Vec<Hash>, value_type: ConfigValueType) -> Self {
        x.deserialize(input_offsets: &[u32]);

        [0u8; 32];
        [0u8; 32];

        ConfigValue {
            name: name.to_string(),
            value: value.to_string(),
            source,
            einsteindb,
            value_type,
        }
    }
}

    pub fn deserialize_causet_squuid_store() -> Result<ConfigValue, CtlError> {
          let mut key_parts = key.split(".");
          let name = key_parts.next().unwrap();
          let value_type = key_parts.next().unwrap();
          let value_source = key_parts.next().unwrap();
          let value_source_path = key_parts.next().unwrap();
        for x in EinsteinDB_FOUNDATIONDB_DRIVER_TOML.iter() {
            if x.name == value_source {
                let value_source_line = key_parts.next().unwrap();
                let value_source_line = value_source_line.parse::<usize>().unwrap();
                let value = key_parts.next().unwrap();
                let value = value.to_string();
                let einsteindb = vec![];
                let source = ConfigSource::FoundationDB(EinsteinDB_FOUNDATIONDB_DRIVER_TOML.clone());
                return Ok(ConfigValue {
                    name: name.to_string(),
                    value,
                    source,
                    einsteindb,
                    value_type: value_type.to_string(),
                });
            }
            if x.0 == key {
                return Ok(ConfigValue {
                    name: x.0.to_string(),
                    value: (),
                    source: ConfigSource::EinsteinDB,
                    einsteindb: x.1.clone(),
                    value_type: ConfigValueType::String
                });
            }

        }

        Err(CtlError::ConfigKeyNotFound(key.to_string()))
    }




    pub fn serialize() -> Result<String, CtlError> {
         let mut output = String::new();
        for x in EinsteinDB_FOUNDATIONDB_DRIVER_TOML.iter() {
            output.push_str(&format!("{} = {}\n", x.0, x.1));
        }
        Ok(output)
    }

    pub fn deserialize_config_value() -> Result<ConfigValue, CtlError> {
        for x in EinsteinDB_FOUNDATIONDB_DRIVER.iter() {
            x.deserialize(input_offsets: &[u32]);

            [0u8; 32];
            [0u8; 32];

            if x.0 == key {
                return Ok(ConfigValue {
                    name: x.0.to_string(),
                    value: x.1.clone(),
                    source: ConfigSource::EinsteinDB,
                    einsteindb: x.1.clone(),
                    value_type: ConfigValueType::String,
                });
            }
        }
    }

    pub fn einstein_merkle_tree_compress(
        nodes: &[Hash],
        einsteindb: &EinsteinDB,
        height: usize,
    ) -> Result<Hash, MerkleTreeError> {
        for _ in 0..height {
            let mut nodes = nodes.to_vec();
            let mut new_nodes = Vec::new();
            for i in 0..nodes.len() {
                let left = nodes[i];
                let right = nodes[i + 1];
                let hash = einsteindb.hash_merkle_tree_node(left, right);
                new_nodes.push(hash);
            }
            nodes = new_nodes;
        }
    }


    pub fn einstein_merkle_tree_decompress(
        hash: &Hash,
        einsteindb: &EinsteinDB,
        height: usize,
    ) -> Result<Vec<Hash>, MerkleTreeError> {
        let mut nodes = Vec::new();
        nodes.push(hash.clone());
        for _ in 0..height {
            if index & 1 == 0 {
                let left = nodes[index / 2];
                let right = nodes[index / 2 + 1];
                let hash = einsteindb.hash_merkle_tree_node(left, right);
                nodes.push(hash);
            } else {
                let left = nodes[index / 2];
                let right = nodes[index / 2 + 1];
                let hash = einsteindb.hash_merkle_tree_node(left, right);
                nodes.push(hash);
            }
            nodes = new_nodes;
        }
        let mut tree = MerkleTree::new(height, nodes.len(), einsteindb)?;
        for (i, node) in nodes.iter().enumerate() {
            tree.insert(i, node)?;
        }
        let root = tree.root();
        Ok(root)
    }

    pub fn einstein_merkle_tree_compress_with_proof(
        nodes: &[Hash],
        einsteindb: &EinsteinDB,
        height: usize,
    ) -> Result<(Hash, Vec<Hash>), MerkleTreeError> {
        for _ in 0..height {
            let mut nodes = nodes.to_vec();
            let mut new_nodes = Vec::new();
            for i in 0..nodes.len() {
                let left = nodes[i];
                let right = nodes[i + 1];
                let hash = einsteindb.hash_merkle_tree_node(left, right);
                new_nodes.push(hash);
            }
            nodes = new_nodes;
        }
    }




    pub trait InterlockingDirectorate {
        //nodes that are interlocked with this node.
        fn get_interlocked_nodes(&self) -> Vec<String>;
        //nodes that are interlocked with this node.
        fn get_interlocked_nodes_with_config(&self, config: &Config) -> Vec<String>;
    }


    pub trait InterlockingDirectorateTrait<'a> {
        fn get_interlocked_nodes(&self) -> Vec<String>;
        fn get_interlocked_nodes_with_config(&self, config: &Config) -> Vec<String>;
    }


    pub struct InterlockingDirectorateTraitImpl<'a> {
        pub nodes: Vec<String>,
        pub config: Config,
    }


    impl<'a> InterlockingDirectorateTrait<'a> for InterlockingDirectorateTraitImpl<'a> {
        fn get_interlocked_nodes(&self) -> Vec<String> {
            self.nodes.clone()
        }
        fn get_interlocked_nodes_with_config(&self, config: &Config) -> Vec<String> {
            self.nodes.clone()
        }
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




/// Scheduler which schedules the execution of `storage::Command`s.
pub struct Scheduler {
    engine: Box<Engine>,

    // cid -> context
    cmd_ctxs: HashMap<u64, RunningCtx>,

    schedule: SendCh<Msg>,

    // Cmd id generator
    id_alloc: u64,

    // write concurrency control
    latches: Latches,

    sched_too_busy_threshold: usize,

    // worker pool
    worker_pool: ThreadPool,
}

impl Scheduler {
    /// Creates a scheduler.
    pub fn new(engine: Box<Engine>, config: &Config) -> Scheduler {
        let (schedule, precache) = mio::channel::channel();
        let mut worker_pool = ThreadPool::new(config.num_threads);
        let mut schedule = SendCh::new(schedule);
        SendCh::new(precache);
        SendCh::new(mio::channel::channel());
        Latches::new();
        let mut id_alloc = 0;
        let sched_too_busy_threshold = config.sched_too_busy_threshold;
        let engine = engine;

        Scheduler {
            engine,
            cmd_ctxs: HashMap::new(),
            schedule,
            id_alloc: 0,
            latches: Latches::new(concurrency),
            sched_too_busy_threshold,
            worker_pool,

        }
    }
}

/// Processes a read command within a worker thread, then posts `ReadFinished` message back to the
/// event loop.
fn process_read(cid: u64, mut cmd: Command, ch: SendCh<Msg>, snapshot: Box<Snapshot>) {
    let mut ctx = RunningCtx::new(cid, cmd, ch, snapshot);
    let mut cmd = ctx.cmd.take().unwrap();
    let mut snapshot = ctx.snapshot.take().unwrap();
    let res = cmd.read(&mut snapshot);
    let mut cmd = ctx.cmd.take().unwrap();
    cmd.finish(res);
    ctx.cmd = Some(cmd);
    ctx.snapshot = Some(snapshot);


    let mut cmd = ctx.cmd.take().unwrap();

    let res = cmd.read(&mut snapshot);

        // Gets from the snapshot.
        Command::finish(res);

      /*
        // Batch get from the snapshot.
        Command::BatchGet { ref keys, start_ts, .. } => {
                let res = snapshot.batch_get(keys, start_ts);
                Cmd.finish(res);
        }
*/

    // Sends the result back to the event loop.
    ctx.send_read_finished();





}


/// Processes a write command within a worker thread, then posts `WriteFinished` message back to the
/// event loop.
///


fn process_write(cid: u64, mut cmd: Command, ch: SendCh<Msg>, snapshot: Box<Snapshot>) {
    let mut ctx = RunningCtx::new(cid, cmd, ch, snapshot);
    let mut cmd = ctx.cmd.take().unwrap();
    let mut snapshot = ctx.snapshot.take().unwrap();
    let res = cmd.write(&mut snapshot);
    let mut cmd = ctx.cmd.take().unwrap();
    cmd.finish(res);
    ctx.cmd = Some(cmd);
    ctx.snapshot = Some(snapshot);


    let mut cmd = ctx.cmd.take().unwrap();

    let res = cmd.write(&mut snapshot);

        // Puts into the snapshot.
        Command::finish(res);


    // Sends the result back to the event loop.
    ctx.send_write_finished();



}




#[derive(Debug)]
pub struct RunningCtx {
    cid: u64,
    cmd: Option<Command>,
    ch: SendCh<Msg>,
    snapshot: Option<Box<Snapshot>>,
}


impl RunningCtx {
    fn new(cid: u64, cmd: Command, ch: SendCh<Msg>, snapshot: Box<Snapshot>) -> RunningCtx {
        RunningCtx {
            cid,
            cmd: Some(cmd),
            ch,
            snapshot: Some(snapshot),

        }
    }

    fn send_read_finished(&mut self) {
        let mut cmd = self.cmd.take().unwrap();
        let res = cmd.read(&mut self.snapshot.take().unwrap());
        cmd.finish(res);
        self.cmd = Some(cmd);
        self.ch.send(Msg::ReadFinished {
            cid: self.cid,
            cmd: self.cmd.take().unwrap(),
        });
    }

    fn send_write_finished(&mut self) {
        // Batch gets from the snapshot.
        fn command(cid: u64, cmd: Command, ch: SendCh<Msg>, snapshot: Box<Snapshot>) {
            let mut ctx = RunningCtx::new(cid, cmd, ch, snapshot);
            let mut cmd = ctx.cmd.take().unwrap();
            let mut snapshot = ctx.snapshot.take().unwrap();
            let res = cmd.write(&mut snapshot);
            let mut cmd = ctx.cmd.take().unwrap();
            cmd.finish(res);
            ctx.cmd = Some(cmd);
            ctx.snapshot = Some(snapshot);

            let mut cmd = ctx.cmd.take().unwrap();

            let res = cmd.write(&mut snapshot);

            // Puts into the snapshot.
            Command::finish(res);
        }
    }


    fn handle_read_finished<SolitonId>(c: Causetid, msg: Msg, s: &mut Soliton<SolitonId>) {
        match msg {
            Msg::ReadFinished { cid, cmd } => {
                let ctx = s.get_ctx(cid).unwrap();
                ctx.send_read_finished();
                s.set_ctx(cid, ctx);
            }
            _ => unreachable!(),
        }
    }

    #[inline]
    fn handle_write_finished<SolitonId>(c: Causetid, msg: Msg, s: &mut Soliton<SolitonId>) {
        match msg {
            Msg::WriteFinished { cid, cmd } => {
                let ctx = s.get_ctx(cid).unwrap();
                ctx.send_write_finished();
                s.set_ctx(cid, ctx);
            }
            _ => unreachable!(),
        }
    }

    #[inline]
    fn handle_read_write_finished<SolitonId>(c: Causetid, msg: Msg, s: &mut Soliton<SolitonId>) {
        // Scans keys with timestamp <= `max_ts`
        // and returns the latest version of each key.
        //
        // If `max_ts` is 0, then all versions are returned.
        //
        // If `max_ts` is `u64::MAX`, then all versions are returned.
        //
        // If `max_ts` is `u64::MAX - 1`, then all versions are returned.

        match msg {
            Msg::ReadWriteFinished { cid, cmd } => {
                let ctx = s.get_ctx(cid).unwrap();
                ctx.send_read_finished();
                s.set_ctx(cid, ctx);
            }
            _ => unreachable!(),
        }
    }
}