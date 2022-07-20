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
//1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
//2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following disclaimer in the documentation and/or other materials provided with the distribution.
//3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products derived from this software without specific prior written permission.



///! This module contains the configuration for the application.
/// It is used to configure the application.
use std::fs::File;
use std::io::{BufRead, BufReader, ErrorKind, Read};
use std::path::Path;


use crate::util::escape;
use einstein_db_ctl::*;

use std::path::PathBuf;
use std::str::FromStr;
use allegro_poset::*;
use causet::*;
use chrono::NaiveDateTime;
use futures::channel::oneshot;
use futures::future::{self, Future};
use std::collections::HashMap;
use std::hash::Hash;
use std::process::Command;
use causet_def::Error;

use crate::berolinasql::{Error as BerolinaSqlError, ErrorKind as BerolinaSqlErrorKind};

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
    pub einsteindb: Vec<dyn Hash>,
}




impl ConfigKey {
    pub fn _new(name: &str, source: ConfigSource, einsteindb: Vec<dyn Hash>) -> Self {

        ConfigKey {
            key: String::new(),
            value: String::new(),
            value_type: String::new(),
            value_source: String::new(),
            value_source_path: String::new(),
            value_source_line: String::new(),
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
                    key: key.to_string(),
                    value: x.1.to_string(),
                    value_type: value_type.to_string(),
                    value_source: value_source.to_string(),
                    value_source_path: value_source_path.to_string(),
                    value_source_line: "0".to_string(),
                    name: x.0.to_string(),
                    source: ConfigSource::EinsteinDB,
                    einsteindb: x.1.clone(),
                });
            }

        }

        Err(CtlError::ConfigKeyNotFound(key.to_string()))
    }










impl ConfigValue {
    pub fn _new(name: &str, value: &str, source: ConfigSource, einsteindb: Vec<dyn Hash>, value_type: ConfigValueType) -> Self {
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



            }

            if x.0 == key {
                return Ok(ConfigValue {
                    name: x.0.to_string(),
                    value: x.1.to_string(),
                    source: ConfigSource::EinsteinDB,
                    einsteindb: x.1.clone(),
                    value_type: ConfigValueType::String
                });
            }



        }




        Err(CtlError::ConfigKeyNotFound(key.to_string()))
    }




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConfigValueType {
    String,
    Boolean,
    Integer,
    Float,
    Array,
    Object,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConfigSource {

    FoundationDB(Vec<(String, String)>),
    EinsteinDB,
}


    pub fn einstein_merkle_tree_compress() -> [u8; 4] {
        vec![];
        [0u8; 4]
    }

    pub fn einstein_merkle_tree_decompress() -> [u8; 4] {
        vec![];
        [0u8; 4]
    }

    pub fn einstein_merkle_tree_compress_node() -> [u8; 4] {
        vec![];
        [0u8; 4]
    }

    pub fn einstein_merkle_tree_decompress_node<'a, 'b>() -> [u8; 4] {
        vec![];
        [0u8; 4]
    }

    pub fn einstein_merkle_tree_compress_leaf() -> [u8; 4] {
        vec![];
        [0u8; 4]
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


    pub struct InterlockingDirectorateImpl {
        pub nodes: Vec<String>,
        pub config: Config,
    }



    pub struct InterlockingDirectorateImpl2 {
        pub nodes: Vec<String>,
        pub config: Config,
    }



    pub struct InterlockingDirectorateImpl3 {
        pub nodes: Vec<String>,
    }

    pub struct InterlockingDirectorateImpl4 {
        pub nodes: Vec<String>,

    }


    impl InterlockingDirectorate for InterlockingDirectorateImpl2 {
        fn get_interlocked_nodes(&self) -> Vec<String> {
            self.nodes.clone()
        }
        fn get_interlocked_nodes_with_config(&self, config: &Config) -> Vec<String> {
            self.nodes.clone()
        }
    }


    impl InterlockingDirectorate for InterlockingDirectorateImpl3 {
        fn get_interlocked_nodes(&self) -> Vec<String> {
            self.nodes.clone()
        }
        fn get_interlocked_nodes_with_config(&self, config: &Config) -> Vec<String> {
            self.nodes.clone()
        }
    }



#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InterlockingMultiplexer {
    pub nodes: Vec<String>,
    pub syncer: String,
    pub tau: usize,
}
pub struct MerkleTreeLSHBuffer {
    pub nodes: Vec<String>,
    pub lsh_buffer: Vec<u8>,
    height: usize,

    causet_locale: <EinsteinDB::CausetSquuidStore as merkle_store_fdb_2_einstein_db>::Locale,
    pub lsh_buffer_size: usize, // in bytes
    pub lsh_buffer_height: usize, // in bytes
    pub lsh_buffer_width: usize, // in bytes
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
            nodes: vec![],
            lsh_buffer,
            height,
            causet_locale, // in bytes
            lsh_buffer_size: lsh_buffer.len(), // in bytes
            lsh_buffer_height: 0,
            lsh_buffer_width: 0
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

//slice and fill causet locale buffer
fn fill_causet_locale_buffer(causet_locale: <EinsteinDB::CausetSquuidStore as merkle_store_fdb_2_einstein_db>::Locale) {}


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


        pub fn lsh_buffer<T>(lsh_buffer: Vec<u8>, causet_poset_uuid::< einstein_db::causet_squuid_store::locale >: String) -> Self {

            let mut lsh_buffer = lsh_buffer;
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
            lsh_buffer_size_bytes_bytes = lsh_buffer_size_bytes_bytes + lsh_buffer.len()
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
        }

        impl Default for Config {
            fn default() -> Self {
                Config::new()
            }
        }

        impl Config {


            fn load_from_file(&mut self, path: &str) -> Result<(), Error> {
                let mut file = File::open(path)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                self.load_from_toml(&contents)
            }

             fn load_from_toml(&mut self, toml: &str) -> Result<(), Error> {
                let value = toml::from_str(toml)?;
                self.merge_from(value)
            }

             fn merge_from(&mut self, value: toml::Value) -> Result<(), Error> {
                let value = value.as_table().ok_or(Error::InvalidConfigFormat)?;
                self.merge_from_toml(value)
            }

            fn merge_from_toml(&mut self, value: &toml::value::Table) -> Result<(), Error> {
                if let Some(num_threads) = value.get("num_threads") {
                    self.num_threads = num_threads.as_integer().ok_or(Error::InvalidConfigFormat)? as usize;
                }
if let Some(stream_channel_window) = value.get("stream_channel_window") {
                    self.stream_channel_window = stream_channel_window.as_integer().ok_or(Error::InvalidConfigFormat)? as usize;
                }

                if let Some(import_mode_timeout) = value.get("import_mode_timeout") {
                    self.import_mode_timeout = ReadableDuration::parse(import_mode_timeout.as_str().ok_or(Error::InvalidConfigFormat)?)?;
                }

                Ok(())
            }
        }

        impl Default for Config {
            fn default() -> Self {
                Config::new()
            }
        }

        impl Config {
            pub fn num_threads(&self) -> usize {
                self.num_threads
            }

            pub fn stream_channel_window(&self) -> usize {
                self.stream_channel_window
            }

            pub fn import_mode_timeout(&self) -> ReadableDuration {
                self.import_mode_timeout
            }

            pub fn load_from_toml(&mut self, toml: &str) -> Result<(), Error> {
                let value = toml::from_str(toml)?;
                self.merge_from(value)
            }
        }

        impl Default for Config {
            fn default() -> Self {
                Config::new()
            }
        }

        impl Config {
            pub fn load_from_file(&mut self, path: &str) -> Result<(), Error> {
                let mut file = File::open(path)?;
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                self.load_from_toml(&contents)
            }
        }


        impl Config {
            pub fn load_from_toml(&mut self, toml: &str) -> Result<(), Error> {
                let value = toml::from_str(toml)?;
                self.merge_from(value)
            }
        }

        impl Default for Config {
            fn default() -> Self {
                Config::new()
            }
        }




        impl Default for Config {
            fn default() -> Self {
                Config::new()
            }
        }

     //get num threads from config_toml_file_with_default

        pub fn get_num_threads_from_config_toml_file_with_default(config_toml_file_with_default: &str) -> Result<usize, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml_file_with_default)?;
            Ok(config.num_threads)
        }


         fn get_stream_channel_window_from_config_toml_file_with_default(config_toml_file_with_default: &str) -> Result<usize, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml_file_with_default)?;
            Ok(config.stream_channel_window)
        }

        pub fn get_import_mode_timeout_from_config_toml_file_with_default(config_toml_file_with_default: &str) -> Result<ReadableDuration, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml_file_with_default)?;
            Ok(config.import_mode_timeout)
        }

         fn get_import_mode_timeout_secs_from_config_toml_file_with_default(config_toml_file_with_default: &str) -> Result<u64, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml_file_with_default)?;
            Ok(config.import_mode_timeout.as_secs())
        }

        pub fn get_import_mode_timeout_millis_from_config_toml_file_with_default(config_toml_file_with_default: &str) -> Result<u64, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml_file_with_default)?;
            Ok(config.import_mode_timeout.as_millis())
        }

        pub fn get_import_mode_timeout_micros(config_toml_file_with_default: &str) -> Result<u64, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml_file_with_default)?;
            Ok(config.import_mode_timeout.as_micros())
        }

        pub fn get_import_mode_timeout_nanos(config_toml_file_with_default: &str) -> Result<u64, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml_file_with_default)?;
            Ok(config.import_mode_timeout.as_nanos())
        }

        pub fn get_import_mode_timeout_secs_f32(config_toml_file_with_default: &str) -> Result<f32, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml_file_with_default)?;
            Ok(config.import_mode_timeout.as_secs_f32())
        }

        pub fn get_import_mode_timeout_millis_f32(config_toml_file_with_default: &str) -> Result<f32, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml_file_with_default)?;
            Ok(config.import_mode_timeout.as_millis_f32())
        }

        pub fn get_import_mode_timeout_micros_f32(config_toml_file_with_default: &str) -> Result<f32, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml_file_with_default)?;
            Ok(config.import_mode_timeout.as_micros_f32())
        }

        pub fn get_import_mode_timeout_nanos_f32(config_toml_file_with_default: &str) -> Result<f32, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml_file_with_default)?;
            Ok(config.import_mode_timeout.as_nanos_f32())
        }

        pub fn get_import_mode_timeout_secs_f64(config_toml_file_with_default: &str) -> Result<f64, Error> {
            let mut config = Config::new();
            config.load_from_toml(config_toml_file_with_default)?;
            Ok(config.import_mode_timeout.as_secs_f64())
        }


    fn get_config() {
        if let Some(num_threads) = value.get("num_threads") {
            if let Some(num_threads_value) = num_threads.as_u64() {
                config.num_threads = num_threads_value as usize;
            }
        }
    }
        pub fn get_num_threads_from_config_toml_file() -> Result<usize, Error> {
            let mut file = File::open(path)?;
            let mut contents = String::new();
            file.read_to_string(&mut contents)?;
            toml::from_str(&contents)?;
            get_num_threads_from_config_toml_file_with_default(&contents)

        }

        pub fn get_config_from_tom_file(config_toml_file: &str) -> Result<Config, Error> {

            if let Ok(mut file) = File::open(config_toml_file) {
                let mut contents = String::new();
                file.read_to_string(&mut contents)?;
                toml::from_str(&contents)?;
                Ok(Config::new())
            } else {
                Err(Error::new(ErrorKind::Other, "Could not open config file"))
            }
        }


        pub fn get_config_from_toml_file_with_default(config_toml_file: &str, default_config: &Config) -> Result<Config, Error> {
            let mut config = Config::new();
            config.load_from_toml_file_with_default(config_toml_file, default_config)?;
            Ok(config)
        }

        pub fn get_config_from_toml_file_with_default_and_override(config_toml_file: &str, default_config: &Config, override_config: &Config) -> Result<Config, Error> {
            let mut config = Config::new();
            config.load_from_toml_file_with_default_and_override(config_toml_file, default_config, override_config)?;
            Ok(config)
        }

        pub fn get_config_from_toml_file_with_default_and_override_and_override_with_default(config_toml_file: &str, default_config: &Config, override_config: &Config, override_with_default_config: &Config) -> Result<Config, Error> {
            let mut config = Config::new();
            config.load_from_toml_file_with_default_and_override_and_override_with_default(config_toml_file, default_config, override_config, override_with_default_config)?;
            Ok(config)
        }







impl Scheduler {
    pub fn new(engine: Box<Engine>, sched_too_busy_threshold: usize) -> Self {
        Scheduler {

            id_alloc: 0,
            ch: (),
            cmd: (),
            snapshot: (),
            cid: 0,
            latches: Latches::new(),
            worker_pool: ThreadPool::new(num_cpus::get()),

            sched_too_busy_threshold: 0
        }
    }
}




impl Scheduler {
    pub fn schedule(&mut self, cmd: Command) {
        self.schedule.push(cmd);
    }
}


impl Scheduler {
    pub fn run(&mut self) {
        let mut schedule = self.schedule.clone();
        self.schedule.clear();
        self.worker_pool.execute(move || {
            for cmd in schedule {
                self.run_cmd(cmd);
            }
        });
    }
}


impl Scheduler {
    pub fn run_cmd(&mut self, cmd: Command) {
        match cmd {
            Command::Snapshot => {
                self.snapshot();
            }
            Command::Cmd(cmd) => {
                self.cmd(cmd);
            }
            Command::Ch(ch) => {
                self.ch(ch);
            }
        }
    }
}


impl Scheduler {
    pub fn snapshot(&mut self) {
        self.snapshot = self.latches.clone();
    }
}


impl Scheduler {
    pub fn ch(&mut self, ch: ()) {
        self.ch = ch;
    }
}


impl Scheduler {
    pub fn cmd(&mut self, cmd: ()) {
        self.cmd = cmd;
    }
}


impl Scheduler {
    pub fn cid(&mut self) -> u64 {
        self.cid += 1;
        self.cid
    }
}


impl Scheduler {
    pub fn latches(&mut self) -> &mut Latches {
        &mut self.latches
    }
}


impl Scheduler {
    pub fn sched_too_busy_threshold(&mut self) -> usize {
        self.sched_too_busy_threshold
    }
}




impl Scheduler {
    pub fn schedule(&mut self, cmd: Command) {
        self.schedule.push(cmd);
    }

    pub fn schedule_with_ctx(&mut self, cmd: Command, ctx: CommandContext) {
        self.schedule.push(cmd);
        self.cmd_ctx.insert(ctx.get_id(), ctx);
    }

    pub fn schedule_with_ctx_and_id(&mut self, cmd: std::process::Command, ctx: CommandContext, id: u64) {
        self.schedule.push(cmd);
        self.cmd_ctx.insert(id, ctx);
    }

    pub fn run_cmd(&mut self, cmd: Command) {
        let ctx = CommandContext::new(cmd, self);
        self.cmd_ctx.insert(ctx.get_id(), ctx);
        self.run_cmd_ctx(ctx);
    }

    pub fn schedule_with_ctx_and_id_and_latch(&mut self, ctx: CommandContext) {
        /// The name of the metric.
        const METRIC_NAME: &str = "scheduler_too_busy";

        /// The label for the metric.
        /// The label is the name of the scheduler.
        /// The value is the number of times the scheduler was too busy.
        /// The value is 0 if the scheduler was not too busy.

        let mut labels = HashMap::new();
        labels.insert("scheduler", self.get_name());
        labels.insert("scheduler_too_busy", "0");
        let mut counter = Counter::new(METRIC_NAME, "The number of times the scheduler was too busy.", labels).unwrap();
        counter.inc();


        let mut too_busy = false;
        if self.latches.len() > self.sched_too_busy_threshold {
            too_busy = true;
        }

        if too_busy {
            let mut counter = self.latches.get_counter(METRIC_NAME);
            counter.inc();
        }

        if too_busy {
            return;
        }

        self.run_cmd_ctx(ctx);
    }

    pub fn run_cmd_ctx(&mut self, cmd_ctx: CommandContext) {
        let cmd = cmd_ctx.get_cmd();
        let id = cmd_ctx.get_id();
        let ctx = cmd_ctx.get_ctx();
        let mut latch = Latch::new();
        latch.set_id(id);
        self.latches.insert(latch);
        let res = cmd.run(ctx);
        self.latches.remove(id);
    }

    pub fn get_name(&self) -> String {
        "scheduler".to_string()
    }

    pub fn get_name_mut(&mut self) -> &mut String {
        &mut self.name_mut
    }

    pub fn get_name_ref(&self) -> &String {
        &self.name_ref
    }

    pub fn get_name_ref_mut(&mut self) -> &mut String {
        &mut self.name_ref_mut
    }

    pub fn get_name_ref_ref(&self) -> &String {
        &self.name_ref_ref
    }

    pub fn get_name_ref_ref_mut(&mut self) -> &mut String {
        &mut self.name_ref_ref_mut
    }


    pub fn get_name_ref_ref_ref(&self) -> &String {
        self.worker_pool.execute(move || {
            let mut counter = self.latches.get_counter("scheduler_too_busy");
            counter.inc();
        })
            & self.name_ref_ref_ref
    }

    pub fn get_name_ref_ref_ref_mut(&mut self) -> &mut String {
        &mut self.name_ref_ref_ref_mut
    }
}








impl Scheduler {
    pub fn run_cmd_ctx_impl(&mut self, ctx: CommandContext) {
        let cmd = ctx.get_cmd();
        let id = ctx.get_id();
        let mut latch = ctx.get_latch();
        let mut too_busy = false;
        if self.latches.len() > self.sched_too_busy_threshold {
            too_busy = true;
        }

        if too_busy {
            let mut counter = self.latches.get_counter(METRIC_NAME);
            counter.inc();
        }

        if too_busy {
            return;
        }

        let res = cmd.execute(self.engine.as_mut());
        if res.is_ok() {
            latch.set_result(res.unwrap());
        } else {
            latch.set_error(res.unwrap_err());
        }
        self.latches.remove(id);
    }
}


impl Scheduler {
    pub fn get_cmd_ctx(&self, id: u64) -> Option<CommandContext> {
        self.cmd_ctx.get(&id).cloned()
    }

    pub fn get_cmd_ctx_mut(&mut self, id: u64) -> Option<CommandContext> {
        self.cmd_ctx.get_mut(&id).cloned()
    }

    pub fn remove_cmd_ctx(&mut self, id: u64) {
        self.cmd_ctx.remove(&id);
    }

    pub fn get_latch(&self, id: u64) -> Option<Latch> {
        self.latches.get(&id).cloned()
    }

    pub fn get_latch_mut(&mut self, id: u64) -> Option<Latch> {
        self.latches.get_mut(&id).cloned()
    }

    pub fn remove_latch(&mut self, id: u64) {
        self.latches.remove(&id);
    }

    pub fn get_latch_counter(&self, name: &str) -> Counter {
        self.latches.get_counter(name)
    }

    pub fn get_latch_counter_mut(&mut self, name: &str) -> Counter {
        self.latches.get_counter_mut(name)
    }

    pub fn get_latch_gauge(&self, name: &str) -> Gauge {
        self.latches.get_gauge(name)
    }

    pub fn get_latch_gauge_mut(&mut self, name: &str) -> Gauge {
        self.latches.get_gauge_mut(name)
    }

    pub fn get_latch_histogram(&self, name: &str) -> Histogram {
        self.latches.get_histogram(name)
    }
}


impl Scheduler {
    pub fn get_engine(&self) -> &Engine {
        &self.engine
    }

    pub fn get_engine_mut(&mut self) -> &mut Engine {
        &mut self.engine
    }


}


impl Scheduler {
    pub fn schedule_with_ctx_and_id_and_latch_and_latch(&mut self, cmd: Command, ctx: CommandContext, id: u64, latch: Latch, latch2: Latch) {
        /// The name of the metric.

        const METRIC_NAME: &str = "scheduler_too_busy";

        self.schedule.push(cmd);
        self.cmd_ctx.insert(id, ctx);
        self.latches.insert(latch);
        self.latches.insert(latch2);
    }
}


impl Scheduler {
    pub fn schedule_with_ctx_and_id_and_latch_and_latch_and_latch(&mut self, cmd: Command, ctx: CommandContext, id: u64, latch: Latch) {
        /// The name of the metric.

        const METRIC_NAME: &str = "scheduler_too_busy";

        self.schedule.push(cmd);
        self.cmd_ctx.insert(id, ctx);
        self.latches.insert(latch);
    }
}


impl Scheduler {
    pub fn schedule_with_ctx_and_id_and_latch_and_latch_and_latch_and_latch(&mut self) {
/// The name of the metric.
/// The name of the metric.

        const METRIC_NAME: &str = "scheduler_too_busy";

        let mut too_busy = false;
        if self.latches.len() > self.sched_too_busy_threshold {
            too_busy = true;
        }

        if too_busy {
            let mut counter = self.latches.get_counter(METRIC_NAME);
            counter.inc();
        }

        if too_busy {
            return;
        }

    }
}





pub fn get_scheduler() {
    let mut scheduler = Scheduler::new(Box::new(Engine::new()), config.get_sched_too_busy_threshold());
    scheduler.get_engine_mut();
    let mut cmd_ctx = scheduler.get_cmd_ctx_mut();
    scheduler.get_schedule_mut();
    scheduler.get_id_alloc_mut();
    scheduler.get_latches_mut();
    scheduler.get_sched_too_busy_threshold_mut();
    let mut worker_pool = scheduler.get_worker_pool_mut();

    worker_pool.execute(());
    let mut msg = Msg::default();
    loop {
        msg = recv.recv().unwrap();
        match msg.ty {
            MsgType::Schedule => {
                let cmd = msg.cmd.clone();
                let cid = msg.cid;
                let ctx = RunningCtx {
                    cmd,
                    ch: (),
                    cid,
                    snapshot: (),
                    latches: Latches {
                        pending: 0,
                        running: 0,
                        finished: 0,
                        failed: 0,
                        cancelled: 0,
                        timeout: 0,
                        panic: 0,
                        concurrency: 0
                    },
                    id_alloc: 0
                };
                cmd_ctx.insert(cid, ctx);
                let ctx = cmd_ctx.get(&cid).unwrap();
                let ctx = ctx.clone();
                worker_pool();
                worker_pool.execute(move || {
                    let res = cmd.execute(scheduler.get_engine_mut());
                    if res.is_ok() {
                        ctx.latches.set_result(res.unwrap());
                    } else {
                        ctx.latches.set_error(res.unwrap_err());
                    }
                });
            }
            MsgType::Cancel => {
                let cid = msg.cid;
                let ctx = cmd_ctx.get(&cid).unwrap();
                ctx.cancel();
            }
            MsgType::Stop => {
                break;
            }
        }
    }
}





pub fn get_scheduler_with_ctx_and_id_and_latch() {
    fn get_scheduler() {
        cmd_ctx.insert(cid, ctx);
        let ctx = cmd_ctx.get(&cid).unwrap();
        let ctx = ctx.clone();
        worker_pool();
        let x = worker_pool.execute(move || {
            let res = cmd.execute(scheduler.get_engine_mut());
            if res.is_ok() {
                ctx.latches.set_result(res.unwrap());
            } else {
                ctx.latches.set_error(res.unwrap_err());
            }
        });

            let res = cmd.execute(scheduler.get_engine_mut());
            if res.is_ok() {
                ctx.latches.set_result(res.unwrap());
            } else {
                ctx.latches.set_error(res.unwrap_err());
            }

        }
    }





pub fn start_ts_with_config () {

    let mut scheduler = Scheduler::new(Box::new(Engine::new()), config.get_sched_too_busy_threshold());
    scheduler.get_engine_mut();
    let mut cmd_ctx = scheduler.get_cmd_ctx_mut();
    scheduler.get_schedule_mut();
    scheduler.get_id_alloc_mut();
    scheduler.get_latches_mut();
    scheduler.get_sched_too_busy_threshold_mut();
    scheduler.get_worker_pool_mut();
    let cid = msg.cid;
    let ctx = cmd_ctx.get(&cid).unwrap();
    ctx.cancel();
    fn start_ts() {
        // let mut worker_pool = ThreadPool::new(num_cpus::get());
        // let mut schedule = Vec::new();
        // let mut latches = Latches::new();
        // let mut cmd_ctx = HashMap::new();
        Box::new(Engine::new());
    }
}

    pub fn start_ts_with_config_and_recv() {
        loop {
            let msg = schedule.recv().unwrap();
            match msg.ty {
                MsgType::Schedule => {
                    msg.cmd.clone();
                    let cid = msg.cid;
                    cmd_ctx.insert(cid, ctx);
                    let ctx = cmd_ctx.get(&cid).unwrap();
                    let ctx = ctx.clone();
                    worker_pool.execute(move || {
                        ctx.run();
                    });
                }
                MsgType::Cancel => {
                    let cid = msg.cid;
                    let ctx = cmd_ctx.get(&cid).unwrap();
                    ctx.cancel();
                }
                MsgType::Stop => {
                    break;
                }
            }
        }
    }

    impl RunningCtx {
        pub fn get_sched_too_busy_threshold(&self) -> usize {
            self.sched_too_busy_threshold_mut
        }

        pub fn get_sched_too_busy_threshold_mut(&mut self) -> &mut usize {
            &mut self.sched_too_busy_threshold_mut
        }
    }

    impl Scheduler {
        pub fn get_engine(&self) -> &Box<Engine> {
            &self.engine
        }

        pub fn get_engine_mut(&mut self) -> &mut Box<Engine> {
            &mut self.engine
        }

        pub fn get_schedule(&self) -> &Vec<Msg> {
            &self.schedule
        }

        pub fn get_schedule_mut(&mut self) -> &mut SendCh<Msg> {
            &mut self.schedule
        }

        pub fn get_latches(&self) -> &crate::config::Latches {
            &self.latches
        }

        pub fn get_latches_mut(&mut self) -> &mut crate::config::Latches {
            &mut self.latches
        }

        pub fn get_cmd_ctx(&self) -> &HashMap<u64, RunningCtx> {
            &self.cmd_ctx
        }

        pub fn get_cmd_ctx_mut(&mut self) -> &mut HashMap<u64, RunningCtx> {
            &mut self.cmd_ctx
        }

        pub fn get_id_alloc(&self) -> u64 {
            self.id_alloc_mut
        }
    }

        pub fn new(engine: Box<Engine>, config: &Config) -> Scheduler {
            Scheduler {
                id_alloc: 0,
                ch: (),
                cmd: (),
                snapshot: (),
                latches: Latches::new(),
                sched_too_busy_threshold: config.get_sched_too_busy_threshold(),

                worker_pool: (|| {
                    let mut worker_pool = ThreadPool::new(num_cpus::get());
                    worker_pool
                })(),
                cid: 0
            }
        }


    impl RunningCtx {
        pub fn new(cmd: storage::Command, cid: u64, engine: Box<Engine>, latches: Latches) -> Self {
            RunningCtx {
                cmd,
                ch: (),
                cid,
                latches,
                id_alloc: 0,
                snapshot: ()
            }
        }

        pub fn get_cmd(&self) -> &storage::Command {
            &self.cmd
        }
    }

    impl CommandContext {
        pub fn new(cmd: storage::Command, cid: u64, engine: Box<Engine>, latches: Latches) -> Self {
            CommandContext {
                cmd,
                cid,
                engine,
                latches,
                scheduler: Scheduler::new(),
                snapshot: Snapshot::new(),
                id_alloc: 0,

            }
        }
    }

    impl CommandContext {
        pub fn get_scheduler(&self) -> &Scheduler {
            &self.scheduler_too_busy_threshold_mut
        }
        pub fn get_scheduler_mut(&mut self) -> &mut Scheduler {
            &mut self.scheduler_too_busy_threshold_mut
        }
        pub fn get_snapshot(&self) -> &Snapshot {
            match &self.snapshot {
                Snapshot::Snapshot(snapshot) => snapshot,
                Snapshot::NoSnapshot => panic!("snapshot is not set"),
            }
        }

        pub fn get_snapshot_mut(&mut self) -> &mut Snapshot {
            match &mut self.snapshot {
                Snapshot::Snapshot(snapshot) => snapshot,
                Snapshot::NoSnapshot => panic!("snapshot is not set"),
            }
        }
        pub fn get_running_ctx(&self) -> &RunningCtx {
            &self.running_ctx
        }
        pub fn get_running_ctx_mut(&mut self) -> &mut RunningCtx {
            &mut self.running_ctx
        }
        pub fn get_cid(&self) -> u64 {
            self.cid
        }
        pub fn get_cmd(&self) -> &storage::Command {
            &self.cmd
        }
    }




#[derive(Debug)]
pub enum RunningState {
    Pending,
    Running,
    Finished,
    Failed,
    Cancelled,
    Timeout,
}



impl Scheduler {
    pub fn new() -> Scheduler {
        Scheduler {
            latches: HashMap::new(),
            id_alloc: 0,
            ch: (),
            cmd: (),
            snapshot: (),
            cid: 0,
            sched_too_busy_threshold: 0,
            worker_pool: WorkerPool::new(),

        }
    }
}

pub fn run(engine: Engine, scheduler: Scheduler) -> Scheduler {
        let mut scheduler = scheduler;
        let mut engine = engine;
        let mut scheduler = Scheduler {
            latches: HashMap::new(),
            id_alloc: 0,
            ch: (),
            cmd: (),
            snapshot: (),
            cid: 0,
            sched_too_busy_threshold: 0,
            worker_pool: WorkerPool::new(),
        };
        let mut engine = engine;
        let mut scheduler = scheduler;
    let mut cmd_ctx = HashMap::new();
        let mut id_alloc = 0;
        let mut latches = Latches::new();
        let mut sched_too_busy_threshold = scheduler.sched_too_busy_threshold;
        let mut worker_pool = scheduler.worker_pool.clone();
    scheduler.engine.clone();

        loop {

            let msg = recv.recv().unwrap();
            match msg {
                Msg::Command(cmd) => {
                    let cid = cmd.cid;
                    let ctx = cmd_ctx.entry(cid).or_insert_with(|| {
                        let ctx = RunningCtx::new(cid, &mut latches, &mut sched_too_busy_threshold);
                        id_alloc += 1;
                        ctx
                    });
                    ctx.push_command(cmd);
                }
                Msg::CommandFinished(cid) => {
                    let ctx = cmd_ctx.remove(&cid).unwrap();
                    ctx.finished();
                }
                Msg::CommandFailed(cid) => {
                    let ctx = cmd_ctx.remove(&cid).unwrap();
                    ctx.failed();
                }
                Msg::CommandCancelled(cid) => {
                    let ctx = cmd_ctx.remove(&cid).unwrap();
                    ctx.cancelled();
                }
                Msg::CommandTimeout(cid) => {
                    let ctx = cmd_ctx.remove(&cid).unwrap();
                    ctx.timeout();
                }
                Msg::CommandPanic(cid, panic_msg) => {
                    let ctx = cmd_ctx.remove(&cid).unwrap();
                    ctx.panic(panic_msg);
                }
                Msg::CommandTooBusy => {
                    sched_too_busy_threshold += 1;
                }
                Msg::Stop => {
                    break;
                }
            }
        }

        scheduler_too_busy_threshold += 1;


        scheduler_too_busy_threshold_mut = sched_too_busy_threshold;
        worker_pool_mut = worker_pool;
        cmd_ctx_mut = cmd_ctx;
        id_alloc_mut = id_alloc;
        latches_mut = latches;
        engine_mut = engine;
        scheduler_mut = scheduler;
        recv_mut = recv;
        return scheduler;
}


pub fn run_scheduler(engine: Engine, scheduler: Scheduler) -> Scheduler {
    let mut scheduler = scheduler;
    let mut engine = engine;
    let mut scheduler = Scheduler {
        latches: HashMap::new(),
        id_alloc: 0,
        ch: (),
        cmd: (),
        snapshot: (),
        cid: 0,
        sched_too_busy_threshold: 0,
        worker_pool: WorkerPool::new(),
    };
    let mut engine = engine;
    let mut scheduler = scheduler;
    let mut cmd_ctx = HashMap::new();
    let mut id_alloc = 0;

    let mut sched_too_busy_threshold = scheduler.sched_too_busy_threshold;
    let mut worker_pool = scheduler.worker_pool.clone();
    scheduler.engine.clone();

    loop {
        impl RunningCtx {
            fn new(cid: u64, cmd: Command, ch: SendCh<Msg>, snapshot: Box<Snapshot>) -> RunningCtx {
                RunningCtx {
                    cid,
                    cmd: Some(cmd),
                    ch,
                    snapshot: Some(snapshot),

                    latches: Latches {
                        pending: 0,
                        running: 0,
                        finished: 0,
                        failed: 0,
                        cancelled: 0,
                        timeout: 0,
                        panic: 0,
                        concurrency: 0
                    },
                    id_alloc: 0
                }
            }
        }
        let mut cmd_ctx = cmd_ctx;
        let mut id_alloc = id_alloc;
        let mut sched_too_busy_threshold = sched_too_busy_threshold;
        let mut worker_pool = worker_pool;
        let mut engine = engine;
        let mut scheduler = scheduler;
        let mut recv = recv;
        let mut latches = Latches::new();
        let mut scheduler = scheduler;
        let mut engine = engine;
        let mut recv = recv;
        let mut cmd_ctx = cmd_ctx;


        let msg = recv.recv().unwrap();
match msg {
            Msg::Command(cmd) => {
                let cid = cmd.cid;
                let ctx = cmd_ctx.entry(cid).or_insert_with(|| {
                    let ctx = RunningCtx::new(cid, cmd, ch, snapshot);
                    id_alloc += 1;
                    ctx
                });
                ctx.push_command(cmd);
            }
            Msg::CommandFinished(cid) => {
                let ctx = cmd_ctx.remove(&cid).unwrap();
                ctx.finished();
            }
            Msg::CommandFailed(cid) => {
                let ctx = cmd_ctx.remove(&cid).unwrap();
                ctx.failed();
            }
            Msg::CommandCancelled(cid) => {
                let ctx = cmd_ctx.remove(&cid).unwrap();
                ctx.cancelled();
            }
            Msg::CommandTimeout(cid) => {
                let ctx = cmd_ctx.remove(&cid).unwrap();
                ctx.timeout();
            }
            Msg::CommandPanic(cid, panic_msg) => {
                let ctx = cmd_ctx.remove(&cid).unwrap();
                ctx.panic(panic_msg);
            }
            Msg::CommandTooBusy => {
                sched_too_busy_threshold += 1;
            }
            Msg::Stop => {
                break;
            }
        }

        scheduler_too_busy_threshold += 1;

        scheduler_too_busy_threshold_mut = sched_too_busy_threshold;

        worker_pool_mut = worker_pool;
        cmd_ctx_mut = cmd_ctx;
        id_alloc_mut = id_alloc;
        latches_mut = latches;
        engine_mut = engine;
        scheduler_mut = scheduler;
        recv_mut = recv;

        return scheduler;

    }
}




pub fn run_scheduler_with_engine(engine: Engine, scheduler: Scheduler) -> Scheduler {
    let mut scheduler = scheduler;
    let mut engine = engine;
    let mut scheduler = Scheduler {
        latches: HashMap::new(),
        id_alloc: 0,
        ch: (),
        cmd: (),
        snapshot: (),
        cid: 0,
        sched_too_busy_threshold: 0,
        worker_pool: WorkerPool::new(),
    };
    let mut engine = engine;
    let mut scheduler = scheduler;
    let mut cmd_ctx = HashMap::new();
    let mut id_alloc = 0;

    let mut sched_too_busy_threshold = scheduler.sched_too_busy_threshold;
    let mut worker_pool = scheduler.worker_pool.clone();
    scheduler.engine.clone();

    loop {
        impl RunningCtx {
            fn new(cid: u64, cmd: Command, ch: SendCh<Msg>, snapshot: Box<Snapshot>) -> RunningCtx {
                RunningCtx {
                    cid,
                    cmd: Some(cmd),
                    ch,
                    snapshot: Some(snapshot),

                    latches: Latches {
                        pending: 0,
                        running: 0,
                        finished: 0,
                        failed: 0,
                        cancelled: 0,
                        timeout: 0,
                        panic: 0,
                        concurrency: 0
                    },
                    id_alloc: 0
                }
            }
        }
        let mut cmd_ctx = cmd_ctx;
        let mut id_alloc = id_alloc;
        let mut sched_too_busy_threshold = sched_too_busy_threshold;
        let mut worker_pool = worker_pool;
        let mut engine = engine;
        let mut scheduler = scheduler;
        let mut recv = recv;
        let mut latches = Latches::new();
        let mut scheduler = scheduler;
        let mut engine = engine;
        let mut recv = recv;
        let mut cmd_ctx = cmd_ctx;

        let msg = recv.recv().unwrap();

        match msg {
            Msg::Command(cmd) => {
                let cid = cmd.cid;
                let ctx = cmd_ctx.entry(cid).or_insert_with(|| {
                    let ctx = RunningCtx::new(cid, cmd, ch, snapshot);
                    id_alloc += 1;
                    ctx
                });
                ctx.push_command(cmd);
            }
            Msg::CommandFinished(cid) => {
                let ctx = cmd_ctx.remove(&cid).unwrap();
                ctx.finished();
            }
            Msg::CommandFailed(cid) => {
                let ctx = cmd_ctx.remove(&cid).unwrap();
                ctx.failed();
            }
            Msg::CommandCancelled(cid) => {
                let ctx = cmd_ctx.remove(&cid).unwrap();
                ctx.cancelled();
            }
            Msg::CommandTimeout(cid) => {
                let ctx = cmd_ctx.remove(&cid).unwrap();
                ctx.timeout();
            }
            Msg::CommandPanic(cid, panic_msg) => {
                let ctx = cmd_ctx.remove(&cid).unwrap();
                ctx.panic(panic_msg);
            }
            Msg::CommandTooBusy => {
                sched_too_busy_threshold += 1;
            }
            Msg::Stop => {
                break;
            }
        }

        scheduler_too_busy_threshold += 1;

        scheduler_too_busy_threshold_mut = sched_too_busy_threshold ;

        worker_pool_mut = worker_pool;
        cmd_ctx_mut = cmd_ctx;
        id_alloc_mut = id_alloc;

        latches_mut = latches;
        engine_mut = engine;
        scheduler_mut = scheduler;
        recv_mut = recv;

        return scheduler;

    }
}