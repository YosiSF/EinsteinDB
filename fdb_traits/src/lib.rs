mod fdb_traits;
pub mod errors;

//users
use crate::*;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::fmt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;



//fdb_opts
//             compression_strategy: String::from("default"),
//             compression_dict: Vec::new(),
//             enable_statistics: false,
//             statistics_interval: 0,
//             statistics_block_size: 0,
//             statistics_block_cache_size: 0,
//             statistics_block_cache_shard_bits: 0,


// Language: rust
// Compare this snippet from einstein_db_alexandrov_processing/file_system.rs:
//     pub fn create_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
//         fs::create_dir_all(path).with_context(|| format!("failed to create directory: {}", path.as_ref().display()))
//     }
//     pub fn create_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
//         fs::create_dir(path).with_context(|| format!("failed to create directory: {}", path.as_ref().display()))
//     }
//     pub fn create_file<P: AsRef<Path>>(path: P) -> io::Result<File> {
//         File::create(path).with_context(|| format!("failed to create file: {}", path.as_ref().display()))
//     }
//     pub fn open_file<P: AsRef<Path>>(path: P) -> io::Result<File> {
//         File::open(path).with_context(|| format!("failed to open file: {}", path.as_ref().display()))
//     }
//     pub fn open_file_with_options<P: AsRef<Path>>(path: P, options: &OpenOptions) -> io::Result<File> {
//         File::open(path).with_context(|| format!("failed to open file: {}", path.as_ref().display()))





extern crate fdb_sys;
extern crate fdb_traits;
extern crate fdb_file_system;
extern crate fdb_options;
//itertools
extern crate itertools;
extern crate petgraph;
extern crate petgraph_dot;
extern crate petgraph_vis;
//foundationdb
extern crate foundationdb;
extern crate foundationdb_sys;
//fdb_traits
extern crate fdb_traits;

#[macro_use]
extern crate log;
extern crate simple_logger;
extern crate serde;
extern crate serde_json;

mod fdb_opts;
pub mod errors;
pub mod fdb_traits;
pub mod fdb_file_system;
pub mod fdb_options;
 
mod fdb_file_system_impl;
mod fdb_options_impl;
mod fdb_traits_impl;

//spacetime schema
mod spacetime_schema;
mod spacetime_schema_impl;







// Provide a default implementation of `Display` for `T`
// if you want to use `println!` instead of `print!`
// impl<T: fmt::Display> fmt::Display for T {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.to_string())
//     }
// }
// impl<T: fmt::Display> fmt::Display for &T {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.to_string())
//     }


struct FileSystem {
    root_path: PathBuf,
    file_path: PathBuf,
    file_name: String,
    file_extension: String,
}




impl FileSystem {
    pub fn new(root_path: PathBuf, file_path: PathBuf, file_name: String, file_extension: String) -> FileSystem {
        FileSystem {
            root_path,
            file_path,
            file_name,
            file_extension,
        }
    }
}


impl FileSystem {
    pub fn get_root_path(&self) -> PathBuf {
        self.root_path.clone()
    }
    pub fn get_file_path(&self) -> PathBuf {
        self.file_path.clone()
    }
    pub fn get_file_name(&self) -> String {
        self.file_name.clone()
    }
    pub fn get_file_extension(&self) -> String {
        self.file_extension.clone()
    }
}


impl FileSystem {
    pub fn get_file_path_as_string(&self) -> String {
        self.file_path.to_str().unwrap().to_string()
    }
    pub fn get_file_name_as_string(&self) -> String {
        self.file_name.clone()
    }
    pub fn get_file_extension_as_string(&self) -> String {
        self.file_extension.clone()
    }
}


impl FileSystem {
    pub fn get_file_path_as_path(&self) -> PathBuf {
        self.file_path.clone()
    }
    pub fn get_file_name_as_path(&self) -> PathBuf {
        self.file_path.clone()
    }
    pub fn get_file_extension_as_path(&self) -> PathBuf {
        self.file_path.clone()
    }
}   //impl FileSystem




