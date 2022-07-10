use crate::causets::causet_partitioner::CausetPartitioner;
use crate::causets::causet_partitioner::CausetPartitionerError;
use crate::causets::causet_partitioner::CausetPartitionerErrorKind;

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

use std::sync::mpsc::{RecvError, RecvTimeoutError};
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::TrySendError;
use std::sync::mpsc::SendError;



use crate::einsteindb::einstein_db::*;
use crate::einsteindb::einstein_db::{EinsteinDB};
use crate::einsteindb::einstein_db::{EinsteinDBError};
use crate::einsteindb::einstein_db::{EinsteinDBErrorKind};


use crate::einsteindb::einstein_ml::*;
use crate::einsteindb::einstein_ml::{EinsteinML};
use crate::einsteindb::einstein_ml::{EinsteinMLError};
use crate::einsteindb::einstein_ml::{EinsteinMLErrorKind};
use crate::einsteindb::einstein_ml::{EinsteinMLNode};


use crate::einsteindb::einstein_ml::{EinsteinMLNode};
use crate::einsteindb::einstein_ml::{EinsteinMLNodeId};
use crate::einsteindb::einstein_ml::{EinsteinMLNodeData};




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
extern crate serde_yaml;
extern crate serde_derive;
extern crate serde_cbor;
extern crate serde_json_derive;
extern crate serde_yaml_derive;


#[macro_use]
extern crate serde_derive;


#[macro_use]
extern crate lazy_static;


#[macro_use]
extern crate failure;


#[macro_use]
extern crate failure_derive;





///!





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




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EinsteinDBPartitionerError {
    pub kind: EinsteinDBPartitionerErrorKind,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EinsteinDBPartitionerErrorKind {
    EinsteinDBError(EinsteinDBError),
    EinsteinMLError(EinsteinMLError),
    CausetPartitionerError(CausetPartitionerError),
    EinsteinDBPartitionerErrorKind(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind2(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind3(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind4(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind5(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind6(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind7(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind8(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind9(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind10(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind11(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind12(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind13(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind14(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind15(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind16(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind17(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind18(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind19(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind20(EinsteinDBPartitionerErrorKind),
    EinsteinDBPartitionerErrorKind21(EinsteinDBPartitionerErrorKind),
}




impl fmt::Display for EinsteinDBPartitionerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EinsteinDBPartitionerError")
    }
}


impl fmt::Display for EinsteinDBPartitionerErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "EinsteinDBPartitionerErrorKind")
    }
}


impl EinsteinDBPartitionerError {
    pub fn new(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerError {
        EinsteinDBPartitionerError {
            kind,
        }
    }
}


impl EinsteinDBPartitionerErrorKind {
    pub fn new(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerErrorKind {
        EinsteinDBPartitionerErrorKind::EinsteinDBPartitionerErrorKind(kind)
    }
}


impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerError {
    fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerError {
        EinsteinDBPartitionerError::new(kind)
    }
}


impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerErrorKind {
    fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerErrorKind {
        EinsteinDBPartitionerErrorKind::EinsteinDBPartitionerErrorKind(kind)
    }
}


impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerError {
    fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerError {
        EinsteinDBPartitionerError::new(kind)
    }
}


impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerErrorKind {
    fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerErrorKind {
        EinsteinDBPartitionerErrorKind::EinsteinDBPartitionerErrorKind(kind)
    }
}


impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerError {
    fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerError {
        EinsteinDBPartitionerError::new(kind)
    }
}


impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerErrorKind {
    fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerErrorKind {
        EinsteinDBPartitionerErrorKind::EinsteinDBPartitionerErrorKind(kind)
    }
}


impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerError {
    fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerError {
        EinsteinDBPartitionerError::new(kind)
    }
}


impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerErrorKind {
    fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerErrorKind {
        EinsteinDBPartitionerErrorKind::EinsteinDBPartitionerErrorKind(kind)
    }
}



//// impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerError {
////     fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerError {
////         EinsteinDBPartitionerError::new(kind)
////     }
//// }