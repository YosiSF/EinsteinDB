mod einsteindb_options;
mod encryption;
mod fdb_traits;
mod util;
mod peekable;
mod options;
mod errors;
mod violetabft_engine;
mod schema;
mod vocabulary;

/// Copyright 2020-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
/// AUTHORS: WHITFORD LEDER
/// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
/// this file File except in compliance with the License. You may obtain a copy of the
/// License at http://www.apache.org/licenses/LICENSE-2.0
/// Unless required by applicable law or agreed to in writing, software distributed
/// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
/// CONDITIONS OF ANY KIND, either express or implied. See the License for the
/// specific language governing permissions and limitations under the License.
/// =================================================================
///


/*
///! # EinsteinDB
///
/// Description: EinsteinDB is a distributed key-value store for Rust with semantic knowledge and fast performance.
///
/// ## Features
/// ```
/// - Fast and efficient
/// - High performance
/// - High availability
/// - Scalable
///
/// - High-level API
/// - High-level API for high-concurrency workloads

use crate::causets::causet_partitioner::CausetPartitioner;
use crate::causets::causet_partitioner::CausetPartitionerError;
use crate::causets::causet_partitioner::CausetPartitionerErrorKind;
*/

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



//Changelog: The following is the changelog of this project.
//
// Version 0.1.0:
// - Initial version.
// - Initial version of EinsteinDB.
// - Initial version of EinsteinML.
// - Initial version of EinsteinMLNode.



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




///! SUSE file system with SmartNIC virtualized IPAM
/// This is a SUSE file system with SmartNIC virtualized IPAM.
/// It is a file system that is designed to be used with SmartNIC.



#[derive(Debug)]
pub struct SUSEFileSystem {
    /// The path of the file system.
    path: PathBuf,
    /// The path of the file system.
    path_str: String,
    /// The path of the file system.
    path_str_ref: &'static str,
    /// The path of the file system.
    path_str_ref_mut: &'static mut str,
    /// The path of the file system.
}


///We will implement a jail system which will issue out a cron job to clean up the file system.
/// The jail system will be able to clean up the file system.
/// The interlock will deal mostly with lightlike nodes.


const SUSE_FILE_SYSTEM_PATH: &'static str = "/tmp/suse_file_system";
const SUSE_FILE_SYSTEM_PATH_STR: &'static str = "/tmp/suse_file_system";
const SUSE_FILE_SYSTEM_PATH_STR_REF: &'static str = "/tmp/suse_file_system";

#[macro_export(local_inner_macros)]
macro_rules! local_inner_macros {
    ($($tokens:tt)*) => {
        $($tokens)*
    };
}






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
    pub fn get_file_path_str(&self) -> String {
        if self.file_path.is_relative() {
            self.file_path.to_str().unwrap().to_string()
        } else {
            self.file_path.to_string_lossy().to_string()
        }

        for k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"] {
            if self.file_path.to_str().unwrap().starts_with(k8s_path) {
                return self.file_path.to_str().unwrap().replace(k8s_path, "").to_string();
            }
        }

        if self.file_path.to_str().unwrap().starts_with("/var/lib/kubelet/pods/") {
            return self.file_path.to_str().unwrap().replace("/var/lib/kubelet/pods/", "").to_string();
        }

        match self.file_path.to_str() {
            Some(s) => s.to_string(),
            None => "".to_string(),
        }
    }
}


impl FileSystem {
    pub fn get_file_path_str_ref(&self) -> &'static str {
        SUSE_FILE_SYSTEM_PATH_STR_REF
    }
}


impl FileSystem {
    pub fn get_file_path_str_ref_mut_ref(&self) -> &'static mut &'static str {
        SUSE_FILE_SYSTEM_PATH_STR_REF_MUT_REF
    }


    pub fn get_file_path_str_ref_mut_ref_mut(&self) -> &'static mut &'static mut &'static str {
        SUSE_FILE_SYSTEM_PATH_STR_REF_MUT_REF_MUT
    }


    pub fn get_file_path_str_ref_mut_ref_mut_mut(&self) -> &'static mut &'static mut &'static mut &'static str {
        SUSE_FILE_SYSTEM_PATH_STR_REF_MUT_REF_MUT_MUT
    }
}
// }
//
// #[derive(Debug)]
// #[derive(Clone)]
// #[derive(Copy)]
// #[derive(PartialEq)]
// async fn get_file_system_path() {
//
//     let mut path = PathBuf::new();
//     path.push(SUSE_FILE_SYSTEM_PATH){
//         path
//     };
//     path.push("/");
//     path.push(SUSE_FILE_SYSTEM_PATH){
//         while(if path.exists() {
//             path.is_dir()
//         } else {
//             false
//         }) {
//             path.pop();
//         }
//     };
//     path.push(SUSE_FILE_SYSTEM_PATH){
//         if (k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"]) {
//             path.push(k8s_path);
//         } else {
//             path.push("/var/lib/kubelet/pods/");
//         }
//     };
//
//     // println!("{:?}", path);
//     // println!("{:?}", path.to_str());
//     // println!("{:?}", path.to_str().unwrap());
//     #[derive(Debug)]
//     #[derive(Clone)]
//
//     #[derive(Copy)]
//
//     #[derive(PartialEq)]
//
//     async fn get_file_system_path() {
//
//         let mut path = PathBuf::new();
//         path.push(SUSE_FILE_SYSTEM_PATH){
//             path
//         };
//
//         loop{
//             path.push("/");
//             path.push(SUSE_FILE_SYSTEM_PATH){
//                 while(if path.exists() {
//                     path.is_dir()
//                 } else {
//                     false
//                 }) {
//                     path.pop();
//                 }
//             };
//             path.push(SUSE_FILE_SYSTEM_PATH){
//                 if (k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"]) {
//                     path.push(k8s_path);
//                 } else {
//                     path.push("/var/lib/kubelet/pods/");
//                 }
//             };
//             break;
//         }
//
//         // println!("{:?}", path);
//         // println!("{:?}", path.to_str());
//
//
//         #[derive(Debug)]
//         #[derive(Clone)]
//
//
//
//         #[derive(Copy)]
//
//
//         match path.to_str() {
//             for k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"] {
//                 if path.to_str().unwrap().starts_with(k8s_path) {
//                     return path.to_str().unwrap().replace(k8s_path, "").to_string();
//                 }
//             }
//
//             let mut path = PathBuf::new(){
//                 if (k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"]) {
//                     for k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"] {
//                         if path.to_str().unwrap().starts_with(k8s_path) {
//                             return path.to_str().unwrap().replace(k8s_path, "").to_string();
//                         }
//                     }
//                 }
//             };
//             path.push(SUSE_FILE_SYSTEM_PATH){
//                 path.push(k8s_path);
//                 //lock the path
//                 path.push("/var/lib/kubelet/pods/");
//                 path.push(k8s_path);
//             } else {
//                 path.push("/var/lib/kubelet/pods/");
//             }
//
//         loop {
//             if self.file_path.to_str().unwrap().starts_with(k8s_path) {
//                match self.file_path.to_str() {
//                    let mut s = self.file_path.to_str().unwrap().replace(k8s_path, "");
//                    let mut s = s.replace("/var/lib/kubelet/pods/", "");
//                    while s.starts_with("/") {
//                        s = &s[1..];
//
//                        if s.starts_with("/") {
//                            s = &s[1..];
//                        }
//
//                        match s.find("/") {
//                            Some(i) => {
//                                s = &s[i..];
//                            }
//                            None => {
//                                s = "";
//                            }
//                        }
//                     None => return "".as_str(),
//                 }
//
//                    for k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"] {
//                        if self.file_path.to_str().unwrap().starts_with(k8s_path) {
//                            return self.file_path.to_str().unwrap().replace(k8s_path, "").to_string();
//                        }
//                    }
//                 }
//             }
//         }
//     }
//
//         }
//     }
//
//     for k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"] {
//         if self.file_path.to_str().unwrap().starts_with(k8s_path) {
//             return self.file_path.to_str().unwrap().replace(k8s_path, "").to_string();
//         }
//     }
//
//     if self.file_path.to_str().unwrap().starts_with("/var/lib/kubelet/pods/") {
//         return self.file_path.to_str().unwrap().replace("/var/lib/kubelet/pods/", "").to_string();
//     }
//
// }
//
//     ///lock the thread that is calling this function
//     /// to prevent the thread from being interrupted
//
//     match self.file_path.to_str() {
//         Some(s) => s.to_string(),
//         None => "".to_string(),
//
//         }
//
//     for k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"] {
//         if self.file_path.to_str().unwrap().starts_with(k8s_path) {
//             return self.file_path.to_str().unwrap().replace(k8s_path, "").to_string();
//         }
//     } while (k8s_path) {
//         match (k8s_path) {
//             Some(s) => s.to_string(),
//             None => "".to_string(),
//         }
//         for k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"] {
//           \
//                 if self.file_path.to_str().unwrap().starts_with(k8s_path) {
//                     return self.file_path.to_str().unwrap().replace(k8s_path, "").to_string();
//                 } loop{
//                     if self.file_path.to_str().unwrap().starts_with(k8s_path) {
//                         return self.file_path.to_str().unwrap().replace(k8s_path, "").to_string();
//                     }
//                 }
//             }
//         }
//
//         if self.file_path.to_str().unwrap().starts_with("/var/lib/kubelet/pods/") {
//             return self.file_path.to_str().unwrap().replace("/var/lib/kubelet/pods/", "").to_string();
//         }
//     }
//     #[cfg(feature = "k8s")]
//         for k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"] {
//             if self.file_path.to_str().unwrap().starts_with(k8s_path) {
//                 return self.file_path.to_str().unwrap().replace(k8s_path, "").to_string();
//             }
//         }
// for k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"] {
//     if self.file_path.to_str().unwrap().starts_with(k8s_path) {
//
//         return self.file_path.to_str().unwrap().replace(k8s_path, "").to_string();
//     }
// }
//     if self.file_path.to_str().unwrap().starts_with("/var/lib/kubelet/pods/") {
//         return self.file_path.to_str().unwrap().replace("/var/lib/kubelet/pods/", "").to_string();
//     }
//     for k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"] {
//         while (ifulltext)
//         {
//             if self.file_path.to_str().unwrap().starts_with("/var/lib/kubelet/pods/") {
//                 return self.file_path.to_str().unwrap().replace("/var/lib/kubelet/pods/", "").to_string();
//             }
//         }
//     }
// }
// for k8s_path in &["/var/lib/kubelet/pods/", "/var/lib/kubelet/pods/"] {
//             if self.file_path.to_str().unwrap().starts_with(k8s_path) {
//
//
//         if causetq_index_fulltext_view_path.to_str().unwrap().starts_with("/var/lib/kubelet/pods/") {
//
//             return self.file_path.to_str().unwrap().replace("/var/lib/kubelet/pods/", "").to_string();
//         }
//
//
//         self.file_path.to_str().unwrap().to_string()
//     }
//
//     pub fn get_file_name_str(&self) -> String {
//         self.file_name.clone()
//     }
//
//
//     pub fn get_file_path_as_string(&self) -> String {
//         self.file_path.to_str().unwrap().to_string()
//     }
//     pub fn get_file_name_as_string(&self) -> String {
//         self.file_name.clone()
//     }
//     pub fn get_file_extension_as_string(&self) -> String {
//         self.file_extension.clone()
//     }
// }
//
//
// impl FileSystem {
//     pub fn get_file_path_as_path(&self) -> PathBuf {
//         self.file_path.clone()
//     }
//     pub fn get_file_name_as_path(&self) -> PathBuf {
//         self.file_path.clone()
//     }
//     pub fn get_file_extension_as_path(&self) -> PathBuf {
//         self.file_path.clone()
//     }
// }   //impl FileSystem
//
//
//
//
// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
// pub struct EinsteinDBPartitionerError {
//     pub kind: EinsteinDBPartitionerErrorKind,
// }
//
//
// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
// pub enum EinsteinDBPartitionerErrorKind {
//     EinsteinDBError(EinsteinDBError),
//     EinsteinMLError(EinsteinMLError),
//     CausetPartitionerError(CausetPartitionerError),
//     EinsteinDBPartitionerErrorKind(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind2(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind3(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind4(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind5(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind6(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind7(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind8(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind9(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind10(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind11(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind12(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind13(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind14(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind15(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind16(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind17(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind18(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind19(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind20(EinsteinDBPartitionerErrorKind),
//     EinsteinDBPartitionerErrorKind21(EinsteinDBPartitionerErrorKind),
// }
//
//
//
//
// impl fmt::Display for EinsteinDBPartitionerError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "EinsteinDBPartitionerError")
//     }
// }
//
//
// impl fmt::Display for EinsteinDBPartitionerErrorKind {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "EinsteinDBPartitionerErrorKind")
//     }
// }
//
//
// impl EinsteinDBPartitionerError {
//     pub fn new(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerError {
//         EinsteinDBPartitionerError {
//             kind,
//         }
//     }
// }
//
//
// impl EinsteinDBPartitionerErrorKind {
//     pub fn new(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerErrorKind {
//         EinsteinDBPartitionerErrorKind::EinsteinDBPartitionerErrorKind(kind)
//     }
// }
//
//
// impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerError {
//     fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerError {
//         EinsteinDBPartitionerError::new(kind)
//     }
// }
//
//
// impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerErrorKind {
//     fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerErrorKind {
//         EinsteinDBPartitionerErrorKind::EinsteinDBPartitionerErrorKind(kind)
//     }
// }
//
//
// impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerError {
//     fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerError {
//         EinsteinDBPartitionerError::new(kind)
//     }
// }
//
//
// impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerErrorKind {
//     fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerErrorKind {
//         EinsteinDBPartitionerErrorKind::EinsteinDBPartitionerErrorKind(kind)
//     }
// }
//
//
// impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerError {
//     fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerError {
//         EinsteinDBPartitionerError::new(kind)
//     }
// }
//
//
// impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerErrorKind {
//     fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerErrorKind {
//         EinsteinDBPartitionerErrorKind::EinsteinDBPartitionerErrorKind(kind)
//     }
// }
//
//
// impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerError {
//     fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerError {
//         EinsteinDBPartitionerError::new(kind)
//     }
// }
//
//
// impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerErrorKind {
//     fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerErrorKind {
//         EinsteinDBPartitionerErrorKind::EinsteinDBPartitionerErrorKind(kind)
//     }
// }
//
//
//
// //// impl From<EinsteinDBPartitionerErrorKind> for EinsteinDBPartitionerError {
// ////     fn from(kind: EinsteinDBPartitionerErrorKind) -> EinsteinDBPartitionerError {
// ////         EinsteinDBPartitionerError::new(kind)
// ////     }
// //// }



//CHANGELOG:  Added the following functions:
// 1. einstein_db_partitioner_error_kind_to_string()
// 2. einstein_db_partitioner_error_to_string()
//

