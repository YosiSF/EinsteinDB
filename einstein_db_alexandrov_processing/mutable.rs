/// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
/// @author   jmzhao, Slushie, CavHack
/// @license  Apache-2.0
/// @email
/// @date     2019/08/05
/// @file     file_system.rs

use crate::*;
use std::fs::{self, File};
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use std::path::Path;
use std::fs::{OpenOptions, File};
use std::io::{Read, Write};
use std::io::SeekFrom;
use std::fs::{remove_file, rename};
use std::fs::{create_dir_all, remove_dir_all};
use std::fs::{metadata, read_dir};


///! This module contains the file system interface for the EinsteinDB database.
/// It is a thin wrapper around the underlying database.
/// The interface is designed to be similar to the standard library's `std::fs` module.
/// The interface is implemented using the `einstein_db` crate's `einstein_db_alexandrov_processing` module.
///


/// A file system interface for the EinsteinDB database.
#[derive(Clone, Debug)]
pub struct FileSystem {
    /// The path to the database.
    path: PathBuf,
}


#[derive(Clone, Debug)]
pub struct FileSystemOptions {
    /// The path to the database.
    path: PathBuf,
}


impl FileSystemOptions {
    /// Creates a new `FileSystemOptions` instance.
    pub fn new(path: PathBuf) -> Self {
        FileSystemOptions { path }
    }
}


impl FileSystem {
    /// Creates a new `FileSystem` instance.
    pub fn new(options: FileSystemOptions) -> Self {
        FileSystem { path: options.path }
    }
}


impl FileSystem {
    /// Creates a new `FileSystem` instance.
    pub fn new_with_path(path: PathBuf) -> Self {
        FileSystem { path }
    }
}


impl FileSystem {
    /// Creates a new `FileSystem` instance.
    pub fn new_with_path_str(path: &str) -> Self {
        FileSystem { path: PathBuf::from(path) }
    }
}

/// Creates a new `FileSystem` instance.
/// Syncable version of `FileSystem::new`.
/// # Arguments
/// * `path` - The path to the database.
/// # Returns
/// * `FileSystem` - The new `FileSystem` instance.
/// # Errors
/// * `String` - The error message.
/// # Examples
/// ```
/// use einstein_db::file::FileSystem;
/// use std::path::Path;
/// use std::fs::{DirBuilder, OpenOptions};
/// use std::sync::Arc;



#[derive(Clone, Debug)]
pub struct FileSystemSync {
    /// The path to the database.
    path: PathBuf,
}



impl FileSystemSync {
    /// Creates a new `FileSystemSync` instance.
    pub fn new(options: FileSystemOptions) -> Self {
        FileSystemSync { path: options.path }
    }
}


#[derive(Clone, Debug)]
pub struct SyncMutable {
    /// The path to the database.
    path: PathBuf,
}




impl SyncMutable {
    /// Creates a new `SyncMutable` instance.
    /// Syncable version of `FileSystem::new`.

    pub fn new(options: FileSystemOptions) -> Self {
        SyncMutable { path: options.path }
    }

    /// Creates a new `SyncMutable` instance.
    /// Syncable version of `FileSystem::new`.

    pub fn new_with_path(path: PathBuf) -> Self {
        SyncMutable { path }
    }

    /// Creates a new `SyncMutable` instance.
    /// Syncable version of `FileSystem::new`.

    pub fn new_with_path_str(path: &str) -> Self {
        SyncMutable { path: PathBuf::from(path) }
    }


    /// Creates a new `SyncMutable` instance.

    pub fn new_with_path_str_sync(path: &str) -> Self {
        SyncMutable { path: PathBuf::from(path) }
    }

    fn put_namespaced_file_sync(&self, namespace: &str, file_name: &str, data: &[u8]) -> Result<(), String> {
        let mut path = self.path.clone();
        path.push(namespace);
        path.push(file_name);
        let mut file = File::create(&path).map_err(|e| format!("{}", e))?;
        file.write_all(data).map_err(|e| format!("{}", e))?;
        Ok(())
    }

    fn delete_namespaced_file_sync(&self, namespace: &str, file_name: &str) -> Result<(), String> {
        let mut path = self.path.clone();
        path.push(namespace);
        path.push(file_name);
        remove_file(&path).map_err(|e| format!("{}", e))?;
        Ok(())
    }

    fn delete_namespaced_dir_sync(&self, namespace: &str) -> Result<(), String> {
        let mut path = self.path.clone();
        path.push(namespace);
        remove_dir_all(&path).map_err(|e| format!("{}", e))?;
        Ok(())
    }

    fn delete_range_sync(&self, namespace: &str, start: u64, end: u64) -> Result<(), String> {
        let mut path = self.path.clone();
        path.push(namespace);
        let mut file = File::open(&path).map_err(|e| format!("{}", e))?;
        let mut file_data = Vec::new();
        file.read_to_end(&mut file_data).map_err(|e| format!("{}", e))?;
        let mut new_file_data = Vec::new();
        for i in 0..file_data.len() {
            if i as u64 >= start && i as u64 <= end {
                continue;
            }
            new_file_data.push(file_data[i]);
        }
        let mut file = File::create(&path).map_err(|e| format!("{}", e))?;
        file.write_all(&new_file_data).map_err(|e| format!("{}", e))?;
        Ok(())
    }

    fn delete_range_namespaced_sync(&self, namespace: &str, start: u64, end: u64) -> Result<(), String> {
        let mut path = self.path.clone();
        path.push(namespace);
        let mut file = File::open(&path).map_err(|e| format!("{}", e))?;
        let mut file_data = Vec::new();
        file.read_to_end(&mut file_data).map_err(|e| format!("{}", e))?;
        let mut new_file_data = Vec::new();
        for i in 0..file_data.len() {
            if i as u64 >= start && i as u64 <= end {
                continue;
            }
            new_file_data.push(file_data[i]);
        }
        let mut file = File::create(&path).map_err(|e| format!("{}", e))?;
        file.write_all(&new_file_data).map_err(|e| format!("{}", e))?;
        Ok(())
    }

    fn put_msg<M: protobuf::Message>(&self, soliton_id: &[u8], m: &M) -> Result<()> {
        let mut path = self.path.clone();
        path.push("messages");
        path.push(soliton_id);
        let mut file = File::create(&path).map_err(|e| format!("{}", e))?;
        let mut data = Vec::new();
        m.write_to_vec(&mut data).map_err(|e| format!("{}", e))?;
        file.write_all(&data).map_err(|e| format!("{}", e))?;
        Ok(())
    }

    fn put_msg_namespaced<M: protobuf::Message>(&self, namespaced: &str, soliton_id: &[u8], m: &M) -> Result<()> {
        self.put_namespaced(namespaced, soliton_id, &m.write_to_bytes()?)
    }
}
