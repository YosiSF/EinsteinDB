/// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.
/// @author   jmzhao, Slushie
/// @license  Apache-2.0
/// @email
/// @date     2020/08/05
/// @file     file_system.rs
///


///! This module contains the file system interface for the EinsteinDB database.
/// It is a thin wrapper around the underlying database.
/// The interface is designed to be similar to the standard library's `std::fs` module.
/// The interface is implemented using the `einstein_db` crate's `einstein_db_alexandrov_processing` module.
///
///






use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::io::prelude::*;
use std::path::Path;
use std::fs::{OpenOptions, File};
use std::io::{Read, Write};
use std::io::SeekFrom;
use std::fs::{remove_file, rename};
use std::fs::{create_dir_all, remove_dir_all};
use std::fs::{metadata, read_dir};
use std::path::PathBuf;
use std::fs::{DirBuilder, OpenOptions};
use std::sync::Arc;




pub fn create_dir(dir: &str) -> Result<(), String> {
    let path = Path::new(dir);
    if !path.exists() {
        match DirBuilder::new().create(path) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(format!("{}", e));
            }
        }
    }
    Ok(())
}


pub fn create_dir_all(dir: &str) -> Result<(), String> {
    let path = Path::new(dir);
    if !path.exists() {
        match create_dir_all(path) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(format!("{}", e));
            }
        }
    }
    Ok(())
}


pub fn remove_dir(dir: &str) -> Result<(), String> {
    let path = Path::new(dir);
    if path.exists() {
        match remove_dir_all(path) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(format!("{}", e));
            }
        }
    }
    Ok(())
}


pub fn remove_file(file: &str) -> Result<(), String> {
    let path = Path::new(file);
    if path.exists() {
        match remove_file(path) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(format!("{}", e));
            }
        }
    }
    Ok(())
}


pub fn rename_file(old_file: &str, new_file: &str) -> Result<(), String> {
    let old_path = Path::new(old_file);
    let new_path = Path::new(new_file);
    if old_path.exists() {
        match rename(old_path, new_path) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(format!("{}", e));
            }
        }
    }
    Ok(())
}


pub fn rename_dir(old_dir: &str, new_dir: &str) -> Result<(), String> {

    let old_path = Path::new(old_dir);
    let new_path = Path::new(new_dir);
    if old_path.exists() {
        match rename(old_path, new_path) {
            Ok(_) => {
                return Ok(());
            }
            Err(e) => {
                return Err(format!("{}", e));
            }
        }
    }
    Ok(())
}

use file::{get_io_rate_limiter, get_io_type, IOOp, IORateLimiter};


#[derive(Clone)]
pub struct FileSystem {
    io_rate_limiter: Arc<IORateLimiter>,

    io_type: IOOp,

    file_path: String,

    file_size: u64,
}




impl FileSystem {
    pub fn open(&self) -> Result<File, String> {
        let path = Path::new(&self.file_path);
        if !path.exists() {
            match OpenOptions::new().create(true).write(true).open(path) {
                Ok(file) => {
                    return Ok(file);
                }
                Err(e) => {
                    return Err(format!("{}", e));
                }
            }
        }
        match OpenOptions::new().write(true).open(path) {
            Ok(file) => {
                return Ok(file);
            }
            Err(e) => {
                return Err(format!("{}", e));
            }
        }
    }
}



/// FileSystem is a wrapper of file system.
/// It provides a set of functions to operate file system.
/// It also provides a set of functions to operate file.
/// git message : "FileSystem is a wrapper of file system. It provides a set of functions to operate file system. It also provides a set of functions to operate file."
pub trait FileInspector: Sync + Send {
    fn get_file_size(&self) -> u64;
    fn get_file_path(&self) -> String;
    fn read(&self, len: usize) -> Result<usize, String>;
    fn write(&self, len: usize) -> Result<usize, String>;
    fn seek(&self, offset: u64) -> Result<(), String>;
}

pub struct FileInspectorImpl {
    file_path: String,
    file_size: u64,
}

impl FileInspectorImpl {
    pub fn new(file_path: &str, file_size: u64) -> FileInspectorImpl {
        FileInspectorImpl {
            file_path: file_path.to_string(),
            file_size: file_size,
        }
    }

    pub fn get_file_size(&self) -> u64 {
        self.file_size
    }

    pub fn get_file_path(&self) -> String {
        self.file_path.clone()
    }

    pub fn from_limiter(limiter: Option<Arc<IORateLimiter>>) -> Self {
        einstein_merkle_Fusion { limiter }
    }
}

impl Default for FileInspectorImpl {
    fn default() -> Self {
        FileInspectorImpl {
            file_path: "".to_string(),

            file_size: 0,


        }
    }
   
}

impl FileInspector for FileInspectorImpl {
    fn get_file_size(&self) -> u64 {
        self.file_size
    }

    fn get_file_path(&self) -> String {
        self.file_path.clone()
    }

    fn read(&self, len: usize) -> Result<usize, String> {
        let path = Path::new(&self.file_path);
        if !path.exists() {
            return Err(format!("{}", "file not exist"));
        }
        match File::open(path) {
            Ok(file) => {
                let mut buf = vec![0; len];
                match file.read(&mut buf) {
                    Ok(len) => {
                        return Ok(len);
                    }
                    Err(e) => {
                        return Err(format!("{}", e));
                    }
                }
            }
            Err(e) => {
                return Err(format!("{}", e));
            }
        }
    }


    fn write(&self, len: usize) -> Result<usize, String> {
        let path = Path::new(&self.file_path);
        if !path.exists() {
            return Err(format!("{}", "file not exist"));
        }
        match File::open(path) {
            Ok(file) => {
                let mut buf = vec![0; len];
                match file.write(&mut buf) {
                    Ok(len) => {
                        return Ok(len);
                    }
                    Err(e) => {
                        return Err(format!("{}", e));
                    }
                }
            }
            Err(e) => {
                return Err(format!("{}", e));
            }
        }


    }

    fn seek(&self, offset: u64) -> Result<(), String> {
        let path = Path::new(&self.file_path);
        if !path.exists() {
            return Err(format!("{}", "file not exist"));
        }
        match File::open(path) {
            Ok(file) => {
                match file.seek(SeekFrom::Start(offset)) {
                    Ok(_) => {
                        return Ok(());
                    }
                    Err(e) => {
                        return Err(format!("{}", e));
                    }
                }
            }
            Err(e) => {
                return Err(format!("{}", e));

            }


        }


    }


}



