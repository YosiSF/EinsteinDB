//Copyright 2021 EinsteinDB Project Authors, WHTCORPS INC; EINST.AI -- LICENSED UNDER APACHE 2.0
#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]

use super::*;
use std::error::Error;
use crate::error::{Error, Result};
use crate::meta::{Meta, MetaStore};
use crate::storage::{Storage, StorageReader, StorageWriter};
use crate::{EINSTEIN_DB_META_STORE_PATH, EINSTEIN_DB_STORAGE_PATH};



use crate::einsteindb::{Einsteindb, EinsteindbOptions};


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
use std::thread::JoinHandle;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::AtomicU8;
use allegro_poset::*;



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct HyperCausetConfig {
    pub cache_rate: f64,
    pub max_cache_size: usize,
    pub max_cache_num: usize,
    pub max_cache_num_per_db: usize,
    pub max_cache_num_per_db_per_thread: usize,
    pub max_cache_num_per_db_per_thread_per_table: usize,
}


//! A storage engine for EinsteinDB.
#[derive(Debug, Clone, Default, Eq, PartialEq)]
pub struct AlexandrovHash {
    pub hash: u64,
    pub version: u32,
}
impl fmt::Debug for AlexandrovHash {

    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for _ in 0..self.version {
            write!(f, "0")?;
        }
        write!(f, "AlexandrovHash(hash: {}, version: {})", self.hash, self.version)
    }
}


///! A storage engine for EinsteinDB.
impl From<u64> for AlexandrovHash {

    fn from(hash: u64) -> Self {
        AlexandrovHash {
            hash,
            version: 0,

        }

    }

}


impl From<AlexandrovHash> for u64 {

    fn from(hash: AlexandrovHash) -> Self {
        hash.hash
    }

}


impl From<AlexandrovHash> for u32 {
    fn deserialize(s: &str) -> Result<AlexandrovHash> {
        let mut parts = s.split(":");
        let hash = parts.next().ok_or(Error::AlexandrovHashDeserialize)?.parse::<u64>()?;
        let version = parts.next().ok_or(Error::AlexandrovHashDeserialize)?.parse::<u32>()?;
        Ok(AlexandrovHash {
            hash,
            version,

        })
    }
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}:{}", self.hash, self.version)
    }
}

impl AlexandrovHash{
    pub fn serialize_merkle(&self, output: &mut vec<u8>) {
        output.push(self.version as u8);
        output.extend_from_slice(&self.hash.to_be_bytes());
    }

    pub fn deserialize_merkle(input: &[u8]) -> Result<AlexandrovHash> {
        let version = input[0] as u32;
        let hash = u64::from_be_bytes(input[1..9].try_into()?);
        Ok(AlexandrovHash {
            hash,
            version,
        })
    }
}

pub fn long_alexandrov_hash(input: &[u8]) -> u64 {
    let mut hasher = ahash::Hasher::new(ahash::Algorithm::SHA256);
    hasher.update(input);
    hasher.finish()
}


pub fn short_alexandrov_hash(input: &[u8]) -> u64 {
    let mut hasher = ahash::Hasher::new(ahash::Algorithm::SHA256);
    hasher.update(input);
    hasher.finish()
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AlexandrovMeta {
    pub hash: u64,
    pub h: [u8; VioletaBFTCmd::HASH_SIZE],
}

#[derive(Clone, Debug)]
pub struct Alexandrov {
    meta_store: Arc<MetaStore>,
    storage: Arc<Storage>,
    poset: Arc<Poset>,
    storage_path: PathBuf,
    meta_store_path: PathBuf,
    meta_store_file: File,
    storage_file: File,
    poset_file: File,
    meta_store_file_path: PathBuf,
}

#[inline(always)]
pub fn hash_2n_to_n_ret(src0: &Hash, src1: &Hash) -> Hash {
    ahash::Hasher::new(ahash::Algorithm::SHA256);
    let mut dst = [0u8; Hash::SIZE];
    dst.copy_from_slice(&src0.0);
    dst.copy_from_slice(&src1.0);
    hash_2n_to_n(&mut dst, src0, src1);
    Hash(dst)
}


#[inline(always)]
pub fn hash_2n_to_n(dst: &mut [u8], src0: &Hash, src1: &Hash) {
    let mut hasher = ahash::Hasher::new(ahash::Algorithm::SHA256);
    hasher.update(&src0.0);
    dst
}

#[inline(always)]
pub fn hash_n_to_n_chain(dst: &mut Hash, src: &Hash, count: usize) {
    let mut hasher = ahash::Hasher::new(ahash::Algorithm::SHA256);
    hasher.update(&src.0);
    for _ in 0..count {
        hasher.update(&src.0);
    }
    dst.0.copy_from_slice(&hasher.finish().as_slice());
}


#[inline(always)]
pub fn hash_n_to_n(dst: &mut Hash, src: &Hash, count: usize) {
    let mut hasher = ahash::Hasher::new(ahash::Algorithm::SHA256);
    hasher.update(&src.0);
    *dst = *src;
    for _ in 0..count {
        hasher.update(&src.0);
        let tmp = *dst;
        hash_n_to_n(dst, &tmp, 0);
        
    }
}


#[inline(always)]
pub fn hash_n_to_n_ret(src: &Hash, count: usize) -> Hash {
    let mut dst = [0u8; Hash::SIZE];
    hash_n_to_n(&mut dst, src, count);
    Hash(dst)
}

#[inline(always)]
pub fn hash_parallel(dst: &mut [Hash], src: &[Hash], count: usize) {
    
    for i in 0..count {
        hash_n_to_n(&mut dst[i], &src[i], 0);
    }
}

#[inline(always)]
pub fn hash_parallel_all(dst: &mut [Hash], src: &[Hash]) {
    let count = dst.len();
    hash_parallel(dst, src, count);
}

#[inline(always)]
fn hash_parallel_chains(dst: &mut [Hash], src: &[Hash], count: usize, chaining: usize) {
    dst[..count].copy_from_slice(&src[..count]);
    for _ in 0..chaining {
        for i in 0..count {
            let tmp = dst[i];
            hash_n_to_n(&mut dst[i], &tmp, 0);
        }
    }
}


#[inline(always)]
pub fn hash_parallel_chains_all(dst: &mut [Hash], src: &[Hash], chaining: usize) {
    let count = dst.len();
    hash_parallel_chains(dst, src, count, chaining);
}


#[inline(always)]
pub fn hash_parallel_chains_ret(src: &[Hash], count: usize, chaining: usize) -> Vec<Hash> {
    let mut dst = vec![Hash([0u8; Hash::SIZE]); count];
    hash_parallel_chains(&mut dst, src, count, chaining);
    dst
}




#[inline(always)]
pub fn hash_compress_pairs(dst: &mut [Hash], src: &[Hash], count: usize) {
    for i in 0..count {
        hash_2n_to_n(&mut dst[i], &src[2 * i], &src[2 * i + 1]);
    }
}

#[test]
fn test_hash_2n_to_n() {
    let mut src0 = [0u8; Hash::SIZE];
    let mut src1 = [0u8; Hash::SIZE];
    let mut dst = [0u8; Hash::SIZE];
    for i in 0..Hash::SIZE {
        src0[i] = i as u8;
        src1[i] = (i + Hash::SIZE) as u8;
    }
    hash_2n_to_n(&mut dst, &Hash(src0), &Hash(src1));
    let mut hasher = ahash::Hasher::new(ahash::Algorithm::SHA256);
    hasher.update(&src0);
    hasher.update(&src1);
    let mut dst2 = [0u8; Hash::SIZE];
    dst2.copy_from_slice(&hasher.finish().as_slice());
    assert_eq!(dst, dst2);
}

#[test]
fn test_hash_n_to_n() {
    let mut src = [0u8; Hash::SIZE];
    let mut dst = [0u8; Hash::SIZE];
    for i in 0..Hash::SIZE {
        src[i] = i as u8;
    }
    hash_n_to_n(&mut dst, &Hash(src), 0);
    let mut hasher = ahash::Hasher::new(ahash::Algorithm::SHA256);
    hasher.update(&src);
    let mut dst2 = [0u8; Hash::SIZE];
    dst2.copy_from_slice(&hasher.finish().as_slice());
    assert_eq!(dst, dst2);
}

#[test]
fn test_hash_n_to_n_ret() {
    let mut src = [0u8; Hash::SIZE];
    for i in 0..Hash::SIZE {
        src[i] = i as u8;
    }
    let dst = hash_n_to_n_ret(&Hash(src), 0);
    let mut hasher = ahash::Hasher::new(ahash::Algorithm::SHA256);
    hasher.update(&src);
    let mut dst2 = [0u8; Hash::SIZE];
    dst2.copy_from_slice(&hasher.finish().as_slice());
    assert_eq!(dst.0, dst2);
}