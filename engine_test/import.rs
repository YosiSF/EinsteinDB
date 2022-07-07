// Copyright 2019 EinsteinDB Project Authors. 
// Licensed under Apache-2.0.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0.
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and limitations under the License.
//

use std::{
    collections::HashMap,
    fmt::{self, Debug, Display, Formatter},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    einsteindb_macro::einsteindb_macro,
    engine::{
        self,
        cf::{CF_DEFAULT, CF_LOCK, CF_WRITE},
        CF_DEFAULT as DEFAULT_CF,
        CF_LOCK as LOCK_CF,
        CF_WRITE as WRITE_CF,
        CF_WRITE_WAIT as WRITE_WAIT_CF,
        CF_DEFAULT_INDEX as DEFAULT_INDEX_CF,
        CF_LOCK_INDEX as LOCK_INDEX_CF,
        CF_WRITE_INDEX as WRITE_INDEX_CF,
        CF_WRITE_WAIT_INDEX as WRITE_WAIT_INDEX_CF,
        CF_DEFAULT_MAIN as DEFAULT_MAIN_CF,
        CF_LOCK_MAIN as LOCK_MAIN_CF,
        CF_WRITE_MAIN as WRITE_MAIN_CF,
        CF_WRITE_WAIT_MAIN as WRITE_WAIT_MAIN_CF,
        CF_DEFAULT_WRITE as DEFAULT_WRITE_CF,
        CF_LOCK_WRITE as LOCK_WRITE_CF,
        CF_WRITE_WRITE as WRITE_WRITE_CF,
        CF_WRITE_WAIT_WRITE as WRITE_WAIT_WRITE_CF,
        CF_DEFAULT_COMPACT as DEFAULT_COMPACT_CF,
        CF_LOCK_COMPACT as LOCK_COMPACT_CF,
        CF_WRITE_COMPACT as WRITE_COMPACT_CF,
        CF_WRITE_WAIT_COMPACT as WRITE_WAIT_COMPACT_CF,
        CF_DEFAULT_CHECK as DEFAULT_CHECK_CF,
        CF_LOCK_CHECK as LOCK_CHECK_CF,
        CF_WRITE_CHECK as WRITE_CHECK_CF,
        CF_WRITE_WAIT_CHECK as WRITE_WAIT_CHECK_CF,
        CF_DEFAULT_GC as DEFAULT_GC_CF,
    },
    storage::{
        self,
        kv::{
            self,
            Key,
            KeyValue,
            KeyValueType,
            KeyValueTypePair,
            KeyValuePair,
            KeyValuePairs,
            KeyValues,
            Iterator as KvIterator,
            IteratorMode,
            SeekKey,
            SeekKeyType,
            SeekKeyTypePair,
            SeekKeyPair,
            SeekKeyPairs,
            SeekKeyValues,
            SeekIterator as KvSeekIterator,
            SeekIteratorMode,
        },
        CF_LOCK,
        CF_WRITE,
        CF_WRITE_WAIT,
        CF_DEFAULT_INDEX,
        CF_LOCK_INDEX,
        CF_WRITE_INDEX,
        CF_WRITE_WAIT_INDEX,
        CF_DEFAULT_MAIN,
        CF_LOCK_MAIN,
        CF_WRITE_MAIN,
        CF_WRITE_WAIT_MAIN,
        CF_DEFAULT_WRITE,
        CF_LOCK_WRITE,
        CF_WRITE_WRITE,
        CF_WRITE_WAIT_WRITE,
        CF_DEFAULT_COMPACT,
        CF_LOCK_COMPACT,
        CF_WRITE_COMPACT,
        CF_WRITE_WAIT_COMPACT,
        CF_DEFAULT_CHECK,
        CF_LOCK_CHECK,
        CF_WRITE_CHECK,
        CF_WRITE_WAIT_CHECK,
        CF_DEFAULT_GC,
        CF_LOCK_GC,
        CF_WRITE_GC,
        CF_WRITE_WAIT_GC,
        CF_DEFAULT_RAFT,
        CF_LOCK_RAFT,
        CF_WRITE_RAFT,
        CF_WRITE_WAIT_RAFT,
        CF_DEFAULT_RAFT_INDEX,
        CF_LOCK_RAFT_INDEX,
        CF_WRITE_RAFT_INDEX,
        CF_WRITE_WAIT_RAFT_INDEX,
    }
};


#[derive(Clone, Debug, PartialEq)]
pub enum ImportMode {
    Insert,
    Overwrite,
}


#[derive(Clone, Debug, PartialEq)]
pub enum ImportOption {
    Mode(ImportMode),
    Path(PathBuf),
    Threads(usize),
    Compression(Compression),
    CompressionLevel(i32),
    CompressionType(CompressionType),
    Checksum(bool),
    ChecksumType(ChecksumType),
    ChecksumLevel(i32),
    ChecksumBlockSize(i32),
    ChecksumBlockStart(i32),
    ChecksumBlockEnd(i32),
    ChecksumBlockStep(i32),
    ChecksumBlockOffset(i32),
    ChecksumBlockLength(i32),
    ChecksumBlockMask(i32),
    ChecksumBlockShift(i32),
    ChecksumBlockRotate(i32),
    ChecksumBlockXor(i32),
    ChecksumBlockXorOffset(i32),
    ChecksumBlockXorLength(i32),
    ChecksumBlockXorMask(i32),
    ChecksumBlockXorShift(i32),
    ChecksumBlockXorRotate(i32),
    ChecksumBlockXorRotateOffset(i32),
    ChecksumBlockXorRotateLength(i32),
}


#[derive(Clone, Debug, PartialEq)]
pub enum Compression {
    None,
    Snappy,
    Lz4,
    Zlib,
    Zstd,
}



pub trait ImportExt {

    fn import_kv(&self, path: &Path, options: &[ImportOption]) -> Result<()>;
}


impl ImportExt for Engine {

    fn import_kv(&self, path: &Path, options: &[ImportOption]) -> Result<()> {
        let mut opts = ImportOptions::new();
        for option in options {
            match option {
                ImportOption::Mode(mode) => opts.mode = mode.clone(),
                ImportOption::Path(path) => opts.path = path.clone(),
                ImportOption::Threads(threads) => opts.threads = *threads,
                ImportOption::Compression(compression) => opts.compression = compression.clone(),
                ImportOption::CompressionLevel(level) => opts.compression_level = *level,
                ImportOption::CompressionType(compression_type) => opts.compression_type = compression_type.clone(),
                ImportOption::Checksum(checksum) => opts.checksum = *checksum,
                ImportOption::ChecksumType(checksum_type) => opts.checksum_type = checksum_type.clone(),
                ImportOption::ChecksumLevel(level) => opts.checksum_level = *level,
                ImportOption::ChecksumBlockSize(block_size) => opts.checksum_block_size = *block_size,
                ImportOption::ChecksumBlockStart(start) => opts.checksum_block_start = *start,
                ImportOption::ChecksumBlockEnd(end) => opts.checksum_block_end = *end,
                ImportOption::ChecksumBlockStep(step) => opts.checksum_block_step = *step,
                ImportOption::ChecksumBlockOffset(offset) => opts.checksum_block_offset = *offset,
                ImportOption::ChecksumBlockLength(length) => opts.checksum_block_length = *length,
                ImportOption::ChecksumBlockMask(mask) => opts.checksum_block_mask = *mask,
                ImportOption::ChecksumBlockShift(shift) => opts.checksum_block_shift = *shift,
            } // match
        } // for
        self.import_kv(path, &opts)
    }
}


#[derive(Clone, Debug, PartialEq)]
pub struct ImportOptions {
    pub mode: ImportMode,
    pub path: PathBuf,
    pub threads: usize,
    pub compression: Compression,
    pub compression_level: i32,
    pub compression_type: CompressionType,
    pub checksum: bool,
    pub checksum_type: ChecksumType,
    pub checksum_level: i32,
    pub checksum_block_size: i32,
    pub checksum_block_start: i32,
    pub checksum_block_end: i32,
    pub checksum_block_step: i32,
    pub checksum_block_offset: i32,
}


impl ImportOptions {
    pub fn new() -> ImportOptions {
        ImportOptions {
            mode: ImportMode::Insert,
            path: PathBuf::new(),
            threads: 1,
            compression: Compression::None,
            compression_level: 0,
            compression_type: CompressionType::Snappy,
            checksum: false,
            checksum_type: ChecksumType::Crc32,
            checksum_level: 0,
            checksum_block_size: 0,
            checksum_block_start: 0,
            checksum_block_end: 0,
            checksum_block_step: 0,
            checksum_block_offset: 0,
        }
    }

    pub fn set_mode(&mut self, mode: ImportMode) {
        self.mode = mode;
    }

    pub fn set_path(&mut self, path: PathBuf) {
        self.path = path;
    }

    pub fn set_threads(&mut self, threads: usize) {
        self.threads = threads;
    }

    pub fn set_compression(&mut self, compression: Compression) {
        self.compression = compression;
    }

    pub fn set_compression_level(&mut self, level: i32) {
        self.compression_level = level;
    }

    pub fn set_compression_type(&mut self, compression_type: CompressionType) {
        self.compression_type = compression_type;
    }

    pub fn set_checksum(&mut self, checksum: bool) {
        self.checksum = checksum;
    }

    pub fn set_checksum_type(&mut self, checksum_type: ChecksumType) {
        self.checksum_type = checksum_type;
    }

    pub fn set_checksum_level(&mut self, level: i32) {
        self.checksum_level = level;
    }

    pub fn set_checksum_block_size(&mut self, block_size: i32) {
        self.checksum_block_size = block_size;
    }

    pub fn set_checksum_block_start(&mut self, start: i32) {
        self.checksum_block_start = start;
    }

    pub fn set_checksum_block_end(&mut self, end: i32) {
        self.checksum_block_end = end;
    }

    pub fn set_checksum_block_step(&mut self, step: i32) {
        self.checksum_block_step = step;
    }

    pub fn set_checksum_block_offset(&mut self, offset: i32) {
        self.checksum_block_offset = offset;
    }

    pub fn set_checksum_block_length(&mut self, length: i32) {
        self.checksum_block_length = length;
    }

    pub fn set_checksum_block_mask(&mut self, mask: i32) {
        self.checksum_block_mask = mask;
    }

    pub fn set_checksum_block_shift(&mut self, shift: i32) {
        self.checksum_block_shift = shift;
    }

    pub fn get_mode(&self) -> ImportMode {
        self.mode
    }

    pub fn get_path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn get_threads(&self) -> usize {
        self.threads
    }

    pub fn get_compression(&self) -> Compression {
        self.compression
    }

    pub fn get_compression_level(&self) -> i32 {
        self.compression_level
    }

    pub fn get_compression_type(&self) -> CompressionType {
        self.compression_type
    }

    pub fn get_checksum(&self) -> bool {
        self.checksum
    }

    pub fn to_ingestlightlike_file_options(&self) -> Result<Self::IngestlightlikeFileOptions> {
        let mut opts = IngestlightlikeFileOptions::new();
        opts.mode = self.mode.clone();
        opts.path = self.path.clone();
        opts.threads = self.threads;
        opts.compression = self.compression.clone();
        opts.compression_level = self.compression_level;
        opts.compression_type = self.compression_type.clone();
        opts.checksum = self.checksum;
        opts.checksum_type = self.checksum_type.clone();
        opts.checksum_level = self.checksum_level;
        opts.checksum_block_size = self.checksum_block_size;
        opts.checksum_block_start = self.checksum_block_start;
        opts.checksum_block_end = self.checksum_block_end;
        opts.checksum_block_step = self.checksum_block_step;
        opts.checksum_block_offset = self.checksum_block_offset;
        opts.checksum_block_length = self.checksum_block_length;
        Ok(opts)
    }

    pub fn from_ingestlightlike_file_options(opts: Self::IngestlightlikeFileOptions) -> Result<Self> {
        let mut import_opts = ImportOptions::new();
        import_opts.mode = opts.mode.clone();
        import_opts.path = opts.path.clone();
        import_opts.threads = opts.threads;
        import_opts.compression = opts.compression.clone();
        import_opts.compression_level = opts.compression_level;
        import_opts.compression_type = opts.compression_type.clone();
        import_opts.checksum = opts.checksum;
        import_opts.checksum_type = opts.checksum_type.clone();
        import_opts.checksum_level = opts.checksum_level;
        import_opts.checksum_block_size = opts.checksum_block_size;
        import_opts.checksum_block_start = opts.checksum_block_start;
        import_opts.checksum_block_end = opts.checksum_block_end;
        import_opts.checksum_block_step = opts.checksum_block_step;
        import_opts.checksum_block_offset = opts.checksum_block_offset;
        import_opts.checksum_block_length = opts.checksum_block_length;
        Ok(import_opts)
    }

    pub fn to_ingestlightlike_file_options_string(&self) -> Result<String> {
        let mut opts = IngestlightlikeFileOptions::new();
        opts.mode = self.mode.clone();
        opts.path = self.path.clone();
        opts.threads = self.threads;
        opts.compression = self.compression.clone();
        opts.compression_level = self.compression_level;
        opts.compression_type = self.compression_type.clone();
        opts.checksum = self.checksum;
        opts.checksum_type = self.checksum_type.clone();
        opts.checksum_level = self.checksum_level;
        opts.checksum_block_size = self.checksum_block_size;
        opts.checksum_block_start = self.checksum_block_start;
        opts.checksum_block_end = self.checksum_block_end;
        opts.checksum_block_step = self.checksum_block_step;
        opts.checksum_block_offset = self.checksum_block_offset;
        opts.checksum_block_length = self.checksum_block_length;
        Ok(opts.to_string())
    }
    fn ingest_lightlike_file_namespaced(&self, namespaced: bool) -> Result<()> {
        let mut opts = IngestlightlikeFileOptions::new();
        opts.mode = self.mode.clone();
        opts.path = self.path.clone();
        opts.threads = self.threads;
        opts.compression = self.compression.clone();
        opts.compression_level = self.compression_level;
        opts.compression_type = self.compression_type.clone();
        opts.checksum = self.checksum;
        opts.checksum_type = self.checksum_type.clone();
        opts.checksum_level = self.checksum_level;
        opts.checksum_block_size = self.checksum_block_size;
        opts.checksum_block_start = self.checksum_block_start;
        opts.checksum_block_end = self.checksum_block_end;
        opts.checksum_block_step = self.checksum_block_step;
        opts.checksum_block_offset = self.checksum_block_offset;
        opts.checksum_block_length = self.checksum_block_length;
        opts.namespaced = namespaced;
        opts.to_ingestlightlike_file()
    }
}

pub trait IngestlightlikeFileOptions {

    /// The mode to use when creating the file.
    /// The default is `FileMode::Create`.
    /// 
    /// # Arguments
    /// * `mode` - The mode to use when creating the file.
    /// The default is `FileMode::Create`.
    /// 
    /// # Example
    /// ```rust
    /// use ingestlightlike::ImportOptions;
    /// use ingestlightlike::FileMode;
    /// 
    /// let mut opts = ImportOptions::new();
    /// opts.set_mode(FileMode::Create);
    /// ```
    /// # Errors
    /// * `Error::InvalidFileMode` - The mode is invalid.
    /// 
    

    fn set_mode(&mut self, mode: FileMode) -> Result<()>;

    /// The path to the file to import.
    /// The default is `"./ingestlightlike.json"`.
    ///     
    /// # Arguments
    /// * `path` - The path to the file to import.
    /// The default is `"./ingestlightlike.json"`.
    /// 
    /// # Example
    /// ```rust
    /// use ingestlightlike::ImportOptions;
    /// 
    /// let mut opts = ImportOptions::new();
    /// opts.set_path("./ingestlightlike.json");
    /// ```
    /// # Errors
    /// * `Error::InvalidPath` - The path is invalid.
    

    fn set_path(&mut self, path: &str) -> Result<()>;

    /// The number of threads to use when importing.
    /// The default is `1`.
    /// 
    /// # Arguments
    /// * `threads` - The number of threads to use when importing.
    /// The default is `1`.
    /// 
    /// # Example
    /// ```rust
    /// use ingestlightlike::ImportOptions;
    /// 
    /// let mut opts = ImportOptions::new();
    /// opts.set_threads(2);
    /// ```
    /// # Errors
    /// * `Error::InvalidThreads` - The number of threads is invalid.
    /// 
    /// # Panics
    /// * `Error::InvalidThreads` - The number of threads is invalid.
    

    fn set_threads(&mut self, threads: u32) -> Result<()>;
    fn new() -> Self;

    fn move_filefs(&mut self, f: bool);

    fn get_write_global_seqno(&self) -> bool;


    fn set_write_global_seqno(&mut self, write_global_seqno: bool);

  
}
