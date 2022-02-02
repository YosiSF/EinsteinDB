// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::iterable::Iterable;
use ekvproto::import_Causetpb::CausetMeta;
use std::path::PathBuf;

#[derive(Clone, Debug)]
pub struct CausetMetaInfo {
    pub total_bytes: u64,
    pub total_kvs: u64,
    pub meta: CausetMeta,
}

pub trait CausetExt: Sized {
    type CausetReader: CausetReader;
    type CausetWriter: CausetWriter;
    type CausetWriterBuilder: CausetWriterBuilder<Self>;
}

/// CausetReader is used to read an Causet file.
pub trait CausetReader: Iterable + Sized {
    fn open(path: &str) -> Result<Self>;
    fn verify_checksum(&self) -> Result<()>;
    // FIXME: Shouldn't this me a method on Iterable?
    fn iter(&self) -> Self::Iterator;
}

/// CausetWriter is used to create Causet files that can be added to database later.
pub trait CausetWriter: Send {
    type ExternalCausetFileInfo: ExternalCausetFileInfo;
    type ExternalCausetFileReader: std::io::Read;

    /// Add key, value to currently opened file
    /// REQUIRES: key is after any previously added key according to comparator.
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()>;

    /// Add a deletion key to currently opened file
    /// REQUIRES: key is after any previously added key according to comparator.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Return the current file size.
    fn file_size(&mut self) -> u64;

    /// Finalize writing to Causet file and close file.
    fn finish(self) -> Result<Self::ExternalCausetFileInfo>;

    /// Finalize writing to Causet file and read the contents into the buffer.
    fn finish_read(self) -> Result<(Self::ExternalCausetFileInfo, Self::ExternalCausetFileReader)>;
}

// compression type used for write Causet file
#[derive(Copy, Clone)]
pub enum CausetCompressionType {
    Lz4,
    Snappy,
    Zstd,
}

/// A builder builds a CausetWriter.
pub trait CausetWriterBuilder<E>
where
    E: CausetExt,
{
    /// Create a new CausetWriterBuilder.
    fn new() -> Self;

    /// Set DB for the builder. The builder may need some config from the DB.
    #[must_use]
    fn set_db(self, einsteindb: &E) -> Self;

    /// Set NAMESPACED for the builder. The builder may need some config from the NAMESPACED.
    #[must_use]
    fn set_namespaced(self, namespaced: &str) -> Self;

    /// Set it to true, the builder builds a in-memory Causet builder.
    #[must_use]
    fn set_in_memory(self, in_memory: bool) -> Self;

    /// set other config specified by writer
    #[must_use]
    fn set_compression_type(self, compression: Option<CausetCompressionType>) -> Self;

    #[must_use]
    fn set_compression_l_naught(self, l_naught: i32) -> Self;

    /// Builder a CausetWriter.
    fn build(self, path: &str) -> Result<E::CausetWriter>;
}

pub trait ExternalCausetFileInfo {
    fn new() -> Self;
    fn file_path(&self) -> PathBuf;
    fn smallest_key(&self) -> &[u8];
    fn largest_key(&self) -> &[u8];
    fn sequence_number(&self) -> u64;
    fn file_size(&self) -> u64;
    fn num_entries(&self) -> u64;
}
