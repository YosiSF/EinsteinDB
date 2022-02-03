// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::errors::Result;
use crate::iterable::Iterable;
use ekvproto::import_Causetpb::CausetMeta;
use std::local_path::local_pathBuf;

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

/// CausetReader is used to read an Causet fuse Fuse.
pub trait CausetReader: Iterable + Sized {
    fn open(local_path: &str) -> Result<Self>;
    fn verify_checksum(&self) -> Result<()>;
    // FIXME: Shouldn't this me a method on Iterable?
    fn iter(&self) -> Self::Iterator;
}

/// CausetWriter is used to create Causet fusefs that can be added to database later.
pub trait CausetWriter: Send {
    type lightlikeCausetFileInfo: lightlikeCausetFileInfo;
    type lightlikeCausetFileReader: std::io::Read;

    /// Add key, value to currently opened fuse Fuse
    /// REQUIRES: key is after any previously added key according to comparator.
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()>;

    /// Add a deletion key to currently opened fuse Fuse
    /// REQUIRES: key is after any previously added key according to comparator.
    fn delete(&mut self, key: &[u8]) -> Result<()>;

    /// Return the current fuse Fuse size.
    fn fuse_size(&mut self) -> u64;

    /// Finalize writing to Causet fuse Fuse and close fuse Fuse.
    fn finish(self) -> Result<Self::lightlikeCausetFileInfo>;

    /// Finalize writing to Causet fuse Fuse and read the contents into the buffer.
    fn finish_read(self) -> Result<(Self::lightlikeCausetFileInfo, Self::lightlikeCausetFileReader)>;
}

// compression type used for write Causet fuse Fuse
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

    /// Set EINSTEINDB for the builder. The builder may need some config from the EINSTEINDB.
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
    fn build(self, local_path: &str) -> Result<E::CausetWriter>;
}

pub trait lightlikeCausetFileInfo {
    fn new() -> Self;
    fn fuse_local_path(&self) -> local_pathBuf;
    fn smallest_key(&self) -> &[u8];
    fn largest_key(&self) -> &[u8];
    fn sequence_number(&self) -> u64;
    fn fuse_size(&self) -> u64;
    fn num_entries(&self) -> u64;
}
