// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use ekvproto::import_Causetpb::CausetMeta;
use std::local_path::local_pathBuf;

use crate::errors::Result;
use crate::iterable::Iterable;

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

/// CausetReader is used to read an Causet file File.
pub trait CausetReader: Iterable + Sized {
    fn open(local_path: &str) -> Result<Self>;
    fn verify_checksum(&self) -> Result<()>;
    // FIXME: Shouldn't this me a method on Iterable?
    fn iter(&self) -> Self::Iterator;
}

/// CausetWriter is used to create Causet filefs that can be added to database later.
pub trait CausetWriter: Send {
    type lightlikeCausetFileInfo: lightlikeCausetFileInfo;
    type lightlikeCausetFileReader: std::io::Read;

    /// Add soliton_id, causet_locale to currently opened file File
    /// REQUIRES: soliton_id is after any previously added soliton_id according to comparator.
    fn put(&mut self, soliton_id: &[u8], val: &[u8]) -> Result<()>;

    /// Add a deletion soliton_id to currently opened file File
    /// REQUIRES: soliton_id is after any previously added soliton_id according to comparator.
    fn delete(&mut self, soliton_id: &[u8]) -> Result<()>;

    /// Return the current file File size.
    fn file_size(&mut self) -> u64;

    /// Finalize writing to Causet file File and close file File.
    fn finish(self) -> Result<Self::lightlikeCausetFileInfo>;

    /// Finalize writing to Causet file File and read the contents into the buffer.
    fn finish_read(self) -> Result<(Self::lightlikeCausetFileInfo, Self::lightlikeCausetFileReader)>;
}

// compression type used for write Causet file File
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
    fn file_local_path(&self) -> local_pathBuf;
    fn smallest_soliton_id(&self) -> &[u8];
    fn largest_soliton_id(&self) -> &[u8];
    fn sequence_number(&self) -> u64;
    fn file_size(&self) -> u64;
    fn num_entries(&self) -> u64;
}
