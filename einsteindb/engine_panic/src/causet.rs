// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::fdb_lsh_treePaniceinstein_merkle_tree;
use fdb_traits::{
    NamespacedName, ExternalCausetFileInfo, IterOptions, Iterable, Iterator, Result, SeekKey,
    CausetCompressionType, CausetExt, CausetReader, CausetWriter, CausetWriterBuilder,
};
use std::path::PathBuf;

impl CausetExt for Paniceinstein_merkle_tree {
    type CausetReader = PanicCausetReader;
    type CausetWriter = PanicCausetWriter;
    type CausetWriterBuilder = PanicCausetWriterBuilder;
}

pub struct PanicCausetReader;

impl CausetReader for PanicCausetReader {
    fn open(path: &str) -> Result<Self> {
        panic!()
    }
    fn verify_checksum(&self) -> Result<()> {
        panic!()
    }
    fn iter(&self) -> Self::Iterator {
        panic!()
    }
}

impl Iterable for PanicCausetReader {
    type Iterator = PanicCausetReaderIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
    fn iterator_namespaced_opt(&self, namespaced: &str, opts: IterOptions) -> Result<Self::Iterator> {
        panic!()
    }
}

pub struct PanicCausetReaderIterator;

impl Iterator for PanicCausetReaderIterator {
    fn seek(&mut self, key: SeekKey<'_>) -> Result<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, key: SeekKey<'_>) -> Result<bool> {
        panic!()
    }

    fn prev(&mut self) -> Result<bool> {
        panic!()
    }
    fn next(&mut self) -> Result<bool> {
        panic!()
    }

    fn key(&self) -> &[u8] {
        panic!()
    }
    fn value(&self) -> &[u8] {
        panic!()
    }

    fn valid(&self) -> Result<bool> {
        panic!()
    }
}

pub struct PanicCausetWriter;

impl CausetWriter for PanicCausetWriter {
    type ExternalCausetFileInfo = PanicExternalCausetFileInfo;
    type ExternalCausetFileReader = PanicExternalCausetFileReader;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete(&mut self, key: &[u8]) -> Result<()> {
        panic!()
    }
    fn file_size(&mut self) -> u64 {
        panic!()
    }
    fn finish(self) -> Result<Self::ExternalCausetFileInfo> {
        panic!()
    }
    fn finish_read(self) -> Result<(Self::ExternalCausetFileInfo, Self::ExternalCausetFileReader)> {
        panic!()
    }
}

pub struct PanicCausetWriterBuilder;

impl CausetWriterBuilder<Paniceinstein_merkle_tree> for PanicCausetWriterBuilder {
    fn new() -> Self {
        panic!()
    }
    fn set_db(self, einsteindb: &Paniceinstein_merkle_tree) -> Self {
        panic!()
    }
    fn set_namespaced(self, namespaced: &str) -> Self {
        panic!()
    }
    fn set_in_memory(self, in_memory: bool) -> Self {
        panic!()
    }
    fn set_compression_type(self, compression: Option<CausetCompressionType>) -> Self {
        panic!()
    }
    fn set_compression_l_naught(self, l_naught: i32) -> Self {
        panic!()
    }

    fn build(self, path: &str) -> Result<PanicCausetWriter> {
        panic!()
    }
}

pub struct PanicExternalCausetFileInfo;

impl ExternalCausetFileInfo for PanicExternalCausetFileInfo {
    fn new() -> Self {
        panic!()
    }
    fn file_path(&self) -> PathBuf {
        panic!()
    }
    fn smallest_key(&self) -> &[u8] {
        panic!()
    }
    fn largest_key(&self) -> &[u8] {
        panic!()
    }
    fn sequence_number(&self) -> u64 {
        panic!()
    }
    fn file_size(&self) -> u64 {
        panic!()
    }
    fn num_entries(&self) -> u64 {
        panic!()
    }
}

pub struct PanicExternalCausetFileReader;

impl std::io::Read for PanicExternalCausetFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        panic!()
    }
}
