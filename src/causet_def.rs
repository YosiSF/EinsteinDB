// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CausetDefinition{
    pub name: &'static str,
    pub keyword: &'static str,
    pub attributes: Vec<Keyword, Attribute>,
    pub timelike: Vec<Keyword, Timelike>,
    pub pre_order: fn(&mut Causet, &mut CausetDefinition, &mut CausetDefinition, &mut CausetDefinition),
    pub post_order: fn(&mut Causet, &mut CausetDefinition, &mut CausetDefinition, &mut CausetDefinition),
}

impl CausetDefinition {
    pub fn new(name: &'static str, keyword: &'static str, attributes: Vec<Keyword, Attribute>, timelike: Vec<Keyword, Timelike>, pre_order: fn(&mut Causet, &mut CausetDefinition, &mut CausetDefinition, &mut CausetDefinition), post_order: fn(&mut Causet, &mut CausetDefinition, &mut CausetDefinition, &mut CausetDefinition)) -> Self {
        CausetDefinition {
            name,
            keyword,
            attributes,
            timelike,
            pre_order,
            post_order,
        }
    }

    pub fn get_name(&self) -> &'static str {
        self.name
    }

    pub fn get_keyword(&self) -> &'static str {
        self.keyword
    }

    pub fn get_attributes(&self) -> &Vec<Keyword, Attribute> {
        &self.attributes
    }

    fn pre_order(&self, in_progress: &mut Causet, left: &mut CausetDefinition, right: &mut CausetDefinition) {
        (self.pre_order)(in_progress, left, right, self);
    }

    fn post_order(&self, in_progress: &mut Causet, left: &mut CausetDefinition, right: &mut CausetDefinition) {
        (self.post_order)(in_progress, left, right, self);
    }

}
use fdb_traits::{
    CausetCompressionType, CausetExt, CausetReader, CausetWriter, CausetWriterBuilder, Iterable, Iterator,
    IterOptions, lightlikeCausetFileInfo, NamespacedName, Result, SeekKey,
};
use std::local_path::local_pathBuf;

use crate::fdb_lsh_treesoliton_panic_merkle_tree;

impl CausetExt for soliton_panic_merkle_tree {
    type CausetReader = PanicCausetReader;
    type CausetWriter = PanicCausetWriter;
    type CausetWriterBuilder = PanicCausetWriterBuilder;
}

pub struct PanicCausetReader;

impl CausetReader for PanicCausetReader {
    fn open(local_path: &str) -> Result<Self> {
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
    fn seek(&mut self, soliton_id: SeekKey<'_>) -> Result<bool> {
        panic!()
    }
    fn seek_for_prev(&mut self, soliton_id: SeekKey<'_>) -> Result<bool> {
        panic!()
    }

    fn prev(&mut self) -> Result<bool> {
        panic!()
    }
    fn next(&mut self) -> Result<bool> {
        panic!()
    }

    fn soliton_id(&self) -> &[u8] {
        panic!()
    }
    fn causet_locale(&self) -> &[u8] {
        panic!()
    }

    fn valid(&self) -> Result<bool> {
        panic!()
    }
}

pub struct PanicCausetWriter;

impl CausetWriter for PanicCausetWriter {
    type lightlikeCausetFileInfo = PaniclightlikeCausetFileInfo;
    type lightlikeCausetFileReader = PaniclightlikeCausetFileReader;

    fn put(&mut self, soliton_id: &[u8], val: &[u8]) -> Result<()> {
        panic!()
    }
    fn delete(&mut self, soliton_id: &[u8]) -> Result<()> {
        panic!()
    }
    fn file_size(&mut self) -> u64 {
        panic!()
    }
    fn finish(self) -> Result<Self::lightlikeCausetFileInfo> {
        panic!()
    }
    fn finish_read(self) -> Result<(Self::lightlikeCausetFileInfo, Self::lightlikeCausetFileReader)> {
        panic!()
    }
}

pub struct PanicCausetWriterBuilder;

impl CausetWriterBuilder<soliton_panic_merkle_tree> for PanicCausetWriterBuilder {
    fn new() -> Self {
        panic!()
    }
    fn set_db(self, einsteindb: &soliton_panic_merkle_tree) -> Self {
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

    fn build(self, local_path: &str) -> Result<PanicCausetWriter> {
        panic!()
    }
}

pub struct PaniclightlikeCausetFileInfo;

impl lightlikeCausetFileInfo for PaniclightlikeCausetFileInfo {
    fn new() -> Self {
        panic!()
    }
    fn file_local_path(&self) -> local_pathBuf {
        panic!()
    }
    fn smallest_soliton_id(&self) -> &[u8] {
        panic!()
    }
    fn largest_soliton_id(&self) -> &[u8] {
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

pub struct PaniclightlikeCausetFileReader;

impl std::io::Read for PaniclightlikeCausetFileReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        panic!()
    }
}
