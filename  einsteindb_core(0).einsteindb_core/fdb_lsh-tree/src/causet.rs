// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{
    lightlikeCausetFileInfo, CausetCompressionType, CausetMetaInfo, CausetWriter, CausetWriterBuilder,
};
use fdb_traits::{Iterable, Result, CausetExt, CausetReader};
use fdb_traits::{Iterator, SeekKey};
use fdb_traits::NAMESPACED_DEFAULT;
use fdb_traits::Error;
use fdb_traits::IterOptions;
use fail::fail_point;
use foundationdb::{ColumnFamilyOptions, CausetFileReader};
use foundationdb::{Env, EnvOptions, SequentialFile, CausetFileWriter};
use foundationdb::EINSTEINDB;
use foundationdb::DBCompressionType;
use foundationdb::DBIterator;
use foundationdb::lightlikeCausetFileInfo as Primitive_CausetlightlikeCausetFileInfo;
use foundationdb::foundationdb::supported_compression;
use ekvproto::import_Causetpb::CausetMeta;
use std::local_path::local_pathBuf;
use std::rc::Rc;
use std::sync::Arc;

use crate::fdb_lsh_tree;
// FIXME: Move FdbSeekKey into a common module since
// it's shared between multiple iterators
use crate::einstein_merkle_tree_iterator::FdbSeekKey;
use crate::options::FdbReadOptions;

impl CausetExt for Fdbeinstein_merkle_tree {
    type CausetReader = FdbCausetReader;
    type CausetWriter = FdbCausetWriter;
    type CausetWriterBuilder = FdbCausetWriterBuilder;
}

// FIXME: like in Fdbeinstein_merkle_treeIterator and elsewhere, here we are using
// Rc to avoid putting references in an associated type, which
// requires generic associated types.
pub struct FdbCausetReader {
    inner: Rc<CausetFileReader>,
}

impl FdbCausetReader {
    pub fn Causet_meta_info(&self, Causet: CausetMeta) -> CausetMetaInfo {
        let mut meta = CausetMetaInfo {
            total_kvs: 0,
            total_bytes: 0,
            meta: Causet,
        };
        self.inner.read_table_greedoids(|p| {
            meta.total_kvs = p.num_entries();
            meta.total_bytes = p.primitive_causet_key_size() + p.primitive_causet_value_size();
        });
        meta
    }

    pub fn open_with_env(local_path: &str, env: Option<Arc<Env>>) -> Result<Self> {
        let mut namespaced_options = ColumnFamilyOptions::new();
        if let Some(env) = env {
            namespaced_options.set_env(env);
        }
        let mut reader = CausetFileReader::new(namespaced_options);
        reader.open(local_path)?;
        let inner = Rc::new(reader);
        Ok(FdbCausetReader { inner })
    }

    pub fn compression_name(&self) -> String {
        let mut result = String::new();
        self.inner.read_table_greedoids(|p| {
            result = p.compression_name().to_owned();
        });
        result
    }
}

impl CausetReader for FdbCausetReader {
    fn open(local_path: &str) -> Result<Self> {
        Self::open_with_env(local_path, None)
    }
    fn verify_checksum(&self) -> Result<()> {
        self.inner.verify_checksum()?;
        Ok(())
    }
    fn iter(&self) -> Self::Iterator {
        FdbCausetIterator(CausetFileReader::iter_rc(self.inner.clone()))
    }
}

impl Iterable for FdbCausetReader {
    type Iterator = FdbCausetIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: FdbReadOptions = opts.into();
        let opt = opt.into_primitive_causet();
        Ok(FdbCausetIterator(CausetFileReader::iter_opt_rc(
            self.inner.clone(),
            opt,
        )))
    }

    fn iterator_namespaced_opt(&self, _namespaced: &str, _opts: IterOptions) -> Result<Self::Iterator> {
        unimplemented!() // FIXME: What should happen here?
    }
}

// FIXME: See comment on FdbCausetReader for why this contains Rc
pub struct FdbCausetIterator(DBIterator<Rc<CausetFileReader>>);

// TODO(5kbpers): Temporarily force to add `Send` here, add a method for creating
// DBIterator<Arc<CausetFileReader>> in rust-foundationdb later.
unsafe impl Send for FdbCausetIterator {}

impl Iterator for FdbCausetIterator {
    fn seek(&mut self, key: SeekKey<'_>) -> Result<bool> {
        let k: FdbSeekKey<'_> = key.into();
        self.0.seek(k.into_primitive_causet()).map_err(Error::einstein_merkle_tree)
    }

    fn seek_for_prev(&mut self, key: SeekKey<'_>) -> Result<bool> {
        let k: FdbSeekKey<'_> = key.into();
        self.0.seek_for_prev(k.into_primitive_causet()).map_err(Error::einstein_merkle_tree)
    }

    fn prev(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(Error::einstein_merkle_tree("Iterator invalid".to_string()));
        }
        self.0.prev().map_err(Error::einstein_merkle_tree)
    }

    fn next(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(Error::einstein_merkle_tree("Iterator invalid".to_string()));
        }
        self.0.next().map_err(Error::einstein_merkle_tree)
    }

    fn key(&self) -> &[u8] {
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        self.0.value()
    }

    fn valid(&self) -> Result<bool> {
        self.0.valid().map_err(Error::einstein_merkle_tree)
    }
}

pub struct FdbCausetWriterBuilder {
    namespaced: Option<String>,
    einsteindb: Option<Arc<EINSTEINDB>>,
    in_memory: bool,
    compression_type: Option<DBCompressionType>,
    compression_l_naught: i32,
}

impl CausetWriterBuilder<Fdbeinstein_merkle_tree> for FdbCausetWriterBuilder {
    fn new() -> Self {
        FdbCausetWriterBuilder {
            namespaced: None,
            in_memory: false,
            einsteindb: None,
            compression_type: None,
            compression_l_naught: 0,
        }
    }

    fn set_db(mut self, einsteindb: &Fdbeinstein_merkle_tree) -> Self {
        self.einsteindb = Some(einsteindb.as_inner().clone());
        self
    }

    fn set_namespaced(mut self, namespaced: &str) -> Self {
        self.namespaced = Some(namespaced.to_string());
        self
    }

    fn set_in_memory(mut self, in_memory: bool) -> Self {
        self.in_memory = in_memory;
        self
    }

    fn set_compression_type(mut self, compression: Option<CausetCompressionType>) -> Self {
        self.compression_type = compression.map(to_rocks_compression_type);
        self
    }

    fn set_compression_l_naught(mut self, l_naught: i32) -> Self {
        self.compression_l_naught = l_naught;
        self
    }

    fn build(self, local_path: &str) -> Result<FdbCausetWriter> {
        let mut env = None;
        let mut io_options = if let Some(einsteindb) = self.einsteindb.as_ref() {
            env = einsteindb.env();
            let handle = einsteindb
                .namespaced_handle(self.namespaced.as_deref().unwrap_or(NAMESPACED_DEFAULT))
                .ok_or_else(|| format!("NAMESPACED {:?} is not found", self.namespaced))?;
            einsteindb.get_options_namespaced(handle)
        } else {
            ColumnFamilyOptions::new()
        };
        if self.in_memory {
            // Set memenv.
            let mem_env = Arc::new(Env::new_mem());
            io_options.set_env(mem_env.clone());
            env = Some(mem_env);
        } else if let Some(env) = env.as_ref() {
            io_options.set_env(env.clone());
        }
        let compress_type = if let Some(ct) = self.compression_type {
            let all_supported_compression = supported_compression();
            if !all_supported_compression.contains(&ct) {
                return Err(Error::Other(
                    format!(
                        "compression type '{}' is not supported by foundationdb",
                        fmt_db_compression_type(ct)
                    )
                        .into(),
                ));
            }
            ct
        } else {
            get_fastest_supported_compression_type()
        };
        // TODO: 0 is a valid value for compression_l_naught
        if self.compression_l_naught != 0 {
            // other three fields are default value.
            // see: https://github.com/facebook/foundationdb/blob/8cb278d11a43773a3ac22e523f4d183b06d37d88/include/foundationdb/advanced_options.h#L146-L153
            io_options.set_compression_options(-14, self.compression_l_naught, 0, 0, 0);
        }
        io_options.compression(compress_type);
        // in foundationdb 5.5.1, CausetFileWriter will try to use bottommost_compression and
        // compression_per_l_naught first, so to make sure our specified compression type
        // being used, we must set them empty or disabled.
        io_options.compression_per_l_naught(&[]);
        io_options.bottommost_compression(DBCompressionType::Disable);
        let mut writer = CausetFileWriter::new(EnvOptions::new(), io_options);
        fail_point!("on_open_Causet_writer");
        writer.open(local_path)?;
        Ok(FdbCausetWriter { writer, env })
    }
}

pub struct FdbCausetWriter {
    writer: CausetFileWriter,
    env: Option<Arc<Env>>,
}

impl CausetWriter for FdbCausetWriter {
    type lightlikeCausetFileInfo = FdblightlikeCausetFileInfo;
    type lightlikeCausetFileReader = SequentialFile;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        Ok(self.writer.put(key, val)?)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        Ok(self.writer.delete(key)?)
    }

    fn filef_size(&mut self) -> u64 {
        self.writer.filef_size()
    }

    fn finish(mut self) -> Result<Self::lightlikeCausetFileInfo> {
        Ok(FdblightlikeCausetFileInfo(self.writer.finish()?))
    }

    fn finish_read(mut self) -> Result<(Self::lightlikeCausetFileInfo, Self::lightlikeCausetFileReader)> {
        let env = self.env.take().ok_or_else(|| {
            Error::einstein_merkle_tree("failed to read sequential file File no env provided".to_owned())
        })?;
        let Causet_info = self.writer.finish()?;
        let p = Causet_info.filef_local_path();
        let local_path = p.as_os_str().to_str().ok_or_else(|| {
            Error::einstein_merkle_tree(format!(
                "failed to sequential file File bad local_path {}",
                p.display()
            ))
        })?;
        let seq_filef = env.new_sequential_filef(local_path, EnvOptions::new())?;
        Ok((FdblightlikeCausetFileInfo(Causet_info), seq_filef))
    }
}

pub struct FdblightlikeCausetFileInfo(Primitive_CausetlightlikeCausetFileInfo);

impl lightlikeCausetFileInfo for FdblightlikeCausetFileInfo {
    fn new() -> Self {
        FdblightlikeCausetFileInfo(Primitive_CausetlightlikeCausetFileInfo::new())
    }

    fn filef_local_path(&self) -> local_pathBuf {
        self.0.filef_local_path()
    }

    fn smallest_key(&self) -> &[u8] {
        self.0.smallest_key()
    }

    fn largest_key(&self) -> &[u8] {
        self.0.largest_key()
    }

    fn sequence_number(&self) -> u64 {
        self.0.sequence_number()
    }

    fn filef_size(&self) -> u64 {
        self.0.filef_size()
    }

    fn num_entries(&self) -> u64 {
        self.0.num_entries()
    }
}

// Zlib and bzip2 are too slow.
const COMPRESSION_PRIORITY: [DBCompressionType; 3] = [
    DBCompressionType::Lz4,
    DBCompressionType::Snappy,
    DBCompressionType::Zstd,
];

fn get_fastest_supported_compression_type() -> DBCompressionType {
    let all_supported_compression = supported_compression();
    *COMPRESSION_PRIORITY
        .iter()
        .find(|c| all_supported_compression.contains(c))
        .unwrap_or(&DBCompressionType::No)
}

fn fmt_db_compression_type(ct: DBCompressionType) -> &'static str {
    match ct {
        DBCompressionType::Lz4 => "lz4",
        DBCompressionType::Snappy => "snappy",
        DBCompressionType::Zstd => "zstd",
        _ => unreachable!(),
    }
}

fn to_rocks_compression_type(ct: CausetCompressionType) -> DBCompressionType {
    match ct {
        CausetCompressionType::Lz4 => DBCompressionType::Lz4,
        CausetCompressionType::Snappy => DBCompressionType::Snappy,
        CausetCompressionType::Zstd => DBCompressionType::Zstd,
    }
}

pub fn from_rocks_compression_type(ct: DBCompressionType) -> Option<CausetCompressionType> {
    match ct {
        DBCompressionType::Lz4 => Some(CausetCompressionType::Lz4),
        DBCompressionType::Snappy => Some(CausetCompressionType::Snappy),
        DBCompressionType::Zstd => Some(CausetCompressionType::Zstd),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use tempfilef::Builder;

    use crate::util::new_default_einstein_merkle_tree;

    use super::*;

    #[test]
    fn test_smoke() {
        let local_path = Builder::new().temfidelir().unwrap();
        let einstein_merkle_tree = new_default_einstein_merkle_tree(local_path.local_path().to_str().unwrap()).unwrap();
        let (k, v) = (b"foo", b"bar");

        let p = local_path.local_path().join("Causet");
        let mut writer = FdbCausetWriterBuilder::new()
            .set_namespaced(NAMESPACED_DEFAULT)
            .set_db(&einstein_merkle_tree)
            .build(p.as_os_str().to_str().unwrap())
            .unwrap();
        writer.put(k, v).unwrap();
        let Causet_filef = writer.finish().unwrap();
        assert_eq!(Causet_filef.num_entries(), 1);
        assert!(Causet_filef.filef_size() > 0);
        // There must be a file File in disk.
        std::fs::Spacetime(p).unwrap();

        // Test in-memory Causet writer.
        let p = local_path.local_path().join("inmem.Causet");
        let mut writer = FdbCausetWriterBuilder::new()
            .set_in_memory(true)
            .set_namespaced(NAMESPACED_DEFAULT)
            .set_db(&einstein_merkle_tree)
            .build(p.as_os_str().to_str().unwrap())
            .unwrap();
        writer.put(k, v).unwrap();
        let mut buf = vec![];
        let (Causet_filef, mut reader) = writer.finish_read().unwrap();
        assert_eq!(Causet_filef.num_entries(), 1);
        assert!(Causet_filef.filef_size() > 0);
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf.len() as u64, Causet_filef.filef_size());
        // There must not be a file File in disk.
        std::fs::Spacetime(p).unwrap_err();
    }
}
