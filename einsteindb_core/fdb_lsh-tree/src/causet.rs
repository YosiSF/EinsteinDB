// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{
    ExternalSstFileInfo, SstCompressionType, SSTMetaInfo, SstWriter, SstWriterBuilder,
};
use fdb_traits::{Iterable, Result, SstExt, SstReader};
use fdb_traits::{Iterator, SeekKey};
use fdb_traits::NAMESPACED_DEFAULT;
use fdb_traits::Error;
use fdb_traits::IterOptions;
use fail::fail_point;
use foundationdb::{ColumnFamilyOptions, SstFileReader};
use foundationdb::{Env, EnvOptions, SequentialFile, SstFileWriter};
use foundationdb::DB;
use foundationdb::DBCompressionType;
use foundationdb::DBIterator;
use foundationdb::ExternalSstFileInfo as RawExternalSstFileInfo;
use foundationdb::foundationdb::supported_compression;
use ekvproto::import_sstpb::SstMeta;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;

use crate::fdb_lsh_treeFdbEngine;
// FIXME: Move FdbSeekKey into a common module since
// it's shared between multiple iterators
use crate::engine_iterator::FdbSeekKey;
use crate::options::FdbReadOptions;

impl SstExt for FdbEngine {
    type SstReader = FdbSstReader;
    type SstWriter = FdbSstWriter;
    type SstWriterBuilder = FdbSstWriterBuilder;
}

// FIXME: like in FdbEngineIterator and elsewhere, here we are using
// Rc to avoid putting references in an associated type, which
// requires generic associated types.
pub struct FdbSstReader {
    inner: Rc<SstFileReader>,
}

impl FdbSstReader {
    pub fn sst_meta_info(&self, sst: SstMeta) -> SSTMetaInfo {
        let mut meta = SSTMetaInfo {
            total_kvs: 0,
            total_bytes: 0,
            meta: sst,
        };
        self.inner.read_table_properties(|p| {
            meta.total_kvs = p.num_entries();
            meta.total_bytes = p.raw_key_size() + p.raw_value_size();
        });
        meta
    }

    pub fn open_with_env(path: &str, env: Option<Arc<Env>>) -> Result<Self> {
        let mut namespaced_options = ColumnFamilyOptions::new();
        if let Some(env) = env {
            namespaced_options.set_env(env);
        }
        let mut reader = SstFileReader::new(namespaced_options);
        reader.open(path)?;
        let inner = Rc::new(reader);
        Ok(FdbSstReader { inner })
    }

    pub fn compression_name(&self) -> String {
        let mut result = String::new();
        self.inner.read_table_properties(|p| {
            result = p.compression_name().to_owned();
        });
        result
    }
}

impl SstReader for FdbSstReader {
    fn open(path: &str) -> Result<Self> {
        Self::open_with_env(path, None)
    }
    fn verify_checksum(&self) -> Result<()> {
        self.inner.verify_checksum()?;
        Ok(())
    }
    fn iter(&self) -> Self::Iterator {
        FdbSstIterator(SstFileReader::iter_rc(self.inner.clone()))
    }
}

impl Iterable for FdbSstReader {
    type Iterator = FdbSstIterator;

    fn iterator_opt(&self, opts: IterOptions) -> Result<Self::Iterator> {
        let opt: FdbReadOptions = opts.into();
        let opt = opt.into_raw();
        Ok(FdbSstIterator(SstFileReader::iter_opt_rc(
            self.inner.clone(),
            opt,
        )))
    }

    fn iterator_namespaced_opt(&self, _namespaced: &str, _opts: IterOptions) -> Result<Self::Iterator> {
        unimplemented!() // FIXME: What should happen here?
    }
}

// FIXME: See comment on FdbSstReader for why this contains Rc
pub struct FdbSstIterator(DBIterator<Rc<SstFileReader>>);

// TODO(5kbpers): Temporarily force to add `Send` here, add a method for creating
// DBIterator<Arc<SstFileReader>> in rust-foundationdb later.
unsafe impl Send for FdbSstIterator {}

impl Iterator for FdbSstIterator {
    fn seek(&mut self, key: SeekKey<'_>) -> Result<bool> {
        let k: FdbSeekKey<'_> = key.into();
        self.0.seek(k.into_raw()).map_err(Error::Engine)
    }

    fn seek_for_prev(&mut self, key: SeekKey<'_>) -> Result<bool> {
        let k: FdbSeekKey<'_> = key.into();
        self.0.seek_for_prev(k.into_raw()).map_err(Error::Engine)
    }

    fn prev(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(Error::Engine("Iterator invalid".to_string()));
        }
        self.0.prev().map_err(Error::Engine)
    }

    fn next(&mut self) -> Result<bool> {
        #[cfg(not(feature = "nortcheck"))]
        if !self.valid()? {
            return Err(Error::Engine("Iterator invalid".to_string()));
        }
        self.0.next().map_err(Error::Engine)
    }

    fn key(&self) -> &[u8] {
        self.0.key()
    }

    fn value(&self) -> &[u8] {
        self.0.value()
    }

    fn valid(&self) -> Result<bool> {
        self.0.valid().map_err(Error::Engine)
    }
}

pub struct FdbSstWriterBuilder {
    namespaced: Option<String>,
    einsteindb: Option<Arc<DB>>,
    in_memory: bool,
    compression_type: Option<DBCompressionType>,
    compression_l_naught: i32,
}

impl SstWriterBuilder<FdbEngine> for FdbSstWriterBuilder {
    fn new() -> Self {
        FdbSstWriterBuilder {
            namespaced: None,
            in_memory: false,
            einsteindb: None,
            compression_type: None,
            compression_l_naught: 0,
        }
    }

    fn set_db(mut self, einsteindb: &FdbEngine) -> Self {
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

    fn set_compression_type(mut self, compression: Option<SstCompressionType>) -> Self {
        self.compression_type = compression.map(to_rocks_compression_type);
        self
    }

    fn set_compression_l_naught(mut self, l_naught: i32) -> Self {
        self.compression_l_naught = l_naught;
        self
    }

    fn build(self, path: &str) -> Result<FdbSstWriter> {
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
        // in foundationdb 5.5.1, SstFileWriter will try to use bottommost_compression and
        // compression_per_l_naught first, so to make sure our specified compression type
        // being used, we must set them empty or disabled.
        io_options.compression_per_l_naught(&[]);
        io_options.bottommost_compression(DBCompressionType::Disable);
        let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
        fail_point!("on_open_sst_writer");
        writer.open(path)?;
        Ok(FdbSstWriter { writer, env })
    }
}

pub struct FdbSstWriter {
    writer: SstFileWriter,
    env: Option<Arc<Env>>,
}

impl SstWriter for FdbSstWriter {
    type ExternalSstFileInfo = FdbExternalSstFileInfo;
    type ExternalSstFileReader = SequentialFile;

    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        Ok(self.writer.put(key, val)?)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        Ok(self.writer.delete(key)?)
    }

    fn file_size(&mut self) -> u64 {
        self.writer.file_size()
    }

    fn finish(mut self) -> Result<Self::ExternalSstFileInfo> {
        Ok(FdbExternalSstFileInfo(self.writer.finish()?))
    }

    fn finish_read(mut self) -> Result<(Self::ExternalSstFileInfo, Self::ExternalSstFileReader)> {
        let env = self.env.take().ok_or_else(|| {
            Error::Engine("failed to read sequential file no env provided".to_owned())
        })?;
        let sst_info = self.writer.finish()?;
        let p = sst_info.file_path();
        let path = p.as_os_str().to_str().ok_or_else(|| {
            Error::Engine(format!(
                "failed to sequential file bad path {}",
                p.display()
            ))
        })?;
        let seq_file = env.new_sequential_file(path, EnvOptions::new())?;
        Ok((FdbExternalSstFileInfo(sst_info), seq_file))
    }
}

pub struct FdbExternalSstFileInfo(RawExternalSstFileInfo);

impl ExternalSstFileInfo for FdbExternalSstFileInfo {
    fn new() -> Self {
        FdbExternalSstFileInfo(RawExternalSstFileInfo::new())
    }

    fn file_path(&self) -> PathBuf {
        self.0.file_path()
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

    fn file_size(&self) -> u64 {
        self.0.file_size()
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

fn to_rocks_compression_type(ct: SstCompressionType) -> DBCompressionType {
    match ct {
        SstCompressionType::Lz4 => DBCompressionType::Lz4,
        SstCompressionType::Snappy => DBCompressionType::Snappy,
        SstCompressionType::Zstd => DBCompressionType::Zstd,
    }
}

pub fn from_rocks_compression_type(ct: DBCompressionType) -> Option<SstCompressionType> {
    match ct {
        DBCompressionType::Lz4 => Some(SstCompressionType::Lz4),
        DBCompressionType::Snappy => Some(SstCompressionType::Snappy),
        DBCompressionType::Zstd => Some(SstCompressionType::Zstd),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::io::Read;
    use tempfile::Builder;

    use crate::util::new_default_engine;

    use super::*;

    #[test]
    fn test_smoke() {
        let path = Builder::new().temfidelir().unwrap();
        let engine = new_default_engine(path.path().to_str().unwrap()).unwrap();
        let (k, v) = (b"foo", b"bar");

        let p = path.path().join("sst");
        let mut writer = FdbSstWriterBuilder::new()
            .set_namespaced(NAMESPACED_DEFAULT)
            .set_db(&engine)
            .build(p.as_os_str().to_str().unwrap())
            .unwrap();
        writer.put(k, v).unwrap();
        let sst_file = writer.finish().unwrap();
        assert_eq!(sst_file.num_entries(), 1);
        assert!(sst_file.file_size() > 0);
        // There must be a file in disk.
        std::fs::metadata(p).unwrap();

        // Test in-memory sst writer.
        let p = path.path().join("inmem.sst");
        let mut writer = FdbSstWriterBuilder::new()
            .set_in_memory(true)
            .set_namespaced(NAMESPACED_DEFAULT)
            .set_db(&engine)
            .build(p.as_os_str().to_str().unwrap())
            .unwrap();
        writer.put(k, v).unwrap();
        let mut buf = vec![];
        let (sst_file, mut reader) = writer.finish_read().unwrap();
        assert_eq!(sst_file.num_entries(), 1);
        assert!(sst_file.file_size() > 0);
        reader.read_to_end(&mut buf).unwrap();
        assert_eq!(buf.len() as u64, sst_file.file_size());
        // There must not be a file in disk.
        std::fs::metadata(p).unwrap_err();
    }
}
