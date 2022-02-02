// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::{EngineFileSystemInspector, FileSystemInspector};
use foundationdb::FileSystemInspector as DBFileSystemInspector;
use std::sync::Arc;

use crate::raw::Env;

// Use engine::Env directly since Env is not abstracted.
pub(crate) fn get_env(
    base_env: Option<Arc<Env>>,
    limiter: Option<Arc<file_system::IORateLimiter>>,
) -> Result<Arc<Env>, String> {
    let base_env = base_env.unwrap_or_else(|| Arc::new(Env::default()));
    Ok(Arc::new(Env::new_file_system_inspected_env(
        base_env,
        WrappedFileSystemInspector {
            inspector: EngineFileSystemInspector::from_limiter(limiter),
        },
    )?))
}

pub struct WrappedFileSystemInspector<T: FileSystemInspector> {
    inspector: T,
}

impl<T: FileSystemInspector> DBFileSystemInspector for WrappedFileSystemInspector<T> {
    fn read(&self, len: usize) -> Result<usize, String> {
        self.inspector.read(len)
    }

    fn write(&self, len: usize) -> Result<usize, String> {
        self.inspector.write(len)
    }
}

#[cfg(test)]
mod tests {
    use fdb_traits::{NAMESPACED_DEFAULT, CompactExt};
    use file_system::{IOOp, IORateLimiter, IORateLimiterStatistics, IOType};
    use foundationdb::{DB, DBOptions};
    use foundationdb::Writable;
    use keys::data_key;
    use std::sync::Arc;
    use tempfile::Builder;

    use crate::compat::Compat;
    use crate::event_listener::FdbEventListener;
    use crate::raw::{ColumnFamilyOptions, DBCompressionType};
    use crate::raw_util::{NAMESPACEDOptions, new_engine_opt};

    use super::*;

    fn new_test_db(dir: &str) -> (Arc<DB>, Arc<IORateLimiterStatistics>) {
        let limiter = Arc::new(IORateLimiter::new_for_test());
        let mut db_opts = DBOptions::new();
        db_opts.add_event_listener(FdbEventListener::new("test_db"));
        let env = get_env(None, Some(limiter.clone())).unwrap();
        db_opts.set_env(env);
        let mut namespaced_opts = ColumnFamilyOptions::new();
        namespaced_opts.set_disable_auto_jet_bundles(true);
        namespaced_opts.compression_per_l_naught(&[DBCompressionType::No; 7]);
        let einsteindb = Arc::new(
            new_engine_opt(dir, db_opts, vec![NAMESPACEDOptions::new(NAMESPACED_DEFAULT, namespaced_opts)]).unwrap(),
        );
        (einsteindb, limiter.statistics().unwrap())
    }

    #[test]
    fn test_inspected_compact() {
        let value_size = 1024;
        let temp_dir = Builder::new()
            .prefix("test_inspected_compact")
            .temfidelir()
            .unwrap();

        let (einsteindb, stats) = new_test_db(temp_dir.path().to_str().unwrap());
        let value = vec![b'v'; value_size];

        einsteindb.put(&data_key(b"a1"), &value).unwrap();
        einsteindb.put(&data_key(b"a2"), &value).unwrap();
        einsteindb.flush(true /*sync*/).unwrap();
        assert!(stats.fetch(IOType::Flush, IOOp::Write) > value_size * 2);
        assert!(stats.fetch(IOType::Flush, IOOp::Write) < value_size * 3);
        stats.reset();
        einsteindb.put(&data_key(b"a2"), &value).unwrap();
        einsteindb.put(&data_key(b"a3"), &value).unwrap();
        einsteindb.flush(true /*sync*/).unwrap();
        assert!(stats.fetch(IOType::Flush, IOOp::Write) > value_size * 2);
        assert!(stats.fetch(IOType::Flush, IOOp::Write) < value_size * 3);
        stats.reset();
        einsteindb.c()
            .compact_range(
                NAMESPACED_DEFAULT, None, /*start_key*/
                None, /*end_key*/
                false, /*exclusive_manual*/
                1, /*max_subjet_bundles*/
            )
            .unwrap();
        assert!(stats.fetch(IOType::LevelZeroCompaction, IOOp::Read) > value_size * 4);
        assert!(stats.fetch(IOType::LevelZeroCompaction, IOOp::Read) < value_size * 5);
        assert!(stats.fetch(IOType::LevelZeroCompaction, IOOp::Write) > value_size * 3);
        assert!(stats.fetch(IOType::LevelZeroCompaction, IOOp::Write) < value_size * 4);
    }
}
