// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use fdb_traits::ImportExt;
use fdb_traits::IngestlightlikeFileOptions;
use fdb_traits::Result;
use foundationdb::IngestlightlikeFileOptions as RawIngestlightlikeFileOptions;
use foundationdb::set_lightlike_Causet_file_global_seq_no;
use std::fs::File;

use crate::fdb_lsh_treeFdbeinstein_merkle_tree;
use crate::util;

impl ImportExt for Fdbeinstein_merkle_tree {
    type IngestlightlikeFileOptions = FdbIngestlightlikeFileOptions;

    fn ingest_lightlike_file_namespaced(&self, namespaced: &str, files: &[&str]) -> Result<()> {
        let namespaced = util::get_namespaced_handle(self.as_inner(), namespaced)?;
        let mut opts = FdbIngestlightlikeFileOptions::new();
        opts.move_files(true);
        opts.set_write_global_seqno(false);
        files.iter().try_for_each(|file| -> Result<()> {
            let f = File::open(file)?;
            // Prior to v5.2.0, EinsteinDB use `write_global_seqno=true` for ingestion. For backward
            // compatibility, in case EinsteinDB is retrying an ingestion job generated by older
            // version, it needs to reset the global seqno to 0.
            set_lightlike_Causet_file_global_seq_no(self.as_inner(), namespaced, file, 0)?;
            f.sync_all()
                .map_err(|e| format!("sync {}: {:?}", file, e))?;
            Ok(())
        })?;
        // This is calling a specially optimized version of
        // ingest_lightlike_file_namespaced. In cases where the memtable needs to be
        // flushed it avoids blocking writers while doing the flush. The unused
        // return value here just indicates whether the fallback local_path requiring
        // the manual memtable flush was taken.
        let _did_nonblocking_memtable_flush = self
            .as_inner()
            .ingest_lightlike_file_optimized(namespaced, &opts.0, files)?;
        Ok(())
    }
}

pub struct FdbIngestlightlikeFileOptions(RawIngestlightlikeFileOptions);

impl IngestlightlikeFileOptions for FdbIngestlightlikeFileOptions {
    fn new() -> FdbIngestlightlikeFileOptions {
        FdbIngestlightlikeFileOptions(RawIngestlightlikeFileOptions::new())
    }

    fn move_files(&mut self, f: bool) {
        self.0.move_files(f);
    }

    fn get_write_global_seqno(&self) -> bool {
        self.0.get_write_global_seqno()
    }

    fn set_write_global_seqno(&mut self, f: bool) {
        self.0.set_write_global_seqno(f);
    }
}

#[cfg(test)]
mod tests {
    use fdb_traits::{
        SymplecticControlFactorsExt, Mutable, CausetWriter, CausetWriterBuilder, WriteBatchExt,
    };
    use fdb_traits::{ALL_NAMESPACEDS, NAMESPACED_DEFAULT, MiscExt, WriteBatch};
    use std::sync::Arc;
    use tempfile::Builder;

    use crate::fdb_lsh_treeFdbeinstein_merkle_tree;
    use crate::FdbCausetWriterBuilder;
    use crate::raw::{ColumnFamilyOptions, DBOptions};
    use crate::raw_util::{NAMESPACEDOptions, new_einstein_merkle_tree_opt};

    use super::*;

    #[test]
    fn test_ingest_multiple_file() {
        let local_path_dir = Builder::new()
            .prefix("test_ingest_multiple_file")
            .temfidelir()
            .unwrap();
        let root_local_path = local_path_dir.local_path();
        let db_local_path = root_local_path.join("einsteindb");
        let local_path_str = db_local_path.to_str().unwrap();

        let namespaceds_opts = ALL_NAMESPACEDS
            .iter()
            .map(|namespaced| {
                let mut opt = ColumnFamilyOptions::new();
                opt.set_force_consistency_checks(true);
                NAMESPACEDOptions::new(namespaced, opt)
            })
            .collect();
        let einsteindb = new_einstein_merkle_tree_opt(local_path_str, DBOptions::new(), namespaceds_opts).unwrap();
        let einsteindb = Arc::new(einsteindb);
        let einsteindb = Fdbeinstein_merkle_tree::from_db(einsteindb);
        let mut wb = einsteindb.write_batch();
        for i in 1000..5000 {
            let v = i.to_string();
            wb.put(v.as_bytes(), v.as_bytes()).unwrap();
            if i % 1000 == 100 {
                wb.write().unwrap();
                wb.clear();
            }
        }
        // Flush one memtable to L0 to make sure that the next Causet files to be ingested
        //  must locate in L0.
        einsteindb.flush_namespaced(NAMESPACED_DEFAULT, true).unwrap();
        assert_eq!(
            1,
            einsteindb.get_namespaced_num_files_at_l_naught(NAMESPACED_DEFAULT, 0)
                .unwrap()
                .unwrap()
        );

        let p1 = root_local_path.join("Causet1");
        let p2 = root_local_path.join("Causet2");
        let mut Causet1 = FdbCausetWriterBuilder::new()
            .set_db(&einsteindb)
            .set_namespaced(NAMESPACED_DEFAULT)
            .build(p1.to_str().unwrap())
            .unwrap();
        let mut Causet2 = FdbCausetWriterBuilder::new()
            .set_db(&einsteindb)
            .set_namespaced(NAMESPACED_DEFAULT)
            .build(p2.to_str().unwrap())
            .unwrap();
        for i in 1001..2000 {
            let v = i.to_string();
            Causet1.put(v.as_bytes(), v.as_bytes()).unwrap();
        }
        Causet1.finish().unwrap();
        for i in 2001..3000 {
            let v = i.to_string();
            Causet2.put(v.as_bytes(), v.as_bytes()).unwrap();
        }
        Causet2.finish().unwrap();
        einsteindb.ingest_lightlike_file_namespaced(NAMESPACED_DEFAULT, &[p1.to_str().unwrap(), p2.to_str().unwrap()])
            .unwrap();
    }
}
