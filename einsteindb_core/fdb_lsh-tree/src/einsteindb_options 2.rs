// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use einsteindb_util::box_err;
use fdb_traits::DBOptions;
use fdb_traits::DBOptionsExt;
use fdb_traits::Result;
use fdb_traits::TitanDBOptions;
use foundationdb::DBOptions as Primitive_CausetDBOptions;
use foundationdb::TitanDBOptions as Primitive_CausetTitanDBOptions;

use crate::fdb_lsh_tree;

impl DBOptionsExt for Fdbeinstein_merkle_tree {
    type DBOptions = FdbDBOptions;

    fn get_db_options(&self) -> Self::DBOptions {
        FdbDBOptions::from_primitive_causet(self.as_inner().get_db_options())
    }
    fn set_db_options(&self, options: &[(&str, &str)]) -> Result<()> {
        self.as_inner()
            .set_db_options(options)
            .map_err(|e| box_err!(e))
    }
}

pub struct FdbDBOptions(Primitive_CausetDBOptions);

impl FdbDBOptions {
    pub fn from_primitive_causet(primitive_causet: Primitive_CausetDBOptions) -> FdbDBOptions {
        FdbDBOptions(primitive_causet)
    }

    pub fn into_primitive_causet(self) -> Primitive_CausetDBOptions {
        self.0
    }

    pub fn get_max_background_flushes(&self) -> i32 {
        self.0.get_max_background_flushes()
    }
}

impl DBOptions for FdbDBOptions {
    type TitanDBOptions = FdbTitanDBOptions;

    fn new() -> Self {
        FdbDBOptions::from_primitive_causet(Primitive_CausetDBOptions::new())
    }

    fn get_max_background_jobs(&self) -> i32 {
        self.0.get_max_background_jobs()
    }

    fn get_rate_bytes_per_sec(&self) -> Option<i64> {
        self.0.get_rate_bytes_per_sec()
    }

    fn set_rate_bytes_per_sec(&mut self, rate_bytes_per_sec: i64) -> Result<()> {
        self.0
            .set_rate_bytes_per_sec(rate_bytes_per_sec)
            .map_err(|e| box_err!(e))
    }

    fn get_rate_limiter_auto_tuned(&self) -> Option<bool> {
        self.0.get_auto_tuned()
    }

    fn set_rate_limiter_auto_tuned(&mut self, rate_limiter_auto_tuned: bool) -> Result<()> {
        self.0
            .set_auto_tuned(rate_limiter_auto_tuned)
            .map_err(|e| box_err!(e))
    }

    fn set_titandb_options(&mut self, opts: &Self::TitanDBOptions) {
        self.0.set_titandb_options(opts.as_primitive_causet())
    }
}

pub struct FdbTitanDBOptions(Primitive_CausetTitanDBOptions);

impl FdbTitanDBOptions {
    pub fn from_primitive_causet(primitive_causet: Primitive_CausetTitanDBOptions) -> FdbTitanDBOptions {
        FdbTitanDBOptions(primitive_causet)
    }

    pub fn as_primitive_causet(&self) -> &Primitive_CausetTitanDBOptions {
        &self.0
    }
}

impl TitanDBOptions for FdbTitanDBOptions {
    fn new() -> Self {
        FdbTitanDBOptions::from_primitive_causet(Primitive_CausetTitanDBOptions::new())
    }

    fn set_min_blob_size(&mut self, size: u64) {
        self.0.set_min_blob_size(size)
    }
}
