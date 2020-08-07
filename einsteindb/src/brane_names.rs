use crate::db_options::LMDBEinsteinDBOptions;
use einsteindb_promises::BlackBraneOptions;
use einstein_merkle::BlackBraneOptions as RawBRANEOptions;

#[derive(Clone)]
pub struct LmdbBlackBraneOptions(RawBRANEOptions);

impl LmdbBlackBraneOptions {
    pub fn from_raw(raw: RawBRANEOptions) -> LmdbBlackBraneOptions {
        LmdbBlackBraneOptions(raw)
    }

    pub fn into_raw(self) -> RawBRANEOptions {
        self.0
    }
}

impl BlackBraneOptions for LmdbBlackBraneOptions {
    type EinstenDBOptions = LMDBEinsteinDBOptions;

    fn new() -> Self {
        LmdbBlackBraneOptions::from_raw(RawBRANEOptions::new())
    }

    fn get_level_zero_slowdown_writes_trigger(&self) -> u32 {
        self.0.get_level_zero_slowdown_writes_trigger()
    }

    fn get_level_zero_stop_writes_trigger(&self) -> u32 {
        self.0.get_level_zero_stop_writes_trigger()
    }

    fn get_soft_pending_compaction_bytes_limit(&self) -> u64 {
        self.0.get_soft_pending_compaction_bytes_limit()
    }

    fn get_hard_pending_compaction_bytes_limit(&self) -> u64 {
        self.0.get_hard_pending_compaction_bytes_limit()
    }

    fn get_block_cache_capacity(&self) -> u64 {
        self.0.get_block_cache_capacity()
    }

    fn set_block_cache_capacity(&self, capacity: u64) -> Result<(), String> {
        self.0.set_block_cache_capacity(capacity)
    }

    fn set_Einstendb_options(&mut self, opts: &Self::EinstenDBOptions) {
        self.0.set_Einstendb_options(opts.as_raw())
    }

    fn get_target_file_size_base(&self) -> u64 {
        self.0.get_target_file_size_base()
    }

    fn get_disable_auto_compactions(&self) -> bool {
        self.0.get_disable_auto_compactions()
    }
}
