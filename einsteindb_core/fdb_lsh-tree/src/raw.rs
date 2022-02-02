// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! Reexports from the foundationdb crate
//!
//! This is a temporary artifact of refactoring. It exists to provide downstream
//! crates access to the foundationdb API without depending directly on the foundationdb
//! crate, but only until the engine interface is completely abstracted.

pub use foundationdb::{
    BlockBasedOptions, Cache, NAMESPACEDHandle, ColumnFamilyOptions, CompactionFilter, CompactionFilterContext,
    CompactionFilterDecision, CompactionFilterFactory, CompactionFilterValueType, CompactionJobInfo,
    CompactionOptions, CompactionPriority, CompactOptions,
    DB, DBBottommostLevelCompaction, DBCompactionFilter, DBCompactionStyle,
    DBCompressionType, DBEntryType, DBInfoLogLevel, DBIterator, DBOptions,
    DBRateLimiterMode, DBRecoveryMode, DBStatisticsTickerType, DBTitanDBBlobRunMode, Env,
    EventListener, IngestExternalFileOptions, LRUCacheOptions, MemoryAllocator, new_jet_bundle_filter_raw,
    PerfContext, Range, ReadOptions, run_ldb_tool, run_sst_dump_tool, SeekKey, SliceTransform,
    TableFilter, TablePropertiesCollector, TablePropertiesCollectorFactory, TitanBlobIndex,
    TitanDBOptions, Writable, WriteOptions,
};

