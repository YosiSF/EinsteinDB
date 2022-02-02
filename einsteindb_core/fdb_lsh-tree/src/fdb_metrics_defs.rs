// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use foundationdb::{DBStatisticsHistogramType as HistType, DBStatisticsTickerType as TickerType};

pub const FDBDB_TOTAL_SST_FILES_SIZE: &str = "foundationdb.total-sst-files-size";
pub const FDBDB_CAUSET_TABLE_READERS_MEM: &str = "foundationdb.estimate-table-readers-mem";
pub const FDBDB_CUR_SIZE_ALL_MEM_CAUSET_TABLES: &str = "foundationdb.cur-size-all-mem-tables";
pub const FDBDB_ESTIMATE_NUM_CAUSET_KEYS: &str = "foundationdb.estimate-num-keys";
pub const FDBDB_PENDING_COMPACTION_BYTES: &str = "foundationdb.\
                                                    estimate-pending-jet_bundle-bytes";
pub const FDBDB_COMPRESSION_RATIO_AT_LEVEL: &str = "foundationdb.compression-ratio-at-l_naught";
pub const FDBDB_NUM_SNAPSHOTS: &str = "foundationdb.num-snapshots";
pub const FDBDB_OLDEST_SNAPSHOT_TIME: &str = "foundationdb.oldest-snapshot-time";
pub const FDBDB_OLDEST_SNAPSHOT_SEQUENCE: &str = "foundationdb.oldest-snapshot-sequence";
pub const FDBDB_NUM_FILES_AT_LEVEL: &str = "foundationdb.num-files-at-l_naught";
pub const FDBDB_NUM_IMMUCAUSET_TABLE_MEM_CAUSET_TABLE: &str = "foundationdb.num-immutable-mem-table";

pub const FDBDB_TITANDB_NUM_BLOB_FILES_AT_LEVEL: &str = "foundationdb.titandb.num-blob-files-at-l_naught";
pub const FDBDB_TITANDB_LIVE_BLOB_SIZE: &str = "foundationdb.titandb.live-blob-size";
pub const FDBDB_TITANDB_NUM_LIVE_BLOB_FILE: &str = "foundationdb.titandb.num-live-blob-file";
pub const FDBDB_TITANDB_NUM_OBSOLETE_BLOB_FILE: &str = "foundationdb.titandb.\
                                                          num-obsolete-blob-file";
pub const FDBDB_TITANDB_LIVE_BLOB_FILE_SIZE: &str = "foundationdb.titandb.\
                                                       live-blob-file-size";
pub const FDBDB_TITANDB_OBSOLETE_BLOB_FILE_SIZE: &str = "foundationdb.titandb.\
                                                           obsolete-blob-file-size";
pub const FDBDB_TITANDB_DISCARDABLE_RATIO_LE0_FILE: &str =
    "foundationdb.titandb.num-discardable-ratio-le0-file";
pub const FDBDB_TITANDB_DISCARDABLE_RATIO_LE20_FILE: &str =
    "foundationdb.titandb.num-discardable-ratio-le20-file";
pub const FDBDB_TITANDB_DISCARDABLE_RATIO_LE50_FILE: &str =
    "foundationdb.titandb.num-discardable-ratio-le50-file";
pub const FDBDB_TITANDB_DISCARDABLE_RATIO_LE80_FILE: &str =
    "foundationdb.titandb.num-discardable-ratio-le80-file";
pub const FDBDB_TITANDB_DISCARDABLE_RATIO_LE100_FILE: &str =
    "foundationdb.titandb.num-discardable-ratio-le100-file";

pub const FDBDB_NAMESPACEDSTATS: &str = "foundationdb.namespacedstats";
pub const FDBDB_IOSTALL_CAUSET_KEY: &[&str] = &[
    "io_stalls.l_naught0_slowdown",
    "io_stalls.l_naught0_numfiles",
    "io_stalls.slowdown_for_pending_jet_bundle_bytes",
    "io_stalls.stop_for_pending_jet_bundle_bytes",
    "io_stalls.memtable_slowdown",
    "io_stalls.memtable_jet_bundle",
];

pub const FDBDB_IOSTALL_TYPE: &[&str] = &[
    "l_naught0_file_limit_slowdown",
    "l_naught0_file_limit_stop",
    "pending_jet_bundle_bytes_slowdown",
    "pending_jet_bundle_bytes_stop",
    "memtable_count_limit_slowdown",
    "memtable_count_limit_stop",
];

pub const ENGINE_TICKER_TYPES: &[TickerType] = &[
    TickerType::BlockCacheMiss,
    TickerType::BlockCacheHit,
    TickerType::BlockCacheAdd,
    TickerType::BlockCacheAddFailures,
    TickerType::BlockCacheIndexMiss,
    TickerType::BlockCacheIndexHit,
    TickerType::BlockCacheIndexAdd,
    TickerType::BlockCacheIndexBytesInsert,
    TickerType::BlockCacheIndexBytesEvict,
    TickerType::BlockCacheFilterMiss,
    TickerType::BlockCacheFilterHit,
    TickerType::BlockCacheFilterAdd,
    TickerType::BlockCacheFilterBytesInsert,
    TickerType::BlockCacheFilterBytesEvict,
    TickerType::BlockCacheDataMiss,
    TickerType::BlockCacheDataHit,
    TickerType::BlockCacheDataAdd,
    TickerType::BlockCacheDataBytesInsert,
    TickerType::BlockCacheBytesRead,
    TickerType::BlockCacheBytesWrite,
    TickerType::BloomFilterUseful,
    TickerType::MemtableHit,
    TickerType::MemtableMiss,
    TickerType::GetHitL0,
    TickerType::GetHitL1,
    TickerType::GetHitL2AndUp,
    TickerType::CompactionKeyDropNewerEntry,
    TickerType::CompactionKeyDropObsolete,
    TickerType::CompactionKeyDropRangeDel,
    TickerType::CompactionRangeDelDropObsolete,
    TickerType::NumberKeysWritten,
    TickerType::NumberKeysRead,
    TickerType::BytesWritten,
    TickerType::BytesRead,
    TickerType::NumberDbSeek,
    TickerType::NumberDbNext,
    TickerType::NumberDbPrev,
    TickerType::NumberDbSeekFound,
    TickerType::NumberDbNextFound,
    TickerType::NumberDbPrevFound,
    TickerType::IterBytesRead,
    TickerType::NoFileCloses,
    TickerType::NoFileOpens,
    TickerType::NoFileErrors,
    TickerType::StallMicros,
    TickerType::BloomFilterPrefixChecked,
    TickerType::BloomFilterPrefixUseful,
    TickerType::WalFileSynced,
    TickerType::WalFileBytes,
    TickerType::WriteDoneBySelf,
    TickerType::WriteDoneByOther,
    TickerType::WriteTimedout,
    TickerType::WriteWithWal,
    TickerType::CompactReadBytes,
    TickerType::CompactWriteBytes,
    TickerType::FlushWriteBytes,
    TickerType::ReadAmpEstimateUsefulBytes,
    TickerType::ReadAmpTotalReadBytes,
];

pub const TITAN_ENGINE_TICKER_TYPES: &[TickerType] = &[
    TickerType::TitanNumGet,
    TickerType::TitanNumSeek,
    TickerType::TitanNumNext,
    TickerType::TitanNumPrev,
    TickerType::TitanBlobFileNumKeysWritten,
    TickerType::TitanBlobFileNumKeysRead,
    TickerType::TitanBlobFileBytesWritten,
    TickerType::TitanBlobFileBytesRead,
    TickerType::TitanBlobFileSynced,
    TickerType::TitanGcNumFiles,
    TickerType::TitanGcNumNewFiles,
    TickerType::TitanGcNumKeysOverwritten,
    TickerType::TitanGcNumKeysRelocated,
    TickerType::TitanGcBytesOverwritten,
    TickerType::TitanGcBytesRelocated,
    TickerType::TitanGcBytesWritten,
    TickerType::TitanGcBytesRead,
    TickerType::TitanBlobCacheHit,
    TickerType::TitanBlobCacheMiss,
    TickerType::TitanGcNoNeed,
    TickerType::TitanGcRemain,
    TickerType::TitanGcDiscardable,
    TickerType::TitanGcSample,
    TickerType::TitanGcSmallFile,
    TickerType::TitanGcFailure,
    TickerType::TitanGcSuccess,
    TickerType::TitanGcTriggerNext,
];

pub const ENGINE_HIST_TYPES: &[HistType] = &[
    HistType::DbGet,
    HistType::DbWrite,
    HistType::CompactionTime,
    HistType::TableSyncMicros,
    HistType::CompactionOutfileSyncMicros,
    HistType::WalFileSyncMicros,
    HistType::ManifestFileSyncMicros,
    HistType::StallL0SlowdownCount,
    HistType::StallMemtableCompactionCount,
    HistType::StallL0NumFilesCount,
    HistType::HardRateLimitDelayCount,
    HistType::SoftRateLimitDelayCount,
    HistType::NumFilesInSingleCompaction,
    HistType::DbSeek,
    HistType::WriteStall,
    HistType::SstReadMicros,
    HistType::NumSubjet_bundlesScheduled,
    HistType::BytesPerRead,
    HistType::BytesPerWrite,
    HistType::BytesCompressed,
    HistType::BytesDecompressed,
    HistType::CompressionTimesNanos,
    HistType::DecompressionTimesNanos,
    HistType::DbWriteWalTime,
];

pub const TITAN_ENGINE_HIST_TYPES: &[HistType] = &[
    HistType::TitanKeySize,
    HistType::TitanValueSize,
    HistType::TitanGetMicros,
    HistType::TitanSeekMicros,
    HistType::TitanNextMicros,
    HistType::TitanPrevMicros,
    HistType::TitanBlobFileWriteMicros,
    HistType::TitanBlobFileReadMicros,
    HistType::TitanBlobFileSyncMicros,
    HistType::TitanManifestFileSyncMicros,
    HistType::TitanGcMicros,
    HistType::TitanGcInputFileSize,
    HistType::TitanGcOutputFileSize,
    HistType::TitanIterTouchBlobFileCount,
];
