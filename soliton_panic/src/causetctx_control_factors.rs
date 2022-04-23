// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use super::*;
use crate::storage::{Engine, SnapshotStore, SnapshotStoreStatistics, Statistics};
use crate::causetq::{CausetQ, CausetQStatistics};
use crate::causet::{Causet, CausetStatistics};
use crate::causetctx::{CausetCtx, CausetCtxStatistics};
use crate::einstein_db::{EINSTEIN_DB_VERSION, EINSTEIN_DB_VERSION_LATEST};
use crate::storage::{SnapshotStore, SnapshotStoreStatistics, Statistics};
use crate::soliton::{Soliton, SolitonStatistics};
use crate as einstein_db_causet;
use crate::allegro_poset::{AllegroPoset, AllegroPosetStatistics};


/// A `CausetCtx` is a `Causet` with a `CausetQ` and a `SnapshotStore`.
/// It is used to control the causetctx.
/// ///! # Examples
/// ```
/// use einstein_db::causetctx_control_factors::*;
/// use einstein_db::causetctx::*;
/// use einstein_db::causet::*;
/// use einstein_db::causetq::*;
///
/// let mut causetctx = CausetCtx::new();
/// let mut causet = Causet::new();
///
/// let mut causetq = CausetQ::new();
/// let mut snapshot_store = SnapshotStore::new();
///
/// causetctx.set_causet(&mut causet);
/// causetctx.set_causetq(&mut causetq);
///
/// causetctx.set_snapshot_store(&mut snapshot_store);
///
/// causetctx.set_allegro_poset(&mut allegro_poset);
/// ```
#[cfg_attr(feature = "flame_it", flame)]
pub struct CausetCtxControlFactors {
    pub causet: Causet,
    pub causetq: CausetQ,
    pub snapshot_store: SnapshotStore,
    pub soliton: Soliton,
    pub allegro_poset: AllegroPoset,
}

#[cfg_attr(feature = "flame_it", flame)]
pub struct CausetCtxControlFactorsStatistics {
    pub causet: CausetStatistics,
    pub causetq: CausetQStatistics,
    pub snapshot_store: SnapshotStoreStatistics,
    pub soliton: SolitonStatistics,
    pub allegro_poset: AllegroPosetStatistics,
}






