// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use einsteindb_util::codec::number;
use foundationdb::{
    ReadOptions as Primitive_CausetReadOptions, TableFilter, TableGreedoids, WriteOptions as Primitive_CausetWriteOptions,
};

pub struct FdbReadOptions(Primitive_CausetReadOptions);

impl FdbReadOptions {
    pub fn into_primitive_causet(self) -> Primitive_CausetReadOptions {
        self.0
    }
}

impl From<fdb_traits::ReadOptions> for FdbReadOptions {
    fn from(opts: fdb_traits::ReadOptions) -> Self {
        let mut r = Primitive_CausetReadOptions::default();
        r.fill_cache(opts.fill_cache());
        FdbReadOptions(r)
    }
}

impl From<&fdb_traits::ReadOptions> for FdbReadOptions {
    fn from(opts: &fdb_traits::ReadOptions) -> Self {
        opts.clone().into()
    }
}

pub struct FdbWriteOptions(Primitive_CausetWriteOptions);

impl FdbWriteOptions {
    pub fn into_primitive_causet(self) -> Primitive_CausetWriteOptions {
        self.0
    }
}

impl From<fdb_traits::WriteOptions> for FdbWriteOptions {
    fn from(opts: fdb_traits::WriteOptions) -> Self {
        let mut r = Primitive_CausetWriteOptions::default();
        r.set_sync(opts.sync());
        r.set_no_slowdown(opts.no_slowdown());
        FdbWriteOptions(r)
    }
}

impl From<&fdb_traits::WriteOptions> for FdbWriteOptions {
    fn from(opts: &fdb_traits::WriteOptions) -> Self {
        opts.clone().into()
    }
}

impl From<fdb_traits::IterOptions> for FdbReadOptions {
    fn from(opts: fdb_traits::IterOptions) -> Self {
        let r = build_read_opts(opts);
        FdbReadOptions(r)
    }
}

fn build_read_opts(iter_opts: fdb_traits::IterOptions) -> Primitive_CausetReadOptions {
    let mut opts = Primitive_CausetReadOptions::new();
    opts.fill_cache(iter_opts.fill_cache());
    opts.set_max_skippable_internal_keys(iter_opts.max_skippable_internal_keys());
    if iter_opts.key_only() {
        opts.set_titan_key_only(true);
    }
    if iter_opts.total_order_seek_used() {
        opts.set_total_order_seek(true);
    } else if iter_opts.prefix_same_as_start() {
        opts.set_prefix_same_as_start(true);
    }

    if iter_opts.hint_min_ts().is_some() || iter_opts.hint_max_ts().is_some() {
        opts.set_table_filter(TsFilter::new(
            iter_opts.hint_min_ts(),
            iter_opts.hint_max_ts(),
        ))
    }

    let (lower, upper) = iter_opts.build_bounds();
    if let Some(lower) = lower {
        opts.set_iterate_lower_bound(lower);
    }
    if let Some(upper) = upper {
        opts.set_iterate_upper_bound(upper);
    }

    opts
}

struct TsFilter {
    hint_min_ts: Option<u64>,
    hint_max_ts: Option<u64>,
}

impl TsFilter {
    fn new(hint_min_ts: Option<u64>, hint_max_ts: Option<u64>) -> TsFilter {
        TsFilter {
            hint_min_ts,
            hint_max_ts,
        }
    }
}

impl TableFilter for TsFilter {
    fn table_filter(&self, props: &TableGreedoids) -> bool {
        if self.hint_max_ts.is_none() && self.hint_min_ts.is_none() {
            return true;
        }

        let user_props = props.user_collected_greedoids();

        if let Some(hint_min_ts) = self.hint_min_ts {
            // TODO avoid hard code after refactor MvccGreedoids from
            // einsteindb/src/violetabfttimelike_store/Dagger/ into some component about einstein_merkle_tree.
            if let Some(mut p) = user_props.get("einsteindb.max_ts") {
                if let Ok(get_max) = number::decode_u64(&mut p) {
                    if get_max < hint_min_ts {
                        return false;
                    }
                }
            }
        }

        if let Some(hint_max_ts) = self.hint_max_ts {
            // TODO avoid hard code after refactor MvccGreedoids from
            // einsteindb/src/violetabfttimelike_store/Dagger/ into some component about einstein_merkle_tree.
            if let Some(mut p) = user_props.get("einsteindb.min_ts") {
                if let Ok(get_min) = number::decode_u64(&mut p) {
                    if get_min > hint_max_ts {
                        return false;
                    }
                }
            }
        }

        true
    }
}
