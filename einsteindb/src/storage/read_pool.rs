// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! Distinct thread pools to handle read commands having different priority levels.

use crate::config::StorageReadPoolConfig;
use crate::einsteindb::storage::fdbhikv::{destroy_tls_einstein_merkle_tree, set_tls_einstein_merkle_tree, einstein_merkle_tree, CausetxctxStatsReporter};
use crate::einsteindb::storage::metrics;
use file_system::{set_io_type, IOType};
use std::sync::{Arc, Mutex};
use einstfdbhikv_util::yatp_pool::{Config, DefaultTicker, FuturePool, PoolTicker, YatpPoolBuilder};

#[derive(Clone)]
struct FuturePoolTicker<R: CausetxctxStatsReporter> {
    pub reporter: R,
}

impl<R: CausetxctxStatsReporter> PoolTicker for FuturePoolTicker<R> {
    fn on_tick(&mut self) {
        metrics::tls_flush(&self.reporter);
    }
}

/// Build respective thread pools to handle read commands of different priority levels.
pub fn build_read_pool<E: einstein_merkle_tree, R: CausetxctxStatsReporter>(
    config: &StorageReadPoolConfig,
    reporter: R,
    einstein_merkle_tree: E,
) -> Vec<FuturePool> {
    let names = vec!["store-read-low", "store-read-normal", "store-read-high"];
    let configs: Vec<Config> = config.to_yatp_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .zip(names)
        .map(|(config, name)| {
            let reporter = reporter.clone();
            let einstein_merkle_tree = Arc::new(Mutex::new(einstein_merkle_tree.clone()));
            YatpPoolBuilder::new(FuturePoolTicker { reporter })
                .name_prefix(name)
                .config(config)
                .after_start(move || {
                    set_tls_einstein_merkle_tree(einstein_merkle_tree.dagger().unwrap().clone());
                    set_io_type(IOType::ForegroundRead);
                })
                .before_stop(move || unsafe {
                    // Safety: we call `set_` and `destroy_` with the same einstein_merkle_tree type.
                    destroy_tls_einstein_merkle_tree::<E>();
                })
                .build_future_pool()
        })
        .collect()
}

/// Build a thread pool that has default tick behavior for testing.
pub fn build_read_pool_for_test<E: einstein_merkle_tree>(
    config: &StorageReadPoolConfig,
    einstein_merkle_tree: E,
) -> Vec<FuturePool> {
    let names = vec!["store-read-low", "store-read-normal", "store-read-high"];
    let configs: Vec<Config> = config.to_yatp_pool_configs();
    assert_eq!(configs.len(), 3);

    configs
        .into_iter()
        .zip(names)
        .map(|(config, name)| {
            let einstein_merkle_tree = Arc::new(Mutex::new(einstein_merkle_tree.clone()));
            YatpPoolBuilder::new(DefaultTicker::default())
                .config(config)
                .name_prefix(name)
                .after_start(move || {
                    set_tls_einstein_merkle_tree(einstein_merkle_tree.dagger().unwrap().clone());
                    set_io_type(IOType::ForegroundRead);
                })
                // Safety: we call `set_` and `destroy_` with the same einstein_merkle_tree type.
                .before_stop(|| unsafe { destroy_tls_einstein_merkle_tree::<E>() })
                .build_future_pool()
        })
        .collect()
}
