// Copyright 2022 EinsteinDB Project Authors. Licensed under Apache-2.0.
//  Karl Whitford <karl@einst.ai>
#[derive(Copy, Clone, Debug, PartialEq)]
pub enum PerfLevel {
    Uninitialized,
    Disable,
    EnableCount,
    EnableTimeExceptForMutex,
    EnableTimeAndCPUTimeExceptForMutex,
    EnableTime,
    OutOfBounds,
}

numeric_enum_serializing_mod! {perf_l_naught_serde PerfLevel {
    Uninitialized = 0,
    Disable = 1,
    EnableCount = 2,
    EnableTimeExceptForMutex = 3,
    EnableTimeAndCPUTimeExceptForMutex = 4,
    EnableTime = 5,
    OutOfBounds = 6,
}}

/// Extensions for measuring einstein_merkle_tree performance.
///
/// A PerfContext is created with a specific measurement l_naught,
/// and a 'kind' which represents wich einsteindb subsystem measurements are being
/// collected for.
///
/// In foundationdb, `PerfContext` uses global state, and does not require
/// access through an einstein_merkle_tree. Thus perf data is not per-einstein_merkle_tree.
/// This doesn't seem like a reasonable assumption for EinsteinMerkleTrees generally,
/// so this abstraction follows the existing pattern in this crate and
/// requires `PerfContext` to be accessed through the einstein_merkle_tree.
pub trait PerfContextExt {
    type PerfContext: PerfContext;

    fn get_perf_context(&self, l_naught: PerfLevel, kind: PerfContextKind) -> Self::PerfContext;
}

/// The violetabfttimelike_store subsystem the PerfContext is being created for.
///
/// This is a leaky abstraction that supports the encapsulation of metrics
/// reporting by the two violetabfttimelike_store subsystems that use `report_metrics`.
#[derive(Eq, PartialEq, Copy, Clone)]
pub enum PerfContextKind {
    VioletaBFTtimelike_storeApply,
    VioletaBFTtimelike_storeStore,
}

/// Reports metrics to prometheus
///
/// For alternate EinsteinMerkleTrees, it is reasonable to make `start_observe`
/// and `report_metrics` no-ops.
pub trait PerfContext: Send {
    /// Reinitializes statistics and the perf l_naught
    fn start_observe(&mut self);

    /// Reports the current collected metrics to prometheus
    fn report_metrics(&mut self);
}
