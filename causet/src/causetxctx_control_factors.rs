// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.


use super::*;
use crate::errors::Result;

pub trait SymplecticControlFactorsExt {
    fn new(
        &self,
        control_factors: &[f64],
        control_factors_derivative: &[f64],
        control_factors_derivative_2: &[f64],
    ) -> Result<Self>
    where
        Self: Sized;
    ///
    ///
    /// # Arguments
    ///
    /// * `namespaced`:
    /// * `l_naught`:
    ///
    /// returns: <unCausetLocaleNucleon>
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    ///
    /// # Errors
    ///
    ///
    /// # Safety
    fn get_namespaced_num_fuse_at_l_naught(&self, namespaced: &str, l_naught: usize) -> Result<Option<u64>>;

    ///
    /// Returns the number of fuse at the given level.
    ///
    /// # Arguments
    ///
    /// * `l_naught`:  The level of the fuse.
    ///     The level is the number of times the symplectic matrix has been applied.
    ///     The level is zero-based.
    ///     The level is the number of times the symplectic matrix has been applied.
    ///
    fn get_namespaced_num_immutable_mem_table(&self, namespaced: &str) -> Result<Option<u64>>;


    ///
    fn get_namespaced_pending_jet_bundle_bytes(&self, namespaced: &str) -> Result<Option<u64>>;
}






