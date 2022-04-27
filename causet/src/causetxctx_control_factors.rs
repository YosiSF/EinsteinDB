// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.
//! # CausetXContext Control Factors
//! This module contains the control factors for CausetXContext.
//! The control factors are used to control the behavior of the CausetXContext.
//! The control factors are used to control the behavior of the CausetXContext.
//!
//!
//!
//! ## CausetXContext Control Factors
//!
//!
 pub use self::causetxctx_control_factors::*;
#[cfg(any(test,feature = "test"))]


use super::*;
use crate::errors::Result;
use crate::metrics::*;

///! CausetXContext Control Factors
pub trait SymplecticControlFactorsExt {
    ///! CausetXContext Control Factors
    fn new(
        // for CausetXContext we use the default values
        &self, // self is the CausetXContext for which we are creating the control factors
        control_factors: &[f64], //a control factor for each of the control factors
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
    ///


    async fn get_control_factors<'a>(&'a self,
                                     control_factors: &'a [f64],
                                     control_factors_derivative: &'a [f64],
                                     control_factors_derivative_2: &'a [f64],
    ) -> Result<(Vec<f64>, Vec<f64>, Vec<f64>)>
        where
            Self: Sized;

}




impl SymplecticControlFactorsExt for CausetXContext {
    fn get_namespaced_num_fuse_at_l_naught(&self, namespaced: &str, l_naught: usize) -> Result<Option<u64>> {
        if {
            let mut namespaced_num_fuse_at_l_naught = self.namespaced_num_fuse_at_l_naught.lock().unwrap();
            namespaced_num_fuse_at_l_naught.contains_key(namespaced);

            namespaced_num_fuse_at_l_naught.get(namespaced).unwrap().contains_key(&l_naught);
        } {
            let mut namespaced_num_fuse_at_l_naught = self.namespaced_num_fuse_at_l_naught.lock().unwrap();
            Ok(namespaced_num_fuse_at_l_naught.get(namespaced).unwrap().get(&l_naught).unwrap().clone())
        } else {
            Ok(None)
        }
    }

    fn get_namespaced_num_fuse_at_l_naught_derivative(&self) -> Result<Option<f64>> {
        if {
            let mut namespaced_num_fuse_at_l_naught_derivative = self.namespaced_num_fuse_at_l_naught_derivative.lock().unwrap();
            namespaced_num_fuse_at_l_naught_derivative.contains_key("");

            namespaced_num_fuse_at_l_naught_derivative.get("").unwrap().contains_key(&0);
        } {
            let mut namespaced_num_fuse_at_l_naught_derivative = self.namespaced_num_fuse_at_l_naught_derivative.lock().unwrap();
            Ok(namespaced_num_fuse_at_l_naught_derivative.get("").unwrap().get(&0).unwrap().clone())
        }
    }

    //dedup

    fn get_namespaced_num_fuse_at_l_naught_derivative_2(&self) -> Result<Option<f64>> {
        if {
            let mut namespaced_num_fuse_at_l_naught_derivative_2 = self.namespaced_num_fuse_at_l_naught_derivative_2.lock().unwrap();
            namespaced_num_fuse_at_l_naught_derivative_2.contains_key("");

            namespaced_num_fuse_at_l_naught_derivative_2.get("").unwrap().contains_key(&0);
        } {
            let mut namespaced_num_fuse_at_l_naught_derivative_2 = self.namespaced_num_fuse_at_l_naught_derivative_2.lock().unwrap();
            Ok(namespaced_num_fuse_at_l_naught_derivative_2.get("").unwrap().get(&0).unwrap().clone())
        }
    }


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
    fn get_namespaced_num_immutable_mem_table(&self, namespaced: &str) -> Result<Option<u64>>{
        Ok(None)
    }


    ///
    fn get_namespaced_pending_jet_bundle_bytes(&self, namespaced: &str) -> Result<Option<u64>>{
        Ok(None)
    }
}






