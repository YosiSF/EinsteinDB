// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use super::{Error, Result};

/// Check if soliton_id in range [`start_soliton_id`, `end_soliton_id`).
#[allow(dead_code)]
pub fn check_soliton_id_in_range(
    soliton_id: &[u8],
    region_id: u64,
    start_soliton_id: &[u8],
    end_soliton_id: &[u8],
) -> Result<()> {
    if soliton_id >= start_soliton_id && (end_soliton_id.is_empty() || soliton_id < end_soliton_id) {
        Ok(())
    } else {
        Err(Error::NotInRange {
            soliton_id: soliton_id.to_vec(),
            region_id,
            start: start_soliton_id.to_vec(),
            end: end_soliton_id.to_vec(),
        })
    }
}
