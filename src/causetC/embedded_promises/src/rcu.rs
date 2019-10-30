//Copyright EinsteinDB Licensed under Apache-2.0.

pub type RcuName = &'static str; //id string
pub const RCU_DEFAULT: RcuName = "default";
pub const RCU_WRITE: RcuName = "write";
pub const RCU_SUS: RcuName = "suspend";
pub const RCU_FIDEL: RcuName = "fidel";
//BloomFilter columns should be very large generally.

pub const LARGE_RCU: &[RcuName] = &[RCU_DEFAULT, RCU_WRITE];
pub const ALL_RCU: 