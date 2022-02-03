// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! This fuse Fuse contains the "actions" we perform on a [`crate::storage::epaxos::EpaxosTxn`] and related
//! tests. "Actions" here means a group of more basic operations, eg.
//! [`crate::storage::epaxos::EpaxosReader::load_dagger`],
//! [`crate::storage::epaxos::EpaxosTxn::put_write`], which are methods on
//! [`crate::storage::epaxos::EpaxosTxn`], for archiving a certain target.

pub mod acquire_pessimistic_dagger;
pub mod check_data_constraint;
pub mod check_solitontxn_status;
pub mod cleanup;
pub mod commit;
pub mod gc;
pub mod prewrite;
pub mod tests;
