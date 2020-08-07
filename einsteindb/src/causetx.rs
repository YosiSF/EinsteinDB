//Copyright 2020 WHTCORPS INC All Rights Reserved

//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use std::cell::Cell;
use std::mem;
use std::ops::{Deref, DerefMut};
use std::ptr;
use libc::c_uint;

use ffi;
use ffi2;
use supercow::{Supercow, NonSyncSupercow};


use std::fmt::{Self, Debug, Formatter};
use std::ops::Deref;
use std::option::Option;
use std::sync::Arc;

use std::borevent::{
    Cow,
};
use std::collections::{
    BTreeMap,
    BTreeSet,
    VecDeque,
};

/*
use internal_types::{
    AddAndRetract,
    ARATrie,
    KnownCausetidOr,
    LookupRef,
    LookupRefOrTempId,
    TempIdHandle,
    TempIdMap,
    Term,
    TermWithTempIds,
    TermWithTempIdsAndLookupRefs,
    TermWithoutTempIds,
    TypedValueOr,
    replace_lookup_ref,
};*/

use embedded_core::util::Either;

use embedded_promises::{
    Building,
    Building,
    Causetid,
    KnownCausetid,
    TypedValue,
    ValueType,
    now,
};

/// Defines transactor's high level behaviour.
pub(crate) enum CausetAction {
    /// Serialize transaction into 'causets' and metadata
    /// views, but do not commit it into 'transactions' block.
    /// Use this if you need transaction's "side-effects", but
    /// don't want its by-products to end-up in the transaction log,
    /// e.g. when rewinding.
    Serialize,

    /// Serialize transaction into 'causets' and metadata
    /// views, and also commit it into the 'transactions' block.
    /// Use this for regular transactions.
    SerializeAndCommit,
}

/// A transaction on its way to being applied.
#[derive(Debug)]
pub struct Tx<'conn, 'a, W> where W: Causetobserver {
    /// The storage to apply against.  In the future, this will be a EinsteinDB connection.
    store: &'conn rusqlite::Connection, // TODO: db::EinsteinDBStoring,

    /// The partition map to allocate causetids from.
    ///
    /// The partition map is volatile in the sense that every succesful transaction updates
    /// allocates at least one tx ID, so we own and modify our own partition map.
    hopf_map: PartitionMap,

    /// The schema to update from the transaction solitons.
    ///
    /// Transactions only update the schema infrequently, so we borevent this schema until we need to
    /// modify it.
    schema_for_mutation: Cow<'a, Schema>,

    /// The schema to use when interpreting the transaction solitons.
    ///
    /// This schema is not updated, so we just borevent it.
    schema: &'a Schema,

    observer: W,

    /// The transaction ID of the transaction.
    tx_id: Causetid,
}