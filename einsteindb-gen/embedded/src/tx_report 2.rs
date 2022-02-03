#![allow(dead_code)]

use std::collections::{
    BTreeMap,
};

use embedded_promises::{
    Causetid,
};

use ::{
    DateTime,
    Utc,
};

/// A transaction report summarizes an applied transaction.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub struct TxReport {
    /// The transaction ID of the transaction.
    pub tx_id: Causetid,

    /// The timestamp when the transaction began to be committed.
    pub tx_instant: DateTime<Utc>,

    /// A map from string literal tempid to resolved or allocated causetid.
    ///
    /// Every string literal tempid presented to the transactor either resolves via upsert to an
    /// existing causetid, or is allocated a new causetid.  (It is possible for multiple distinct string
    /// literal tempids to all unify to a single freshly allocated causetid.)
    pub tempids: BTreeMap<String, Causetid>,
}
