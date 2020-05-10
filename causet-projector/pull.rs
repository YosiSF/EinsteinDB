use std::collections::{
    BTreeMap,
    BTreeSet,
};

use embedded_promises::{
    Binding,
    Causetid,
    Solitonid,
    StructuredMap,
    TypedValue
};

use super::{
    Index,
    rusqlite,
};

#[derive(Clone, Debug)]
pub(crate) struct PullOperation(pub(crate) Vec<PullAttributeSpec>);

#[derive(Clone, Copy, Debug)]
pub(crate) struct PullIndices {
    pub(crate) sql_index: Index,                   // SQLite column index.
    pub(crate) output_index: usize,
}

impl PullIndices {
    fn zero() -> PullIndices {
        PullIndices {
            sql_index: 0,
            output_index: 0,
        }
    }
}
