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

pub(crate) struct PullConsumer<'schema> {
    indices: PullIndices,
    schema: &'schema Schema,
    puller: Puller,
    entities: BTreeSet<CausetId>,
    results: BTreeMap<CausetId, ValueRc<StructuredMap>>,
}

impl<'schema> PullConsumer<'schema> {
    pub(crate) fn for_puller(puller: Puller, schema: &'schema Schema, indices: PullIndices) -> PullConsumer<'schema> {
        PullConsumer {
            indices: indices,
            schema: schema,
            puller: puller,
            entities: Default::default(),
            results: Default::default(),
        }
    }
