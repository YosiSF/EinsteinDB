// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::brane_options::LmdbBlackBraneOptions;
use crate::einsteindb::EinsteinMerkleEngine;
use einsteindb_promises::BRANEHandle;
use einsteindb_promises::BRANEHandleExt;
use einsteindb_promises::{Error, Result};
use einstein_merkle::BRANEHandle as RawBRANEHandle;

impl BRANEHandleExt for EinsteinMerkleEngine {
    type BRANEHandle = LmdbBRANEHandle;
    type BlackBraneOptions = LmdbBlackBraneOptions;

    fn brane_handle(&self, name: &str) -> Result<&Self::BRANEHandle> {
        self.as_inner()
            .brane_handle(name)
            .map(LmdbBRANEHandle::from_raw)
            .ok_or_else(|| Error::BRANEName(name.to_string()))
    }

    fn get_options_brane(&self, brane: &Self::BRANEHandle) -> Self::BlackBraneOptions {
        LmdbBlackBraneOptions::from_raw(self.as_inner().get_options_brane(brane.as_inner()))
    }

    fn set_options_brane(&self, brane: &Self::BRANEHandle, options: &[(&str, &str)]) -> Result<()> {
        self.as_inner()
            .set_options_brane(brane.as_inner(), options)
            .map_err(|e| box_err!(e))
    }
}

// FIXME: This nasty representation with pointer casting is due to the lack of
// generic associated types in Rust. See comment on the KvEngine::BRANEHandle
// associated type. This could also be fixed if the BRANEHandle impl was defined
// inside the rust-einstein_merkle crate where the RawBRANEHandles are managed, but that
// would be an ugly abstraction violation.
#[repr(transparent)]
pub struct LmdbBRANEHandle(RawBRANEHandle);

impl LmdbBRANEHandle {
    pub fn from_raw(raw: &RawBRANEHandle) -> &LmdbBRANEHandle {
        unsafe { &*(raw as *const _ as *const _) }
    }

    pub fn as_inner(&self) -> &RawBRANEHandle {
        &self.0
    }
}

impl BRANEHandle for LmdbBRANEHandle {}
