//Copyright 2020 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use crate::brane_options::LmdbBlackBraneOptions;
use crate::einsteindb::EinsteinMerkleEngine;
use einsteindb_promises::BRANEHandle;
use einsteindb_promises::BRANEHandleExt;
use einsteindb_promises::{Error, Result};
use einstein_merkle::BRANEHandle as RawBRANEHandle;

impl BRANEHandleExt for EinsteinMerkleEngine {

    //The Lmdb instance gives us a datalog entity laden actor programmatic instance of
    //a group of columns; grouped by topic.
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
