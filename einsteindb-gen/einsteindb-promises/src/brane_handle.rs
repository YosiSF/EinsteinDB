//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use crate::brane_options::BlackBraneOptions;
use crate::errors::Result;


pub trait BRANEHandleExt {
    type BRANEhandle: BRANEhandle;
    type BlackBraneOptions: BlackBraneOptions;

    fn brane_handle(&self, name: &str) -> Result<&Self::BRANEHandle>;
    fn get_options_brane(&self, brane: &Self::BRANEHandle) -> Self::BlackBraneOptions;
    fn set_options_brane(&self, brane: &Self::BRANEHandle, options: &[(&str, &str)]) -> Result<()>;


    ///This is the setup interface, it will also contain some of the CRUD functionality. When an engine is added we can then initialize it.

    /// add engines! (Black Brane) - BBRANE_RESOURCE_NAME=1 (this is our default engine). This implements a regex for matching files against... and it will return true if a file matches. The file matcher will be used for validating that a file has not been added before. For now we have all files in memory so we shouldnt be storing them on disk! (we should use this later when creating new users or something like that.)

    pub trait BRANEHandle {}

    fn brane(&self, name: &str) -> Result<Self::BRANEhandle>;
    fn b_create(&self, name: &str) -> Result<Self::BRANEhandle>;
    fn b_destroy(&self, brane: Self::BRANEhandle) -> Result<()>;
    fn b_get(&self, brane: &Self::BRANEHandle, key: &[u8]) -> Result<Vec<u8>>;
    fn b_set(&mut self, brane: &Self::BRANEHandle, key: Vec<u8>, value: Vec<u8>) -> Result<()>;
}