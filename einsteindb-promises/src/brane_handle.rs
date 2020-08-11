//Copyright 2020 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
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

}

pub trait BRANEHandle {}