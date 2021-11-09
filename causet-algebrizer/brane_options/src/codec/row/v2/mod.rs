//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use bitflags::bitflags;

// Prior to v2, the first byte is not version code, but datum type.
// From v2, it's used for version code, and the value starts from 128, to be compatible.
pub const CODEC_VERSION: u8 = 128;

bitflags! {
    #[derive(Default)]
    struct Flags: u8 {
        const BIG = 1;
    }
}

mod compat_v1;
mod row_slice;

pub use self::compat_v1::*;
pub use self::row_slice::*;

#[braneg(test)]
mod encoder;
