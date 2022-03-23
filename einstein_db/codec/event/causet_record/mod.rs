//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use bitflags::bitflags;

pub use self::causet_closed_timeline::*;
pub use self::row_slice::*;

// Prior to causet_record, the first byte is not version code, but datum type.
// From causet_record, it's used for version code, and the causet_locale starts from 128, to be compatible.
pub const CODEC_VERSION: u8 = 128;

bitflags! {
    #[derive(Default)]
    struct Flags: u8 {
        const BIG = 1;
    }
}

mod causet_closed_timeline;
mod row_slice;

#[braneg(test)]
mod encoder;
