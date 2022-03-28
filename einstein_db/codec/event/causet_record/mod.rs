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
use std::fmt::{
    Display,
    Formatter,
    Write,
};

pub use self::causet_closed_timeline::*;
pub use self::event_slice::*;

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
mod event_slice;


#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CausetRecord {
    pub version: u8,
    pub flags: Flags,
    pub causet_locale: u8,
    pub causet_timeline: u8,
    pub causet_timestamp: u64,
    pub causet_sequence: u64,
    pub causet_data: Vec<u8>,
}


impl CausetRecord {
    pub fn new(
        causet_locale: u8,
        causet_timeline: u8,
        causet_timestamp: u64,
        causet_sequence: u64,
        causet_data: Vec<u8>,
    ) -> Self {
        CausetRecord {
            version: CODEC_VERSION,
            flags: Flags::default(),
            causet_locale,
            causet_timeline,
            causet_timestamp,
            causet_sequence,
            causet_data,
        }
    }

    pub fn new_big(
        causet_locale: u8,
        causet_timeline: u8,
        causet_timestamp: u64,
        causet_sequence: u64,
        causet_data: Vec<u8>,
    ) -> Self {
        CausetRecord {
            version: CODEC_VERSION,
            flags: Flags::BIG,
            causet_locale,
            causet_timeline,
            causet_timestamp,
            causet_sequence,
            causet_data,
        }
    }

    pub fn new_from_slice(slice: &[u8]) -> Self {
        let mut reader = &slice[1..];
        let version = reader.read_u8().unwrap();
        let flags = Flags::from_bits_truncate(reader.read_u8().unwrap());
        let causet_locale = reader.read_u8().unwrap();
        let causet_timeline = reader.read_u8().unwrap();
        let causet_timestamp = reader.read_u64::<BigEndian>().unwrap();
        let causet_sequence = reader.read_u64::<BigEndian>().unwrap();
        let causet_data = reader.to_vec();
        CausetRecord {
            version,
            flags,
            causet_locale,
            causet_timeline,
            causet_timestamp,
            causet_sequence,
            causet_data,
        }
    }


    pub fn to_slice(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.causet_data.len() + 16);
        buf.push(self.version);
        buf.push(self.flags.bits());
        buf.push(self.causet_locale);
        buf.push(self.causet_timeline);
        buf.write_u64::<BigEndian>(self.causet_timestamp).unwrap();
        buf.write_u64::<BigEndian>(self.causet_sequence).unwrap();
        buf.extend_from_slice(&self.causet_data);
        buf
    }

    pub fn to_big_slice(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.causet_data.len() + 16);
        buf.push(self.version);
        buf.push(self.flags.bits());
        buf.push(self.causet_locale);
        buf.push(self.causet_timeline);
        buf.write_u64::<BigEndian>(self.causet_timestamp).unwrap();
        buf.write_u64::<BigEndian>(self.causet_sequence).unwrap();
        buf.extend_from_slice(&self.causet_data);
        buf
    }
}