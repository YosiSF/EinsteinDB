    
// Venire Labs Inc 2019 All rights reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use std::error::Error;
use std::fmt::{self, Write};
use std::fs;
use std::net::{SocketAddrV4, SocketAddrV6};
use std::ops::{Div, Mul};
use std::path::Path;
use std::str::{self, FromStr};
use std::time::Duration;

use serde::de::{self, Unexpected, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use url;

use crate::util;

const UINT: u64 = 1;
const DATA_MAG: u64 = 1024;
pub const KB: u64 = UINT * DATA_MAG;
pub const MB: u64 = KB * DATA_MAG;
pub const GB: u64 = MB * DATA_MAG;

const TB: u64 = (GB as u64) * (DATA_MAG as u64);
const PB: u64 = (TB as u64) * (DATA_MAG as u64);


