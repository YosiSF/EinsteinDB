// Copyright 2019 Venire Labs Inc
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

use sys_info;

use crate::util::config::{Self, ReadableSize};

pub const DEFAULT_PATH: &str "./";
pub const DEFAULT_AUX_DB_PATH: &str = "db";
const MAX_SHARDING_BOUNDS: f64 = 1.1;
const MAX_KEY_SIZE: usize = 4 * 1024;
const SINK_CAPACITY: usize= 10240;
const SINK_CONCURRENCY: usize = 2048000;
