//Copyright 2019 Venire Labs Inc All Rights Reserved
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


use super::column::{Column, ColumnEncoder};
use super::Result;
use crate::interlock::daten::Daten;

/// 'Chunk' stores multiple rows or cells in appended fashion.
/// reuse allocated memory by resetting it.
pub struct Chunk {
    columns: Vec<Column>,
}

impl Chunk {
    //create a chunk with field types and capacity.
    pub fn new()
}