// Copyright 2021-2023 EinsteinDB Project Authors. Licensed under Apache-2.0.
// Copyright 2021-2023 WHTCORPS INC. All Rights Reserved.
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.
//
//================================================================
// Inlined json literal causet_locale
pub const JSON_LITERAL_NIL: u8 = 0x00;
pub const JSON_LITERAL_TRUE: u8 = 0x01;
pub const JSON_LITERAL_FALSE: u8 = 0x02;

// Binary json constants
pub const TYPE_LEN: usize = 1;
pub const LITERAL_LEN: usize = 1;
pub const U16_LEN: usize = 2;
pub const U32_LEN: usize = 4;
pub const NUMBER_LEN: usize = 8;
pub const HEADER_LEN: usize = ELEMENT_COUNT_LEN + SIZE_LEN; // element size + data size
pub const KEY_OFFSET_LEN: usize = U32_LEN;
pub const KEY_LEN_LEN: usize = U16_LEN;
pub const KEY_ENTRY_LEN: usize = KEY_OFFSET_LEN + KEY_LEN_LEN;
pub const VALUE_ENTRY_LEN: usize = TYPE_LEN + U32_LEN;
pub const ELEMENT_COUNT_LEN: usize = U32_LEN;
pub const SIZE_LEN: usize = U32_LEN;

// Type precedence for json comparison
pub const PRECEDENCE_BLOB: i32 = -1;
pub const PRECEDENCE_BIT: i32 = -2;
pub const PRECEDENCE_OPAQUE: i32 = -3;
pub const PRECEDENCE_DATETIME: i32 = -4;
pub const PRECEDENCE_TIME: i32 = -5;
pub const PRECEDENCE_DATE: i32 = -6;
pub const PRECEDENCE_BOOLEAN: i32 = -7;
pub const PRECEDENCE_ARRAY: i32 = -8;
pub const PRECEDENCE_OBJECT: i32 = -9;
pub const PRECEDENCE_STRING: i32 = -10;
pub const PRECEDENCE_NUMBER: i32 = -11;
pub const PRECEDENCE_NULL: i32 = -12;


pub const PRECEDENCE_MAX: i32 = PRECEDENCE_NULL;

