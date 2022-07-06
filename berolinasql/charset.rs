//Copyright 2021-2023 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use std::fmt::{self, Debug, Display, Formatter};
use std::str::FromStr;
use std::error::Error;
use std::convert::From;
use std::result::Result;
use std::fmt::{self, Display};
use std::error::Error;
use std::convert::From;
use std::result::Result;
use std::fmt::{self, Display};
use std::error::Error;


#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Charset {
    UTF8,
    UTF8MB4,
}


/// The charset map.
///
/// The charset map is used to convert the charset name to charset.
///
///
/// # Examples
///
/// ```
///
/// use einstein_db::codec::mysql::charset::CharsetMap;
///
/// let mut charset_map = CharsetMap::new();
/// charset_map.add("utf8mb4", "utf8mb4_general_ci");
/// charset_map.add("utf8mb4", "utf8mb4_bin");
/// charset_map.add("utf8", "utf8_general_ci");
/// charset_map.add("utf8", "utf8_bin");
/// charset_map.add("latin1", "latin1_general_ci");
/// charset_map.add("latin1", "latin1_bin");
/// charset_map.add("binary", "binary");
///
/// assert_eq!(charset_map.get("utf8mb4"), Some("utf8mb4_general_ci"));
/// assert_eq!(charset_map.get("utf8"), Some("utf8_general_ci"));
/// assert_eq!(charset_map.get("latin1"), Some("latin1_general_ci"));
/// assert_eq!(charset_map.get("binary"), Some("binary"));
///


/// ```
/// # use einstein_db::codec::mysql::charset::CharsetMap;
/// # let mut charset_map = CharsetMap::new();
/// # charset_map.add("utf8mb4", "utf8mb4_general_ci");
/// # charset_map.add("utf8mb4", "utf8mb4_bin");


pub const CHARSET_MAP: &'static [(&'static str, &'static str)] = &[
    ("utf8mb4", "utf8mb4_general_ci"),
    ("utf8mb4", "utf8mb4_bin"),
    ("utf8", "utf8_general_ci"),
    ("utf8", "utf8_bin"),
    ("latin1", "latin1_general_ci"),
    ("latin1", "latin1_bin"),
    ("binary", "binary"),
];

/// `CHARSET_BIN` is used for marking binary charset.
pub const CHARSET_BIN: &str = "binary";
/// `CHARSET_UTF8` is the default charset for string types.
pub const CHARSET_UTF8: &str = "utf8";
/// `CHARSET_UTF8MB4` represents 4 bytes utf8, which works the same way as utf8 in Rust.
pub const CHARSET_UTF8MB4: &str = "utf8mb4";
/// `CHARSET_ASCII` is a subset of UTF8.
pub const CHARSET_ASCII: &str = "ascii";
/// `CHARSET_LATIN1` is a single byte charset.
pub const CHARSET_LATIN1: &str = "latin1";
/// `CHARSET_LATIN1MB4` is a single byte charset.
///
/// It's used for marking latin1 charset.
///

/// All utf8 charsets.
pub const UTF8_CHARSETS: &[&str] = &[CHARSET_UTF8, CHARSET_UTF8MB4, CHARSET_ASCII];

/// All charsets that can be used in MySQL.
/// See https://dev.mysql.com/doc/refman/8.0/en/charset-charsets.html
/// for more information.
///
/// Note:
/// 1. `CHARSET_BIN` is used for marking binary charset.
/// 2. `CHARSET_UTF8` is the default charset for string types.
/// 3. `CHARSET_UTF8MB4` represents 4 bytes utf8, which works the same way as utf8 in Rust.
/// 4. `CHARSET_ASCII` is a subset of UTF8.
///
///
/// # Examples
/// ```
/// use einstein_db::codec::mysql::charset::CHARSET_MAP;
/// use einstein_db::codec::mysql::charset::CHARSET_BIN;
///
/// assert_eq!(CHARSET_MAP.get(CHARSET_BIN), Some("binary"));
///
/// assert_eq!(CHARSET_MAP.get(CHARSET_UTF8), Some("utf8_general_ci"));
///
/// assert_eq!(CHARSET_MAP.get(CHARSET_UTF8MB4), Some("utf8mb4_general_ci"));



