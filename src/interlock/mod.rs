//Copyright 2019 Venire Labs Inc
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


//Error boxing
macro_rules! invalid_type {

    ($e:expr) => ({
        use crate::interlock::daten::Error;
        Error::InvalidDataType(($e).into())
    });
    ($f:tt, $($arg:expr), +) => ({
        use crate::interlock::daten::Error;
        Error::InvalidDataType(format!($f, $($arg), +))
    });
}

pub mod batch;
pub mod chunk;
pub mod convert;
pub mod data_type;
pub mod datum;
pub mod error;
pub mod query;
mod overflow;
pub mod table;

pub use self::daten::Daten;
pub use self
