//Venire Labs Inc 2019 All rights reserved

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

use std::fmt;

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum EvalType {
    Int,
    Real, 
    Decimal,
    Bytes,
    DateTime,
    Duration,
    Json,
}

impl fmt::Display for EvalType {
    fn fmt(&self, f:&mut fmt::Formatter<'_>)->fmnt::Result {
        fmt::Debug::fmt(self, f)
    }

}


