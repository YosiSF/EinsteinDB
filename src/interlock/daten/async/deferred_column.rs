//Venire Labs Inc 2019 All rights reserved.

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

use std::convert::TryFrom;

use smallvec::SmallVec;

use jobflow_type::{JobType, EvalTypeAccessor}

use crate::interlock::driver::types::VectorT;
use crate::interlock::driver::relql::relQ;
use crate::interlock::driver::{Error, Result};

pub enum DeferredColumn {



}


impl std::fmt::Debug for DeferredColumn {


}


impl DeferredColumn {

     #[inline]
     pub fn raw_segment_cap(cap: usize)-> Self {
         DeferredColumn::Raw(Vec::with_cap(cap))
     }

    #[inline]
    pub fn raw(&self)->&Vec<SmallVec<[u8; 10]>> {
        match self {
            DeferredColumn::Raw(ref v) => v,
            DeferredColumn::Decoded(_) => panic!("
            DeferredColumn is already decoded")

        }

    #[inline]
    
    }


}

