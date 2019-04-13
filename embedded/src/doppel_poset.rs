//Copyright 2019 Venire Labs Inc
//
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::cell::Cell;
use std::rc::Rc;

#[derive(Clone)]
pub struct RcPosetCounter {
    c: Rc<Cell<usize>>,
}

impl RcPosetCounter {
    pub fn with_initial(value: usize)-> Self{
        RcPosetCounter { c: Rc::new(Cell::new(value)) }

    }

    pub fn new()-> Self{
        RcPosetCounter { c: Rc::new(Cell::new(0)) }
    }

    pub fn next(&self) -> usize {
        let current = self.c.get();
        self.c.replace(current+1)

    }

}

