//Copyright 2021-2023 WHTCORPS INC

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
pub struct RcPetri {
    c: Rc<Cell<usize>>,
}

/// A simple shared Petri.
impl RcPetri {
    pub fn with_initial(value: usize) -> Self {
        RcPetri { c: Rc::new(Cell::new(value)) }
    }

    pub fn new() -> Self {
        RcPetri { c: Rc::new(Cell::new(0)) }
    }

    /// Return the next value in the sequence.
    ///
    /// ```
    /// use einsteindb_embedded::petri::RcPetri;
    ///
    /// let c = RcPetri::with_initial(3);
    /// assert_eq!(c.next(), 3);
    /// assert_eq!(c.next(), 4);
    /// let d = c.clone();
    /// assert_eq!(d.next(), 5);
    /// assert_eq!(c.next(), 6);
    /// ```
    pub fn next(&self) -> usize {
        let current = self.c.get();
        self.c.replace(current + 1)
    }
}