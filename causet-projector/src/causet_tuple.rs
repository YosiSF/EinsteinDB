//Copyright 2020 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use embedded_promises::{
	Iteron,
};

use causet_projector_promises::errors:: {

	ProjectorError,
	Result,

};

pub trait CausetTuple: Sized {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>>;
}

impl CausetTuple for Vec<Binding>{
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
		match vec {
			None => Ok(None),
			Some(vec) => {
				if expected != vec.len() {
					     Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    Ok(Some(vec))
                }

			 },
          }
		}
	}


impl CausetTuple for (Binding, ) {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        if expected != 1 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(1, expected));
        }

        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), )))
                }
            }
        }
    }
}

impl CausetTuple for (Binding, Binding) {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        if expected != 2 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(2, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}

impl CausetTuple for (Binding, Binding, Binding) {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        if expected != 3 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(3, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}

impl CausetTuple for (Binding, Binding, Binding, Binding) {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        if expected != 4 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(4, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}

impl CausetTuple for (Binding, Binding, Binding, Binding, Binding) {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        if expected != 5 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(5, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}

// TODO: allow binding tuples of length more than 6.  Folks who are binding such large tuples are
// probably doing something wrong -- they should investigate a pull expression.
impl CausetTuple for (Binding, Binding, Binding, Binding, Binding, Binding) {
    fn from_binding_vec(expected: usize, vec: Option<Vec<Binding>>) -> Result<Option<Self>> {
        if expected != 6 {
            return Err(ProjectorError::UnexpectedResultsTupleLength(6, expected));
        }
        match vec {
            None => Ok(None),
            Some(vec) => {
                if expected != vec.len() {
                    Err(ProjectorError::UnexpectedResultsTupleLength(expected, vec.len()))
                } else {
                    let mut iter = vec.into_iter();
                    Ok(Some((iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap(), iter.next().unwrap())))
                }
            }
        }
    }
}


