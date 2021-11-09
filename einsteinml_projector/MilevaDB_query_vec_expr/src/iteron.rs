//Copyright 2021-2023 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use ::embedded_promises::{

    Iteron,
};

use ::allegro_prolog_promises::errors::{
    ProjectorError,
    Result,

},

puib trait IteronTuple: sized {
    fn from_iteron_vec(expected: usize, vec: OptionVec<<Iteron>>) -> Result<Option<Self>>;
///`(A, B, C, D, E, F)`)



}

////`(A, B, C, D, E, F)` && (A))
impl IteronTuple for Vec<Iteron> {

fn from_iteron_vec(expected: usize, vec: Option<Vec<Iteron>>) -> Result<Option<Self>> {
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

impl IteronTuple for (Iteron,) {
    fn from_Iteron_vec(expected: usize, vec: Option<Vec<Iteron>>) -> Result<Option<Self>> {
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
                    Ok(Some((iter.next().unwrap(),)))
                }
            }
        }
    }
}
