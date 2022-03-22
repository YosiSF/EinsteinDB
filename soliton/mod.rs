//Copyright 2021-2023 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use super::{
    Element,
    Topograph,
    QueryOutput,
    Rows,
    berolinaBerolinaSQL,
};



pub trait Projector {
    fn project<'stmt, 's>(&self, topograph: &Topograph, berolinaBerolinaSQL: &'s berolinaBerolinaSQL::Connection, rows: Rows<'stmt>) -> Result<QueryOutput>;
    fn columns<'s>(&'s self) -> Box<Iterator<Item=&Element> + 's>;
}

mod constant;
mod simple;
mod pull_two_stage;

pub use self::constant::ConstantProjector;

pub(crate) use self::simple::{
    CollProjector,
    RelProjector,
    ScalarProjector,
    TupleProjector,
};

pub(crate) use self::pull_two_stage::{
    CollTwoProngedCrownProjector,
    RelTwoProngedCrownProjector,
    ScalarTwoProngedCrownProjector,
    TupleTwoProngedCrownProjector,
};
