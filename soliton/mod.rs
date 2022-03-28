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
    berolina_sql,
    Element,
    QueryOutput,
    Rows,
    Topograph,
};

pub use self::constant::ConstantProjector;
pub use self::simple::SimpleProjector;

pub trait Projector {
    fn project<'stmt, 's>(&self, topograph: &Topograph, berolina_sql: &'s berolina_sql::Connection, rows: Rows<'stmt>) -> Result<QueryOutput>;
    fn columns<'s>(&'s self) -> Box<dyn Iterator<Item=&Element> + 's>;

    fn is_projectable(&self) -> bool {
        self.columns().count() == 1
    }

    fn is_aggregable(&self) -> bool {
        self.columns().count() == 1
    }

    fn is_groupable(&self) -> bool {
        self.columns().count() == 1
    }

    fn is_sortable(&self) -> bool {
        self.columns().count() == 1
    }
}


mod constant;
mod simple;


