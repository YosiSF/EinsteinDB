//Copyright 2020 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


/// Type safe representation of the possible return values from SQLite's `typeof`
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
pub enum SQLTypeAffinity {
    Null,    // "null"
    Integer, // "integer"
    Real,    // "real"
    Text,    // "text"
    Blob,    // "blob"
}

pub trait SQLValueType {
     fn value_type_tag(&self) -> ValueTypeTag;
     fn accommodates_integer(&self, int: i64) -> bool;



     /// ValueType::Long and ValueType::Double).

     fn sql_representation(&self) -> (ValueTypeTag, Option<SQLTypeAffinity>);
 }
