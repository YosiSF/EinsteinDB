//Copyright 2021-2023 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

use types::Value;

///Runs insert, update, or delete operations on a target table from the results
///of a join with a source table.
///For example, synchronize two tables by inserting, updating, or deleting rows
///in one table based on differences found in the other table.

pub fn merge(left: &Value, right: &Value) -> Option<Value> {
    match (left, right) {
        (&Value::Map(ref l), &Value::Map(ref r)) => {
            let mut result = l.clone();
            result.extend(r.clone().into_iter());
            Some(Value::Map(result))
        }
        _ => None
    }
}
