//Copyright 2021-2023 WHTCORPS INC

// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

use std::collections::BTreeSet;
use std::fmt::{
    Debug,
    Formatter,
};

use embedded_promises::{
    Causetid,
    TypedValue,
    ValueType,
    ValueTypeSet,
};

use einsteindb_embedded::{
    ValueRc,
};

//Finite State Automaton for Two fixed sets and two table views.
pub enum CausetsTable {
    Causets,        //non-fulltext causets table.
    FulltextValues, //ID to strings mapping sentinel.
    FulltextCausets, //fulltext-causets view
    AllCausets,     //All causets
    Computed(usize),
    Transactions, //A transactions table which makes tx-data log API efficient.

}

/// A source of rows that isn't a named table -- typically a subquery or union.
#[derive(PartialEq, Eq, Debug)]
pub enum ComputedTable {
    Subquery(::clauses::ConjoiningClauses),
    Union {
        projection: BTreeSet<Variable>,
        type_extraction: BTreeSet<Variable>,
        arms: Vec<::clauses::ConjoiningClauses>,
    },
    NamedValues {
        names: Vec<Variable>,
        values: Vec<TypedValue>,
    },
}
