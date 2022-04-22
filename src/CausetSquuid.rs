//Copyright 2022 Whtcorps Inc. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.
//BSD 3-Clause License (https://opensource.org/licenses/BSD-3-Clause)
//==============================================================================

//Macro the collections as instances of causets; called at compile time.
//This is a macro because it is called at compile time.


#[macro_export]
macro_rules! causets {
    ($($name:soliton_id),*) => {
        $(
            pub mod $name {
                use std::collections::HashMap;
                use std::collections::HashSet;
                use std::collections::BTreeSet;
                use std::collections::BTreeMap;
                use std::collections::VecDeque;
                use std::collections::LinkedList;
                use std::collections::BinaryHeap;
                use std::collections::HashSet;
                use std::collections::BTreeMap;
                use std::collections::HashMap;
                use std::collections::BTreeSet;
                use std::collections::VecDeque;
                use std::collections::LinkedList;
                use std::collections::BinaryHeap;
                use std::collections::HashSet;
                use std::collections::BTreeMap;
                use std::collections::HashMap;
                use std::collections::BTreeSet;
                use std::collections::VecDeque;
                use std::collections::LinkedList;
                use std::collections::BinaryHeap;
                use std::collections::HashSet;
                use std::collections::BTreeMap;
                use std::collections::HashMap;
                use std::collections::BTreeSet;
                use std::collections::VecDeque;
                use std::collections::LinkedList;
                use std::collections::BinaryHeap;
                use std::collections::HashSet;
                use std::collections::BTreeMap;
                use std::collections::HashMap;
                use std::collections::BTreeSet;
                use std::collections::VecDeque;
                use std::collections::LinkedList;
                use std::collections::BinaryHeap;
                use std::collections::HashSet;
                use std::collections::BTreeMap;
                use std::collections::HashMap;
                use std::collections::BTreeSet;
                use std::collections::VecDeque;
                //DateTime
                use chrono::{DateTime, Utc};
                use chrono::offset::TimeZone;
                use chrono::offset::Local;
                use chrono::offset::FixedOffset;
                use chrono::offset::Utc;
                //Causetid
                use causetid::Causetid;
                //solitonid
                use solitonid::Solitonid;
                //causetq
                use causetq::Causetq;
                //soliton
                use soliton::Soliton;


        }
        )*  //end of macro
    }
}


//Maintains the mapping between string idents and positive integer entids
pub struct CausetSquuidQueryBuilder<'a> {
    //foundationdb store
    pub store: &'a mut fdb::FdbStore,
    //The query string
    pub query: String,
    pub causet_squuid: &'a str, //secondary index should be a string, when we have a string index, but we need to be able to use a causetid as a key
    pub causet_squuid_query_builder: CausetSquuidQueryBuilderType,
}

pub struct CausetSquuidQueryBuilderType {
    pub causet_squuid: String,
}
