//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//mod for soliton_panic


//mod for soliton_panic

#![crate_type= "lib"]
#![crate_name= "einsteindb"]
#![recursion_limit = "1024"]
#![feature(proc_macro_hygiene)]
#![feature(proc_macro_non_items)]
#![feature(proc_macro_def_site_meta)]
#![feature(proc_macro_derive)]
#![feature(proc_macro_expr)]


#[macro_use]
extern crate soliton_panic;




extern crate soliton;

//mods
use causal_set::CausalSet;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Partitioning};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::{thread, time};
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::TrySendError;
use std::sync::mpsc::SendError;


use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::HashSet;
use std::collections::hash_map::Entry;




use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::collections::hash_map::Entry;


use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::atomic::AtomicBool;


use std::sync::atomic::AtomicUsize;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PosetNodeId(pub usize);


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PosetNodeData(pub String);


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PosetNode(pub PosetNodeId, pub PosetNodeData);


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PosetEdge(pub PosetNodeId, pub PosetNodeId);


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PosetEdgeData(pub String);


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Poset(pub Vec<PosetNode>, pub Vec<PosetEdge>, pub Vec<PosetEdgeData>);


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PosetError(pub PosetErrorKind);




#[derive(Deserialize, Serialize)]
pub struct PosetErrorKind(pub String);


#[derive(Serialize, Deserialize)]
pub struct PosetConfig {
    pub name: String,
    pub thread_count: usize,
}





//mod for causal_set
pub mod causal_set;








//mod for causal_set_test
//TODO: test the causal_set module  and the causet module  in the same time  and the causet module is not used in the causal_set module
//mod for causal_set_test


//mod for einsteindb_macro_test


//mod for einsteindb_macro_impl_test




