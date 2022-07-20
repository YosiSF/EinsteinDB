//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


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

use einsteindb::causetq::sync::{CausalContext, CausalContextMut};
use einsteindb::causetq::sync::{CausalContextMutRef, CausalContextRef};
use einsteindb::causetq::sync::{CausalContextRefMut, CausalContextRefMutRef};

use crate::{
    berolinasql::{Error as BerolinaSqlError, ErrorKind as BerolinaSqlErrorKind},
    berolinasql::{ErrorImpl as BerolinaSqlErrorImpl},
    berolinasql::{Error as BerolinaSqlError, ErrorKind as BerolinaSqlErrorKind},
    causetq::*
};

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




#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PegMuxSingleton {
    pub id: usize,
    pub name: String,
}


impl PegMuxSingleton {
    pub fn new(id: usize, name: String) -> PegMuxSingleton {
        PegMuxSingleton {
            id,
            name,
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PegMux {
    pub id: usize,
    pub name: String,
}


impl PegMux {
    pub fn new(id: usize, name: String) -> PegMux {
        PegMux {
            id: id,
            name: name,
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
//rust peg mux implementation
pub struct PegMuxImpl {
    pub id: usize,
    pub name: String,
    pub state: Arc<Mutex<PegMuxState>>,
}


impl PegMuxImpl {
    pub fn new(id: usize, name: String) -> PegMuxImpl {
        PegMuxImpl {
            id: id,
            name: name,
            state: Arc::new(Mutex::new(PegMuxState::new(id, name))),
        }
    }
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PegMuxState {
    pub id: usize,
    pub name: String,
    pub state: Arc<Mutex<PegMuxStateImpl>>,
}


impl PegMuxState {
    pub fn new(id: usize, name: String) -> PegMuxState {
        PegMuxState {
            id: id,
            name: name,
            state: Arc::new(Mutex::new(PegMuxStateImpl::new(id, name))),
        }
    }
}


pub fn from_context_grammar_peg_mux_impl(context_grammar_peg_mux_impl: &ContextGrammarPegMuxImpl) -> PegMuxImpl {
    PegMuxImpl::new(
        context_grammar_peg_mux_impl.id,
        context_grammar_peg_mux_impl.name.clone(),
    )


}
pub struct PegMuxInstance {
    pub id: usize,
    /// The id of the PegMux that this instance belongs to.
    pub name: String,
}



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct NodeId(pub usize);

trait NodeIdTrait {
    fn id(&self) -> usize;
}


impl NodeIdTrait for NodeId {
    fn get_id(&self) -> usize {
        self.0
    }
}


impl NodeIdTrait for NodeId {
    fn get_id(&self) -> usize {
        self.0
    }
}

pub fn get_node_id() -> NodeId {
    NodeId(thread::current().id())
}


impl NodeId {
    pub fn new(id: usize) -> NodeId {
        NodeId(id)
    }
}




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








use std::error::Error;
use std::fmt;
use std::io;
use std::result;

use causet::*;

#[derive(Debug)]
pub enum ErrorKind {
    Io(io::Error),
    BerolinaSql(BerolinaSqlError),
    Utf8(Utf8Error),
    FromUtf8(FromUtf8Error),
    Other(String),
}


#[derive(Debug)]
pub struct ErrorImpl {
    pub kind: ErrorKind,
}


#[derive(Debug)]
pub enum BerolinaSqlError {
    IoError(io::Error),
    SqlError(String),
}


impl fmt::Display for BerolinaSqlError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            BerolinaSqlError::IoError(ref err) => write!(f, "IO error: {}", err),
            BerolinaSqlError::SqlError(ref err) => write!(f, "SQL error: {}", err),
        }
    }

    fn cause(&self) -> Option<&dyn Error> {
        match *self {
            BerolinaSqlError::IoError(ref err) => Some(err),
            BerolinaSqlError::SqlError(_) => None,
        }
    }

    fn description(&self) -> &str {
        match *self {
            BerolinaSqlError::IoError(_) => "IO error",
            BerolinaSqlError::SqlError(_) => "SQL error",
        }
    }
}


impl From<BerolinaSqlError> for ErrorImpl {
    fn from(err: BerolinaSqlError) -> ErrorImpl {
        ErrorImpl {
            kind: ErrorKind::BerolinaSql(err),
        }
    }
}


impl From<io::Error> for ErrorImpl {
    fn from(err: io::Error) -> ErrorImpl {
        ErrorImpl {
            kind: ErrorKind::Io(err),
        }
    }
}


impl From<Utf8Error> for ErrorImpl {
    fn from(err: Utf8Error) -> ErrorImpl {
        ErrorImpl {
            kind: ErrorKind::Utf8(err),
        }
    }
}





//mods
