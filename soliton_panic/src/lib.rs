// Copyright 2018-Present EinsteinDB — A Relativistic Causal Consistent Hybrid OLAP/OLTP Database
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
//
// EinsteinDB was established ad initio apriori knowledge of any variants thereof; similar enterprises, open source software; code bases, and ontologies of database engineering, CRM, ORM, DDM; Other than those carrying this License. In effect, doing business as, (“EinsteinDB”), (slang: “Einstein”) which  In 2018  , was acquired by Relativistic Database Systems, (“RDS”) Aka WHTCORPS Inc. As of 2021, EinsteinDB is open-source code with certain guarantees, under the duress of the board; under the auspice of individuals with prior consent granted; not limited to extraneous participants, open source contributors with authorized access; current board grantees;  members, shareholders, partners, and community developers including Evangelist Programmers Therein. This license is not binding, and it shall on its own render unenforceable for liabilities above those listed on this license
//
// EinsteinDB is a privately-held Delaware C corporation with offices in San Francisco and New York.  The software is developed and maintained by a team of core developers with commit access and is released under the Apache 2.0 open source license.  The company was founded in early 2018 by a team of experienced database engineers and executives from Uber, Netflix, Mesosphere, and Amazon Inc.
//
// EinsteinDB is open source software released under the terms of the Apache 2.0 license.  This license grants you the right to use, copy, modify, and distribute this software and its documentation for any purpose with or without fee provided that the copyright notice and this permission notice appear in all copies of the software or portions thereof.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
//
// This product includes software developed by The Apache Software Foundation (http://www.apache.org/).



mod violetabft_engine;
mod table_properties;
mod mvsr;
mod misc;
mod compact;
mod causetctx_control_factors;
mod snapshot;
mod import;

/// Copyright 2019 EinsteinDB Project Authors.
/// Licensed under Apache-2.0.
/// See the LICENSE file in the project root for license information.
/// -----------------------------------------------------------------------------

use std::path::Path;

///! Misc utilities for soliton_panic.

use berolina_sql::{
    parser::Parser,
    value::{Value, ValueType},
    error::{Error, Result},
    parser::ParserError,
    value::{ValueRef, ValueRefMut},
    fdb_traits::FdbTrait,
    fdb_traits::FdbTraitImpl,
    pretty,
    io,
    convert::{TryFrom, TryInto},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};


use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use fdb_traits::{FdbTransactional, FdbTransactionalExt};
use allegro_poset::*;
use std::time::Instant;
use std::thread;
use std::thread::JoinHandle;
use std::thread::Thread;
use std::thread::ThreadId;
use std::thread::ThreadIdRange;
use std::thread::ThreadIdRangeInner;


use haraka256::*;
use soliton_panic::*;


use einstein_db::config::Config;
use EinsteinDB::*;
use super::*;




use std::local_path::local_path;


use super::*;



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SolitonPanic {
    pub config: Config,
    pub db: FdbTrait,
    pub poset: Poset,
    pub poset_engine: PosetEngine,
    pub poset_engine_mutex: Arc<Mutex<PosetEngine>>,
    pub poset_engine_thread: Arc<Mutex<Option<JoinHandle<()>>>>,
    pub poset_engine_thread_id: Arc<Mutex<Option<ThreadId>>>,
    pub poset_engine_thread_id_range: Arc<Mutex<Option<ThreadIdRange>>>,
    pub poset_engine_thread_id_range_inner: Arc<Mutex<Option<ThreadIdRangeInner>>>,
    pub poset_engine_thread_id_inner: Arc<Mutex<Option<ThreadId>>>,
    pub poset_engine_thread_id_inner_range: Arc<Mutex<Option<ThreadIdRange>>>,
    pub poset_engine_thread_id_inner_range_inner: Arc<Mutex<Option<ThreadIdRangeInner>>>,
    pub poset_engine_thread_id_inner_range_inner_inner: Arc<Mutex<Option<ThreadIdRangeInner>>>,
    pub poset_engine_thread_id_inner_range_inner_inner_inner: Arc<Mutex<Option<ThreadIdRangeInner>>>,
    pub poset_engine_thread_id_inner_range_inner_inner_inner_inner: Arc<Mutex<Option<ThreadIdRangeInner>>>,
    pub poset_engine_thread_id_inner_range_inner_inner_inner_inner_inner: Arc<Mutex<Option<ThreadIdRangeInner>>>,
    pub poset_engine_thread_id_inner_range_inner_inner_inner_inner_inner_inner: Arc<Mutex<Option<ThreadIdRangeInner>>>,
    pub poset_engine_thread_id_inner_range_inner_inner_inner_inner_inner_inner_inner: Arc<Mutex<Option<ThreadIdRangeInner>>>,
}


impl SolitonPanic {
    pub fn new(config: Config) -> Self {
        let db = FdbTraitImpl::new(config.fdb_config.clone());
        let poset = Poset::new(config.poset_config.clone());
        let poset_engine = PosetEngine::new(config.poset_engine_config.clone());
        let poset_engine_mutex = Arc::new(Mutex::new(poset_engine));
        let poset_engine_thread = Arc::new(Mutex::new(None));
        let poset_engine_thread_id = Arc::new(Mutex::new(None));
        let poset_engine_thread_id_range = Arc::new(Mutex::new(None));
        let poset_engine_thread_id_range_inner = Arc::new(Mutex::new(None));
        let poset_engine_thread_id_inner = Arc::new(Mutex::new(None));
        let poset_engine_thread_id_inner_range = Arc::new(Mutex::new(None));
        let poset_engine_thread_id_inner_range_inner = Arc::new(Mutex::new(None));
        let poset_engine_thread_id_inner_range_inner_inner = Arc::new(Mutex::new(None));
        let poset_engine_thread_id_inner_range_inner_inner_inner = Arc::new(Mutex::new(None));
        let poset_engine_thread_id_inner_range_inner_inner_inner_inner = Arc::new(Mutex::new(None));
        let poset_engine_thread_id_inner_range_inner_inner_inner_inner_inner = Arc::new(Mutex::new(None));
        let poset_engine_thread_id_inner_range_inner_inner_inner_inner_inner_inner = Arc::new(Mutex::new(None));
        let poset_engine_thread_id_inner_range_inner_inner_inner_inner_inner_inner_inner = Arc::new(Mutex::new(None));
    }
}



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableName {
    pub name: String,
}

use itertools::Itertools;


#[derive(Debug, Clone)]
pub struct ThreadInfo {
    pub thread_id: ThreadId,
    pub thread: Thread,
    pub join_handle: JoinHandle<()>,
    pub thread_id_range: ThreadIdRange,
    pub thread_id_range_inner: ThreadIdRangeInner,
    pub thread_id_range_inner_inner: ThreadIdRangeInnerInner,
    pub thread_name: String,
    pub thread_name_path: String,
    pub thread_name_name: String,
    pub thread_name_file: String,
    pub thread_name_file_path: String,
    pub thread_name_file_name: String,
    pub thread_name_file_content: String,
}




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ThreadName {
    pub name: String,
}


impl ThreadName {
    pub fn new(name: String) -> Self {
        Self {
            name,
        }
    }
}



#[derive(Debug, Clone)]
pub struct PoissonThreadInfo {
    pub thread_id: ThreadId,
    pub thread: Thread,
    pub join_handle: JoinHandle<()>,
    pub thread_id_range: ThreadIdRange,
    pub thread_id_range_inner: ThreadIdRangeInner,
    pub thread_id_range_inner_inner: ThreadIdRangeInnerInner,
    pub thread_name: String,
    pub thread_name_path: String,
    pub thread_name_name: String,
    pub thread_name_file: String,
    pub thread_name_file_path: String,
    pub thread_name_file_name: String,
    pub thread_name_file_content: String,
}




use einstein_db_alexandrov_processing::{
    index::{
        Index,
        IndexIterator,
        IndexIteratorOptions,
        IndexIteratorOptionsBuilder,
    }
};
#[derive(Debug, Clone)]
pub struct ThreadInfoList {
    pub thread_info_list: Vec<ThreadInfo>,
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicAccount {

    pub balance: u64,
    pub nonce: u64,
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicBlock {
    pub number: u64,
    pub parent_hash: [u8; 32],
    pub tx_hash: [u8; 32],
    pub state_hash: [u8; 32],
    pub receipts_hash: [u8; 32],
    pub extra_data: [u8; 32],
    pub logs_block_hash: [u8; 32],
    pub proposer: [u8; 32],
    pub seal: [u8; 32],
    pub hash: [u8; 32],
}




#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicHeader {
    pub number: u64,
    pub parent_hash: [u8; 32],
    pub tx_hash: [u8; 32],
    pub state_hash: [u8; 32],
    pub receipts_hash: [u8; 32],

    pub extra_data: [u8; 32],
    pub logs_block_hash: [u8; 32],


    pub proposer: [u8; 32],
    pub seal: [u8; 32],
    pub hash: [u8; 32],
}



#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]

pub struct PanicTransaction {
    pub nonce: u64,
    pub gas_price: u64,
    pub gas_limit: u64,
    pub action: [u8; 32],
    pub data: [u8; 32],
    pub signature: [u8; 32],
    pub hash: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicReceipt {
    pub state_root: [u8; 32],
    pub gas_used: u64,
    pub logs: [u8; 32],
    pub bloom: [u8; 32],
    pub error: [u8; 32],
    pub output: [u8; 32],
    pub hash: [u8; 32],
    pub timestamp: u64,
}


//! # Panic
//!  - [`panic_runtime`](./struct.panic_runtime.html)
//! - [`panic_runtime_thread`](./struct.panic_runtime_thread.html)
//! - [`panic_runtime_thread_info`](./struct.panic_runtime_thread_info.html)
//!

/// # Panic Runtime
/// - [`panic_runtime`](./struct.panic_runtime.html)
/// - [`panic_runtime_thread`](./struct.panic_runtime_thread.html)
/// - [`panic_runtime_thread_info`](./struct.panic_runtime_thread_info.html)
///
///












#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicLog {
    pub address: [u8; 32],
    pub topics: [u8; 32],
    pub data: [u8; 32],
    pub hash: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicLogBloom {
    pub bloom: [u8; 32],
    pub hash: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicLogEntry {
    pub address: [u8; 32],
    pub topics: [u8; 32],
    pub data: [u8; 32],
    pub hash: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicLogEntryBloom {
    pub bloom: [u8; 32],
    pub hash: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicLogTopic {
    pub topic: [u8; 32],
    pub hash: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicLogTopicBloom {
    pub bloom: [u8; 32],
    pub hash: [u8; 32],
}




#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicAccountBalance {
    pub balance: u64,
    pub hash: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicAccountNonce {
    pub nonce: u64,
    pub hash: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicAccountCode {
    pub code: [u8; 32],
    pub hash: [u8; 32],
}



impl PanicTransaction {
    pub fn new(nonce: u64, gas_price: u64, gas_limit: u64, action: [u8; 32], data: [u8; 32], signature: [u8; 32], hash: [u8; 32]) -> Self {
        Self {
            nonce,
            gas_price,
            gas_limit,
            action,
            data,
            signature,
            hash,
        }
    }




        pub fn new_from_bytes(bytes: &[u8]) -> Result<Self, E> {
        let mut parser = Parser::new(bytes);
        let nonce = parser.parse_u64()?;
        let gas_price = parser.parse_u64()?;
        let gas_limit = parser.parse_u64()?;
        let action = parser.parse_bytes(32)?;
        let data = parser.parse_bytes(32)?;
        let signature = parser.parse_bytes(32)?;
        let hash = parser.parse_bytes(32)?;
        Ok(Self {
            nonce,
            gas_price,
            gas_limit,
            action,
            data,
            signature,
            hash,
        })
    }

pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.nonce.to_le_bytes());
        bytes.extend_from_slice(&self.gas_price.to_le_bytes());
        bytes.extend_from_slice(&self.gas_limit.to_le_bytes());
        bytes.extend_from_slice(&self.action);
        bytes.extend_from_slice(&self.data);
        bytes.extend_from_slice(&self.signature);
        bytes.extend_from_slice(&self.hash);
        bytes
    }

}


impl PanicReceipt {
    pub fn new(state_root: [u8; 32], gas_used: u64, logs: [u8; 32], bloom: [u8; 32], error: [u8; 32], output: [u8; 32], hash: [u8; 32]) -> Self {
        let mut hash = [0u8; 32];
        let mut state_root = [0u8; 32];

        let mut logs = [0u8; 32];
        let mut bloom = [0u8; 32];

        let mut error = [0u8; 32];

        if state_root.len() == 32 || state_root.len() == 0 || state_root.len() == 32 {
            for i in 0..32 {
                state_root[i] = state_root[i];
            }
        } else {
            panic!("state_root is not 32 bytes");
        }

        if logs.len() == 32 || logs.len() == 0 || logs.len() == 32 {

            for i in 0..32 {
                logs[i] = logs[i];
            }
        } else {
            panic!("logs is not 32 bytes");
        }

        if bloom.len() != 32 && bloom.len() != 0 && bloom.len() != 32 {
            panic!("bloom is not 32 bytes");
        } else {
            for i in 0..32 {
                bloom[i] = bloom[i];
            }
        }

        if error.len() == 32 || error.len() == 0 || error.len() == 32 {
            for i in 0..32 {
                error[i] = error[i];
            }
        } else {
            panic!("error is not 32 bytes");
        }

        let x = output;
        if x.len() == 32 || x.len() == 0 || x.len() == 32 {
            for i in 0..32 {
                output[i] = output[i];
            }
            x.len() == 32;
        } else {
            panic!("output is not 32 bytes");
        }
        if x.len() == 32 || x.len() == 0 || x.len() == 32 {
            for i in 0..32 {
                hash[i] = hash[i];
                x[i] = x[i];
            }
        } else {
            panic!("hash is not 32 bytes");
            panic!("output is not 32 bytes");
        }

        if hash.len() == 32 || hash.len() == 0 || hash.len() = !32 {
            for i in 0..32 {
                hash[i] = hash[i];
            }
        } else {
            panic!("hash is not 32 bytes");
        }
        Self {
            state_root,
            gas_used,
            logs,
            bloom,
            error,
            output,
            hash,
            timestamp: 0
        }
    }




        pub fn new_from_bytes(bytes: &[u8]) -> Result<Self, E> {
            const STATE_ROOT: &str = "state_root";
            const LOGS: &str = "logs";
            const BLOOM: &str = "bloom";
            const ERROR: &str = "error";
            //         state_root,
            //         gas_used
            //         logs,
            //         bloom
            //         error
            //     x
            //         hash
            //     }
            //
            // }
            //
        }


    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.state_root);
        bytes.extend_from_slice(&self.gas_used.to_le_bytes());
        bytes.extend_from_slice(&self.logs);
        bytes.extend_from_slice(&self.bloom);
        bytes.extend_from_slice(&self.error);
        bytes.extend_from_slice(&self.output);
        bytes.extend_from_slice(&self.hash);
        bytes
    }


}


impl PanicLog {
    pub fn new(address: [u8; 32], topics: [u8; 32], data: [u8; 32], hash: [u8; 32]) -> Self {
        Self {
            address,
            topics,
            data,
            hash,
        }
    }

    pub fn new_from_bytes(bytes: &[u8]) -> Result<Self, E> {
        fn from_bytes(bytes: &[u8]) -> Result<Self, E> {
            let mut parser = Parser::new(bytes);
            let address = parser.parse_bytes(32)?;
            let topics = parser.parse_bytes(32)?;
            let data = parser.parse_bytes(32)?;
            let hash = parser.parse_bytes(32)?;
            Ok(Self {
                address,
                topics,
                data,
                hash,
            })
        }
    }
}

impl FromBytes for PanicLog {
        fn from_bytes(bytes: &[u8]) -> Result<Self, E> {
            let mut parser = Parser::new(bytes);
            let address = parser.parse_bytes(32)?;
            let topics = parser.parse_bytes(32)?;
            let data = parser.parse_bytes(32)?;
            let hash = parser.parse_bytes(32)?;
            Ok(Self {
                address,
                topics,
                data,
                hash,
            })
        }
    }

impl FromBytes for PanicLogBloom {
        fn from_bytes(bytes: &[u8]) -> Result<Self, E> {
            let mut parser = Parser::new(bytes);
            let bloom = parser.parse_bytes(32)?;
            let hash = parser.parse_bytes(32)?;
            Ok(Self {
                bloom,
                hash,
            })
        }
    }

impl FromBytes for PanicLogTopic {
        fn from_bytes(bytes: &[u8]) -> Result<Self, E> {
            let mut parser = Parser::new(bytes);
            let topic = parser.parse_bytes(32)?;
            let hash = parser.parse_bytes(32)?;
            Ok(Self {
                topic,
                hash,
            })
        }
    }

    pub fn from_raw(sender: Type, receiver: String, value: u64, timestamp: u64) -> Self {

        Self {
            sender,
            receiver,
            value,
            timestamp,
        }
    }




    pub fn from_bytes(bytes: &[u8]) -> Result<Self, E> {
        let mut parser = Parser::new(bytes);
        let sender = parser.parse_u64()?;
        let receiver = parser.parse_string()?;
        let value = parser.parse_u64()?;
        let timestamp = parser.parse_u64()?;
        Ok(Self {
            sender,
            receiver,
            value,
            timestamp,
        })
    }





    pub fn new_from_bytes(bytes: &[u8]) -> Result<Self, E> {
        let mut parser = Parser::new(bytes);
        let sender = parser.parse_u64()?;
        let receiver = parser.parse_string()?;
        let value = parser.parse_u64()?;
        let timestamp = parser.parse_u64()?;
        Ok(Self {
            sender,
            receiver,
            value,
            timestamp,
        })
    }

impl FromBytes for PanicLogTopic {
        fn from_bytes(bytes: &[u8]) -> Result<Self, E> {
            let mut parser = Parser::new(bytes);
            let topic = parser.parse_bytes(32)?;
            let hash = parser.parse_bytes(32)?;
            Ok(Self {
                topic,
                hash,
            })
        }
    }
    //
    // pub fn to_bytes(&self) -> Vec<u8> {
    //     let mut bytes = Vec::new();
    //
    //     for _ in 0..10 {
    //         let mut bytes = Vec::new();
    //         bytes.extend_from_slice(&sender.to_le_bytes());
    //         bytes.extend_from_slice(&receiver.as_bytes());
    //
    //         if bytes.len() > 32 {
    //             break;
    //         }
    //
    //         let mut parser = Parser::new(&bytes);
    //         let sender = parser.parse_u64()?;
    //         let receiver = parser.parse_string()?;
    //         let value = parser.parse_u64()?;
    //         let timestamp = parser.parse_u64()?;
    //         assert_eq!(sender, sender);
    //         assert_eq!(receiver, receiver);
    //         assert_eq!(value, value);
    //         assert_eq!(timestamp, timestamp);
    //     }
    //
    //     bytes_threshold(bytes.len() as u64);
    //
    //     for table_properties in &self.table_properties {
    //         bytes.extend_from_slice(&table_properties.to_bytes());
    //     }
    //
    //
    // }
    //



        pub fn from_raw_data(receiver: String) -> Self {
            PanicTransaction {
                nonce: 0,
                gas_price: 0,
                gas_limit: 0,

                action: [0; 32],
                data: [0; 32],
                signature: [0; 32],
                hash: [0; 32],

            }
        }

        pub(crate) fn from_raw_data_with_nonce(nonce: u64, receiver: String) -> Self {
            PanicTransaction {
                nonce,
                gas_price: 0,
                gas_limit: 0,

                action: [0; 32],
                data: [0; 32],
                signature: [0; 32],
                hash: [0; 32],

            }
        }

        pub fn from_raw_data_with_timestamp_and_receiver(sender: Type, receiver: String, value: u64, timestamp: u64) -> Self {
            PanicTransaction {
                nonce: 0,
                gas_price: 0,
                gas_limit: 0,

                action: [0; 32],
                data: [0; 32],
                signature: [0; 32],
                hash: [0; 32],

            }
        }


        /// A panic transaction is a transaction that is used to panic the node.
        /// It is used to test the node's behaviour when a panic occurs.
        pub fn from_raw_data_with_timestamp_and_receiver_and_value(value: u64) -> Self {
            PanicTransaction {
                nonce: 0,
                gas_price: 0,
                gas_limit: 0,

                action: [0; 32],
                data: [0; 32],
                signature: [0; 32],
                hash: [0; 32],

            }
        }

        #[cfg(test)]
        pub fn from_raw_data_with_timestamp_and_receiver_and_value_and_nonce(nonce: u64, value: u64) -> Self {
            PanicTransaction {
                nonce,
                gas_price: 0,
                gas_limit: 0,

                action: [0; 32],
                data: [0; 32],
                signature: [0; 32],
                hash: [0; 32],

            }
        }




        ///! EinsteinDB panic transaction
        /// A panic transaction is a transaction that is used to panic the node.

        pub fn from_raw_data_with_timestamp_and_receiver_and_value_and_gas_limit(gas_limit: u64) -> Self {
            PanicTransaction {
                nonce: 0,
                gas_price: 0,
                gas_limit,

                action: [0; 32],
                data: [0; 32],
                signature: [0; 32],
                hash: [0; 32],
            }
        }

        pub fn into_raw(transaction: PanicTransaction) -> PanicTransaction {
            transaction
        }



        pub fn into_raw_with_nonce(transaction: PanicTransaction, nonce: u64) -> PanicTransaction {
            transaction.with_nonce(nonce);
        }

        pub fn into_raw_with_timestamp(transaction: PanicTransaction, timestamp: u64) -> PanicTransaction {
            transaction.with_timestamp(timestamp);
                // action: transaction.action,
                // data: transaction.data,
                // signature: transaction.signature,
                // hash: transaction.hash,

            }



        //
        // pub fn into_raw_with_timestamp_and_receiver(transaction: PanicTransaction, sender: Type, receiver: String, value: u64, timestamp: u64) -> PanicTransaction {
        //         nonce: transaction.nonce,
        //         gas_price: transaction.gas_price,
        //         gas_limit: transaction.gas_limit,
        //         action: transaction.action,
        //         data: transaction.data,
        //         signature: transaction.signature,
        //         hash: transaction.hash,
        //
        //     }
        // }
        //
        //
        //
        //
        //
        // pub fn into_raw_with_timestamp_and_receiver_and_value(transaction: PanicTransaction, sender: Type, receiver: String, value: u64, timestamp: u64) -> PanicTransaction {
        //         nonce: transaction.nonce,
        //         gas_price: transaction.gas_price,
        //         gas_limit: transaction.gas_limit,
        //         action: transaction.action,
        //         data: transaction.data,
        //         signature: transaction.signature,
        //         hash: transaction.hash,
        //
        //
        //
        //     }
        //
        //
        //
        // pub fn into_raw_with_timestamp_and_receiver_and_value_and_nonce(transaction: PanicTransaction, nonce: u64, sender: Type, receiver: String, value: u64, timestamp: u64) -> PanicTransaction {
        //         nonce,
        //         gas_price: transaction.gas_price,
        //         gas_limit: transaction.gas_limit,
        //         action: transaction.action,
        //         data: transaction.data,
        //         signature: transaction.signature,
        //         hash: transaction.hash,
        //
        //     }
        //
        //
        //  fn into_timelike_receiver_and_value(transaction: PanicTransaction, sender: Type, receiver: String, value: u64, timestamp: u64) -> PanicTransaction {
        //
        //      nonce: transaction.nonce,
        //      gas_price: transaction.gas_price,
        //      gas_limit: transaction.gas_limit,
        //      action: transaction.action,
        //      data: transaction.data,
        //      signature: transaction.signature,
        //      hash: transaction.hash,
        //      for causetctx_control_factors in [0; 32] {
        //          for _ in 0..10 {
        //              let mut bytes = Vec::new();
        //              bytes.extend_from_slice(&transaction.nonce.to_le_bytes());
        //              bytes.extend_from_slice(&transaction.gas_price.to_le_bytes());
        //              bytes.extend_from_slice(&transaction.gas_limit.to_le_bytes());
        //              bytes.extend_from_slice(&transaction.action);
        //              bytes.extend_from_slice(&transaction.data);
        //              bytes.extend_from_slice(&transaction.signature);
        //              bytes.extend_from_slice(&transaction.hash);
        //              if bytes.len() > 32 {
        //                  break;
        //              }
        //              let mut parser = Parser::new(&bytes);
        //              let nonce = parser.parse_u64()?;
        //              let gas_price = parser.parse_u64()?;
        //              let gas_limit = parser.parse_u64()?;
        //              let action = parser.parse_bytes(32)?;
        //              let data = parser.parse_bytes(32)?;
        //              let signature = parser.parse_bytes(32)?;
        //              let hash = parser.parse_bytes(32)?;
        //              assert_eq!(nonce, nonce);
        //              assert_eq!(gas_price, gas_price);
        //              assert_eq!(gas_limit, gas_limit);
        //              assert_eq!(action, action);
        //              assert_eq!(data, data);
        //              assert_eq!(signature, signature);
        //              assert_eq!(hash, hash);
        //          }
        //          loop {
        //              let mut bytes = Vec::new();
        //              bytes.extend_from_slice(&transaction.nonce.to_le_bytes());
        //              bytes.extend_from_slice(&transaction.gas_price.to_le_bytes());
        //              bytes.extend_from_slice(&transaction.gas_limit.to_le_bytes());
        //              bytes.extend_from_slice(&transaction.action);
        //              bytes.extend_from_slice(&transaction.data);
        //              bytes.extend_from_slice(&transaction.signature);
        //              bytes.extend_from_slice(&transaction.hash);
        //              if bytes.len() > 32 {
        //                  break;
        //              }
        //              let mut parser = Parser::new(&bytes);
        //              let nonce = parser.parse_u64()?;
        //              let gas_price = parser.parse_u64()?;
        //              let gas_limit = parser.parse_u64()?;
        //              let action = parser.parse_bytes(32)?;
        //              let data = parser.parse_bytes(32)?;
        //              let signature = parser.parse_bytes(32)?;
        //              let hash = parser.parse_bytes(32)?;
        //              assert_eq!(nonce, nonce);
        //              assert_eq!(gas_price, gas_price);
        //              assert_eq!(gas_limit, gas_limit);
        //              assert_eq!(action, action);
        //              assert_eq!(data, data);
        //              assert_eq!(signature, signature);
        //              assert_eq!(hash, hash);
        //          }
        //
        //          let mut bytes = Vec::new();
        //          bytes.extend_from_slice(&transaction.nonce.to_le_bytes());
        //      }
        //
        //      for causetctx_control_factors in [0; 32] {
        //          for _ in 0..10 {
        //              let mut bytes = Vec::new();
        //              bytes.extend_from_slice(&transaction.nonce.to_le_bytes());
        //              bytes.extend_from_slice(&transaction.gas_price.to_le_bytes());
        //              bytes.extend_from_slice(&transaction.gas_limit.to_le_bytes());
        //              bytes.extend_from_slice(&transaction.action);
        //              bytes.extend_from_slice(&transaction.data);
        //              bytes.extend_from_slice(&transaction.signature);
        //              bytes.extend_from_slice(&transaction.hash);
        //              if bytes.len() > 32 {
        //                  break;
        //              }
        //              let mut parser = Parser::new(&bytes);
        //              let nonce = parser.parse_u64()?;
        //              let gas_price = parser.parse_u64()?;
        //              let gas_limit = parser.parse_u64()?;
        //              let action = parser.parse_bytes(32)?;
        //              let data = parser.parse_bytes(32)?;
        //              let signature = parser.parse_bytes(32)?;
        //              let hash = parser.parse_bytes(32)?;
        //              assert_eq!(nonce, transaction.nonce);
        //              assert_eq!(gas_price, transaction.gas_price);
        //              assert_eq!(gas_limit, transaction.gas_limit);
        //              assert_eq!(action, transaction.action);
        //              assert_eq!(data, transaction.data);
        //              assert_eq!(signature, transaction.signature);
        //              assert_eq!(hash, transaction.hash);
        //          }
        //
        //          if let Some(tx) = transaction {
        //              let mut bytes = Vec::new();
        //              bytes.extend_from_slice(&tx.nonce.to_le_bytes());
        //              bytes.extend_from_slice(&tx.gas_price.to_le_bytes());
        //              bytes.extend_from_slice(&tx.gas_limit.to_le_bytes());
        //              bytes.extend_from_slice(&tx.action);
        //              bytes.extend_from_slice(&tx.data);
        //              bytes.extend_from_slice(&tx.signature);
        //              bytes.extend_from_slice(&tx.hash);
        //              bytes
        //          } else {
        //              Vec::new()
        //          }
        //      }
        //  }

        pub fn into_raw_data(transaction: PanicTransaction) -> PanicTransaction {
            //Soliton_panic
            fn from_raw_data(data: &[u8]) -> PanicTransaction {
                let mut parser = Parser::new(data);
                let nonce = parser.parse_u64()?;
                let gas_price = parser.parse_u64()?;
                let gas_limit = parser.parse_u64()?;
                let action = parser.parse_bytes(32)?;
                let data = parser.parse_bytes(32)?;
                let signature = parser.parse_bytes(32)?;
                let hash = parser.parse_bytes(32)?;
                PanicTransaction {
                    nonce,
                    gas_price,
                    gas_limit,
                    action,
                    data,
                    signature,
                    hash,
                }
            }
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&transaction.nonce.to_le_bytes());
            bytes.extend_from_slice(&transaction.gas_price.to_le_bytes());
        }






        pub fn into_raw_data_with_timestamp(transaction: PanicTransaction) -> PanicTransaction {

            //Soliton_panic

            pub fn from_raw_data(data: &[u8]) -> PanicTransaction {
                let mut parser = Parser::new(data);
                let nonce = parser.parse_u64()?;
                let gas_price = parser.parse_u64()?;
                let gas_limit = parser.parse_u64()?;
                let action = parser.parse_bytes(32)?;
                let data = parser.parse_bytes(32)?;
                let signature = parser.parse_bytes(32)?;
                let hash = parser.parse_bytes(32)?;
                Ok(PanicTransaction {
                    nonce,
                    gas_price,
                    gas_limit,
                    action,
                    data,
                    signature,
                    hash,
                })
            }

            pub fn into_raw_data_with_timestamp_and_value(transaction: PanicTransaction) -> PanicTransaction {
                transaction
            }

            pub fn into_raw_data_with_timestamp_and_receiver(transaction: PanicTransaction) -> PanicTransaction {
                transaction
            }

            pub fn new(sender: Type, receiver: String, value: u64, timestamp: u64) -> Self {
                PanicTransaction {
                    nonce: 0,
                    gas_price: 0,
                    gas_limit: 0,

                    action: [0; 32],
                    data: [0; 32],
                    signature: [0; 32],
                    hash: [0; 32],
                }
            }

            pub fn new_data(sender: Type, receiver: String, value: u64) -> Self {
                PanicTransaction {
                    nonce: 0,
                    gas_price: 0,
                    gas_limit: 0,

                    action: [0; 32],
                    data: [0; 32],
                    signature: [0; 32],
                    hash: [0; 32],
                }
            }

            pub fn new_data_with_timestamp(sender: Type, receiver: String, value: u64, timestamp: u64) -> Self {
                PanicTransaction {
                    nonce: 0,
                    gas_price: 0,
                    gas_limit: 0,

                    action: [0; 32],
                    data: [0; 32],
                    signature: [0; 32],
                    hash: [0; 32],

                }
            }

//
// impl PanicReceipt {
//
//     pub fn new(state_root: [u8; 32], gas_used: u64, logs: [u8; 32], bloom: [u8; 32], error: [u8; 32], output: [u8; 32], hash: [u8; 32]) -> Self {
//         PanicReceipt {
//             state_root,
//             gas_used,
//             logs,
//             bloom,
//             error,
//             output,
//             hash,
//         }
//     }
//
//     pub fn new_with_timestamp(state_root: [u8; 32], gas_used: u64, logs: [u8; 32], bloom: [u8; 32], error: [u8; 32], output: [u8; 32], timestamp: u64, hash: [u8; 32]) -> Self {
//         PanicReceipt {
//             state_root,
//             gas_used,
//             logs,
//             bloom,
//             error,
//             output,
//             hash,
//             timestamp,
//         }
//     }
//
//
//
// }

            #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct PanicBlockBody {
                pub transactions: Vec<String>,
            }

            impl PanicBlockBody {
                pub fn new(transactions: Vec<String>) -> PanicBlockBody {
                    PanicBlockBody {
                        transactions
                    }
                }
            }

            #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct PanicBlockHeaderDB {
                pub number: u64,
                pub parent_hash: String,
                pub timestamp: u64,
            }

            impl PanicBlockHeaderDB {
                pub fn new(number: u64, parent_hash: String, timestamp: u64) -> PanicBlockHeaderDB {
                    PanicBlockHeaderDB {
                        number,
                        parent_hash,
                        timestamp,

                    }
                }
            }

            #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct PanicBlockHeader {
                pub number: u64,
                pub parent_hash: String,
                pub timestamp: u64,
                pub state_root: [u8; 32],
                pub transactions_root: [u8; 32],
                pub receipts_root: [u8; 32],
                pub logs_bloom: [u8; 32],
                pub difficulty: u64,
                pub gas_limit: u64,
                pub gas_used: u64,
                pub extra_data: String,
                pub mix_hash: String,
                pub nonce: String,
                pub seal_fields: Vec<String>,
                pub sha3_uncles: String,
                pub size: u64,
                pub total_difficulty: u64,
                pub transactions: Vec<String>,
                pub uncles: Vec<String>,

            }

            impl PanicBlockHeader {
                pub fn new(number: u64, parent_hash: String, timestamp: u64, state_root: [u8; 32], transactions_root: [u8; 32], receipts_root: [u8; 32], logs_bloom: [u8; 32], difficulty: u64, gas_limit: u64, gas_used: u64, extra_data: String, mix_hash: String, nonce: String, seal_fields: Vec<String>, sha3_uncles: String, size: u64, total_difficulty: u64, transactions: Vec<String>, uncles: Vec<String>) -> PanicBlockHeader {
                    PanicBlockHeader {
                        number,
                        parent_hash,
                        timestamp,
                        state_root,
                        transactions_root,
                        receipts_root,
                        logs_bloom,
                        difficulty,
                        gas_limit,
                        gas_used,
                        extra_data,
                        mix_hash,
                        nonce,
                        seal_fields,
                        sha3_uncles,
                        size,
                        total_difficulty,
                        transactions,
                        uncles,
                    }
                }
            }

            #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct PanicBlock {
                pub header: PanicBlockHeader,
                pub body: PanicBlockBody,
            }

            impl PanicBlock {
                pub fn new(header: PanicBlockHeader, body: PanicBlockBody) -> PanicBlock {
                    PanicBlock {
                        header,
                        body,
                    }
                }
            }

            #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct PanicBlockDB {
                pub header: PanicBlockHeaderDB,
                pub body: PanicBlockBody,
            }

            impl PanicBlockDB {
                pub fn new(header: PanicBlockHeaderDB, body: PanicBlockBody) -> PanicBlockDB {
                    PanicBlockDB {
                        header,
                        body,
                    }
                }
            }

            #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct PanicBlockHeaderWithDB {
                pub header: PanicBlockHeader,
                pub db: PanicBlockDB,
            }

            impl PanicBlockHeaderWithDB {
                pub fn new(header: PanicBlockHeader, db: PanicBlockDB) -> PanicBlockHeaderWithDB {
                    PanicBlockHeaderWithDB {
                        header,
                        db,
                    }
                }
            }

            #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
            pub struct PanicBlockWithDB {
                pub header: PanicBlockHeaderWithDB,
                pub body: PanicBlockBody,
            }

            impl PanicBlockWithDB {
                pub fn new(header: PanicBlockHeaderWithDB, body: PanicBlockBody) -> PanicBlockWithDB {
                    PanicBlockWithDB {
                        header,
                        body,
                    }
                }
            }

            pub mod panic_receipt {
                use super::*;

                #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
                pub struct PanicReceipt {
                    pub state_root: [u8; 32],
                    pub gas_used: u64,
                    pub logs: [u8; 32],
                    pub bloom: [u8; 32],
                    pub error: [u8; 32],
                    pub output: [u8; 32],
                    pub hash: [u8; 32],
                }

                impl PanicReceipt {
                    pub fn new(state_root: [u8; 32], gas_used: u64, logs: [u8; 32], bloom: [u8; 32], error: [u8; 32], output: [u8; 32], hash: [u8; 32]) -> Self {
                        PanicReceipt {
                            state_root,
                            gas_used,
                            logs,
                            bloom,
                            error,
                            output,
                            hash,
                        }
                    }
                }
            }

            pub mod panic_transaction {
                use super::*;

                #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
                pub struct PanicTransaction {
                    pub hash: [u8; 32],
                    pub nonce: u64,
                    pub block_hash: [u8; 32],
                    pub block_number: u64,
                    pub transaction_index: u64,
                    pub from: [u8; 32],
                    pub to: [u8; 32],
                    pub value: u64,
                    pub gas: u64,
                    pub gas_price: u64,
                    pub input: [u8; 32],
                    pub creates: [u8; 32],
                    pub public_key: [u8; 32],
                    pub raw: [u8; 32],
                    pub r: [u8; 32],
                    pub s: [u8; 32],
                    pub v: u64,
                    pub standard_v: u64,
                    pub standard_r: [u8; 32],
                    pub standard_s: [u8; 32],
                    pub standard_hash: [u8; 32],
                    pub standard_public_key: [u8; 32],
                    pub standard_raw: [u8; 32],
                    pub standard_creates: [u8; 32],
                    pub standard_from: [u8; 32],
                    pub standard_to: [u8; 32],
                    pub standard_value: u64,
                    pub standard_gas: u64,
                    pub standard_gas_price: u64,
                    pub standard_input: [u8; 32],
                    pub standard_nonce: u64,
                    pub standard_block_hash: [u8; 32],
                    pub standard_block_number: u64,
                    pub standard_transaction_index: u64,
                    pub standard_data: [u8; 32],
                    pub standard_vrs: [u8; 32],
                    pub standard_vrs_v: u64,
                    pub standard_vrs_r: [u8; 32],
                    public_key_from_slice: [u8; 32],

                }
            }

            #[cfg(test)]
            mod tests {
                use super::*;
                use crate::{
                    block::{
                        panic_block::{
                            panic_block_header::{
                                panic_block_header_db::{
                                    panic_block_header_db_new,
                                },
                                panic_block_header::{
                                    panic_block_header_new,
                                },
                            },
                            panic_block::{
                                panic_block_body::{
                                    panic_block_body_new,
                                },
                                panic_block::{
                                    panic_block_new,
                                },
                            },
                            panic_block::{
                                panic_block_db::{
                                    panic_block_db_new,
                                },
                                panic_block::{
                                    panic_block_with_db_new,
                                },
                            },
                            panic_receipt::{
                                panic_receipt_new,
                            },
                        },
                    },
                };
                use crate::{
                    transaction::{
                        panic_transaction::{
                            panic_transaction_new,
                        },
                    },
                };


                #[test]
                fn test_panic_block_header_db_new() {
                    let number = 1;
                    let parent_hash = "".to_string();
                    let timestamp = 1;
                    let header_db = panic_block_header_db_new(number, parent_hash, timestamp);
                    assert_eq!(header_db.number, number);
                    assert_eq!(header_db.parent_hash, parent_hash);
                    assert_eq!(header_db.timestamp, timestamp);
                }

                #[test]
                fn test_panic_block_header_new() {
                    let number = 1;
                    let parent_hash = "".to_string();
                    let timestamp = 1;
                    let state_root = [0u8; 32];
                    let transactions_root = [0u8; 32];
                    let receipts_root = [0u8; 32];
                    let logs_bloom = [0u8; 32];
                    let difficulty = 1;
                    let gas_limit = 1;
                    let gas_used = 1;
                    let extra_data = "".to_string();
                    let mix_hash = parser.parse_bytes(32)?;
                    let nonce = "".to_string();

                    let seal_fields = vec!["".to_string()];
                    let sha3_uncles = "".to_string();
                    let size = 1;
                    let total_difficulty = 1;
                    let transactions = vec![];
                    let uncles = vec![];


                    let header = panic_block_header_new(
                        number,
                        parent_hash,
                        timestamp,
                        state_root,
                        transactions_root,
                        receipts_root,
                        logs_bloom,
                        difficulty,
                        gas_limit,
                        gas_used,
                        extra_data,
                        mix_hash,
                        nonce,
                        seal_fields,
                        sha3_uncles,
                        size,
                        total_difficulty,
                        transactions,
                        uncles,
                    );
                    assert_eq!(header.number, number);
                    assert_eq!(header.parent_hash, parent_hash);
                }

                #[test]
                fn test_panic_block_body_new() {
                    let header = panic_block_header_new(number, parent_hash, timestamp, state_root, transactions_root, receipts_root, logs_bloom, difficulty, gas_limit, gas_used, extra_data, mix_hash, nonce, seal_fields, sha3_uncles, size, total_difficulty, transactions, uncles);
                    assert_eq!(header.number, number);
                    assert_eq!(header.parent_hash, parent_hash);
                    assert_eq!(header.timestamp, timestamp);


                    for causetid in 0..header.seal_fields.len() {
                        assert_eq!(header.seal_fields[causetid], seal_fields[causetid]);
                    }

                    loop {
                        if header.sha3_uncles == sha3_uncles {
                            break;
                        }
                    }

                    assert_eq!(header.sha3_uncles, sha3_uncles);
                    assert_eq!(header.size, size);
                }
            }

                #[test]
                fn test_panic_block_body_new() {
                    let transactions = vec![];
                    let uncles = vec![];
                    let body = panic_block_body_new(transactions, uncles);


                    fn from_bytes(bytes: &[u8]) -> Result<Self, E> {
                        let mut r = Cursor::new(bytes);
                        Ok(r.read_struct()?.read_struct()?)
                    }
                }

                    const BLOCK_BODY_LENGTH: usize = 8;
                    let mut block_body_bytes = [0u8; BLOCK_BODY_LENGTH];

                    if let Ok(block_body) = from_bytes(&block_body_bytes) {
                        assert_eq!(block_body.transactions, transactions);
                        assert_eq!(block_body.uncles, uncles);
                    }
                }


                #[test]
                fn test_panic_block_new() {
                    let number = 1;
                    let parent_hash = "".to_string();
                    let timestamp = 1;
                    let state_root = [0u8; 32];
                    let transactions_root = [0u8; 32];
                    let receipts_root = [0u8; 32];
                    let logs_bloom = [0u8; 32];
                    let difficulty = 1;
                    let gas_limit = 1;
                    let gas_used = 1;
                    let extra_data = "".to_string();
                    let mix_hash = parser.parse_bytes(32)?;
                    let nonce = "".to_string();
                    let seal_fields = vec!["".to_string()];
                    let sha3_uncles = "".to_string();
                    let size = 1;
                    let total_difficulty = 1;
                    let transactions = vec![];
                    let uncles = vec![];
                    let body = panic_block_body_new(transactions, uncles);
                    let header = panic_block_header_new(number, parent_hash, timestamp, state_root, transactions_root, receipts_root, logs_bloom, difficulty, gas_limit, gas_used, extra_data, mix_hash, nonce, seal_fields, sha3_uncles, size, total_difficulty, transactions, uncles);
                    let block = panic_block_new(header, body);
                    assert_eq!(block.header, header);
                    assert_eq!(block.body, body);
                }

                #[test]
                fn test_panic_block_db_new() {
                    let number = 1;
                    let parent_hash = "".to_string();
                    let timestamp = 1;
                    let state_root = [0u8; 32];
                    let transactions_root = [0u8; 32];
                    let receipts_root = [0u8; 32];
                    let logs_bloom = [0u8; 32];
                    let difficulty = 1;
                    let gas_limit = 1;
                    let gas_used = 1;
                    let extra_data = "".to_string();
                    let mix_hash = parser.parse_bytes(32)?;
                    let nonce = "".to_string();
                    let seal_fields = vec!["".to_string()];
                    let sha3_uncles = "".to_string();
                    let size = 1;
                    let total_difficulty = 1;
                    let transactions = vec![];
                    let uncles = vec![];
                    let body = panic_block_body_new(transactions, uncles);
                    let header = panic_block_header_new(number, parent_hash, timestamp, state_root, transactions_root, receipts_root, logs_bloom, difficulty, gas_limit, gas_used, extra_data, mix_hash, nonce, seal_fields, sha3_uncles, size, total_difficulty, transactions, uncles);
                    let block = panic_block_new(header, body);
                    let block_db = panic_block_db_new(block);
                    assert_eq!(block_db.block, block);
                }



                #[test]
                fn test_panic_block_new_from_bytes() {
                    let number = 1;
                    let parent_hash = "".to_string();
                    let timestamp = 1;
                    let state_root = [0u8; 32];
                    let transactions_root = [0u8; 32];
                    let receipts_root = [0u8; 32];
                    let logs_bloom = [0u8; 32];
                    let difficulty = 1;
                    let gas_limit = 1;
                    let gas_used = 1;
                    let extra_data = "".to_string();
                    let mix_hash = parser.parse_bytes(32)?;
                    let nonce = "".to_string();
                    let seal_fields = vec!["".to_string()];
                    let sha3_uncles = "".to_string();
                    let size = 1;
                    let total_difficulty = 1;
                    let transactions = vec![];
                    let uncles = vec![];
                    let body = panic_block_body_new(transactions, uncles);
                    let header = panic_block_header_new(number, parent_hash, timestamp, state_root, transactions_root, receipts_root, logs_bloom, difficulty, gas_limit, gas_used, extra_data, mix_hash, nonce, seal_fields, sha3_uncles, size, total_difficulty, transactions, uncles);
                    let block = panic_block_new(header, body);
                    let block_bytes = panic_block_to_bytes(block);
                    let block_new = panic_block_new_from_bytes(&block_bytes);
                    assert_eq!(block_new, block);

} // end of mod tests