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

//! An example EinsteinDB timelike_storage einstein_merkle_tree.
//!
//! This project is intended to serve as a skeleton for other einstein_merkle_tree
//! implementations. It lays out the complex system of einstein_merkle_tree modules and traits
//! in a way that is consistent with other EinsteinMerkleTrees. To create a new einstein_merkle_tree
//! simply copy the entire directory structure and replace all "Panic*" names
//! with your einstein_merkle_tree's own name; then fill in the implementations; remove
//! the allow(unused) attribute;
//! 




//! The `einstein_merkle_tree` module provides the core API for einstein_merkle_tree.
//! It defines the `EinsteinMerkleTree` trait, which is the core interface for
//! einstein_merkle_tree. It also defines the `EinsteinMerkleTreeReader` trait, which is the
//! core interface for einstein_merkle_tree readers.
//! It also defines the `EinsteinMerkleTreeWriter` trait, which is the core interface for
//! einstein_merkle_tree writers.


use std::path::Path;



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


use einstein_db_alexandrov_processing::{
    index::{
        Index,
        IndexIterator,
        IndexIteratorOptions,
        IndexIteratorOptionsBuilder,
    }
};



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
}


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
        Self {
            state_root,
            gas_used,
            logs,
            bloom,
            error,
            output,
            hash,
        }
    }

    pub fn new_from_bytes(bytes: &[u8]) -> Result<Self, E> {
        let mut parser = Parser::new(bytes);
        let state_root = parser.parse_bytes(32)?;
        let gas_used = parser.parse_u64()?;
        let logs = parser.parse_bytes(32)?;
        let bloom = parser.parse_bytes(32)?;
        let error = parser.parse_bytes(32)?;
        let output = parser.parse_bytes(32)?;
        let hash = parser.parse_bytes(32)?;
        Ok(Self {
            state_root,
            gas_used,
            logs,
            bloom,
            error,
            output,
            hash,
        })
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


    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.address);
        bytes.extend_from_slice(&self.topics);
        bytes.extend_from_slice(&self.data);
        bytes.extend_from_slice(&self.hash);
        bytes
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

    pub fn into_raw_data(transaction: PanicTransaction) -> PanicTransaction {
        transaction
    }


    pub fn into_raw_data_with_timestamp(transaction: PanicTransaction) -> PanicTransaction {
        transaction
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

///CHANGELOG: Added uncles field_type: Vec<String>
/// CHANGELOG: Added size field_type: u64x2
/// CHANGELOG: Added total_difficulty field_type: u64x2
/// CHANGELOG: Added seal_fields field_type: Vec<String>






