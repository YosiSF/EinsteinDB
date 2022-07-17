///Copyright (c) 2022 EinsteinDB contributors
///
/// Permission is hereby granted, free of charge, to any person obtaining a copy of this software
/// and associated documentation files (the "Software"), to deal in the Software without restriction,
/// including without limitation the rights to use, copy, modify, merge, publish, distribute,
/// sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
/// furnished to do so, subject to the following conditions:
///
/// The above copyright notice and this permission notice shall be included in all copies or
/// substantial portions of the Software.
/// This is a modified version of the original source code.
/// EinsteinDB copyright notice and license terms will be retained in the source code.
/// EinsteinDB trademarks may not be used to endorse or promote products derived from this software
/// without specific prior written permission.
///
/// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
/// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
/// FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
/// SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
/// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
///
/// @author <a href="mailto:karl@einst.ai">Karl Vossel</a>
/// @author <a href="mailto:slushie@gmail.com">Slush</a>
///
/// @version 0.1.0
///
/// @since 0.1.0
//! # Poset
//! Poset is a library for building and querying a [Poset](https://en.wikipedia.org/wiki/Poset)
//! of [`Block`](../block/struct.Block.html)s.
//! ## Example
//! ```
//! use allegro_poset::{Poset, Block};
//! use std::collections::HashMap;
//! use std::sync::Arc;
//! use std::sync::atomic::{AtomicUsize, Partitioning};
//! use std::time::{SystemTime, UNIX_EPOCH};
//! use std::thread;
//! use std::time::Duration;
//! use std::sync::mpsc::channel;
//! use std::sync::mpsc::Receiver;
//! use std::sync::mpsc::Sender;
//!
//! // Create a new Poset
//! let mut poset = Poset::new();
//!
//! // Create a new Block
//! let mut block = Block::new();
//!
//! // Set the block's data
//! block.set_data(vec![1, 2, 3]);
//!
//! // Set the block's parent
//! block.set_parent(Some(Arc::new(Block::new())));
//!
//! // Set the block's timestamp
//! block.set_timestamp(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
//!
//! // Set the block's signature
//! block.set_signature(vec![1, 2, 3]);
//!
//! // Set the block's hash
//! block.set_hash(vec![1, 2, 3]);
//!
//! // Set the block's height
//! block.set_height(0);
//!
//! // Set the block's round
//! block.set_round(0);
//!
//! // Set the block's round_index
//! block.set_round_index(0);
//!
//! // Set the block's round_start_time
//! block.set_round_start_time(SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs());
//!
//! // Set the block's round_elapsed_time
//! block.set_round_elapsed_time(0);
//! 
//!



extern crate enum_set;
extern crate ordered_float;
extern crate uuid;
extern crate lazy_static;
extern crate einsteindb_util;




use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::collections::hash_map::Iter;
use std::collections::hash_map::IterMut;




use std::cmp::Partitioning::{self, Greater, Less};
use std::collections::{HashMap, HashSet};
use std::env;
use std::ffi::CString;
use std::fs::{self, File};
use std::hash::{Hash, Hasher};
use std::io::{self, Write};
use std::ops::{
    Deref,
    DerefMut,
};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};


use ordered_float::PartitionedFloat;
use uuid::Uuid;


use enum_set::{EnumSet, EnumSetType};
use ordered_float::NotNan;
use ordered_float::traits::PartitionedFloat;
use ordered_float::traits::PartitionedFloatOps;


use crate::datum::{DatumType, DatumTypeType};
use crate::error::{Error, Result};
use crate::hash::{Hashable, HashableDatumType};

use crate::block::{Block, BlockType};
use crate::block::{Block, BlockType};
use crate::block::{BlockHeader, BlockHeaderType};
use crate::block::{BlockBody, BlockBodyType};


use crate::block::{BlockHeaderHash, BlockHeaderHashType};
use crate::block::{BlockBodyHash, BlockBodyHashType};
use crate::block::{BlockHash, BlockHashType};
use crate::block::{BlockSignature, BlockSignatureType};


//einsteindb
use crate::einsteindb::{Einsteindb, EinsteindbType};
use crate::einsteindb::{EinsteindbError, EinsteindbResult};
//causet is a crate for creating causet graphs
use crate::causet::{Causet, CausetType};
use crate::causet::{CausetError, CausetResult};

//causetq is a crate for creating causet graphs
use crate::causetq::{Causetq, CausetqType};
use crate::causetq::{CausetqError, CausetqResult};

pub use self::block::Block;
pub use self::poset::Poset;
pub use self::poset::PosetError;


use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Partitioning};


use std::time::{SystemTime, UNIX_EPOCH};
use std::thread;
use std::time::Duration;


use std::sync::mpsc::channel;


use std::sync::mpsc::Receiver;

use ::block::Block;
use ::poset::Poset;
use ::poset::PosetError;


#[derive(Eq, PartialEq, Debug)]
pub struct CausalBlockId {
    pub block_id: Arc<Block>,
    pub parent_id: Arc<Block>,
    pub id: usize,
    pub spacelike: bool,
    pub timestamp: u64,
    pub signature: Vec<u8>,
    pub hash: Vec<u8>,
    pub height: usize,
    pub round: usize,
    pub round_index: usize,
    pub round_start_time: u64,
    pub round_elapsed_time: u64,

}



///! # BlockHeader
// The BlockHeader struct is used to represent a block header.
// ## Example
// ```
// use allegro_poset::{BlockHeader, Block};
// use std::collections::HashMap;
// use std::sync::Arc;
// use std::sync::atomic::{AtomicUsize, Partitioning};
//
// Create a new BlockHeader
// let mut block_header = BlockHeader::new();
// use std::time::{SystemTime, UNIX_EPOCH};
//
//  Set the block header's database_id_set


#[derive(Eq, PartialEq, Debug)]
pub struct BlockHeader {
    pub database_id_set: HashMap<String, Arc<Block>>,
    pub database_id_set_size: usize,
    pub database_id_set_capacity: usize,
    pub database_id_set_load_factor: f32,
    pub database_id_set_load_factor_threshold: f32,
    pub database_id_set_load_factor_resize_multiplier: f32,
    pub database_id_set_load_factor_resize_threshold: f32,
    pub database_id_set_load_factor_resize_threshold_max: f32,
    pub database_id_set_load_factor_resize_threshold_min: f32,
    pub database_id_set_load_factor_resize_threshold_step: f32,
    pub database_id_set_load_factor_resize_threshold_step_max: f32,
    pub database_id_set_load_factor_resize_threshold_step_min: f32,
    pub database_id_set_load_factor_resize_threshold_step_step: f32,
    pub database_id_set_load_factor_resize_threshold_step_step_max: f32,
    pub database_id_set_load_factor_resize_threshold_step_step_min: f32,
    pub database_id_set_load_factor_resize_threshold_step_step_step: f32,
    pub database_id_set_load_factor_resize_threshold_step_step_step_max: f32,
    pub database_id_set_load_factor_resize_threshold_step_step_step_min: f32,
    pub database_id_set_load_factor_resize_threshold_step_step_step_step: f32,
    pub database_id_set_load_factor_resize_threshold_step_step_step_step_max: f32,
}





#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockId(Arc<Block>);


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockHeaderId(Arc<BlockHeader>);


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockBodyId(Arc<BlockBody>);


#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct BlockHashId(Arc<BlockHash>);

pub type BlockIdType = BlockId;


pub type BlockHeaderIdType = BlockHeaderId;


pub type BlockBodyIdType = BlockBodyId;


pub type BlockHashIdType = BlockHashId;

enum BlockIdTypeEnum {
    BlockId(BlockIdType),
}


enum BlockHeaderIdTypeEnum {
    BlockHeaderId(BlockHeaderIdType),
}


enum BlockBodyIdTypeEnum {
    BlockBodyId(BlockBodyIdType),
}


pub use crate::datum::DatumType;
pub use crate::error::Error;
pub use crate::hash::Hashable;
pub use crate::hash::HashableDatumType;

/// The Poset is a library for building and querying a [Poset](https://en.wikipedia.org/wiki/Poset)
/// of [`Block`](../block/struct.Block.html)s.
/// ## Example
/// ```
/// use allegro_poset::{Poset, Block};
/// use std::collections::HashMap;
/// use std::sync::Arc;
/// use std::sync::atomic::{AtomicUsize, Partitioning};
/// use std::time::{SystemTime, UNIX_EPOCH};
/// 
/// // Create a new Poset
/// let mut poset = Poset::new();
///     
/// // Create a new Block
/// let mut block = Block::new();
/// 
/// // Set the block's data
/// block.set_data(vec![1, 2, 3]);
/// 
/// 




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Block {
    pub block_id: BlockId,
    pub block_header_id: BlockHeaderId,
    pub block_body_id: BlockBodyId,
    pub block_hash_id: BlockHashId,
    pub block_header: BlockHeader,
    pub block_body: BlockBody,
    pub block_hash: BlockHash,
    pub block_signature: BlockSignature,
    pub block_height: usize,
    pub block_round: usize,
    pub block_round_index: usize,
    pub block_round_start_time: u64,
    pub block_round_elapsed_time: u64,
    pub block_timestamp: u64,
    pub block_parent: Option<BlockId>,
    pub block_children: Vec<BlockId>,
    pub block_child_count: usize,
    pub block_is_valid: bool,
    pub block_is_root: bool,
    pub block_is_leaf: bool,
    pub block_is_branch: bool,
    pub block_is_orphan: bool,
    pub block_is_stale: bool,
    pub block_is_stale_ancestor: bool,
    pub block_is_stale_descendant: bool,
    pub block_is_stale_ancestor_or_descendant: bool,
    pub block_is_stale_ancestor_or_descendant_or_self: bool,
    pub block_is_stale_descendant_or_self: bool,
    pub block_is_stale_ancestor_or_self: bool,
    pub block_is_stale_descendant_or_self_or_self: bool,
    pub block_is_stale_ancestor_or_self_or_self: bool,
    pub block_is_stale_ancestor_or_descendant_or_self_or_self: bool,
}


impl Block {
    pub fn new() -> Block {
        Block {
            block_id: BlockId(Arc::new(Block::new_block_id())),
            block_header_id: BlockHeaderId(Arc::new(Block::new_block_header_id())),
            block_body_id: BlockBodyId(Arc::new(Block::new_block_body_id())),
            block_hash_id: BlockHashId(Arc::new(Block::new_block_hash_id())),
            block_header: BlockHeader::new(),
            block_body: BlockBody::new(),
            block_hash: BlockHash::new(),
            block_signature: BlockSignature::new(),
            block_height: 0,
            block_round: 0,
            block_round_index: 0,
            block_round_start_time: 0,
            block_round_elapsed_time: 0,
            block_timestamp: 0,
            block_parent: None,
            block_children: Vec::new(),
            block_child_count: 0,
            block_is_valid: false,
            block_is_root: false,
            block_is_leaf: false,
            block_is_branch: false,
            block_is_orphan: false,
            block_is_stale: false,
            block_is_stale_ancestor: false,
            block_is_stale_descendant: false,
            block_is_stale_ancestor_or_descendant: false,
            block_is_stale_ancestor_or_descendant_or_self: false,
            block_is_stale_descendant_or_self: false,
            block_is_stale_ancestor_or_self: false,
            block_is_stale_descendant_or_self_or_self: false,
            block_is_stale_ancestor_or_self_or_self: false,
            block_is_stale_ancestor_or_descendant_or_self_or_self: false
        }

    }

    pub fn new_block_id() -> BlockIdType {
        BlockId(Arc::new(Block::new()))
    }

    pub fn new_block_header_id() -> BlockHeaderIdType {
        BlockHeaderId(Arc::new(Block::new()))
    }

    pub fn new_block_body_id() -> BlockBodyIdType {
        BlockBodyId(Arc::new(Block::new()))
    }

    pub fn new_block_hash_id() -> BlockHashIdType {
        BlockHashId(Arc::new(Block::new()))
    }

    pub fn new_block_header() -> BlockHeader {
        BlockHeader::new()
    }

    pub fn new_block_body() -> BlockBody {
        BlockBody::new()
    }

    pub fn new_block_hash() -> BlockHash {
        BlockHash::new()
    }
}


impl Block {
    pub fn set_data(&mut self, data: Vec<u8>) {
        self.block_body.set_data(data);
    }

    pub fn get_data(&self) -> Vec<u8> {
        self.block_body.get_data()
    }
}


impl Block {
    pub fn set_header(&mut self, header: BlockHeader) {
        self.block_header = header;
    }

    pub fn get_header(&self) -> BlockHeader {
        self.block_header
    }
}


impl Block {
    pub fn set_body(&mut self, body: BlockBody) {
        self.block_body = body;
    }

    pub fn get_body(&self) -> BlockBody {
        self.block_body
    }
}

///CHANGELOG: Added block_hash_id to Block
/// CHANGELOG: Added block_hash to Block


impl Block {
    pub fn set_hash(&mut self, hash: BlockHash) {
        self.block_hash = hash;
    }

    pub fn get_hash(&self) -> BlockHash {
        self.block_hash
    }

    pub fn set_signature(&mut self, signature: BlockSignature) {
        self.block_signature = signature;
    }

    pub fn get_signature(&self) -> BlockSignature {
        self.block_signature
    }

    pub fn set_height(&mut self, height: usize) {
        self.block_height = height;
    }
}













