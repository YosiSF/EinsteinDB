//Copyright (c) 2022 EinsteinDB contributors
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




//! // Set the block's round_elapsed_time
//! block.set_round_elapsed_time(0);
//! 
//! 
//! 
//! // Add the block to the Poset
//! poset.add_block(Arc::new(block));
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
use crate::block::{BlockHeader, BlockHeaderType};
use crate::block::{BlockBody, BlockBodyType};


use crate::block::{BlockHeaderHash, BlockHeaderHashType};
use crate::block::{BlockBodyHash, BlockBodyHashType};
use crate::block::{BlockHash, BlockHashType};
use crate::block::{BlockSignature, BlockSignatureType};


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




impl Block {
    pub fn set_hash(&mut self, hash: BlockHash) {
        self.block_hash = hash;
    }

    pub fn get_hash(&self) -> BlockHash {
        self.block_hash
    }
}











