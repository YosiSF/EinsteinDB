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

switch on BlockHashIdType {
    case BlockHashIdType(BlockHashIdType):
        BlockHashIdType(BlockHashIdType),
}


mod datum;
mod error;
mod hash;
mod block;
mod poset;
mod util;


pub use crate::datum::DatumType;
pub use crate::error::Error;
pub use crate::hash::Hashable;
pub use crate::hash::HashableDatumType;









