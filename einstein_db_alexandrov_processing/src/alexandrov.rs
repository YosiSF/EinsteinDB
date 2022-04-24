//Copyright 2021 EinsteinDB Project Authors, WHTCORPS INC; EINST.AI -- LICENSED UNDER APACHE 2.0


use crate::error::{Error, Result};
use crate::meta::{Meta, MetaStore};
use crate::storage::{Storage, StorageReader, StorageWriter};
use crate::{EINSTEIN_DB_META_STORE_PATH, EINSTEIN_DB_STORAGE_PATH};
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};


pub struct Alexandrov {
    meta_store: Arc<dyn MetaStore>,
    storage: Arc<dyn Storage>,
}


