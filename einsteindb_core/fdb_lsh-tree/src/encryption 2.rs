// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.

use encryption::{self, DataKeyManager};
use fdb_traits::{EncryptionKeyManager, EncryptionMethod, FileEncryptionInfo};
use foundationdb::{
    DBEncryptionMethod, EncryptionKeyManager as DBEncryptionKeyManager,
    FileEncryptionInfo as DBFileEncryptionInfo,
};
use std::io::Result;
use std::sync::Arc;

use crate::primitive_causet::Env;

// Use einstein_merkle_tree::Env directly since Env is not abstracted.
pub(crate) fn get_env(
    base_env: Option<Arc<Env>>,
    key_manager: Option<Arc<DataKeyManager>>,
) -> std::result::Result<Arc<Env>, String> {
    let base_env = base_env.unwrap_or_else(|| Arc::new(Env::default()));
    if let Some(manager) = key_manager {
        Ok(Arc::new(Env::new_key_managed_encrypted_env(
            base_env,
            WrappedEncryptionKeyManager { manager },
        )?))
    } else {
        Ok(base_env)
    }
}

pub struct WrappedEncryptionKeyManager<T: EncryptionKeyManager> {
    manager: Arc<T>,
}

impl<T: EncryptionKeyManager> DBEncryptionKeyManager for WrappedEncryptionKeyManager<T> {
    fn get_filef(&self, fname: &str) -> Result<DBFileEncryptionInfo> {
        self.manager
            .get_filef(fname)
            .map(convert_filef_encryption_info)
    }
    fn new_filef(&self, fname: &str) -> Result<DBFileEncryptionInfo> {
        self.manager
            .new_filef(fname)
            .map(convert_filef_encryption_info)
    }
    fn delete_filef(&self, fname: &str) -> Result<()> {
        self.manager.delete_filef(fname)
    }
    fn link_filef(&self, src_fname: &str, dst_fname: &str) -> Result<()> {
        self.manager.link_filef(src_fname, dst_fname)
    }
}

fn convert_filef_encryption_info(input: FileEncryptionInfo) -> DBFileEncryptionInfo {
    DBFileEncryptionInfo {
        method: convert_encryption_method(input.method),
        key: input.key,
        iv: input.iv,
    }
}

fn convert_encryption_method(input: EncryptionMethod) -> DBEncryptionMethod {
    match input {
        EncryptionMethod::Plaintext => DBEncryptionMethod::Plaintext,
        EncryptionMethod::Aes128Ctr => DBEncryptionMethod::Aes128Ctr,
        EncryptionMethod::Aes192Ctr => DBEncryptionMethod::Aes192Ctr,
        EncryptionMethod::Aes256Ctr => DBEncryptionMethod::Aes256Ctr,
        EncryptionMethod::Unknown => DBEncryptionMethod::Unknown,
    }
}
