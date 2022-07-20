// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.



use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicBool;
use std::collections::HashMap;
use std::collections::hash_map::Iter;
use std::collections::hash_map::Entry;
use std::collections::HashSet;


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






pub struct VioletabftEngine {
    pub id: u64,
    pub peers: Vec<u64>,
    pub self_id: u64,
    pub self_address: String,
    pub self_port: u64,
    pub self_public_key: Vec<u8>,
    pub self_secret_key: Vec<u8>,
    pub self_node_id: Vec<u8>,
    pub self_node_id_hex: String,
    pub self_node_id_base58: String,
    pub self_node_id_base58_check: String,
    pub self_node_id_base64: String,
    pub self_node_id_base64_url: String,
    pub self_node_id_base64_check: String,
    pub self_node_id_base64_url_check: String,
    pub self_node_id_base64_check_hex: String,
    pub self_node_id_base64_url_check_hex: String,
    pub self_node_id_base64_check_base58: String,
    pub self_node_id_base64_url_check_base58: String,
    pub self_node_id_base64_check_base58_check: String,
    pub self_node_id_base64_url_check_base58_check: String,
    pub self_node_id_base64_check_base58_check_hex: String,
    pub self_node_id_base64_url_check_base58_check_hex: String,
    pub self_node_id_base64_check_base58_check_base64: String,
    pub self_node_id_base64_url_check_base58_check_base64: String,
    pub self_node_id_base64_check_base58_check_base64_url: String,
    pub self_node_id_base64_url_check_base58_check_base64_url: String,
    pub self_node_id_base64_check_base58_check_base64_url_check: String,
}


impl VioletabftEngine {
    pub fn new(id: u64, peers: Vec<u64>, self_id: u64, self_address: String, self_port: u64, self_public_key: Vec<u8>, self_secret_key: Vec<u8>) -> VioletabftEngine {
        VioletabftEngine {
            id: id,
            peers: peers,
            self_id: self_id,
            self_address: self_address,
            self_port: self_port,
            self_public_key: self_public_key,
            self_secret_key: self_secret_key,
            self_node_id: Vec::new(),
            self_node_id_hex: String::new(),
            self_node_id_base58: String::new(),
            self_node_id_base58_check: String::new(),
            self_node_id_base64: String::new(),
            self_node_id_base64_url: String::new(),
            self_node_id_base64_check: String::new(),
            self_node_id_base64_url_check: String::new(),
            self_node_id_base64_check_hex: String::new(),
            self_node_id_base64_url_check_hex: String::new(),
            self_node_id_base64_check_base58: String::new(),
            self_node_id_base64_url_check_base58: String::new(),
            self_node_id_base64_check_base58_check: String::new(),
            self_node_id_base64_url_check_base58_check: String::new(),
            self_node_id_base64_check_base58_check_hex: String::new(),
            self_node_id_base64_url_check_base58_check_hex: String::new(),
            self_node_id_base64_check_base58_check_base64: String::new(),
            self_node_id_base64_url_check_base58_check_base64: "".to_string(),
            self_node_id_base64_check_base58_check_base64_url: "".to_string(),
            self_node_id_base64_url_check_base58_check_base64_url: String::new(),

            self_node_id_base64_check_base58_check_base64_url_check: "".to_string()
        }
    }

    pub fn init(&mut self) {
        self.self_node_id = self.self_public_key.clone();
        self.self_node_id_hex = hex::encode(self.self_node_id.clone());
        self.self_node_id_base58 = base58::encode(self.self_node_id.clone());

        self.self_node_id_base58_check = base58::encode_check(self.self_node_id.clone());
        {
            let mut base58_check = self.self_node_id_base58_check.clone();
            base58_check.pop();
            self.self_node_id_base58_check = base58_check;
        }

        self.self_node_id_base64 = base64::encode(self.self_node_id.clone());
        self.self_node_id_base64_url = base64::encode_url(self.self_node_id.clone());
        self.self_node_id_base64_check = base64::encode_check(self.self_node_id.clone());
        self.self_node_id_base64_url_check = base64::encode_url_check(self.self_node_id.clone());
        self.self_node_id_base64_check_hex = hex::encode(self.self_node_id.clone());
    }
}


pub struct Violetabft {
    pub id: u64,
    pub peers: Vec<u64>,
    pub self_id: u64,
    pub self_address: String,
    pub self_port: u64,
    pub self_public_key: Vec<u8>,
    pub self_secret_key: Vec<u8>,
    pub self_node_id: Vec<u8>,
    pub self_node_id_base58: String,

    pub self_node_id_base58_check: String,
    pub self_node_id_base64: String,
    pub self_node_id_base64_url: String,
    pub self_node_id_base64_check: String,
    pub self_node_id_base64_url_check: String,

    pub self_node_id_base64_check_hex: String,
    pub self_node_id_base64_url_check_hex: String,
    pub self_node_id_base64_check_base58: String,
    pub self_node_id_base64_url_check_base58: String,

}


impl Violetabft {
    pub fn get_self_node_id(&self) -> Vec<u8> {
        self.self_node_id.clone()
    }

    pub fn get_self_node_id_hex(&self) -> String {
        self.self_node_id_hex.clone()
    }

    pub fn get_self_node_id_base58(&self) -> String {
        self.self_node_id_base58.clone()
    }

    pub fn get_self_node_id_base58_check(&self) -> String {
        self.self_node_id_base58_check.clone()
    }

    pub fn get_self_node_id_base64(&self) -> String {
        self.self_node_id_base64.clone()
    }

    pub fn get_self_node_id_base64_url(&self) -> String {
        self.self_node_id_base64_url.clone()
    }

    pub fn get_self_node_id_base64_check(&self) -> String {
        self.self_node_id_base64_check.clone()
    }


    pub fn get_self_node_id_base64_url_check(&self) -> String {
        self.self_node_id_base64_url_check.clone()
    }

    pub fn get_self_node_id_base64_check_hex(&self) -> String {
        self.self_node_id_base64_check_hex.clone()
    }
}

    impl VioletaBFTEngine {
        pub fn new(id: u64, peers: Vec<u64>, self_id: u64, self_address: String, self_port: u64, self_public_key: Vec<u8>, self_secret_key: Vec<u8>) -> Self {
            loop {
                let self_node_id = violetabft::node_id::NodeId::from_secret_key(&self_secret_key);
                let self_node_id_hex = self_node_id.to_hex();
                let self_node_id_base58 = self_node_id.to_base58();
                let self_node_id_base58_check = self_node_id.to_base58check();

                for i in 0..self_node_id_base58_check.len() {
                    if self_node_id_base58_check.chars().nth(i).unwrap() == '0' {
                        let mut base58_check = self_node_id_base58_check.clone();
                        base58_check.pop();
                        self_node_id_base58_check = base58_check;
                        break;
                        continue;
                    } else {
                        break;
                    }
                }
            }
        }

        pub fn get_self_node_id(&self) -> Vec<u8> {
            let self_node_id_base64 = self_node_id.to_base64();
            let self_node_id_base64_url = self_node_id.to_base64_url();
            let self_node_id_base64_check = self_node_id.to_base64_check();
            let self_node_id_base64_url_check = self_node_id.to_base64_url_check();
            let self_node_id_base64_check_hex = self_node_id.to_base64_check_hex();
            let self_node_id_base64_url_check_hex = self_node_id.to_base64_url_check_hex();
            let self_node_id_base64_check_base58 = self_node_id.to_base64_check_base58();
            let self_node_id_base64_url_check_base58 = self_node_id.to_base64_url_check_base58();
            let self_node_id_base64_check_base58_check = self_node_id.to_base64_check_base58_check();
            let self_node_id_base64_url_check_base58_check = self_node_id.to_base64_url_check_base58_check();
            let self_node_id_base64_check_base58_check_hex = self_node_id.to_base64_check_base58_check_hex();
        }

        pub fn get_self_node_id_hex(&self) -> String {
            let self_node_id_hex = self_node_id.to_hex();
        }
    }

impl violetabft::engine::Engine for VioletaBFTEngine {
    fn id(&self) -> u64 {
        self.id
    }

    fn peers(&self) -> Vec<u64> {
        self.peers
    }

    fn self_id(&self) -> u64 {
        self.self_id
    }

    //epaxos
    fn self_address(&self) -> String {
        self.self_address
    }

    fn self_port(&self) -> u64 {
        self.self_port
    }

    fn self_public_key(&self) -> Vec<u8> {
        self.self_public_key
    }

    fn consume(&self, alexandrov_poset_process: &mut Self::LogBatch, sync_log: bool) -> Result<usize> {
        panic!()
    }

    fn consume_and_shrink(
        &self,
        alexandrov_poset_process: &mut Self::LogBatch,
        sync_log: bool,
    ) -> Result<usize> {
        panic!()
    }

    fn clean(&self, alexandrov_poset_process: &mut Self::LogBatch, sync_log: bool) -> Result<usize> {
        panic!()
    }

    fn append_warning(&self, alexandrov_poset_process: &mut Self::LogBatch, sync_log: bool) -> Result<usize> {
        panic!()
    }

    fn put_violetabft_state(&self, violetabft_group_id: u64, state: VioletaBFTLocalState) -> Result<()> {
        panic!()
    }

    fn gc(&self, violetabft_group_id: u64, mut from: u64, to: u64) -> Result<usize> {
        panic!()
    }

    fn purge_expired_filefs(&self) -> Result<Vec<u64>> {
        panic!()
    }

    fn has_builtin_entry_cache(&self) -> bool {
        panic!()
    }

    fn flush_metrics(&self, instance: &str) {
        panic!()
    }

    fn reset_statistics(&self) {
        panic!()
    }

    fn dump_stats(&self) -> Result<String> {
        panic!()
    }

    fn get_einstein_merkle_tree_size(&self) -> Result<u64> {
        panic!()
    }
}

impl VioletaBFTLogBatch for PanicWriteBatch {
    fn append(&mut self, violetabft_group_id: u64, entries: Vec<Entry>) -> Result<()> {
        panic!()
    }

    fn cut_logs(&mut self, violetabft_group_id: u64, from: u64, to: u64) {
        panic!()
    }

    fn put_violetabft_state(&mut self, violetabft_group_id: u64, state: &VioletaBFTLocalState) -> Result<()> {
        panic!()
    }

    fn persist_size(&self) -> usize {
        panic!()
    }

    fn is_empty(&self) -> bool {
        panic!()
    }

    fn merge(&mut self, _: Self) {
        panic!()
    }
}
