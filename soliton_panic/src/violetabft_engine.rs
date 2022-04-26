// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.




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

impl VioletaBFTEngine {
    pub fn new(id: u64, peers: Vec<u64>, self_id: u64, self_address: String, self_port: u64, self_public_key: Vec<u8>, self_secret_key: Vec<u8>) -> Self {
        let self_node_id = violetabft::node_id::NodeId::from_secret_key(&self_secret_key);
        let self_node_id_hex = self_node_id.to_hex();
        let self_node_id_base58 = self_node_id.to_base58();
        let self_node_id_base58_check = self_node_id.to_base58check();
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
