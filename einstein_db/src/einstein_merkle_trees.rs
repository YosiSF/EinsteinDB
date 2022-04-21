//Store everything in a mongodb instance for now
use mongodb::{
    bson::{Bson, doc},
    Client,
    coll::options::FindOptions,
    error::Error,
    options::ClientOptions,
};

// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.
//foundationdb crate
use crate::{
    einsteindb_grpc::{
        einsteindb_grpc::{einsteindb_grpc::EinsteinDBGrpcClient, EinsteinDBGrpcClientStreaming},
        EinsteinDBGrpcClientStreamingExt,
    },
    EinsteinDBGrpcClientExt,
    EinsteinDBGrpcClientStreamingExt,
};
use crate::errors::Result;
use crate::fdb_lsh_treeKV;
use crate::fdb_lsh_treeKV;
use crate::options::WriteOptions;
use crate::violetabft_einstein_merkle_tree::VioletaBFTeinstein_merkle_tree;
use crate::write_alexandro::WriteBatch;

#[derive(Clone, Debug)]
pub struct EinsteinMerkleTrees<K, R> {
    client: EinsteinDBGrpcClient,
    db_name: String,
    collection_name: String,
    key_type: K,
    value_type: R,
}


impl<K: KV, R: VioletaBFTeinstein_merkle_tree> EinsteinMerkleTrees<K, R> {
    pub fn new(kv_einstein_merkle_tree: K, violetabft_einstein_merkle_tree: R) -> Self {
        let client = EinsteinDBGrpcClient::new_plain(
            "localhost",
            50051,
            Default::default(),
        )
            .unwrap();
        let db_name = "einstein_merkle_tree".to_string();
        let collection_name = "einstein_merkle_tree".to_string();
        EinsteinMerkleTrees {
            client,
            db_name,
            collection_name,
            key_type: kv_einstein_merkle_tree,
            value_type: violetabft_einstein_merkle_tree,
        }
    }

    pub fn get_einstein_merkle_tree(&self, key: &K) -> Result<R> {
        unimplemented!()
    }

    pub fn put_einstein_merkle_tree(&self, key: &K, value: &R) -> Result<()> {
        unimplemented!()
    }

    pub fn delete_einstein_merkle_tree(&self, key: &K) -> Result<()> {
        unimplemented!()
    }

    pub fn get_einstein_merkle_tree_range(&self, start_key: &K, end_key: &K) -> Result<Vec<R>> {
        unimplemented!()
    }

    pub fn get_einstein_merkle_tree_range_with_options(&self, start_key: &K, end_key: &K, options: &FindOptions) -> Result<Vec<R>> {
        unimplemented!()
    }

    pub fn get_einstein_merkle_tree_range_with_options_with_options(&self, start_key: &K, end_key: &K, options: &FindOptions) -> Result<Vec<R>> {
        unimplemented!()
    }

    pub fn write_kv(&self, wb: &K::WriteBatch) -> Result<()> {
        wb.write()
    }

    pub fn write_kv_opt(&self, wb: &K::WriteBatch, opts: &WriteOptions) -> Result<()> {
        wb.write_opt(opts)
    }

    pub fn sync_kv(&self) -> Result<()> {
        self.kv.sync()
    }
}
