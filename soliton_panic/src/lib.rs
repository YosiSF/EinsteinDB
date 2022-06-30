// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! An example EinsteinDB timelike_storage einstein_merkle_tree.
//!
//! This project is intended to serve as a skeleton for other einstein_merkle_tree
//! implementations. It lays out the complex system of einstein_merkle_tree modules and traits
//! in a way that is consistent with other EinsteinMerkleTrees. To create a new einstein_merkle_tree
//! simply copy the entire directory structure and replace all "Panic*" names
//! with your einstein_merkle_tree's own name; then fill in the implementations; remove
//! the allow(unused) attribute;
#![allow(unused)]
#![cfg_attr(not(feature = "std"), no_std)]




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
pub struct PanicBlockHeader {
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



impl PanicTransaction {
    
    pub fn from_raw(sender: Type, receiver: String, value: u64, timestamp: u64) -> Self {
        PanicTransaction {
            sender,
            receiver,
            value,
            timestamp,
        }
    }

    pub fn from_raw_data(sender: Type, receiver: String, value: u64) -> Self {
        PanicTransaction {
            sender,
            receiver,
            value,
            timestamp: 0,
        }
    }

    pub(crate) fn from_raw_data_with_timestamp(sender: Type, receiver: String, value: u64, timestamp: u64) -> Self {
        PanicTransaction {
            sender,
            receiver,
            value,
            timestamp,
        }
    }
}

    pub fn from_raw_data_with_timestamp_and_receiver(sender: Type, receiver: String, value: u64, timestamp: u64, receiver: String) -> Self {
        PanicTransaction {
            sender,
            receiver,
            value,
            timestamp,
        }
    }

    pub fn from_raw_data_with_timestamp_and_receiver_and_value(sender: Type, receiver: String, value: u64, timestamp: u64, receiver: String, value: u64) -> Self {
        PanicTransaction {
            sender,
            receiver,
            value,
            timestamp,
        }
    }

    pub fn into_raw(self) -> (Type, String, u64, u64) {
        (self.sender, self.receiver, self.value, self.timestamp)
    }

    pub fn into_raw_data(self) -> (Type, String, u64) {
        (self.sender, self.receiver, self.value)
    }

    pub fn into_raw_data_with_timestamp(self) -> (Type, String, u64, u64) {
        (self.sender, self.receiver, self.value, self.timestamp)
    }

    pub fn into_raw_data_with_timestamp_and_receiver(self) -> (Type, String, u64, u64, String) {
        (self.sender, self.receiver, self.value, self.timestamp, self.receiver)
    }

    pub fn into_raw_data_with_timestamp_and_receiver_and_value(self) -> (Type, String, u64, u64, String, u64) {

        (self.sender, self.receiver, self.value, self.timestamp, self.receiver, self.value)
    }

    pub fn new(sender: Type, receiver: String, value: u64, timestamp: u64) -> Self {
        PanicTransaction {
            sender,
            receiver,
            value,
            timestamp,
        }
    }

    pub fn new_data(sender: Type, receiver: String, value: u64) -> Self {
        PanicTransaction {
            sender,
            receiver,
            value,
            timestamp: 0,
        }
    }

    pub fn new_data_with_timestamp(sender: Type, receiver: String, value: u64, timestamp: u64) -> Self {
        PanicTransaction {
            sender,
            receiver,
            value,
            timestamp,
        }
    }

    pub fn new_data_with_timestamp_and_receiver(sender: Type, receiver: String, value: u64, timestamp: u64, receiver: String) -> Self {
        PanicTransaction {
            sender,
            receiver,
            value,
            timestamp,
        }
    }

    pub fn new_data_with_timestamp_and_receiver_and_value(sender: Type, receiver: String, value: u64, timestamp: u64, receiver: String, value: u64) -> Self {
        PanicTransaction {
            sender,
            receiver,
            value,
            timestamp,
        }
    }

    pub fn sender(&self) -> &Type {
        &self.sender
    }

    pub fn receiver(&self) -> &String {
        &self.receiver
    }
    pub fn value(&self) -> &u64 {
        &self.value
    }
    pub fn timestamp(&self) -> &u64 {
        &self.timestamp
    }
    pub fn set_sender(&mut self, sender: Type) {
        self.sender = sender;
    }
    pub fn set_receiver(&mut self, receiver: String) {
        self.receiver = receiver;
    }

    pub fn new_data_with_timestamp_and_receiver_and_value(sender: Type, receiver: String, value: u64, timestamp: u64, receiver: String, value: u64) -> Self {
        PanicTransaction {
            sender,
            receiver,
            value,
            timestamp,
        }
    }

    pub fn sender(&self) -> &Type {
        &self.sender
    }

    pub fn receiver(&self) -> &String {
        &self.receiver
    }

    pub fn value(&self) -> &u64 {
        &self.value
    }

    pub fn timestamp(&self) -> &u64 {
        &self.timestamp
    }

    pub fn set_sender(&mut self, sender: Type) {
        self.sender = sender;
    }
    pub fn set_receiver(&mut self, receiver: String) {
        self.receiver = receiver;
    }
    pub fn set_value(&mut self, value: u64) {
        self.value = value;
    }
    pub fn set_timestamp(&mut self, timestamp: u64) {
        self.timestamp = timestamp;
    }

    pub fn with_sender(mut self, sender: Type) -> Self {
        self.sender = sender;
        self
    }
    pub fn with_receiver(mut self, receiver: String) -> Self {
        self.receiver = receiver;
        self
    }
    pub fn with_value(mut self, value: u64) -> Self {
        self.value = value;
        self
    }
    pub fn with_timestamp(mut self, timestamp: u64) -> Self {
        self.timestamp = timestamp;
        self
    }

    pub fn into_builder(self) -> PanicTransactionBuilder {
        PanicTransactionBuilder {
            sender: self.sender,
            receiver: self.receiver,
            value: self.value,
            timestamp: self.timestamp,
        }
    }

    pub fn into_builder_data(self) -> PanicTransactionBuilderData {
        PanicTransactionBuilderData {
            sender: self.sender,
            receiver: self.receiver,
            value: self.value,
        }
    }

    pub fn into_builder_data_with_timestamp(self) -> PanicTransactionBuilderDataWithTimestamp {
        PanicTransactionBuilderDataWithTimestamp {
            sender: self.sender,
            receiver: self.receiver,
            value: self.value,
            timestamp: self.timestamp,
        }
    }
    pub fn into_builder_data_with_timestamp_and_receiver(self) -> PanicTransactionBuilderDataWithTimestampAndReceiver {
        PanicTransactionBuilderDataWithTimestampAndReceiver {
            sender: self.sender,
            receiver: self.receiver,
            value: self.value,
            timestamp: self.timestamp,
            receiver: self.receiver,
        }
    }
    pub fn into_builder_data_with_timestamp_and_receiver_and_value(self) -> PanicTransactionBuilderDataWithTimestampAndReceiverAndValue {
        PanicTransactionBuilderDataWithTimestampAndReceiverAndValue {
            sender: self.sender,
            receiver: self.receiver,
            value: self.value,
            timestamp: self.timestamp,
            receiver: self.receiver,
            value: self.value,
        }


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






