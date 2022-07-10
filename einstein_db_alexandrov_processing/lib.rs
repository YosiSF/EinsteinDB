//Allow unused and add module
// Language: rust
// Path: EinsteinDB/einstein_db_alexandrov_processing/src/lib.rs
// Compare this snippet from EinsteinDB/soliton_panic/src/lib.rs:
//
// //! An example EinsteinDB timelike_storage einstein_merkle_tree.
// //!
// //! This project is intended to serve as a skeleton for other einstein_merkle_tree
// //! implementations. It lays out the complex system of einstein_merkle_tree modules and traits
// //! in a way that is consistent with other EinsteinMerkleTrees. To create a new einstein_merkle_tree
// //! simply copy the entire directory structure and replace all "Panic*" names
// //! with your einstein_merkle_tree's own name; then fill in the implementations; remove
// //! the allow(unused) attribute;
// #![allow(unused)]
// #![cfg_attr(not(feature = "std"), no_std)]
//

//soliton_panic
// Language: rust
// Path: EinsteinDB/soliton_panic/src/lib.rs


//add type path to scope
// Language: rust
// Path: EinsteinDB/soliton_panic/src/lib.rs
// Compare this snippet from EinsteinDB/soliton_panic/src/lib.rs:
//
// //! An example EinsteinDB timelike_storage einstein_merkle_tree

use std::time::{Instant, Duration};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{self, Debug, Formatter};


use crate::time::{self, Time};
use crate::time::{Time, TimeError};
use crate::time::{TimeContext, TimeContextError};

#[derive(Debug)]
pub struct PosetError(String);


#[derive(Debug)]
pub struct PerfContextBuilder {
    name: String,
    parent: Option<Arc<PerfContext>>,
    children: Vec<Arc<PerfContext>>,
    child_count: Arc<AtomicUsize>,
    child_count_mutex: Arc<Mutex<()>>,
    child_count_map: Arc<Mutex<HashMap<String, usize>>>,
    child_count_map_mutex: Arc<Mutex<()>>,
}










///Alexandrov Topology Processing is a library that provides a way to process
/// transactions in a topology. For example, EinsteinDB is hybrid OLAP and OLTP.
/// The topology is a graph that represents the structure of the database.
/// The transactions are processed in the topology in a partial order fashion.
/// 
/// The library is written in Rust.
///
/// # Examples
/// ```
/// use einstein_db_alexandrov_processing::*;
/// use einstein_db_alexandrov_processing::topology::*;
/// use einstein_db_alexandrov_processing::topology::topology_processing::*;
#[macro_use]
extern crate lazy_static;



#[macro_use]
extern crate log;

#[macro_use]
extern crate serde_derive;



#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicAccount {
    pub id: String,
    pub balance: i64,
    pub nonce: i32,
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



#[derive(Debug, Clone)]
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
pub struct Workspace {
    pub account: PanicAccount,
    pub block: PanicBlock,
   // pub header: PanicHeader,

}
   


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PanicTransaction {
    pub sender: [u8; 32],
    pub receiver: [u8; 32],
    pub amount: u64,
    pub nonce: u64,
    pub signature: [u8; 32],
    pub hash: [u8; 32],
}



#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceHeader {
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
pub struct WorkspaceAccount {
    pub balance: u64,
    pub nonce: u64,
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceBlock {
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

/// A merkle tree.
/// This is a very simple implementation of a merkle tree.
/// It is not a secure implementation.
/// It is not a merkle tree.
/// It is not a merkle proof.
/// 













#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceReceipts {
    pub receipts: Vec<[u8; 32]>,
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceReceipt {
    pub receipt: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceTx {
    pub tx: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceState {
    pub state: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceLogs {
    pub logs: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceProposer {
    pub proposer: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceSeal {
    pub seal: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceHash {
    pub hash: [u8; 32],

}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceExtraData {
    pub extra_data: [u8; 32],
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkspaceLogsBlockHash {
    pub logs_block_hash: [u8; 32],
}


impl Workspace {
    pub fn new(account: PanicAccount, block: PanicBlock) -> Self {
        Workspace {
            account,
            block,
        }
    }

    pub fn new_header(header: PanicHeader) -> Self {
        Workspace {
            account: PanicAccount {
                id: (),
                balance: 0,
                nonce: 0,
            },
            block: PanicBlock {
                number: header.number,
                parent_hash: header.parent_hash,
                tx_hash: header.tx_hash,
                state_hash: header.state_hash,
                receipts_hash: header.receipts_hash,
                extra_data: header.extra_data,
                logs_block_hash: header.logs_block_hash,
                proposer: header.proposer,
                seal: header.seal,
                hash: header.hash,
            },
        }
    }


    pub fn new_header_with_account(header: PanicHeader, account: PanicAccount) -> Self {
        Workspace {
            account,
            block: PanicBlock {
                number: header.number,
                parent_hash: header.parent_hash,
                tx_hash: header.tx_hash,
                state_hash: header.state_hash,
                receipts_hash: header.receipts_hash,
                extra_data: header.extra_data,
                logs_block_hash: header.logs_block_hash,
                proposer: header.proposer,
                seal: header.seal,
                hash: header.hash,
            },
        }
    }

    pub fn new_header_with_block(header: PanicHeader, block: PanicBlock) -> Self {
        Workspace {
            account: PanicAccount {
                id: (),
                balance: 0,
                nonce: 0,
            },
            block,
        }
    }

    pub fn new_header_with_account_and_block(header: PanicHeader, account: PanicAccount, block: PanicBlock) -> Self {
        Workspace {
            account,
            block,
        }
    }

    pub fn new_header_with_account_and_block_with_account(header: PanicHeader, account: PanicAccount, block: PanicBlock, account2: PanicAccount) -> Self {
        Workspace {
            account,
            block,
        }
    }
}


impl WorkspaceHeader {
    pub fn new(number: u64, parent_hash: [u8; 32], tx_hash: [u8; 32], state_hash: [u8; 32], receipts_hash: [u8; 32], extra_data: [u8; 32], logs_block_hash: [u8; 32], proposer: [u8; 32], seal: [u8; 32], hash: [u8; 32]) -> Self {
        WorkspaceHeader {
            number,
            parent_hash,
            tx_hash,
            state_hash,
            receipts_hash,
            extra_data,
            logs_block_hash,
            proposer,
            seal,
            hash,
        }
    }

    pub fn new_with_account(number: u64, parent_hash: [u8; 32], tx_hash: [u8; 32], state_hash: [u8; 32], receipts_hash: [u8; 32], extra_data: [u8; 32], logs_block_hash: [u8; 32], proposer: [u8; 32], seal: [u8; 32], hash: [u8; 32], account: PanicAccount) -> Self {
        WorkspaceHeader {
            number,
            parent_hash,
            tx_hash,
            state_hash,
            receipts_hash,
            extra_data,
            logs_block_hash,
            proposer,
            seal,
            hash,
        }
    }
}


impl WorkspaceBlock {
    pub fn new(number: u64, parent_hash: [u8; 32], tx_hash: [u8; 32], state_hash: [u8; 32], receipts_hash: [u8; 32], extra_data: [u8; 32], logs_block_hash: [u8; 32], proposer: [u8; 32], seal: [u8; 32], hash: [u8; 32]) -> Self {
        WorkspaceBlock {
            number,
            parent_hash,
            tx_hash,
            state_hash,
            receipts_hash,
            extra_data,
            logs_block_hash,
            proposer,
            seal,
            hash,
        }
    }

    pub fn new_with_account(number: u64, parent_hash: [u8; 32], tx_hash: [u8; 32], state_hash: [u8; 32], receipts_hash: [u8; 32], extra_data: [u8; 32], logs_block_hash: [u8; 32], proposer: [u8; 32], seal: [u8; 32], hash: [u8; 32], account: PanicAccount) -> Self {
        WorkspaceBlock {
            number,
            parent_hash,
            tx_hash,
            state_hash,
            receipts_hash,
            extra_data,
            logs_block_hash,
            proposer,
            seal,
            hash,
        }
    }

    pub fn new_with_block(number: u64, parent_hash: [u8; 32], tx_hash: [u8; 32], state_hash: [u8; 32], receipts_hash: [u8; 32], extra_data: [u8; 32], logs_block_hash: [u8; 32], proposer: [u8; 32], seal: [u8; 32], hash: [u8; 32], block: PanicBlock) -> Self {
        WorkspaceBlock {
            number,
            parent_hash,
            tx_hash,
            state_hash,
            receipts_hash,
            extra_data,
            logs_block_hash,
            proposer,
            seal,
            hash,
        }
    }

    pub fn new_with_account_and_block(number: u64, parent_hash: [u8; 32], tx_hash: [u8; 32], state_hash: [u8; 32], receipts_hash: [u8; 32], extra_data: [u8; 32], logs_block_hash: [u8; 32], proposer: [u8; 32], seal: [u8; 32], hash: [u8; 32], account: PanicAccount, block: PanicBlock) -> Self {
        WorkspaceBlock {
            number,
            parent_hash,
            tx_hash,
            state_hash,
            receipts_hash,
            extra_data,
            logs_block_hash,
            proposer,
            seal,
            hash,
        }
    }

    pub fn new_with_account_and_block_with_account(number: u64, parent_hash: [u8; 32], tx_hash: [u8; 32], state_hash: [u8; 32], receipts_hash: [u8; 32], extra_data: [u8; 32], logs_block_hash: [u8; 32], proposer: [u8; 32], seal: [u8; 32], hash: [u8; 32], account: PanicAccount, block: PanicBlock, account2: PanicAccount) -> Self {
        WorkspaceBlock {
            number,
            parent_hash,
            tx_hash,
            state_hash,
            receipts_hash,
            extra_data,
            logs_block_hash,
            proposer,
            seal,
            hash,
        }
    }
}


    /// Create a new workspace.
    /// 
    /// # Arguments
    /// * `db_path` - The path to the database.
    /// * `db_ctl_path` - The path to the database control file.
    /// * `db_path_tmp` - The path to the temporary database.
    /// * `db_ctl_path_tmp` - The path to the temporary database control file.
    /// 
    /// # Returns
    /// * `Workspace` - The new workspace.
    /// 
    /// # Example
    /// ```
    /// use einstein_db::Workspace;
    ///     
    /// let workspace = Workspace::new("/tmp/einstein_db", "/tmp/einstein_db.ctl", "/tmp/einstein_db.tmp", "/tmp/einstein_db.tmp.ctl");
    /// ```
    ///     
    /// # Panics
    /// * `Panic` - If the workspace could not be created.
    ///     
    /// # Notes
    /// * The workspace will be created if it does not exist.
    /// * The workspace will be opened if it exists.
    /// 
    
    /// Create a new workspace.
    /// 
    /// # Arguments
    /// * `db_path` - The path to the database.
    /// * `db_ctl_path` - The path to the database control file.
    /// * `db_path_tmp` - The path to the temporary database.
    /// * `db_ctl_path_tmp` - The path to the temporary database control file.
    ///     
    /// # Returns
    /// * `Workspace` - The new workspace.
    ///     
    /// # Panics
    /// * `Panic` - If the workspace could not be created.
    /// 
    
    /// Create a new workspace.
    



pub struct EinsteinDB {
    pub workspace: Workspace,
    pub space_name: String,
    pub space_id: u64,
    pub space_type: String,
    pub space_dimension: u64,
    pub space_size: u64,
    pub space_causets_size: u64,
    pub space_causets_hash: String,
}




/*FoundationDB and #EinsteinDB subspaces.

subspaces are isolated spaces that are used to store data in the
namespace of the EinsteinDB. Using FoundationDB subspaces, we can
transmute the data from the EinsteinDB namespace to a namespace
with less overhead and more flexibility. A cache-miss is a cost that is paid

*/


/*FoundationDB and #EinsteinDB subspaces.

subspaces are isolated spaces that are used to store data in the
namespace of the EinsteinDB. Using FoundationDB subspaces, we can
transmute the data from the EinsteinDB namespace to a namespace
with less overhead and more flexibility. A cache-miss is a cost that is paid

*/

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FoundationDbSubspace {
    pub subspace_name: String,
    pub subspace_id: u64,
    pub subspace_type: String,
    pub subspace_dimension: u64,
    pub subspace_size: u64,
    pub subspace_causets_size: u64,
    pub subspace_causets_hash: String,
}

use std::collections::HashMap;

pub(crate) fn read_file(path: &Path) -> String {

    if let Ok(mut file) = File::open(path) {
        let mut contents = String::new();
        if let Ok(_) = file.read_to_string(&mut contents) {
            return contents;
        }
    
    }
    panic!("Could not read file: {}", path.to_str().unwrap());

    String::new()
}


pub(crate) fn write_file(path: &Path, contents: &str) {
    if let Ok(mut file) = File::create(path) {
        if let Ok(_) = file.write_all(contents.as_bytes()) {
            return;
        }
    }
    panic!("Could not write file: {}", path.to_str().unwrap());
}


pub(crate) fn read_file_to_string(path: &Path) -> String {
    let mut file = File::open(path).unwrap();
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    contents
}





pub(crate) fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> T {
    let contents = read_file(path);
    let json: T = serde_json::from_str(&contents).unwrap();
    json
}


pub(crate) fn write_json<T: serde::ser::Serialize>(path: &Path, json: &T) {
    let contents = serde_json::to_string(json).unwrap();
    write_file(path, &contents);
}


pub(crate) fn read_json_from_string<T: serde::de::DeserializeOwned>(contents: &str) -> T {
    let json: T = serde_json::from_str(&contents).unwrap();
    json
}

//file


pub(crate) fn read_file_from_string(contents: &str) -> String {
    let json: String = serde_json::from_str(&contents).unwrap();
    json
}


pub(crate) fn write_file_from_string(path: &Path, contents: &str) {
    let json: String = serde_json::from_str(&contents).unwrap();
    write_file(path, &json);
}


pub(crate) fn read_file_from_string_to_string(contents: &str) -> String {
    let json: String = serde_json::from_str(&contents).unwrap();
    json
}


pub(crate) fn write_file_from_string_to_string(path: &Path, contents: &str) {
    let json: String = serde_json::from_str(&contents).unwrap();
    write_file(path, &json);
}





#[cfg(test)]
//serde
#[test]
pub(crate) fn read_json_from_string_to_string<T: serde::de::DeserializeOwned>(contents: &str) -> T {
    let contents = read_file(&Path::new("/tmp/einstein_db.tmp"));
    contents
}

pub fn write_json_from_string_to_string<T: serde::ser::Serialize>(path: &Path, contents: &str) {
    let json: String = serde_json::from_str(&contents).unwrap();
    write_file(path, &json);
}




pub(crate) fn read_json_from_string_to_string_to_string<T: serde::de::DeserializeOwned>(contents: &str) -> T {
    let json: T = serde_json::from_str(&contents).unwrap();
    json
}


pub(crate) fn write_file_from_vec(path: &Path, contents: &[u8]) {
    let mut file = File::create(path).unwrap();
    file.write_all(contents).unwrap();
}


pub(crate) fn read_file_to_vec_u8(path: &Path) -> Vec<u8> {
    let mut file = File::open(path).unwrap();
    let mut contents = Vec::new();
    file.read_to_end(&mut contents).unwrap();
    contents
}



pub fn read_file_lines(path: &Path) -> Vec<String> {
    let mut file = File::open(path).unwrap();
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents).expect("Failed to read file");
    contents.lines().map(|x| x.to_string()).collect()
}


pub fn read_file_lines_to_vec(path: &Path) -> Vec<Vec<String>> {
    let file = File::open(path).expect("Failed to open file");
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents).expect("Failed to read file");
    contents.lines().map(|x| x.split_whitespace().map(|x| x.to_string()).collect()).collect()
}


pub fn read_file_lines_to_vec_of_ints(path: &Path) -> Vec<Vec<i32>> {
    let file = File::open(path).expect("Failed to open file");
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents).expect("Failed to read file");
    contents.lines().map(|x| x.split_whitespace().map(|x| x.parse::<i32>().unwrap()).collect()).collect()
}


pub fn read_file_lines_to_vec_of_strings(path: &Path) -> Vec<Vec<String>> {
    let file = File::open(path).expect("Failed to open file");
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents).expect("Failed to read file");
    contents.lines().map(|x| x.split_whitespace().map(|x| x.to_string()).collect()).collect()
}

///!By using the key ordering of FoundationDB, we can store indexes so that an index query can return multiple matches using a single efficient range read operation. By updating the data element and all of its associated indexes together within a single ACID transaction
/// we can guarantee that the data and indexes stay in sync.

///!Pattern
// Let’s say the primary copy of the data is stored with key-value pairs where the key has a tuple-structure consisting of a subspace and an ID:
//
// (main_subspace, ID) = value
// This structure lets you lookup an “ID” easily and get its associated value. But, let’s say part of the value is a zipcode. You might be interested in all IDs that have a zipcode of 22182. You could answer that question, but it would require scanning every single ID. What we need to improve the efficiency is an “index on zipcode”.
//
// An index is essentially another representation of the data, designed to be looked up in a different way:
//
// (index_subspace, zipcode, ID) = ''


///!Indexes
// The index is a key-value pair where the key has a tuple-structure consisting of a subspace, a zipcode, and an ID:
//
// (index_subspace, zipcode, ID) = ''
// This structure lets you lookup an “ID” easily and get its associated value. But, let’s say part of the value is a zipcode. You might be interested in all IDs that have a zipcode of 22182. You could answer that question, but it would require scanning every single ID. What we need to improve the efficiency is an “index on zipcode”.
//
// An index is essentially another representation of the data, designed to be looked up in a different way:
//
// (index_subspace, zipcode, ID) = ''



#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::prelude::*;
    use std::path::Path;

    #[test]
    fn test_read_file_lines_to_vec_of_strings() {
        let path = Path::new("test_data/test_file.txt");
        let mut file = File::open(path).expect("Failed to open file");
        let mut buf_reader = BufReader::new(file);
        let mut contents = String::new();
        buf_reader.read_to_string(&mut contents).expect("Failed to read file");
        let lines = contents.lines().map(|x| x.to_string()).collect();
        assert_eq!(lines, read_file_lines_to_vec_of_strings(path));
    }

    #[test]
    fn test_read_file_lines_to_vec_of_ints() {
        let path = Path::new("test_data/test_file.txt");
        let mut file = File::open(path).expect("Failed to open file");
        let mut buf_reader = BufReader::new(file);
        let mut contents = String::new();
        buf_reader.read_to_string(&mut contents).expect("Failed to read file");
        let lines = contents.lines().map(|x| x.split_whitespace().map(|x| x.parse::<i32>().unwrap()).collect()).collect();
        assert_eq!(lines, read_file_lines_to_vec_of_ints(path));
    }

    #[test]
    fn test_read_file_lines_to_vec() {
        let path = Path::new("test_data/test_file.txt");
        let mut file = File::open(path).expect("Failed to open file");
        let mut buf_reader = BufReader::new(file);
        let mut contents = String::new();
        buf_reader.read_to_string(&mut contents).expect("Failed to read file");

        let lines = contents.lines().map(|x| x.to_string()).collect();
        assert_eq!(lines, read_file_lines_to_vec(path));
    }
}



