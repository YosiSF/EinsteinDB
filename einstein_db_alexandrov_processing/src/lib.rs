
use std::collections::hash_map::Entry;
use std::collections::HashSet;
use soliton::*;
use causets::{ Causets, CausetsError };
use causets::causets_traits::*;
use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;



use std::sync::mpsc::{ channel, Receiver, Sender };

#[derive(Debug, Clone)]
pub struct Workspace( EinsteinDB::einstein_db::Workspace );


impl Workspace {
    pub fn new(db_path: String, db_ctl_path: String, db_path_tmp: String, db_ctl_path_tmp: String) -> Workspace {
        Workspace(EinsteinDB::einstein_db::Workspace::new(db_path, db_ctl_path, db_path_tmp, db_ctl_path_tmp))
    }

    pub fn open(db_path: String, db_ctl_path: String, db_path_tmp: String, db_ctl_path_tmp: String) -> Workspace {
        Workspace(EinsteinDB::einstein_db::Workspace::open(db_path, db_ctl_path, db_path_tmp, db_ctl_path_tmp))
    }

    pub fn open_existing(db_path: String, db_ctl_path: String, db_path_tmp: String, db_ctl_path_tmp: String) -> Workspace {
        Workspace(EinsteinDB::einstein_db::Workspace::open_existing(db_path, db_ctl_path, db_path_tmp, db_ctl_path_tmp))
    }

    pub fn get_db_path(&self) -> String {
        self.0.get_db_path()
    }

    pub fn get_db_ctl_path(&self) -> String {
        self.0.get_db_ctl_path()
    }

    pub fn get_db_path_tmp(&self) -> String {
        self.0.get_db_path_tmp()
    }

    pub fn get_db_ctl_path_tmp(&self) -> String {
        self.0.get_db_ctl_path_tmp()
    }

    pub fn get_db_path_tmp_tmp(&self) -> String {
        self.0.get_db_path_tmp_tmp()
    }

    pub fn get_db_ctl_path_tmp_tmp(&self) -> String {
        self.0.get_db_ctl_path_tmp
    }
}

//! # The Alexandrov Processing Library
//!  This library provides a set of functions for processing data from the
//! [EinsteinDB](https://www.github.com/YosiSF/EinsteinDB/).
//!  The library is designed to be used with the [EinsteinDB](https://www.github.com/YosiSF/EinsteinDB/)
//! library.
//!
//! ## The Library
//!
//!  The library is designed to be used with the [EinsteinDB](https://www.github.com/YosiSF/EinsteinDB/)


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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct AlexandrovSpacesList {
    pub spaces: Vec<AlexandrovSpaces>,


}

//! #FoundationDB and #EinsteinDB subspaces.
//!
//! subspaces are isolated spaces that are used to store data in the
//! namespace of the EinsteinDB. Using FoundationDB subspaces, we can
//! transmute the data from the EinsteinDB namespace to a namespace
//! with less overhead and more flexibility. A cache-miss is a cost that is paid


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

pub fn read_file(path: &Path) -> String {

    let file = File::open(path).expect("Failed to open file");
    let mut buf_reader = BufReader::new(file);
    let mut contents = String::new();
    buf_reader.read_to_string(&mut contents).expect("Failed to read file");
    contents
}


pub fn read_file_lines(path: &Path) -> Vec<String> {
    let file = File::open(path).expect("Failed to open file");
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



