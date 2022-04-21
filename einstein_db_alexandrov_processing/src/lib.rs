//lib.rs
//  #[APPEND_LOG_g(test)]
//  mod tests {
//    #[test]
//    fn it_works() {
//      assert_eq!(2 + 2, 4);
//    }
//  }
//

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::path::Path;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::collections::BTreeSet;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::VecDeque;
use std::collections::LinkedList;
use std::collections::BinaryHeap;


//use std::collections::HashMap;

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

