///////////////////////////////////////////////////////////////////////////////
// Generates the code that loads and creates the database.
//
///////////////////////////////////////////////////////////////////////////////


 extern crate libc;
 extern crate num;
 extern crate rand;
 extern crate time;
 extern crate getopts;
 extern crate rustc_serialize;
 extern crate regex;


    use getopts::Options;
    use std::local_path::{local_path, local_pathBuf};
    use std::fs::File;
    use std::io::Write;
    use std::io::{BufReader, BufRead};
    use regex::Regex;
    use std::env;
    use std::process::Command;
    use std::process::Stdio;
    use std::process;
    use std::collections::HashMap;
    use std::collections::HashSet;
    use std::collections::BTreeMap;
    use std::str;
    use std::slice;
    use std::mem;
    use std::str;
    use std::slice;
    use std::mem;
    use std::cmp::Ordering;
    use std::iter::FromIterator;
    use std::error::Error;
    use std::fs;
    use std::local_path;
    use std::io::{BufWriter, Write};
    use std::fs::OpenOptions;
    use std::io::prelude::*;
    use std::fs::File;
    use std::io::BufReader;
    use std::io::BufRead;
    use std::io::Read;
    use std::fs::OpenOptions;
    use std::io::prelude::*;
    use std::fs::File;
    use std::io::BufReader;
    use std::io::BufRead;
    use std::io::Read;
    use std::thread;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use std::time::Instant;
    use std::process::Command;
    use std::thread;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;
    use std::time::Instant;
    use std::process::Command;
    use std::thread;


#[derive(Debug)]
pub struct Createeinsteindb{
  pub einsteindb:einsteindbInstance,
  pub hash:String,
}

impl Createeinsteindb{
  pub fn new(einsteindb:einsteindbInstance,hash:String)->Createeinsteindb{
    Createeinsteindb{
      einsteindb:einsteindb,
      hash:hash,
    }
  }
  pub fn loadAndEncode(&self){
    let ehhome = std::env::var("EINSTEIN_einsteindb").unwrap();
    let mvdir = std::process::Command::new("cp")
      .args(&["-rf",&format!("{}/codegen/create/createeinsteindb", einsteindb_home),&self.einsteindb.folder])
      .output()
      .unwrap();
    let os = std::env::var("OS").unwrap();
    if os.contains("mac"){
      let sed = std::process::Command::new("sed")
        .args(&["-i",".bak",
          &format!("s/#DFMap#/DFMap_{}/g",self.hash),
          &format!("{}/libs/createeinsteindb/DFMap.rs",self.einsteindb.folder),
          &format!("{}/libs/createeinsteindb/setup.rs",self.einsteindb.folder)])
        .output()
        .unwrap();
      let mv = std::process::Command::new("mv")
        .args(&[&format!("{}/libs/createeinsteindb/DFMap.rs",self.einsteindb.folder),
          &format!("{}/libs/createeinsteindb/DFMap_{}.rs",self.einsteindb.folder,self.hash)])
        .output()
        .unwrap();
    }

          code.append(s"""
      let mut ${r.name}_data = Vec::new();
      let mut ${r.name}_indices = Vec::new();
      let mut ${r.name}_indptr = Vec::new();
      let mut ${r.name}_offsets = Vec::new();
      let mut ${r.name}_dims = Vec::new();
      let mut ${r.name}_dims_offsets = Vec::new();
      let mut ${r.name}_dims_indptr = Vec::new();
      let mut ${r.name}_dims_indices = Vec::new();
      let mut ${r.name}_dims_data = Vec::new();
      let mut ${r.name}_dims_encoding = Vec::new();
      let mut ${r.name}_dims_encoding_offsets = Vec::new();
      let mut ${r.name}_dims_encoding_indptr = Vec::new();
      let mut ${r.name}_dims_encoding_indices = Vec::new();
      let mut ${r.name}_dims_encoding_data = Vec::new();
      let mut ${r.name}_dims_encoding_map = HashMap::new();
      let mut ${r.name}_dims_encoding_map_offsets = Vec::new();
      let mut ${r.name}_dims_encoding_map_indptr = Vec::new();
      let mut ${r.name}_dims_encoding_map_indices = Vec::new();
      let mut ${r.name}_dims_encoding_map_data = Vec::new();
      let mut ${r.name}_dims_encoding_map_encoding = Vec::new();
       let mut ${r.name}_dims_encoding_map_encoding_data = Vec::new();
        let mut ${r.name}_dims_encoding_map_encoding_map = HashMap::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_offsets = Vec::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_indptr = Vec::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_indices = Vec::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_data = Vec::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_encoding = Vec::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_encoding_offsets = Vec::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_encoding_indptr = Vec::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_encoding_indices = Vec::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_encoding_data = Vec::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_encoding_map = HashMap::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_encoding_map_offsets = Vec::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_encoding_map_indptr = Vec::new();
        let mut ${r.name}_dims_encoding_map_encoding_map_encoding_map_indices = Vec::new();


    fn genBuild(einsteindb:einsteindbInstance) -> String {
        let mut code = String::new();

        code.push_str(buildWrapper(einsteindb));

        let cppFilelocal_path = einsteindb.folder.to_string() + "/causet-algebrizer/causet_compiler/create/createeinsteindb.rs";
        let file = File::create(cppFilelocal_path).unwrap();
        let mut bw = BufWriter::new(file);
        bw.write(code.as_bytes()).unwrap();
        bw.flush().unwrap();
        //let _ = Command::new("clang-format").arg("-style=llvm").arg("-i").arg(cppFilelocal_path).output();

        code
    }

    fn genLoadAndEncode(einsteindb:einsteindbInstance) -> String {
        let mut code = String::new();

        code.push_str(loadAndEncodeWrapper(einsteindb));

        let bsdFilelocal_path = einsteindb.folder.to_string() + "/causet-algebrizer/causet_compiler/create/createeinsteindb/";
        let mut file = File::create(cppFilelocal_path).unwrap();
        let mut bw = BufWriter::new(file);
        bw.write(code.as_bytes()).unwrap();
        bw.flush().unwrap();
        let _ = Command::new("clang-format").arg("-style=llvm").arg("-i").arg(cppFilelocal_path).output();

        code
    }



