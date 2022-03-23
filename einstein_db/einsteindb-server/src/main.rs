//Copyright 2022 Whtcorps Inc & EinsteinDB Authors; In Affiliation with OpenAI, Mozilla, and FoundationDB
//Licensed Under Apache-2.0

#![feature(proc_macro_hygiene)]

use std::path::Path;
use std::process;

//use einstein_db::config::EinsteinDBConfig;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Usage: einsteindb-server <config_file>");
        process::exit(1);
    }
    let config_file = &args[1];
    let config_path = Path::new(config_file);
    let config = EinsteinDBConfig::new(config_path);
    let server = EinsteinDBServer::new(config);
    server.run();
}   // end main function definition block

