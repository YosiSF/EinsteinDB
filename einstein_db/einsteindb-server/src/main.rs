//Copyright 2022 Whtcorps Inc & EinsteinDB Authors; In Affiliation with OpenAI, Mozilla, and FoundationDB
//Licensed Under Apache-2.0

use std::path::Path;
use std::process;

//use einstein_db::config::einstein_db_ctl;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        println!("Usage: einsteindb-server <config_file>");     
        process::exit(1);
    }
   
    let config_file = &args[1];
    let config_path = Path::new(config_file);
    let einstdb_ctl = einstein_db_ctl::new(config_path);
    let einstdb_server = einstein_db_server::new(config);
    server.run();
}   // end main function definition block

