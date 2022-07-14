
/// Copyright (c) 2022 Whtcorps Inc and EinsteinDB Project contributors
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///    http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
///See the License for the specific language governing permissions and
///limitations under the License.
///
/// # About
///
/// This is a library for the [EinsteinDB](https://einsteindb.com
/// "EinsteinDB: A Scalable, High-Performance, Distributed Database")

//import serde
mod causal_set;
mod config;
mod encoder;
mod event_slice;



/// # About
///     This is a library for the [EinsteinDB](https://einsteindb.com





/// A `Sync` implementation for `AllegroPoset`.
/// This implementation is thread-safe.
/// # Examples
/// ```
/// use einsteindb::causetq::sync::new_sync;
/// use einsteindb::causetq::sync::Sync;
/// use std::sync::Arc;
/// use std::sync::Mutex;
///
/// let poset = new_sync();
/// let sync = Sync::new(poset);
///
/// let mutex = Arc::new(Mutex::new(sync));
/// let mutex2 = Arc::new(Mutex::new(sync));
///
/// let mutex3 = Arc::new(Mutex::new(sync));
///
///
///
///




pub enum BerolinaSqlError {
    IoError(io::Error),
    SqlError(String),
}

#[derive(Debug)]
pub enum BerolinaSqlErrorType {
    IoError,
    SqlError,
}

#[derive(Debug)]
pub struct BerolinaSqlErrorInfo {
    pub error_type: BerolinaSqlErrorType,
    pub error_msg: String,
}



pub struct BerolinaSqlErrorInfo2 {
    pub error_type: BerolinaSqlErrorType,
    pub error_msg: String,
}
//
// pub fn get_einstein_db_client_state_str_len() -> usize {
//     return self.einstein_db_client_state_str.len();
//
// }
//


impl BerolinaSqlErrorInfoList {
    pub fn new() -> BerolinaSqlErrorInfoList {
        BerolinaSqlErrorInfoList {
            error_info_list: Vec::new(),
        }
    }
}
// #[derive(Deserialize, Serialize, Debug)]
// pub struct BerolinaSqlErrorInfoListSerialized {
//     pub error_info_list: Vec<BerolinaSqlErrorInfoSerialized>,
// }


// impl BerolinaSqlErrorInfoListSerialized {
//     pub fn new() -> BerolinaSqlErrorInfoListSerialized {
//         BerolinaSqlErrorInfoListSerialized {
//             error_info_list: Vec::new(),
//         }
//     }
// }



//serde::{Deserialize, Serialize};





//macro for unused variables
#[allow(unused_variables)]
#[allow(unused_mut)]
#[allow(unused_imports)]
#[allow(dead_code)]
#[allow(unused_must_use)]
#[warn(unused_extern_crates)]
#[warn(unused_import_braces)]
#[warn(unused_qualifications)]

#[warn(unused_variables)]


use std::io;


pub const EINSTEIN_DB_VERSION: u32 = 0x0101;
pub const EINSTEIN_DB_VERSION_STR: &str = "0.1.1";
pub const EINSTEIN_ML_VERSION: u32 = 0x0101;
pub const EINSTEIN_DB_VERSION_STR_LEN: usize = 16;



#[derive(Debug)]
pub enum EinsteinDBError {
    IoError(io::Error),
    SqlError(String),
}


#[derive(Debug)]
pub enum EinsteinDBErrorType {
    IoError,
    SqlError,
}

/// The error type for EinsteinDB.
/// All errors returned from the EinsteinDB library are of this type.
/// This is a catch-all type for errors that are not specific to any
/// particular operation.


#[derive(Debug)]
pub enum Error {
    /// An error originating from the client library itself.
    /// This is a bug in the library itself and should not occur.
    InternalError(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError2(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError3(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError4(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError5(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError6(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError7(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError8(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError9(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError10(String),
    /// An error originating from the server itself.
    /// This is a bug in the server and should be reported to the server
    /// administrator.
    ServerError11(String),

}


impl Error {
    pub fn new(msg: String) -> Error {
        Error::InternalError(msg)
    }
}




impl Error {
    pub fn new2(msg: String) -> Error {
        Error::ServerError(msg)
    }
}


impl Error {
    pub fn new3(msg: String) -> Error {
        Error::ServerError2(msg)
    }
}






//  use std::sync::{Arc, Mutex};
// use std::sync::atomic::{AtomicBool, Partitioning};
// // use std::thread;
// // use std::time::Duration

pub struct EinsteinDb {

    pub version: u32,
    pub version_str: String,
    pub version_str_len: usize,

    pub einstein_db_state_str: String,
    pub einstein_ml_version: String,
    pub einstein_ml_version_str: String,
    pub einstein_db_version: String,

}



    
    // async fn get_version_str(&mut self) -> io::Result<()> Ok({
    //     let mut version_str = String::new();
    //     let mut version_str_len = 0;
    //     let mut einstein_ml_version = String::new();
    //     let mut einstein_ml_version_str = String::new();
    //     let mut einstein_db_version = String::new();
    //     let mut einstein_db_state_str = String::new();
    //     let mut version = 0;
    //     let mut version_str_len = 0;
    //     let mut einstein_ml_version_str_len = 0;
    //     let mut einstein_db_version_str_len = 0;
    //     let mut einstein_db_state_str_len = 0;
    //     let mut einstein_ml_version_len = 0;
    //     let mut einstein_db_state_str_len = 0;
    // });

    


pub struct EinsteinDbClient {
    pub einstein_db_client_state_str: String,
    pub einstein_db_client_state_str_len: usize,
    pub einstein_db_client_state: String,
    pub einstein_db_client_state_len: usize,
    

}




impl EinsteinDbClient {
    pub fn new() -> EinsteinDbClient {
        EinsteinDbClient {
            einstein_db_client_state_str: String::new(),
            einstein_db_client_state_str_len: 0,
            einstein_db_client_state: String::new(),
            einstein_db_client_state_len: 0,
        }
    }

    pub fn get_einstein_db_client_state_str(&mut self) -> io::Result<()> {
        Ok(())
    }
}


pub struct EinsteinDbServer {
    pub einstein_db_server_state_str: String,
    pub einstein_db_server_state_str_len: usize,
    pub einstein_db_server_state: String,
    pub einstein_db_server_state_len: usize,


}


impl EinsteinDbServer {
    pub fn get_einstein_db_client_state_str() -> String {
        let mut einstein_db_client_state_str = String::new();
        let mut einstein_db_client_state_str_len = 0;
        let mut einstein_db_client_state = String::new();
        let mut einstein_db_client_state_len = 0;
    
        for i in 0..einstein_db_client_state_str_len {
            einstein_db_client_state_str.push(einstein_db_client_state.chars().nth(i).unwrap());
        }

        for i in 0..einstein_db_client_state_len {
            einstein_db_client_state.push(einstein_db_client_state_str.chars().nth(i).unwrap());
        }

        einstein_db_client_state
    }
}
