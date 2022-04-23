//Copyright 2021-2023 WHTCORPS INC ALL RIGHTS RESERVED. APACHE 2.0 COMMUNITY EDITION SL
// AUTHORS: WHITFORD LEDER
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

//! Error handling.
//!
//! This module contains the error types that can be returned by EinsteinDB.
//!
//! # Error handling
//!
//! All errors returned by EinsteinDB are of one of the types defined in this module.
//!
//! # Example
//!
//! ```
//! use EinsteinDB::error::Error;
//!
//! // An error returned by a function.
//! let err = Error::from(Error::Io(io::Error::new(io::ErrorKind::Other, "oh no!")));
//!
//! // An error returned by a function.
//! let err = Error::from(Error::Io(io::Error::new(io::ErrorKind::Other, "oh no!")));
//!

use std::error::Error;
use std::fmt;
use std::io;
use std::result;
use std::string;
use serde_json::error::Error as JsonError;
use capnp::json::Error as JsonCapnpError;
use kubernetes::api::Error as KubernetesError;

//initiate k8s
use kubernetes::api::{KubeConfig, Client};
use kubernetes::api::core::v1::Pod;
use kubernetes::api::core::v1::PodBuilder;


#[derive(Debug)]
#[allow(missing_copy_implementations)]
pub enum K8sEinsteindbStatus {
     PodNotFound,
     PodNotReady,
     PodNotScheduled,
     PodNotRunning,
     PodNotScheduledAndNotRunning,
     PodNotScheduledAndNotRunningAndNotReady,

}



#[derive(Debug)]
pub enum EinsteinDBError {
    Io(io::Error),
    Codec(codec::Error),
    Other(String),
}


impl fmt::Display for EinsteinDBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            EinsteinDBError::Io(ref err) => write!(f, "Io error: {}", err),
            EinsteinDBError::Codec(ref err) => write!(f, "Codec error: {}", err),
            EinsteinDBError::Other(ref s) => write!(f, "Other error: {}", s),
        }
    }

}


impl Error for EinsteinDBError {
    fn description(&self) -> &str {
        match *self {
            EinsteinDBError::Io(ref err) => err.description(),
            EinsteinDBError::Codec(ref err) => err.description(),
            EinsteinDBError::Other(ref s) => s,
        }
    }

    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            EinsteinDBError::Io(ref err) => write!(f, "IO error: {}", err),

            EinsteinDBError::Codec(ref err) => write!(f, "Codec error: {}", err),
            EinsteinDBError::Other(ref s) => write!(f, "Other error: {}", s),
        }
    }
}
use failure::Fail;

#[derive(Debug, Fail)]
pub enum DataTypeError {
    #[fail(display = "Unsupported type: {}", name)]

    UnsupportedType { name: String },
}


impl From<io::Error> for EinsteinDBError {
    fn from(err: io::Error) -> EinsteinDBError {
        EinsteinDBError::Io(err)
    }


}
