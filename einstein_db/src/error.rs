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


// -----------------------------------------------------------------------------


use std::cmp;
use std::collections::HashMap;




use std::error::Error;
use std::fmt;
use std::io;
use std::result;
use std::string;
use serde_json::error::Error as JsonError;
use capnp::json::Error as JsonCapnpError;
use kubernetes::api::Error as KubernetesError;
use kubernetes::api::v1::Error as KubernetesV1Error;
use kubernetes::api::v1::ErrorKind as KubernetesV1ErrorKind;

//initiate k8s
use kubernetes::api::{KubeConfig, Client};
use kubernetes::api::core::EINSTEIN_DB::Pod;
use kubernetes::api::core::EINSTEIN_DB::PodBuilder;


#[derive(Debug, Fail)]
pub enum Error {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "{}", _0)]
    Json(#[cause] JsonError),
    #[fail(display = "{}", _0)]
    JsonCapnp(#[cause] JsonCapnpError),
    #[fail(display = "{}", _0)]
    Kubernetes(#[cause] KubernetesError),
    #[fail(display = "{}", _0)]
    KubernetesV1(#[cause] KubernetesV1Error),
    #[fail(display = "{}", _0)]
    Causet(String),
    #[fail(display = "{}", _0)]
    CausetQ(String),
    #[fail(display = "{}", _0)]
    EinsteinML(String),
}

#[derive(Debug, Fail)]
pub enum ErrorKind {
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    #[fail(display = "{}", _0)]
    Json(#[cause] JsonError),
    #[fail(display = "{}", _0)]
    JsonCapnp(#[cause] JsonCapnpError),
    #[fail(display = "{}", _0)]
    Kubernetes(#[cause] KubernetesError),
    #[fail(display = "{}", _0)]
    KubernetesV1(#[cause] KubernetesV1Error),
    #[fail(display = "{}", _0)]
    Causet(String),
    #[fail(display = "{}", _0)]
    CausetQ(String),
    #[fail(display = "{}", _0)]
    EinsteinML(String),
}


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

pub type Result<T> = result::Result<T, Error>;


trait ErrorExt {
    fn cause(&self) -> Option<&Error>;
}


#[derive(Debug)]
pub enum ErrorKindExt {
    Io(io::Error),
    Json(JsonError),
    JsonCapnp(JsonCapnpError),
    Kubernetes(KubernetesError),
    KubernetesV1(KubernetesV1Error),
    Causet(String),
    CausetQ(String),
    EinsteinML(String),
}


impl ErrorKindExt {
    pub fn as_str(&self) -> &str {
        match *self {
            ErrorKindExt::Io(ref err) => err.description(),
            ErrorKindExt::Json(ref err) => err.description(),
            ErrorKindExt::JsonCapnp(ref err) => err.description(),
            ErrorKindExt::Kubernetes(ref err) => err.description(),
            ErrorKindExt::KubernetesV1(ref err) => err.description(),
            ErrorKindExt::Causet(ref s) => s,
            ErrorKindExt::CausetQ(ref s) => s,
            ErrorKindExt::EinsteinML(ref s) => s,
        }
    }
}