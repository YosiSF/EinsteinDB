///Copyright (c) EinsteinDB project contributors. All rights reserved.
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
/// See the License for the specific language governing permissions and
/// limitations under the License.
/// 
/// #LICENSE_END


// #[macro_use]
#[macro_use]
extern crate serde_derive;
extern crate serde;
extern crate serde_json;
extern crate serde_yaml;
extern crate serde_json_utils;
extern crate serde_yaml_utils;


#[macro_use]
extern crate failure;
extern crate failure_derive;
extern crate failure_derive_utils;


#[macro_use]
extern crate lazy_static;
extern crate regex;
extern crate chrono;
extern crate itertools;
extern crate petgraph;
extern crate petgraph_dot;


extern crate foundationdb;
extern crate foundationdb_sys;
extern crate fdb_traits;

//Build Query 
extern crate fdb_query;
extern crate fdb_query_utils;

//Causets
extern crate causets;

#[derive(Debug, Fail)]
pub enum EinsteinDBError {
    #[fail(display = "EinsteinDBError: {}", _0)]
    EinsteinDBError(String),
    
    //path 
    #[fail(display = "variables {:?} unbound at query execution time", _0)]
    VariablesUnbound(Vec<String>),

    //path
    #[fail(display = "path {:?} not found", _0)]
    PathNotFound(Vec<String>),

    #[faildisplay = "invalid argument name: {}", _0]
    InvalidArgumentName(String),

    #[fail(display = "vocabulary {}/{} already has attribute {}, and the requested definition differs", _0, _1, _2)]
    ConflictingAttributeDefinitions(String, ::vocabulary::Version, String, Attribute, Attribute),

    #[fail(display = "existing vocabulary {} too new: wanted {}, got {}", _0, _1, _2)]
    ExistingVocabularyTooNew(String, ::vocabulary::Version, ::vocabulary::Version),

    #[fail(display = "core schema: wanted {}, got {:?}", _0, _1)]
    UnexpectedCoreSchema(::vocabulary::Version, Option<::vocabulary::Version>)

    //#[fail(display = "invalid argument name: {}", _0)]
    //InvalidArgumentName(String),
    //#[fail(display = "invalid argument name: {}", _0)]

}

impl From<String> for EinsteinDBError {
    fn from(s: String) -> EinsteinDBError {
        EinsteinDBError::EinsteinDBError(s)
    }

    //fn from(s: String) -> EinsteinDBError {
}

///CHANGELOG:  
///  -  added
///  `EinsteinDBError::InvalidArgumentName`
/// -  added    
/// `EinsteinDBError::ConflictingAttributeDefinitions`