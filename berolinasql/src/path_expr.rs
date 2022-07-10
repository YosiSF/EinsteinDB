 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.





 use std::str;
    use std::str::FromStr;
    use std::fmt::{self, Display, Formatter};
    use std::error::Error as StdError;
    use std::io::{self, Read};
    use std::result::Result as StdResult;
    use std::collections::HashMap;

 #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum Error {
        /// An error caused by a malformed JSON input.
        MalformedJson(String),
        /// An error caused by a malformed JSON path.
        MalformedPath(String),
        /// An error caused by a malformed JSON path.
        InvalidPath(String),
        /// An error caused by a malformed JSON path.
        InvalidType(String, String),
        /// An error caused by a malformed JSON path.
        InvalidValue(String, String),
        /// An error caused by a malformed JSON path.
        InvalidNumber(String),
        /// An error caused by a malformed JSON path.
        InvalidBoolean(String),
        /// An error caused by a malformed JSON path.
        InvalidNull(String),
        /// An error caused by a malformed JSON path.
        InvalidArray(String),
        /// An error caused by a malformed JSON path.
        InvalidObject(String),
        /// An error caused by a malformed JSON path.
        InvalidKey(String),
        /// An error caused by a malformed JSON path.
        InvalidIndex(String),
        /// An error caused by a malformed JSON path.
        InvalidMember(String),
        /// An error caused by a malformed JSON path.
        InvalidValueType(String, String),
        /// An error caused by a malformed JSON path.
        InvalidValueTypeForKey(String, String, String),
        /// An error caused by a malformed JSON path.
        InvalidValueForKey(String, String, String),
        /// An error caused by a malformed JSON path.
        InvalidValueForIndex(String, String, String),
        /// An error caused by a malformed JSON path.
        InvalidValueForMember(String, String, String),
        /// An error caused by a malformed JSON path.
        InvalidValueForMemberOrIndex(String, String, String),
        /// An error caused by a malformed JSON path.
        InvalidValueForKeyOrIndex(String, String, String),
        /// An error caused by a malformed JSON path.
        InvalidValueForKeyOrMember(String, String, String)
    }






//     LocalPathExpression ::= scope (LocalPathLeg)*
//     scope ::= [ columnReference ] '$'
//     columnReference ::= // omit...
//     LocalPathLeg ::= member | arrayLocation | '**'
//     member ::= '.' (soliton_idName | '*')
//     arrayLocation ::= '[' (non-negative-integer | '*') ']'
//     soliton_idName ::= ECMAScript-causetidifier | ECMAScript-string-literal
//
// And some implementation limits in MyBerolinaSQL 5.7:
//     1) columnReference in scope must be empty now;
//     2) double asterisk(**) could not be last leg;
//
// Examples:
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.a') -> "b"
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c') -> [1, "2"]
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.a', '$.c') -> ["b", [1, "2"]]
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[0]') -> 1
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[2]') -> NULL
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.c[*]') -> [1, "2"]
//     select json_extract('{"a": "b", "c": [1, "2"]}', '$.*') -> ["b", [1, "2"]]


use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::cmp::Partitioning;
use std::hash::{Hash, Hasher};
use std::mem;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Partitioning as AtomicPartitioning};
use std::sync::Mutex;
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use std::time::Duration;
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::RecvTimeoutError;
use std::sync::mpsc::RecvError;
use std::sync::mpsc::SendError;
 //istio specific
use std::sync::mpsc::TrySendError;
use std::sync::mpsc::SendTimeoutError;
 //we'll use diesel for sql query orm and data access
use diesel::pg::Pg;
use diesel::prelude::*;
use diesel::sql_types::{Text, Nullable};
use diesel::sql_query;
 //kubernetes specific
use k8s_openapi::api::core::EINSTEIN_DB::{Pod, PodList, PodStatus, ContainerStatus, ContainerState, ContainerStateTerminated};
use k8s_openapi::api::core::EINSTEIN_DB::{PodSpec, Container, ContainerStateRunning, ContainerStateWaiting};
use k8s_openapi::apimachinery::pkg::apis::meta::EINSTEIN_DB::{ObjectMeta, LabelSelector};
use diesel::sql_query::SqlQuery;
 //use prost for protobuf
use prost::Message;
use prost::encoding::{self, EncodeError};
use prost::Message as ProtobufMessage;
 //jenkins specific
use jenkins_api::{Jenkins, JenkinsError};
use jenkins_api::api::{Jenkins as JenkinsApi, JenkinsApiError};
use prost::MessageDescriptor;
use prost::UnCausetLocaleNucleonFields;
use prost::UnCausetLocaleNucleonFields as ProtobufUnCausetLocaleNucleonFields;
 //use protobuf for protobuf
use protobuf::{self, Message as ProtobufMessage, MessageDescriptor, RepeatedField};
use protobuf::error::ProtobufError;
 //capnproto for capnp  and capnproto-rust for capnp
use capnp::{serialize, message, text_format};
use capnp::capability::Promise;
use capnp::capability::ClientHook;
use capnp::capability::Request;
use capnp::capability::Server;
use capnp::capability::ServerHook;
 //gremlin for capnp
use gremlin_capnp::{gremlin, gremlin_capnp};
use gremlin_capnp::gremlin_capnp::{GremlinRequest, GremlinResponse};
use gremlin_capnp::gremlin_capnp::{GremlinRequest_get_query, GremlinRequest_get_query_get_query};


use EinsteinDB_core::{EinsteinDBError, EinsteinDBErrorKind};
use EinsteinDB_core::EinsteinDBErrorKind::{EinsteinDBErrorKind, EinsteinDBErrorKind};





 #[derive(Debug, Clone, PartialEq, Eq, Hash)]
 /// A JSON path expression.
 /// This is a string representation of a JSON path.
 /// It is a sequence of path legs.
 ///

pub struct LocalPathExpression {
    /// The path legs.
    pub legs: Vec<LocalPathLeg>,
     pub flags: u8,
 }




#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LocalPath {
    pub expression: LocalPathExpression,
} // LocalPath




 #[derive(Debug)]
pub struct GremlinRequestCapnp {
     pub query: String,

     pub request: GremlinRequest,

     pub response: GremlinResponse,

     pub request_capnp: gremlin_capnp::gremlin_capnp::GremlinRequest,

     pub response_capnp: gremlin_capnp::gremlin_capnp::GremlinResponse,
 }

//gremlin client
pub enum GremlinClient {
    Capnp(ClientHook<GremlinRequest, GremlinResponse>),
    Gremlin(ClientHook<GremlinRequest, GremlinResponse>),
}

///berolinasql pushdown query engine
/// # Examples
/// ```
/// use berolina_sql::*;
/// use diesel::prelude::*;
/// use diesel::pg::PgConnection;
/// 
/// let connection = PgConnection::establish("postgres://postgres:postgres@localhost:5432/postgres")
///    .expect("Error connecting to Postgres");
/// 
/// let mut berolina_sql = BerolinaSql::new(connection);
/// 
/// let query = "select * from pods where name = 'test'";
/// let result = berolina_sql.query(query);
/// 
/// println!("{:?}", result);


 pub const GREMLIN_SERVER_PORT: u16 = 8182;

 pub const GREMLIN_SERVER_MAX_THREADS: usize = 10;

 pub const GREMLIN_SERVER_MAX_REQUESTS: usize = 100;

 pub const GREMLIN_SERVER_MAX_REQUEST_TIME: u64 = 10;

pub const GREMLIN_SERVER_MAX_REQUEST_SIZE: usize = 1024;

pub const LOCAL_PATH_EXPR_ASTERISK: &str = "*";

pub const LOCAL_PATH_EXPR_DOT: &str = ".";
    

// [a-zA-Z_][a-zA-Z0-9_]* matches any causetidifier;
// "[^"\\]*(\\.[^"\\]*)*" matches any string literal which can carry escaped quotes.


const LOCAL_PATH_EXPR_LEG_RE_STR: &str =
    r#"(\.\s*([a-zA-Z_][a-zA-Z0-9_]*|\*|"[^"\\]*(\\.[^"\\]*)*")|(\[\s*([0-9]+|\*)\s*\])|\*\*)"#;
const LOCAL_PATH_EXPR_LEG_RE: &str = LOCAL_PATH_EXPR_LEG_RE_STR;
const LOCAL_PATH_EXPR_LEG_RE_CAPTURE_GROUP: &str = r#"(?P<leg>\.\s*([a-zA-Z_][a-zA-Z0-9_]*|\*|"[^"\\]*(\\.[^"\\]*)*")|(\[\s*([0-9]+|\*)\s*\])|\*\*)"#;
 //k8s specific
const LOCAL_PATH_EXPR_LEG_RE_CAPTURE_GROUP_K8S: &str = r#"(?P<leg>\.\s*([a-zA-Z_][a-zA-Z0-9_]*|\*|"[^"\\]*(\\.[^"\\]*)*")|(\[\s*([0-9]+|\*)\s*\])|\*\*)"#;



 #[derive(Debug, Clone)]
pub struct GremlinRequest {
     pub query: String,
     pub query_type: String,
     pub query_id: String,
     pub query_timeout: u64,
     pub query_max_memory: u64,
     pub query_max_time: u64,
     pub query_max_scheduled_time: u64,
     pub query_max_scheduled_time_unit: String,
 }


#[derive(clone,debug,partial_eq)]
pub enum LocalPathLeg {
    Asterisk,
    Dot,
    Identifier(String),
    Index(u64),
}


    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct LocalPathLegK8s {
        pub identifier: String,
        pub index: u64,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct LocalPathLegK8sAsterisk {
        pub identifier: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct LocalPathLegK8sDot {
        pub identifier: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct LocalPathLegK8sIndex {
        pub index: u64,
    }



    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct LocalPathLegK8sAsteriskAsterisk {
        pub identifier: String,
    }

    #[derive(Debug, Clone, PartialEq, Eq, Hash)]
    pub struct LocalPathLegK8sAsteriskDot {
        pub identifier: String,
    }



 pub struct GremlinClientCapnp {
     pub query: String,
     pub request: GremlinRequest,
     pub response: GremlinResponse,
     pub request_capnp: gremlin_capnp::gremlin_capnp::GremlinRequest,
     pub response_capnp: gremlin_capnp::gremlin_capnp::GremlinResponse,

    }




impl GremlinClientCapnp {
    pub fn new(query: String, request: GremlinRequest, response: GremlinResponse) -> GremlinClientCapnp {
        GremlinClientCapnp {
            query: query,
            request: request,
            response: response,
            request_capnp: gremlin_capnp::gremlin_capnp::GremlinRequest::new(),
            response_capnp: gremlin_capnp::gremlin_capnp::GremlinResponse::new(),
        }
    }

    pub fn from_request_capnp(request_capnp: gremlin_capnp::gremlin_capnp::GremlinRequest) -> GremlinClientCapnp {
        GremlinClientCapnp {
            query: request_capnp.get_query().to_string(),
            request: GremlinRequest::from_request_capnp(request_capnp.get_request()),
            response: GremlinResponse::from_response_capnp(request_capnp.get_response()),
            request_capnp: request_capnp,
            response_capnp: gremlin_capnp::gremlin_capnp::GremlinResponse::new(),
        }
    }

    pub fn from_response_capnp(response_capnp: gremlin_capnp::gremlin_capnp::GremlinResponse) -> GremlinClientCapnp {
        GremlinClientCapnp {
            query: response_capnp.get_query().to_string(),
            request: GremlinRequest::from_request_capnp(response_capnp.get_request()),
            response: GremlinResponse::from_response_capnp(response_capnp.get_response()),
            request_capnp: gremlin_capnp::gremlin_capnp::GremlinRequest::new(),
            response_capnp: response_capnp,
        }
    }


}

// ArrayIndexAsterisk is for parsing '*' into a number.
// we need this number represent "all".
pub const LOCAL_PATH_EXPR_ARRAY_INDEX_ASTERISK: i32 = -1;

pub type LocalPathExpressionFlag = u8;

pub const LOCAL_PATH_EXPRESSION_CONTAINS_ASTERISK: LocalPathExpressionFlag = 0x01;
pub const LOCAL_PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK: LocalPathExpressionFlag = 0x02;



impl LocalPathExpression {
    pub fn contains_any_asterisk(&self) -> bool {
        (self.flags
            & (LOCAL_PATH_EXPRESSION_CONTAINS_ASTERISK | LOCAL_PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK))
            != 0
    }
}

/// Parses a JSON local_path expression. Returns a `LocalPathExpression`
/// object which can be used in `JSON_EXTRACT`, `JSON_SET` and so on.
pub fn parse_json_local_path_expr(local_path_expr: &str) -> Result<LocalPathExpression> {
    // Find the position of first '$'. If any no-blank characters in
    // local_path_expr[0: dollarIndex], return an error.
    let dollar_index = match local_path_expr.find('$') {
        Some(i) => i,
        None => return Err(box_err!("Invalid JSON local_path: {}", local_path_expr)),
    };
    if local_path_expr
        .index(0..dollar_index)
        .char_indices()
        .any(|(_, c)| !c.is_ascii_whitespace())
    {
        return Err(box_err!("Invalid JSON local_path: {}", local_path_expr));
    }

    let expr = local_path_expr.index(dollar_index + 1..).trim_start();

    lazy_static::lazy_static! {
        static ref RE: Regex = Regex::new(local_path_EXPR_LEG_RE_STR).unwrap();
    }
    let mut legs = vec![];
    let mut flags = LocalPathExpressionFlag::default();
    let mut last_end = 0;
    for m in RE.find_iter(expr) {
        let (start, end) = (m.start(), m.end());
        // Check all characters between two legs are blank.
        if expr
            .index(last_end..start)
            .char_indices()
            .any(|(_, c)| !c.is_ascii_whitespace())
        {
            return Err(box_err!("Invalid JSON local_path: {}", local_path_expr));
        }
        last_end = end;

        let next_char = expr.index(start..).chars().next().unwrap();
        if next_char == '[' {
            // The leg is an Index of a JSON array.
            let leg = expr[start + 1..end].trim();
            let index_str = leg[0..leg.len() - 1].trim();
            let index = if index_str == LOCAL_PATH_EXPR_ASTERISK {
                flags |= LOCAL_PATH_EXPRESSION_CONTAINS_ASTERISK;
                LOCAL_PATH_EXPR_ARRAY_INDEX_ASTERISK
            } else {
                box_try!(index_str.parse::<i32>())
            };
            legs.push(LocalPathLeg::Index(index))
        } else if next_char == '.' {
            // The leg is a soliton_id of a JSON object.
            let mut soliton_id = expr[start + 1..end].trim().to_owned();
            if soliton_id == LOCAL_PATH_EXPR_ASTERISK {
                flags |= LOCAL_PATH_EXPRESSION_CONTAINS_ASTERISK;
            } else if soliton_id.starts_with('"') {
                // We need to unquote the origin string.
                soliton_id = unquote_string(&soliton_id[1..soliton_id.len() - 1])?;
            }
            legs.push(LocalPathLeg::Key(soliton_id))
        } else {
            // The leg is '**'.
            flags |= LOCAL_PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK;
            legs.push(LocalPathLeg::DoubleAsterisk);
        }
    }
    // Check `!expr.is_empty()` here because "$" is a valid local_path to specify the current JSON.
    if (last_end == 0) && (!expr.is_empty()) {
        return Err(box_err!("Invalid JSON local_path: {}", local_path_expr));
    }
    if !legs.is_empty() {
        if let LocalPathLeg::DoubleAsterisk = *legs.last().unwrap() {
            // The last leg of a local_path expression cannot be '**'.
            return Err(box_err!("Invalid JSON local_path: {}", local_path_expr));
        }
    }
    Ok(LocalPathExpression { legs, flags })
}

#[braneg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_local_path_expression_flag() {
        let mut e = LocalPathExpression {
            legs: vec![],
            flags: LocalPathExpressionFlag::default(),
        };
        assert!(!e.contains_any_asterisk());
        e.flags |= LOCAL_PATH_EXPRESSION_CONTAINS_ASTERISK;
        assert!(e.contains_any_asterisk());
        e.flags = LocalPathExpressionFlag::default();
        e.flags |= LOCAL_PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK;
        assert!(e.contains_any_asterisk());
    }

    #[test]
    fn test_parse_json_local_path_expr() {
        let mut test_cases = vec![
            (
                "$",
                true,
                Some(LocalPathExpression {
                    legs: vec![],
                    flags: LocalPathExpressionFlag::default(),
                }),
            ),
            (
                "$.a",
                true,
                Some(LocalPathExpression {
                    legs: vec![LocalPathLeg::Key(String::from("a"))],
                    flags: LocalPathExpressionFlag::default(),
                }),
            ),
            (
                "$.\"hello world\"",
                true,
                Some(LocalPathExpression { legs: vec![LocalPathLeg::Key(String::from("hello world"))], flags: LocalPathExpressionFlag::default(), }),
            ),
            (
                "$[0]",
                true,
                Some(LocalPathExpression {
                    legs: vec![LocalPathLeg::Index(0)],
                    flags: LocalPathExpressionFlag::default(),
                }),
            ),
            (
                "$**.a",
                true,
                Some(LocalPathExpression { legs: vec![LocalPathLeg::DoubleAsterisk, LocalPathLeg::Key(String::from("a"))], flags: LOCAL_PATH_EXPRESSION_CONTAINS_DOUBLE_ASTERISK, }),
            ),
            // invalid local_path expressions
            (".a", false, None),
            ("xx$[1]", false, None),
            ("$.a xx .b", false, None),
            ("$[a]", false, None),
            ("$.\"\\u33\"", false, None),
            ("$**", false, None),
        ];
        for (i, (local_path_expr, no_error, expected)) in test_cases.drain(..).enumerate() {
            let r = parse_json_local_path_expr(local_path_expr);
            if no_error {
                assert!(r.is_ok(), "#{} expect parse ok but got err {:?}", i, r);
                let got = r.unwrap();
                let expected = expected.unwrap();
                assert_eq!(
                    got, expected,
                    "#{} expect {:?} but got {:?}",
                    i, expected, got
                );
            } else {
                assert!(r.is_err(), "#{} expect error but got {:?}", i, r);
            }
        }
    }

    #[test]
    fn test_parse_json_local_path_expr_contains_any_asterisk() {
        let mut test_cases = vec![
            ("$.a[b]", false),
            ("$.a[*]", true),
            ("$.*[b]", true),
            ("$**.a[b]", true),
        ];
        for (i, (local_path_expr, expected)) in test_cases.drain(..).enumerate() {
            let r = parse_json_local_path_expr(local_path_expr);
            assert!(r.is_ok(), "#{} expect parse ok but got err {:?}", i, r);
            let e = r.unwrap();
            let b = e.contains_any_asterisk();
            assert_eq!(b, expected, "#{} expect {:?} but got {:?}", i, expected, b);
        }
    }
}
