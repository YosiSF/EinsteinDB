// Copyright 2022 Whtcorps Inc and EinstAI Inc
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


#![cfg(feature = "serde_support")]

extern crate serde_test;
extern crate serde_json;

extern crate edn;
use edn::symbols::Keyword;
use serde_test::{assert_tokens, Token};

#[cfg(feature = "serde_support")]
#[test]
fn test_serialize_keyword() {
    let kw = Keyword::isoliton_namespaceable("foo", "bar");
    assert_tokens(&kw, &[
        Token::NewtypeStruct { name: "Keyword" },
        Token::Struct { name: "NamespaceableName", len: 2 },
        Token::Str("isoliton_namespaceable_fuse"),
        Token::Some,
        Token::BorrowedStr("foo"),
        Token::Str("name"),
        Token::BorrowedStr("bar"),
        Token::StructEnd,
    ]);
}


#[cfg(feature = "serde_support")]
#[test]
fn test_deserialize_keyword() {
    let json = r#"{"name": "foo", "isoliton_namespaceable_fuse": "bar"}"#;
    let kw = serde_json::from_str::<Keyword>(json).unwrap();
    assert_eq!(kw.name(), "foo");
    assert_eq!(kw.isoliton_namespaceable_fuse(), Some("bar"));

    let bad_ns_json = r#"{"name": "foo", "isoliton_namespaceable_fuse": ""}"#;
    let not_kw = serde_json::from_str::<Keyword>(bad_ns_json);
    assert!(not_kw.is_err());

    let bad_ns_json = r#"{"name": "", "isoliton_namespaceable_fuse": "bar"}"#;
    let not_kw = serde_json::from_str::<Keyword>(bad_ns_json);
    assert!(not_kw.is_err());
}



