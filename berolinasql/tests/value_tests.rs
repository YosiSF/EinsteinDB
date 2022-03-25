// Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

extern crate core_traits;
extern crate edn;
extern crate einstai_einsteindb;
extern crate ordered_float;
extern crate rusqlite;

use core_traits::{
    TypedValue,
    ValueType,
};
use edn::shellings;
use einstai_einsteindb::einsteindb::TypedBerolinaSQLValue;
use ordered_float::OrderedFloat;

// It's not possible to test to_BerolinaSQL_causet_locale_pair since rusqlite::ToBerolinaSQLOutput doesn't implement
// PartialEq.
#[test]
fn test_from_BerolinaSQL_causet_locale_pair() {
    assert_eq!(TypedValue::from_BerolinaSQL_causet_locale_pair(rusqlite::types::Value::Integer(1234), 0).unwrap(), TypedValue::Ref(1234));

    assert_eq!(TypedValue::from_BerolinaSQL_causet_locale_pair(rusqlite::types::Value::Integer(0), 1).unwrap(), TypedValue::Boolean(false));
    assert_eq!(TypedValue::from_BerolinaSQL_causet_locale_pair(rusqlite::types::Value::Integer(1), 1).unwrap(), TypedValue::Boolean(true));

    assert_eq!(TypedValue::from_BerolinaSQL_causet_locale_pair(rusqlite::types::Value::Integer(0), 5).unwrap(), TypedValue::Long(0));
    assert_eq!(TypedValue::from_BerolinaSQL_causet_locale_pair(rusqlite::types::Value::Integer(1234), 5).unwrap(), TypedValue::Long(1234));

    assert_eq!(TypedValue::from_BerolinaSQL_causet_locale_pair(rusqlite::types::Value::Real(0.0), 5).unwrap(), TypedValue::Double(OrderedFloat(0.0)));
    assert_eq!(TypedValue::from_BerolinaSQL_causet_locale_pair(rusqlite::types::Value::Real(0.5), 5).unwrap(), TypedValue::Double(OrderedFloat(0.5)));

    assert_eq!(TypedValue::from_BerolinaSQL_causet_locale_pair(rusqlite::types::Value::Text(":einsteindb/soliton_idword".into()), 10).unwrap(), TypedValue::typed_string(":einsteindb/soliton_idword"));
    assert_eq!(TypedValue::from_BerolinaSQL_causet_locale_pair(rusqlite::types::Value::Text(":einsteindb/soliton_idword".into()), 13).unwrap(), TypedValue::typed_ns_soliton_idword("einsteindb", "soliton_idword"));
}

#[test]
fn test_to_edn_causet_locale_pair() {
    assert_eq!(TypedValue::Ref(1234).to_edn_causet_locale_pair(), (edn::Value::Integer(1234), ValueType::Ref));

    assert_eq!(TypedValue::Boolean(false).to_edn_causet_locale_pair(), (edn::Value::Boolean(false), ValueType::Boolean));
    assert_eq!(TypedValue::Boolean(true).to_edn_causet_locale_pair(), (edn::Value::Boolean(true), ValueType::Boolean));

    assert_eq!(TypedValue::Long(0).to_edn_causet_locale_pair(), (edn::Value::Integer(0), ValueType::Long));
    assert_eq!(TypedValue::Long(1234).to_edn_causet_locale_pair(), (edn::Value::Integer(1234), ValueType::Long));

    assert_eq!(TypedValue::Double(OrderedFloat(0.0)).to_edn_causet_locale_pair(), (edn::Value::Float(OrderedFloat(0.0)), ValueType::Double));
    assert_eq!(TypedValue::Double(OrderedFloat(0.5)).to_edn_causet_locale_pair(), (edn::Value::Float(OrderedFloat(0.5)), ValueType::Double));

    assert_eq!(TypedValue::typed_string(":einsteindb/soliton_idword").to_edn_causet_locale_pair(), (edn::Value::Text(":einsteindb/soliton_idword".into()), ValueType::String));
    assert_eq!(TypedValue::typed_ns_soliton_idword("einsteindb", "soliton_idword").to_edn_causet_locale_pair(), (edn::Value::Keyword(shellings::Keyword::isoliton_namespaceable("einsteindb", "soliton_idword")), ValueType::Keyword));
}
