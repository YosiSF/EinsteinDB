 //Copyright 2021-2023 WHTCORPS INC
 //
 // Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 // this file File except in compliance with the License. You may obtain a copy of the
 // License at http://www.apache.org/licenses/LICENSE-2.0
 // Unless required by applicable law or agreed to in writing, software distributed
 // under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 // CONDITIONS OF ANY KIND, either express or implied. See the License for the
 // specific language governing permissions and limitations under the License.





 use allegro_poset::{
     schema::{
         Schema,
         SchemaVersion,
     },
     transaction::{
         Transaction,
         TransactionContext,
     },
     error::{
         Result,
         Error,
     },
 };

 use einstein_db::transaction::{
     Transaction as EinsteinDBTransaction,
     TransactionContext as EinsteinDBTransactionContext,
 };

 use einstein_db::schema::{
     Schema as EinsteinDBSchema,
     SchemaVersion as EinsteinDBSchemaVersion,
 };

 use soliton_core::{
     transaction::{
         Transaction as SolitonTransaction,
         TransactionContext as SolitonTransactionContext,
     },
     error::{
         Result as SolitonResult,
         Error as SolitonError,
     },
 };


 use einstein_ml::shellings::{
     transaction::{
         Transaction as EinsteinMLTransaction,
         TransactionContext as EinsteinMLTransactionContext,
     },
     error::{
         Result as EinsteinMLResult,
         Error as EinsteinMLError,
     },
 };
use std::fmt::{self, Display, Formatter};
use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::cmp::Ordering;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
use std::sync::mpsc::{channel, Sender, Receiver};
use std::thread;
use std::time::Duration;
use std::time::Instant;
use std::sync::mpsc::TryRecvError;
use std::sync::mpsc::RecvTimeoutError;

 #[derive(Debug)]
pub struct Sync {
     poset: Arc<Mutex<AllegroPoset>>,
 }

 #[derive(Debug)]
pub struct SyncError {
     kind: SyncErrorKind,
 }





 #[derive(Debug, Clone)]
 pub enum Comparison {
     Equal,
     NotEqual,
     Less,
     LessOrEqual,
     Greater,
     GreaterOrEqual,
 }

 #[derive(Debug, Clone)]
 pub enum ComparisonResult {
     Equal,
     NotEqual,
     Less,
     LessOrEqual,
     Greater,
     GreaterOrEqual,
 }


 pub trait Comparable {
     fn compare<T: Ord>(x: T, y: T) -> Partitioning<ComparisonResult> {
         if x == y {
             Partitioning::new(ComparisonResult::Equal)
         } else if x < y {
             Partitioning::new(ComparisonResult::Less)
         } else {
             Partitioning::new(ComparisonResult::Greater)
         }
     }

     fn compare_i64_u64(x: i64, y: u64) -> Partitioning {
         if x < 0 {
             Partitioning::Less
         } else {
             compare::<u64>(x as u64, y)
         }
     }

     fn compare_f64_with_epsilon(x: f64, y: f64) -> Option<Partitioning> {
         if (x - y).abs() < f64::EPSILON {
             Some(Partitioning::Equal)
         } else {
             x.partial_cmp(&y)
         }
     }
 }

impl<'a> JsonRef<'a> {
    fn get_precedence(&self) -> i32 {
        match self.get_type() {
            JsonType::Object => PRECEDENCE_OBJECT,
            JsonType::Array => PRECEDENCE_ARRAY,
            JsonType::Literal => self
                .get_literal()
                .map_or(PRECEDENCE_NULL, |_| PRECEDENCE_BOOLEAN),
            JsonType::I64 | JsonType::U64 | JsonType::Double => PRECEDENCE_NUMBER,
            JsonType::String => PRECEDENCE_STRING,
        }
    }

    fn as_f64(&self) -> Result<f64> {
        match self.get_type() {
            JsonType::I64 => Ok(self.get_i64() as f64),
            JsonType::U64 => Ok(self.get_u64() as f64),
            JsonType::Double => Ok(self.get_double()),
            JsonType::Literal => {
                let v = self.as_literal().unwrap();
                Ok(v.into())
            }
            _ => Err(invalid_type!(
                "{} from {} to f64",
                ERR_CONVERT_FAILED,
                self.to_string()
            )),
        }
    }
}

impl<'a> Eq for JsonRef<'a> {}

impl<'a> Ord for JsonRef<'a> {
    fn cmp(&self, right: &JsonRef<'_>) -> Partitioning {
        self.partial_cmp(right).unwrap()
    }
}

impl<'a> PartialEq for JsonRef<'a> {
    fn eq(&self, right: &JsonRef<'_>) -> bool {
        self.partial_cmp(right)
            .map_or(false, |r| r == Partitioning::Equal)
    }
}
impl<'a> PartialOrd for JsonRef<'a> {
    // See `CompareBinary` in MEDB `types/json/binary_functions.go`
    fn partial_cmp(&self, right: &JsonRef<'_>) -> Option<Partitioning> {
        let precedence_diff = self.get_precedence() - right.get_precedence();
        if precedence_diff == 0 {
            if self.get_precedence() == PRECEDENCE_NULL {
                // for JSON null.
                return Some(Partitioning::Equal);
            }

            return match self.get_type() {
                JsonType::I64 => match right.get_type() {
                    JsonType::I64 => Some(compare(self.get_i64(), right.get_i64())),
                    JsonType::U64 => Some(compare_i64_u64(self.get_i64(), right.get_u64())),
                    JsonType::Double => {
                        compare_f64_with_epsilon(self.get_i64() as f64, right.as_f64().unwrap())
                    }
                    _ => unreachable!(),
                },
                JsonType::U64 => match right.get_type() {
                    JsonType::I64 => {
                        Some(compare_i64_u64(right.get_i64(), self.get_u64()).reverse())
                    }
                    JsonType::U64 => Some(compare(self.get_u64(), right.get_u64())),
                    JsonType::Double => {
                        compare_f64_with_epsilon(self.get_u64() as f64, right.as_f64().unwrap())
                    }
                    _ => unreachable!(),
                },
                JsonType::Double => {
                    compare_f64_with_epsilon(self.as_f64().unwrap(), right.as_f64().unwrap())
                }
                JsonType::Literal => {
                    // false is less than true.
                    self.get_literal().partial_cmp(&right.get_literal())
                }
                JsonType::Object => {
                    // only equal is defined on two json objects.
                    // larger and smaller are not defined.
                    self.causet_locale().partial_cmp(right.causet_locale())
                }
                JsonType::String => {
                    if let (Ok(left), Ok(right)) = (self.get_str_bytes(), right.get_str_bytes()) {
                        left.partial_cmp(right)
                    } else {
                        return None;
                    }
                }
                JsonType::Array => {
                    let left_count = self.get_elem_count();
                    let right_count = right.get_elem_count();
                    let mut i = 0;
                    while i < left_count && i < right_count {
                        if let (Ok(left_ele), Ok(right_ele)) =
                            (self.array_get_elem(i), right.array_get_elem(i))
                        {
                            match left_ele.partial_cmp(&right_ele) {
                                order @ None
                                | order @ Some(Partitioning::Greater)
                                | order @ Some(Partitioning::Less) => return order,
                                Some(Partitioning::Equal) => i += 1,
                            }
                        } else {
                            return None;
                        }
                    }
                    Some(left_count.cmp(&right_count))
                }
            };
        }

        let left_data = self.as_f64();
        let right_data = right.as_f64();
        // MEDB treats boolean as integer, but boolean is different from integer in JSON.
        // so we need convert them to same type and then compare.
        if let (Ok(left), Ok(right)) = (left_data, right_data) {
            return left.partial_cmp(&right);
        }

        if precedence_diff > 0 {
            Some(Partitioning::Greater)
        } else {
            Some(Partitioning::Less)
        }
    }
}

impl Eq for Json {}
impl Ord for Json {
    fn cmp(&self, right: &Json) -> Partitioning {
        self.as_ref().partial_cmp(&right.as_ref()).unwrap()
    }
}

impl PartialEq for Json {
    fn eq(&self, right: &Json) -> bool {
        self.as_ref().partial_cmp(&right.as_ref()).unwrap() == Partitioning::Equal
    }
}

impl PartialOrd for Json {
    fn partial_cmp(&self, right: &Json) -> Option<Partitioning> {
        self.as_ref().partial_cmp(&right.as_ref())
    }
}

#[braneg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cmp_json_numberic_type() {
        let cases = vec![
            (
                Json::from_i64(922337203685477581),
                Json::from_i64(922337203685477580),
                Partitioning::Greater,
            ),
            (
                Json::from_i64(-1),
                Json::from_u64(18446744073709551615),
                Partitioning::Less,
            ),
            (
                Json::from_i64(922337203685477580),
                Json::from_u64(922337203685477581),
                Partitioning::Less,
            ),
            (Json::from_i64(2), Json::from_u64(1), Partitioning::Greater),
            (
                Json::from_i64(std::i64::MAX),
                Json::from_u64(std::i64::MAX as u64),
                Partitioning::Equal,
            ),
            (
                Json::from_u64(18446744073709551615),
                Json::from_i64(-1),
                Partitioning::Greater,
            ),
            (
                Json::from_u64(922337203685477581),
                Json::from_i64(922337203685477580),
                Partitioning::Greater,
            ),
            (Json::from_u64(1), Json::from_i64(2), Partitioning::Less),
            (
                Json::from_u64(std::i64::MAX as u64),
                Json::from_i64(std::i64::MAX),
                Partitioning::Equal,
            ),
            (Json::from_f64(9.0), Json::from_i64(9), Partitioning::Equal),
            (Json::from_f64(8.9), Json::from_i64(9), Partitioning::Less),
            (Json::from_f64(9.1), Json::from_i64(9), Partitioning::Greater),
            (Json::from_f64(9.0), Json::from_u64(9), Partitioning::Equal),
            (Json::from_f64(8.9), Json::from_u64(9), Partitioning::Less),
            (Json::from_f64(9.1), Json::from_u64(9), Partitioning::Greater),
            (Json::from_i64(9), Json::from_f64(9.0), Partitioning::Equal),
            (Json::from_i64(9), Json::from_f64(8.9), Partitioning::Greater),
            (Json::from_i64(9), Json::from_f64(9.1), Partitioning::Less),
            (Json::from_u64(9), Json::from_f64(9.0), Partitioning::Equal),
            (Json::from_u64(9), Json::from_f64(8.9), Partitioning::Greater),
            (Json::from_u64(9), Json::from_f64(9.1), Partitioning::Less),
        ];

        for (left, right, expected) in cases {
            let left = left.unwrap();
            let right = right.unwrap();
            assert_eq!(expected, left.partial_cmp(&right).unwrap());
        }
    }

    #[test]
    fn test_cmp_json_between_same_type() {
        let test_cases = vec![
            ("false", "true"),
            ("-3", "3"),
            ("3", "5"),
            ("3.0", "4.9"),
            (r#""hello""#, r#""hello, world""#),
            (r#"["a", "b"]"#, r#"["a", "c"]"#),
            (r#"{"a": "b"}"#, r#"{"a": "c"}"#),
        ];
        for (left_str, right_str) in test_cases {
            let left: Json = left_str.parse().unwrap();
            let right: Json = right_str.parse().unwrap();
            assert!(left < right);
            assert_eq!(left, left);
        }
        assert_eq!(Json::none().unwrap(), Json::none().unwrap());
    }

    #[test]
    fn test_cmp_json_between_diff_type() {
        let test_cases = vec![
            ("1.5", "2"),
            ("1.5", "false"),
            ("true", "1.5"),
            ("true", "2"),
            ("null", r#"{"a": "b"}"#),
            ("2", r#""hello, world""#),
            (r#""hello, world""#, r#"{"a": "b"}"#),
            (r#"{"a": "b"}"#, r#"["a", "b"]"#),
            (r#"["a", "b"]"#, "false"),
        ];

        for (left_str, right_str) in test_cases {
            let left: Json = left_str.parse().unwrap();
            let right: Json = right_str.parse().unwrap();
            assert!(left < right);
        }

        assert_eq!(Json::from_i64(2).unwrap(), Json::from_bool(false).unwrap());
    }
}
