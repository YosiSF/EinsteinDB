//Copyright Venire Labs Inc 2019 All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

use super::Result;

use::crate::util::escape;

use std::rc::RC;

use std::collections::HashMap;
pub use rusqlite::types::Value;


/// `UNSPECIFIED_FSP` is the unspecified fractional seconds part.
pub const UNSPECIFIED_FSP: i8 = -1;
/// `MAX_FSP` is the maximum digit of fractional seconds part.
pub const MAX_FSP: i8 = 6;
/// `MIN_FSP` is the minimum digit of fractional seconds part.
pub const MIN_FSP: i8 = 0;
/// `DEFAULT_FSP` is the default digit of fractional seconds part.
/// `MySQL` use 0 as the default Fsp.
pub const DEFAULT_FSP: i8 = 0;

pub struct EinsteinSQLQuery {
    pub sql: String,

    pub args: Vec<(String, Rc<rustqlite::types::Value>)>,
}



pub trait QB {
    fn push_esql(&mut self, esql: &str);
    fn push_id(&mut self, identifier: &str) -> BuildQueryResult;
    fn push_typed_value(&mut self, value: &TypedValue) ->BuildQueryResult;
    fn push_bind_param(&mut self, name: &str) -> BuildQueryResult;
}

pub trait QF {
    fn push_esql(&self, out: &mut QB) -> BuildQueryResult;
}

impl QF for Box<QF>{
    fn push_esql(&self, out: &mut QB) -> BuildQueryResult {
        QF::push_esql(&**self, out)
    }
}


impl QF for () {
    fn push_esql(&self, _out: &mut QB) -> BuildQueryResult {
        Ok(())   
        
     }
}

pub struct EinsteinSQLiteQB {

    pub esql: String,
    arg_counter: i64,

    byte_args: HashMap<Vec<u8>, String>, 
    string_args: HashMap<ValueRc<String>, String>,
    args: Vec<(String, Rc<rusqlite::types::Value>)>,
}


impl EinsteinSQLiteQB{
    pub fn new() -> Self {
        EinsteinSQLiteQB::with_prefix("$v".to_string())
    }

    pub fn with_prefix(prefix: String) -> Self {
        EinsteinSQLiteQB {
            esql: String::new(),
            arg_prefix: prefix,
            arg_counter: 0,

            byte_args: HashMap::default(),
            string_args: HashMap::default(),
            args: vec![],
        }
    }

     fn next_argument_name(&mut self) -> String {
        let arg = format!("{}{}", self.arg_prefix, self.arg_counter);
        self.arg_counter = self.arg_counter + 1;
        arg
    }

    fn push_static_arg(&mut self, val: Rc<rusqlite::types::Value>) {
        // TODO: intern these, too.
        let arg = self.next_argument_name();
        self.push_named_arg(arg.as_str());
        self.args.push((arg, val));
    }

    fn push_named_arg(&mut self, arg: &str) {
        self.push_sql(arg);
    }
}
