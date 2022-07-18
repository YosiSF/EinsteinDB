// Copyright 2018-Present EinsteinDB — A Relativistic Causal Consistent Hybrid OLAP/OLTP Database
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions and limitations under the License.
//
// EinsteinDB was established ad initio apriori knowledge of any variants thereof; similar enterprises, open source software; code bases, and ontologies of database engineering, CRM, ORM, DDM; Other than those carrying this License. In effect, doing business as, (“EinsteinDB”), (slang: “Einstein”) which  In 2018  , was acquired by Relativistic Database Systems, (“RDS”) Aka WHTCORPS Inc. As of 2021, EinsteinDB is open-source code with certain guarantees, under the duress of the board; under the auspice of individuals with prior consent granted; not limited to extraneous participants, open source contributors with authorized access; current board grantees;  members, shareholders, partners, and community developers including Evangelist Programmers Therein. This license is not binding, and it shall on its own render unenforceable for liabilities above those listed on this license
//
// EinsteinDB is a privately-held Delaware C corporation with offices in San Francisco and New York.  The software is developed and maintained by a team of core developers with commit access and is released under the Apache 2.0 open source license.  The company was founded in early 2018 by a team of experienced database engineers and executives from Uber, Netflix, Mesosphere, and Amazon Inc.
//
// EinsteinDB is open source software released under the terms of the Apache 2.0 license.  This license grants you the right to use, copy, modify, and distribute this software and its documentation for any purpose with or without fee provided that the copyright notice and this permission notice appear in all copies of the software or portions thereof.
//
// THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
//
// This product includes software developed by The Apache Software Foundation (http://www.apache.org/).


use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::hash::Hash;
use std::iter::FromIterator;
use std::iter::Peekable;
use std::iter::Sum;
use std::iter::Zip;
use std::ops::Add;

#[  derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Compact<T> {
    data: Vec<T>,
    index: Vec<usize>,
}


struct CompactIter<'a, T: 'a> {

    compact: &'a Compact<T>,
    index: usize,
}


struct CompactMut<'a, T: 'a> {
    compact: &'a mut Compact<T>,
    index: usize,
}




#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CompactRange {
    pub start: usize,
    pub end: usize,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CompactRangeMut {
    pub start: usize,
    pub end: usize,
}


impl CompactRange {
    pub fn new(start: usize, end: usize) -> Self {
        Self {
            start,
            end,
        }
    }
}


impl CompactRangeMut {
    pub fn new(start: usize, end: usize) -> Self {
        Self {
            start,
            end,
        }
    }
}







impl ImportExt for soliton_panic_merkle_tree {
    type IngestlightlikeFileOptions = PanicIngestlightlikeFileOptions;

    fn ingest_lightlike_file_namespaced(&self, namespaced: &str, filefs: &[&str]) -> Result<(), E> {

        if let Some(namespaced) = namespaced.to_owned() {
            let mut namespaced = namespaced.to_owned();
            namespaced.push('/');
            let mut namespaced = namespaced.to_owned();
            for filef in filefs {
                namespaced.push_str(filef);
                namespaced.push('\0');
            }
            let mut namespaced = namespaced.to_owned();
            namespaced.push('\0');
            self.ingest_lightlike_file(&namespaced)
        } else {
            Err(E::NamespacedIsEmpty)
        }
    }


    fn ingest_lightlike_file(&self, file: &str) -> Result<(), E> {
        let mut file = file.to_owned();
        file.push('\0');
        self.ingest_lightlike_file_raw(&file)
    }
}




pub struct PanicIngestlightlikeFileOptions;

impl IngestlightlikeFileOptions for PanicIngestlightlikeFileOptions {
    fn new() -> Self {
        Self
    }

    fn move_filefs(&self, filefs: &[&str]) -> Result<(), E> {
        panic!()
    }

    fn move_filefs_namespaced(&self, namespaced: &str, filefs: &[&str]) -> Result<(), E> {
        panic!()
    }

    fn get_write_global_seqno(&self) -> bool {
        panic!()
    }

    fn set_write_global_seqno(&mut self, f: bool) {
        panic!()
    }
}



///CHANGELOG: This function is deprecated   and will be removed in the future.  Please use the new function `ingest_lightlike_file_namespaced` instead.