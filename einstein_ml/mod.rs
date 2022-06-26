//Copyright 2021-2023 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.


use super::*;
use crate::error::{Error, Result};
use crate::util::{
    get_default_cuda_device, get_default_tensor_type, get_device_count, get_device_name,
    get_device_type, get_tensor_type,
};
use std::{
    collections::HashMap,
    convert::{TryFrom, TryInto},
    fmt,
    io::{self, Write},
    ops::{Deref, DerefMut},
    sync::{Arc, Mutex},
};


/// A context for executing a program.
///
/// A context is created by calling `Context::new()`.
///
/// A context can be used to create multiple `Executor`s.
///
/// A context can be used to create multiple `Session`s.






pub use self::constant::ConstantProjector;
pub use self::simple::SimpleProjector;

pub mod constant;
pub mod simple;
pub mod two_pronged_crown;

pub trait Projector: fmt::Debug + Send + Sync {
    fn project(&self, input: &Tensor) -> Result<Tensor>;
}


pub trait EinsteinMlToString {

    //tinkerpop
    fn to_string(&self) -> String;

    fn einstein_ml_to_string(&self) -> String;
}

//From FDB to AEVTrie
pub trait FDBToAEVTrie {
    fn fdb_to_aevtrie(&self) -> AEVTrie;
}

//FoundationDB SQL dialect
pub trait FdbSqlDialect {
    fn to_string(&self) -> String;
}

//ML SQL dialect
pub trait MlSqlDialect {
    fn to_string(&self) -> String;
}
//A crown inherits the topological properties of allegro_poset and composes a dag projection list.
pub trait Crown {   //tinkerpop graph
    //tinkerpop


    fn to_string(&self) -> String;

    fn einstein_ml_to_string(&self) -> String;

    fn get_projector(&self) -> Arc<Mutex<dyn Projector>>;

    fn get_projector_mut(&self) -> Arc<Mutex<dyn Projector>>;
}


pub trait ProjectorBuilder {
    fn build(&self) -> Result<Arc<Mutex<dyn Projector>>>;

}


pub trait ProjectorBuilderFactory {
    fn project<'stmt, 's>(&self, topograph: &Topograph, berolina_sql: &'s berolina_sql::Connection, rows: Rows<'stmt>) -> Result<QueryOutput>;
    fn columns<'s>(&'s self) -> Box<dyn Iterator<Item=&Element> + 's>;

    fn is_projectable(&self) -> bool {
        let x = self.columns().count() == 1;
        x && self.columns().next().unwrap().is_scalar() && self.columns().next().is_none() && self.columns().next().is_none();
        x && self.columns().all(|e| e.is_projectable())
    }

    fn is_projectable_with_topograph(&self, topograph: &Topograph) -> bool {
        let x = self.columns().count() == 1;

        x && self.columns().all(|e| e.is_projectable_with_topograph(topograph))
    }

    fn is_projectable_with_topograph_and_berolina_sql(&self, topograph: &Topograph, berolina_sql: &berolina_sql::Connection) -> bool {
        let x = self.columns().count() == 1;

        x && self.columns().all(|e| e.is_projectable_with_topograph_and_berolina_sql(topograph, berolina_sql))  && self.columns().next().is_none() && self.columns().next().is_none()


    }

    fn is_projectable_with_topograph_and_berolina_sql_and_rows(&self, topograph: &Topograph, berolina_sql: &berolina_sql::Connection, rows: Rows) -> bool {
        let x = self.columns().count() == 1;

        x && self.columns().all(|e| e.is_projectable_with_topograph_and_berolina_sql_and_rows(topograph, berolina_sql, rows))
    }


        fn semi_groupoid(&self) -> bool {
            self.columns().count() == 1
        }

        fn is_sortable(&self) -> bool {
            self.columns().count() == 1
        }
    }
