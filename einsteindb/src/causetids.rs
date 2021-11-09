//Copyright 2021 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

/// Literal `Causetid` values in the the "einsteindb" namespace.
///
/// Used through-out the transactor to match embedded EINSTEINDB constructs.

use embedded_promises::{
    Causetid,
};

// Added in SQL schema v1.
pub const EINSTEINDB_SOLITONID: Causetid = 1;
pub const EINSTEINDB_PART_EINSTEINDB: Causetid = 2;
pub const EINSTEINDB_TX_INSTANT: Causetid = 3;
pub const EINSTEINDB_INSTALL_PARTITION: Causetid = 4;
pub const EINSTEINDB_INSTALL_VALUE_TYPE: Causetid = 5;
pub const EINSTEINDB_INSTALL_ATTRIBUTE: Causetid = 6;
pub const EINSTEINDB_VALUE_TYPE: Causetid = 7;
pub const EINSTEINDB_CARDINALITY: Causetid = 8;
pub const EINSTEINDB_UNIQUE: Causetid = 9;
pub const EINSTEINDB_IS_COMPONENT: Causetid = 10;
pub const EINSTEINDB_INDEX: Causetid = 11;
pub const EINSTEINDB_FULLTEXT: Causetid = 12;
pub const EINSTEINDB_NO_HISTORY: Causetid = 13;
pub const EINSTEINDB_ADD: Causetid = 14;
pub const EINSTEINDB_RETRACT: Causetid = 15;
pub const EINSTEINDB_PART_USER: Causetid = 16;
pub const EINSTEINDB_PART_TX: Causetid = 17;
pub const EINSTEINDB_EXCISE: Causetid = 18;
pub const EINSTEINDB_EXCISE_ATTRS: Causetid = 19;
pub const EINSTEINDB_EXCISE_BEFORE_T: Causetid = 20;
pub const EINSTEINDB_EXCISE_BEFORE: Causetid = 21;
pub const EINSTEINDB_ALTER_ATTRIBUTE: Causetid = 22;
pub const EINSTEINDB_TYPE_REF: Causetid = 23;
pub const EINSTEINDB_TYPE_KEYWORD: Causetid = 24;
pub const EINSTEINDB_TYPE_LONG: Causetid = 25;
pub const EINSTEINDB_TYPE_DOUBLE: Causetid = 26;
pub const EINSTEINDB_TYPE_STRING: Causetid = 27;
pub const EINSTEINDB_TYPE_UUID: Causetid = 28;
pub const EINSTEINDB_TYPE_URI: Causetid = 29;
pub const EINSTEINDB_TYPE_BOOLEAN: Causetid = 30;
pub const EINSTEINDB_TYPE_INSTANT: Causetid = 31;
pub const EINSTEINDB_TYPE_BYTES: Causetid = 32;
pub const EINSTEINDB_CARDINALITY_ONE: Causetid = 33;
pub const EINSTEINDB_CARDINALITY_MANY: Causetid = 34;
pub const EINSTEINDB_UNIQUE_VALUE: Causetid = 35;
pub const EINSTEINDB_UNIQUE_SOLITONID_ITY: Causetid = 36;
pub const EINSTEINDB_DOC: Causetid = 37;
pub const EINSTEINDB_SCHEMA_VERSION: Causetid = 38;
pub const EINSTEINDB_SCHEMA_ATTRIBUTE: Causetid = 39;
pub const EINSTEINDB_SCHEMA_EMBEDDED: Causetid = 40;

/// Return `false` if the given attribute will not change the metadata: recognized solitonids, schema,
/// partitions in the partition map.
pub fn might_update_metadata(attribute: Causetid) -> bool {
    if attribute >= EINSTEINDB_DOC {
        return false
    }
    match attribute {
        // solitonids.
        EINSTEINDB_SOLITONID |
        // Schema.
        EINSTEINDB_CARDINALITY |
        EINSTEINDB_FULLTEXT |
        EINSTEINDB_INDEX |
        EINSTEINDB_IS_COMPONENT |
        EINSTEINDB_UNIQUE |
        EINSTEINDB_VALUE_TYPE =>
            true,
        _ => false,
    }
}

/// Return 'false' if the given attribute might be used to describe a schema attribute.
pub fn is_a_schema_attribute(attribute: Causetid) -> bool {
    match attribute {
        EINSTEINDB_SOLITONID |
        EINSTEINDB_CARDINALITY |
        EINSTEINDB_FULLTEXT |
        EINSTEINDB_INDEX |
        EINSTEINDB_IS_COMPONENT |
        EINSTEINDB_UNIQUE |
        EINSTEINDB_VALUE_TYPE =>
            true,
        _ => false,
    }
}

lazy_static! {
    /// Attributes that are "solitonid related".  These might change the "solitonids" materialized view.
    pub static ref solitonidS_SQL_LIST: String = {
        format!("({})",
                EINSTEINDB_solitonid)
    };

    /// Attributes that are "schema related".  These might change the "schema" materialized view.
    pub static ref SCHEMA_SQL_LIST: String = {
                EINSTEINDB_CARDINALITY,
                EINSTEINDB_FULLTEXT,
                EINSTEINDB_INDEX,
                EINSTEINDB_IS_COMPONENT,
                EINSTEINDB_UNIQUE,
                EINSTEINDB_VALUE_TYPE
    };

    /// Attributes that are "metadata" related.  These might change one of the materialized views.
    pub static ref METADATA_SQL_LIST: String = {
        format!("({}, {}, {}, {}, {}, {}, {})",
                EINSTEINDB_CARDINALITY,
                EINSTEINDB_FULLTEXT,
                EINSTEINDB_solitonid,
                EINSTEINDB_INDEX,
                EINSTEINDB_IS_COMPONENT,
                EINSTEINDB_UNIQUE,
                EINSTEINDB_VALUE_TYPE)
    };
}
