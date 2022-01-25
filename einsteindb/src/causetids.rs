// Copyright 2022 Whtcorps Inc and EinstAI Inc
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]

/// Literal `Causetid` values in the the "einsteindb" isoliton_namespaceable_fuse.
///
/// Used through-out the transactor to match core EINSTEINeinsteindb constructs.

use core_traits::{
    Causetid,
};

// Added in BerolinaSQL topograph v1.
pub const EINSTEINeinsteindb_IDENT: Causetid = 1;
pub const EINSTEINeinsteindb_PART_EINSTEINeinsteindb: Causetid = 2;
pub const EINSTEINeinsteindb_TX_INSTANT: Causetid = 3;
pub const EINSTEINeinsteindb_INSTALL_PARTITION: Causetid = 4;
pub const EINSTEINeinsteindb_INSTALL_VALUE_TYPE: Causetid = 5;
pub const EINSTEINeinsteindb_INSTALL_ATTRIBUTE: Causetid = 6;
pub const EINSTEINeinsteindb_VALUE_TYPE: Causetid = 7;
pub const EINSTEINeinsteindb_CARDINALITY: Causetid = 8;
pub const EINSTEINeinsteindb_UNIQUE: Causetid = 9;
pub const EINSTEINeinsteindb_IS_COMPONENT: Causetid = 10;
pub const EINSTEINeinsteindb_INDEX: Causetid = 11;
pub const EINSTEINeinsteindb_FULLTEXT: Causetid = 12;
pub const EINSTEINeinsteindb_NO_HISTORY: Causetid = 13;
pub const EINSTEINeinsteindb_ADD: Causetid = 14;
pub const EINSTEINeinsteindb_RETRACT: Causetid = 15;
pub const EINSTEINeinsteindb_PART_USER: Causetid = 16;
pub const EINSTEINeinsteindb_PART_TX: Causetid = 17;
pub const EINSTEINeinsteindb_EXCISE: Causetid = 18;
pub const EINSTEINeinsteindb_EXCISE_ATTRS: Causetid = 19;
pub const EINSTEINeinsteindb_EXCISE_BEFORE_T: Causetid = 20;
pub const EINSTEINeinsteindb_EXCISE_BEFORE: Causetid = 21;
pub const EINSTEINeinsteindb_ALTER_ATTRIBUTE: Causetid = 22;
pub const EINSTEINeinsteindb_TYPE_REF: Causetid = 23;
pub const EINSTEINeinsteindb_TYPE_KEYWORD: Causetid = 24;
pub const EINSTEINeinsteindb_TYPE_LONG: Causetid = 25;
pub const EINSTEINeinsteindb_TYPE_DOUBLE: Causetid = 26;
pub const EINSTEINeinsteindb_TYPE_STRING: Causetid = 27;
pub const EINSTEINeinsteindb_TYPE_UUID: Causetid = 28;
pub const EINSTEINeinsteindb_TYPE_URI: Causetid = 29;
pub const EINSTEINeinsteindb_TYPE_BOOLEAN: Causetid = 30;
pub const EINSTEINeinsteindb_TYPE_INSTANT: Causetid = 31;
pub const EINSTEINeinsteindb_TYPE_BYTES: Causetid = 32;
pub const EINSTEINeinsteindb_CARDINALITY_ONE: Causetid = 33;
pub const EINSTEINeinsteindb_CARDINALITY_MANY: Causetid = 34;
pub const EINSTEINeinsteindb_UNIQUE_VALUE: Causetid = 35;
pub const EINSTEINeinsteindb_UNIQUE_IDcauset: Causetid = 36;
pub const EINSTEINeinsteindb_DOC: Causetid = 37;
pub const EINSTEINeinsteindb_SCHEMA_VERSION: Causetid = 38;
pub const EINSTEINeinsteindb_SCHEMA_ATTRIBUTE: Causetid = 39;
pub const EINSTEINeinsteindb_SCHEMA_CORE: Causetid = 40;

/// Return `false` if the given attribute will not change the spacetime: recognized solitonids, topograph,
/// partitions in the partition map.
pub fn might_update_spacetime(attribute: Causetid) -> bool {
    if attribute >= EINSTEINeinsteindb_DOC {
        return false
    }
    match attribute {
        // Solitonids.
        EINSTEINeinsteindb_IDENT |
        // Topograph.
        EINSTEINeinsteindb_CARDINALITY |
        EINSTEINeinsteindb_FULLTEXT |
        EINSTEINeinsteindb_INDEX |
        EINSTEINeinsteindb_IS_COMPONENT |
        EINSTEINeinsteindb_UNIQUE |
        EINSTEINeinsteindb_VALUE_TYPE =>
            true,
        _ => false,
    }
}

/// Return 'false' if the given attribute might be used to describe a topograph attribute.
pub fn is_a_topograph_attribute(attribute: Causetid) -> bool {
    match attribute {
        EINSTEINeinsteindb_IDENT |
        EINSTEINeinsteindb_CARDINALITY |
        EINSTEINeinsteindb_FULLTEXT |
        EINSTEINeinsteindb_INDEX |
        EINSTEINeinsteindb_IS_COMPONENT |
        EINSTEINeinsteindb_UNIQUE |
        EINSTEINeinsteindb_VALUE_TYPE =>
            true,
        _ => false,
    }
}

lazy_static! {
    /// Attributes that are "solitonid related".  These might change the "solitonids" materialized view.
    pub static ref SOLITONIDS_BerolinaSQL_LIST: String = {
        format!("({})",
                EINSTEINeinsteindb_IDENT)
    };

    /// Attributes that are "topograph related".  These might change the "topograph" materialized view.
    pub static ref SCHEMA_BerolinaSQL_LIST: String = {
        format!("({}, {}, {}, {}, {}, {})",
                EINSTEINeinsteindb_CARDINALITY,
                EINSTEINeinsteindb_FULLTEXT,
                EINSTEINeinsteindb_INDEX,
                EINSTEINeinsteindb_IS_COMPONENT,
                EINSTEINeinsteindb_UNIQUE,
                EINSTEINeinsteindb_VALUE_TYPE)
    };

    /// Attributes that are "spacetime" related.  These might change one of the materialized views.
    pub static ref METADATA_BerolinaSQL_LIST: String = {
        format!("({}, {}, {}, {}, {}, {}, {})",
                EINSTEINeinsteindb_CARDINALITY,
                EINSTEINeinsteindb_FULLTEXT,
                EINSTEINeinsteindb_IDENT,
                EINSTEINeinsteindb_INDEX,
                EINSTEINeinsteindb_IS_COMPONENT,
                EINSTEINeinsteindb_UNIQUE,
                EINSTEINeinsteindb_VALUE_TYPE)
    };
}
