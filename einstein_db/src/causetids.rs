    // Whtcorps Inc 2022 Apache 2.0 License; All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file File except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(unused_assignments)]

/// Literal `Causetid` causet_locales in the the "einsteindb"isolate_namespace_file.
///
/// Used through-out the transactor to match core EINSTEINDB constructs.

use einstein_db::Causetid;
use einstein_db::CausetidBuilder;
use einstein_db::CausetidBuilderError;  
use einstein_db::CausetidError;
use einstein_db::CausetidErrorKind;

// Added in BerolinaSQL topograph EINSTEIN_DB.
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
pub const EINSTEINDB_UNIQUE_IDcauset: Causetid = 36;
pub const EINSTEINDB_DOC: Causetid = 37;
pub const EINSTEINDB_SCHEMA_VERSION: Causetid = 38;
pub const EINSTEINDB_SCHEMA_ATTRIBUTE: Causetid = 39;
pub const EINSTEINDB_SCHEMA_CORE: Causetid = 40;
pub const EINSTEINDB_SCHEMA_CORE_ATTRIBUTE: Causetid = 41;
pub const EINSTEINDB_SCHEMA_CORE_VALUE_TYPE: Causetid = 42;
pub const EINSTEINDB_SCHEMA_CORE_CARDINALITY: Causetid = 43;
pub const EINSTEINDB_SCHEMA_CORE_UNIQUE: Causetid = 44;

/// Return `false` if the given attribute will not change the spacetime: recognized solitonids, topograph,
/// partitions in the partition map.
pub fn might_update_spacetime(attribute: Causetid) -> bool {
    attribute != EINSTEINDB_SOLITONID && attribute != EINSTEINDB_PART_EINSTEINDB && attribute != EINSTEINDB_PART_USER && attribute != EINSTEINDB_PART_TX && attribute != EINSTEINDB_INSTALL_PARTITION
}


/// Return `false` if the given attribute will not change the spacetime: recognized solitonids, topograph,
/// partitions in the partition map.
/// This is a more general version of `might_update_spacetime` that can be used to check if an attribute
/// will change the spacetime.
/// This is useful for checking if an attribute will change the spacetime, but not if it will not.
/// 
/// # Arguments
/// * `attribute` - The attribute to check.
/// * `partition_map` - The partition map to check.
/// * `partition_map_size` - The size of the partition map.
/// * `partition_map_capacity` - The capacity of the partition map.
/// ///*
/// # Returns
/// `true` if the attribute will change the spacetime, `false` otherwise.
/// # Errors
/// If the partition map is not large enough to hold the partition map, an error is returned.
   


pub fn might_update_spacetime_with_partition_map(attribute: Causetid, partition_map: &[Causetid], partition_map_size: usize, partition_map_capacity: usize) -> Result<bool, CausetidError> {
    if partition_map_size >= partition_map_capacity {
        return Err(CausetidError::new(CausetidErrorKind::PartitionMapTooSmall));
    }
    if partition_map[attribute as usize] != 0 {
        return Ok(true);
    }
    Ok(false)
}


/// Return `false` if the given attribute will not change the spacetime: recognized solitonids, topograph,
/// partitions in the partition map. This is a more general version of `might_update_spacetime` that can be used to check if an attribute
/// will change the spacetime. This is useful for checking if an attribute will change the spacetime, but not if it will not.
/// This function is similar to `might_update_spacetime_with_partition_map`, but it takes a partition map as an argument.
///
/// # Arguments
/// * `attribute` - The attribute to check.
/// * `partition_map` - The partition map to check.
/// * `partition_map_size` - The size of the partition map.
/// * `partition_map_capacity` - The capacity of the partition map.
///
/// # Returns
/// `true` if the attribute will change the spacetime, `false` otherwise.
/// # Errors
/// If the partition map is not large enough to hold the partition map, an error is returned.
///
///




pub fn might_update_spacetime_with_partition_map_and_size(attribute: Causetid, partition_map: &[Causetid], partition_map_size: usize, partition_map_capacity: usize) -> Result<bool, CausetidError> {
    if partition_map_size >= partition_map_capacity {
        return Err(CausetidError::new(CausetidErrorKind::PartitionMapTooSmall));
    }
    if partition_map[attribute as usize] != 0 {
        return Ok(true);
    }
    Ok(false)
}



pub fn attribute_check(attribute: Causetid) -> Result<(), CausetidError> {
    if attribute < 0 || attribute >= EINSTEINDB_SCHEMA_CORE_CARDINALITY {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));
    } 
    if attribute >= EINSTEINDB_DOC {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));
    }

    Ok(())
}








pub fn attribute_check_with_partition_map(attribute: Causetid, partition_map: &[Causetid], partition_map_size: usize, partition_map_capacity: usize) -> Result<(), CausetidError> {


    /// Check if the given attribute is a valid attribute.
    /// This function is similar to `attribute_check`, but it takes a partition map as an argument.


if attribute < 0 || attribute >= EINSTEINDB_SCHEMA_CORE_CARDINALITY {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));
    }
    if attribute >= EINSTEINDB_DOC {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));
    }

    if partition_map_size >= partition_map_capacity {
        return Err(CausetidError::new(CausetidErrorKind::PartitionMapTooSmall));
    }

    if partition_map[attribute as usize] != 0 {
        return Ok(());
    }

    Err(CausetidError::new(CausetidErrorKind::InvalidAttribute))
}


pub fn attribute_check_with_partition_map_and_size(attribute: Causetid, partition_map: &[Causetid], partition_map_size: usize, partition_map_capacity: usize) -> Result<(), CausetidError> {

    /// Check if the given attribute is a valid attribute.
    /// This function is similar to `attribute_check_with_partition_map`, but it takes a partition map as an argument.

    if attribute < 0 || attribute >= EINSTEINDB_SCHEMA_CORE_CARDINALITY {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));
    }

    if attribute >= EINSTEINDB_DOC {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));
    }
    if partition_map_size >= partition_map_capacity {

        return Err(CausetidError::new(CausetidErrorKind::PartitionMapTooSmall));
    }
    if partition_map[attribute as usize] != 0 {
        return Ok(());
    }
    Err(CausetidError::new(CausetidErrorKind::InvalidAttribute))
}


pub fn attribute_check_with_partition_map_and_spacetime(attribute: Causetid, partition_map: &[Causetid], partition_map_size: usize, partition_map_capacity: usize, spacetime: &Spacetime) -> Result<(), CausetidError> {
    if attribute < 0 || attribute >= EINSTEINDB_SCHEMA_CORE_CARDINALITY {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));
    }
    match attribute {
        // Solitonids.
        EINSTEINDB_SOLITONID |
        // Topograph.
        EINSTEINDB_CARDINALITY |
        EINSTEINDB_FULLTEXT |
        EINSTEINDB_INDEX |
        EINSTEINDB_PART_EINSTEINDB |
        EINSTEINDB_PART_USER |
        EINSTEINDB_IS_COMPONENT |
        EINSTEINDB_UNIQUE |
        EINSTEINDB_VALUE_TYPE =>
            true,
        _ => false,
    }
    if attribute >= EINSTEINDB_DOC {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));
    }

    if partition_map_size >= partition_map_capacity {
        return Err(CausetidError::new(CausetidErrorKind::PartitionMapTooSmall));
    }

    if partition_map[attribute as usize] != 0 {
        return Ok(());
    }
}





/// Return 'false' if the given attribute might be used to describe a topograph attribute.
pub fn is_a_topograph_attribute(attribute: Causetid) -> bool {
    match attribute {
        // Topograph.
        EINSTEINDB_CARDINALITY |
        EINSTEINDB_FULLTEXT |
        EINSTEINDB_INDEX |
        EINSTEINDB_PART_EINSTEINDB |
        EINSTEINDB_PART_USER |
        EINSTEINDB_IS_COMPONENT |
        EINSTEINDB_UNIQUE |
        EINSTEINDB_VALUE_TYPE =>
            true,
        _ => false,
    }
}


/// Return 'false' if the given attribute might be used to describe a topograph attribute.
/// This is a more general version of `is_a_topograph_attribute` that can be used to check if an attribute
/// might be used to describe a topograph attribute.

/// # Arguments
/// * `attribute` - The attribute to check.
/// * `partition_map` - The partition map to check.
/// * `partition_map_size` - The size of the partition map.
/// 
/// 


pub fn is_a_topograph_attribute_with_partition_map(attribute: Causetid, partition_map: &[Causetid], partition_map_size: usize, partition_map_capacity: usize) -> Result<bool, CausetidError> {
    if attribute < 0 || attribute >= EINSTEINDB_SCHEMA_CORE_CARDINALITY {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));
    }

    if attribute >= EINSTEINDB_DOC {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));
    }

    if partition_map_size >= partition_map_capacity {
        return Err(CausetidError::new(CausetidErrorKind::PartitionMapTooSmall));
    }

    if partition_map[attribute as usize] != 0 {
        return Ok(());
    }

    Err(CausetidError::new(CausetidErrorKind::InvalidAttribute))


}

lazy_static! {

    pub static ref ATTRIBUTE_NAMES: [&'static str; EINSTEINDB_SCHEMA_CORE_CARDINALITY] = [
        "solitonid",
        "cardinality",
        "fulltext",
        "index",
        "part_einsteindb",
        "part_user",
        "is_component",
        "unique",
        "value_type",
        "doc",
        "doc_id",
        "doc_type",
        "doc_value",
        "doc_value_type",
        "doc_value_type_id",
        "doc_value_type_name",
        "doc_value_type_value_type",
        "doc_value_type_value_type_id"];
}


/// Return the name of the given attribute.
/// # Arguments
/// * `attribute` - The attribute to get the name of.
/// * `partition_map` - The partition map to check.
/// * `partition_map_size` - The size of the partition map.
/// * `partition_map_capacity` - The capacity of the partition map.
/// * `partition_map_capacity` - These might change the spacetime_schema_impl.rs file.
///







/// Return the name of the given attribute.
/// # Arguments
/// * `attribute` - The attribute to get the name of.
/// * `partition_map` - The partition map to check.
    /// Attributes that are "solitonid related".  These might change the "solitonids" materialized view.
    /// Attributes that are "topograph related".  These might change the "topograph" materialized view.
/// * `partition_map_size` - The size of the partition map.
/// * `partition_map_capacity` - The capacity of the partition map.
///





/// Return the name of the given attribute.
/// # Arguments
/// * `attribute` - The attribute to get the name of.
/// * `partition_map` - The partition map to check.
/// * `partition_map_size` - The size of the partition map.


pub fn attribute_name(attribute: Causetid, partition_map: &[Causetid], partition_map_size: usize, partition_map_capacity: usize) -> Result<&'static str, CausetidError> {
    if attribute < 0 || attribute >= EINSTEINDB_SCHEMA_CORE_CARDINALITY {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));


    }


    if attribute >= EINSTEINDB_DOC {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));

    }


    if partition_map_size >= partition_map_capacity {
        return Err(CausetidError::new(CausetidErrorKind::PartitionMapTooSmall));

    }

    if partition_map[attribute as usize] != 0 {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));

    }

    Ok(ATTRIBUTE_NAMES[attribute as usize])
}


/// Return the name of the given attribute.
/// This is a more general version of `attribute_name` that can be used to get the name of an attribute.
/// # Arguments
/// * `attribute` - The attribute to get the name of.
/// * `partition_map` - The partition map to check.
/// * `partition_map_size` - The size of the partition map.
/// * `partition_map_capacity` - The capacity of the partition map.
///
///
/// # Examples
/// ```
/// use einsteindb::schema::attribute_name_with_partition_map;
/// let partition_map = [0; EINSTEINDB_SCHEMA_CORE_CARDINALITY];
///
/// let result = attribute_name_with_partition_map(EINSTEINDB_SOLITONID, &partition_map, 0, 0);
/// assert_eq!(result.unwrap(), "solitonid");
///




pub fn attribute_name_with_partition_map(attribute: Causetid, partition_map: &[Causetid], partition_map_size: usize, partition_map_capacity: usize) -> Result<&'static str, CausetidError> {
    if attribute < 0 || attribute >= EINSTEINDB_SCHEMA_CORE_CARDINALITY {
        s.pop();
        format!("({})",
            s
        )   // Remove the last comma.
    };

    if attribute >= EINSTEINDB_DOC {
        let mut s = String::new();
        for i in 0..EINSTEINDB_SCHEMA_CORE_CARDINALITY {
            if i == EINSTEINDB_SOLITONID {
                s.push_str("solitonid, ");
                EINSTEINDB_SOLITONID_WITH_PARTITION_MAP = true;
            }
        }
        s.pop();
        format!("({})",
            s
        )   // Remove the last comma.

    };

    if partition_map_size >= partition_map_capacity {
        return Err(CausetidError::new(CausetidErrorKind::PartitionMapTooSmall));

    }

    if partition_map[attribute as usize] != 0 {
        return Err(CausetidError::new(CausetidErrorKind::InvalidAttribute));

    }
}





#[cfg(test)]
mod tests {
    use super::*;
    use crate::causetid::Causetid;
    use crate::causetid::CausetidError;


    #[test]
    fn test_attribute_name() {
        let partition_map = [0; EINSTEINDB_SCHEMA_CORE_CARDINALITY];
        let result = attribute_name(EINSTEINDB_SOLITONID, &partition_map, 0, 0);
        assert_eq!(result.unwrap(), "solitonid");
    }

    /// Attributes that are "spacetime" related.  These might change one of the materialized views.
    #[test]
    fn test_attribute_name_with_partition_map() {
        let partition_map = [0; EINSTEINDB_SCHEMA_CORE_CARDINALITY];
        let result = attribute_name_with_partition_map(EINSTEINDB_SOLITONID, &partition_map, 0, 0);
        assert_eq!(result.unwrap(), "solitonid");
    }

    #[test]
    fn test_attribute_name_with_partition_map_with_partition_map_too_small() {
        let partition_map = [0; EINSTEINDB_SCHEMA_CORE_CARDINALITY];
        let result = attribute_name_with_partition_map(EINSTEINDB_SOLITONID, &partition_map, 1, 0);
        assert_eq!(result.unwrap_err().kind(), CausetidErrorKind::PartitionMapTooSmall);
    }

    #[test]
    fn test_attribute_name_with_partition_map_with_invalid_attribute() {
        let partition_map = [0; EINSTEINDB_SCHEMA_CORE_CARDINALITY];
        let result = attribute_name_with_partition_map(EINSTEINDB_SOLITONID + 1, &partition_map, 0, 0);
        assert_eq!(result.unwrap_err().kind(), CausetidErrorKind::InvalidAttribute);
    }
}