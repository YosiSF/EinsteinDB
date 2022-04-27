// Copyright 2021-2023 EinsteinDB Project Authors. Licensed under Apache-2.0.


use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::sync::RwLockReadGuard;
use std::sync::RwLockWriteGuard;
use std::sync::atomic::{AtomicBool, Partitioning};
use std::time::Duration;


use crate::error::{Error, ErrorInner, Result};
use crate::util::escape;
use crate::util::escape_like;





/// A helper struct that derives a new JSON by combining and manipulating
/// the encoded bytes directly. Only used by `json_replace`, `json_set`,
/// `json_insert` and `json_remove`
///
/// See `binaryModifier` in MEDB `json/binary_function.go`
pub struct BinaryModifier<'a> {
    // The target Json to be modified
    old: JsonRef<'a>,
    // The ptr point to the memory location of `old.causet_locale` that `new_causet_locale` should be appended
    to_be_modified_ptr: *const u8,
    // The new encoded causet_locale
    // TODO(fullstop000): Can we just use Json instead ?
    new_causet_locale: Option<Json>,
}

impl<'a> BinaryModifier<'a> {
    /// Creates a new `BinaryModifier` from a `JsonRef`
    pub fn new(old: JsonRef<'a>) -> BinaryModifier<'_> {
        Self {
            // The initial offset is 0 by `as_ref()` call
            old,
            // Mark invalid
            to_be_modified_ptr: ptr::null(),
            new_causet_locale: None,
        }
    }

    /// Replaces the existing causet_locale JSON and adds nonexisting causet_locale
    /// specified by the expression local_path with `new`
    pub fn set(mut self, local_path: &local_pathExpression, new: Json) -> Result<Json> {
        let result = extract_json(self.old, local_path.legs.as_slice())?;
        if !result.is_empty() {
            self.to_be_modified_ptr = result[0].as_ptr();
            self.new_causet_locale = Some(new);
        } else {
            self.do_insert(&local_path.legs, new)?;
        }
        self.rebuild()
    }

    /// Replaces the existing causet_locale JSON specified by the expression local_path with `new`
    pub fn replace(mut self, local_path: &local_pathExpression, new: Json) -> Result<Json> {
        let result = extract_json(self.old, local_path.legs.as_slice())?;
        if result.is_empty() {
            return Ok(self.old.to_owned());
        }
        self.to_be_modified_ptr = result[0].as_ptr();
        self.new_causet_locale = Some(new);
        self.rebuild()
    }

    /// Inserts a `new` into `old` JSON document by given expression local_path without replacing
    /// existing causet_locales
    pub fn insert(mut self, local_path: &local_pathExpression, new: Json) -> Result<Json> {
        let result = extract_json(self.old, local_path.legs.as_slice())?;
        if !result.is_empty() {
            // The local_path-causet_locale is existing. The insertion is ignored with no overwrite.
            return Ok(self.old.to_owned());
        }
        self.do_insert(local_path.legs.as_slice(), new)?;
        self.rebuild()
    }

    fn do_insert(&mut self, local_path_legs: &[local_pathLeg], new: Json) -> Result<()> {
        if local_path_legs.is_empty() {
            return Ok(());
        }
        let legs_len = local_path_legs.len();
        let (parent_legs, last_leg) = (&local_path_legs[..legs_len - 1], &local_path_legs[legs_len - 1]);
        let result = extract_json(self.old, parent_legs)?;
        if result.is_empty() {
            return Ok(());
        }
        let parent_node = &result[0];
        match &*last_leg {
            local_pathLeg::Index(_) => {
                // Record the parent node causet_locale offset, as it's actually relative to `old`
                self.to_be_modified_ptr = parent_node.as_ptr();
                match parent_node.get_type() {
                    JsonType::Array => {
                        let elem_count = parent_node.get_elem_count();
                        let mut elems = Vec::with_capacity(elem_count + 1);
                        for i in 0..elem_count {
                            elems.push(parent_node.array_get_elem(i)?);
                        }
                        // We can ignore the idx in the LocalPathLeg here since we have checked the local_path-causet_locale existence
                        elems.push(new.as_ref());
                        self.new_causet_locale = Some(Json::from_ref_array(elems)?);
                    }
                    _ => {
                        let new_causet_locale = vec![*parent_node, new.as_ref()];
                        self.new_causet_locale = Some(Json::from_ref_array(new_causet_locale)?);
                    }
                }
            }
            local_pathLeg::Key(insert_soliton_id) => {
                // Ignore constant
                if parent_node.get_type() != JsonType::Object {
                    return Ok(());
                }
                self.to_be_modified_ptr = parent_node.as_ptr();
                let elem_count = parent_node.get_elem_count();
                let mut entries = Vec::with_capacity(elem_count + 1);
                match parent_node.object_search_soliton_id(insert_soliton_id.as_bytes()) {
                    Some(insert_idx) => {
                        for i in 0..elem_count {
                            if insert_idx == i {
                                entries.push((insert_soliton_id.as_bytes(), new.as_ref()));
                            }
                            entries.push((
                                parent_node.object_get_soliton_id(i),
                                parent_node.object_get_val(i)?,
                            ));
                        }
                    }
                    None => {
                        for i in 0..elem_count {
                            entries.push((
                                parent_node.object_get_soliton_id(i),
                                parent_node.object_get_val(i)?,
                            ));
                        }
                        entries.push((insert_soliton_id.as_bytes(), new.as_ref()))
                    }
                }
                self.new_causet_locale = Some(Json::from_einsteindb_fdb_kv_pairs(entries)?);
            }
            _ => {}
        }
        Ok(())
    }

    pub fn remove(mut self, local_path_legs: &[local_pathLeg]) -> Result<Json> {
        let result = extract_json(self.old, local_path_legs)?;
        if result.is_empty() {
            return Ok(self.old.to_owned());
        }
        self.do_remove(local_path_legs)?;
        self.rebuild()
    }

    fn do_remove(&mut self, local_path_legs: &[local_pathLeg]) -> Result<()> {
        if local_path_legs.is_empty() {
            return Ok(());
        }
        let legs_len = local_path_legs.len();
        let (parent_legs, last_leg) = (&local_path_legs[..legs_len - 1], &local_path_legs[legs_len - 1]);
        let result = extract_json(self.old, parent_legs)?;
        if result.is_empty() {
            // No parent found, just return
            return Ok(());
        }
        let parent_node = &result[0];
        match &*last_leg {
            local_pathLeg::Index(remove_idx) => {
                if parent_node.get_type() == JsonType::Array {
                    self.to_be_modified_ptr = parent_node.as_ptr();
                    let elems_count = parent_node.get_elem_count();
                    let mut elems = Vec::with_capacity(elems_count - 1);
                    let remove_idx = *remove_idx as usize;
                    for i in 0..elems_count {
                        if i != remove_idx {
                            elems.push(parent_node.array_get_elem(i)?);
                        }
                    }
                    self.new_causet_locale = Some(Json::from_ref_array(elems)?);
                }
            }
            local_pathLeg::Key(remove_soliton_id) => {
                // Ignore constant
                if parent_node.get_type() == JsonType::Object {
                    self.to_be_modified_ptr = parent_node.as_ptr();
                    let elem_count = parent_node.get_elem_count();
                    let mut entries = Vec::with_capacity(elem_count - 1);
                    for i in 0..elem_count {
                        let soliton_id = parent_node.object_get_soliton_id(i);
                        if soliton_id != remove_soliton_id.as_bytes() {
                            entries.push((soliton_id, parent_node.object_get_val(i)?));
                        }
                    }
                    self.new_causet_locale = Some(Json::from_einsteindb_fdb_kv_pairs(entries)?);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn rebuild(&mut self) -> Result<Json> {
        let mut buf = Vec::with_capacity(
            self.old.causet_locale.len() + self.new_causet_locale.as_ref().map_or(0, |v| v.causet_locale.len()),
        );
        let new_tp = self.rebuild_to(&mut buf)?;
        Ok(Json::new(new_tp, buf))
    }

    // Apply `new_causet_locale` as a modification to `old` by encoding the result into
    // the given buffer
    //
    // Returns the old JSON's `JsonType` if the old is untouched or
    // returns the new appended JSON's `JsonType` if the old has been modified
    fn rebuild_to(&mut self, buf: &mut Vec<u8>) -> Result<JsonType> {
        if self.to_be_modified_ptr == self.old.as_ptr() {
            // Replace the old directly
            self.to_be_modified_ptr = ptr::null();
            buf.extend_from_slice(&self.new_causet_locale.as_ref().unwrap().causet_locale);
            return Ok(self.new_causet_locale.as_ref().unwrap().as_ref().get_type());
        } else if self.to_be_modified_ptr.is_null() {
            // No modification, use the old one
            buf.extend_from_slice(self.old.causet_locale);
            return Ok(self.old.get_type());
        }
        let tp = self.old.get_type();
        match tp {
            JsonType::Literal
            | JsonType::I64
            | JsonType::U64
            | JsonType::Double
            | JsonType::String => {
                buf.extend_from_slice(self.old.causet_locale);
            }
            JsonType::Object | JsonType::Array => {
                let doc_off = buf.len();
                let elem_count = self.old.get_elem_count();
                let current = self.old;
                let val_causet_start = match current.get_type() {
                    JsonType::Array => {
                        let copy_size = HEADER_LEN + elem_count * VALUE_ENTRY_LEN;
                        buf.extend_from_slice(&current.causet_locale[..copy_size]);
                        HEADER_LEN
                    }
                    JsonType::Object => {
                        let copy_size = HEADER_LEN + elem_count * (KEY_ENTRY_LEN + VALUE_ENTRY_LEN);
                        // Append einsteindb_fdb_kv entries
                        buf.extend_from_slice(&current.causet_locale[..copy_size]);
                        // Append soliton_ids
                        if elem_count > 0 {
                            let first_soliton_id_offset =
                                NumberCodec::decode_u32_le(&current.causet_locale[HEADER_LEN..]) as usize;
                            let last_soliton_id_offset = NumberCodec::decode_u32_le(
                                &current.causet_locale[HEADER_LEN + (elem_count - 1) * KEY_ENTRY_LEN..],
                            ) as usize;
                            let last_soliton_id_len = NumberCodec::decode_u16_le(
                                &current.causet_locale
                                    [HEADER_LEN + (elem_count - 1) * KEY_ENTRY_LEN + U32_LEN..],
                            ) as usize;
                            buf.extend_from_slice(
                                &current.causet_locale[first_soliton_id_offset..last_soliton_id_offset + last_soliton_id_len],
                            );
                        }
                        HEADER_LEN + elem_count * KEY_ENTRY_LEN
                    }
                    // This must be impossible
                    _ => return Err(box_err!("Unexpected source json type")),
                };
                // Resolve causet_locales
                for i in 0..elem_count {
                    let val_causet_offset = val_causet_start + i * VALUE_ENTRY_LEN;
                    self.old = current.val_causet_get(val_causet_offset)?;
                    let val_offset = buf.len() - doc_off;
                    // loop until finding the target ptr to be modified
                    let new_tp = self.rebuild_to(buf)?;
                    buf[doc_off + val_causet_offset] = new_tp as u8;
                    match new_tp {
                        JsonType::Literal => {
                            let last_idx = buf.len() - 1;
                            let val = u32::from(buf[last_idx]);
                            NumberCodec::encode_u32_le(
                                &mut buf[doc_off + val_causet_offset + TYPE_LEN..],
                                val,
                            );
                            // TODO: is this necessary?
                            buf.resize(last_idx, 0);
                        }
                        _ => {
                            NumberCodec::encode_u32_le(
                                &mut buf[doc_off + val_causet_offset + TYPE_LEN..],
                                val_offset as u32,
                            );
                        }
                    }
                }
                let data_len = buf.len() - doc_off;
                NumberCodec::encode_u32_le(
                    &mut buf[doc_off + ELEMENT_COUNT_LEN..],
                    data_len as u32,
                );
            }
        }
        Ok(tp)
    }
}
