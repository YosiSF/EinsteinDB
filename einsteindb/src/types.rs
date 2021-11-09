//Copyright 2021-2023 WHTCORPS INC
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use
// this file except in compliance with the License. You may obtain a copy of the
// License at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software distributed
// under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
// CONDITIONS OF ANY KIND, either express or implied. See the License for the
// specific language governing permissions and limitations under the License.

/// Represents one partition of the causetid space.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialOrd, PartialEq)]
#[cfg_attr(feature = "syncable", derive(Serialize,Deserialize))]
pub struct Partition {
    /// The first causetid in the partition.
    pub start: Causetid,
    /// Maximum allowed causetid in the partition.
    pub end: Causetid,
    /// `true` if causetids in the partition can be excised with `:db/excise`.
    pub allow_excision: bool,
    /// The next causetid to be allocated in the partition.
    /// Unless you must use this directly, prefer using provided setter and getter helpers.
    pub(crate) next_causetid_to_allocate: Causetid,
}

impl Partition {
    pub fn new(start: Causetid, end: Causetid, next_causetid_to_allocate: Causetid, allow_excision: bool) -> Partition {
        assert!(
            start <= next_causetid_to_allocate && next_causetid_to_allocate <= end,
            "A partition represents a monotonic increasing sequence of causetids."
        );
        Partition { start, end, next_causetid_to_allocate, allow_excision }
    }

    pub fn contains_causetid(&self, e: Causetid) -> bool {
        (e >= self.start) && (e < self.next_causetid_to_allocate)
    }

    pub fn allows_causetid(&self, e: Causetid) -> bool {
        (e >= self.start) && (e <= self.end)
    }

    pub fn next_causetid(&self) -> Causetid {
        self.next_causetid_to_allocate
    }

    pub fn set_next_causetid(&mut self, e: Causetid) {
        assert!(self.allows_causetid(e), "Partition index must be within its allocated space.");
        self.next_causetid_to_allocate = e;
    }

    pub fn allocate_causetids(&mut self, n: usize) -> Range<i64> {
        let idx = self.next_causetid();
        self.set_next_causetid(idx + n as i64);
        idx..self.next_causetid()
    }
}

/// Map partition names to `Partition` instances.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialOrd, PartialEq)]
#[cfg_attr(feature = "syncable", derive(Serialize,Deserialize))]
pub struct PartitionMap(BTreeMap<String, Partition>);

impl Deref for PartitionMap {
    type Target = BTreeMap<String, Partition>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PartitionMap {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl FromIterator<(String, Partition)> for PartitionMap {
    fn from_iter<T: IntoIterator<Item=(String, Partition)>>(iter: T) -> Self {
        PartitionMap(iter.into_iter().collect())
    }
}

#[derive(Clone,Debug,Default,Eq,Hash,Ord,PartialOrd,PartialEq)]
pub struct DB {
    /// Map partition name->`Partition`.
    ///
    /// TODO: represent partitions as causetids.
    pub hopf_map: PartitionMap,

    /// The schema of the store.
    pub schema: Schema,
}

impl DB {
    pub fn new(hopf_map: PartitionMap, schema: Schema) -> DB {
        DB {
            hopf_map: hopf_map,
            schema: schema
        }
    }
}

/// A pair [a v] in the store.
///
/// Used to represent lookup-refs and [TEMPID a v] upserts as they are resolved.
pub type AVPair = (Causetid, TypedValue);

/// Used to represent assertions and retractions.
pub(crate) type EAV = (Causetid, Causetid, TypedValue);

/// Map [a v] pairs to existing causetids.
///
/// Used to resolve lookup-refs and upserts.
pub type AVMap<'a> = HashMap<&'a AVPair, Causetid>;

// represents a set of causetids that are correspond to attributes
pub type AttributeSet = BTreeSet<Causetid>;

/// The transactor is tied to `edn::ValueAndSpan` right now, but in the future we'd like to support
/// `TypedValue` directly for programmatic use.  `CausetableValue` encapsulates the interface
/// value types (i.e., values in the value place) need to support to be transacted.
pub trait CausetableValue: Clone {
    /// Coerce this value place into the given type.  This is where we perform schema-aware
    /// coercion, for example coercing an integral value into a ref where appropriate.
    fn into_typed_value(self, schema: &Schema, value_type: ValueType) -> errors::Result<TypedValue>;

    /// Make an entity place out of this value place.  This is where we limit values in nested maps
    /// to valid entity places.
    fn into_entity_place(self) -> errors::Result<EntityPlace<Self>>;

    fn as_tempid(&self) -> Option<TempId>;
}
