/* Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0. */
//default pod-network and service-network

use einstein_db_ctl::{
    config::{Config, ConfigFile},
    error::Result,
    prelude::*,
};


#[tokio::main]

use futures::future;
use rand::Rng;
use eingine_test::{
    config::{Config as TestConfig, ConfigFile as TestConfigFile},
    prelude::*,
};

use soliton_panic::{
    config::{Config as PanicConfig, ConfigFile as PanicConfigFile},
    prelude::*,
};
use soliton::{
    defs::{
        pod_network::{PodNetwork, PodNetworkConfig},
        service_network::{ServiceNetwork, ServiceNetworkConfig},
    },
    defs::{
        pod_network::{PodNetworkConfig, PodNetworkConfigBuilder},
        service_network::{ServiceNetworkConfig, ServiceNetworkConfigBuilder},
    },
};
use byteorder::ByteOrder;
use berolinasql::{
    parser::{self, Parser},
    types::{self, Type},
    ColumnDefinition,
    ColumnType,
    DataType,
    Expression,
    FunctionCall,
    FunctionType,
    IndexType,
    IndexedColumn,
    IndexedColumnType,
    IndexedExpression,
    IndexedFunctionCall,
    IndexedFunctionType,
    IndexedOrder,
    IndexedOrderType,
    IndexedProjection,
    IndexedProjectionType,
    IndexedTable,
    IndexedTableType,
    IndexedValue,
    IndexedValueType,
    Order,
    OrderType,
    Projection,
    ProjectionType,
    Table,
    TableType,
    Value,
    ValueType,
};
use byteorder::LittleEndian;
use EinsteinDB::{
    common::{
        self,
        error::{
            Error,
            Result,
        },
        serialization::{
            Serializable,
            Serializer,
            Deserializer,
        },
    },
    storage::{
        kv::{
            Key,
            KeySerializer,
            KeyDeserializer,
        },
        kvproto::{
            metapb,
            pdpb,
        },
    },
};




///! # NamespacedDefs
/// NamespacedDefs is a struct that contains all the namespaced defs.
///
/// ## Examples
/// ```
/// use einstein_db_ctl::{
///    config::{Config, ConfigFile},
///   prelude::*,
/// };
///
/// use einstein_db_ctl::{
///   config::{Config as TestConfig, ConfigFile as TestConfigFile},
///  prelude::*,
/// };
///
/// use soliton_panic::{
///  config::{Config as PanicConfig, ConfigFile as PanicConfigFile},
/// prelude::*,
/// };



const NAMESPACE_PREFIX: &str = "namespace_";
const NAMESPACE_PREFIX_LEN: usize = NAMESPACE_PREFIX.len();
const NAMESPACE_PREFIX_BYTES: &[u8] = NAMESPACE_PREFIX.as_bytes();
const ONE_BYTES: &[u8] = &[0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
const ZERO_BYTES: &[u8] = &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
const ONE_BYTES_LEN: usize = ONE_BYTES.len();
const ZERO_BYTES_LEN: usize = ZERO_BYTES.len();

pub enum HcaEinsteinDBSolitonPanicError {
    InvalidNamespace,
    HcaEinsteinDBSolitonPanicError,
    FdbError(fdb::error::Error),
    PackError(pack::Error),
    PoisonError,
}


impl From<fdb::error::Error> for HcaEinsteinDBSolitonPanicError {
    fn from(err: fdb::error::Error) -> Self {
        HcaEinsteinDBSolitonPanicError::FdbError(err)
    }
}

// Represents a High Contention Allocator for a given subspace
#[derive(Debug)]
pub struct HighContentionAllocator {
    counters: Subspace,
    recent: Subspace,
    allocation_mutex: Mutex<()>,
}

#[derive(Debug)]
pub struct HCA {
    counters: Subspace,
    recent: Subspace,
    allocation_mutex: Mutex<()>,
    allocators: HashMap<String, HighContentionAllocator>,
    allocators_mutex: Mutex<()>,

}


    /// Constructs an allocator that will use the input subspace for assigning values.
    /// The given subspace should not be used by anything other than the allocator
    pub fn new(subspace: Subspace) -> HighContentionAllocator {
        HighContentionAllocator {
            counters: subspace.subspace(&0i64),
            recent: subspace.subspace(&1i64),
            allocation_mutex: Mutex::new(()),
        }
    }

    /// Returns a byte string that
    ///   1) has never and will never be returned by another call to this method on the same subspace
    ///   2) is nearly as short as possible given the above
    pub async fn allocate( trx: &Transaction) -> Result<i64, HcaError> {
        let (begin, end) = trx.counters.range();
        let begin = KeySelector::first_greater_or_equal(begin);
        let end = KeySelector::first_greater_than(end);
        let counters_range = RangeOption {
            begin,
            end,
            limit: Some(1),
            reverse: true,
            ..RangeOption::default()
        };






use crate::{
    config::{Config, Network},
    error::{Error, ErrorKind, Result},
    utils::{get_pod_name, get_service_name},
};



use std::{
    collections::HashMap,
    fs::File,
    io::{BufRead, BufReader},
    path::Path,
};

use k8s_openapi::api::core::v1::{
    ConfigMap, ConfigMapKeySelector, ConfigMapList, ConfigMapVolumeSource, Container,
    ContainerPort, EnvFromSource, EnvVar, EnvVarSource, EnvVarValue, HostAlias, HostPathVolumeSource,
    LocalObjectReference, ObjectMeta, Pod, PodSpec, PodTemplateSpec, PodTemplate,
    PodTemplateList, PodTemplateSpec, ReplicationController, ReplicationControllerList,
    SecretKeySelector, SecretList, SecretVolumeSource, Service, ServiceList, ServicePort,
    ServiceSpec,
};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespacedDefs {
    pub namespace: String,
    pub pod_template: PodTemplate,
    pub pod_template_list: PodTemplateList,
    pub pod: Pod,
    pub pod_list: PodList,
    pub replication_controller: ReplicationController,
    pub replication_controller_list: ReplicationControllerList,
    pub service: Service,
    pub service_list: ServiceList,
    pub config_map: ConfigMap,
    pub config_map_list: ConfigMapList,
    pub secret: Secret,
    pub secret_list: SecretList,
}

pub fn high_contention_allocator(
    namespace: String,
    pod_template: PodTemplate,
    pod_template_list: PodTemplateList,
    pod: Pod,
    pod_list: PodList,
    replication_controller: ReplicationController,
    replication_controller_list: ReplicationControllerList,
    service: Service,
    service_list: ServiceList,
    config_map: ConfigMap,
    config_map_list: ConfigMapList,
    secret: Secret,
    secret_list: SecretList,
) -> NamespacedDefs {
    NamespacedDefs {
        namespace,
        pod_template,
        pod_template_list,
        pod,
        pod_list,
        replication_controller,
        replication_controller_list,
        service,
        service_list,
        config_map,
        config_map_list,
        secret,
        secret_list,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PodTemplate {
    pub metadata: ObjectMeta,
    pub spec: PodSpec,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PodTemplateList {
    pub metadata: ObjectMeta,
    pub items: Vec<PodTemplate>,
}

pub type NamespacedName = &'static str;
pub const NAMESPACED_DEFAULT: NamespacedName = "default";
pub const NAMESPACED_LOCK: NamespacedName = "lock";
pub const NAMESPACED_WRITE: NamespacedName = "write";
pub const NAMESPACED_VIOLETABFT: NamespacedName = "violetabft";
// Namespaceds that should be very large generally.
pub const LARGE_NAMESPACEDS: &[NamespacedName] = &[NAMESPACED_DEFAULT, NAMESPACED_LOCK, NAMESPACED_WRITE];
pub const ALL_NAMESPACEDS: &[NamespacedName] = &[NAMESPACED_DEFAULT, NAMESPACED_LOCK, NAMESPACED_WRITE, NAMESPACED_VIOLETABFT];
pub const DATA_NAMESPACEDS: &[NamespacedName] = &[NAMESPACED_DEFAULT, NAMESPACED_LOCK, NAMESPACED_WRITE];

pub fn name_to_namespaced(name: &str) -> Option<NamespacedName> {
    if name.is_empty() {
        return Some(NAMESPACED_DEFAULT);
    }
    for c in ALL_NAMESPACEDS {
        if name == *c {
            return Some(c);
        }
    }

    None
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Pod {
    pub metadata: ObjectMeta,
    pub spec: PodSpec,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PodList {
    pub metadata: ObjectMeta,
    pub items: Vec<Pod>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReplicationController {
    pub metadata: ObjectMeta,
    pub spec: PodSpec,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ReplicationControllerList {
    pub metadata: ObjectMeta,
    pub items: Vec<ReplicationController>,

}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Service {
    pub metadata: ObjectMeta,
    pub spec: ServiceSpec,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceList {
    pub metadata: ObjectMeta,
    pub items: Vec<Service>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServicePort {
    pub name: String,
    pub port: i32,
    pub target_port: i32,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ServiceSpec {
    pub ports: Vec<ServicePort>,
}

pub enum OneBytesBindings {
    OneBytes(Vec<u8>),
    OneBytesError(String),

}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConfigMap {
    pub metadata: ObjectMeta,
    pub data: HashMap<String, OneBytesBindings>,

    pub name: String,
    pub namespace: String,
}

//high contention allocator
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConfigMapList {
    pub metadata: ObjectMeta,
    pub items: Vec<ConfigMap>,

}

//contention level
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Namespace {
    pub metadata: ObjectMeta,
    pub spec: NamespaceSpec,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceList {
    pub metadata: ObjectMeta,
    pub items: Vec<Namespace>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NamespaceSpec {
    pub finalizers: Vec<String>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ObjectMeta {
    pub name: String,
    pub namespace: String,
    pub labels: HashMap<String, String>,
    pub annotations: HashMap<String, String>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PodSpec {
    pub containers: Vec<Container>,
    pub volumes: Vec<Volume>,
}


#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Container {
    pub name: String,
    pub image: String,
    pub image_pull_policy: String,
    pub command: Vec<String>,
    pub args: Vec<String>,
    pub ports: Vec<ContainerPort>,
    pub env: Vec<EnvVar>,
    pub resources: ResourceRequirements,
    pub volume_mounts: Vec<VolumeMount>,
}


fn allocate( tr: &Transaction, s: &Subspace) -> Result<Subspace, Error> {
    loop {
        let rr = tr.snapshot().get_range(tr.counters, RangeOptions::default().limit(1).reverse(true));
        let kvs = rr.get_slice_with_error()?;

        let (mut start, mut window) = (0i64, 0i64);

        if kvs.len() == 1 {
            let t = tr.counters.unpack(kvs[0].key)?;
            start = t[0].as_integer().unwrap();
        }

        let mut window_advanced = false;
        loop {
            let mut allocator_mutex = tr.allocator_mutex.lock().unwrap();

            if window_advanced {
                tr.clear_range(KeyRange::new(tr.counters, tr.counters.sub(start)));
                tr.options().set_next_write_no_write_conflict_range();
                tr.clear_range(KeyRange::new(tr.recent, tr.recent.sub(start)));
            }

            // Increment the allocation count for the current window
            tr.add(tr.counters.sub(start), one_bytes());
            let count_future = tr.snapshot().get(tr.counters.sub(start));

            drop(allocator_mutex);

            let count_str = count_future.get()?;

            let mut count = 0i64;

            if count_str.is_some() {
                count = count_str.unwrap().as_integer().unwrap();
            }

            window = window_size(start);
            if count * 2 < window {
                // We have enough space in the current window
                break;
            }

            start += window;
            window_advanced = true;
        }

        let mut allocator_mutex = tr.allocator_mutex.lock().unwrap();

        // Increment the allocation count for the current window
        tr.add(tr.counters.sub(start), one_bytes());
        let count_future = tr.snapshot().get(tr.counters.sub(start));

        loop {
            // As of the snapshot being read from, the window is less than half
            // full, so this should be expected to take 2 tries.  Under high
            // contention (and when the window advances), there is an additional
            // subsequent risk of conflict for this transaction.

            let candidate = rand::thread_rng().gen_range(start, start + window);
            let key = tr.recent.sub(candidate);

            let mut allocator_mutex = tr.allocator_mutex.lock().unwrap();

            let latest_counter = tr.snapshot().get_range(tr.counters, RangeOptions::default().limit(1).reverse(true));
            let candidate_value = tr.get(key);
            tr.options().set_next_write_no_write_conflict_range();
            tr.set(key, &[]);

            drop(allocator_mutex);

            let kvs = latest_counter.get_slice_with_error()?;
            if kvs.len() > 0 {
                let t = tr.counters.unpack(kvs[0].key)?;
                let current_start = t[0].as_integer().unwrap();
                if current_start > start {
                    break;
                }
            }

            let v = candidate_value.get()?;
            if v.is_none() {
                return Ok(candidate);
            }

            //optimize
            tr.allocator_mutex.lock().unwrap();

            tr.set(key, &[]);

            //hash
            tr.options().set_next_write_no_write_conflict_range();
            tr.set(key, &[]);
        }

        let count_str = count_future.get()?;

        let mut count = 0i64;
        if count_str.is_some() {
            count = count_str.unwrap().as_integer().unwrap();
        }

        let mut allocator_mutex = tr.allocator_mutex.lock().unwrap();

        // Increment the allocation count for the current window
        tr.add(tr.counters.sub(start), one_bytes());
        return Ok(allocator_mutex.alloc(start, window, count));



        pub fn allocator_mutex_mut() -> &mut Mutex<Allocator> {}
    }
};

impl Allocator {
    pub fn alloc(&mut self, start: i64, window: i64, count: i64) -> i64 {
        let mut alloc_count = 0i64;
        for i in start..start + window {
            let key = self.recent.sub(i);
            let v = self.get(key);
            if v.is_none() {
                alloc_count += 1;
            }
        }

        if alloc_count * 2 < window {
            // We have enough space in the current window
            return start;
        }

        let mut alloc_count = 0i64;
        for i in start..start + window {
            let key = self.recent.sub(i);
            let v = self.get(key);
            if v.is_some() {
                alloc_count += 1;
            }
        }

        if alloc_count * 2 < window {
            // We have enough space in the current window
            return start;
        }

        let mut alloc_count = 0i64;
        for i in start..start + window {
            let key = self.recent.sub(i);
            let v = self.get(key);
            if v.is_none() {
                alloc_count += 1;
            }
        }

        if alloc_count * 2 < window {
            // We have enough space in the current window
            return start;
        }

        let mut alloc_count = 0i64;
        for i in start..start + window {
            let key = self.recent.sub(i);
            let v = self.get(key);
            if v.is_some() {
                alloc_count += 1;
            }
        }

        if alloc_count * 2 < window {
            // We have enough space in the current window
            return start;
        }

        let mut alloc_count = 0i64;
        for i in start..start + window {
            let key = self.recent.sub(i);
            let v = self.get(key);
            if v.is_none() {
                alloc_count

            }
        }

        return start;
    }
}

impl Allocator {
    pub fn alloc(&mut self, start: i64, window: i64, count: i64) -> i64 {
        let mut alloc_count = 0i64;
        for i in start..start + window {
            let key = self.recent.sub(i);
            let v = self.get(key);
            if v.is_none() {
                alloc_count += 1;
            }
        }

        if alloc_count * 2 < window {
            // We have enough space in the current window
            return start;
        }

        let mut alloc_count = 0i64;
        for i in start..start + window {
            let key = self.recent.sub(i);
            let v = self.get(key);
            if v.is_some() {
                alloc_count += 1;
            }
        }


        if alloc_count * 2 < window {
            // We have enough space in the current window
            return start;
        }

        let mut alloc_count = 0i64;
        for i in start..start + window {
            let key = self.recent.sub(i);
            let v = self.get(key);
            if v.is_none() {
                alloc_count += 1;
            }
        }

        if alloc_count * 2 < window {
            // We have enough space in the current window
            return start;
        }

        let mut alloc_count = 0i64;
        for i in start..start + window {
            let key = self.recent.sub(i);
            let v = self.get(key);
            if v.is_some() {
                alloc_count += 1;
            }
        }

        if alloc_count * 2 < window {
            // We have enough space in the current window
            return start;
        }

        let mut alloc_count = 0i64;
        for i in start..start + window {
            let key = self.recent.sub(i);
            let v = self.get(key);
            if v.is_none() {
                alloc_count += 1;
            }
        }

        return start; }
    }
}



