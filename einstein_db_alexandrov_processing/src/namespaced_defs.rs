/* Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0. */
//default pod-network and service-network

use einstein_db_ctl::{
    config::{Config, ConfigFile},
    error::Result,
    prelude::*,
};

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

#[inline]
pub fn allocate(&self, tr: &Transaction, s: &Subspace) -> Result<Subspace, Error> {
    loop {
        let rr = tr.snapshot().get_range(self.counters, RangeOptions::default().limit(1).reverse(true));
        let kvs = rr.get_slice_with_error()?;

        let (mut start, mut window) = (0i64, 0i64);

        if kvs.len() == 1 {
            let t = self.counters.unpack(kvs[0].key)?;
            start = t[0].as_integer().unwrap();
        }

        let mut window_advanced = false;
        loop {
            let mut allocator_mutex = self.allocator_mutex.lock().unwrap();

            if window_advanced {
                tr.clear_range(KeyRange::new(self.counters, self.counters.sub(start)));
                tr.options().set_next_write_no_write_conflict_range();
                tr.clear_range(KeyRange::new(self.recent, self.recent.sub(start)));
            }

            // Increment the allocation count for the current window
            tr.add(self.counters.sub(start), one_bytes());
            let count_future = tr.snapshot().get(self.counters.sub(start));

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

        let mut allocator_mutex = self.allocator_mutex.lock().unwrap();

        // Increment the allocation count for the current window
        tr.add(self.counters.sub(start), one_bytes());
        let count_future = tr.snapshot().get(self.counters.sub(start));

        loop {
            // As of the snapshot being read from, the window is less than half
            // full, so this should be expected to take 2 tries.  Under high
            // contention (and when the window advances), there is an additional
            // subsequent risk of conflict for this transaction.
            let candidate = rand::thread_rng().gen_range(start, start + window);
            let key = self.recent.sub(candidate);

            let mut allocator_mutex = self.allocator_mutex.lock().unwrap();

            let latest_counter = tr.snapshot().get_range(self.counters, RangeOptions::default().limit(1).reverse(true));
            let candidate_value = tr.get(key);
        }
    }

