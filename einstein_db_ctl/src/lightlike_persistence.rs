// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.


///! Lightlike persistence
///
/// # Lightlike persistence
///
/// Lightlike persistence is a simple persistence layer that stores data in memory.
///
/// # Example
///
/// ```
/// use einstein_db_ctl::lightlike_persistence::LightlikePersistence;
///
/// let mut persistence = LightlikePersistence::new();
/// persistence.set("key", "value");
/// assert_eq!(persistence.get("key"), Some("value"));
/// ```
/// it works as a knowledge base for the control plane.
///
/// # Note
///
/// This is a simple implementation that does not support transactions.
///
/// # Limitations
///
/// This is a simple implementation that does not support transactions.


use std::collections::HashMap;
use std::sync::{Arc, RwLock};


pub struct LightlikePersistence {
    data: Arc<RwLock<HashMap<String, String>>>
}


impl LightlikePersistence {
    pub fn new() -> Self {
        LightlikePersistence {
            data: Arc::new(RwLock::new(HashMap::new()))
        }
    }

    pub fn set(&self, key: &str, value: &str) {
        let mut data = self.data.write().unwrap();
        data.insert(key.to_string(), value.to_string());
    }

    pub fn get(&self, key: &str) -> Option<String> {
        let data = self.data.read().unwrap();
        data.get(key).map(|v| v.to_string())
    }
}



