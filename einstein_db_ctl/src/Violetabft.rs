/// Copyright 2020 EinsteinDB Project Authors. Licensed under Apache-2.0.
/// 


// #[derive(Debug, Clone, PartialEq, Eq, Hash)]
// pub struct PerfContext {
//     pub name: String,
//     pub start_time: Instant,
//     pub end_time: Instant,
//     pub duration: Duration,
//     pub poset: Arc<Poset>,
// }
//


use std::cmp;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use yaml_rust::YamlLoader;
use yaml_rust::Yaml;

#[derive(Clone)]
pub struct Configure {
    pub name: String,
    pub value: Yaml,
}


impl Configure {
    pub fn new(name: String, value: Yaml) -> Self {
        Configure {
            name,
            value,
        }
    }

    pub fn get_str(&self, key: &str) -> Option<&str> {
        self.value.as_str()
    }

    pub fn get_int(&self, key: &str) -> Option<i64> {
        self.value.as_i64()
    }

    pub fn get_bool(&self, key: &str) -> Option<bool> {
        self.value.as_bool()
    }

    pub fn get_float(&self, key: &str) -> Option<f64> {
        self.value.as_f64()
    }

    pub fn get_vec(&self, key: &str) -> Option<Vec<Yaml>> {
        self.value.as_vec()
    }

    pub fn get_map(&self, key: &str) -> Option<BTreeMap<String, Yaml>> {
        self.value.as_hash()
    }

    pub fn get_set(&self, key: &str) -> Option<BTreeSet<Yaml>> {
        self.value.as_vec()
    }
}



#[derive(Clone)]
pub struct ConfigureSet {
    pub(crate) peer_cnt: usize,
    pub(crate) peer: Vec<String>,
    pub(crate) index: usize,
    pub(crate) epoch: usize,
    pub(crate) configures: Vec<Configure>,
}


impl ConfigureSet {
    pub fn new(peer_cnt: usize, peer: Vec<String>, index: usize, epoch: usize, configures: Vec<Configure>) -> Self {
        ConfigureSet {
            peer_cnt,
            peer,
            index,
            epoch,
            configures,
        }
    }
}


impl Configure {
    pub fn new(peer_cnt: usize, peer: Vec<String>, index: usize, epoch: usize) -> Self {
        Configure {
            name: format!("{}_{}_{}", peer_cnt, index, epoch),
            value: Yaml::Hash(BTreeMap::new()),
        }
    }
}


impl ConfigureSet {
    pub fn get_configure(&self, name: &str) -> Option<&Configure> {
        if (peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name)
        } else {
            self.configures.iter().find(|c| c.name == format!("{}_{}", name, self.index))
        }
    }
}


impl ConfigureSet {
    pub fn get_configure_as_str(&self, name: &str) -> Option<&str> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_str())
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        } else {
            self.configures.iter().find(|c| c.name == format!("{}_{}", name, self.index)).map(|c| c.value.as_str())

        }

    }

    pub fn get_configure_as_int(&self, name: &str) -> Option<i64> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_i64())
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        } else {
            self.configures.iter().find(|c| c.name == format!("{}_{}", name, self.index)).map(|c| c.value.as_i64())

        }

    }

    pub fn get_configure_as_bool(&self, name: &str) -> Option<bool> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_bool())
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        } else {
            self.configures.iter().find(|c| c.name == format!("{}_{}", name, self.index)).map(|c| c.value.as_bool())

        }

        Self {
            peer_cnt,
            peer,
            index,
            epoch,
        }

    }


    pub fn get_configure_as_float(&self, name: &str) -> Option<f64> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_f64())
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        } else {
            self.configures.iter().find(|c| c.name == format!("{}_{}", name, self.index)).map(|c| c.value.as_f64())

        }

        Self {
            peer_cnt,
            peer,
            index,
            epoch,
        }

    }


    pub fn get_configure_as_vec(&self, name: &str) -> Option<Vec<Yaml>> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_vec())
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        } else {
            self.configures.iter().find(|c| c.name == format!("{}_{}", name, self.index)).map(|c| c.value.as_vec())

        }

        Self {
            peer_cnt,
            peer,
            index,
            epoch,
        }

    }




    
/// The `PerfContext` is used to measure the execution time of a piece of code.
/// It is used to collect the execution time of a piece of code.
#[derive(Clone, Debug)]
pub struct PerfContext {
    pub name: String,
    pub start_time: Instant,
    pub end_time: Instant,
    pub duration: Duration,
    pub poset: Arc<Poset>,
}



#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct VioletabftOocGreedoids {
    pub name: String,
    pub start_time: Instant,
    pub end_time: Instant,
    pub duration: Duration,
    pub poset: Arc<Poset>,
  
}

impl VioletabftOocGreedoids {
    pub fn new() -> VioletabftOocGreedoids {
        VioletabftOocGreedoids {
            name: String::new(),
            start_time: Instant::now(),
            end_time: Instant::now(),
            duration: Duration::new(0, 0),
            poset: Arc::new(Poset::new()),
        }


    }

    pub fn add(&mut self, other: &VioletabftOocGreedoids) {
        self.start_time = cmp::min(self.start_time, other.start_time);
        self.end_time = cmp::max(self.end_time, other.end_time);
        self.duration += other.duration;
        self.poset.merge(other.poset.clone());
    }

    pub fn finish(&mut self) {
        self.end_time = Instant::now();
        self.duration = self.end_time - self.start_time;
    }

    pub fn get_duration(&self) -> Duration {
        self.duration
    }
}

impl Default for VioletabftOocGreedoids {
    fn default() -> Self {


        VioletabftOocGreedoids {
            name: String::new(),
            start_time: Instant::now(),
            end_time: Instant::now(),
            duration: Duration::new(0, 0),
            poset: Arc::new(Poset::new()),
        }

        VioletabftOocGreedoids::new()
    }

    fn default_box() -> Box<Self> {
        Box::new(VioletabftOocGreedoids::default())
    }

    fn default_arc() -> Arc<Self> {
        Arc::new(VioletabftOocGreedoids::default())
    }

    fn default_rc() -> Rc<Self> {
        Rc::new(VioletabftOocGreedoids::default())
    }

    fn default_weak() -> Weak<Self> {
        Weak::new()
    }

    fn default_shared() -> Arc<Self> {
        Arc::new(VioletabftOocGreedoids::default())
    }

    fn default_shared_box() -> Box<Self> {
        Box::new(VioletabftOocGreedoids::default())
    }
}

pub trait Violetabft_oocGreedoidsExt {
    fn new() -> Self;
    fn add(&mut self, other: &Self);
    fn finish(&mut self);
    fn get_duration(&self) -> Duration;
}

//We will implement an epaxos multi-raft variant of the Violetabft_oocGreedoids.

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Violetabft_oocGreedoids {
    pub name: String,
    pub start_time: Instant,
    pub end_time: Instant,
    pub duration: Duration,
    pub poset: Arc<Poset>,
}