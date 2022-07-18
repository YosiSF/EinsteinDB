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

use command_parser::{
    Command,
};

use command_parser::{
    COMMAND_CACHE,
    COMMAND_EXIT_LONG,
    COMMAND_EXIT_SHORT,
    COMMAND_HELP,
    COMMAND_IMPORT_LONG,
    COMMAND_OPEN,
    COMMAND_QUERY_LONG,
    COMMAND_QUERY_SHORT,
    COMMAND_QUERY_EXPLAIN_LONG,
    COMMAND_QUERY_EXPLAIN_SHORT,
    COMMAND_QUERY_PREPARED_LONG,
    COMMAND_SCHEMA,
    COMMAND_TIMER_LONG,
    COMMAND_TRANSACT_LONG,
    COMMAND_TRANSACT_SHORT,
};

// These are still defined when this feature is disabled (so that we can
// give decent error messages when a user tries open_encrypted when
// we weren't compiled with sqlcipher), but they're unused, since we
// omit them from help message (since they wouldn't work).
#[cfg(feature = "sqlcipher")]
use command_parser::{
    COMMAND_OPEN_ENCRYPTED,
};

#[cfg(feature = "syncable")]
use command_parser::{
    COMMAND_SYNC,
};

use input::InputReader;
use input::InputResult::{
    Empty,
    Eof,
    MetaCommand,
    More,
};

lazy_static! {
    static ref HELP_COMMANDS: Vec<(&'static str, &'static str)> = {
        vec![
            (COMMAND_HELP, "Show this message."),

            (COMMAND_EXIT_LONG, "Close the current database and exit the REPL."),
            (COMMAND_EXIT_SHORT, "Shortcut for `.exit`. Close the current database and exit the REPL."),

            (COMMAND_OPEN, "Open a database at path."),

            #[cfg(feature = "sqlcipher")]
            (COMMAND_OPEN_ENCRYPTED, "Open an encrypted database at path using the provided key."),

            (COMMAND_SCHEMA, "Output the schema for the current open database."),

            (COMMAND_IMPORT_LONG, "Transact the contents of a file against the current open database."),

            (COMMAND_QUERY_LONG, "Execute a query against the current open database."),
            (COMMAND_QUERY_SHORT, "Shortcut for `.query`. Execute a query against the current open database."),

            (COMMAND_QUERY_PREPARED_LONG, "Prepare a query against the current open database, then run it, timed."),

            (COMMAND_TRANSACT_LONG, "Execute a transact against the current open database."),
            (COMMAND_TRANSACT_SHORT, "Shortcut for `.transact`. Execute a transact against the current open database."),

            (COMMAND_QUERY_EXPLAIN_LONG, "Show the SQL and query plan that would be executed for a given query."),
            (COMMAND_QUERY_EXPLAIN_SHORT, "Shortcut for `.explain_query`. Show the SQL and query plan that would be executed for a given query."),

            (COMMAND_TIMER_LONG, "Enable or disable timing of query and transact operations."),

            (COMMAND_CACHE, "Cache an attribute. Usage: `.cache :foo/bar reverse`"),

            #[cfg(feature = "syncable")]
            (COMMAND_SYNC, "Synchronize the database against a Mentat Sync Server URL for a provided user UUID."),
        ]
    };
}

use std::cmp;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::os::unix::thread;
use std::rc::{Rc, Weak};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use Command;
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



/// VioletaBFT is a Byzantine fault tolerant consensus algorithm.
/// It is a variant of BFT where Byzantine faults are injected into the system.
/// It is a variant of Epaxos and HoneyBadger.
/// Egalitarian Byzantine Fault Tolerant Consensus Algorithm.
/// It is a variant of Byzantine Fault Tolerant Consensus Algorithm.


impl Configure {
    pub fn new(peer_cnt: usize, peer: Vec<String>, index: usize, epoch: usize) -> Self {
        Configure {
            name: format!("{}_{}_{}", peer_cnt, index, epoch),
            value: Yaml::Hash(BTreeMap::new()),
        }
    }
}





#[derive(Clone)]
pub struct ConfigureSetBuilder {
    pub(crate) peer_cnt: usize,
    pub(crate) peer: Vec<String>,
    pub(crate) index: usize,
    pub(crate) epoch: usize,
    pub(crate) configures: Vec<Configure>,
}


#[derive(Clone)]
pub struct ConfigureSetBuilderBuilder {
    pub(crate) peer_cnt: usize,
    pub(crate) peer: Vec<String>,
    pub(crate) index: usize,
    pub(crate) epoch: usize,
    pub(crate) configures: Vec<Configure>,
}




///! ConfigureSetBuilderBuilder is a builder for ConfigureSet.
/// It is a builder for ConfigureSet.
/// !



impl ConfigureSet {
    pub fn get_configure(&self, name: &str) -> Option<&Configure> {
        if (peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name)
        } else {
            self.configures.iter().find(|c| c.name == format!("{}_{}", name, self.index))
        }
    }


    pub fn get_configure_as_bool(&self, name: &str) {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_bool());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }


    pub fn get_configure_as_str(&self, name: &str) {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_str());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }


        // if (self.peer_cnt % 2) == 0 {
        //     self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_str());
        // } else {
        //     panic!("The peer count should be odd, but we got {}", peer_cnt);
        // }


        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_str());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }


    pub fn get_configure_as_int(&self, name: &str) {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_i64());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }


    pub fn get_configure_as_float(&self, name: &str) {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_f64());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }


    pub fn get_configure_as_vec(&self, name: &str) {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_vec());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }


    pub fn get_configure_as_map(&self, name: &str) {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_hash());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }


    pub fn get_configure_as_set(&self, name: &str) {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_set());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }
}


impl ConfigureSetBuilder {
    pub fn get_configure_as_int(&self, name: &str) {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_i64());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }



}




///! Start a new performance context.
///




impl ConfigureSetBuilder {
    pub fn new(peer_cnt: usize, peer: Vec<String>, index: usize, epoch: usize) -> Self {
        ConfigureSetBuilder {
            peer_cnt,
            peer,
            index,
            epoch,
            configures: Vec::new(),
        }
    }
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
            name: "VioletabftOocGreedoids".to_string(),
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
    }
}


impl VioletabftOocGreedoids {
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

pub trait VioletabftOocGreedoidsExt {
    fn new() -> Self;
    fn add(&mut self, other: &Self);
    fn finish(&mut self);
    fn get_duration(&self) -> Duration;
}

//We will implement an epaxos multi-raft variant of the VioletabftOocGreedoids.



    fn eprint_out(s: &str) {
        eprint!("{green}{s}{reset}", green = color::Fg(::GREEN), s = s, reset = color::Fg(color::Reset));
    }

    fn parse_namespaced_keyword(input: &str) -> Option<Keyword> {
        let splits = [':', '/'];
        let mut i = input.split(&splits[..]);
        match (i.next(), i.next(), i.next(), i.next()) {
            (Some(""), Some(namespace), Some(name), None) => {
                Some(Keyword::namespaced(namespace, name))
            },
            _ => None,
        }
    }

    fn format_time(duration: Duration) {
        let secs = duration.as_secs();
        let millis = duration.subsec_nanos() / 1_000_000;
        let micros = duration.subsec_nanos() / 1_000;
        let nanos = duration.subsec_nanos();
        let m_nanos = duration.num_nanoseconds();
        if let Some(nanos) = m_nanos {
            if nanos < 1_000 {
                eprintln!("{bold}{nanos}{reset}ns",
                          bold = style::Bold,
                          nanos = nanos,
                          reset = style::Reset);
                return;
            }
        }

        let m_micros = duration.num_microseconds();
        if let Some(micros) = m_micros {

            if micros < 1_000 {
                eprintln!("{bold}{micros}{reset}µs",
                          bold = style::Bold,
                          micros = micros,
                          reset = style::Reset);
                return;
            }

            if micros < 1_000_000 {
                eprintln!("{bold}{micros}{reset}ms",
                          bold = style::Bold,
                          micros = micros,
                          reset = style::Reset);
                return;
            }
        }

        let millis = duration.num_milliseconds();
        let seconds = (millis as f64) / 1000f64;
        eprintln!("{bold}{seconds}{reset}s",
                  bold = style::Bold,
                  seconds = seconds,
                  reset = style::Reset);
    }

    /// Executes input and maintains state of persistent items.
    pub struct Repl {
        pub config: Config,

        pub state: State,
        pub commands: Commands,
        pub commands_mutex: Mutex<Commands>,
        pub perf: PerfContext,
        pub violetabft_ooc_greedoids: VioletabftOocGreedoids,
        input_reader: InputReader,
        pub output_writer: OutputWriter,
        pub output_writer_thread: Option<thread::JoinHandle<()>>,
        store: Store,
        timer_on: bool,
        timer_start: Instant,

        pub logger: Logger,
        pub logger_thread: Option<thread::JoinHandle<()>>,
        pub logger_thread_handle: Option<thread::JoinHandle<()>>,
        pub logger_thread_handle_mutex: Mutex<Option<thread::JoinHandle<()>>>,

        pub path: String,
    }



    impl Repl {
        pub fn new(config: Config) -> Repl {
            let state = State::new();
            let commands = Commands::new();
            let commands_mutex = Mutex::new(commands);
            let perf = PerfContext::new();
            let violetabft_ooc_greedoids = VioletabftOocGreedoids::new();
            let input_reader = InputReader::new();
            let output_writer = OutputWriter::new();
            let output_writer_thread = None;
            let store = Store::new();
            let timer_on = false;
            let timer_start = Instant::now();
            let logger = Logger::new();
            let logger_thread = None;
        }
    }

    impl Repl {
        pub fn db_name(&self) -> String {
            if self.path.is_empty() {
                "in-memory db".to_string()
            } else {
                self.path.clone()
            }

            while let Some(line) = self.input_reader.read_line() {
                let line = line.trim();
                if line.is_empty() {
                    continue;
                }
                if line.starts_with("#") {
                    continue;
                }
                if line.starts_with("db ") {
                    let db_name = line[3..].trim();
                    if db_name.is_empty() {
                        eprintln!("db name cannot be empty");
                        continue;
                    }
                    return db_name.to_string();
                }
            }
            "in-memory db".to_string()

        }
    }

