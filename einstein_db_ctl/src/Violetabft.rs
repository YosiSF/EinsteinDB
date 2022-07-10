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


    pub fn get_configure_as_bool(&self, name: &str) -> Option<bool> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_bool());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }


    pub fn get_configure_as_str(&self, name: &str) -> Option<&str> {
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


    pub fn get_configure_as_int(&self, name: &str) -> Option<i64> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_i64());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }


    pub fn get_configure_as_float(&self, name: &str) -> Option<f64> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_f64());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }


    pub fn get_configure_as_vec(&self, name: &str) -> Option<Vec<Yaml>> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_vec());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }


    pub fn get_configure_as_map(&self, name: &str) -> Option<BTreeMap<String, Yaml>> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_hash());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }


    pub fn get_configure_as_set(&self, name: &str) -> Option<BTreeSet<Yaml>> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_set());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        }
    }
}


impl ConfigureSetBuilder {
    pub fn get_configure_as_int(&self, name: &str) -> Option<i64> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_i64());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        };
    }

    pub fn get_configure_as_float(&self, name: &str) -> Option<f64> {
        if (self.peer_cnt % 2) == 0 {
            self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_f64());
        } else {
            panic!("The peer count should be odd, but we got {}", peer_cnt);
        };

        loop {
            if (self.peer_cnt % 2) == 0 {
                self.configures.iter().find(|c| c.name == name).map(|c| c.value.as_f64());
            } else {
                panic!("The peer count should be odd, but we got {}", peer_cnt);
            };
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

    impl Repl {

        /// Constructs a new `repl`.
        pub fn new(tty: bool) -> Result<Repl, String> {
            let interface = if tty {
                Some(Interface::new("mentat").map_err(|_| "failed to create tty interface; try --no-tty")?)
            } else {
                None
            };

            let input_reader = InputReader::new(interface);

            let store = Store::open("").map_err(|e| e.to_string())?;
            Ok(Repl {
                config: (),
                state: (),
                commands: (),
                commands_mutex: (),
                perf: (),
                violetabft_ooc_greedoids: VioletabftOocGreedoids {
                    name: (),
                    start_time: (),
                    end_time: (),
                    duration: (),
                    poset: ()
                },
                input_reader,
                output_writer: (),
                path: "".to_string(),
                store,
                timer_on: false,
                timer_start: (),
                logger: (),
                logger_thread: (),
                logger_thread_handle: (),
                output_writer_thread: (),
                logger_thread_handle_mutex: ()
            })
        }

        /// Runs the REPL interactively.
        pub fn run(&mut self, startup_commands: Option<Vec<Command>>) {
            if let Some(cmds) = startup_commands {
                for command in cmds.iter() {
                    println!("{}", command.output());
                    self.handle_command(command.clone());
                }
            }

            loop {
                let res = self.input_reader.read_input();

                match res {
                    Ok(MetaCommand(cmd)) => {
                        debug!("read command: {:?}", cmd);
                        if !self.handle_command(cmd) {
                            break;
                        }
                    },
                    Ok(Empty) |
                    Ok(More) => (),
                    Ok(Eof) => {
                        if self.input_reader.is_tty() {
                            println!();
                        }
                        break;
                    },
                    Err(e) => eprintln!("{}", e.to_string()),
                }
            }

            self.input_reader.save_history();
        }

        fn cache(&mut self, attr: String, direction: CacheDirection) {
            if let Some(kw) = parse_namespaced_keyword(attr.as_str()) {
                match self.store.cache(&kw, direction) {
                    Result::Ok(_) => (),
                    Result::Err(e) => eprintln!("Couldn't cache attribute: {}", e),
                };
            } else {
                eprintln!("Invalid attribute {}", attr);
            }
        }

        /// Runs a single command input.
        fn handle_command(&mut self, cmd: Command) -> bool {
            let should_print_times = self.timer_on && cmd.is_timed();

            let mut start = PreciseTime::now();
            let mut end: Option<PreciseTime> = None;

            match cmd {
                Command::Cache(attr, direction) => {
                    self.cache(attr, direction);
                },
                Command::Close => {
                    self.close();
                },
                Command::Exit => {
                    eprintln!("Exiting…");
                    return false;
                },
                Command::Help(args) => {
                    self.help_command(args);
                },
                Command::Import(path) => {
                    self.execute_import(path);
                },
                Command::Open(db) => {
                    match self.open(db) {
                        Ok(_) => println!("Database {:?} opened", self.db_name()),
                        Err(e) => eprintln!("{}", e.to_string()),
                    };
                },
                Command::OpenEncrypted(db, encryption_key) => {
                    match self.open_with_key(db, &encryption_key) {
                        Ok(_) => println!("Database {:?} opened with key {:?}", self.db_name(), encryption_key),
                        Err(e) => eprintln!("{}", e.to_string()),
                    }
                },
                Command::Query(query) => {
                    self.store
                        .q_once(query.as_str(), None)
                        .map_err(|e| e.into())
                        .and_then(|o| {
                            end = Some(PreciseTime::now());
                            self.print_results(o)
                        })
                        .map_err(|err| {
                            eprintln!("{:?}.", err);
                        })
                        .ok();
                },
                Command::QueryExplain(query) => {
                    self.explain_query(query);
                },
                Command::QueryPrepared(query) => {
                    self.store
                        .q_prepare(query.as_str(), None)
                        .and_then(|mut p| {
                            let prepare_end = PreciseTime::now();
                            if should_print_times {
                                eprint_out("Prepare time");
                                eprint!(": ");
                                format_time(start.to(prepare_end));
                            }
                            // This is a hack.
                            start = PreciseTime::now();
                            let r = p.run(None);
                            end = Some(PreciseTime::now());
                            return r;
                        })
                        .map(|o| self.print_results(o))
                        .map_err(|err| {
                            eprintln!("{:?}.", err);
                        })
                        .ok();
                },
                Command::Schema => {
                    let edn = self.store.conn().current_schema().to_edn_value();
                    match edn.to_pretty(120) {
                        Ok(s) => println!("{}", s),
                        Err(e) => eprintln!("{}", e)
                    };
                },

                #[cfg(feature = "syncable")]
                Command::Sync(args) => {
                    match self.store.sync(&args[0], &args[1]) {
                        Ok(report) => println!("Sync report: {}", report),
                        Err(e) => eprintln!("{:?}", e)
                    };
                },

                #[cfg(not(feature = "syncable"))]
                Command::Sync(_) => {
                    eprintln!(".sync requires the syncable Mentat feature");
                },

                Command::Timer(on) => {
                    self.toggle_timer(on);
                },
                Command::Transact(transaction) => {
                    self.execute_transact(transaction);
                },
            }

            let end = end.unwrap_or_else(PreciseTime::now);
            if should_print_times {
                eprint_out("Run time");
                eprint!(": ");
                format_time(start.to(end));
            }

            return true;
        }

        fn execute_import<T>(&mut self, path: T)
            where T: Into<String> {
            use ::std::io::Read;
            let path = path.into();
            let mut content: String = "".to_string();
            match ::std::fs::File::open(path.clone()).and_then(|mut f| f.read_to_string(&mut content)) {
                Ok(_) => self.execute_transact(content),
                Err(e) => eprintln!("Error reading file {}: {}", path, e)
            }
        }

        fn open_common(
            &mut self,
            path: String,
            encryption_key: Option<&str>
        ) -> ::mentat::errors::Result<()> {
            if self.path.is_empty() || path != self.path {
                let next = match encryption_key {
                    #[cfg(not(feature = "sqlcipher"))]
                    Some(_) => return Err(::mentat::MentatError::RusqliteError(".open_encrypted requires the sqlcipher Mentat feature".into(), "".into())),
                    #[cfg(feature = "sqlcipher")]
                    Some(k) => {
                        Store::open_with_key(path.as_str(), k)?
                    },
                    _ => {
                        Store::open(path.as_str())?
                    }
                };
                self.path = path;
                self.store = next;
            }

            Ok(())
        }

        fn open<T>(&mut self, path: T) -> ::mentat::errors::Result<()> where T: Into<String> {
            self.open_common(path.into(), None)
        }

        fn open_with_key<T, U>(&mut self, path: T, encryption_key: U)
                               -> ::mentat::errors::Result<()> where T: Into<String>, U: AsRef<str> {
            self.open_common(path.into(), Some(encryption_key.as_ref()))
        }

        // Close the current store by opening a new in-memory store in its place.
        fn close(&mut self) {
            let old_db_name = self.db_name();
            match self.open("") {
                Ok(_) => println!("Database {:?} closed.", old_db_name),
                Err(e) => eprintln!("{}", e),
            };
        }

        fn toggle_timer(&mut self, on: bool) {
            self.timer_on = on;
        }

        fn help_command(&self, args: Vec<String>) {
            let stdout = ::std::io::stdout();
            let mut output = TabWriter::new(stdout.lock());
            if args.is_empty() {
                for &(cmd, msg) in HELP_COMMANDS.iter() {
                    write!(output, ".{}\t", cmd).unwrap();
                    writeln!(output, "{}", msg).unwrap();
                }
            } else {
                for mut arg in args {
                    if arg.chars().nth(0).unwrap() == '.' {
                        arg.remove(0);
                    }
                    if let Some(&(cmd, msg)) = HELP_COMMANDS.iter()
                        .filter(|&&(c, _)| c == arg.as_str())
                        .next() {
                        write!(output, ".{}\t", cmd).unwrap();
                        writeln!(output, "{}", msg).unwrap();
                    } else {
                        eprintln!("Unrecognised command {}", arg);
                        return;
                    }
                }
            }
            writeln!(output, "").unwrap();
            output.flush().unwrap();
        }

        fn print_results(&self, query_output: QueryOutput) -> Result<(), Error> {
            let stdout = ::std::io::stdout();
            let mut output = TabWriter::new(stdout.lock());

            // Print the column headers.
            for e in query_output.spec.columns() {
                write!(output, "| {}\t", e)?;
            }
            writeln!(output, "|")?;
            for _ in 0..query_output.spec.expected_column_count() {
                write!(output, "---\t")?;
            }
            writeln!(output, "")?;

            match query_output.results {
                QueryResults::Scalar(v) => {
                    if let Some(val) = v {
                        writeln!(output, "| {}\t |", &self.binding_as_string(&val))?;
                    }
                },

                QueryResults::Tuple(vv) => {
                    if let Some(vals) = vv {
                        for val in vals {
                            write!(output, "| {}\t", self.binding_as_string(&val))?;
                        }
                        writeln!(output, "|")?;
                    }
                },

                QueryResults::Coll(vv) => {
                    for val in vv {
                        writeln!(output, "| {}\t|", self.binding_as_string(&val))?;
                    }
                },

                QueryResults::Rel(vvv) => {
                    for vv in vvv {
                        for v in vv {
                            write!(output, "| {}\t", self.binding_as_string(&v))?;
                        }
                        writeln!(output, "|")?;
                    }
                },
            }
            for _ in 0..query_output.spec.expected_column_count() {
                write!(output, "---\t")?;
            }
            writeln!(output, "")?;
            output.flush()?;
            Ok(())
        }

        pub fn explain_query(&self, query: String) {
            match self.store.q_explain(query.as_str(), None) {
                Result::Err(err) =>
                    println!("{:?}.", err),
                Result::Ok(QueryExplanation::KnownConstant) =>
                    println!("Query is known constant!"),
                Result::Ok(QueryExplanation::KnownEmpty(empty_because)) =>
                    println!("Query is known empty: {:?}", empty_because),
                Result::Ok(QueryExplanation::ExecutionPlan { query, steps }) => {
                    println!("SQL: {}", query.sql);
                    if !query.args.is_empty() {
                        println!("  Bindings:");
                        for (arg_name, value) in query.args {
                            println!("    {} = {:?}", arg_name, *value)
                        }
                    }

                    println!("Plan: select id | order | from | detail");
                    // Compute the number of columns we need for order, select id, and from,
                    // so that longer query plans don't become misaligned.
                    let (max_select_id, max_order, max_from) = steps.iter().fold((0, 0, 0), |acc, step|
                        (acc.0.max(step.select_id), acc.1.max(step.order), acc.2.max(step.from)));
                    // This is less efficient than computing it via the logarithm base 10,
                    // but it's clearer and doesn't have require special casing "0"
                    let max_select_digits = max_select_id.to_string().len();
                    let max_order_digits = max_order.to_string().len();
                    let max_from_digits = max_from.to_string().len();
                    for step in steps {
                        // Note: > is right align.
                        println!("  {:>sel_cols$}|{:>ord_cols$}|{:>from_cols$}|{}",
                                 step.select_id, step.order, step.from, step.detail,
                                 sel_cols = max_select_digits,
                                 ord_cols = max_order_digits,
                                 from_cols = max_from_digits);
                    }
                }
            };
        }

        pub fn execute_transact(&mut self, transaction: String) {
            match self.transact(transaction) {
                Result::Ok(report) => println!("{:?}", report),
                Result::Err(err) => eprintln!("Error: {:?}.", err),
            }
        }

        fn transact(&mut self, transaction: String) -> ::mentat::errors::Result<TxReport> {
            let mut tx = self.store.begin_transaction()?;
            let report = tx.transact(transaction)?;
            tx.commit()?;
            Ok(report)
        }

        fn binding_as_string(&self, value: &Binding) -> String {
            use self::Binding::*;
            match value {
                &Scalar(ref v) => self.value_as_string(v),
                &Map(ref v) => self.map_as_string(v),
                &Vec(ref v) => self.vec_as_string(v),
            }
        }

        fn vec_as_string(&self, value: &Vec<Binding>) -> String {
            let mut out: String = "[".to_string();
            let vals: Vec<String> = value.iter()
                .map(|v| self.binding_as_string(v))
                .collect();

            out.push_str(vals.join(", ").as_str());
            out.push_str("]");
            out
        }

        fn map_as_string(&self, value: &StructuredMap) -> String {
            let mut out: String = "{".to_string();
            let mut first = true;
            for (k, v) in value.0.iter() {
                if !first {
                    out.push_str(", ");
                    first = true;
                }
                out.push_str(&k.to_string());
                out.push_str(" ");
                out.push_str(self.binding_as_string(v).as_str());
            }
            out.push_str("}");
            out
        }

        fn value_as_string(&self, value: &TypedValue) -> String {
            use self::TypedValue::*;
            match value {
                &Boolean(b) => if b { "true".to_string() } else { "false".to_string() },
                &Double(d) => format!("{}", d),
                &Instant(ref i) => format!("{}", i),
                &Keyword(ref k) => format!("{}", k),
                &Long(l) => format!("{}", l),
                &Ref(r) => format!("{}", r),
                &String(ref s) => format!("{:?}", s.to_string()),
                &Uuid(ref u) => format!("{}", u),
            }
        }
    }