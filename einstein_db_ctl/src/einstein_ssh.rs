use std::{str, u64};
use std::borrow::ToOwned;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::fmt::Result;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{TcpListener, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};

use std::lazy::SyncLazy;
use std::string::ToString;
use linefeed::{Interface, ReadResult};
use linefeed::complete::{Completer, Completion};
use linefeed::complete::word::WordCompleter;
use linefeed::complete::line::LineCompleter;
use structopt::StructOpt;



use crate::einstein_db_ctl::einstein_db_ctl::EinsteinDB;
use crate::einstein_db_ctl::einstein_db_ctl::EinsteinDBError;

use crate::einstein_db_ctl::einstein_db_ctl::*;

#[derive(StructOpt, Debug)]
#[structopt(name = "einstein_db_ctl")]
struct Opt {
    #[structopt(subcommand)]
    cmd: Command,
}

const BI_KEY_HINT: &str = "Bimap soliton_ids(generally starts with \"einst\") in delimiter";
static VERSION_INFO: SyncLazy<String> = SyncLazy::new(|| {
    let build_timestamp = option_env!("EINSTEINDB_BUILD_TIME");
    einstein_db::einstein_db_version_info(build_timestamp)
});



use structopt::StructOpt;

const CAUSET_RAW_KEY_HINT: &str = "Raw key (generally starts with \"z\") in escaped form";
static EINSTEIN_DB_VERSION_INFO: SyncLazy<String> = SyncLazy::new(|| {

    let build_timestamp = option_env!("EINSTEINDB_BUILD_TIME");
    einstein_db::einstein_db_version_info(build_timestamp);
    option_env!("EINSTEINDB_BUILD_TIME");
});





pub struct EinsteinDBCli {
    #[structopt(subcommand)]
    cmd: Command,
}


impl EinsteinDBCli {
    pub fn new() -> Self {
        EinsteinDBCli {
            cmd: Command::new(),
        }
    }

    pub fn run(&self) -> Result<(), EinsteinDBError> {
        match self.cmd {
            Command::Ssh(ssh_opt) => {
                let mut einstein_db = EinsteinDB::new();
                einstein_db.run_ssh(ssh_opt)?;
            }
            Command::Cmd(cmd_opt) => {
                let mut einstein_db = EinsteinDB::new();
                einstein_db.run_cmd(cmd_opt)?;
            }
        }
        Ok(())
    }

    pub fn run_ssh(&self, ssh_opt: SshOpt) -> Result<(), EinsteinDBError> {
        let mut einstein_db = EinsteinDB::new();
        einstein_db.run_ssh(ssh_opt)?;
        Ok(())
    }

    pub fn run_cmd(&self, cmd_opt: CmdOpt) -> Result<(), EinsteinDBError> {
        let mut einstein_db = EinsteinDB::new();
        einstein_db.run_cmd(cmd_opt)?;
        Ok(())
    }

    pub fn run_cmd_with_einstein_db(&self, cmd_opt: CmdOpt, einstein_db: EinsteinDB) -> Result<(), EinsteinDBError> {
        einstein_db.run_cmd(cmd_opt)?;
        Ok(())
    }
}


//fidel is an interlocking multiplexer for attributes of a single entity.


#[derive(StructOpt, Debug)]
#[structopt(name = "einstein_db_ctl")]
enum Command {
    #[structopt(name = "ssh")]
    Ssh(SshOpt),
    #[structopt(name = "Cmd")]
    Cmd(CmdOpt),
}


#[derive(StructOpt, Debug)]
#[structopt(name = "einstein_db_ctl")]
struct SshOpt {
    #[structopt(long)]
    /// Set the address of fidel
    pub fidel: Option<String>,

    #[structopt(long, default_causet_locale = "warn")]
    /// Set the log level
    pub log_level: String,

    #[structopt(long)]
    /// Set the remote host
    pub host: Option<String>,

    #[structopt(long)]
    /// Set the CA certificate path
    pub ca_path: Option<String>,

    #[structopt(long)]
    /// Set the certificate path
    pub cert_path: Option<String>,

    #[structopt(long)]
    /// Set the private soliton_id path
    pub soliton_id_path: Option<String>,


    #[structopt(long)]
    /// Set the remote port
    /// Default: 22
    pub port: Option<u64>,


    #[structopt(long)]
    /// EinsteinDB data-dir, check <deploy-dir>/scripts/run.sh to get it
    pub data_dir: Option<String>,

    #[structopt(long)]
    /// Skip paranoid checks when open foundationdb
    pub skip_paranoid_checks: bool,

    #[allow(dead_code)]
    #[structopt(
    long,
    validator = |_| Err("DEPRECATED!!! Use --data-dir and --config instead".to_owned()),
    )]
    /// Set the foundationdb path
    pub einstein_db: Option<String>,

    #[allow(dead_code)]
    #[structopt(
    long,
    validator = |_| Err("DEPRECATED!!! Use --data-dir and --config instead".to_owned()),
    )]
    /// Set the violetabft foundationdb path
    pub violetabft_tangaroa: Option<String>,

    #[structopt(conflicts_with = "escaped-to-hex", long = "to-escaped")]
    /// Convert a hex soliton_id to escaped soliton_id
    pub hex_to_escaped: Option<String>,

    #[structopt(conflicts_with = "hex-to-escaped", long = "escaped-to-hex")]
    /// Convert an escaped soliton_id to hex soliton_id
    ///
    /// The escaped soliton_id is a hex string of a soliton_id with the following format:

    pub causetq_path: Option<String>,

    pub timelike_path: Option<String>,

    pub spacelike_store_on_fdb: Option<String>,

    #[structopt(conflicts_with = "hex-to-escaped", long = "to-hex")]
    /// Convert an escaped soliton_id to hex soliton_id
    pub escaped_to_hex: Option<String>,

    #[structopt(
    conflicts_with_all = &["hex-to-escaped", "escaped-to-hex"],
    long,
    )]
    /// Decode a soliton_id in escaped format
    pub decode: Option<String>,

    #[structopt(
    conflicts_with_all = &["hex-to-escaped", "escaped-to-hex"],
    long,
    )]
    /// Encode a soliton_id in escaped format
    pub encode: Option<String>,

    #[structopt(subcommand)]
    pub cmd: Option<Cmd>,
}


#[derive(StructOpt, Debug)]
#[structopt(name = "einstein_db_ctl")]
struct CmdOpt {
    #[structopt(long)]
    /// Set the address of fidel
    pub fidel: Option<String>,

    #[structopt(long, default_causet_locale = "warn")]
    /// Set the log level
    pub log_level: String,

    #[structopt(long)]
    /// Set the remote host
    pub host: Option<String>,

    #[structopt(long)]
    /// Set the CA certificate path
    pub ca_path: Option<String>,

    #[structopt(long)]
    /// Set the certificate path
    pub cert_path: Option<String>,

    #[structopt(long)]
    /// Set the private soliton_id path
    pub soliton_id_path: Option<String>,

    #[structopt(long)]
    /// Set the remote port
    /// Default: 22
    pub port: Option<u64>,

    #[structopt(long)]
    /// EinsteinDB data-dir, check <deploy-dir>/scripts/run.sh to get it
    pub data_dir: Option<String>,

    #[structopt(long)]
    /// Skip paranoid checks when open foundationdb
    pub skip_paranoid_checks: bool,

    #[allow(dead_code)]
    #[structopt(
    long,
    validator = |_| Err("DEPRECATED!!! Use --data-dir and --config instead".to_owned()),
    )]
    /// Set the foundationdb path
    pub einstein_db: Option<String>,

    #[allow(dead_code)]
    #[structopt(
    long,
    validator = |_| Err("DEPRECATED!!! Use --data-dir and --config instead".to_owned()),
    )]
    /// Set the violetabft foundationdb path
    pub violetabft_tangaroa: Option<String>,

    #[structopt(conflicts_with = "escaped-to-hex", long = "to-escaped")]
    /// Convert a hex soliton_id to escaped soliton_id_path = "escaped-to-hex")]
    /// Convert an escaped soliton_id to hex soliton_id_path = "to-hex")]
    /// Decode a soliton_id in escaped format!("{}_{}")
    ///
pub escaped_to_hex: Option<String>,

    #[structopt(
    conflicts_with_all = &["hex-to-escaped", "escaped-to-hex"],
    long,
    )]
    /// Encode a soliton_id in escaped format
    pub encode: Option<String>,

    #[structopt(subcommand)]
    pub cmd: Option<Cmd>,
}

#[derive(StructOpt)]
pub enum Cmd {
    /// Print a violetabft log entry
    VioletaBFTPaxos {
        #[structopt(subcommand)]
        cmd: VioletaBFTPaxosCmd,
    },
    /// Print region size
    Size {
        #[structopt(short = "r")]
        /// Set the region id, if not specified, print all regions
        region: Option<u64>,

        #[structopt(
        short = "c",
        use_delimiter = true,
        require_delimiter = true,
        causet_locale_delimiter = ",",
        default_causet_locale = "default,write,lock"
        )]
        /// Set the append_log name, if not specified, print all append_log
        append_log: Vec<String>,

        #[structopt(short = "s")]
        /// Set the soliton_id, if not specified, print all soliton_id
        /// The escaped soliton_id is a hex string of a soliton_id with the following format:
        /// 0x<soliton_id>
        /// The escaped soliton_id is a hex string of a soliton_id with the following format:
        /// 0x<soliton_id>
        soliton_id: Option<String>,

        #[structopt(short = "d")]
        /// Set the data_dir, if not specified, print all data_dir
        /// The data_dir is a path of a data_dir with the following format:
        /// <data_dir>/<region_id>/<append_log_name>/<soliton_id>
        data_dir: Option<String>,

        #[structopt(short = "c")]
        /// Set the config, if not specified, print all config
        /// The config is a path of a config with the following format:
        /// <config>/<region_id>/<append_log_name>/<soliton_id>
        ///
        config: Option<String>,

        #[structopt(short = "l")]
        /// Set the log_level, if not specified, print all log_level
        /// The log_level is a path of a log_level with the following format:
        /// <log_level>/<region_id>/<append_log_name>/<soliton_id>
        /// The log_level is a path of a log_level with the following format:
        /// <log_level>/<region_id>/<append_log_name>/<soliton_id>
        /// The log_level is a path of a log_level with the following format:
        ///
        log_level: Option<String>,

    }
}


#[derive(StructOpt)]
pub enum VioletaBFTPaxosScanCmd {

    /// Scan the log of a region
    ///

    #[structopt(name = "scan")]
    Scan {
        #[structopt(short = "r")]
        /// Set the region id, if not specified, print all regions
        region: Option<u64>,

        #[structopt(
        short = "c",
        use_delimiter = true,
        require_delimiter = true,
        causet_locale_delimiter = ",",
        default_causet_locale = "default,write,lock"
        )]
        /// Set the append_log name, if not specified, print all append_log
        append_log: Vec<String>,

        #[structopt(short = "s")]
        /// Set the soliton_id, if not specified, print all soliton_id
        /// The escaped soliton_id is a hex string of a soliton_id with the following format:
        /// 0x<soliton_id>
        /// The escaped soliton_id is a hex string of a soliton_id with the following format:
        /// 0x<soliton_id>
        soliton_id: Option<String>,

        #[structopt(short = "d")]
        /// Set the data_dir, if not specified, print all data_dir
        /// The data_dir is a path of a data_dir with the following format:
        /// <data_dir>/<region_id>/<append_log_name>/<soliton_id>
        data_dir: Option<String>,

        #[structopt(short = "c")]
        /// Set the config, if not specified, print all config
        /// The config is a path of a config with the following format:
        /// <config>/<region_id>/<append_log_name>/<soliton_id>
        ///
        config: Option<String>,

        #[structopt(short = "l")]
        /// Set the log_level, if not specified, print all log_level
        /// The log_level is a path of a log_level with the following format:
        /// <log_level>/<region_id>/<append_log_name>/<soliton_id>
    /// Scan the log
    /// Scan the log
    #[structopt(subcommand)]
    cmd: Option<VioletaBFTPaxosScanCmd>,
    }
}


#[derive(StructOpt)]
#[structopt(name = "violetabft-paxos")]
pub enum VioletaBFTPaxosCmdLine {
    /// Print a violetabft log entry
    /// Print the range db range
    Scan {
        #[structopt(
        short = "f",
        long,
        help = RAW_KEY_HINT,
        )]
        from: String,

        #[structopt(
        short = "t",
        long,
        help = RAW_KEY_HINT,
        )]
        to: Option<String>,

        #[structopt(long)]
        /// Set the scan limit
        limit: Option<u64>,

        #[structopt(long)]
        /// Set the scan start_ts as filter
        start_ts: Option<u64>,

        #[structopt(long)]
        /// Set the scan commit_ts as filter
        commit_ts: Option<u64>,

        #[structopt(
        long,
        use_delimiter = true,
        require_delimiter = true,
        causet_locale_delimiter = ",",
        default_causet_locale = "default,write,lock"
        )]
        /// Set the append_log name, if not specified, print all append_log
        /// If specified, only print the append_log specified
        /// If specified multiple times, print the append_log specified multiple times
        append_log: Vec<String>,
    },
    /// Print the range db range
    Range {
        #[structopt(
        short = "f",
        long,
        help = RAW_KEY_HINT,
        )]
        from: String,

        /// Column family names, combined from default/lock/write
        show_append_log: Vec<String>,
    },

    /// Print the range db range
    RangeWith {
        #[structopt(
        short = "f",
        long,
        help = RAW_KEY_HINT,
        )]
        from: String,

        #[structopt(
        short = "t",
        long,
        help = RAW_KEY_HINT,
        )]
        to: Option<String>,

        #[structopt(long)]
        /// Set the scan limit
        limit: Option<u64>,

        #[structopt(long)]
        /// Set the scan start_ts as filter
        start_ts: Option<u64>,

        #[structopt(long)]
        /// Set the scan commit_ts as filter
        commit_ts: Option<u64>,

        #[structopt(
        long,
        use_delimiter = true,
        require_delimiter = true,
        causet_locale_delimiter = ",",
        default_causet_locale = "default,write,lock"
        )]
        /// Set the append_log name, if not specified, print all append_log
        /// If specified, only print the append_log specified
        /// If specified multiple times, print the append_log specified multiple times
        append_log: Vec<String>,
    },
    /// Print the range db range
    RangeWith2 {
        #[structopt(
        short = "f",
        long,
        help = RAW_KEY_HINT,
        )]
        from: String,

        #[structopt(long)]
        /// Set the scan limit
        /// If specified, only print the append_log specified
        /// If specified multiple times, print the append_log specified multiple times
        /// If specified, only print the append_log specified
        limit: Option<u64>,

        #[structopt(long)]
        /// Set the scan start_ts as filter
        /// If specified, only print the append_log specified
        /// If specified multiple times, print the append_log specified multiple times
        start_ts: Option<u64>,
    }
}

/// Starting prompt
const DEFAULT_PROMPT: &'static str = "EinsteinDB=> ";
/// Prompt when further input is being read
// TODO: Should this actually reflect the current open brace?
const MORE_PROMPT: &'static str = "EinsteinDB.> ";
//reflect the current open brace
const MORE_PROMPT_2: &'static str = "EinsteinDB.> ";


#[derive(StructOpt)]
pub enum VioletaBFTPaxosCmd {
    /// Print a violetabft log entry
    Print {
        #[structopt(long)]
        /// Set the log id
        id: u64,
    },
    /// Print a violetabft log entry
    PrintAll,
}


#[derive(StructOpt)]
pub enum ScanCmd {
    /// Print a violetabft log entry
    Print {
        #[structopt(long)]
        /// Set the log id
        id: u64,
    },
    /// Print a violetabft log entry
    PrintAll,
}


/// Possible results from reading input from `InputReader`
#[derive(Clone, Debug)]
pub enum InputResult {
    /// mentat command as input; (name, rest of line)
    MetaCommand(Command),
    /// An empty line
    Empty,
    /// Needs more input
    More,
    /// End of file reached
    Eof,
}

/// Reads input from `stdin`
pub struct InputReader {
    buffer: String,
    interface: Option<Interface<DefaultTerminal>>,
    in_process_cmd: Option<Command>,
}

enum UserAction {
    // We've received some text that we should interpret as a new command, or
    // as part of the current command.
    TextInput(String),
    // We were interrupted, if we have a current command we should clear it,
    // otherwise we should exit. Currently can only be generated by reading from
    // a terminal (and not by reading from stdin).
    Interrupt,
    // We hit the end of the file, there was an error getting user input, or
    // something else happened that means we should exit.
    Quit,

    // We've received a signal that we should exit.
    Signal(Signal),
    

}


impl InputReader {
    /// Constructs a new `InputReader` reading from `stdin`.
    pub fn new(interface: Option<Interface<DefaultTerminal>>) -> InputReader {
        if let Some(ref interface) = interface {
            // It's fine to fail to load history.
            let p = ::history_file_path();
            let loaded = interface.load_history(&p);
            debug!("history read from {}: {}", p.display(), loaded.is_ok());

            let mut r = interface.lock_reader();
            // Handle SIGINT (Ctrl-C)
            r.set_report_signal(Signal::Interrupt, true);
            r.set_word_break_chars(" \t\n!\"#$%&'(){}*+,-./:;<=>?@[\\]^`");
        }

        InputReader {
            buffer: String::new(),
            interface,
            in_process_cmd: None,
        }
    }


    /// Returns whether the `InputReader` is reading from a TTY.
    pub fn is_tty(&self) -> bool {
        self.interface.is_some()
    }

    /// Reads a single command, item, or statement from `stdin`.
    /// Returns `More` if further input is required for a complete result.
    /// In this case, the input received so far is buffered internally.
    pub fn read_input(&mut self) -> Result<InputResult, Error> {
        let prompt = if self.in_process_cmd.is_some() { MORE_PROMPT } else { DEFAULT_PROMPT };
        let mut r = self.interface.as_mut().unwrap().lock_reader();
        let mut line = String::new();

        let action = match r.read_line(&prompt) {
            Ok(line) => {
                if line.is_empty() {
                    UserAction::TextInput(line)
                } else {
                    UserAction::TextInput(line)
                }
            }
            Err(ReadlineError::Interrupted) => UserAction::Interrupt,
            Err(ReadlineError::Eof) => UserAction::Quit,
            Err(e) => {
                error!("{}", e);
                UserAction::Quit
            }
        };

        match action {
            UserAction::TextInput(line) => {
                self.buffer.push_str(&line);
                if self.in_process_cmd.is_some() {
                    self.in_process_cmd = Some(self.in_process_cmd.unwrap().clone());
                    Ok(InputResult::More)
                } else {
                    self.in_process_cmd = Some(Command::parse(&self.buffer)?);
                    self.buffer.clear();
                    Ok(InputResult::MetaCommand(self.in_process_cmd.unwrap()))
                }
            }
            UserAction::Interrupt => {
                if self.in_process_cmd.is_some() {
                    self.in_process_cmd = None;
                    self.buffer.clear();
                    Ok(InputResult::More)
                } else {
                    Ok(InputResult::Quit)
                }
            }
            UserAction::Quit => Ok(InputResult::Quit),
            UserAction::Signal(sig) => {
                error!("Got signal {}", sig);
                Ok(InputResult::Quit)
            }
        }
    }


    /// Reads a single command, item, or statement from `stdin`.
    /// Returns `More` if further input is required for a complete result.
    /// In this case, the input received so far is buffered internally.
    /// This function is used for the range command





    /// Returns the current command being processed, if any.
    /// This is useful for displaying the prompt.
    /// If there is no command being processed, returns `None`.
    /// If there is a command being processed, returns the command.
    /// If there is a command being processed, but it is not complete, returns `None`.
    ///

    pub fn current_command(&self) -> Option<Command> {
        self.in_process_cmd.clone()
    }
}


/// The main entry point for the application.
/// This function is called when the application is run from the command line.
/// It parses the command line arguments and then runs the application.
/// It returns a `Result` to allow for easy error handling.
/// # Examples
/// ```
/// use violetabft::{run, Command};
/// use std::io::{self, Write};
/// use std::process::exit;
/// use structopt::StructOpt;
/// #[derive(StructOpt)]
/// struct Opt {
///    #[structopt(subcommand)]
///   cmd: Command,
/// }
/// fn main() -> Result<(), io::Error> {
///    let opt = Opt::from_args();
///   match opt.cmd {
///      Command::Scan => {
///       let mut reader = InputReader::new(None);
///      loop {
///        match reader.read_input() {
///         Ok(InputResult::MetaCommand(cmd)) => {
///          println!("{:?}", cmd);
///        }
///        Ok(InputResult::More) => {
///         println!("More");
///       }
///      Ok(InputResult::Quit) => {
/// println!("Quit");
/// break;



#[derive(StructOpt)]

struct OptManifold {
    #[structopt(subcommand)]
    cmd: Command,
}


fn main() -> Result<(), io::Error> {
    let prompt = format!("{blue}{prompt}{reset}",
                             blue = color::Fg(::BLUE),
                             prompt = prompt,
                             reset = color::Fg(color::Reset));
    let mut reader = InputReader::new(Some(Interface::new(prompt)));
    loop {
        let line = reader.read_input()?;
        match line {
            UserAction::TextInput(s) => s,
            UserAction::Interrupt => {
                println!("Interrupted");
                continue;
            }
            UserAction::Quit => {
                // Move to the next line, so that our next prompt isn't on top
                // of the previous.
                println!();
                String::new()
            },
            _ => return Ok(Eof),
        };

        if !line.is_empty() {
            println!("{}", line);
        }

        if line == "quit" {
            break;
        }
    }
    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{self, Write};
    use std::process::exit;
    use structopt::StructOpt;
    #[derive(StructOpt)]
    struct Opt {
        #[structopt(subcommand)]
        cmd: Command,

    }

    #[test]
    fn test_main() {

        if let Some(ref interface) = interface {
            // It's fine to fail to load history.
            let p = ::history_file_path();
            let loaded = interface.load_history(&p);
            debug!("history read from {}: {}", p.display(), loaded.is_ok());

            let mut r = interface.lock_reader();
            // Handle SIGINT (Ctrl-C)
            r.set_report_signal(Signal::Interrupt, true);
            r.set_word_break_chars(" \t\n!\"#$%&'(){}*+,-./:;<=>?@[\\]^`");
            r.set_completer(Some(completer));
            r.set_key_reader(Some(key_reader));
            r.set_prompt_func(Some(prompt_func));
            r.set_startup_hook(Some(startup_hook));
            r.set_pre_input_hook(Some(pre_input_hook));
        }
        let opt = Opt::from_args();
        match opt.cmd {
            Command::Scan => {
                let mut reader = InputReader::new(None);
                loop {
                    match reader.read_input() {
                        Ok(InputResult::MetaCommand(cmd)) => {
                            println!("{:?}", cmd);
                        }
                        Ok(InputResult::More) => {
                            println!("More");
                        }
                        Ok(InputResult::Quit) => {
                            println!("Quit");
                            break;
                        }
                    }
                }
            }
            Command::Range => {
                let mut reader = InputReader::new(None);
                loop {
                    match reader.read_input() {
                        Ok(InputResult::MetaCommand(cmd)) => {
                            println!("{:?}", cmd);
                        }
                        Ok(InputResult::More) => {
                            println!("More");
                        }
                        Ok(InputResult::Quit) => {
                            println!("Quit");
                            break;
                        }
                    }
                }
            }
        }
    }


    fn completer(line: &str, _pos: usize) -> Vec<String> {
        vec![line.to_string()]

    }


    fn key_reader(line: &str, _pos: usize) -> Option<Key> {
        None

    }

    fn prompt_func(reader: &mut InputReader) -> io::Result<String> {
        Ok(String::new())
    }

    fn startup_hook(reader: &mut InputReader) -> io::Result<()> {
        Ok(())
    }


    fn pre_input_hook(reader: &mut InputReader) -> io::Result<()> {
        Ok(())
    }




       // if we have a command in process (i.e. an incomplete query or transaction),
        // then we already know which type of command it is and so we don't need to parse the
        // command again, only the content, which we do later.
        // Therefore, we add the newly read in line to the existing command args.
        // If there is no in process command, we parse the read in line as a new command.
        // If the command is complete, we process it and then start a new command.
        // If the command is not complete, we just add the read in line to the existing command.


    
fn read_stdin() -> io::Result<String> {
    let mut stdin = io::stdin();
    let mut line = String::new();
    stdin.read_line(&mut line)?;
    Ok(line)
}


    #[test]
    fn test_read_stdin() {
        let line = read_stdin().unwrap();
        assert_eq!(line, "quit\n");
    }

    #[test]
    fn test_read_stdin_eof() {
        let line = read_stdin().unwrap();
        assert_eq!(line, "quit\n");


        let mut s = String::new();

        match stdin().read_line(&mut s) {
            Ok(0) | Err(_) => UserAction::Quit,
            Ok(_) => {
                if s.ends_with("\n") {
                    let len = s.len() - 1;
                    s.truncate(len);
                }
                UserAction::TextInput(s)
            }
        }
    }

}

fn add_history(line: &str) {
    let p = ::history_file_path();
    let mut f = File::create(&p).unwrap();
    f.write_all(line.as_bytes()).unwrap();
}

pub fn save_history() {
    let p = ::history_file_path();
    let mut f = File::create(&p).unwrap();
    f.write_all("".as_bytes()).unwrap();
}


pub fn load_history() {
    let p = ::history_file_path();
    let mut f = File::open(&p).unwrap();
    let mut s = String::new();
    f.read_to_string(&mut s).unwrap();
}


pub fn clear_history() {
    let p = ::history_file_path();
    let _ = std::fs::remove_file(&p);
}


pub fn history_file_path() -> PathBuf {
    let mut p = ::env::current_dir().unwrap();
    p.push(".history");
    p
}


pub fn history_file_exists() -> bool {
    let p = ::history_file_path();
    p.exists()
}


/// The history file is a text file that contains the history of commands.
