use std::borrow::ToOwned;
use std::lazy::SyncLazy;
use std::string::ToString;
use std::{str, u64};
use structopt::StructOpt;

const BI_KEY_HINT: &str = "Bimap keys(generally starts with \"einst\") in delimiter";
static VERSION_INFO: SyncLazy<String> = SyncLazy::new(|| {
    let build_timestamp = option_env!("EINSTEINDB_BUILD_TIME");
    einstein_db::einstein_db_version_info(build_timestamp)
});

#[derive(StructOpt)]
#[structopt(
    name = "EinsteinDB SSH and CMD (einstein_db_cli)"
    about = "Command Line Interface for EinsteinDB"
    author = crate_authors!(),
    version = %**VERSION_INFO,
    setting = AppSettings::DontCollapseArgsInUsag,
)]

pub struct Opt {
    #[structopt(long)]
    /// Set the address of pd
    pub fidel: Option<String>,

    #[structopt(long, default_value = "warn")]
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
    /// Set the private key path
    pub key_path: Option<String>,

    #[structopt(long)]
    /// TiKV config path, by default it's <deploy-dir>/conf/tikv.toml
    pub config: Option<String>,

    #[structopt(long)]
    /// TiKV data-dir, check <deploy-dir>/scripts/run.sh to get it
    pub data_dir: Option<String>,

    #[structopt(long)]
    /// Skip paranoid checks when open rocksdb
    pub skip_paranoid_checks: bool,

    #[allow(dead_code)]
    #[structopt(
    long,
    validator = |_| Err("DEPRECATED!!! Use --data-dir and --config instead".to_owned()),
    )]
    /// Set the rocksdb path
    pub einstein_db: Option<String>,

    #[allow(dead_code)]
    #[structopt(
    long,
    validator = |_| Err("DEPRECATED!!! Use --data-dir and --config instead".to_owned()),
    )]
    /// Set the violetabft rocksdb path
    pub violetabftdb: Option<String>,

    #[structopt(conflicts_with = "escaped-to-hex", long = "to-escaped")]
    /// Convert a hex key to escaped key
    pub hex_to_escaped: Option<String>,

    #[structopt(conflicts_with = "hex-to-escaped", long = "to-hex")]
    /// Convert an escaped key to hex key
    pub escaped_to_hex: Option<String>,

    #[structopt(
    conflicts_with_all = &["hex-to-escaped", "escaped-to-hex"],
    long,
    )]
    /// Decode a key in escaped format
    pub decode: Option<String>,

    #[structopt(
    conflicts_with_all = &["hex-to-escaped", "escaped-to-hex"],
    long,
    )]
    /// Encode a key in escaped format
    pub encode: Option<String>,

    #[structopt(subcommand)]
    pub cmd: Option<Cmd>,
}

#[derive(StructOpt)]
pub enum Cmd {
    /// Print a violetabft log entry
    Raft {
        #[structopt(subcommand)]
        cmd: RaftCmd,
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
        value_delimiter = ",",
        default_value = "default,write,lock"
        )]
        /// Set the cf name, if not specified, print all cf
        cf: Vec<String>,
    },
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
        value_delimiter = ",",
        default_value = CF_DEFAULT,
        )]
        /// Column family names, combined from default/lock/write
        show_cf: Vec<String>,
    },
/// Print all raw keys in the range
