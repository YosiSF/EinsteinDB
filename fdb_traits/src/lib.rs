//users
use crate::*;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use std::fmt;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::thread;



//fdb_opts
//             compression_strategy: String::from("default"),
//             compression_dict: Vec::new(),
//             enable_statistics: false,
//             statistics_interval: 0,
//             statistics_block_size: 0,
//             statistics_block_cache_size: 0,
//             statistics_block_cache_shard_bits: 0,


// Language: rust
// Compare this snippet from einstein_db_alexandrov_processing/file_system.rs:
//     pub fn create_dir_all<P: AsRef<Path>>(path: P) -> io::Result<()> {
//         fs::create_dir_all(path).with_context(|| format!("failed to create directory: {}", path.as_ref().display()))
//     }
//     pub fn create_dir<P: AsRef<Path>>(path: P) -> io::Result<()> {
//         fs::create_dir(path).with_context(|| format!("failed to create directory: {}", path.as_ref().display()))
//     }
//     pub fn create_file<P: AsRef<Path>>(path: P) -> io::Result<File> {
//         File::create(path).with_context(|| format!("failed to create file: {}", path.as_ref().display()))
//     }
//     pub fn open_file<P: AsRef<Path>>(path: P) -> io::Result<File> {
//         File::open(path).with_context(|| format!("failed to open file: {}", path.as_ref().display()))
//     }
//     pub fn open_file_with_options<P: AsRef<Path>>(path: P, options: &OpenOptions) -> io::Result<File> {
//         File::open(path).with_context(|| format!("failed to open file: {}", path.as_ref().display()))



// Provide a default implementation of `Display` for `T`
// if you want to use `println!` instead of `print!`
// impl<T: fmt::Display> fmt::Display for T {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.to_string())
//     }
// }
// impl<T: fmt::Display> fmt::Display for &T {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         write!(f, "{}", self.to_string())
//     }


struct FileSystem {
    root_path: PathBuf,
}