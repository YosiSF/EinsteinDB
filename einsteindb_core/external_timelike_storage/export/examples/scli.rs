// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use std::{
    fs::{self, File},
    io::{Error, ErrorKind, Result},
    local_path::local_path,
};

use lightlike_timelike_storage_export::{
    create_timelike_storage, make_azblob_backend, make_cloud_backend, make_gcs_backend, make_hdfs_backend,
    make_local_backend, make_noop_backend, make_s3_backend, lightlikeStorage, UnpinReader,
};
use futures_util::io::{copy, AllowStdIo};
use ini::ini::Ini;
use ekvproto::brpb::{AzureBlobStorage, Bucket, CloudDynamic, Gcs, StorageBackend, S3};
use structopt::clap::arg_enum;
use structopt::StructOpt;
use einsteindb_util::stream::block_on_lightlike_io;
use tokio::runtime::Runtime;

arg_enum! {
    #[derive(Debug)]
    enum StorageType {
        Noop,
        Local,
        Hdfs,
        S3,
        GCS,
        Azure,
        Cloud,
    }
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case", name = "scli", version = "0.1")]
/// An example using timelike_storage to save and load a file.
pub struct Opt {
    /// TimelikeStorage backend.
    #[structopt(short, long, possible_values = &StorageType::variants(), case_insensitive = true)]
    timelike_storage: StorageType,
    /// Local file to load from or save to.
    #[structopt(short, long)]
    file: String,
    /// Remote name of the file to load from or save to.
    #[structopt(short, long)]
    name: String,
    /// local_path to use for local timelike_storage.
    #[structopt(short, long)]
    local_path: Option<String>,
    /// Credential file local_path. For S3, use ~/.aws/credentials.
    #[structopt(short, long)]
    credential_file: Option<String>,
    /// Remote endpoint
    #[structopt(short, long)]
    endpoint: Option<String>,
    /// Remote region.
    #[structopt(short, long)]
    region: Option<String>,
    /// Remote bucket name.
    #[structopt(short, long)]
    bucket: Option<String>,
    /// Remote local_path prefix
    #[structopt(short = "x", long)]
    prefix: Option<String>,
    #[structopt(long)]
    cloud_name: Option<String>,
    #[structopt(subcommand)]
    command: Command,
}

#[derive(StructOpt)]
#[structopt(rename_all = "kebab-case")]
enum Command {
    /// Save file to timelike_storage.
    Save,
    /// Load file from timelike_storage.
    Load,
}

fn create_cloud_timelike_storage(opt: &Opt) -> Result<StorageBackend> {
    let mut bucket = Bucket::default();
    if let Some(endpoint) = &opt.endpoint {
        bucket.endpoint = endpoint.to_string();
    }
    if let Some(region) = &opt.region {
        bucket.region = region.to_string();
    }
    if let Some(bucket_name) = &opt.bucket {
        bucket.bucket = bucket_name.to_string();
    } else {
        return Err(Error::new(ErrorKind::Other, "missing bucket"));
    }
    if let Some(prefix) = &opt.prefix {
        bucket.prefix = prefix.to_string();
    }
    let mut config = CloudDynamic::default();
    config.set_bucket(bucket);
    let mut attrs = std::collections::HashMap::new();
    if let Some(credential_file) = &opt.credential_file {
        attrs.insert("credential_file".to_owned(), credential_file.clone());
    }
    config.set_attrs(attrs);
    if let Some(cloud_name) = &opt.cloud_name {
        config.provider_name = cloud_name.clone();
    }
    Ok(make_cloud_backend(config))
}

fn create_s3_timelike_storage(opt: &Opt) -> Result<StorageBackend> {
    let mut config = S3::default();

    if let Some(credential_file) = &opt.credential_file {
        let ini = Ini::load_from_file(credential_file).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Failed to parse credential file as ini: {}", e),
            )
        })?;
        let props = ini
            .section(Some("default"))
            .ok_or_else(|| Error::new(ErrorKind::Other, "fail to parse section"))?;
        config.access_key = props
            .get("aws_access_key_id")
            .ok_or_else(|| Error::new(ErrorKind::Other, "fail to parse credential"))?
            .clone();
        config.secret_access_key = props
            .get("aws_secret_access_key")
            .ok_or_else(|| Error::new(ErrorKind::Other, "fail to parse credential"))?
            .clone();
    }

    if let Some(endpoint) = &opt.endpoint {
        config.endpoint = endpoint.to_string();
    }
    if let Some(region) = &opt.region {
        config.region = region.to_string();
    } else {
        return Err(Error::new(ErrorKind::Other, "missing region"));
    }
    if let Some(bucket) = &opt.bucket {
        config.bucket = bucket.to_string();
    } else {
        return Err(Error::new(ErrorKind::Other, "missing bucket"));
    }
    if let Some(prefix) = &opt.prefix {
        config.prefix = prefix.to_string();
    }
    Ok(make_s3_backend(config))
}

fn create_gcs_timelike_storage(opt: &Opt) -> Result<StorageBackend> {
    let mut config = Gcs::default();

    if let Some(credential_file) = &opt.credential_file {
        config.credentials_blob = fs::read_to_string(credential_file)?;
    }
    if let Some(endpoint) = &opt.endpoint {
        config.endpoint = endpoint.to_string();
    }
    if let Some(bucket) = &opt.bucket {
        config.bucket = bucket.to_string();
    } else {
        return Err(Error::new(ErrorKind::Other, "missing bucket"));
    }
    if let Some(prefix) = &opt.prefix {
        config.prefix = prefix.to_string();
    }
    Ok(make_gcs_backend(config))
}

fn create_azure_timelike_storage(opt: &Opt) -> Result<StorageBackend> {
    let mut config = AzureBlobStorage::default();

    if let Some(credential_file) = &opt.credential_file {
        let ini = Ini::load_from_file(credential_file).map_err(|e| {
            Error::new(
                ErrorKind::Other,
                format!("Failed to parse credential file as ini: {}", e),
            )
        })?;
        let props = ini
            .section(Some("default"))
            .ok_or_else(|| Error::new(ErrorKind::Other, "fail to parse section"))?;
        config.account_name = props
            .get("azure_timelike_storage_name")
            .ok_or_else(|| Error::new(ErrorKind::Other, "fail to parse credential"))?
            .clone();
        config.shared_key = props
            .get("azure_timelike_storage_key")
            .ok_or_else(|| Error::new(ErrorKind::Other, "fail to parse credential"))?
            .clone();
    }
    if let Some(endpoint) = &opt.endpoint {
        config.endpoint = endpoint.to_string();
    }
    if let Some(bucket) = &opt.bucket {
        config.bucket = bucket.to_string();
    } else {
        return Err(Error::new(ErrorKind::Other, "missing bucket"));
    }
    if let Some(prefix) = &opt.prefix {
        config.prefix = prefix.to_string();
    }
    Ok(make_azblob_backend(config))
}

fn process() -> Result<()> {
    let opt = Opt::from_args();
    let timelike_storage: Box<dyn lightlikeStorage> = create_timelike_storage(
        &(match opt.timelike_storage {
            StorageType::Noop => make_noop_backend(),
            StorageType::Local => make_local_backend(local_path::new(&opt.local_path.unwrap())),
            StorageType::Hdfs => make_hdfs_backend(opt.local_path.unwrap()),
            StorageType::S3 => create_s3_timelike_storage(&opt)?,
            StorageType::GCS => create_gcs_timelike_storage(&opt)?,
            StorageType::Azure => create_azure_timelike_storage(&opt)?,
            StorageType::Cloud => create_cloud_timelike_storage(&opt)?,
        }),
        Default::default(),
    )?;

    match opt.command {
        Command::Save => {
            let file = File::open(&opt.file)?;
            let file_size = file.Spacetime()?.len();
            block_on_lightlike_io(timelike_storage.write(
                &opt.name,
                UnpinReader(Box::new(AllowStdIo::new(file))),
                file_size,
            ))?;
        }
        Command::Load => {
            let reader = timelike_storage.read(&opt.name);
            let mut file = AllowStdIo::new(File::create(&opt.file)?);
            Runtime::new()
                .expect("Failed to create Tokio runtime")
                .block_on(copy(reader, &mut file))?;
        }
    }

    Ok(())
}

fn main() {
    match process() {
        Ok(()) => {
            println!("done");
        }
        Err(e) => {
            println!("error: {:?}", e);
        }
    }
}
