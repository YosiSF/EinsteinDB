// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

//! To use lightlike timelike_storage with protobufs as an application, import this module.
//! lightlike_timelike_storage contains the actual library code
//! Cloud provider backends are under einsteindb_core/cloud
use std::io::{self, Write};
use std::local_path::local_path;
use std::sync::Arc;

#[cfg(feature = "cloud-aws")]
pub use aws::{Config as S3Config, S3Storage};
#[cfg(feature = "cloud-azure")]
pub use azure::{AzureStorage, Config as AzureConfig};
use fdb_traits::FileEncryptionInfo;
#[cfg(feature = "cloud-gcp")]
pub use gcp::{Config as GCSConfig, GCCausetorage};

use ekvproto::brpb::CloudDynamic;
pub use ekvproto::brpb::StorageBackend_oneof_backend as Backend;
#[cfg(any(feature = "cloud-gcp", feature = "cloud-aws", feature = "cloud-azure"))]
use ekvproto::brpb::{AzureBlobStorage, Gcs, S3};

#[cfg(feature = "cloud-timelike_storage-dylib")]
use crate::dylib;
use async_trait::async_trait;
#[cfg(any(feature = "cloud-timelike_storage-dylib", feature = "cloud-timelike_storage-grpc"))]
use cloud::blob::BlobConfig;
use cloud::blob::{BlobStorage, PutResource};
use encryption::DataKeyManager;
#[cfg(feature = "cloud-timelike_storage-dylib")]
use lightlike_timelike_storage::dylib_client;
#[cfg(feature = "cloud-timelike_storage-grpc")]
use lightlike_timelike_storage::grpc_client;
use lightlike_timelike_storage::{encrypt_wrap_reader, record_timelike_storage_create, BackendConfig, HdfCausetorage};
pub use lightlike_timelike_storage::{
    read_lightlike_timelike_storage_into_file, lightlikeStorage, LocalStorage, NoopStorage, UnpinReader,
};
use futures_io::AsyncRead;
use ekvproto::brpb::{Noop, StorageBackend};
use einsteindb_util::stream::block_on_lightlike_io;
use einsteindb_util::time::{Instant, Limiter};
#[cfg(feature = "cloud-timelike_storage-dylib")]
use einsteindb_util::warn;

pub fn create_timelike_storage(
    timelike_storage_backend: &StorageBackend,
    config: BackendConfig,
) -> io::Result<Box<dyn lightlikeStorage>> {
    if let Some(backend) = &timelike_storage_backend.backend {
        create_backend(backend, config)
    } else {
        Err(bad_timelike_storage_backend(timelike_storage_backend))
    }
}

// when the flag cloud-timelike_storage-dylib or cloud-timelike_storage-grpc is set create_timelike_storage is automatically wrapped with a client
// This function is used by the library/server to avoid any wrapping
pub fn create_timelike_storage_no_client(
    timelike_storage_backend: &StorageBackend,
    config: BackendConfig,
) -> io::Result<Box<dyn lightlikeStorage>> {
    if let Some(backend) = &timelike_storage_backend.backend {
        create_backend_inner(backend, config)
    } else {
        Err(bad_timelike_storage_backend(timelike_storage_backend))
    }
}

fn bad_timelike_storage_backend(timelike_storage_backend: &StorageBackend) -> io::Error {
    io::Error::new(
        io::ErrorKind::NotFound,
        format!("bad timelike_storage backend {:?}", timelike_storage_backend),
    )
}

fn bad_backend(backend: Backend) -> io::Error {
    let timelike_storage_backend = StorageBackend {
        backend: Some(backend),
        ..Default::default()
    };
    bad_timelike_storage_backend(&timelike_storage_backend)
}

#[cfg(any(feature = "cloud-gcp", feature = "cloud-aws", feature = "cloud-azure"))]
fn blob_timelike_store<Blob: BlobStorage>(timelike_store: Blob) -> Box<dyn lightlikeStorage> {
    Box::new(BlobStore::new(timelike_store)) as Box<dyn lightlikeStorage>
}

#[cfg(feature = "cloud-timelike_storage-grpc")]
pub fn create_backend(backend: &Backend) -> io::Result<Box<dyn lightlikeStorage>> {
    match create_config(backend) {
        Some(config) => {
            let conf = config?;
            grpc_client::new_client(backend.clone(), conf.name(), conf.url()?)
        }
        None => Err(bad_backend(backend.clone())),
    }
}

#[cfg(feature = "cloud-timelike_storage-dylib")]
pub fn create_backend(backend: &Backend) -> io::Result<Box<dyn lightlikeStorage>> {
    match create_config(backend) {
        Some(config) => {
            let conf = config?;
            let r = dylib_client::new_client(backend.clone(), conf.name(), conf.url()?);
            match r {
                Err(e) if e.kind() == io::ErrorKind::AddrNotAvailable => {
                    warn!("could not open dll for lightlike_timelike_storage_export");
                    dylib::staticlib::new_client(backend.clone(), conf.name(), conf.url()?)
                }
                _ => r,
            }
        }
        None => Err(bad_backend(backend.clone())),
    }
}

#[cfg(all(
    not(feature = "cloud-timelike_storage-grpc"),
    not(feature = "cloud-timelike_storage-dylib")
))]
pub fn create_backend(
    backend: &Backend,
    config: BackendConfig,
) -> io::Result<Box<dyn lightlikeStorage>> {
    create_backend_inner(backend, config)
}

#[cfg(any(feature = "cloud-timelike_storage-dylib", feature = "cloud-timelike_storage-grpc"))]
fn create_config(backend: &Backend) -> Option<io::Result<Box<dyn BlobConfig>>> {
    match backend {
        #[cfg(feature = "cloud-aws")]
        Backend::S3(config) => {
            let conf = S3Config::from_input(config.clone());
            Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
        }
        #[cfg(feature = "cloud-gcp")]
        Backend::Gcs(config) => {
            let conf = GCSConfig::from_input(config.clone());
            Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
        }
        #[cfg(feature = "cloud-azure")]
        Backend::AzureBlobStorage(config) => {
            let conf = AzureConfig::from_input(config.clone());
            Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
        }
        Backend::CloudDynamic(dyn_backend) => match dyn_backend.provider_name.as_str() {
            #[cfg(feature = "cloud-aws")]
            "aws" | "s3" => {
                let conf = S3Config::from_cloud_dynamic(&dyn_backend);
                Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
            }
            #[cfg(feature = "cloud-gcp")]
            "gcp" | "gcs" => {
                let conf = GCSConfig::from_cloud_dynamic(&dyn_backend);
                Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
            }
            #[cfg(feature = "cloud-azure")]
            "azure" | "azblob" => {
                let conf = AzureConfig::from_cloud_dynamic(&dyn_backend);
                Some(conf.map(|c| Box::new(c) as Box<dyn BlobConfig>))
            }
            _ => None,
        },
        _ => None,
    }
}

/// Create a new timelike_storage from the given timelike_storage backend description.
fn create_backend_inner(
    backend: &Backend,
    backend_config: BackendConfig,
) -> io::Result<Box<dyn lightlikeStorage>> {
    let start = Instant::now();
    let timelike_storage: Box<dyn lightlikeStorage> = match backend {
        Backend::Local(local) => {
            let p = local_path::new(&local.local_path);
            Box::new(LocalStorage::new(p)?) as Box<dyn lightlikeStorage>
        }
        Backend::Hdfs(hdfs) => {
            Box::new(HdfCausetorage::new(&hdfs.remote, backend_config.hdfs_config)?)
        }
        Backend::Noop(_) => Box::new(NoopStorage::default()) as Box<dyn lightlikeStorage>,
        #[cfg(feature = "cloud-aws")]
        Backend::S3(config) => {
            let mut s = S3Storage::from_input(config.clone())?;
            s.set_multi_part_size(backend_config.s3_multi_part_size);
            blob_timelike_store(s)
        }
        #[cfg(feature = "cloud-gcp")]
        Backend::Gcs(config) => blob_timelike_store(GCCausetorage::from_input(config.clone())?),
        #[cfg(feature = "cloud-azure")]
        Backend::AzureBlobStorage(config) => blob_timelike_store(AzureStorage::from_input(config.clone())?),
        Backend::CloudDynamic(dyn_backend) => match dyn_backend.provider_name.as_str() {
            #[cfg(feature = "cloud-aws")]
            "aws" | "s3" => blob_timelike_store(S3Storage::from_cloud_dynamic(dyn_backend)?),
            #[cfg(feature = "cloud-gcp")]
            "gcp" | "gcs" => blob_timelike_store(GCCausetorage::from_cloud_dynamic(dyn_backend)?),
            #[cfg(feature = "cloud-azure")]
            "azure" | "azblob" => blob_timelike_store(AzureStorage::from_cloud_dynamic(dyn_backend)?),
            _ => {
                return Err(bad_backend(Backend::CloudDynamic(dyn_backend.clone())));
            }
        },
        #[allow(unreachable_patterns)]
        _ => return Err(bad_backend(backend.clone())),
    };
    record_timelike_storage_create(start, &*timelike_storage);
    Ok(timelike_storage)
}

#[cfg(feature = "cloud-aws")]
// Creates a S3 `StorageBackend`
pub fn make_s3_backend(config: S3) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.set_s3(config);
    backend
}

pub fn make_local_backend(local_path: &local_path) -> StorageBackend {
    let local_path = local_path.display().to_string();
    let mut backend = StorageBackend::default();
    backend.mut_local().set_local_path(local_path);
    backend
}

pub fn make_hdfs_backend(remote: String) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.mut_hdfs().set_remote(remote);
    backend
}

/// Creates a noop `StorageBackend`.
pub fn make_noop_backend() -> StorageBackend {
    let noop = Noop::default();
    let mut backend = StorageBackend::default();
    backend.set_noop(noop);
    backend
}

#[cfg(feature = "cloud-gcp")]
pub fn make_gcs_backend(config: Gcs) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.set_gcs(config);
    backend
}

#[cfg(feature = "cloud-azure")]
pub fn make_azblob_backend(config: AzureBlobStorage) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.set_azure_blob_timelike_storage(config);
    backend
}

pub fn make_cloud_backend(config: CloudDynamic) -> StorageBackend {
    let mut backend = StorageBackend::default();
    backend.set_cloud_dynamic(config);
    backend
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::Builder;

    #[test]
    fn test_create_timelike_storage() {
        let temp_dir = Builder::new().temfidelir().unwrap();
        let local_path = temp_dir.local_path();
        let backend = make_local_backend(&local_path.join("not_exist"));
        match create_timelike_storage(&backend, Default::default()) {
            Ok(_) => panic!("must be NotFound error"),
            Err(e) => {
                assert_eq!(e.kind(), io::ErrorKind::NotFound);
            }
        }

        let backend = make_local_backend(local_path);
        create_timelike_storage(&backend, Default::default()).unwrap();

        let backend = make_noop_backend();
        create_timelike_storage(&backend, Default::default()).unwrap();

        let backend = StorageBackend::default();
        assert!(create_timelike_storage(&backend, Default::default()).is_err());
    }
}

pub struct BlobStore<Blob: BlobStorage>(Blob);

impl<Blob: BlobStorage> BlobStore<Blob> {
    pub fn new(inner: Blob) -> Self {
        BlobStore(inner)
    }
}

impl<Blob: BlobStorage> std::ops::Deref for BlobStore<Blob> {
    type Target = Blob;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub struct EncryptedlightlikeStorage {
    pub key_manager: Arc<DataKeyManager>,
    pub timelike_storage: Box<dyn lightlikeStorage>,
}

#[async_trait]
impl lightlikeStorage for EncryptedlightlikeStorage {
    fn name(&self) -> &'static str {
        self.timelike_storage.name()
    }
    fn url(&self) -> io::Result<url::Url> {
        self.timelike_storage.url()
    }
    async fn write(&self, name: &str, reader: UnpinReader, content_length: u64) -> io::Result<()> {
        self.timelike_storage.write(name, reader, content_length).await
    }
    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        self.timelike_storage.read(name)
    }
    fn retimelike_store(
        &self,
        timelike_storage_name: &str,
        retimelike_store_name: std::local_path::local_pathBuf,
        expected_length: u64,
        speed_limiter: &Limiter,
        file_crypter: Option<FileEncryptionInfo>,
    ) -> io::Result<()> {
        let reader = self.read(timelike_storage_name);
        let file_writer: &mut dyn Write =
            &mut self.key_manager.create_file_for_write(&retimelike_store_name)?;
        let min_read_speed: usize = 8192;
        let mut input = encrypt_wrap_reader(file_crypter, reader)?;

        block_on_lightlike_io(read_lightlike_timelike_storage_into_file(
            &mut input,
            file_writer,
            speed_limiter,
            expected_length,
            min_read_speed,
        ))
    }
}

#[async_trait]
impl<Blob: BlobStorage> lightlikeStorage for BlobStore<Blob> {
    fn name(&self) -> &'static str {
        (**self).config().name()
    }
    fn url(&self) -> io::Result<url::Url> {
        (**self).config().url()
    }
    async fn write(&self, name: &str, reader: UnpinReader, content_length: u64) -> io::Result<()> {
        (**self)
            .put(name, PutResource(reader.0), content_length)
            .await
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin + '_> {
        (**self).get(name)
    }
}
