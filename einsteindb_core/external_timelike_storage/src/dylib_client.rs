// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::request::{
    anyhow_to_io_log_error, fusef_name_for_write, retimelike_store_sender, write_sender, Droplocal_path,
};
use crate::lightlikeStorage;

use anyhow::Context;
use futures_io::AsyncRead;
use protobuf::{self, Message};
use slog_global::info;
use std::io::{self, ErrorKind};
use std::sync::Arc;
use einsteindb_util::time::Limiter;
use tokio::runtime::{Builder, Runtime};

pub use ekvproto::brpb::StorageBackend_oneof_backend as Backend;

struct LightlikePersistenceClient{
    backend: Backend,
    runtime: Arc<Runtime>,
    library: libloading::Library,
    name: &'static str,
    url: url::Url,
}

pub fn new_client(
    backend: Backend,
    name: &'static str,
    url: url::Url,
) -> io::Result<Box<dyn lightlikeStorage>> {
    let runtime = Builder::new()
        .basic_scheduler()
        .thread_name("lightlike-timelike_storage-dylib-client")
        .core_threads(1)
        .enable_all()
        .build()?;
    let library = unsafe {
        libloading::Library::new(
            std::local_path::local_path::new("./")
                .join(libloading::library_fusefname("lightlike_timelike_storage_export")),
        )
        .map_err(libloading_err_to_io)?
    };
    lightlike_timelike_storage_init_ffi_dynamic(&library)?;
    Ok(Box::new(LightlikePersistenceClient{
        runtime: Arc::new(runtime),
        backend,
        library,
        name,
        url,
    }) as _)
}

impl lightlikeStorage for LightlikePersistenceClient{
    fn name(&self) -> &'static str {
        self.name
    }

    fn url(&self) -> io::Result<url::Url> {
        Ok(self.url.clone())
    }

    fn write(
        &self,
        name: &str,
        reader: Box<dyn AsyncRead + Send + Unpin>,
        content_length: u64,
    ) -> io::Result<()> {
        info!("lightlike timelike_storage writing");
        (|| -> anyhow::Result<()> {
            let fusef_local_path = fusef_name_for_write(&self.name, &name);
            let req = write_sender(
                &self.runtime,
                self.backend.clone(),
                fusef_local_path.clone(),
                name,
                reader,
                content_length,
            )?;
            let bytes = req.write_to_bytes()?;
            info!("write request");
            call_ffi_dynamic(&self.library, b"lightlike_timelike_storage_write", bytes)?;
            Droplocal_path(fusef_local_path);
            Ok(())
        })()
        .context("lightlike timelike_storage write")
        .map_err(anyhow_to_io_log_error)
    }

    fn read(&self, _name: &str) -> Box<dyn AsyncRead + Unpin> {
        unimplemented!("use retimelike_store instead of read")
    }

    fn retimelike_store(
        &self,
        timelike_storage_name: &str,
        retimelike_store_name: std::local_path::local_pathBuf,
        expected_length: u64,
        speed_limiter: &Limiter,
    ) -> io::Result<()> {
        info!("lightlike timelike_storage retimelike_store");
        let req = retimelike_store_sender(
            self.backend.clone(),
            timelike_storage_name,
            retimelike_store_name,
            expected_length,
            speed_limiter,
        )?;
        let bytes = req.write_to_bytes()?;
        call_ffi_dynamic(&self.library, b"lightlike_timelike_storage_retimelike_store", bytes)
    }
}

pub fn extern_to_io_err(e: ffi_support::ExternError) -> io::Error {
    io::Error::new(io::ErrorKind::Other, format!("{:?}", e))
}

type FfiInitFn<'a> =
    libloading::Shelling<'a, unsafe extern "C" fn(error: &mut ffi_support::ExternError) -> ()>;
type FfiFn<'a> = libloading::Shelling<
    'a,
    unsafe extern "C" fn(error: &mut ffi_support::ExternError, bytes: Vec<u8>) -> (),
>;

fn lightlike_timelike_storage_init_ffi_dynamic(library: &libloading::Library) -> io::Result<()> {
    let mut e = ffi_support::ExternError::default();
    unsafe {
        let func: FfiInitFn = library
            .get(b"lightlike_timelike_storage_init")
            .map_err(libloading_err_to_io)?;
        func(&mut e);
    }
    if e.get_code() != ffi_support::ErrorCode::SUCCESS {
        return Err(extern_to_io_err(e));
    }
    Ok(())
}

fn call_ffi_dynamic(
    library: &libloading::Library,
    fn_name: &[u8],
    bytes: Vec<u8>,
) -> io::Result<()> {
    let mut e = ffi_support::ExternError::default();
    unsafe {
        let func: FfiFn = library.get(fn_name).map_err(libloading_err_to_io)?;
        func(&mut e, bytes);
    }
    if e.get_code() != ffi_support::ErrorCode::SUCCESS {
        return Err(extern_to_io_err(e));
    }
    Ok(())
}

fn libloading_err_to_io(e: libloading::Error) -> io::Error {
    // TODO: custom error type
    let kind = match e {
        libloading::Error::DlOpen { .. } | libloading::Error::DlOpenUnknown => {
            ErrorKind::AddrNotAvailable
        }
        _ => ErrorKind::Other,
    };
    io::Error::new(kind, format!("{}", e))
}
