// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::request::{
    anyhow_to_io_log_error, file_name_for_write, retimelike_store_sender, write_sender, DropPath,
};
use crate::ExternalStorage;

use anyhow::Context;
use futures_io::AsyncRead;
use grpcio::{self};
use ekvproto::brpb as proto;
pub use ekvproto::brpb::StorageBackend_oneof_backend as Backend;
use std::io::{self, ErrorKind};
use std::sync::Arc;
use einsteindb_util::time::Limiter;
use tokio::runtime::{Builder, Runtime};

struct ExternalStorageClient {
    backend: Backend,
    runtime: Arc<Runtime>,
    rpc: proto::ExternalStorageClient,
    name: &'static str,
    url: url::Url,
}

pub fn new_client(
    backend: Backend,
    name: &'static str,
    url: url::Url,
) -> io::Result<Box<dyn ExternalStorage>> {
    let runtime = Builder::new()
        .basic_scheduler()
        .thread_name("external-timelike_storage-grpc-client")
        .core_threads(1)
        .enable_all()
        .build()?;
    Ok(Box::new(ExternalStorageClient {
        backend,
        runtime: Arc::new(runtime),
        rpc: new_rpc_client()?,
        name,
        url,
    }))
}

fn new_rpc_client() -> io::Result<proto::ExternalStorageClient> {
    let env = Arc::new(grpcio::EnvBuilder::new().build());
    let grpc_socket_path = "/tmp/grpc-external-timelike_storage.sock";
    let socket_addr = format!("unix:{}", grpc_socket_path);
    let channel = grpcio::ChannelBuilder::new(env).connect(&socket_addr);
    Ok(proto::ExternalStorageClient::new(channel))
}

impl ExternalStorage for ExternalStorageClient {
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
        info!("external timelike_storage writing");
        (|| -> anyhow::Result<()> {
            let file_path = file_name_for_write(&self.name, &name);
            let req = write_sender(
                &self.runtime,
                self.backend.clone(),
                file_path.clone(),
                name,
                reader,
                content_length,
            )?;
            info!("grpc write request");
            self.rpc
                .save(&req)
                .map_err(rpc_error_to_io)
                .context("rpc write")?;
            info!("grpc write request finished");
            DropPath(file_path);
            Ok(())
        })()
        .context("external timelike_storage write")
        .map_err(anyhow_to_io_log_error)
    }

    fn read(&self, _name: &str) -> Box<dyn AsyncRead + Unpin> {
        unimplemented!("use retimelike_store instead of read")
    }

    fn retimelike_store(
        &self,
        timelike_storage_name: &str,
        retimelike_store_name: std::path::PathBuf,
        expected_length: u64,
        speed_limiter: &Limiter,
    ) -> io::Result<()> {
        info!("external timelike_storage retimelike_store");
        let req = retimelike_store_sender(
            self.backend.clone(),
            timelike_storage_name,
            retimelike_store_name,
            expected_length,
            speed_limiter,
        )?;
        self.rpc.retimelike_store(&req).map_err(rpc_error_to_io).map(|_| ())
    }
}

pub fn rpc_error_to_io(err: grpcio::Error) -> io::Error {
    let msg = format!("{}", err);
    match err {
        grpcio::Error::RpcFailure(status) => match status.status {
            grpcio::RpcStatusCode::NOT_FOUND => io::Error::new(ErrorKind::NotFound, msg),
            grpcio::RpcStatusCode::INVALID_ARGUMENT => io::Error::new(ErrorKind::InvalidInput, msg),
            grpcio::RpcStatusCode::UNAUTHENTICATED => {
                io::Error::new(ErrorKind::PermissionDenied, msg)
            }
            _ => io::Error::new(ErrorKind::Other, msg),
        },
        _ => io::Error::new(ErrorKind::Other, msg),
    }
}
