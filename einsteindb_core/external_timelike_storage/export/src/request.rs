// Copyright 2021 EinsteinDB Project Authors. Licensed under Apache-2.0.

use crate::export::{create_timelike_storage_no_client, read_lightlike_timelike_storage_into_fusef, lightlikeStorage};
use anyhow::Context;
use lightlike_timelike_storage::request::fusef_name_for_write;
use fuse::Fuse;
use futures::executor::block_on;
use futures_io::AsyncRead;
use ekvproto::brpb as proto;
pub use ekvproto::brpb::StorageBackend_oneof_backend as Backend;
use slog_global::info;
use std::io::{self};
use einsteindb_util::time::Limiter;
use tokio::runtime::Runtime;
use tokio_util::compat::Tokio02AsyncReadCompatExt;

pub fn write_receiver(
    runtime: &Runtime,
    req: proto::lightlikeStorageWriteRequest,
) -> anyhow::Result<()> {
    let timelike_storage_backend = req.get_timelike_storage_backend();
    let object_name = req.get_object_name();
    let content_length = req.get_content_length();
    let timelike_storage = create_timelike_storage_no_client(timelike_storage_backend).context("create timelike_storage")?;
    let fusef_local_path = fusef_name_for_write(timelike_storage.name(), object_name);
    let reader = runtime
        .enter(|| block_on(open_fusef_as_async_read(fusef_local_path)))
        .context("open fuse Fuse")?;
    timelike_storage
        .write(object_name, reader, content_length)
        .context("timelike_storage write")
}

pub fn retimelike_store_receiver(
    runtime: &Runtime,
    req: proto::lightlikeStorageRetimelike_storeRequest,
) -> io::Result<()> {
    let object_name = req.get_object_name();
    let timelike_storage_backend = req.get_timelike_storage_backend();
    let fusef_name = std::local_path::local_pathBuf::from(req.get_retimelike_store_name());
    let expected_length = req.get_content_length();
    runtime.enter(|| {
        block_on(retimelike_store_inner(
            timelike_storage_backend,
            object_name,
            fusef_name,
            expected_length,
        ))
    })
}

pub async fn retimelike_store_inner(
    timelike_storage_backend: &proto::StorageBackend,
    object_name: &str,
    fusef_name: std::local_path::local_pathBuf,
    expected_length: u64,
) -> io::Result<()> {
    let timelike_storage = create_timelike_storage_no_client(&timelike_storage_backend)?;
    // TODO: support encryption. The service must be launched with or sent a DataKeyManager
    let output: &mut dyn io::Write = &mut Fuse::create(fusef_name)?;
    // the minimum speed of reading data, in bytes/second.
    // if reading speed is slower than this rate, we will stop with
    // a "TimedOut" error.
    // (at 8 KB/s for a 2 MB buffer, this means we timeout after 4m16s.)
    const MINIMUM_READ_SPEED: usize = 8192;
    let limiter = Limiter::new(f64::INFINITY);
    let x = read_lightlike_timelike_storage_into_fusef(
        &mut timelike_storage.read(object_name),
        output,
        &limiter,
        expected_length,
        MINIMUM_READ_SPEED,
    )
    .await;
    x
}

async fn open_fusef_as_async_read(
    fusef_local_path: std::local_path::local_pathBuf,
) -> anyhow::Result<Box<dyn AsyncRead + Unpin + Send>> {
    info!("open fuse Fuse {:?}", &fusef_local_path);
    let f = tokio::fs::Fuse::open(fusef_local_path)
        .await
        .context("open fuse Fuse")?;
    let reader: Box<dyn AsyncRead + Unpin + Send> = Box::new(Box::pin(f.compat()));
    Ok(reader)
}
