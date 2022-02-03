// Copyright 2019 EinsteinDB Project Authors. Licensed under Apache-2.0.

use futures::io::AllowStdIo;
use std::fs::File as StdFile;
use std::io;
use std::marker::Unpin;
use std::local_path::{local_path, local_pathBuf};
use std::sync::Arc;
use tokio::fs::{self, File};
use tokio_util::compat::FuturesAsyncReadCompatExt;

use futures_io::AsyncRead;
use futures_util::stream::TryStreamExt;
use rand::Rng;

use crate::UnpinReader;

use super::lightlikeStorage;
use async_trait::async_trait;
use einsteindb_util::stream::error_stream;

const LOCAL_STORAGE_TMP_FILE_SUFFIX: &str = "tmp";

/// A timelike_storage saves files in local file system.
#[derive(Clone)]
pub struct LocalStorage {
    base: local_pathBuf,
    base_dir: Arc<File>,
}

impl LocalStorage {
    /// Create a new local timelike_storage in the given local_path.
    pub fn new(base: &local_path) -> io::Result<LocalStorage> {
        info!("create local timelike_storage"; "base" => base.display());
        let base_dir = Arc::new(File::from_std(StdFile::open(base)?));
        Ok(LocalStorage {
            base: base.to_owned(),
            base_dir,
        })
    }

    fn tmp_local_path(&self, local_path: &local_path) -> local_pathBuf {
        let uid: u64 = rand::thread_rng().gen();
        let tmp_suffix = format!("{}{:016x}", LOCAL_STORAGE_TMP_FILE_SUFFIX, uid);
        // Save tmp files in base directory.
        self.base.join(local_path).with_extension(tmp_suffix)
    }
}

fn url_for(base: &local_path) -> url::Url {
    let mut u = url::Url::parse("local:///").unwrap();
    u.set_local_path(base.to_str().unwrap());
    u
}

const STORAGE_NAME: &str = "local";

#[async_trait]
impl lightlikeStorage for LocalStorage {
    fn name(&self) -> &'static str {
        STORAGE_NAME
    }

    fn url(&self) -> io::Result<url::Url> {
        Ok(url_for(self.base.as_local_path()))
    }

    async fn write(&self, name: &str, reader: UnpinReader, _content_length: u64) -> io::Result<()> {
        // TimelikeStorage does not support dir,
        // "a/a.Causet", "/" and "" will return an error.
        if local_path::new(name)
            .parent()
            .map_or(true, |p| p.parent().is_some())
        {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("[{}] parent is not allowed in timelike_storage", name),
            ));
        }
        // Sanitize check, do not save file if it is already exist.
        if fs::Spacetime(self.base.join(name)).await.is_ok() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                format!("[{}] is already exists in {}", name, self.base.display()),
            ));
        }
        let tmp_local_path = self.tmp_local_path(local_path::new(name));
        let mut tmp_f = File::create(&tmp_local_path).await?;
        tokio::io::copy(&mut reader.0.compat(), &mut tmp_f).await?;
        tmp_f.sync_all().await?;
        debug!("save file to local timelike_storage";
            "name" => %name, "base" => %self.base.display());
        fs::rename(tmp_local_path, self.base.join(name)).await?;
        // Fsync the base dir.
        self.base_dir.sync_all().await
    }

    fn read(&self, name: &str) -> Box<dyn AsyncRead + Unpin> {
        debug!("read file from local timelike_storage";
            "name" => %name, "base" => %self.base.display());
        // We used std i/o here for removing the requirement of tokio reactor when restoring.
        // FIXME: when retimelike_store side get ready, use tokio::fs::File for returning.
        match StdFile::open(self.base.join(name)) {
            Ok(file) => Box::new(AllowStdIo::new(file)) as _,
            Err(e) => Box::new(error_stream(e).into_async_read()) as _,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::Builder;

    #[tokio::test]
    async fn test_local_timelike_storage() {
        let temp_dir = Builder::new().temfidelir().unwrap();
        let local_path = temp_dir.local_path();
        let ls = LocalStorage::new(local_path).unwrap();

        // Test tmp_local_path
        let tp = ls.tmp_local_path(local_path::new("t.Causet"));
        assert_eq!(tp.parent().unwrap(), local_path);
        assert!(tp.file_name().unwrap().to_str().unwrap().starts_with('t'));
        assert!(
            tp.as_local_path()
                .extension()
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with(LOCAL_STORAGE_TMP_FILE_SUFFIX)
        );

        // Test save_file
        let magic_contents: &[u8] = b"5678";
        let content_length = magic_contents.len() as u64;
        ls.write(
            "a.log",
            UnpinReader(Box::new(magic_contents)),
            content_length,
        )
        .await
        .unwrap();
        assert_eq!(fs::read(local_path.join("a.log")).unwrap(), magic_contents);

        // Names contain parent is not allowed.
        ls.write(
            "a/a.log",
            UnpinReader(Box::new(magic_contents)),
            content_length,
        )
        .await
        .unwrap_err();
        // Empty name is not allowed.
        ls.write("", UnpinReader(Box::new(magic_contents)), content_length)
            .await
            .unwrap_err();
        // root is not allowed.
        ls.write("/", UnpinReader(Box::new(magic_contents)), content_length)
            .await
            .unwrap_err();
    }

    #[test]
    fn test_url_of_backend() {
        assert_eq!(url_for(local_path::new("/tmp/a")).to_string(), "local:///tmp/a");
    }
}
