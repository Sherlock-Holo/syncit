use std::ops::{Deref, DerefMut};
use std::path::{Path, PathBuf};

use rand::distributions::Alphanumeric;
use rand::Rng;
use tokio::fs::{File, OpenOptions};
use tokio::{fs, io};

#[derive(Debug)]
pub struct AsyncTempFile {
    path: PathBuf,
    file: Option<File>,
}

impl Deref for AsyncTempFile {
    type Target = File;

    fn deref(&self) -> &Self::Target {
        self.file.as_ref().unwrap()
    }
}

impl DerefMut for AsyncTempFile {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.file.as_mut().unwrap()
    }
}

impl AsyncTempFile {
    pub async fn create(dir: &Path) -> io::Result<Self> {
        let filename = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .map(char::from)
            .collect::<String>();
        let path = dir.join(filename);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;

        Ok(Self {
            path,
            file: Some(file),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn close(&mut self) {
        self.file.take();
    }
}

impl Drop for AsyncTempFile {
    fn drop(&mut self) {
        let path = self.path.clone();
        let file = self.file.take();

        tokio::spawn(async move {
            drop(file);
            let _ = fs::remove_file(path).await;
        });
    }
}
