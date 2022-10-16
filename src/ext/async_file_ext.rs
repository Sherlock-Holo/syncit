use std::fs::File as StdFile;
use std::io::Error;
use std::mem::ManuallyDrop;
use std::os::unix::io::{AsRawFd, FromRawFd};

use async_trait::async_trait;
use bytes::{Buf, Bytes, BytesMut};
use tokio::fs::File;
use tokio::task;

#[async_trait]
pub trait AsyncFileExt {
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<u64, Error>;

    async fn write_at(&self, data: &[u8], offset: u64) -> Result<u64, Error>;
}

#[async_trait]
impl AsyncFileExt for File {
    async fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<u64, Error> {
        use std::os::unix::fs::FileExt;

        let fd = self.as_raw_fd();
        let std_file = unsafe { ManuallyDrop::new(StdFile::from_raw_fd(fd)) };

        let mut bytes_mut = BytesMut::zeroed(buf.len());

        let (mut bytes_mut, n) = task::spawn_blocking(move || {
            let n = std_file.read_at(&mut bytes_mut, offset)?;

            Ok::<_, Error>((bytes_mut, n))
        })
        .await
        .unwrap()?;

        bytes_mut.copy_to_slice(&mut buf[..n]);

        Ok(n as _)
    }

    async fn write_at(&self, data: &[u8], offset: u64) -> Result<u64, Error> {
        use std::os::unix::fs::FileExt;

        let fd = self.as_raw_fd();
        let std_file = unsafe { ManuallyDrop::new(StdFile::from_raw_fd(fd)) };

        let data = Bytes::copy_from_slice(data);

        let n = task::spawn_blocking(move || std_file.write_at(&data, offset))
            .await
            .unwrap()?;

        Ok(n as _)
    }
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::io::SeekFrom;

    use tempfile::TempDir;
    use tokio::fs::OpenOptions;
    use tokio::io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt};

    use super::*;

    #[tokio::test]
    async fn test_read_at() {
        let temp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(temp_dir.path().join("test"))
            .await
            .unwrap();

        file.write_all(b"test").await.unwrap();

        let mut buf = [0; 2];
        let n = file.read_at(&mut buf, 2).await.unwrap() as usize;

        assert_eq!(&b"st"[..n], &buf[..n]);
    }

    #[tokio::test]
    async fn test_write_at() {
        let temp_dir = TempDir::new_in(env::temp_dir()).unwrap();
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(temp_dir.path().join("test"))
            .await
            .unwrap();

        file.write_all(b"test").await.unwrap();
        let n = file.write_at(b"11", 2).await.unwrap() as usize;
        file.seek(SeekFrom::Start(0)).await.unwrap();

        let mut buf = vec![];
        file.read_to_end(&mut buf).await.unwrap();

        assert_eq!(&buf[2..n + 2], &b"11"[..n]);
    }
}
