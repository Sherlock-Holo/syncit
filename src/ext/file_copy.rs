use std::io;
use std::os::fd::AsRawFd;

use async_trait::async_trait;
use nix::fcntl;
use tokio::fs::File;
use tokio::task;

#[async_trait]
pub trait AsyncFileCopy {
    async fn copy(
        &self,
        target: &Self,
        offset_in: u64,
        offset_out: u64,
        size: u64,
    ) -> io::Result<u64>;
}

#[async_trait]
impl AsyncFileCopy for File {
    async fn copy(
        &self,
        target: &Self,
        offset_in: u64,
        offset_out: u64,
        size: u64,
    ) -> io::Result<u64> {
        let self_fd = self.as_raw_fd();
        let target_fd = target.as_raw_fd();
        let mut offset_in = offset_in as i64;
        let mut offset_out = offset_out as i64;

        let remaining = task::spawn_blocking(move || {
            let mut remaing = size;

            while remaing > 0 {
                let n = fcntl::copy_file_range(
                    self_fd,
                    Some(&mut offset_in),
                    target_fd,
                    Some(&mut offset_out),
                    remaing as _,
                )?;

                if n == 0 {
                    return Ok::<_, io::Error>(remaing);
                }

                remaing -= n as u64;
            }

            Ok(0)
        })
        .await
        .unwrap()?;

        Ok(size - remaining)
    }
}
