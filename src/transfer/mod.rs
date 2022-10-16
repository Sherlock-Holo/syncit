use std::error::Error;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;

use crate::index::Sha256sum;

#[derive(Clone)]
pub struct DownloadBlock {
    pub offset: u64,
    pub data: Bytes,
}

#[derive(Debug)]
pub struct DownloadBlockRequest {
    pub offset: u64,
    pub len: u64,
    pub hash_sum: Sha256sum,
}

#[async_trait]
pub trait DownloadTransfer {
    type Error: Error;
    type BlockStream: Stream<Item = Result<DownloadBlock, Self::Error>>;

    async fn download(
        &self,
        block_offset: &[DownloadBlockRequest],
    ) -> Result<Self::BlockStream, Self::Error>;
}
