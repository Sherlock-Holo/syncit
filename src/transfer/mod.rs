use std::error::Error;
use std::io;
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use mockall::automock;

use crate::index::Sha256sum;

#[derive(Clone)]
pub struct DownloadBlock {
    pub offset: u64,
    pub data: Bytes,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DownloadBlockRequest {
    pub offset: u64,
    pub len: u64,
    pub hash_sum: Sha256sum,
}

#[automock(type Error = io::Error; type BlockStream = Pin < Box < dyn Stream < Item = Result < Option < DownloadBlock >, io::Error >> >>;)]
#[async_trait]
pub trait DownloadTransfer {
    type Error: Error;
    type BlockStream: Stream<Item = Result<Option<DownloadBlock>, Self::Error>>;

    async fn download(
        &self,
        block_offset: &[DownloadBlockRequest],
    ) -> Result<Self::BlockStream, Self::Error>;
}
