use std::error::Error;
use std::io;
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::Stream;
use mockall::automock;
use uuid::Uuid;

use crate::index::Sha256sum;

pub mod grpc;

#[derive(Clone, Eq, PartialEq, Debug)]
pub struct DownloadBlock {
    pub offset: u64,
    pub data: Bytes,
}

#[derive(Debug, Eq, PartialEq)]
pub struct DownloadBlockRequest {
    pub dir_id: Uuid,
    pub filename: String,
    pub offset: u64,
    pub len: u64,
    pub hash_sum: Sha256sum,
}

#[automock(type Error = io::Error; type BlockStream = Pin < Box < dyn Stream < Item = Result < Option < DownloadBlock >, io::Error >> >>;)]
#[async_trait]
pub trait DownloadTransfer {
    type Error: Error;
    type BlockStream<'a>: Stream<Item = Result<Option<DownloadBlock>, Self::Error>>
    where
        Self: 'a;

    async fn download<'a>(
        &'a self,
        block_offset: &'a [DownloadBlockRequest],
    ) -> Result<Self::BlockStream<'a>, Self::Error>;
}
