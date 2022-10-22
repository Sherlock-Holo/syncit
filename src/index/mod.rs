use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::io;
use std::ops::DerefMut;
use std::pin::Pin;
use std::time::SystemTime;

use async_trait::async_trait;
use futures_util::Stream;
use mockall::automock;

mod sqlite_index;

// 4MiB
pub const BLOCK_SIZE: usize = 4 * 1024 * 1024;

pub type Sha256sum = [u8; 32];

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct Block {
    pub offset: u64,
    pub len: u64,
    pub hash_sum: Sha256sum,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BlockChain {
    pub block_size: u64,
    pub blocks: Vec<Block>,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub enum FileKind {
    File,
    Symlink,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct FileDetail {
    pub gen: u32,
    pub hash_sum: Sha256sum,
    pub block_chain: Option<BlockChain>,
    pub deleted: bool,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IndexFile {
    pub filename: OsString,
    pub kind: FileKind,
    pub detail: FileDetail,
    pub previous_details: Vec<FileDetail>,
    pub update_time: SystemTime,
    pub update_by: String,
}

#[automock(type Error = io::Error; type IndexStream = Pin < Box < dyn Stream < Item = Result < IndexFile, io::Error >> >>; type Guard = MockIndexGuard;)]
#[async_trait]
pub trait Index {
    type Error: Error;
    type IndexStream: Stream<Item = Result<IndexFile, Self::Error>>;
    type Guard: IndexGuard<Error = Self::Error>;

    async fn list_all_files(&self) -> Result<Self::IndexStream, Self::Error>;

    async fn get_file(&self, filename: &OsStr) -> Result<Option<IndexFile>, Self::Error>;

    async fn begin(&self) -> Result<Self::Guard, Self::Error>;
}

#[automock(type Error = io::Error; type IndexStream = Pin < Box < dyn Stream < Item = Result < IndexFile, io::Error >> >>;)]
#[async_trait]
pub trait IndexGuard {
    type Error: Error;
    type IndexStream: Stream<Item = Result<IndexFile, Self::Error>>;

    async fn list_all_files(&mut self) -> Result<Self::IndexStream, Self::Error>;

    async fn create_file(&mut self, file: &IndexFile) -> Result<(), Self::Error>;

    async fn get_file(&mut self, filename: &OsStr) -> Result<Option<IndexFile>, Self::Error>;

    async fn update_file(&mut self, file: &IndexFile) -> Result<(), Self::Error>;

    async fn commit(self) -> Result<(), Self::Error>;
}

#[async_trait]
impl<G> IndexGuard for Box<G>
where
    G: IndexGuard + Send,
    G::Error: Send + Sync + 'static,
{
    type Error = G::Error;
    type IndexStream = G::IndexStream;

    async fn list_all_files(&mut self) -> Result<Self::IndexStream, Self::Error> {
        self.deref_mut().deref_mut().list_all_files().await
    }

    async fn create_file(&mut self, file: &IndexFile) -> Result<(), Self::Error> {
        self.deref_mut().deref_mut().create_file(file).await
    }

    async fn get_file(&mut self, filename: &OsStr) -> Result<Option<IndexFile>, Self::Error> {
        self.deref_mut().deref_mut().get_file(filename).await
    }

    async fn update_file(&mut self, file: &IndexFile) -> Result<(), Self::Error> {
        self.deref_mut().deref_mut().update_file(file).await
    }

    async fn commit(mut self) -> Result<(), Self::Error> {
        let this = *self;
        this.commit().await
    }
}
