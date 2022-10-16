use std::error::Error;
use std::io;
use std::path::PathBuf;

use anyhow::Result;
use bytes::BytesMut;
use futures_util::{Sink, Stream, TryStreamExt};
use sha2::{Digest, Sha256};
use tap::TapFallible;
use tokio::io::{AsyncRead, AsyncReadExt};
use tracing::{error, info};
use uuid::Uuid;

use crate::file_event_produce::WatchEvent;
use crate::index::{Block, BlockChain, Index, IndexFile, IndexGuard, Sha256sum, BLOCK_SIZE};
use crate::sync_control::rumors_event_handler::RumorsEventHandler;
use crate::sync_control::sync_all_handler::SyncAllHandler;
use crate::sync_control::watch_event_handler::WatchEventHandler;
use crate::transfer::DownloadTransfer;

mod rumors_event_handler;
mod sync_all_handler;
mod watch_event_handler;

#[derive(Debug)]
pub enum Event {
    WatchEvent(Vec<WatchEvent>),

    Rumors {
        sender_id: Uuid,
        remote_index: Vec<IndexFile>,
    },

    SyncAll,
}

#[derive(Debug)]
pub struct SyncController<I, St, Si, Dl> {
    user_id: Uuid,
    dir_id: Uuid,
    sync_dir: PathBuf,
    index: I,
    event_stream: St,
    rumor_sender: Si,
    download_transfer: Dl,
}

impl<I, St, Si, E, Dl> SyncController<I, St, Si, Dl>
where
    I: Index,
    <I::Guard as IndexGuard>::Error: Send + Sync + 'static,
    E: Error + Send + Sync + 'static,
    St: Stream<Item = Result<Event, E>> + Unpin,
    Si: Sink<Event>,
    Dl: DownloadTransfer,
    Dl::BlockStream: Unpin,
    Dl::Error: Into<io::Error>,
{
    pub async fn run(&mut self) -> Result<()> {
        while let Some(event) = self
            .event_stream
            .try_next()
            .await
            .tap_err(|err| error!(%err, "try next event failed"))?
        {
            self.pause_watch().await?;

            info!("pause watch done");

            match event {
                Event::WatchEvent(watch_events) => {
                    let handler = WatchEventHandler::new(
                        &self.user_id,
                        &self.dir_id,
                        &self.sync_dir,
                        &self.index,
                    );

                    handler.handle_watch_events(watch_events).await?;

                    info!("handle watch events done");
                }

                Event::Rumors {
                    sender_id,
                    remote_index: rumors,
                } => {
                    let rumors_event_handler = RumorsEventHandler::new(
                        &self.user_id,
                        &self.dir_id,
                        &self.sync_dir,
                        &self.index,
                        &self.download_transfer,
                    );

                    rumors_event_handler
                        .handle_rumors_event(&sender_id, rumors)
                        .await?;

                    info!("handle rumors events done");
                }

                Event::SyncAll => {
                    let sync_all_handler = SyncAllHandler::new(
                        &self.user_id,
                        &self.dir_id,
                        &self.sync_dir,
                        &self.index,
                    );

                    sync_all_handler.handle_sync_all_event().await?;

                    info!("handle sync all event done");
                }
            }

            self.resume_watch().await?;

            info!("resume watch done");
        }

        todo!()
    }
}

impl<I, St, Si, Dl> SyncController<I, St, Si, Dl> {
    async fn pause_watch(&mut self) -> Result<()> {
        todo!()
    }

    async fn resume_watch(&mut self) -> Result<()> {
        todo!()
    }
}

async fn read_fill<R: AsyncRead + Unpin>(reader: &mut R, mut buf: &mut [u8]) -> io::Result<usize> {
    let mut sum = 0;
    while !buf.is_empty() {
        let n = reader.read(buf).await?;
        if n == 0 {
            return Ok(sum);
        }

        sum += n;

        buf = &mut buf[n..];
    }

    Ok(sum)
}

async fn hash_file<R: AsyncRead + Unpin>(mut reader: R) -> Result<(Sha256sum, BlockChain)> {
    let mut hasher = Sha256::new();
    let mut block_hasher = Sha256::new();

    let mut buf = BytesMut::zeroed(BLOCK_SIZE);
    let mut offset = 0;
    let mut blocks = vec![];
    loop {
        let n = read_fill(&mut reader, &mut buf)
            .await
            .tap_err(|err| error!(%err, "read file block failed"))?;

        hasher.update(&buf);

        block_hasher.update(&buf);
        let block_hash_sum = block_hasher.finalize_reset();

        blocks.push(Block {
            offset,
            len: n as _,
            hash_sum: block_hash_sum.into(),
        });

        offset += n as u64;

        if n < buf.len() {
            break;
        }
    }

    let hash_sum = hasher.finalize();

    Ok((
        hash_sum.into(),
        BlockChain {
            block_size: BLOCK_SIZE as _,
            blocks,
        },
    ))
}
