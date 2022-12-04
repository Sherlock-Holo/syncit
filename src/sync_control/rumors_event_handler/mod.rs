use std::cmp::Ordering;
use std::error::Error;
use std::ffi::OsStr;
use std::io::ErrorKind;
use std::path::Path;
use std::pin::pin;
use std::{io, u64};

use anyhow::{anyhow, Result};
use chrono::{FixedOffset, Utc};
use futures_util::stream::FuturesUnordered;
use futures_util::{Sink, SinkExt, Stream, TryStreamExt};
use itertools::{EitherOrBoth, Itertools};
use tap::TapFallible;
use tokio::fs;
use tokio::fs::{File, OpenOptions};
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::ext::{AsyncFileCopy, AsyncFileExt, AsyncTempFile};
use crate::index::{Block, Index, IndexFile, IndexGuard};
use crate::sync_control::SendRumors;
use crate::transfer::{DownloadBlock, DownloadBlockRequest, DownloadTransfer};

pub struct RumorsEventHandler<'a, I, Dl, Si> {
    user_id: Uuid,
    dir_id: Uuid,
    sync_dir: &'a Path,
    index: &'a I,
    download_transfer: &'a Dl,
    rumor_sender: Si,
}

impl<'a, I, Dl, Si> RumorsEventHandler<'a, I, Dl, Si> {
    pub fn new(
        user_id: Uuid,
        dir_id: Uuid,
        sync_dir: &'a Path,
        index: &'a I,
        download_transfer: &'a Dl,
        rumor_sender: Si,
    ) -> Self {
        Self {
            user_id,
            dir_id,
            sync_dir,
            index,
            download_transfer,
            rumor_sender,
        }
    }
}

impl<'a, 'b, I, Dl, Si> RumorsEventHandler<'a, I, Dl, Si>
where
    I: Index,
    <I::Guard as IndexGuard>::Error: Send + Sync + 'static,
    Dl: DownloadTransfer + 'b,
    Dl::BlockStream<'b>: Unpin,
    Dl::Error: Into<io::Error>,
    Si: Sink<SendRumors> + Unpin,
    Si::Error: Error + Send + Sync + 'static,
{
    pub async fn handle_rumors_event(
        mut self,
        sender_id: Uuid,
        rumors: Vec<IndexFile>,
    ) -> Result<()> {
        let mut new_rumors = Vec::with_capacity(rumors.len());
        for rumor in rumors {
            let new = self.handle_rumor(&rumor).await?;

            info!(new, filename = ?rumor.filename, "handle rumor done");

            if new {
                new_rumors.push(rumor);
            }
        }

        if !new_rumors.is_empty() {
            self.send_rumors_to_others(sender_id, new_rumors).await?;

            info!("send new rumors to others done");
        }

        Ok(())
    }

    /// when return false, means the rumor is old and should be ignore
    async fn handle_rumor(&mut self, remote_index_file: &IndexFile) -> Result<bool> {
        let mut index_guard = self.index.begin().await?;

        match index_guard.get_file(&remote_index_file.filename).await? {
            None => {
                index_guard.create_file(remote_index_file).await?;

                info!(filename = ?remote_index_file.filename, "create file index done");

                let path = self.sync_dir.join(&remote_index_file.filename);

                // file has been deleted
                if remote_index_file.detail.deleted {
                    match fs::remove_file(&path).await {
                        Err(err) if err.kind() == ErrorKind::NotFound => {
                            info!(?path, "file may have been deleted");
                        }

                        Err(err) => {
                            error!(%err, ?path, "delete file failed");

                            return Err(err.into());
                        }

                        Ok(_) => {}
                    }

                    index_guard.commit().await?;

                    info!("index guard commit done");

                    return Ok(true);
                }

                let mut file = AsyncTempFile::create(self.sync_dir)
                    .await
                    .tap_err(|err| error!(%err, "create temp file failed"))?;

                info!(?path, "open file done");

                let block_chain = match &remote_index_file.detail.block_chain {
                    None => {
                        error!(filename = ?remote_index_file.filename, "index file doesn't have block chain");

                        return Err(anyhow!(
                            "{:?} index file doesn't have block chain",
                            remote_index_file.filename
                        ));
                    }

                    Some(block_chain) => block_chain,
                };

                let file_size = block_chain.blocks.iter().map(|block| block.len).sum();

                file.set_len(file_size)
                    .await
                    .tap_err(|err| error!(%err, ?path, "set file size failed"))?;

                let download_block_requests = blocks_to_download_block_requests(
                    self.dir_id,
                    Path::new(&remote_index_file.filename),
                    &block_chain.blocks,
                );

                let block_stream = self
                    .download_transfer
                    .download(&download_block_requests)
                    .await
                    .map_err(Into::into)?
                    .map_err(Into::into);

                info!(?download_block_requests, "get block stream done");

                if !sync_file(&remote_index_file.filename, &file, block_stream).await? {
                    warn!(filename = ?remote_index_file.filename, "sync file canceled");

                    return Ok(false);
                }

                info!(?path, "sync file data done");

                file.close();
                let temp_file_path = file.path();

                fs::rename(temp_file_path, &path)
                    .await
                    .tap_err(|err| error!(%err, ?path, "move temp file to target file failed"))?;

                info!(?path, "move temp file to target file done");

                index_guard.commit().await?;

                info!("index guard commit done");

                Ok(true)
            }

            Some(local_index_file) => {
                match local_index_file
                    .detail
                    .gen
                    .cmp(&remote_index_file.detail.gen)
                {
                    Ordering::Less => {
                        self.handle_remote_is_latest(
                            remote_index_file,
                            &local_index_file,
                            index_guard,
                        )
                        .await
                    }

                    Ordering::Equal => {
                        self.handle_gen_eq(remote_index_file, &local_index_file, index_guard)
                            .await
                    }

                    Ordering::Greater => {
                        self.handle_local_is_latest(remote_index_file, &local_index_file);

                        Ok(false)
                    }
                }
            }
        }
    }

    async fn handle_gen_eq(
        &mut self,
        remote_index_file: &IndexFile,
        local_index_file: &IndexFile,
        mut index_guard: I::Guard,
    ) -> Result<bool> {
        if remote_index_file == local_index_file {
            info!("nothing changed");

            return Ok(false);
        }

        // remote and local change together so they have same gen but different update time,
        // however, local is newer, so ignore remote
        if remote_index_file.update_time < local_index_file.update_time {
            info!("ignore remote");

            return Ok(false);
        }

        if remote_index_file.update_time > local_index_file.update_time {
            index_guard.update_file(remote_index_file).await?;

            info!(filename = ?remote_index_file.filename, "update file index done");

            if remote_index_file.detail.deleted {
                let path = self.sync_dir.join(&remote_index_file.filename);

                fs::remove_file(&path)
                    .await
                    .tap_err(|err| error!(%err, ?path, "delete file failed"))?;

                info!(?path, "delete file done");

                index_guard.commit().await?;

                info!("index guard commit done");

                return Ok(true);
            }

            let path = self.sync_dir.join(&remote_index_file.filename);
            if !local_index_file.detail.deleted {
                let origin_file = File::open(&path)
                    .await
                    .tap_err(|err| error!(%err, ?path, "open origin target file failed"))?;

                create_conflict_file_from(&origin_file, self.sync_dir, &remote_index_file.filename)
                    .await?;

                info!(filename = ?remote_index_file.filename, "create conflict file done");
            }

            let remote_block_chain = match &remote_index_file.detail.block_chain {
                None => {
                    error!(filename = ?remote_index_file.filename, "index file doesn't have block chain");

                    return Err(anyhow!(
                        "{:?} index file doesn't have block chain",
                        remote_index_file.filename
                    ));
                }

                Some(block_chain) => block_chain,
            };

            let file_size = remote_block_chain
                .blocks
                .iter()
                .map(|block| block.len)
                .sum::<u64>();
            let mut temp_file = AsyncTempFile::create(self.sync_dir)
                .await
                .tap_err(|err| error!(%err, "create temp file failed"))?;

            info!("create temp file done");

            temp_file
                .set_len(file_size)
                .await
                .tap_err(|err| error!(%err, "set temp file size failed"))?;

            let download_block_requests = blocks_to_download_block_requests(
                self.dir_id,
                Path::new(&remote_index_file.filename),
                &remote_block_chain.blocks,
            );

            let block_stream = self
                .download_transfer
                .download(&download_block_requests)
                .await
                .map_err(Into::into)?
                .map_err(Into::into);

            info!(?download_block_requests, "get block stream done");

            sync_file(&remote_index_file.filename, &temp_file, block_stream).await?;

            info!("sync file data done");

            temp_file.close();
            let temp_path = temp_file.path();

            fs::rename(temp_path, &path).await.tap_err(
                |err| error!(%err, ?temp_path, ?path, "rename temp file to target file failed"),
            )?;

            info!(?temp_path, ?path, "rename temp file to target file done");

            index_guard.commit().await?;

            info!("index guard commit done");

            return Ok(true);
        }

        warn!(
            filename = ?remote_index_file.filename,
            "remote and local change file at the same time, that's so hard to sync, we only can warn and ingore it"
        );

        Ok(false)
    }

    fn handle_local_is_latest(
        &mut self,
        remote_index_file: &IndexFile,
        local_index_file: &IndexFile,
    ) {
        // rumor is old
        if local_index_file
            .previous_details
            .iter()
            .any(|previous_detail| {
                previous_detail.gen == remote_index_file.detail.gen
                    && previous_detail.hash_sum == remote_index_file.detail.hash_sum
            })
        {
            info!("rumor is old, ignore");
        } else {
            info!("rumor is conflict but old, still ignore");
        }
    }

    async fn handle_remote_is_latest(
        &mut self,
        remote_index_file: &IndexFile,
        local_index_file: &IndexFile,
        mut index_guard: I::Guard,
    ) -> Result<bool> {
        // remote is latest and no conflict, can apply directly
        let path = self.sync_dir.join(&remote_index_file.filename);

        if remote_index_file
            .previous_details
            .iter()
            .any(|previous_detail| {
                previous_detail.gen == local_index_file.detail.gen
                    && previous_detail.hash_sum == local_index_file.detail.hash_sum
            })
        {
            index_guard.update_file(remote_index_file).await?;

            info!(filename = ?remote_index_file.filename, "update file index done");

            // file has been deleted
            if remote_index_file.detail.deleted {
                match fs::remove_file(&path).await {
                    Err(err) if err.kind() == ErrorKind::NotFound => {
                        info!(?path, "file may have been deleted");
                    }

                    Err(err) => {
                        error!(%err, ?path, "delete file failed");

                        return Err(err.into());
                    }

                    Ok(_) => {}
                }

                index_guard.commit().await?;

                info!("index guard commit done");

                return Ok(true);
            }

            let mut temp_file = AsyncTempFile::create(self.sync_dir)
                .await
                .tap_err(|err| error!(%err, ?path, "open temp file failed"))?;

            info!(?path, "open temp file done");

            let file = File::open(&path)
                .await
                .tap_err(|err| error!(%err, ?path, "open target file failed"))?;

            info!(?path, "open target file done");

            let metadata = file
                .metadata()
                .await
                .tap_err(|err| error!(%err, ?path, "get target origin file metadata failed"))?;

            info!("get target origin file metadata done");

            file.copy(&temp_file, 0, 0, metadata.len())
                .await
                .tap_err(|err| error!(%err, "copy origin file data to temp file failed"))?;

            drop(file);

            let remote_block_chain = match &remote_index_file.detail.block_chain {
                None => {
                    error!(filename = ?remote_index_file.filename, "index file doesn't have block chain");

                    return Err(anyhow!(
                        "{:?} index file doesn't have block chain",
                        remote_index_file.filename
                    ));
                }

                Some(block_chain) => block_chain,
            };

            let file_size = remote_block_chain
                .blocks
                .iter()
                .map(|block| block.len)
                .sum();

            temp_file
                .set_len(file_size)
                .await
                .tap_err(|err| error!(%err, "set temp file size failed"))?;

            let download_block_requests = match &local_index_file.detail.block_chain {
                None => blocks_to_download_block_requests(
                    self.dir_id,
                    Path::new(&remote_index_file.filename),
                    &remote_block_chain.blocks,
                ),

                Some(local_block_chain) => compare_blocks(
                    self.dir_id,
                    Path::new(&remote_index_file.filename),
                    &remote_block_chain.blocks,
                    &local_block_chain.blocks,
                ),
            };

            let block_stream = self
                .download_transfer
                .download(&download_block_requests)
                .await
                .map_err(Into::into)?
                .map_err(Into::into);

            info!(?download_block_requests, "get block stream done");

            sync_file(&remote_index_file.filename, &temp_file, block_stream).await?;

            info!(?path, "sync file data done");

            temp_file.close();
            let temp_file_path = temp_file.path();

            fs::rename(temp_file_path, &path)
                .await
                .tap_err(|err| error!(%err, ?path, "move temp file to target file failed"))?;

            info!(?path, "move temp file to target file done");

            index_guard.commit().await?;

            info!("index guard commit done");

            return Ok(true);
        }

        // remote file and local file is conflict, need copy the local file as conflict file then
        // apply the remote file
        let origin_file = File::open(&path)
            .await
            .tap_err(|err| error!(%err, "open target origin file failed"))?;

        info!(?path, "open target origin file done");
        create_conflict_file_from(&origin_file, self.sync_dir, &remote_index_file.filename).await?;

        info!(origin_filename = ?remote_index_file.filename, "create conflict file done");

        let remote_block_chain = match &remote_index_file.detail.block_chain {
            None => {
                error!(filename = ?remote_index_file.filename, "index file doesn't have block chain");

                return Err(anyhow!(
                    "{:?} index file doesn't have block chain",
                    remote_index_file.filename
                ));
            }

            Some(remote_block_chain) => remote_block_chain,
        };

        let file_size = remote_block_chain
            .blocks
            .iter()
            .map(|block| block.len)
            .sum::<u64>();

        let mut temp_file = AsyncTempFile::create(self.sync_dir)
            .await
            .tap_err(|err| error!(%err, "create temp file failed"))?;

        info!("create temp file done");

        temp_file
            .set_len(file_size)
            .await
            .tap_err(|err| error!(%err, "set temp file size failed"))?;

        let download_block_requests = blocks_to_download_block_requests(
            self.dir_id,
            Path::new(&remote_index_file.filename),
            &remote_block_chain.blocks,
        );

        let block_stream = self
            .download_transfer
            .download(&download_block_requests)
            .await
            .map_err(Into::into)?
            .map_err(Into::into);

        info!(?download_block_requests, "get block stream done");

        sync_file(&remote_index_file.filename, &temp_file, block_stream).await?;

        info!(?path, "sync file data done");

        temp_file.close();
        let temp_path = temp_file.path();

        fs::rename(temp_path, &path)
            .await
            .tap_err(|err| error!(%err, "move temp file to target file failed"))?;

        index_guard.commit().await?;

        info!("index guard commit done");

        Ok(true)
    }

    async fn send_rumors_to_others(
        &mut self,
        sender_id: Uuid,
        rumors: Vec<IndexFile>,
    ) -> Result<()> {
        let send_rumors = SendRumors {
            dir_id: self.dir_id,
            rumors,
            except: Some(sender_id),
        };

        self.rumor_sender.send(send_rumors).await?;

        Ok(())
    }
}

fn blocks_to_download_block_requests<'a>(
    dir_id: Uuid,
    filename: &'a Path,
    blocks: &'a [Block],
) -> Vec<DownloadBlockRequest> {
    blocks
        .iter()
        .map(|block| DownloadBlockRequest {
            dir_id,
            filename: filename.to_string_lossy().to_string(),
            offset: block.offset,
            len: block.len,
            hash_sum: block.hash_sum,
        })
        .collect()
}

async fn create_conflict_file_from(
    origin_file: &File,
    sync_dir: &Path,
    filename: &OsStr,
) -> io::Result<()> {
    let now_str = Utc::now()
        .with_timezone(&FixedOffset::east_opt(8 * 3600).expect("create fixed offset failed"))
        .format("%Y-%m-%d-%H-%M-%S");
    let mut filename = filename.to_os_string();
    filename.push(format!(".{now_str}"));
    filename.push(".conflict");

    let conflict_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create_new(true)
        .open(sync_dir.join(&filename))
        .await
        .tap_err(|err| error!(%err, "create conflict file failed"))?;

    info!(?filename, "create conflict file done");

    let metadata = origin_file
        .metadata()
        .await
        .tap_err(|err| error!(%err, "get target origin file metadata failed"))?;

    origin_file
        .copy(&conflict_file, 0, 0, metadata.len())
        .await
        .tap_err(|err| error!(%err, "copy target origin file data to conflict file failed"))?;

    Ok(())
}

async fn sync_file<S: Stream<Item = io::Result<Option<DownloadBlock>>>>(
    filename: &OsStr,
    file: &File,
    block_stream: S,
) -> io::Result<bool> {
    let futures_unordered = FuturesUnordered::new();
    let mut block_stream = pin!(block_stream.map_err(io::Error::from));
    while let Some(download_block) = block_stream.try_next().await? {
        match download_block {
            None => {
                warn!(?filename, "can't find block, maybe file is outdated");

                return Ok(false);
            }

            Some(download_block) => {
                futures_unordered.push(async move {
                    file.write_at(&download_block.data, download_block.offset)
                        .await
                        .tap_err(
                            |err| error!(%err, offset = download_block.offset, "write at failed"),
                        )?;

                    Ok::<_, io::Error>(())
                });
            }
        }
    }

    futures_unordered
        .try_collect()
        .await
        .tap_err(|err| error!(%err, "write at failed"))?;

    Ok(true)
}

fn compare_blocks(
    dir_id: Uuid,
    filename: &Path,
    left_blocks: &[Block],
    right_blocks: &[Block],
) -> Vec<DownloadBlockRequest> {
    let filename = filename.to_string_lossy().to_string();

    left_blocks
        .iter()
        .zip_longest(right_blocks.iter())
        .filter_map(|zip_result| match zip_result {
            EitherOrBoth::Both(remote_block, local_block) => {
                if *remote_block == *local_block {
                    None
                } else {
                    Some(DownloadBlockRequest {
                        dir_id,
                        filename: filename.clone(),
                        offset: remote_block.offset,
                        len: remote_block.len,
                        hash_sum: remote_block.hash_sum,
                    })
                }
            }
            EitherOrBoth::Left(remote_block) => Some(DownloadBlockRequest {
                dir_id,
                filename: filename.clone(),
                offset: remote_block.offset,
                len: remote_block.len,
                hash_sum: remote_block.hash_sum,
            }),

            // when this branch hit, all remaining blocks are right blocks when will be ignore
            EitherOrBoth::Right(_) => None,
        })
        .collect::<Vec<_>>()
}

#[cfg(test)]
mod tests;
