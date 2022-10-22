use std::collections::HashMap;
use std::error::Error;
use std::ffi::{OsStr, OsString};
use std::io::ErrorKind;
use std::path::Path;
use std::time::SystemTime;
use std::{io, mem};

use anyhow::Result;
use futures_util::{Sink, SinkExt, TryStreamExt};
use tap::TapFallible;
use tokio::fs;
use tokio::fs::{DirEntry, File};
use tokio_stream::wrappers::ReadDirStream;
use tracing::{error, info};
use uuid::Uuid;

use crate::index::{FileDetail, FileKind, Index, IndexFile, IndexGuard};
use crate::sync_control::{hash_file, SendRumors};

pub struct SyncAllHandler<'a, I, Si> {
    user_id: &'a Uuid,
    dir_id: &'a Uuid,
    sync_dir: &'a Path,
    index: &'a I,
    rumor_sender: Si,
}

impl<'a, I, Si> SyncAllHandler<'a, I, Si> {
    pub fn new(
        user_id: &'a Uuid,
        dir_id: &'a Uuid,
        sync_dir: &'a Path,
        index: &'a I,
        rumor_sender: Si,
    ) -> Self {
        Self {
            user_id,
            dir_id,
            sync_dir,
            index,
            rumor_sender,
        }
    }
}

impl<'a, I, Si> SyncAllHandler<'a, I, Si>
where
    I: Index,
    <I::Guard as IndexGuard>::Error: Send + Sync + 'static,
    Si: Sink<SendRumors> + Unpin,
    Si::Error: Error + Send + Sync + 'static,
{
    pub async fn handle_sync_all_event(mut self) -> Result<()> {
        let dir = self.sync_dir;

        let read_dir = fs::read_dir(dir)
            .await
            .tap_err(|err| error!(%err, sync_dir = ?self.sync_dir, "read dir failed"))?;

        let entries = ReadDirStream::new(read_dir)
            .try_filter_map(|entry| async move {
                let file_type = entry
                    .file_type()
                    .await
                    .tap_err(|err| error!(%err, "get entry file type failed"))?;
                let path = entry.path();
                let path = path.strip_prefix(dir).map_err(|err| {
                    error!(%err, "trim dir prefix failed");

                    io::Error::new(ErrorKind::Other, err)
                })?;

                if file_type.is_dir() {
                    Ok(None)
                } else {
                    Ok(Some((path.as_os_str().to_os_string(), entry)))
                }
            })
            .try_collect::<HashMap<_, _>>()
            .await?;

        let mut index_guard = self.index.begin().await?;

        info!("get index guard done");

        let all_file_index_stream = index_guard.list_all_files().await?;

        info!("get all file index stream done");

        futures_util::pin_mut!(all_file_index_stream);

        let index_files = all_file_index_stream
            .map_ok(|index_file: IndexFile| (index_file.filename.clone(), index_file))
            .try_collect::<HashMap<_, _>>()
            .await
            .tap_err(|err| error!(%err, "collect all index files failed"))?;

        let new_files = get_new_files(&entries, &index_files);
        let delete_files = get_delete_files(&entries, &index_files);
        let exists_files = get_exists_files(&entries, &index_files);

        let latest_file_index = self
            .update_index(&new_files, &delete_files, &exists_files, index_guard)
            .await?;

        info!("update index done");

        self.send_rumors_to_all(latest_file_index).await?;

        info!("send rumors to all done");

        Ok(())
    }

    async fn update_index(
        &mut self,
        new_files: &[&OsStr],
        delete_files: &[&OsStr],
        exists_files: &[&OsStr],
        mut index_guard: I::Guard,
    ) -> Result<Vec<IndexFile>> {
        for filename in delete_files {
            match index_guard.get_file(filename).await? {
                None => {
                    error!(delete_file = ?filename, "delete file not found in index guard");

                    return Err(anyhow::anyhow!(
                        "delete file {:?} not found in index guard",
                        filename
                    ));
                }

                Some(mut index_file) => {
                    if index_file.detail.deleted {
                        continue;
                    }

                    info!(delete_file = ?filename, "get delete file index done");

                    let gen = index_file.detail.gen + 1;
                    let mut old_detail = mem::replace(
                        &mut index_file.detail,
                        FileDetail {
                            gen,
                            hash_sum: [0; 32],
                            block_chain: None,
                            deleted: true,
                        },
                    );
                    old_detail.block_chain.take();
                    index_file.previous_details.push(old_detail);
                    index_file.update_time = SystemTime::now();
                    index_file.update_by = self.user_id.as_hyphenated().to_string();

                    index_guard.update_file(&index_file).await?;

                    info!(delete_file = ?filename, "update delete file index done");
                }
            }
        }

        for filename in new_files {
            let path = self.sync_dir.join(filename);
            let file = File::open(&path)
                .await
                .tap_err(|err| error!(%err, ?path, "open file failed"))?;

            info!(new_filename = ?filename, "open file done");

            let (hash_sum, block_chain) = hash_file(file).await?;

            info!(new_filename = ?filename, "hash file done");

            match index_guard.get_file(filename).await? {
                Some(mut index_file) => {
                    if !index_file.detail.deleted && index_file.detail.hash_sum == hash_sum {
                        continue;
                    }

                    let gen = index_file.detail.gen + 1;
                    let mut old_detail = mem::replace(
                        &mut index_file.detail,
                        FileDetail {
                            gen,
                            hash_sum,
                            block_chain: Some(block_chain),
                            deleted: false,
                        },
                    );
                    old_detail.block_chain.take();
                    index_file.previous_details.push(old_detail);
                    index_file.update_time = SystemTime::now();
                    index_file.update_by = self.user_id.as_hyphenated().to_string();

                    index_guard.update_file(&index_file).await?;

                    info!(new_filename = ?filename, "update file index done");
                }

                None => {
                    let index_file = IndexFile {
                        filename: filename.to_os_string(),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 1,
                            hash_sum,
                            block_chain: Some(block_chain),
                            deleted: false,
                        },
                        previous_details: vec![],
                        update_time: SystemTime::now(),
                        update_by: self.user_id.as_hyphenated().to_string(),
                    };

                    index_guard.create_file(&index_file).await?;

                    info!(new_filename = ?filename, "create file index done");
                }
            }
        }

        for filename in exists_files {
            let path = self.sync_dir.join(filename);
            let file = File::open(&path)
                .await
                .tap_err(|err| error!(%err, ?path, "open file failed"))?;
            let (hash_sum, block_chain) = hash_file(file).await?;

            match index_guard.get_file(filename).await? {
                None => {
                    error!(exists_filename = ?filename, "exists file not found in index guard");

                    return Err(anyhow::anyhow!(
                        "exists file {:?} not found in index guard",
                        filename
                    ));
                }

                Some(mut index_file) => {
                    if index_file.detail.hash_sum == hash_sum {
                        continue;
                    }

                    info!(exists_filename = ?filename, "get exists file index done");

                    let gen = index_file.detail.gen + 1;
                    let mut old_detail = mem::replace(
                        &mut index_file.detail,
                        FileDetail {
                            gen,
                            hash_sum,
                            block_chain: Some(block_chain),
                            deleted: false,
                        },
                    );
                    old_detail.block_chain.take();
                    index_file.previous_details.push(old_detail);
                    index_file.update_time = SystemTime::now();
                    index_file.update_by = self.user_id.as_hyphenated().to_string();

                    index_guard.update_file(&index_file).await?;

                    info!(exists_filename = ?filename, "update exists file index done");
                }
            }
        }

        let all_file_index_stream = index_guard.list_all_files().await?;

        info!("get all file index stream done");

        futures_util::pin_mut!(all_file_index_stream);

        let index_files = all_file_index_stream
            .try_collect::<Vec<_>>()
            .await
            .tap_err(|err| error!(%err, "collect all index files failed"))?;

        info!("collect all index file done");

        index_guard.commit().await?;

        info!("commit index guard done");

        Ok(index_files)
    }

    async fn send_rumors_to_all<Iter: IntoIterator<Item = IndexFile>>(
        &mut self,
        rumors: Iter,
    ) -> Result<()> {
        let rumors = rumors.into_iter().collect::<Vec<_>>();
        let send_rumors = SendRumors {
            rumors,
            except: None,
        };

        self.rumor_sender.send(send_rumors).await?;

        Ok(())
    }
}

fn get_new_files<'a>(
    entries: &'a HashMap<OsString, DirEntry>,
    index_files: &'a HashMap<OsString, IndexFile>,
) -> Vec<&OsStr> {
    entries
        .keys()
        .filter_map(|filename| match index_files.get(filename) {
            None => Some(filename.as_os_str()),
            Some(index_file) => index_file.detail.deleted.then_some(filename.as_os_str()),
        })
        .collect::<Vec<_>>()
}

fn get_delete_files<'a>(
    entries: &'a HashMap<OsString, DirEntry>,
    index_files: &'a HashMap<OsString, IndexFile>,
) -> Vec<&OsStr> {
    index_files
        .values()
        .filter_map(|index_file| {
            if index_file.detail.deleted {
                None
            } else {
                (!entries.contains_key(&index_file.filename))
                    .then_some(index_file.filename.as_os_str())
            }
        })
        .collect()
}

fn get_exists_files<'a>(
    entries: &'a HashMap<OsString, DirEntry>,
    index_files: &'a HashMap<OsString, IndexFile>,
) -> Vec<&OsStr> {
    entries
        .keys()
        .filter(|filename| match index_files.get(*filename) {
            None => false,
            Some(index_file) => !index_file.detail.deleted,
        })
        .map(|filename| filename.as_os_str())
        .collect()
}
