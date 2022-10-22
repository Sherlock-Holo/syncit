use std::error::Error;
use std::ffi::OsStr;
use std::io::ErrorKind;
use std::mem;
use std::path::Path;
use std::time::SystemTime;

use anyhow::Result;
use futures_util::{Sink, SinkExt};
use tokio::fs::File;
use tracing::{error, info};
use uuid::Uuid;

use crate::file_event_produce::WatchEvent;
use crate::index::{FileDetail, FileKind, Index, IndexFile, IndexGuard};
use crate::sync_control::{hash_file, SendRumors};

pub struct WatchEventHandler<'a, I, Si> {
    user_id: &'a Uuid,
    dir_id: &'a Uuid,
    sync_dir: &'a Path,
    index: &'a I,
    rumor_sender: Si,
}

impl<'a, I, Si> WatchEventHandler<'a, I, Si> {
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

impl<'a, I, Si> WatchEventHandler<'a, I, Si>
where
    I: Index,
    <I::Guard as IndexGuard>::Error: Send + Sync + 'static,
    Si: Sink<SendRumors> + Unpin,
    Si::Error: Error + Send + Sync + 'static,
{
    pub async fn handle_watch_events(mut self, watch_events: Vec<WatchEvent>) -> Result<()> {
        let mut rumors = Vec::with_capacity(watch_events.len());

        for event in watch_events {
            let mut index_guard = self.index.begin().await?;

            match event {
                WatchEvent::Add { name } => {
                    match self.handle_add_watch_event(&name, &mut index_guard).await {
                        Err(err) => {
                            error!(%err, ?name, "handle add watch event failed");

                            break;
                        }

                        Ok(rumor) => {
                            if let Some(rumor) = rumor {
                                rumors.push(rumor);
                            }
                        }
                    }
                }
                WatchEvent::Modify { name } => {
                    match self
                        .handle_modify_watch_event(&name, &mut index_guard)
                        .await
                    {
                        Err(err) => {
                            error!(%err, ?name, "handle modify watch event failed");

                            break;
                        }

                        Ok(rumor) => {
                            if let Some(rumor) = rumor {
                                rumors.push(rumor);
                            }
                        }
                    }
                }
                WatchEvent::Rename { old_name, new_name } => {
                    match self
                        .handle_rename_watch_event(&old_name, &new_name, &mut index_guard)
                        .await
                    {
                        Err(err) => {
                            error!(%err, ?old_name, ?new_name, "handle rename watch event failed");

                            break;
                        }

                        Ok(rename_rumors) => {
                            if let Some(rename_rumors) = rename_rumors {
                                rumors.extend(rename_rumors);
                            }
                        }
                    }
                }
                WatchEvent::Delete { name } => {
                    match self
                        .handle_delete_watch_event(&name, &mut index_guard)
                        .await
                    {
                        Err(err) => {
                            error!(%err, ?name, "handle delete watch event failed");

                            break;
                        }

                        Ok(rumor) => {
                            if let Some(rumor) = rumor {
                                rumors.push(rumor);
                            }
                        }
                    }
                }
            }

            info!("handle watch event done");

            index_guard.commit().await?;

            info!("commit index guard done");
        }

        info!("handle all watch events done");

        self.send_rumors_to_all(rumors).await?;

        info!("send rumors to all done");

        Ok(())
    }

    async fn handle_add_watch_event(
        &mut self,
        name: &OsStr,
        index_guard: &mut I::Guard,
    ) -> Result<Option<IndexFile>> {
        let path = self.sync_dir.join(name);
        let file = match File::open(&path).await {
            Err(err) if err.kind() == ErrorKind::NotFound => {
                info!(?path, "ignore not exists file");

                return Ok(None);
            }

            Err(err) => {
                error!(%err, ?path, "open file failed");

                return Err(err.into());
            }

            Ok(file) => file,
        };

        info!(?path, "open file done");

        let (hash_sum, block_chain) = hash_file(file).await?;

        info!(?path, "hash file done");

        let mut index_file = match index_guard.get_file(name).await? {
            None => {
                let index_file = IndexFile {
                    filename: name.to_os_string(),
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

                info!(?path, "create file index done");

                return Ok(Some(index_file));
            }

            Some(index_file) => index_file,
        };

        if !index_file.detail.deleted && index_file.detail.hash_sum == hash_sum {
            info!(?path, "file hash no changed, ignore add watch event");

            return Ok(None);
        }

        let gen = index_file.detail.gen + 1;
        let mut old_info = mem::replace(
            &mut index_file.detail,
            FileDetail {
                gen,
                hash_sum,
                block_chain: Some(block_chain),
                deleted: false,
            },
        );
        old_info.block_chain.take();

        index_file.previous_details.push(old_info);

        index_guard.update_file(&index_file).await?;

        info!(?path, "update file index done");

        Ok(Some(index_file))
    }

    async fn handle_modify_watch_event(
        &mut self,
        name: &OsStr,
        index_guard: &mut I::Guard,
    ) -> Result<Option<IndexFile>> {
        let path = self.sync_dir.join(name);
        let file = match File::open(&path).await {
            Err(err) if err.kind() == ErrorKind::NotFound => {
                return match index_guard.get_file(name).await? {
                    None => {
                        info!(
                            ?path,
                            "file not exists and index doesn't contain it, ignore"
                        );

                        Ok(None)
                    }

                    Some(index_file) if index_file.detail.deleted => {
                        info!(?path, "file not exists and index file is deleted, ignore");

                        Ok(None)
                    }

                    Some(mut index_file) => {
                        let gen = index_file.detail.gen + 1;
                        let mut old_info = mem::replace(
                            &mut index_file.detail,
                            FileDetail {
                                gen,
                                hash_sum: [0; 32],
                                block_chain: None,
                                deleted: true,
                            },
                        );
                        old_info.block_chain.take();
                        index_file.previous_details.push(old_info);

                        index_guard.update_file(&index_file).await?;

                        info!(?path, "update file index done");

                        Ok(Some(index_file))
                    }
                }
            }

            Err(err) => {
                error!(%err, ?path, "open file failed");

                return Err(err.into());
            }

            Ok(file) => file,
        };

        info!(?path, "open file done");

        let (hash_sum, block_chain) = hash_file(file).await?;

        info!(?path, "hash file done");

        let mut index_file = match index_guard.get_file(name).await? {
            None => {
                let index_file = IndexFile {
                    filename: name.to_os_string(),
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

                info!(?path, "create file index done");

                return Ok(Some(index_file));
            }

            Some(index_file) => index_file,
        };

        if !index_file.detail.deleted && index_file.detail.hash_sum == hash_sum {
            info!(?path, "file hash no changed, ignore modify watch event");

            return Ok(None);
        }

        let gen = index_file.detail.gen + 1;
        let mut old_info = mem::replace(
            &mut index_file.detail,
            FileDetail {
                gen,
                hash_sum,
                block_chain: Some(block_chain),
                deleted: false,
            },
        );
        old_info.block_chain.take();

        index_file.previous_details.push(old_info);

        index_guard.update_file(&index_file).await?;

        info!(?path, "update file index done");

        Ok(Some(index_file))
    }

    async fn handle_rename_watch_event(
        &mut self,
        old_name: &OsStr,
        new_name: &OsStr,
        index_guard: &mut I::Guard,
    ) -> Result<Option<Vec<IndexFile>>> {
        let new_path = self.sync_dir.join(new_name);
        let new_file = match File::open(&new_path).await {
            Err(err) if err.kind() == ErrorKind::NotFound => {
                let mut old_index_file = match index_guard.get_file(old_name).await? {
                    None => {
                        info!(?old_name, "old file index not exists, ignore");

                        return Ok(None);
                    }

                    Some(index_file) if index_file.detail.deleted => {
                        info!(?old_name, "old file index deleted has been set, ignore");

                        return Ok(None);
                    }

                    Some(index_file) => index_file,
                };

                let gen = old_index_file.detail.gen + 1;
                let mut old_old_file_info = mem::replace(
                    &mut old_index_file.detail,
                    FileDetail {
                        gen,
                        hash_sum: [0; 32],
                        block_chain: None,
                        deleted: true,
                    },
                );
                old_old_file_info.block_chain.take();
                old_index_file.previous_details.push(old_old_file_info);

                index_guard.update_file(&old_index_file).await?;

                info!(?old_name, "update old file index done");

                let mut new_index_file = match index_guard.get_file(new_name).await? {
                    None => {
                        info!(?new_path, "new file not exists and file index too");

                        return Ok(Some(vec![old_index_file]));
                    }

                    Some(new_index_file) if new_index_file.detail.deleted => {
                        info!(
                            ?new_path,
                            "new file not exists and file index deleted has been set"
                        );

                        return Ok(Some(vec![old_index_file]));
                    }

                    Some(new_index_file) => new_index_file,
                };

                let gen = new_index_file.detail.gen + 1;
                let mut old_new_file_info = mem::replace(
                    &mut new_index_file.detail,
                    FileDetail {
                        gen,
                        hash_sum: [0; 32],
                        block_chain: None,
                        deleted: true,
                    },
                );
                old_new_file_info.block_chain.take();
                new_index_file.previous_details.push(old_new_file_info);

                index_guard.update_file(&new_index_file).await?;

                info!(?new_name, "update new file index done");

                return Ok(Some(vec![old_index_file, new_index_file]));
            }

            Err(err) => {
                error!(%err, ?new_path, "open file failed");

                return Err(err.into());
            }

            Ok(file) => file,
        };

        let (hash_sum, block_chain) = hash_file(new_file).await?;

        let mut rumors = Vec::with_capacity(2);

        // update old file index at first
        match index_guard.get_file(old_name).await? {
            None => {
                info!(?old_name, "old file index not exists, ignore");
            }

            Some(index_file) if index_file.detail.deleted => {
                info!(?old_name, "old file index deleted has been set, ignore");
            }

            Some(mut old_index_file) => {
                let gen = old_index_file.detail.gen + 1;
                let mut old_old_file_info = mem::replace(
                    &mut old_index_file.detail,
                    FileDetail {
                        gen,
                        hash_sum: [0; 32],
                        block_chain: None,
                        deleted: true,
                    },
                );
                old_old_file_info.block_chain.take();
                old_index_file.previous_details.push(old_old_file_info);

                index_guard.update_file(&old_index_file).await?;

                rumors.push(old_index_file);
            }
        };

        let index_file = match index_guard.get_file(new_name).await? {
            None => {
                let index_file = IndexFile {
                    filename: new_name.to_os_string(),
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

                info!(?new_name, "create new file index done");

                index_file
            }

            Some(mut index_file) => {
                let gen = index_file.detail.gen + 1;
                let mut old_info = mem::replace(
                    &mut index_file.detail,
                    FileDetail {
                        gen,
                        hash_sum,
                        block_chain: Some(block_chain),
                        deleted: false,
                    },
                );
                old_info.block_chain.take();
                index_file.previous_details.push(old_info);

                index_guard.update_file(&index_file).await?;

                info!(?new_name, "update new file index done");

                index_file
            }
        };

        rumors.push(index_file);

        Ok(Some(rumors))
    }

    async fn handle_delete_watch_event(
        &mut self,
        name: &OsStr,
        index_guard: &mut I::Guard,
    ) -> Result<Option<IndexFile>> {
        let mut index_file = match index_guard.get_file(name).await? {
            None => {
                info!(?name, "file has no index, ignore");

                return Ok(None);
            }

            Some(index_file) if index_file.detail.deleted => {
                info!(?name, "file index deleted has been set, ignore");

                return Ok(None);
            }

            Some(index_file) => index_file,
        };

        let gen = index_file.detail.gen + 1;
        let mut old_info = mem::replace(
            &mut index_file.detail,
            FileDetail {
                gen,
                hash_sum: [0; 32],
                block_chain: None,
                deleted: true,
            },
        );
        old_info.block_chain.take();
        index_file.previous_details.push(old_info);

        index_guard.update_file(&index_file).await?;

        info!(?name, "update file index done");

        Ok(Some(index_file))
    }

    async fn send_rumors_to_all<Iter: IntoIterator<Item = IndexFile>>(
        &mut self,
        rumors: Iter,
    ) -> Result<()> {
        let rumors = rumors.into_iter().collect::<Vec<_>>();
        if rumors.is_empty() {
            info!("ignore empty rumors");

            return Ok(());
        }

        let send_rumors = SendRumors {
            rumors,
            except: None,
        };

        self.rumor_sender.send(send_rumors).await?;

        Ok(())
    }
}

#[cfg(test)]
mod add_tests;
#[cfg(test)]
mod delete_tests;
#[cfg(test)]
mod modify_tests;
#[cfg(test)]
mod rename_tests;
