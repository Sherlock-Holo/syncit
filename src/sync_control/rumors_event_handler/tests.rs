use std::ffi::OsString;
use std::io::Cursor;
use std::os::unix::ffi::OsStrExt;
use std::time::{Duration, SystemTime};
use std::{env, future};

use bytes::Bytes;
use futures_util::stream;
use mockall::predicate::*;
use tempfile::TempDir;
use tokio_stream::wrappers::ReadDirStream;

use super::*;
use crate::index::{FileDetail, FileKind, MockIndex, MockIndexGuard};
use crate::sync_control::hash_file;
use crate::transfer::MockDownloadTransfer;

#[tokio::test]
async fn local_not_exist() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let mut index = MockIndex::new();

    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    {
        let block_chain = block_chain.clone();

        index.expect_begin().returning(move || {
            let mut index_guard = MockIndexGuard::new();
            let block_chain = block_chain.clone();

            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(|_| Ok(None));
            index_guard
                .expect_create_file()
                .with(function(move |arg: &IndexFile| {
                    arg.filename == OsStr::new("test.txt")
                        && arg.kind == FileKind::File
                        && arg.detail
                            == FileDetail {
                                gen: 1,
                                hash_sum,
                                block_chain: Some(block_chain.clone()),
                                deleted: false,
                            }
                        && arg.previous_details.is_empty()
                }))
                .returning(|_| Ok(()));

            index_guard.expect_commit().returning(|| Ok(()));

            Ok(index_guard)
        });
    }

    let mut download_transfer = MockDownloadTransfer::new();

    {
        let block_chain = block_chain.clone();

        download_transfer
            .expect_download()
            .with(function(move |arg: &[DownloadBlockRequest]| {
                blocks_to_download_block_requests(&block_chain.blocks) == arg
            }))
            .returning(|_| {
                Ok(Box::pin(stream::iter([Ok(Some(DownloadBlock {
                    offset: 0,
                    data: Bytes::from_static(b"test"),
                }))])))
            });
    }

    let (sender, receiver) = flume::bounded(1);

    let handler = RumorsEventHandler::new(
        &user_id,
        &dir_id,
        dir.path(),
        &index,
        &download_transfer,
        sender.into_sink(),
    );

    handler
        .handle_rumors_event(
            &user_id,
            vec![IndexFile {
                filename: OsString::from("test.txt"),
                kind: FileKind::File,
                detail: FileDetail {
                    gen: 1,
                    hash_sum,
                    block_chain: Some(block_chain.clone()),
                    deleted: false,
                },
                previous_details: vec![],
                update_time: SystemTime::now(),
                update_by: user_id.as_hyphenated().to_string(),
            }],
        )
        .await
        .unwrap();

    let mut send_rumors = receiver.recv_async().await.unwrap();
    assert_eq!(send_rumors.except, Some(user_id));

    let rumor = send_rumors.rumors.remove(0);
    assert_eq!(rumor.filename, OsStr::new("test.txt"));
    assert_eq!(rumor.kind, FileKind::File);
    assert_eq!(
        rumor.detail,
        FileDetail {
            gen: 1,
            hash_sum,
            block_chain: Some(block_chain),
            deleted: false,
        }
    );
    assert!(rumor.previous_details.is_empty());
    assert_eq!(rumor.update_by, user_id.as_hyphenated().to_string());

    let path = dir.path().join("test.txt");
    assert_eq!(fs::read(path).await.unwrap(), b"test");
}

#[tokio::test]
async fn local_is_latest() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let local_user_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let mut index = MockIndex::new();

    fs::write(dir.path().join("test.txt"), b"test")
        .await
        .unwrap();
    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    {
        let block_chain = block_chain.clone();

        index.expect_begin().returning(move || {
            let mut index_guard = MockIndexGuard::new();
            let block_chain = block_chain.clone();

            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(move |_| {
                    Ok(Some(IndexFile {
                        filename: OsString::from("test.txt"),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 2,
                            hash_sum,
                            block_chain: Some(block_chain.clone()),
                            deleted: false,
                        },
                        previous_details: vec![FileDetail {
                            gen: 1,
                            hash_sum,
                            block_chain: None,
                            deleted: false,
                        }],
                        update_time: SystemTime::now(),
                        update_by: local_user_id.as_hyphenated().to_string(),
                    }))
                });

            Ok(index_guard)
        });
    }

    let download_transfer = MockDownloadTransfer::new();

    let (sender, receiver) = flume::bounded(1);

    let handler = RumorsEventHandler::new(
        &user_id,
        &dir_id,
        dir.path(),
        &index,
        &download_transfer,
        sender.into_sink(),
    );

    handler
        .handle_rumors_event(
            &user_id,
            vec![IndexFile {
                filename: OsString::from("test.txt"),
                kind: FileKind::File,
                detail: FileDetail {
                    gen: 1,
                    hash_sum,
                    block_chain: Some(block_chain.clone()),
                    deleted: false,
                },
                previous_details: vec![],
                update_time: SystemTime::now(),
                update_by: user_id.as_hyphenated().to_string(),
            }],
        )
        .await
        .unwrap();

    receiver.recv_async().await.unwrap_err();
}

#[tokio::test]
async fn remote_is_latest() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let local_user_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let mut index = MockIndex::new();

    fs::write(dir.path().join("test.txt"), b"old")
        .await
        .unwrap();

    let (old_hash_sum, old_block_chain) = hash_file(Cursor::new(b"old")).await.unwrap();
    let (new_hash_sum, new_block_chain) = hash_file(Cursor::new(b"new")).await.unwrap();

    {
        let old_block_chain = old_block_chain.clone();
        let new_block_chain = new_block_chain.clone();

        index.expect_begin().returning(move || {
            let mut index_guard = MockIndexGuard::new();
            let old_block_chain = old_block_chain.clone();
            let new_block_chain = new_block_chain.clone();

            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(move |_| {
                    Ok(Some(IndexFile {
                        filename: OsString::from("test.txt"),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 1,
                            hash_sum: old_hash_sum,
                            block_chain: Some(old_block_chain.clone()),
                            deleted: false,
                        },
                        previous_details: vec![],
                        update_time: SystemTime::UNIX_EPOCH,
                        update_by: local_user_id.as_hyphenated().to_string(),
                    }))
                });
            index_guard
                .expect_update_file()
                .with(function(move |arg: &IndexFile| {
                    arg.filename == OsStr::new("test.txt")
                        && arg.kind == FileKind::File
                        && arg.detail
                            == FileDetail {
                                gen: 2,
                                hash_sum: new_hash_sum,
                                block_chain: Some(new_block_chain.clone()),
                                deleted: false,
                            }
                        && arg.previous_details
                            == vec![FileDetail {
                                gen: 1,
                                hash_sum: old_hash_sum,
                                block_chain: None,
                                deleted: false,
                            }]
                }))
                .returning(|_| Ok(()));

            index_guard.expect_commit().returning(|| Ok(()));

            Ok(index_guard)
        });
    }

    let mut download_transfer = MockDownloadTransfer::new();

    {
        let block_chain = new_block_chain.clone();

        download_transfer
            .expect_download()
            .with(function(move |arg: &[DownloadBlockRequest]| {
                blocks_to_download_block_requests(&block_chain.blocks) == arg
            }))
            .returning(|_| {
                Ok(Box::pin(stream::iter([Ok(Some(DownloadBlock {
                    offset: 0,
                    data: Bytes::from_static(b"new"),
                }))])))
            });
    }

    let (sender, receiver) = flume::bounded(1);

    let handler = RumorsEventHandler::new(
        &user_id,
        &dir_id,
        dir.path(),
        &index,
        &download_transfer,
        sender.into_sink(),
    );

    handler
        .handle_rumors_event(
            &user_id,
            vec![IndexFile {
                filename: OsString::from("test.txt"),
                kind: FileKind::File,
                detail: FileDetail {
                    gen: 2,
                    hash_sum: new_hash_sum,
                    block_chain: Some(new_block_chain.clone()),
                    deleted: false,
                },
                previous_details: vec![FileDetail {
                    gen: 1,
                    hash_sum: old_hash_sum,
                    block_chain: None,
                    deleted: false,
                }],
                update_time: SystemTime::now(),
                update_by: user_id.as_hyphenated().to_string(),
            }],
        )
        .await
        .unwrap();

    let mut send_rumors = receiver.recv_async().await.unwrap();
    assert_eq!(send_rumors.except, Some(user_id));

    let rumor = send_rumors.rumors.remove(0);
    assert_eq!(rumor.filename, OsStr::new("test.txt"));
    assert_eq!(rumor.kind, FileKind::File);
    assert_eq!(
        rumor.detail,
        FileDetail {
            gen: 2,
            hash_sum: new_hash_sum,
            block_chain: Some(new_block_chain),
            deleted: false,
        }
    );
    assert_eq!(
        rumor.previous_details,
        vec![FileDetail {
            gen: 1,
            hash_sum: old_hash_sum,
            block_chain: None,
            deleted: false,
        }]
    );
    assert_eq!(rumor.update_by, user_id.as_hyphenated().to_string());

    let path = dir.path().join("test.txt");
    assert_eq!(fs::read(path).await.unwrap(), b"new");
}

#[tokio::test]
async fn local_remote_same() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let update_time = SystemTime::now();
    let mut index = MockIndex::new();

    fs::write(dir.path().join("test.txt"), b"test")
        .await
        .unwrap();
    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    {
        let block_chain = block_chain.clone();

        index.expect_begin().returning(move || {
            let mut index_guard = MockIndexGuard::new();
            let block_chain = block_chain.clone();

            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(move |_| {
                    Ok(Some(IndexFile {
                        filename: OsString::from("test.txt"),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 1,
                            hash_sum,
                            block_chain: Some(block_chain.clone()),
                            deleted: false,
                        },
                        previous_details: vec![],
                        update_time,
                        update_by: user_id.as_hyphenated().to_string(),
                    }))
                });

            Ok(index_guard)
        });
    }

    let download_transfer = MockDownloadTransfer::new();

    let (sender, receiver) = flume::bounded(1);

    let handler = RumorsEventHandler::new(
        &user_id,
        &dir_id,
        dir.path(),
        &index,
        &download_transfer,
        sender.into_sink(),
    );

    handler
        .handle_rumors_event(
            &user_id,
            vec![IndexFile {
                filename: OsString::from("test.txt"),
                kind: FileKind::File,
                detail: FileDetail {
                    gen: 1,
                    hash_sum,
                    block_chain: Some(block_chain.clone()),
                    deleted: false,
                },
                previous_details: vec![],
                update_time,
                update_by: user_id.as_hyphenated().to_string(),
            }],
        )
        .await
        .unwrap();

    receiver.recv_async().await.unwrap_err();
}

#[tokio::test]
async fn eq_gen_local_latest() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let local_user_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let update_time = SystemTime::now();
    let new_update_time = update_time + Duration::from_secs(1);
    let mut index = MockIndex::new();

    fs::write(dir.path().join("test.txt"), b"test")
        .await
        .unwrap();
    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    {
        let block_chain = block_chain.clone();

        index.expect_begin().returning(move || {
            let mut index_guard = MockIndexGuard::new();
            let block_chain = block_chain.clone();

            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(move |_| {
                    Ok(Some(IndexFile {
                        filename: OsString::from("test.txt"),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 1,
                            hash_sum,
                            block_chain: Some(block_chain.clone()),
                            deleted: false,
                        },
                        previous_details: vec![],
                        update_time: new_update_time,
                        update_by: local_user_id.as_hyphenated().to_string(),
                    }))
                });

            Ok(index_guard)
        });
    }

    let download_transfer = MockDownloadTransfer::new();

    let (sender, receiver) = flume::bounded(1);

    let handler = RumorsEventHandler::new(
        &user_id,
        &dir_id,
        dir.path(),
        &index,
        &download_transfer,
        sender.into_sink(),
    );

    handler
        .handle_rumors_event(
            &user_id,
            vec![IndexFile {
                filename: OsString::from("test.txt"),
                kind: FileKind::File,
                detail: FileDetail {
                    gen: 1,
                    hash_sum,
                    block_chain: Some(block_chain.clone()),
                    deleted: false,
                },
                previous_details: vec![],
                update_time,
                update_by: user_id.as_hyphenated().to_string(),
            }],
        )
        .await
        .unwrap();

    receiver.recv_async().await.unwrap_err();
}

#[tokio::test]
async fn eq_gen_remote_latest() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let local_user_id = Uuid::new_v4();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let update_time = SystemTime::now();
    let new_update_time = update_time + Duration::from_secs(1);
    let mut index = MockIndex::new();

    fs::write(dir.path().join("test.txt"), b"old")
        .await
        .unwrap();
    let (old_hash_sum, old_block_chain) = hash_file(Cursor::new(b"new")).await.unwrap();
    let (new_hash_sum, new_block_chain) = hash_file(Cursor::new(b"new")).await.unwrap();

    {
        let old_block_chain = old_block_chain.clone();
        let new_block_chain = new_block_chain.clone();

        index.expect_begin().returning(move || {
            let mut index_guard = MockIndexGuard::new();
            let old_block_chain = old_block_chain.clone();
            let new_block_chain = new_block_chain.clone();

            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(move |_| {
                    Ok(Some(IndexFile {
                        filename: OsString::from("test.txt"),
                        kind: FileKind::File,
                        detail: FileDetail {
                            gen: 1,
                            hash_sum: old_hash_sum,
                            block_chain: Some(old_block_chain.clone()),
                            deleted: false,
                        },
                        previous_details: vec![],
                        update_time,
                        update_by: local_user_id.as_hyphenated().to_string(),
                    }))
                });

            index_guard
                .expect_update_file()
                .with(function(move |arg: &IndexFile| {
                    arg.filename == OsStr::new("test.txt")
                        && arg.kind == FileKind::File
                        && arg.detail
                            == FileDetail {
                                gen: 1,
                                hash_sum: new_hash_sum,
                                block_chain: Some(new_block_chain.clone()),
                                deleted: false,
                            }
                        && arg.previous_details.is_empty()
                        && arg.update_time == new_update_time
                        && arg.update_by == user_id.as_hyphenated().to_string()
                }))
                .returning(|_| Ok(()));

            index_guard.expect_commit().returning(|| Ok(()));

            Ok(index_guard)
        });
    }

    let mut download_transfer = MockDownloadTransfer::new();

    {
        let new_block_chain = new_block_chain.clone();

        download_transfer
            .expect_download()
            .with(function(move |arg: &[DownloadBlockRequest]| {
                blocks_to_download_block_requests(&new_block_chain.blocks) == arg
            }))
            .returning(|_| {
                Ok(Box::pin(stream::iter([Ok(Some(DownloadBlock {
                    offset: 0,
                    data: Bytes::from_static(b"new"),
                }))])))
            });
    }

    let (sender, receiver) = flume::bounded(1);

    let handler = RumorsEventHandler::new(
        &user_id,
        &dir_id,
        dir.path(),
        &index,
        &download_transfer,
        sender.into_sink(),
    );

    handler
        .handle_rumors_event(
            &user_id,
            vec![IndexFile {
                filename: OsString::from("test.txt"),
                kind: FileKind::File,
                detail: FileDetail {
                    gen: 1,
                    hash_sum: new_hash_sum,
                    block_chain: Some(new_block_chain.clone()),
                    deleted: false,
                },
                previous_details: vec![],
                update_time: new_update_time,
                update_by: user_id.as_hyphenated().to_string(),
            }],
        )
        .await
        .unwrap();

    let mut send_rumors = receiver.recv_async().await.unwrap();
    assert_eq!(send_rumors.except, Some(user_id));

    let rumor = send_rumors.rumors.remove(0);
    assert_eq!(rumor.filename, OsStr::new("test.txt"));
    assert_eq!(rumor.kind, FileKind::File);
    assert_eq!(
        rumor.detail,
        FileDetail {
            gen: 1,
            hash_sum: new_hash_sum,
            block_chain: Some(new_block_chain),
            deleted: false,
        }
    );
    assert!(rumor.previous_details.is_empty());
    assert_eq!(rumor.update_by, user_id.as_hyphenated().to_string());

    let path = dir.path().join("test.txt");
    assert_eq!(fs::read(path).await.unwrap(), b"new");

    let read_dir = fs::read_dir(dir.path()).await.unwrap();
    let read_dir = ReadDirStream::new(read_dir);

    let st = read_dir.try_filter(|entry| {
        let filename = entry.file_name();
        let filename = filename.as_bytes();

        future::ready(filename.starts_with(b"test.txt") && filename.ends_with(b".conflict"))
    });
    let mut st = pin!(st);

    let entry = st.try_next().await.unwrap().unwrap();

    dbg!(entry.file_name());
}

#[tokio::test]
async fn no_require_block() {
    let dir = TempDir::new_in(env::temp_dir()).unwrap();
    let user_id = Uuid::new_v4();
    let dir_id = Uuid::new_v4();
    let mut index = MockIndex::new();

    let (hash_sum, block_chain) = hash_file(Cursor::new(b"test")).await.unwrap();

    {
        let block_chain = block_chain.clone();

        index.expect_begin().returning(move || {
            let mut index_guard = MockIndexGuard::new();
            let block_chain = block_chain.clone();

            index_guard
                .expect_get_file()
                .with(eq(OsStr::new("test.txt")))
                .returning(|_| Ok(None));
            index_guard
                .expect_create_file()
                .with(function(move |arg: &IndexFile| {
                    arg.filename == OsStr::new("test.txt")
                        && arg.kind == FileKind::File
                        && arg.detail
                            == FileDetail {
                                gen: 1,
                                hash_sum,
                                block_chain: Some(block_chain.clone()),
                                deleted: false,
                            }
                        && arg.previous_details.is_empty()
                }))
                .returning(|_| Ok(()));

            Ok(index_guard)
        });
    }

    let mut download_transfer = MockDownloadTransfer::new();

    {
        let block_chain = block_chain.clone();

        download_transfer
            .expect_download()
            .with(function(move |arg: &[DownloadBlockRequest]| {
                blocks_to_download_block_requests(&block_chain.blocks) == arg
            }))
            .returning(|_| Ok(Box::pin(stream::iter([Ok(None)]))));
    }

    let (sender, receiver) = flume::bounded(1);

    let handler = RumorsEventHandler::new(
        &user_id,
        &dir_id,
        dir.path(),
        &index,
        &download_transfer,
        sender.into_sink(),
    );

    handler
        .handle_rumors_event(
            &user_id,
            vec![IndexFile {
                filename: OsString::from("test.txt"),
                kind: FileKind::File,
                detail: FileDetail {
                    gen: 1,
                    hash_sum,
                    block_chain: Some(block_chain.clone()),
                    deleted: false,
                },
                previous_details: vec![],
                update_time: SystemTime::now(),
                update_by: user_id.as_hyphenated().to_string(),
            }],
        )
        .await
        .unwrap();

    receiver.recv_async().await.unwrap_err();
}
